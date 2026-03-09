"""
Operações administrativas coordenadas com lock CAS em KV e fencing token.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

from nats.aio.msg import Msg
from nats.js.errors import KeyNotFoundError, NoKeysError

from core.nats_client import NATSClient, is_kv_revision_conflict, wait_forever
from services.vote_protocol import (
    encode_json,
    lock_key,
    monitor_control_all_key,
    monitor_control_key,
    monitor_leader_key,
    parse_json,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [VOTE_ADMIN] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("VOTE_ADMIN")


class LockLease:
    def __init__(self, key: str, token: int, renew_task: asyncio.Task):
        self.key = key
        self.token = token
        self.renew_task = renew_task


class AdminService:
    def __init__(self) -> None:
        self.client = NATSClient()
        self.stop_event = asyncio.Event()
        self.node_id = f"admin-{uuid.uuid4()}"
        self.expected_server_ids = self._parse_expected_server_ids(
            os.getenv("MONITOR_SERVER_IDS", "s1,s2,s3")
        )

        self.kv_control = None
        self.kv_state = None
        self.kv_count = None
        self.kv_monitor_control = None
        self.kv_monitor_leader = None

        self.reset_sub = None
        self.server_sub = None

    async def start(self) -> None:
        await self.client.connect("vote-admin")
        assert self.client.nc is not None
        assert self.client.js is not None

        self.kv_control = await self.client.get_key_value("KV_CONTROL")
        self.kv_state = await self.client.get_key_value("KV_VOTE_STATE")
        self.kv_count = await self.client.get_key_value("KV_VOTE_COUNT")
        self.kv_monitor_control = await self.client.get_key_value("KV_MONITOR_CONTROL")
        self.kv_monitor_leader = await self.client.get_key_value("KV_MONITOR_LEADER")

        self.reset_sub = await self.client.nc.subscribe(
            "svc.vote.admin.reset.*",
            queue="vote-admin",
            cb=self._handle_reset,
        )
        self.server_sub = await self.client.nc.subscribe(
            "svc.vote.admin.server.*",
            queue="vote-admin",
            cb=self._handle_server_control,
        )
        logger.info("Serviço admin escutando em svc.vote.admin.reset.* e svc.vote.admin.server.*")
        await wait_forever(self.stop_event)

    async def stop(self) -> None:
        self.stop_event.set()
        if self.reset_sub:
            await self.reset_sub.unsubscribe()
        if self.server_sub:
            await self.server_sub.unsubscribe()
        await self.client.close()

    async def _handle_reset(self, msg: Msg) -> None:
        payload = parse_json(msg.data) if msg.data else {}
        room_id = str(payload.get("room_id") or self._subject_room(msg.subject))
        trace_id = str(payload.get("trace_id") or "")
        operation = "reset"
        lock = await self._acquire_lock(room_id, operation)
        if not lock:
            await msg.respond(
                encode_json(
                    {
                        "schema": "vote.admin.reset.v1",
                        "status": "locked",
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "error": "operation already in progress",
                    }
                )
            )
            return

        try:
            deleted_state = await self._delete_room_keys(self.kv_state, f"{room_id}.vote.")
            deleted_count = await self._delete_room_keys(self.kv_count, f"{room_id}.candidate.")
            await self._emit_zero_snapshot(room_id)

            monitor_control = await self._control_monitor_servers(
                room_id=room_id,
                action="restart",
                target_server_ids=None,
            )

            await msg.respond(
                encode_json(
                    {
                        "schema": "vote.admin.reset.v1",
                        "status": "success",
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "deleted_state_keys": deleted_state,
                        "deleted_count_keys": deleted_count,
                        "monitor_control": monitor_control,
                        "fencing_token": lock.token,
                    }
                )
            )
        finally:
            await self._release_lock(lock)

    async def _handle_server_control(self, msg: Msg) -> None:
        payload = parse_json(msg.data) if msg.data else {}
        room_id = str(payload.get("room_id") or self._subject_room(msg.subject))
        trace_id = str(payload.get("trace_id") or "")
        action = str(payload.get("action") or "").strip().lower()
        server_id = str(payload.get("server_id") or "").strip().lower()
        service_name = str(payload.get("service_name") or "").strip()

        if action not in {"stop", "recover", "restart"}:
            await msg.respond(
                encode_json(
                    {
                        "schema": "vote.admin.server.v1",
                        "status": "error",
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "error": "invalid_action",
                    }
                )
            )
            return

        target_ids = self._resolve_target_server_ids(server_id, service_name)
        if not target_ids:
            await msg.respond(
                encode_json(
                    {
                        "schema": "vote.admin.server.v1",
                        "status": "error",
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "error": "server_not_found",
                    }
                )
            )
            return

        result = await self._control_monitor_servers(
            room_id=room_id,
            action=action,
            target_server_ids=target_ids,
        )
        if action == "stop":
            await self._expire_leader_if_needed(room_id, set(target_ids))

        status = "success" if result["success_count"] == result["requested"] else "error"
        await msg.respond(
            encode_json(
                {
                    "schema": "vote.admin.server.v1",
                    "status": status,
                    "room_id": room_id,
                    "trace_id": trace_id,
                    "action": action,
                    "target_server_ids": target_ids,
                    "result": result,
                }
            )
        )

    async def _control_monitor_servers(
        self,
        room_id: str,
        action: str,
        target_server_ids: Optional[List[str]],
    ) -> Dict[str, Any]:
        assert self.kv_monitor_control is not None

        target_ids = list(target_server_ids) if target_server_ids else list(self.expected_server_ids)
        command_id = str(uuid.uuid4())
        now = time.time()
        results: List[Dict[str, Any]] = []

        if target_server_ids is None:
            await self._upsert_key(
                kv=self.kv_monitor_control,
                key=monitor_control_all_key(room_id),
                payload={
                    "schema": "vote.monitor.control.v1",
                    "room_id": room_id,
                    "scope": "all",
                    "action": action,
                    "command_id": command_id,
                    "requested_at": now,
                    "requested_by": self.node_id,
                },
            )

        for sid in target_ids:
            key = monitor_control_key(room_id, sid)
            payload = {
                "schema": "vote.monitor.control.v1",
                "room_id": room_id,
                "server_id": sid,
                "action": action,
                "command_id": command_id,
                "requested_at": now,
                "requested_by": self.node_id,
            }
            try:
                await self._upsert_key(
                    kv=self.kv_monitor_control,
                    key=key,
                    payload=payload,
                )
                results.append(
                    {
                        "server_id": sid,
                        "ok": True,
                        "action": action,
                        "command_id": command_id,
                    }
                )
            except Exception as exc:  # pragma: no cover - proteção extra
                results.append(
                    {
                        "server_id": sid,
                        "ok": False,
                        "action": action,
                        "error": f"{type(exc).__name__}:{exc}",
                    }
                )

        success_count = sum(1 for item in results if item.get("ok"))
        return {
            "requested": len(target_ids),
            "success_count": success_count,
            "results": results,
            "command_id": command_id,
        }

    async def _expire_leader_if_needed(self, room_id: str, stopped_ids: set[str]) -> None:
        assert self.kv_monitor_leader is not None
        key = monitor_leader_key(room_id)
        try:
            entry = await self.kv_monitor_leader.get(key)
        except KeyNotFoundError:
            return

        payload = parse_json(entry.value)
        current_leader_id = str(payload.get("server_id") or "").strip().lower()
        if current_leader_id not in stopped_ids:
            return

        payload["lease_until"] = time.time() - 0.1
        payload["updated_at"] = time.time()
        payload["forced_expire"] = True
        try:
            await self.kv_monitor_leader.update(key, encode_json(payload), entry.revision)
        except Exception as exc:
            if not is_kv_revision_conflict(exc):
                raise

    async def _acquire_lock(self, room_id: str, operation: str) -> Optional[LockLease]:
        assert self.kv_control is not None
        key = lock_key(room_id, operation)
        # Token de fencing evita operações administrativas simultâneas.
        value = {
            "holder": self.node_id,
            "room_id": room_id,
            "operation": operation,
            "started_at": time.time(),
            "updated_at": time.time(),
        }
        try:
            token = await self.kv_control.create(key, encode_json(value))
        except Exception as exc:
            if is_kv_revision_conflict(exc):
                return None
            raise

        async def renew_loop() -> None:
            nonlocal token
            while not self.stop_event.is_set():
                await asyncio.sleep(5.0)
                value["updated_at"] = time.time()
                try:
                    token = await self.kv_control.update(key, encode_json(value), token)
                except Exception:
                    logger.warning("Falha ao renovar lock %s", key)
                    return

        task = asyncio.create_task(renew_loop())
        return LockLease(key=key, token=token, renew_task=task)

    async def _release_lock(self, lock: LockLease) -> None:
        assert self.kv_control is not None
        lock.renew_task.cancel()
        try:
            await lock.renew_task
        except asyncio.CancelledError:
            pass
        try:
            await self.kv_control.delete(lock.key, last=lock.token)
        except Exception:
            logger.warning("Não foi possível liberar lock %s", lock.key)

    async def _delete_room_keys(self, kv, prefix: str) -> int:
        deleted = 0
        try:
            keys = await kv.keys()
        except NoKeysError:
            return 0

        for key in keys:
            if not key.startswith(prefix):
                continue
            try:
                entry = await kv.get(key)
            except KeyNotFoundError:
                continue
            await kv.delete(key, last=entry.revision)
            deleted += 1
        return deleted

    async def _emit_zero_snapshot(self, room_id: str) -> None:
        assert self.client.nc is not None
        await self.client.nc.publish(
            f"evt.vote.results.{room_id}",
            encode_json(
                {
                    "schema": "vote.results.v1",
                    "status": "success",
                    "room_id": room_id,
                    "counts": {},
                    "total_votes": 0,
                    "source_event": "vote.admin.reset",
                    "updated_at": time.time(),
                }
            ),
        )

    async def _upsert_key(self, kv, key: str, payload: Dict[str, Any]) -> None:
        encoded = encode_json(payload)
        try:
            entry = await kv.get(key)
            await kv.update(key, encoded, entry.revision)
            return
        except KeyNotFoundError:
            pass
        except Exception as exc:
            if not is_kv_revision_conflict(exc):
                raise

        try:
            await kv.create(key, encoded)
        except Exception as exc:
            if not is_kv_revision_conflict(exc):
                raise
            entry = await kv.get(key)
            await kv.update(key, encoded, entry.revision)

    @staticmethod
    def _subject_room(subject: str) -> str:
        parts = subject.split(".")
        return parts[-1] if parts else "default"

    @staticmethod
    def _parse_expected_server_ids(raw_value: str) -> List[str]:
        values = [item.strip().lower() for item in raw_value.split(",") if item.strip()]
        return values or ["s1", "s2", "s3"]

    def _resolve_target_server_ids(self, server_id: str, service_name: str) -> List[str]:
        if server_id:
            normalized = server_id.lower()
            return [normalized] if normalized in self.expected_server_ids else []

        if service_name:
            lowered = service_name.lower()
            if lowered in self.expected_server_ids:
                return [lowered]
            for sid in self.expected_server_ids:
                token = f"-{sid}-"
                if token in lowered or lowered.endswith(f"-{sid}"):
                    return [sid]

        return []


async def main() -> None:
    service = AdminService()
    try:
        await service.start()
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
