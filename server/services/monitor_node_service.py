"""
Nó de monitoramento via NATS para eleição de líder e simulação de falha.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import uuid
from typing import Any, Dict, Optional

from nats.js.errors import KeyNotFoundError

from core.nats_client import NATSClient, is_kv_revision_conflict
from services.vote_protocol import (
    encode_json,
    monitor_control_all_key,
    monitor_control_key,
    monitor_leader_key,
    monitor_state_key,
    parse_json,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MONITOR_NODE] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("MONITOR_NODE")


class MonitorNodeService:
    def __init__(self) -> None:
        self.client = NATSClient()
        self.stop_event = asyncio.Event()

        self.room_id = os.getenv("MONITOR_ROOM_ID", "default")
        self.server_id = os.getenv("MONITOR_SERVER_ID", "s1").strip().lower()
        self.service_name = os.getenv(
            "MONITOR_SERVICE_NAME",
            f"tally-{self.server_id}-{self.room_id}",
        )
        self.address = os.getenv("MONITOR_ADDRESS", self.server_id)
        self.port = int(os.getenv("MONITOR_PORT", str(self._default_port(self.server_id))))
        self.http_port = int(
            os.getenv(
                "MONITOR_HTTP_PORT",
                str(self._default_http_port(self.server_id)),
            )
        )
        self.heartbeat_seconds = float(os.getenv("MONITOR_HEARTBEAT_SECONDS", "2.0"))
        self.lease_seconds = float(os.getenv("MONITOR_LEASE_SECONDS", "6.0"))
        self.node_id = f"monitor-{self.server_id}-{uuid.uuid4()}"

        self.simulated_failure = False
        self.last_control_by_key: Dict[str, str] = {}
        self.last_applied_command_id = ""

        self.kv_state = None
        self.kv_leader = None
        self.kv_control = None

    async def start(self) -> None:
        await self.client.connect(f"monitor-{self.server_id}")
        assert self.client.js is not None

        self.kv_state = await self.client.get_key_value("KV_MONITOR_STATE")
        self.kv_leader = await self.client.get_key_value("KV_MONITOR_LEADER")
        self.kv_control = await self.client.get_key_value("KV_MONITOR_CONTROL")

        logger.info(
            "Nó monitor online server_id=%s room=%s service_name=%s",
            self.server_id,
            self.room_id,
            self.service_name,
        )

        try:
            while not self.stop_event.is_set():
                now = time.time()
                await self._apply_control_commands()
                current_leader = await self._sync_leader(now)
                await self._publish_state(now, current_leader)
                await asyncio.sleep(self.heartbeat_seconds)
        finally:
            await self.client.close()

    async def _apply_control_commands(self) -> None:
        assert self.kv_control is not None
        # Processa comando do nó e comando global da sala.
        for key in (
            monitor_control_key(self.room_id, self.server_id),
            monitor_control_all_key(self.room_id),
        ):
            try:
                entry = await self.kv_control.get(key)
            except KeyNotFoundError:
                continue

            payload = parse_json(entry.value)
            command_id = str(payload.get("command_id") or "")
            if not command_id:
                continue

            if self.last_control_by_key.get(key) == command_id:
                continue
            if self.last_applied_command_id == command_id:
                self.last_control_by_key[key] = command_id
                continue

            action = str(payload.get("action") or "").strip().lower()
            if action == "stop":
                self.simulated_failure = True
                logger.info("Ação de controle stop aplicada para %s", self.server_id)
            elif action in {"recover", "restart"}:
                self.simulated_failure = False
                logger.info("Ação de controle %s aplicada para %s", action, self.server_id)

            self.last_control_by_key[key] = command_id
            self.last_applied_command_id = command_id

    async def _sync_leader(self, now: float) -> Optional[str]:
        assert self.kv_leader is not None

        # Em falha simulada, o nó abre mão da liderança.
        if self.simulated_failure:
            await self._resign_if_leader(now)
            return None

        leader_entry = await self._read_leader_entry()
        if leader_entry and self._is_lease_valid(leader_entry, now):
            leader_id = str(leader_entry.get("server_id") or "")
            if leader_id == self.server_id:
                renewed = await self._renew_leader(leader_entry, now)
                if renewed:
                    return self.server_id
                current = await self._read_leader_entry()
                if current and self._is_lease_valid(current, time.time()):
                    return str(current.get("server_id") or "") or None
                return None
            return leader_id or None

        acquired = await self._acquire_leader(leader_entry, now)
        if acquired:
            return self.server_id

        current = await self._read_leader_entry()
        if current and self._is_lease_valid(current, time.time()):
            return str(current.get("server_id") or "") or None
        return None

    async def _renew_leader(self, leader_entry: Dict[str, Any], now: float) -> bool:
        assert self.kv_leader is not None
        key = monitor_leader_key(self.room_id)
        payload = self._leader_payload(now)
        try:
            await self.kv_leader.update(
                key,
                encode_json(payload),
                int(leader_entry["_revision"]),
            )
            return True
        except Exception as exc:
            if is_kv_revision_conflict(exc):
                return False
            raise

    async def _acquire_leader(self, leader_entry: Optional[Dict[str, Any]], now: float) -> bool:
        assert self.kv_leader is not None
        key = monitor_leader_key(self.room_id)
        payload = self._leader_payload(now)
        if leader_entry:
            try:
                await self.kv_leader.update(
                    key,
                    encode_json(payload),
                    int(leader_entry["_revision"]),
                )
                logger.info("Lease de líder adquirida room=%s server=%s", self.room_id, self.server_id)
                return True
            except Exception as exc:
                if is_kv_revision_conflict(exc):
                    return False
                raise

        try:
            await self.kv_leader.create(key, encode_json(payload))
            logger.info("Lease de líder criada room=%s server=%s", self.room_id, self.server_id)
            return True
        except Exception as exc:
            if is_kv_revision_conflict(exc):
                return False
            raise

    async def _resign_if_leader(self, now: float) -> None:
        assert self.kv_leader is not None
        leader_entry = await self._read_leader_entry()
        if not leader_entry:
            return
        if str(leader_entry.get("server_id") or "") != self.server_id:
            return

        key = monitor_leader_key(self.room_id)
        payload = self._leader_payload(now)
        payload["lease_until"] = now - 0.1
        payload["simulated_failure"] = True
        try:
            await self.kv_leader.update(key, encode_json(payload), int(leader_entry["_revision"]))
        except Exception:
            pass

    async def _read_leader_entry(self) -> Optional[Dict[str, Any]]:
        assert self.kv_leader is not None
        key = monitor_leader_key(self.room_id)
        try:
            entry = await self.kv_leader.get(key)
        except KeyNotFoundError:
            return None
        payload = parse_json(entry.value)
        payload["_revision"] = entry.revision
        return payload

    async def _publish_state(self, now: float, current_leader: Optional[str]) -> None:
        assert self.kv_state is not None
        key = monitor_state_key(self.room_id, self.server_id)

        role = "failed" if self.simulated_failure else "backup"
        if not self.simulated_failure and current_leader == self.server_id:
            role = "leader"

        payload: Dict[str, Any] = {
            "service_name": self.service_name,
            "server_id": self.server_id,
            "address": self.address,
            "port": self.port,
            "http_port": self.http_port,
            "role": role,
            "current_leader": current_leader or "-",
            "simulated_failure": self.simulated_failure,
            "healthy": True,
            "last_seen": now,
            "updated_at": now,
        }

        try:
            entry = await self.kv_state.get(key)
            await self.kv_state.update(key, encode_json(payload), entry.revision)
            return
        except KeyNotFoundError:
            pass
        except Exception as exc:
            if not is_kv_revision_conflict(exc):
                raise

        try:
            await self.kv_state.create(key, encode_json(payload))
        except Exception as exc:
            if not is_kv_revision_conflict(exc):
                raise
            try:
                entry = await self.kv_state.get(key)
                await self.kv_state.update(key, encode_json(payload), entry.revision)
            except Exception:
                logger.warning("Não foi possível publicar estado do monitor para %s", self.server_id)

    def _leader_payload(self, now: float) -> Dict[str, Any]:
        return {
            "server_id": self.server_id,
            "service_name": self.service_name,
            "address": self.address,
            "port": self.port,
            "http_port": self.http_port,
            "room_id": self.room_id,
            "holder": self.node_id,
            "lease_until": now + self.lease_seconds,
            "updated_at": now,
        }

    @staticmethod
    def _is_lease_valid(leader_entry: Dict[str, Any], now: float) -> bool:
        lease_until = float(leader_entry.get("lease_until") or 0)
        return lease_until > now

    @staticmethod
    def _default_port(server_id: str) -> int:
        suffix = server_id.replace("s", "")
        if suffix.isdigit():
            return 8000 + int(suffix)
        return 8001

    @staticmethod
    def _default_http_port(server_id: str) -> int:
        suffix = server_id.replace("s", "")
        if suffix.isdigit():
            return 9000 + int(suffix)
        return 9001


async def main() -> None:
    service = MonitorNodeService()
    await service.start()


if __name__ == "__main__":
    asyncio.run(main())
