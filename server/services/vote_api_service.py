"""
API NATS request/reply para envio de voto, consulta de status e monitoramento.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

from nats.aio.msg import Msg
from nats.js.errors import KeyNotFoundError, NoKeysError

from core.nats_client import NATSClient, wait_forever
from services.vote_protocol import (
    MONITOR_RESET_SCHEMA,
    MONITOR_SERVER_SCHEMA,
    MONITOR_STATUS_SCHEMA,
    RESULT_SCHEMA,
    VoteCommand,
    encode_json,
    monitor_leader_key,
    parse_json,
    validate_submit_payload,
    vote_count_key,
    vote_state_key,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [VOTE_API] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("VOTE_API")


class VoteAPIService:
    def __init__(self) -> None:
        self.client = NATSClient()
        self.stop_event = asyncio.Event()
        self.processing_wait_seconds = float(os.getenv("VOTE_API_PROCESSING_WAIT_SECONDS", "2.5"))
        self.poll_interval_seconds = float(os.getenv("VOTE_API_POLL_INTERVAL_SECONDS", "0.1"))
        self.expected_server_ids = self._parse_expected_server_ids(
            os.getenv("MONITOR_SERVER_IDS", "s1,s2,s3")
        )

        self.kv_state = None
        self.kv_count = None
        self.kv_monitor_state = None
        self.kv_monitor_leader = None

        self.subscriptions = []

    async def start(self) -> None:
        await self.client.connect("vote-api")
        assert self.client.nc is not None
        assert self.client.js is not None

        self.kv_state = await self.client.get_key_value("KV_VOTE_STATE")
        self.kv_count = await self.client.get_key_value("KV_VOTE_COUNT")
        self.kv_monitor_state = await self.client.get_key_value("KV_MONITOR_STATE")
        self.kv_monitor_leader = await self.client.get_key_value("KV_MONITOR_LEADER")

        self.subscriptions.append(
            await self.client.nc.subscribe(
                "svc.vote.submit.*",
                queue="vote-api",
                cb=self._handle_submit,
            )
        )
        self.subscriptions.append(
            await self.client.nc.subscribe(
                "svc.vote.status.*",
                queue="vote-api",
                cb=self._handle_status,
            )
        )
        self.subscriptions.append(
            await self.client.nc.subscribe(
                "svc.vote.results.*",
                queue="vote-api",
                cb=self._handle_results,
            )
        )
        self.subscriptions.append(
            await self.client.nc.subscribe(
                "svc.vote.monitor.status.*",
                queue="vote-api",
                cb=self._handle_monitor_status,
            )
        )
        self.subscriptions.append(
            await self.client.nc.subscribe(
                "svc.vote.monitor.reset.*",
                queue="vote-api",
                cb=self._handle_monitor_reset,
            )
        )
        self.subscriptions.append(
            await self.client.nc.subscribe(
                "svc.vote.monitor.server.*",
                queue="vote-api",
                cb=self._handle_monitor_server_action,
            )
        )
        logger.info(
            "Serviço Vote API escutando em "
            "svc.vote.submit/status/results + "
            "svc.vote.monitor.status/reset/server"
        )
        await wait_forever(self.stop_event)

    async def stop(self) -> None:
        self.stop_event.set()
        for sub in self.subscriptions:
            await sub.unsubscribe()
        await self.client.close()

    async def _handle_submit(self, msg: Msg) -> None:
        trace_id = ""
        try:
            payload = parse_json(msg.data)
            is_valid, reason = validate_submit_payload(payload)
            trace_id = str(payload.get("trace_id") or "")
            if not is_valid:
                await msg.respond(
                    encode_json(
                        {
                            "schema": RESULT_SCHEMA,
                            "status": "error",
                            "error": reason,
                            "trace_id": trace_id,
                        }
                    )
                )
                return

            cmd = VoteCommand.from_payload(payload)
            trace_id = cmd.trace_id
            subject = f"vote.cmd.{cmd.room_id}"
            assert self.client.js is not None
            # Evita dupla contagem em reenvio do mesmo voto.
            await self.client.js.publish(
                subject=subject,
                payload=encode_json(
                    {
                        "schema": "vote.cmd.v1",
                        "vote_id": cmd.vote_id,
                        "room_id": cmd.room_id,
                        "candidate_id": cmd.candidate_id,
                        "voter_id": cmd.voter_id,
                        "client_ts": cmd.client_ts,
                        "trace_id": cmd.trace_id,
                    }
                ),
                headers={"Nats-Msg-Id": cmd.vote_id},
            )

            result = await self._wait_for_vote_result(cmd.room_id, cmd.vote_id)
            await msg.respond(encode_json(result))
        except Exception as exc:
            logger.exception("Falha no handler de submit: %s", exc)
            await msg.respond(
                encode_json(
                    {
                        "schema": RESULT_SCHEMA,
                        "status": "error",
                        "error": "submit_failed",
                        "trace_id": trace_id,
                    }
                )
            )

    async def _handle_status(self, msg: Msg) -> None:
        try:
            payload = parse_json(msg.data)
            room_id = str(payload.get("room_id") or self._subject_room(msg.subject))
            vote_id = str(payload.get("vote_id") or "")
            trace_id = str(payload.get("trace_id") or "")
            if not vote_id:
                await msg.respond(
                    encode_json(
                        {
                            "schema": RESULT_SCHEMA,
                            "status": "error",
                            "error": "missing vote_id",
                            "trace_id": trace_id,
                        }
                    )
                )
                return

            state = await self._get_vote_state(room_id, vote_id)
            if not state:
                await msg.respond(
                    encode_json(
                        {
                            "schema": RESULT_SCHEMA,
                            "status": "pending",
                            "vote_id": vote_id,
                            "room_id": room_id,
                            "trace_id": trace_id,
                        }
                    )
                )
                return

            if state.get("status") == "notified" and state.get("result"):
                await msg.respond(encode_json(state["result"]))
                return

            await msg.respond(
                encode_json(
                    {
                        "schema": RESULT_SCHEMA,
                        "status": "pending",
                        "vote_id": vote_id,
                        "room_id": room_id,
                        "trace_id": trace_id,
                    }
                )
            )
        except Exception as exc:
            logger.exception("Falha no handler de status: %s", exc)
            await msg.respond(
                encode_json(
                    {
                        "schema": RESULT_SCHEMA,
                        "status": "error",
                        "error": "status_failed",
                    }
                )
            )

    async def _handle_results(self, msg: Msg) -> None:
        try:
            payload = parse_json(msg.data) if msg.data else {}
            room_id = str(payload.get("room_id") or self._subject_room(msg.subject))
            trace_id = str(payload.get("trace_id") or "")
            counts = await self._collect_counts(room_id)
            await msg.respond(
                encode_json(
                    {
                        "schema": "vote.results.v1",
                        "status": "success",
                        "room_id": room_id,
                        "counts": counts,
                        "total_votes": sum(counts.values()),
                        "trace_id": trace_id,
                    }
                )
            )
        except Exception as exc:
            logger.exception("Falha no handler de resultados: %s", exc)
            await msg.respond(
                encode_json(
                    {
                        "schema": "vote.results.v1",
                        "status": "error",
                        "error": "results_failed",
                    }
                )
            )

    async def _handle_monitor_status(self, msg: Msg) -> None:
        try:
            payload = parse_json(msg.data) if msg.data else {}
            room_id = str(payload.get("room_id") or self._subject_room(msg.subject))
            trace_id = str(payload.get("trace_id") or "")
            counts = await self._collect_counts(room_id)
            servers = await self._collect_monitor_servers(room_id)
            leader = await self._resolve_monitor_leader(room_id, servers)
            self._normalize_servers_with_leader(servers, leader)
            servers.sort(key=lambda card: str(card.get("server_id") or "").lower())

            await msg.respond(
                encode_json(
                    {
                        "schema": MONITOR_STATUS_SCHEMA,
                        "status": "success",
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "counts": counts,
                        "total_votes": sum(counts.values()),
                        "leader": leader,
                        "servers": servers,
                    }
                )
            )
        except Exception as exc:
            logger.exception("Falha no handler de status do monitor: %s", exc)
            await msg.respond(
                encode_json(
                    {
                        "schema": MONITOR_STATUS_SCHEMA,
                        "status": "error",
                        "error": "monitor_status_failed",
                    }
                )
            )

    async def _handle_monitor_reset(self, msg: Msg) -> None:
        try:
            payload = parse_json(msg.data) if msg.data else {}
            room_id = str(payload.get("room_id") or self._subject_room(msg.subject))
            trace_id = str(payload.get("trace_id") or "")

            admin_result = await self._request_admin_reset(room_id, trace_id)
            monitor_control = admin_result.get("monitor_control")
            if not isinstance(monitor_control, dict):
                monitor_control = {"requested": 0, "success_count": 0, "results": []}

            status = "success" if admin_result.get("status") == "success" else "error"
            await msg.respond(
                encode_json(
                    {
                        "schema": MONITOR_RESET_SCHEMA,
                        "status": status,
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "nats_admin": admin_result,
                        "monitor_reset": monitor_control,
                    }
                )
            )
        except Exception as exc:
            logger.exception("Falha no handler de reset do monitor: %s", exc)
            await msg.respond(
                encode_json(
                    {
                        "schema": MONITOR_RESET_SCHEMA,
                        "status": "error",
                        "error": "monitor_reset_failed",
                    }
                )
            )

    async def _handle_monitor_server_action(self, msg: Msg) -> None:
        try:
            payload = parse_json(msg.data) if msg.data else {}
            room_id = str(payload.get("room_id") or self._subject_room(msg.subject))
            trace_id = str(payload.get("trace_id") or "")
            service_name = str(payload.get("service_name") or "").strip()
            server_id = str(payload.get("server_id") or "").strip().lower()
            action = str(payload.get("action") or "").strip().lower()

            if action not in {"stop", "recover", "restart"}:
                await msg.respond(
                    encode_json(
                        {
                            "schema": MONITOR_SERVER_SCHEMA,
                            "status": "error",
                            "room_id": room_id,
                            "trace_id": trace_id,
                            "error": "invalid_action",
                        }
                    )
                )
                return

            admin_result = await self._request_admin_server_action(
                room_id=room_id,
                trace_id=trace_id,
                action=action,
                service_name=service_name,
                server_id=server_id,
            )
            target_ids = admin_result.get("target_server_ids") or []
            selected_server_id = str(target_ids[0] if target_ids else server_id).lower()

            servers = await self._collect_monitor_servers(room_id)
            server = self._find_server(servers, selected_server_id, service_name)

            status = "success" if admin_result.get("status") == "success" else "error"
            await msg.respond(
                encode_json(
                    {
                        "schema": MONITOR_SERVER_SCHEMA,
                        "status": status,
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "action": action,
                        "service_name": server.get("service_name") if server else service_name,
                        "server_id": selected_server_id or None,
                        "result": admin_result.get("result") or admin_result,
                        "server": server,
                        "error": admin_result.get("error"),
                    }
                )
            )
        except Exception as exc:
            logger.exception("Falha no handler de ação em servidor monitor: %s", exc)
            await msg.respond(
                encode_json(
                    {
                        "schema": MONITOR_SERVER_SCHEMA,
                        "status": "error",
                        "error": "monitor_server_action_failed",
                    }
                )
            )

    async def _wait_for_vote_result(self, room_id: str, vote_id: str) -> Dict[str, Any]:
        # Faz consultas curtas no KV enquanto aguarda o resultado.
        deadline = time.monotonic() + self.processing_wait_seconds
        while time.monotonic() < deadline:
            state = await self._get_vote_state(room_id, vote_id)
            if state and state.get("status") == "notified":
                stored = state.get("result")
                if isinstance(stored, dict):
                    return stored
            await asyncio.sleep(self.poll_interval_seconds)

        return {
            "schema": RESULT_SCHEMA,
            "status": "pending",
            "vote_id": vote_id,
            "room_id": room_id,
            "trace_id": "",
        }

    async def _get_vote_state(self, room_id: str, vote_id: str) -> Optional[Dict[str, Any]]:
        assert self.kv_state is not None
        try:
            entry = await self.kv_state.get(vote_state_key(room_id, vote_id))
        except KeyNotFoundError:
            return None
        return parse_json(entry.value)

    async def _collect_counts(self, room_id: str) -> Dict[str, int]:
        assert self.kv_count is not None
        prefix = f"{room_id}.candidate."
        counts: Dict[str, int] = {}
        try:
            keys = await self.kv_count.keys()
        except NoKeysError:
            return counts

        for key in keys:
            if not key.startswith(prefix):
                continue
            candidate_id = key.replace(prefix, "", 1)
            try:
                entry = await self.kv_count.get(vote_count_key(room_id, candidate_id))
            except KeyNotFoundError:
                continue
            value = parse_json(entry.value)
            counts[candidate_id] = int(value.get("count", 0))
        return counts

    async def _request_admin_reset(self, room_id: str, trace_id: str) -> Dict[str, Any]:
        assert self.client.nc is not None
        try:
            response = await self.client.nc.request(
                f"svc.vote.admin.reset.{room_id}",
                encode_json(
                    {
                        "schema": "vote.admin.reset.v1",
                        "room_id": room_id,
                        "trace_id": trace_id,
                    }
                ),
                timeout=4.0,
            )
            return parse_json(response.data)
        except Exception as exc:
            return {
                "schema": "vote.admin.reset.v1",
                "status": "error",
                "error": f"admin_reset_failed:{type(exc).__name__}",
            }

    async def _request_admin_server_action(
        self,
        room_id: str,
        trace_id: str,
        action: str,
        service_name: str,
        server_id: str,
    ) -> Dict[str, Any]:
        assert self.client.nc is not None
        try:
            response = await self.client.nc.request(
                f"svc.vote.admin.server.{room_id}",
                encode_json(
                    {
                        "schema": "vote.admin.server.v1",
                        "room_id": room_id,
                        "trace_id": trace_id,
                        "action": action,
                        "service_name": service_name,
                        "server_id": server_id,
                    }
                ),
                timeout=4.0,
            )
            return parse_json(response.data)
        except Exception as exc:
            return {
                "schema": "vote.admin.server.v1",
                "status": "error",
                "error": f"admin_server_failed:{type(exc).__name__}",
            }

    async def _collect_monitor_servers(self, room_id: str) -> List[Dict[str, Any]]:
        assert self.kv_monitor_state is not None
        servers_by_id: Dict[str, Dict[str, Any]] = {}
        prefix = f"{room_id}.server."

        try:
            keys = await self.kv_monitor_state.keys()
        except NoKeysError:
            keys = []

        for key in keys:
            if not key.startswith(prefix):
                continue
            try:
                entry = await self.kv_monitor_state.get(key)
            except KeyNotFoundError:
                continue
            payload = parse_json(entry.value)
            server_id = str(payload.get("server_id") or key.replace(prefix, "", 1)).strip().lower()
            if not server_id:
                continue
            servers_by_id[server_id] = {
                "service_name": str(payload.get("service_name") or f"tally-{server_id}-{room_id}"),
                "server_id": server_id,
                "address": str(payload.get("address") or server_id),
                "port": int(payload.get("port") or self._default_port(server_id)),
                "http_port": int(payload.get("http_port") or self._default_http_port(server_id)),
                "role": str(payload.get("role") or "backup"),
                "current_leader": str(payload.get("current_leader") or "-"),
                "simulated_failure": bool(payload.get("simulated_failure", False)),
                "healthy": bool(payload.get("healthy", True)),
                "last_seen": float(payload.get("last_seen") or 0),
            }

        for server_id in self.expected_server_ids:
            if server_id in servers_by_id:
                continue
            servers_by_id[server_id] = {
                "service_name": f"tally-{server_id}-{room_id}",
                "server_id": server_id,
                "address": server_id,
                "port": self._default_port(server_id),
                "http_port": self._default_http_port(server_id),
                "role": "backup",
                "current_leader": "-",
                "simulated_failure": False,
                "healthy": False,
                "last_seen": 0.0,
                "error": "sem heartbeat",
            }

        return list(servers_by_id.values())

    async def _resolve_monitor_leader(
        self,
        room_id: str,
        servers: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        assert self.kv_monitor_leader is not None
        now = time.time()

        # Prioriza o líder com lease válida e saudável.
        try:
            entry = await self.kv_monitor_leader.get(monitor_leader_key(room_id))
            payload = parse_json(entry.value)
            leader_id = str(payload.get("server_id") or "").strip().lower()
            lease_until = float(payload.get("lease_until") or 0)
            if leader_id and lease_until > now:
                matched = self._find_server(servers, leader_id, "")
                if matched and matched.get("healthy") and not matched.get("simulated_failure"):
                    return self._leader_from_server(matched)
        except KeyNotFoundError:
            pass

        # Se houver servidor ativo, sempre retorna um líder.
        active = [
            server
            for server in servers
            if server.get("healthy") and not server.get("simulated_failure")
        ]
        if not active:
            return None

        active.sort(key=lambda item: str(item.get("server_id") or "").lower())
        return self._leader_from_server(active[0])

    @staticmethod
    def _normalize_servers_with_leader(
        servers: List[Dict[str, Any]],
        leader: Optional[Dict[str, Any]],
    ) -> None:
        leader_id = str((leader or {}).get("server_id") or "").strip().lower()
        for server in servers:
            is_failed = bool(server.get("simulated_failure"))
            if is_failed:
                server["role"] = "failed"
                server["current_leader"] = "-"
                continue

            server_id = str(server.get("server_id") or "").strip().lower()
            if leader_id:
                server["current_leader"] = leader_id
                server["role"] = "leader" if server_id == leader_id else "backup"
            else:
                server["role"] = "backup"
                server["current_leader"] = "-"

    @staticmethod
    def _find_server(
        servers: List[Dict[str, Any]],
        server_id: str,
        service_name: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_id = server_id.strip().lower()
        normalized_service = service_name.strip().lower()
        for server in servers:
            sid = str(server.get("server_id") or "").strip().lower()
            svc = str(server.get("service_name") or "").strip().lower()
            if normalized_id and sid == normalized_id:
                return server
            if normalized_service and svc == normalized_service:
                return server
        return None

    @staticmethod
    def _leader_from_server(server: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "service_name": server.get("service_name"),
            "address": server.get("address"),
            "port": server.get("port"),
            "http_port": server.get("http_port"),
            "server_id": server.get("server_id"),
            "last_seen": server.get("last_seen"),
        }

    @staticmethod
    def _subject_room(subject: str) -> str:
        parts = subject.split(".")
        return parts[-1] if parts else "default"

    @staticmethod
    def _parse_expected_server_ids(raw_value: str) -> List[str]:
        values = [item.strip().lower() for item in raw_value.split(",") if item.strip()]
        return values or ["s1", "s2", "s3"]

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
    service = VoteAPIService()
    try:
        await service.start()
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
