"""
Processes vote commands from JetStream with idempotent state transitions.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from typing import Any, Dict, Optional

from nats.aio.msg import Msg
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js.errors import KeyNotFoundError, KeyWrongLastSequenceError, NoKeysError

from core.nats_client import NATSClient
from services.vote_protocol import (
    RESULT_SCHEMA,
    VoteCommand,
    encode_json,
    parse_json,
    result_subject,
    vote_count_key,
    vote_state_key,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [VOTE_PROCESSOR] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("VOTE_PROCESSOR")


class VoteProcessorService:
    def __init__(self) -> None:
        self.client = NATSClient()
        self.running = True
        self.kv_state = None
        self.kv_count = None

    async def start(self) -> None:
        await self.client.connect("vote-processor")
        assert self.client.js is not None

        self.kv_state = await self.client.js.key_value("KV_VOTE_STATE")
        self.kv_count = await self.client.js.key_value("KV_VOTE_COUNT")
        self.sub = await self.client.js.pull_subscribe(
            subject="vote.cmd.*",
            durable="VOTE_CMD_PROC",
            stream="VOTE_CMD",
        )
        logger.info("Vote processor started and bound to VOTE_CMD_PROC")

        try:
            while self.running:
                try:
                    messages = await self.sub.fetch(batch=25, timeout=2)
                except NATSTimeoutError:
                    # Pull consumers time out when idle; this is expected.
                    continue
                if not messages:
                    continue
                for msg in messages:
                    await self._process_message(msg)
        except asyncio.CancelledError:
            logger.info("Processor cancelled")
        finally:
            await self.client.close()

    async def _process_message(self, msg: Msg) -> None:
        try:
            payload = parse_json(msg.data)
            cmd = VoteCommand.from_payload(payload)
            key = vote_state_key(cmd.room_id, cmd.vote_id)

            state = await self._ensure_received_state(cmd, key)
            if state.get("status") == "notified":
                stored = state.get("result")
                if stored:
                    stored["idempotent_replay"] = True
                    await self._publish_result(cmd.room_id, cmd.vote_id, stored)
                await msg.ack()
                return

            counts = await self._increment_and_collect_counts(cmd.room_id, cmd.candidate_id)
            total_votes = sum(counts.values())
            result = {
                "schema": RESULT_SCHEMA,
                "status": "success",
                "vote_id": cmd.vote_id,
                "room_id": cmd.room_id,
                "candidate_id": cmd.candidate_id,
                "total_votes": total_votes,
                "counts": counts,
                "idempotent_replay": False,
                "trace_id": cmd.trace_id,
            }

            await self._update_state(key, "applied", cmd, result=None)
            await self._publish_vote_event(cmd, result)
            await self._update_state(key, "notified", cmd, result=result)
            await self._publish_result(cmd.room_id, cmd.vote_id, result)

            await msg.ack()
        except Exception as exc:
            logger.exception("Failed to process vote command: %s", exc)
            await msg.nak()

    async def _ensure_received_state(self, cmd: VoteCommand, key: str) -> Dict[str, Any]:
        assert self.kv_state is not None
        now = time.time()
        received = {
            "status": "received",
            "created_at": now,
            "updated_at": now,
            "command": {
                "vote_id": cmd.vote_id,
                "room_id": cmd.room_id,
                "candidate_id": cmd.candidate_id,
                "voter_id": cmd.voter_id,
                "client_ts": cmd.client_ts,
                "trace_id": cmd.trace_id,
            },
        }
        try:
            await self.kv_state.create(key, encode_json(received))
            return received
        except KeyWrongLastSequenceError:
            entry = await self.kv_state.get(key)
            return parse_json(entry.value)

    async def _update_state(
        self,
        key: str,
        status: str,
        cmd: VoteCommand,
        result: Optional[Dict[str, Any]],
    ) -> None:
        assert self.kv_state is not None
        for _ in range(25):
            try:
                entry = await self.kv_state.get(key)
            except KeyNotFoundError:
                await self._ensure_received_state(cmd, key)
                continue

            current = parse_json(entry.value)
            current["status"] = status
            current["updated_at"] = time.time()
            if result is not None:
                current["result"] = result
            try:
                await self.kv_state.update(key, encode_json(current), entry.revision)
                return
            except KeyWrongLastSequenceError:
                continue
        raise RuntimeError(f"could not update vote state for key={key}")

    async def _increment_and_collect_counts(
        self,
        room_id: str,
        candidate_id: str,
    ) -> Dict[str, int]:
        await self._increment_candidate(room_id, candidate_id)
        return await self._collect_counts(room_id)

    async def _increment_candidate(self, room_id: str, candidate_id: str) -> None:
        assert self.kv_count is not None
        key = vote_count_key(room_id, candidate_id)
        for _ in range(25):
            try:
                entry = await self.kv_count.get(key)
                current = parse_json(entry.value)
                next_value = int(current.get("count", 0)) + 1
                payload = encode_json({"count": next_value})
                await self.kv_count.update(key, payload, entry.revision)
                return
            except KeyNotFoundError:
                try:
                    await self.kv_count.create(key, encode_json({"count": 1}))
                    return
                except KeyWrongLastSequenceError:
                    continue
            except KeyWrongLastSequenceError:
                continue
        raise RuntimeError(f"failed to update counter for {key}")

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
                entry = await self.kv_count.get(key)
            except KeyNotFoundError:
                continue
            value = parse_json(entry.value)
            counts[candidate_id] = int(value.get("count", 0))
        return counts

    async def _publish_vote_event(self, cmd: VoteCommand, result: Dict[str, Any]) -> None:
        assert self.client.js is not None
        subject = f"vote.event.accepted.{cmd.room_id}"
        payload = {
            "event": "vote.accepted",
            "room_id": cmd.room_id,
            "vote_id": cmd.vote_id,
            "candidate_id": cmd.candidate_id,
            "counts": result["counts"],
            "total_votes": result["total_votes"],
            "trace_id": cmd.trace_id,
            "ts": time.time(),
        }
        await self.client.js.publish(subject, encode_json(payload))

    async def _publish_result(self, room_id: str, vote_id: str, result: Dict[str, Any]) -> None:
        assert self.client.nc is not None
        await self.client.nc.publish(result_subject(room_id, vote_id), encode_json(result))


async def main() -> None:
    service = VoteProcessorService()
    await service.start()


if __name__ == "__main__":
    asyncio.run(main())
