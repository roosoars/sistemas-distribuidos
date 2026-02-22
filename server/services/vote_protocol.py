"""
Shared protocol helpers for NATS voting services.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Tuple


SUBMIT_SCHEMA = "vote.submit.v1"
RESULT_SCHEMA = "vote.result.v1"
MONITOR_STATUS_SCHEMA = "vote.monitor.status.v1"
MONITOR_RESET_SCHEMA = "vote.monitor.reset.v1"
MONITOR_SERVER_SCHEMA = "vote.monitor.server.v1"


@dataclass(frozen=True)
class VoteCommand:
    vote_id: str
    room_id: str
    candidate_id: str
    voter_id: str
    client_ts: str
    trace_id: str

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "VoteCommand":
        return cls(
            vote_id=str(payload["vote_id"]),
            room_id=str(payload.get("room_id") or "default"),
            candidate_id=str(payload["candidate_id"]),
            voter_id=str(payload.get("voter_id") or "anonymous"),
            client_ts=str(payload.get("client_ts") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            trace_id=str(payload.get("trace_id") or str(uuid.uuid4())),
        )


def parse_json(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode("utf-8"))


def encode_json(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def validate_submit_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    if payload.get("schema") != SUBMIT_SCHEMA:
        return False, f"schema must be {SUBMIT_SCHEMA}"
    required = ("vote_id", "candidate_id", "room_id", "voter_id")
    for key in required:
        if not payload.get(key):
            return False, f"missing required field: {key}"
    return True, ""


def vote_state_key(room_id: str, vote_id: str) -> str:
    return f"{room_id}.vote.{vote_id}"


def vote_count_key(room_id: str, candidate_id: str) -> str:
    return f"{room_id}.candidate.{candidate_id}"


def lock_key(room_id: str, operation: str) -> str:
    return f"{room_id}.lock.{operation}"


def result_subject(room_id: str, vote_id: str) -> str:
    return f"vote.result.{room_id}.{vote_id}"


def monitor_state_key(room_id: str, server_id: str) -> str:
    return f"{room_id}.server.{server_id}"


def monitor_control_key(room_id: str, server_id: str) -> str:
    return f"{room_id}.control.{server_id}"


def monitor_control_all_key(room_id: str) -> str:
    return f"{room_id}.control.all"


def monitor_leader_key(room_id: str) -> str:
    return f"{room_id}.leader"
