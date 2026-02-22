"""
Bootstrap JetStream streams/consumers/KV buckets for the voting platform.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Callable, TypeVar

from nats.js import api
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js.errors import BadRequestError

from core.nats_client import NATSClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NATS_BOOTSTRAP] %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("NATS_BOOTSTRAP")


T = TypeVar("T")


async def ensure(name: str, creator: Callable[[], asyncio.Future]) -> None:
    for attempt in range(1, 7):
        try:
            await creator()
            logger.info("Created %s", name)
            return
        except BadRequestError as exc:
            # JetStream returns "stream name already in use" / "consumer already exists".
            logger.info("%s already exists (%s)", name, exc.description)
            return
        except NATSTimeoutError:
            if attempt == 6:
                raise
            logger.warning("Timeout creating %s (attempt %d/6), retrying...", name, attempt)
            await asyncio.sleep(1.0)


async def main() -> None:
    replicas = int(os.getenv("NATS_STREAM_REPLICAS", "3"))
    client = NATSClient()
    await client.connect("nats-bootstrap")
    assert client.js is not None
    assert client.jsm is not None

    try:
        await ensure(
            "stream VOTE_CMD",
            lambda: client.jsm.add_stream(
                config=api.StreamConfig(
                    name="VOTE_CMD",
                    subjects=["vote.cmd.*"],
                    retention=api.RetentionPolicy.LIMITS,
                    storage=api.StorageType.FILE,
                    num_replicas=replicas,
                    max_age=7 * 24 * 3600,
                    duplicate_window=2 * 3600,
                )
            ),
        )
        await ensure(
            "stream VOTE_EVT",
            lambda: client.jsm.add_stream(
                config=api.StreamConfig(
                    name="VOTE_EVT",
                    subjects=["vote.event.>"],
                    retention=api.RetentionPolicy.LIMITS,
                    storage=api.StorageType.FILE,
                    num_replicas=replicas,
                    max_age=30 * 24 * 3600,
                )
            ),
        )
        await ensure(
            "consumer VOTE_CMD_PROC",
            lambda: client.jsm.add_consumer(
                stream="VOTE_CMD",
                config=api.ConsumerConfig(
                    durable_name="VOTE_CMD_PROC",
                    ack_policy=api.AckPolicy.EXPLICIT,
                    ack_wait=30.0,
                    max_deliver=20,
                    filter_subject="vote.cmd.*",
                    max_ack_pending=500,
                ),
            ),
        )
        await ensure(
            "kv KV_VOTE_STATE",
            lambda: client.js.create_key_value(
                config=api.KeyValueConfig(
                    bucket="KV_VOTE_STATE",
                    description="Vote idempotency and state machine",
                    history=10,
                    ttl=30 * 24 * 3600,
                    storage=api.StorageType.FILE,
                    replicas=replicas,
                )
            ),
        )
        await ensure(
            "kv KV_VOTE_COUNT",
            lambda: client.js.create_key_value(
                config=api.KeyValueConfig(
                    bucket="KV_VOTE_COUNT",
                    description="Vote counters per room/candidate",
                    history=64,
                    storage=api.StorageType.FILE,
                    replicas=replicas,
                )
            ),
        )
        await ensure(
            "kv KV_CONTROL",
            lambda: client.js.create_key_value(
                config=api.KeyValueConfig(
                    bucket="KV_CONTROL",
                    description="Locks and fencing tokens",
                    history=5,
                    ttl=15.0,
                    storage=api.StorageType.FILE,
                    replicas=replicas,
                )
            ),
        )
        await ensure(
            "kv KV_MONITOR_STATE",
            lambda: client.js.create_key_value(
                config=api.KeyValueConfig(
                    bucket="KV_MONITOR_STATE",
                    description="Monitor node heartbeats and status",
                    history=10,
                    ttl=20.0,
                    storage=api.StorageType.FILE,
                    replicas=replicas,
                )
            ),
        )
        await ensure(
            "kv KV_MONITOR_LEADER",
            lambda: client.js.create_key_value(
                config=api.KeyValueConfig(
                    bucket="KV_MONITOR_LEADER",
                    description="Monitor leader lease by room",
                    history=20,
                    ttl=60.0,
                    storage=api.StorageType.FILE,
                    replicas=replicas,
                )
            ),
        )
        await ensure(
            "kv KV_MONITOR_CONTROL",
            lambda: client.js.create_key_value(
                config=api.KeyValueConfig(
                    bucket="KV_MONITOR_CONTROL",
                    description="Monitor stop/recover/restart commands",
                    history=20,
                    ttl=24 * 3600,
                    storage=api.StorageType.FILE,
                    replicas=replicas,
                )
            ),
        )
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
