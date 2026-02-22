"""
Shared NATS/JetStream helpers for the NATS-only voting services.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Optional

import nats
from nats.aio.client import Client as NATS
from nats.errors import DrainTimeoutError
from nats.js.client import JetStreamContext, JetStreamManager


logger = logging.getLogger("NATS_CLIENT")


@dataclass(frozen=True)
class NATSSettings:
    servers: str
    user: str
    password: str
    connect_timeout: float
    reconnect_time_wait: float
    max_reconnect_attempts: int

    @classmethod
    def from_env(cls) -> "NATSSettings":
        return cls(
            servers=os.getenv(
                "NATS_SERVERS",
                "nats://app_backend:app_backend_dev@nats-1:4222,"
                "nats://app_backend:app_backend_dev@nats-2:4222,"
                "nats://app_backend:app_backend_dev@nats-3:4222",
            ),
            user=os.getenv("NATS_USER", "app_backend"),
            password=os.getenv("NATS_PASSWORD", "app_backend_dev"),
            connect_timeout=float(os.getenv("NATS_CONNECT_TIMEOUT", "3.0")),
            reconnect_time_wait=float(os.getenv("NATS_RECONNECT_WAIT", "1.0")),
            max_reconnect_attempts=int(os.getenv("NATS_MAX_RECONNECTS", "-1")),
        )


class NATSClient:
    """
    Thin wrapper to keep connection setup consistent across services.
    """

    def __init__(self, settings: Optional[NATSSettings] = None):
        self.settings = settings or NATSSettings.from_env()
        self.nc: Optional[NATS] = None
        self.js: Optional[JetStreamContext] = None
        self.jsm: Optional[JetStreamManager] = None

    async def connect(self, name: str) -> None:
        if self.nc and self.nc.is_connected:
            return

        servers = [s.strip() for s in self.settings.servers.split(",") if s.strip()]
        self.nc = await nats.connect(
            servers=servers,
            name=name,
            user=self.settings.user,
            password=self.settings.password,
            connect_timeout=self.settings.connect_timeout,
            reconnect_time_wait=self.settings.reconnect_time_wait,
            max_reconnect_attempts=self.settings.max_reconnect_attempts,
        )
        self.js = self.nc.jetstream()
        self.jsm = self.nc.jsm()
        logger.info("Connected to NATS as %s", name)

    async def close(self) -> None:
        if self.nc:
            try:
                await self.nc.drain()
            except DrainTimeoutError:
                logger.warning("Timed out while draining NATS connection; forcing close")
            await self.nc.close()
            logger.info("NATS connection closed")


async def wait_forever(stop_event: asyncio.Event) -> None:
    """
    Keep a service alive until stop_event is set.
    """
    while not stop_event.is_set():
        await asyncio.sleep(0.5)
