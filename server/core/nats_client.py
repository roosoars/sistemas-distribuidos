"""
Utilitários compartilhados de NATS/JetStream para os serviços de votação.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

import nats
from nats.aio.client import Client as NATS
from nats.errors import DrainTimeoutError
from nats.js.client import JetStreamContext, JetStreamManager
from nats.js.errors import BadRequestError, KeyWrongLastSequenceError


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
    Camada fina para padronizar conexão e reconexão entre os serviços.
    """

    def __init__(self, settings: Optional[NATSSettings] = None):
        self.settings = settings or NATSSettings.from_env()
        self.nc: Optional[NATS] = None
        self.js: Optional[JetStreamContext] = None
        self.jsm: Optional[JetStreamManager] = None
        self.client_name = "nats-client"
        self._last_error_log_at = 0.0
        self._last_error_signature = ""
        self._error_log_throttle_seconds = float(os.getenv("NATS_ERROR_LOG_THROTTLE_SECONDS", "5.0"))

    async def connect(self, name: str) -> None:
        if self.nc and self.nc.is_connected:
            return

        self.client_name = name
        servers = [s.strip() for s in self.settings.servers.split(",") if s.strip()]

        async def on_error(exc: Exception) -> None:
            self._log_nats_error(exc)

        async def on_disconnected() -> None:
            logger.warning("%s desconectado do NATS", self.client_name)

        async def on_reconnected() -> None:
            connected_url = "desconhecido"
            if self.nc and self.nc.connected_url:
                connected_url = str(self.nc.connected_url)
            logger.info("%s reconectado ao NATS (%s)", self.client_name, connected_url)

        async def on_closed() -> None:
            logger.warning("%s conexao NATS encerrada", self.client_name)

        self.nc = await nats.connect(
            servers=servers,
            name=name,
            user=self.settings.user,
            password=self.settings.password,
            connect_timeout=self.settings.connect_timeout,
            reconnect_time_wait=self.settings.reconnect_time_wait,
            max_reconnect_attempts=self.settings.max_reconnect_attempts,
            error_cb=on_error,
            disconnected_cb=on_disconnected,
            reconnected_cb=on_reconnected,
            closed_cb=on_closed,
        )
        self.js = self.nc.jetstream()
        self.jsm = self.nc.jsm()
        logger.info("Conectado ao NATS como %s", name)

    async def close(self) -> None:
        if self.nc:
            try:
                await self.nc.drain()
            except DrainTimeoutError:
                logger.warning("Tempo esgotado ao drenar conexão NATS; forçando fechamento")
            await self.nc.close()
            logger.info("Conexão NATS encerrada")

    async def get_key_value(
        self,
        bucket: str,
        *,
        retries: int = 30,
        delay_seconds: float = 0.5,
    ) -> Any:
        if retries < 1:
            raise ValueError("retries deve ser >= 1")
        assert self.js is not None

        last_error: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                return await self.js.key_value(bucket)
            except Exception as exc:
                last_error = exc
                if attempt == retries:
                    break
                if attempt in {1, retries // 2, retries - 1}:
                    logger.warning(
                        "KV %s indisponivel (tentativa %d/%d): %s",
                        bucket,
                        attempt,
                        retries,
                        type(exc).__name__,
                    )
                await asyncio.sleep(delay_seconds)

        assert last_error is not None
        raise last_error

    def _log_nats_error(self, exc: Exception) -> None:
        if exc is None:
            return
        signature = f"{type(exc).__name__}:{exc}"
        now = time.monotonic()
        is_transient = self._is_transient_error(exc)
        # Evita spam de log em falhas transitórias repetidas.
        if (
            is_transient
            and signature == self._last_error_signature
            and (now - self._last_error_log_at) < self._error_log_throttle_seconds
        ):
            return

        self._last_error_signature = signature
        self._last_error_log_at = now

        if is_transient:
            logger.warning(
                "%s erro transitorio de conexao NATS: %s",
                self.client_name,
                signature,
            )
            return

        logger.error("%s erro NATS: %s", self.client_name, signature)

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        text = f"{type(exc).__name__}:{exc}".lower()
        transient_tokens = (
            "unexpected eof",
            "connect call failed",
            "connectionrefused",
            "timeout",
            "timed out",
            "empty response from server when expecting info message",
            "connection reset by peer",
            "broken pipe",
        )
        if any(token in text for token in transient_tokens):
            return True
        return isinstance(exc, (ConnectionRefusedError, TimeoutError, OSError))


def is_kv_revision_conflict(exc: Exception) -> bool:
    if isinstance(exc, KeyWrongLastSequenceError):
        return True
    if isinstance(exc, BadRequestError):
        err_code = getattr(exc, "err_code", None)
        description = str(getattr(exc, "description", "")).lower()
        return err_code == 10164 or "wrong last sequence" in description
    return False


async def wait_forever(stop_event: asyncio.Event) -> None:
    """
    Mantém o serviço ativo até o sinal de parada.
    """
    while not stop_event.is_set():
        await asyncio.sleep(0.5)
