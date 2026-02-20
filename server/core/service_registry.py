"""
Registro de servicos no NameServer.

Responsabilidade unica: registrar e desregistrar servicos no nameserver.
"""

import time
import threading
import logging
from typing import Optional, Callable

from core.protocol import Command

logger = logging.getLogger("SERVICE_REGISTRY")


class ServiceRegistry:
    """
    Gerencia registro de servicos no NameServer.

    Registra periodicamente o servidor no nameserver para descoberta de servicos.
    """

    def __init__(
        self,
        server_id: str,
        host: str,
        port: int,
        http_port: int,
        room_id: str,
        nameserver_host: Optional[str],
        nameserver_port: Optional[int],
        send_message: Callable,
        component_id: str
    ):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.http_port = http_port
        self.room_id = room_id
        self.nameserver_host = nameserver_host
        self.nameserver_port = nameserver_port
        self.send_message = send_message
        self.component_id = component_id

        self.running = False
        self._is_leader = False

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @is_leader.setter
    def is_leader(self, value: bool):
        self._is_leader = value

    @property
    def enabled(self) -> bool:
        return self.nameserver_host is not None and self.nameserver_port is not None

    def start(self):
        """Inicia registro periodico."""
        if not self.enabled:
            return

        self.running = True
        threading.Thread(target=self._registration_loop, daemon=True).start()

    def stop(self):
        """Para registro periodico."""
        self.running = False

    def register(self, role: str):
        """Registra servico no nameserver."""
        if not self.enabled:
            return

        service_name = f"tally-{self.server_id}-{self.room_id}"
        msg = Command("RegisterService", self.component_id, {
            "serviceName": service_name,
            "address": self.host,
            "port": self.port,
            "meta": {
                "role": role,
                "roomId": self.room_id,
                "serverId": self.server_id,
                "http_port": self.http_port
            }
        })

        try:
            self.send_message(self.nameserver_host, self.nameserver_port, msg)
            logger.info(f"Registered as {role} with nameserver (service={service_name})")
        except Exception as e:
            logger.warning(f"Failed to register with nameserver: {e}")

    def unregister(self):
        """Remove registro do servico no nameserver."""
        if not self.enabled:
            return

        service_name = f"tally-{self.server_id}-{self.room_id}"
        msg = Command("UnregisterService", self.component_id, {
            "serviceName": service_name
        })

        try:
            self.send_message(self.nameserver_host, self.nameserver_port, msg)
            logger.info(f"Unregistered {service_name} from nameserver")
        except Exception as e:
            logger.warning(f"Failed to unregister from nameserver: {e}")

    def _registration_loop(self):
        """Registra periodicamente no nameserver."""
        while self.running:
            time.sleep(5)
            if self.enabled:
                role = "primary" if self._is_leader else "backup"
                self.register(role)
