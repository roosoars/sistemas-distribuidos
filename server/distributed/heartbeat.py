"""
Gerenciador de Heartbeat para deteccao de falhas.

Responsabilidade unica: enviar e monitorar heartbeats entre servidores.
"""

import time
import threading
import logging
from typing import Dict, Tuple, Callable, Optional

from core.protocol import Event, DistributedMessageName

logger = logging.getLogger("HEARTBEAT")


class HeartbeatManager:
    """
    Gerencia heartbeats para deteccao de falhas do lider.

    O lider envia heartbeats periodicos para backups.
    Backups monitoram e disparam eleicao se lider falhar.
    """

    def __init__(
        self,
        server_id: str,
        peers: Dict[str, Tuple[str, int]],
        send_message: Callable,
        on_leader_failure: Callable,
        interval: float = 1.0,
        timeout: float = 3.0
    ):
        self.server_id = server_id
        self.peers = peers
        self.send_message = send_message
        self.on_leader_failure = on_leader_failure

        self.interval = interval
        self.timeout = timeout

        self.last_received = time.time()
        self.last_sent = 0.0
        self.running = False

        self._is_leader = False
        self._current_leader: Optional[str] = None
        self._simulated_failure = False

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @is_leader.setter
    def is_leader(self, value: bool):
        self._is_leader = value

    @property
    def current_leader(self) -> Optional[str]:
        return self._current_leader

    @current_leader.setter
    def current_leader(self, value: Optional[str]):
        self._current_leader = value
        if value:
            self.last_received = time.time()

    @property
    def simulated_failure(self) -> bool:
        return self._simulated_failure

    @simulated_failure.setter
    def simulated_failure(self, value: bool):
        self._simulated_failure = value

    def start(self):
        """Inicia threads de heartbeat."""
        self.running = True
        threading.Thread(target=self._send_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    def stop(self):
        """Para as threads de heartbeat."""
        self.running = False

    def handle_heartbeat(self, from_id: str):
        """Processa heartbeat recebido do lider."""
        if from_id == self._current_leader:
            self.last_received = time.time()
            logger.debug(f"Heartbeat received from leader {from_id}")

    def _send_loop(self):
        """Loop de envio de heartbeat (executado pelo lider)."""
        while self.running:
            if self._is_leader and not self._simulated_failure:
                self._send_heartbeat()
            time.sleep(self.interval)

    def _send_heartbeat(self):
        """Envia heartbeat para todos os peers."""
        for peer_id, (host, port) in self.peers.items():
            msg = Event(DistributedMessageName.HEARTBEAT, f"TallyServer-{self.server_id}", {
                "from_id": self.server_id,
                "timestamp": time.time()
            })
            threading.Thread(
                target=self._send_safe,
                args=(host, port, msg),
                daemon=True
            ).start()
        self.last_sent = time.time()

    def _send_safe(self, host: str, port: int, msg):
        """Envia mensagem com tratamento de erro."""
        try:
            self.send_message(host, port, msg)
        except Exception as e:
            logger.warning(f"Failed to send heartbeat to {host}:{port}: {e}")

    def _monitor_loop(self):
        """Monitora lider via heartbeat, dispara eleicao se falhar."""
        while self.running:
            time.sleep(1.0)

            if self._is_leader:
                continue

            if self._current_leader is None:
                continue

            elapsed = time.time() - self.last_received

            if elapsed > self.timeout:
                logger.warning(
                    f"LEADER FAILURE DETECTED! (last heartbeat {elapsed:.1f}s ago)"
                )
                self._current_leader = None
                self.on_leader_failure()
                self.last_received = time.time()
