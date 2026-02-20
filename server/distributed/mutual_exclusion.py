"""
Gerenciador de exclusão mútua distribuída usando o algoritmo de Ricart-Agrawala.

Este módulo implementa um protocolo distribuído que garante acesso exclusivo
à seção crítica sem depender de um coordenador central. Usa relógios de Lamport
para ordenar requisições de forma justa entre processos concorrentes.
"""
import threading
from enum import Enum
from typing import Dict, List, Set, Optional, Any

from clocks.lamport_clock import LamportClock


class ProcessState(Enum):
    """Estados possíveis de um processo no protocolo de Ricart-Agrawala."""
    RELEASED = "RELEASED"  # Fora da SC, não quer entrar
    WANTED = "WANTED"      # Pedindo acesso, esperando respostas
    HELD = "HELD"          # Dentro da seção crítica


class MutualExclusionManager:
    """
    Implementa exclusão mútua distribuída pelo algoritmo de Ricart-Agrawala.

    Garante que apenas um processo acessa a seção crítica por vez, sem servidor
    central. Usa timestamps de Lamport para resolver conflitos de forma justa.
    """

    __slots__ = (
        '_process_id', '_peers', '_clock', '_state',
        '_request_timestamp', '_replies_received', '_deferred_replies',
        '_lock', '_external_clock'
    )

    def __init__(
        self,
        process_id: str,
        all_processes: List[str],
        clock: Optional[LamportClock] = None
    ) -> None:
        if not process_id:
            raise ValueError("Process ID cannot be empty")

        self._process_id: str = process_id
        self._peers: Set[str] = {p for p in all_processes if p != process_id}
        self._external_clock = clock is not None
        self._clock: LamportClock = clock if clock else LamportClock()
        self._state: ProcessState = ProcessState.RELEASED
        self._request_timestamp: Optional[int] = None
        self._replies_received: Set[str] = set()
        self._deferred_replies: Set[str] = set()
        self._lock: threading.RLock = threading.RLock()

    @property
    def process_id(self) -> str:
        return self._process_id

    @property
    def peers(self) -> Set[str]:
        return self._peers.copy()

    @property
    def clock(self) -> LamportClock:
        return self._clock

    @property
    def state(self) -> ProcessState:
        with self._lock:
            return self._state

    @property
    def request_timestamp(self) -> Optional[int]:
        with self._lock:
            return self._request_timestamp

    @property
    def in_critical_section(self) -> bool:
        with self._lock:
            return self._state == ProcessState.HELD

    @property
    def is_requesting(self) -> bool:
        with self._lock:
            return self._state == ProcessState.WANTED

    @property
    def can_enter_cs(self) -> bool:
        """Pode entrar na SC quando recebeu resposta de todos os peers."""
        with self._lock:
            if self._state != ProcessState.WANTED:
                return False
            return self._replies_received >= self._peers

    def request_cs(self) -> List[Dict[str, Any]]:
        """
        Solicita entrada na seção crítica.

        Envia REQUEST com timestamp para todos os peers e aguarda REPLY
        de cada um antes de poder entrar.

        Returns:
            Lista de mensagens REQUEST para broadcast.
        """
        with self._lock:
            if self._state != ProcessState.RELEASED:
                raise RuntimeError(
                    f"Cannot request CS while in state {self._state.value}"
                )

            self._state = ProcessState.WANTED
            self._request_timestamp = self._clock.send()
            self._replies_received.clear()

            messages = []
            for peer in self._peers:
                messages.append({
                    "type": "REQUEST",
                    "from": self._process_id,
                    "to": peer,
                    "timestamp": self._request_timestamp
                })

            return messages

    def handle_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Processa REQUEST de outro processo.

        Decide se responde imediatamente ou adia a resposta conforme o
        algoritmo de Ricart-Agrawala:
        - Se não está interessado: responde na hora
        - Se está na SC: adia resposta
        - Se também quer a SC: compara timestamps (menor vence)

        Returns:
            Mensagem REPLY se deve responder agora, None se adia.
        """
        sender = request["from"]
        sender_ts = request["timestamp"]

        with self._lock:
            self._clock.receive(sender_ts)

            should_reply = False

            if self._state == ProcessState.RELEASED:
                should_reply = True

            elif self._state == ProcessState.HELD:
                should_reply = False

            elif self._state == ProcessState.WANTED:
                # Compara prioridades: menor timestamp vence
                # Empate: desempata pelo ID do processo
                if sender_ts < self._request_timestamp:
                    should_reply = True
                elif sender_ts == self._request_timestamp:
                    if sender < self._process_id:
                        should_reply = True
                    else:
                        should_reply = False
                else:
                    should_reply = False

            if should_reply:
                return {
                    "type": "REPLY",
                    "from": self._process_id,
                    "to": sender
                }
            else:
                # Adia resposta até sair da SC
                self._deferred_replies.add(sender)
                return None

    def handle_reply(self, reply: Dict[str, Any]) -> None:
        """Registra REPLY recebido de um peer."""
        sender = reply["from"]

        with self._lock:
            if self._state != ProcessState.WANTED:
                return

            self._replies_received.add(sender)

    def enter_cs(self) -> None:
        """
        Entra na seção crítica.

        Só funciona se já recebeu REPLY de todos os peers.
        """
        with self._lock:
            if self._state != ProcessState.WANTED:
                raise RuntimeError(
                    f"Cannot enter CS from state {self._state.value}"
                )

            if not self.can_enter_cs:
                missing = self._peers - self._replies_received
                raise RuntimeError(
                    f"Cannot enter CS: missing replies from {missing}"
                )

            self._state = ProcessState.HELD

    def release_cs(self) -> List[Dict[str, Any]]:
        """
        Sai da seção crítica.

        Envia REPLY para todas as requisições que foram adiadas.

        Returns:
            Lista de mensagens REPLY para os processos que esperavam.
        """
        with self._lock:
            if self._state != ProcessState.HELD:
                raise RuntimeError(
                    f"Cannot release CS from state {self._state.value}"
                )

            self._state = ProcessState.RELEASED
            self._request_timestamp = None
            self._replies_received.clear()

            replies = []
            for peer in self._deferred_replies:
                replies.append({
                    "type": "REPLY",
                    "from": self._process_id,
                    "to": peer
                })

            self._deferred_replies.clear()

            return replies

    def __enter__(self) -> 'MutualExclusionManager':
        if self._state == ProcessState.RELEASED:
            self.request_cs()

        if self.can_enter_cs:
            self.enter_cs()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._state == ProcessState.HELD:
            self.release_cs()

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "process_id": self._process_id,
                "peers": list(self._peers),
                "state": self._state.value,
                "request_timestamp": self._request_timestamp,
                "replies_received": list(self._replies_received),
                "deferred_replies": list(self._deferred_replies),
                "clock": self._clock.to_dict()
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MutualExclusionManager':
        manager = cls(
            data["process_id"],
            [data["process_id"]] + data.get("peers", [])
        )
        manager._state = ProcessState(data["state"])
        manager._request_timestamp = data.get("request_timestamp")
        manager._replies_received = set(data.get("replies_received", []))
        manager._deferred_replies = set(data.get("deferred_replies", []))

        if "clock" in data:
            manager._clock = LamportClock.from_dict(data["clock"])

        return manager

    def __str__(self) -> str:
        return (
            f"MutualExclusionManager(process_id={self._process_id}, "
            f"state={self._state.value})"
        )

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"MutualExclusionManager("
                f"process_id={self._process_id!r}, "
                f"state={self._state.value}, "
                f"request_ts={self._request_timestamp}, "
                f"replies={len(self._replies_received)}/{len(self._peers)}, "
                f"deferred={len(self._deferred_replies)})"
            )
