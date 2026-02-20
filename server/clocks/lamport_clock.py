"""
Relógio Lógico de Lamport para Sistemas Distribuídos.

Implementa o algoritmo de ordenação parcial de eventos em sistemas distribuídos
proposto por Leslie Lamport em 1978. Garante que se um evento A acontece antes
de um evento B, então o timestamp de A é menor que o de B.

Referência: Lamport, L. "Time, Clocks, and the Ordering of Events
in a Distributed System", Communications of the ACM, 1978.
"""

import threading
from typing import Dict, Any


class LamportClock:
    """
    Relógio lógico que implementa as três regras de Lamport:
    1. Incrementa em eventos internos
    2. Incrementa antes de enviar mensagens
    3. Atualiza ao receber: max(local, recebido) + 1
    """

    __slots__ = ('_value', '_lock')

    def __init__(self, initial_value: int = 0) -> None:
        if initial_value < 0:
            raise ValueError(
                f"Clock value cannot be negative, got {initial_value}"
            )
        self._value: int = initial_value
        self._lock: threading.Lock = threading.Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    @value.setter
    def value(self, new_value: int) -> None:
        if new_value < 0:
            raise ValueError(
                f"Clock value cannot be negative, got {new_value}"
            )
        with self._lock:
            self._value = new_value

    def increment(self) -> int:
        """Incrementa o relógio em eventos internos (Regra 1)."""
        with self._lock:
            self._value += 1
            return self._value

    def send(self) -> int:
        """Incrementa antes de enviar mensagem (Regra 2)."""
        return self.increment()

    def receive(self, received_ts: int) -> int:
        """
        Atualiza o relógio ao receber mensagem (Regra 3).

        Calcula: max(local, recebido) + 1
        Isso garante que o recebimento é sempre posterior ao envio.
        """
        with self._lock:
            self._value = max(self._value, received_ts) + 1
            return self._value

    def compare(self, other_ts: int) -> int:
        """
        Compara timestamps para ordenação.
        Retorna -1 se menor, 0 se igual, 1 se maior.
        """
        with self._lock:
            if self._value < other_ts:
                return -1
            elif self._value > other_ts:
                return 1
            else:
                return 0

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {"lamport_ts": self._value}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LamportClock':
        if "lamport_ts" not in data:
            raise ValueError("Missing 'lamport_ts' key in data")
        return cls(initial_value=data["lamport_ts"])

    def __str__(self) -> str:
        return f"LamportClock(value={self.value})"

    def __repr__(self) -> str:
        return f"LamportClock(initial_value={self.value})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LamportClock):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, LamportClock):
            return self.value < other.value
        if isinstance(other, int):
            return self.value < other
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, LamportClock):
            return self.value <= other.value
        if isinstance(other, int):
            return self.value <= other
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, LamportClock):
            return self.value > other.value
        if isinstance(other, int):
            return self.value > other
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, LamportClock):
            return self.value >= other.value
        if isinstance(other, int):
            return self.value >= other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)
