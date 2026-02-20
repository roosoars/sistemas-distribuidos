"""
Implementação de Relógio Vetorial (Vector Clock) para sistemas distribuídos.

Este módulo fornece rastreamento de causalidade entre eventos em processos distribuídos.
Permite detectar ordenação causal e eventos concorrentes, essencial para consistência
em sistemas distribuídos.

Baseado em: Mattern, F. (1988). "Virtual Time and Global States of Distributed Systems"
"""
import threading
from typing import Dict, List, Optional, Any


class VectorClock:
    """
    Relógio Vetorial para rastreamento de causalidade em sistemas distribuídos.

    Cada processo mantém um vetor de contadores, um para cada processo conhecido.
    Quando um evento ocorre, o processo incrementa seu próprio contador.
    Quando mensagens são trocadas, os vetores são sincronizados para capturar causalidade.
    """

    __slots__ = ('_process_id', '_vector', '_lock')

    def __init__(
        self,
        process_id: str,
        known_processes: Optional[List[str]] = None
    ) -> None:
        """
        Inicializa o relógio vetorial.

        Args:
            process_id: Identificador único deste processo
            known_processes: Lista de IDs de processos conhecidos (opcional)
        """
        if not process_id:
            raise ValueError("Process ID cannot be empty")

        self._process_id: str = process_id
        self._lock: threading.Lock = threading.Lock()

        # Inicializa vetor zerado para todos os processos conhecidos
        if known_processes:
            self._vector: Dict[str, int] = {p: 0 for p in known_processes}
        else:
            self._vector: Dict[str, int] = {process_id: 0}

        if process_id not in self._vector:
            self._vector[process_id] = 0

    @property
    def process_id(self) -> str:
        """Retorna o ID deste processo."""
        return self._process_id

    @property
    def vector(self) -> Dict[str, int]:
        """Retorna cópia thread-safe do vetor de relógio."""
        with self._lock:
            return dict(self._vector)

    @vector.setter
    def vector(self, new_vector: Dict[str, int]) -> None:
        """Define novo vetor de relógio de forma thread-safe."""
        with self._lock:
            self._vector = dict(new_vector)

    def increment(self) -> Dict[str, int]:
        """
        Incrementa o contador deste processo (evento interno).

        Returns:
            Cópia do vetor atualizado
        """
        with self._lock:
            self._vector[self._process_id] += 1
            return dict(self._vector)

    def send(self) -> Dict[str, int]:
        """
        Prepara o vetor para envio de mensagem.

        Incrementa o contador próprio e retorna o vetor completo
        para ser enviado junto com a mensagem.

        Returns:
            Vetor atualizado para anexar à mensagem
        """
        return self.increment()

    def receive(self, other_vector: Dict[str, int]) -> Dict[str, int]:
        """
        Atualiza o vetor ao receber uma mensagem.

        Aplica a regra de merge: para cada processo, toma o máximo entre
        o valor local e o recebido. Depois incrementa o próprio contador.
        Isso garante que toda a causalidade transitiva seja capturada.

        Args:
            other_vector: Vetor recebido na mensagem

        Returns:
            Vetor atualizado após merge e incremento
        """
        with self._lock:
            for process, value in other_vector.items():
                if process in self._vector:
                    self._vector[process] = max(self._vector[process], value)
                else:
                    self._vector[process] = value

            self._vector[self._process_id] += 1

            return dict(self._vector)

    def merge(self, other_vector: Dict[str, int]) -> None:
        """
        Merge passivo de vetores sem incremento.

        Usado para sincronização que não constitui um novo evento.
        Aplica apenas o max() elemento a elemento, sem incrementar.

        Args:
            other_vector: Vetor a ser mergeado
        """
        with self._lock:
            for process, value in other_vector.items():
                if process in self._vector:
                    self._vector[process] = max(self._vector[process], value)
                else:
                    self._vector[process] = value

    def happens_before(self, other: 'VectorClock') -> bool:
        """
        Verifica se este evento aconteceu-antes (happened-before) do outro.

        A relação "happened-before" é definida como:
        A < B se e somente se:
          - Para todo i: A[i] <= B[i]  (A não é maior em nenhuma posição)
          - Existe j: A[j] < B[j]      (A é menor em ao menos uma posição)

        Esta é a base para determinar ordenação causal entre eventos.

        Args:
            other: Outro relógio vetorial para comparar

        Returns:
            True se este relógio aconteceu-antes do outro
        """
        with self._lock:
            other_vec = other.vector
            self_vec = dict(self._vector)

        all_processes = set(self_vec.keys()) | set(other_vec.keys())

        all_leq = True
        some_lt = False

        for process in all_processes:
            self_val = self_vec.get(process, 0)
            other_val = other_vec.get(process, 0)

            if self_val > other_val:
                all_leq = False
                break
            if self_val < other_val:
                some_lt = True

        return all_leq and some_lt

    def is_concurrent(self, other: 'VectorClock') -> bool:
        """
        Detecta se dois eventos são concorrentes (sem relação causal).

        Eventos A e B são concorrentes (A || B) se:
          - NOT(A < B) AND NOT(B < A)

        Concorrência indica que os eventos ocorreram independentemente
        e podem potencialmente gerar conflitos.

        Args:
            other: Outro relógio vetorial para comparar

        Returns:
            True se os eventos são concorrentes
        """
        return not self.happens_before(other) and not other.happens_before(self)

    def compare(self, other: 'VectorClock') -> Optional[int]:
        """
        Comparação completa entre dois relógios vetoriais.

        Returns:
            -1: Este relógio aconteceu antes do outro
             0: Os relógios são iguais
             1: Este relógio aconteceu depois do outro
          None: Os eventos são concorrentes (sem ordem causal)
        """
        with self._lock:
            self_vec = dict(self._vector)
        other_vec = other.vector

        all_processes = set(self_vec.keys()) | set(other_vec.keys())

        all_equal = True
        self_leq_other = True
        other_leq_self = True
        self_lt_other = False
        other_lt_self = False

        for process in all_processes:
            self_val = self_vec.get(process, 0)
            other_val = other_vec.get(process, 0)

            if self_val != other_val:
                all_equal = False

            if self_val > other_val:
                self_leq_other = False
                other_lt_self = True

            if other_val > self_val:
                other_leq_self = False
                self_lt_other = True

        if all_equal:
            return 0
        elif self_leq_other and self_lt_other:
            return -1
        elif other_leq_self and other_lt_self:
            return 1
        else:
            # Eventos concorrentes - não há ordem causal definida
            return None

    def get_timestamp(self) -> int:
        """Retorna o valor do contador deste processo."""
        with self._lock:
            return self._vector.get(self._process_id, 0)

    def copy(self) -> 'VectorClock':
        """Cria uma cópia independente deste relógio."""
        with self._lock:
            new_clock = VectorClock(self._process_id)
            new_clock._vector = dict(self._vector)
            return new_clock

    def to_dict(self) -> Dict[str, Any]:
        """Serializa o relógio para dicionário."""
        with self._lock:
            return {
                "process_id": self._process_id,
                "vector": dict(self._vector)
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VectorClock':
        """
        Reconstrói um relógio a partir de dicionário serializado.

        Args:
            data: Dicionário com 'process_id' e 'vector'

        Returns:
            Nova instância de VectorClock
        """
        if "process_id" not in data:
            raise ValueError("Missing 'process_id' key in data")
        if "vector" not in data:
            raise ValueError("Missing 'vector' key in data")

        clock = cls(data["process_id"])
        clock._vector = dict(data["vector"])
        return clock

    def __str__(self) -> str:
        with self._lock:
            return f"VectorClock({self._process_id}, {self._vector})"

    def __repr__(self) -> str:
        with self._lock:
            return f"VectorClock(process_id={self._process_id!r}, vector={self._vector!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self.compare(other) == 0

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self.compare(other) == -1

    def __le__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        result = self.compare(other)
        return result is not None and result <= 0

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self.compare(other) == 1

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        result = self.compare(other)
        return result is not None and result >= 0

    def __hash__(self) -> int:
        with self._lock:
            return hash((self._process_id, frozenset(self._vector.items())))
