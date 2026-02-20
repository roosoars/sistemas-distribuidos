"""
Gerenciador de Estado Distribuído com Relógios Lógicos

Este módulo implementa um sistema de gerenciamento de estado para sistemas distribuídos,
utilizando Relógios de Lamport (ordenação total) e Relógios Vetoriais (detecção de causalidade).
Detecta automaticamente eventos concorrentes e mantém histórico de conflitos para resolução.
"""
import threading
import time
import logging
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field

from clocks.lamport_clock import LamportClock
from clocks.vector_clock import VectorClock


logger = logging.getLogger("STATE_MANAGER")


@dataclass
class ConflictEvent:
    """
    Representa um evento de conflito detectado por concorrência.

    Acontece quando dois eventos não possuem relação causal (são concorrentes),
    ou seja, quando nem um aconteceu antes do outro no tempo lógico.
    """
    event_type: str
    timestamp: float
    local_vector: Dict[str, int]
    received_vector: Dict[str, int]
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "local_vector": self.local_vector,
            "received_vector": self.received_vector,
            "details": self.details
        }


class StateManager:
    """
    Gerenciador de Estado com Relógios Distribuídos.

    Combina Relógio de Lamport (ordenação total) com Relógio Vetorial (causalidade)
    para detectar automaticamente eventos concorrentes e possibilitar resolução de conflitos.
    """

    def __init__(self, node_id: str, known_nodes: List[str] = None):
        self.node_id = node_id
        self._lock = threading.RLock()

        all_nodes = known_nodes or [node_id]
        if node_id not in all_nodes:
            all_nodes = [node_id] + list(all_nodes)

        # Relógio de Lamport para ordenação total de eventos
        self.lamport_clock = LamportClock()
        # Relógio Vetorial para rastreamento de causalidade
        self.vector_clock = VectorClock(node_id, all_nodes)

        self._state_version: Dict[str, int] = {n: 0 for n in all_nodes}

        self._pending_conflicts: List[ConflictEvent] = []
        self._conflict_history: List[ConflictEvent] = []
        self._max_history = 100

        self._stats = {
            "events_processed": 0,
            "causal_events": 0,
            "concurrent_events": 0,
            "conflicts_detected": 0
        }
    
    @property
    def current_lamport_ts(self) -> int:
        return self.lamport_clock.value

    @property
    def current_vector(self) -> Dict[str, int]:
        return self.vector_clock.vector.copy()

    def on_local_event(self) -> Tuple[int, Dict[str, int]]:
        """
        Registra evento local/interno do nó.
        Incrementa ambos os relógios para mudanças de estado internas.
        """
        with self._lock:
            lamport_ts = self.lamport_clock.increment()
            vector_ts = self.vector_clock.increment()
            self._state_version = vector_ts.copy()
            self._stats["events_processed"] += 1
            return lamport_ts, vector_ts

    def on_send(self) -> Tuple[int, Dict[str, int]]:
        """
        Prepara timestamps para envio de mensagem.
        Retorna os timestamps que devem ser incluídos na mensagem.
        """
        with self._lock:
            lamport_ts = self.lamport_clock.send()
            vector_ts = self.vector_clock.send()
            self._state_version = vector_ts.copy()
            self._stats["events_processed"] += 1
            return lamport_ts, vector_ts
    
    def on_receive(
        self,
        lamport_ts: int,
        vector_ts: Dict[str, int],
        event_details: Dict[str, Any] = None
    ) -> bool:
        """
        Processa timestamps de mensagem recebida.

        Atualiza ambos os relógios e detecta se há concorrência.
        Retorna True se o evento é causal, False se é concorrente (conflito).
        """
        with self._lock:
            old_vector = self._state_version.copy()

            # Atualiza relógio escalar
            self.lamport_clock.receive(lamport_ts)

            # Verifica concorrência ANTES de atualizar o vetor
            is_concurrent = self._check_concurrent(old_vector, vector_ts)

            # Atualiza relógio vetorial
            self.vector_clock.receive(vector_ts)
            self._state_version = self.vector_clock.vector.copy()

            self._stats["events_processed"] += 1

            if is_concurrent:
                # Conflito detectado - eventos sem relação causal
                self._stats["concurrent_events"] += 1
                self._stats["conflicts_detected"] += 1

                conflict = ConflictEvent(
                    event_type="concurrent_event",
                    timestamp=time.time(),
                    local_vector=old_vector,
                    received_vector=vector_ts,
                    details=event_details or {}
                )
                self._pending_conflicts.append(conflict)
                self._add_to_history(conflict)

                logger.warning(
                    f"Concurrent event detected from {event_details.get('sender', 'unknown')}: "
                    f"local={old_vector}, received={vector_ts}"
                )
            else:
                self._stats["causal_events"] += 1

            return not is_concurrent
    
    def _check_concurrent(
        self,
        local_vector: Dict[str, int],
        received_vector: Dict[str, int]
    ) -> bool:
        """
        Verifica se dois vetores representam eventos concorrentes.

        Eventos são concorrentes quando: NOT(local < received) AND NOT(received < local)
        Isso indica que os eventos não possuem relação de causalidade.
        """
        local_vc = VectorClock(self.node_id)
        local_vc.vector = local_vector.copy()

        received_vc = VectorClock("temp")
        received_vc.vector = received_vector.copy()

        return local_vc.is_concurrent(received_vc)

    def _add_to_history(self, conflict: ConflictEvent) -> None:
        self._conflict_history.append(conflict)
        if len(self._conflict_history) > self._max_history:
            self._conflict_history = self._conflict_history[-self._max_history:]

    def get_pending_conflicts(self) -> List[ConflictEvent]:
        with self._lock:
            conflicts = self._pending_conflicts.copy()
            self._pending_conflicts.clear()
            return conflicts

    def get_conflict_history(self, limit: int = 50) -> List[Dict]:
        with self._lock:
            return [c.to_dict() for c in self._conflict_history[-limit:]]

    def has_pending_conflicts(self) -> bool:
        with self._lock:
            return len(self._pending_conflicts) > 0

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                **self._stats,
                "current_lamport_ts": self.lamport_clock.value,
                "current_vector": self.vector_clock.vector.copy(),
                "pending_conflicts": len(self._pending_conflicts)
            }

    def get_timestamps(self) -> Tuple[int, Dict[str, int]]:
        with self._lock:
            return (
                self.lamport_clock.value,
                self.vector_clock.vector.copy()
            )

    def add_known_node(self, node_id: str) -> None:
        """
        Adiciona novo nó ao vetor.
        Necessário quando um novo processo entra no sistema distribuído.
        """
        with self._lock:
            if node_id not in self.vector_clock.vector:
                self.vector_clock.vector[node_id] = 0
                self._state_version[node_id] = 0

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "lamport_clock": self.lamport_clock.to_dict(),
                "vector_clock": self.vector_clock.to_dict(),
                "state_version": self._state_version.copy(),
                "stats": self._stats.copy(),
                "pending_conflicts": len(self._pending_conflicts)
            }

    def __repr__(self) -> str:
        return (
            f"StateManager(node_id={self.node_id}, "
            f"lamport_ts={self.lamport_clock.value}, "
            f"vector={self.vector_clock.vector})"
        )
