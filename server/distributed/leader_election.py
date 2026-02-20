"""
Módulo de Eleição de Líder para Sistemas Distribuídos

Implementa algoritmo de eleição rápida baseado em timestamps Lamport.
A lógica é simples: qualquer servidor pode iniciar uma eleição, todos respondem
com seus timestamps, e quem tiver o menor timestamp vira líder.
Em caso de empate, o menor ID desempata.

Isso garante consenso rápido e permite que qualquer servidor assuma liderança.
"""
import threading
import time
import random
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional, Dict, Callable

from clocks.lamport_clock import LamportClock


class MessageType(Enum):
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"


@dataclass
class ElectionMessage:
    """Mensagem trocada durante o protocolo de eleição."""
    msg_type: MessageType
    from_id: str
    to_id: str
    timestamp: int

    def to_dict(self) -> Dict:
        return {
            "type": self.msg_type.value,
            "from_id": self.from_id,
            "to_id": self.to_id,
            "timestamp": self.timestamp
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'ElectionMessage':
        return cls(
            msg_type=MessageType(data["type"]),
            from_id=data["from_id"],
            to_id=data["to_id"],
            timestamp=data["timestamp"]
        )


class LeaderElector:
    """
    Implementa eleição de líder baseada em timestamp Lamport.

    Fluxo do algoritmo:
    1. Qualquer servidor inicia eleição e envia ELECTION com seu timestamp
    2. Servidores respondem OK com seus próprios timestamps
    3. Após timeout, cada um compara timestamps recebidos
    4. Quem tem menor timestamp se declara líder e envia COORDINATOR
    5. Em caso de empate de timestamp, o menor ID ganha

    Isso garante eleição rápida e determinística sem depender de hierarquia fixa.
    """

    def __init__(
        self,
        process_id: str,
        known_processes: List[str],
        clock: Optional[LamportClock] = None
    ):
        self.process_id = process_id
        self.known_processes = list(known_processes)
        self.leader_id: Optional[str] = None
        self.election_in_progress = False
        self.waiting_for_coordinator = False
        self.clock = clock if clock else LamportClock()

        self._election_timestamp: Optional[int] = None
        self._ok_received_from: set = set()
        self._candidates: Dict[str, int] = {}  # mapeia process_id -> timestamp
        self._lock = threading.RLock()

        # Callbacks opcionais pra notificar eventos
        self.on_leader_elected: Optional[Callable[[str], None]] = None
        self.on_election_started: Optional[Callable[[], None]] = None

    def has_priority_over(self, my_id: str, my_ts: int, other_id: str, other_ts: int) -> bool:
        """
        Compara prioridade entre dois candidatos.
        Menor timestamp vence. Se timestamps iguais, menor ID desempata.
        """
        if my_ts != other_ts:
            return my_ts < other_ts
        return my_id < other_id

    def start_election(self) -> List[ElectionMessage]:
        """Inicia processo de eleição enviando ELECTION pra todos os peers."""
        with self._lock:
            if self.on_election_started:
                self.on_election_started()

            self.election_in_progress = True
            self._ok_received_from.clear()
            self._candidates.clear()
            self.waiting_for_coordinator = False

            # Gera timestamp único pra essa eleição
            self._election_timestamp = self.clock.send()

            # Me coloco como candidato
            self._candidates[self.process_id] = self._election_timestamp

            # Manda ELECTION pra galera
            messages = []
            for process_id in self.known_processes:
                msg = ElectionMessage(
                    msg_type=MessageType.ELECTION,
                    from_id=self.process_id,
                    to_id=process_id,
                    timestamp=self._election_timestamp
                )
                messages.append(msg)

            return messages

    def _become_leader(self) -> List[ElectionMessage]:
        """Assume papel de líder e anuncia pra todo mundo."""
        self.leader_id = self.process_id
        self.election_in_progress = False
        self.waiting_for_coordinator = False
        self._candidates.clear()

        if self.on_leader_elected:
            self.on_leader_elected(self.process_id)

        # Avisa todo mundo que eu sou o líder
        messages = []
        timestamp = self.clock.send()

        for process_id in self.known_processes:
            msg = ElectionMessage(
                msg_type=MessageType.COORDINATOR,
                from_id=self.process_id,
                to_id=process_id,
                timestamp=timestamp
            )
            messages.append(msg)

        return messages

    def _determine_winner(self) -> str:
        """Calcula quem deve ser o líder baseado nos candidatos conhecidos."""
        if not self._candidates:
            return self.process_id

        winner_id = self.process_id
        winner_ts = self._election_timestamp or self.clock.value

        for cand_id, cand_ts in self._candidates.items():
            if self.has_priority_over(cand_id, cand_ts, winner_id, winner_ts):
                winner_id = cand_id
                winner_ts = cand_ts

        return winner_id

    def handle_election_message(
        self,
        from_id: str,
        timestamp: int
    ) -> List[ElectionMessage]:
        """
        Recebeu ELECTION de outro servidor.
        Registra ele como candidato e responde com OK contendo meu timestamp.
        """
        with self._lock:
            self.clock.receive(timestamp)

            messages = []

            # Registra o cara como candidato
            self._candidates[from_id] = timestamp

            # Pega meu timestamp (ou gera um se eu não estava em eleição)
            my_ts = self._election_timestamp
            if my_ts is None:
                my_ts = self.clock.send()
                self._election_timestamp = my_ts
                self._candidates[self.process_id] = my_ts

            # Responde OK com meu timestamp
            ok_msg = ElectionMessage(
                msg_type=MessageType.OK,
                from_id=self.process_id,
                to_id=from_id,
                timestamp=my_ts
            )
            messages.append(ok_msg)

            # Se eu não estava em eleição, entro também
            if not self.election_in_progress:
                self.election_in_progress = True
                for process_id in self.known_processes:
                    if process_id != from_id:  # pra ele já mandei OK
                        msg = ElectionMessage(
                            msg_type=MessageType.ELECTION,
                            from_id=self.process_id,
                            to_id=process_id,
                            timestamp=my_ts
                        )
                        messages.append(msg)

            return messages

    def handle_ok_message(self, from_id: str, timestamp: int) -> List[ElectionMessage]:
        """Recebeu OK de outro servidor, registra ele como candidato."""
        with self._lock:
            self.clock.receive(timestamp)
            self._ok_received_from.add(from_id)
            self._candidates[from_id] = timestamp
            return []

    def handle_coordinator_message(
        self,
        from_id: str,
        timestamp: int
    ) -> List[ElectionMessage]:
        """Recebeu anúncio de COORDINATOR, aceita o novo líder."""
        with self._lock:
            self.clock.receive(timestamp)

            self.leader_id = from_id
            self.election_in_progress = False
            self.waiting_for_coordinator = False
            self._ok_received_from.clear()
            self._candidates.clear()

            if self.on_leader_elected:
                self.on_leader_elected(from_id)

            return []

    def handle_election_timeout(self) -> List[ElectionMessage]:
        """
        Timeout da eleição: ninguém mais vai responder.
        Calcula quem ganhou e age de acordo.
        """
        with self._lock:
            if not self.election_in_progress:
                return []

            # Me garante na lista de candidatos
            if self.process_id not in self._candidates:
                self._candidates[self.process_id] = self._election_timestamp or self.clock.value

            winner = self._determine_winner()

            if winner == self.process_id:
                # Ganhei! Assumo liderança
                return self._become_leader()
            else:
                # Outro ganhou, espero ele anunciar
                self.waiting_for_coordinator = True
                return []

    def handle_coordinator_timeout(self) -> List[ElectionMessage]:
        """
        Timeout esperando COORDINATOR: o vencedor pode ter caído.
        Remove ele da lista e recalcula, ou reinicia eleição.
        """
        with self._lock:
            if not self.waiting_for_coordinator:
                return []

            # Tira o cara que deveria ter anunciado
            winner = self._determine_winner()
            if winner != self.process_id and winner in self._candidates:
                del self._candidates[winner]

            # Recalcula sem ele
            new_winner = self._determine_winner()
            if new_winner == self.process_id:
                return self._become_leader()
            else:
                # Reseta tudo e começa de novo
                self.waiting_for_coordinator = False
                self._ok_received_from.clear()
                self._candidates.clear()
                return self.start_election()

    def get_leader(self) -> Optional[str]:
        with self._lock:
            return self.leader_id

    def is_leader(self) -> bool:
        with self._lock:
            return self.leader_id == self.process_id

    def on_leader_failure(self) -> List[ElectionMessage]:
        """Chamado quando detecta que o líder atual caiu. Inicia nova eleição."""
        with self._lock:
            self.leader_id = None
            self.waiting_for_coordinator = False
            self._ok_received_from.clear()
            self._candidates.clear()
            self._election_timestamp = None

            return self.start_election()

    def get_state(self) -> Dict:
        with self._lock:
            return {
                "process_id": self.process_id,
                "leader_id": self.leader_id,
                "election_in_progress": self.election_in_progress,
                "waiting_for_coordinator": self.waiting_for_coordinator,
                "clock_value": self.clock.value,
                "known_processes": self.known_processes.copy(),
                "ok_received_from": list(self._ok_received_from),
                "candidates": dict(self._candidates)
            }

    def __repr__(self) -> str:
        return (
            f"LeaderElector(process_id={self.process_id}, "
            f"leader={self.leader_id}, "
            f"in_election={self.election_in_progress}, "
            f"clock={self.clock.value})"
        )
