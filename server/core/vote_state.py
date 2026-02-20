"""
Gerenciador de estado de votacao.

Responsabilidade unica: manter o estado dos votos com ordenacao causal.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Set, List, Optional, Any
from datetime import datetime


@dataclass
class VoteEntry:
    """
    Representa um voto no log ordenado.

    Usa Lamport Clock para ordenacao logica - garante que votos sejam processados
    na mesma ordem em todos os nos, mesmo com clocks fisicos dessincronizados.
    """
    vote_id: str
    option: str
    lamport_ts: int
    physical_ts: float = field(default_factory=time.time)
    voter_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vote_id": self.vote_id,
            "option": self.option,
            "lamport_ts": self.lamport_ts,
            "physical_ts": self.physical_ts,
            "voter_id": self.voter_id
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VoteEntry':
        return cls(
            vote_id=data["vote_id"],
            option=data["option"],
            lamport_ts=data["lamport_ts"],
            physical_ts=data.get("physical_ts", time.time()),
            voter_id=data.get("voter_id")
        )


class VoteState:
    """
    Gerencia o estado da votacao com ordenacao causal.

    Mantem um log ordenado de votos usando Lamport Clocks, garantindo que
    todos os nos tenham a mesma visao do historico.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self.vote_log: List[VoteEntry] = []
        self.vote_counts: Dict[str, int] = {}
        self.voted_ids: Set[str] = set()

    def process_vote(
        self,
        vote_id: str,
        option: str,
        lamport_ts: int = 0,
        voter_id: Optional[str] = None
    ) -> Dict:
        """Processa um novo voto mantendo ordenacao logica."""
        with self.lock:
            if vote_id in self.voted_ids:
                return {
                    "status": "error",
                    "message": "Voto ja foi registrado",
                    "lamport_ts": lamport_ts
                }

            entry = VoteEntry(
                vote_id=vote_id,
                option=option,
                lamport_ts=lamport_ts,
                physical_ts=time.time(),
                voter_id=voter_id
            )

            self._insert_ordered(entry)
            self.voted_ids.add(vote_id)
            self.vote_counts[option] = self.vote_counts.get(option, 0) + 1

            return {
                "status": "success",
                "message": "Votou com sucesso",
                "lamport_ts": lamport_ts,
                "currentCounts": dict(self.vote_counts)
            }

    def _insert_ordered(self, entry: VoteEntry) -> None:
        """Insere voto mantendo ordenacao por Lamport timestamp."""
        i = len(self.vote_log)
        while i > 0:
            prev = self.vote_log[i - 1]
            if prev.lamport_ts < entry.lamport_ts:
                break
            if prev.lamport_ts == entry.lamport_ts and prev.physical_ts <= entry.physical_ts:
                break
            i -= 1
        self.vote_log.insert(i, entry)

    def get_results(self) -> Dict:
        """Retorna contagem atual e ultimo timestamp logico."""
        with self.lock:
            total_votes = sum(self.vote_counts.values())
            latest_lamport_ts = 0
            if self.vote_log:
                latest_lamport_ts = self.vote_log[-1].lamport_ts

            return {
                "status": "success",
                "counts": dict(self.vote_counts),
                "totalVotes": total_votes,
                "latestLamportTs": latest_lamport_ts,
                "timestamp": datetime.now().isoformat()
            }

    def get_vote_log(self, since_ts: int = 0) -> List[Dict]:
        """Retorna votos desde um timestamp."""
        with self.lock:
            return [
                entry.to_dict()
                for entry in self.vote_log
                if entry.lamport_ts > since_ts
            ]

    def apply_replication(
        self,
        vote_id: str,
        option: str,
        lamport_ts: int = 0,
        voter_id: Optional[str] = None
    ) -> bool:
        """Aplica voto replicado do lider."""
        with self.lock:
            if vote_id in self.voted_ids:
                return False

            entry = VoteEntry(
                vote_id=vote_id,
                option=option,
                lamport_ts=lamport_ts,
                physical_ts=time.time(),
                voter_id=voter_id
            )
            self._insert_ordered(entry)
            self.voted_ids.add(vote_id)
            self.vote_counts[option] = self.vote_counts.get(option, 0) + 1
            return True

    def get_state_snapshot(self) -> Dict:
        """Retorna snapshot completo do estado para sincronizacao."""
        with self.lock:
            return {
                "vote_log": [e.to_dict() for e in self.vote_log],
                "vote_counts": dict(self.vote_counts),
                "voted_ids": list(self.voted_ids),
                "total_votes": len(self.vote_log)
            }

    def reset(self) -> None:
        """Limpa todos os votos e reseta o estado."""
        with self.lock:
            self.vote_log.clear()
            self.vote_counts.clear()
            self.voted_ids.clear()
