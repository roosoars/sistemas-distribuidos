"""
Processador de votos com exclusao mutua distribuida.

Responsabilidade unica: processar votos garantindo exclusao mutua via Ricart-Agrawala.
"""

import time
import threading
import logging
from queue import Queue
from dataclasses import dataclass
from typing import Dict, Optional, Any, Callable, Tuple

from core.protocol import Message, MessageType, DistributedMessageName
from distributed.mutual_exclusion import MutualExclusionManager, ProcessState

logger = logging.getLogger("VOTE_PROCESSOR")


@dataclass
class PendingVote:
    """Representa um voto pendente aguardando processamento na secao critica."""
    vote_id: str
    option: str
    lamport_ts: int
    voter_id: Optional[str]
    response_event: threading.Event
    result: Optional[Dict] = None


class VoteProcessor:
    """
    Processa votos com exclusao mutua distribuida (Ricart-Agrawala).

    Garante que apenas um servidor processa votos por vez em ambiente distribuido.
    """

    def __init__(
        self,
        server_id: str,
        mutex_manager: MutualExclusionManager,
        peers: Dict[str, Tuple[str, int]],
        send_message: Callable,
        process_vote: Callable,
        replicate_vote: Callable,
        get_timestamp: Callable
    ):
        self.server_id = server_id
        self.mutex_manager = mutex_manager
        self.peers = peers
        self.send_message = send_message
        self._process_vote = process_vote
        self._replicate_vote = replicate_vote
        self.get_timestamp = get_timestamp

        self._pending_votes: Queue = Queue()
        self._current_vote: Optional[PendingVote] = None
        self._lock = threading.Lock()
        self._enabled = True

        self._stats = {
            "votes_with_mutex": 0,
            "cs_entries": 0,
            "cs_releases": 0,
            "requests_sent": 0,
            "replies_received": 0,
            "requests_handled": 0,
            "replies_sent": 0,
            "deferred_replies": 0
        }

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        self._enabled = value

    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatisticas de exclusao mutua."""
        return {
            **self._stats,
            "current_state": self.mutex_manager.state.value,
            "pending_votes": self._pending_votes.qsize(),
            "has_current_vote": self._current_vote is not None,
            "can_enter_cs": self.mutex_manager.can_enter_cs,
            "replies_pending": len(self.mutex_manager.peers - self.mutex_manager._replies_received)
                if self.mutex_manager.state == ProcessState.WANTED else 0
        }

    def process_vote(
        self,
        vote_id: str,
        option: str,
        lamport_ts: int,
        voter_id: Optional[str]
    ) -> Dict:
        """Processa voto com ou sem exclusao mutua."""
        if self._enabled and len(self.peers) > 0:
            return self._enqueue_vote(vote_id, option, lamport_ts, voter_id)
        else:
            result = self._process_vote(vote_id, option, lamport_ts, voter_id)
            if result['status'] == 'success':
                self._replicate_vote(vote_id, option, lamport_ts, voter_id)
            return result

    def _enqueue_vote(
        self,
        vote_id: str,
        option: str,
        lamport_ts: int,
        voter_id: Optional[str]
    ) -> Dict:
        """Enfileira voto para processamento com exclusao mutua."""
        self._stats["votes_with_mutex"] += 1

        vote = PendingVote(
            vote_id=vote_id,
            option=option,
            lamport_ts=lamport_ts,
            voter_id=voter_id,
            response_event=threading.Event()
        )

        with self._lock:
            if self._current_vote is None:
                logger.info(f"[MUTEX] Iniciando requisicao de CS para voto {vote_id}")
                threading.Thread(
                    target=self._request_critical_section,
                    args=(vote,),
                    daemon=True
                ).start()
            else:
                logger.info(f"[MUTEX] Voto {vote_id} adicionado a fila de pendentes")
                self._pending_votes.put(vote)

        if vote.response_event.wait(timeout=30.0):
            return vote.result or {"status": "error", "message": "No result"}
        else:
            logger.error(f"[MUTEX] Timeout aguardando CS para voto {vote_id}")
            return {"status": "error", "message": "Timeout waiting for critical section"}

    def _request_critical_section(self, vote: PendingVote) -> None:
        """Solicita entrada na secao critica."""
        with self._lock:
            self._current_vote = vote

        request_messages = self.mutex_manager.request_cs()
        self._stats["requests_sent"] += len(request_messages)

        logger.info(
            f"[MUTEX] Requisitando CS para voto {vote.vote_id} "
            f"(lamport_ts={self.mutex_manager.request_timestamp}, peers={len(request_messages)})"
        )

        for req in request_messages:
            peer_id = req["to"]
            if peer_id in self.peers:
                host, port = self.peers[peer_id]
                msg = Message(
                    msg_type=MessageType.EVENT,
                    name=DistributedMessageName.REQUEST,
                    sender=f"TallyServer-{self.server_id}",
                    payload={
                        "from_id": self.server_id,
                        "timestamp": req["timestamp"]
                    }
                )
                threading.Thread(
                    target=self._send_safe,
                    args=(host, port, msg),
                    daemon=True
                ).start()

        if not self.mutex_manager.peers or self.mutex_manager.can_enter_cs:
            logger.info("[MUTEX] Sem peers ou ja pode entrar - entrando na CS imediatamente")
            self._try_enter_critical_section()

    def _send_safe(self, host: str, port: int, msg: Message):
        """Envia mensagem com tratamento de erro."""
        try:
            self.send_message(host, port, msg)
        except Exception as e:
            logger.warning(f"Failed to send message to {host}:{port}: {e}")

    def handle_request(self, from_id: str, timestamp: int) -> None:
        """Processa REQUEST de secao critica."""
        request = {"from": from_id, "timestamp": timestamp}
        self._stats["requests_handled"] += 1

        logger.info(
            f"[MUTEX] REQUEST recebido de {from_id} "
            f"(lamport_ts={timestamp}, meu_estado={self.mutex_manager.state.value})"
        )

        reply = self.mutex_manager.handle_request(request)

        if reply:
            self._stats["replies_sent"] += 1
            logger.info(f"[MUTEX] Enviando REPLY imediato para {from_id}")
            if from_id in self.peers:
                host, port = self.peers[from_id]
                reply_msg = self._create_reply_message(from_id)
                threading.Thread(
                    target=self._send_safe,
                    args=(host, port, reply_msg),
                    daemon=True
                ).start()
        else:
            self._stats["deferred_replies"] += 1
            logger.info(f"[MUTEX] REPLY adiado para {from_id}")

    def handle_reply(self, from_id: str) -> None:
        """Processa REPLY de permissao para entrar na CS."""
        reply = {"from": from_id}
        self._stats["replies_received"] += 1

        logger.info(
            f"[MUTEX] REPLY recebido de {from_id} "
            f"(replies={len(self.mutex_manager._replies_received)+1}/{len(self.mutex_manager.peers)})"
        )

        self.mutex_manager.handle_reply(reply)

        if self.mutex_manager.can_enter_cs:
            logger.info("[MUTEX] Todos os REPLYs recebidos - PODE ENTRAR NA CS!")
            self._try_enter_critical_section()

    def _try_enter_critical_section(self) -> None:
        """Tenta entrar na secao critica e processar o voto pendente."""
        with self._lock:
            if self._current_vote is None:
                logger.warning("[MUTEX] Nenhum voto pendente para processar")
                return

            if not self.mutex_manager.can_enter_cs:
                logger.debug("[MUTEX] Ainda nao pode entrar na CS")
                return

            vote = self._current_vote

        try:
            self.mutex_manager.enter_cs()
            self._stats["cs_entries"] += 1

            logger.info(
                f"[MUTEX] *** ENTROU NA SECAO CRITICA *** "
                f"(voto={vote.vote_id}, lamport_ts={vote.lamport_ts})"
            )

            result = self._process_vote_in_cs(vote)
            vote.result = result

        except RuntimeError as e:
            logger.error(f"[MUTEX] Erro ao entrar na CS: {e}")
            vote.result = {"status": "error", "message": str(e)}

        finally:
            self._release_critical_section()
            vote.response_event.set()

            with self._lock:
                self._current_vote = None

            self._process_next_pending_vote()

    def _process_vote_in_cs(self, vote: PendingVote) -> Dict:
        """Processa voto dentro da secao critica."""
        logger.info(
            f"[MUTEX-CS] Processando voto: option={vote.option}, "
            f"lamport_ts={vote.lamport_ts}"
        )

        result = self._process_vote(
            vote.vote_id, vote.option, vote.lamport_ts, vote.voter_id
        )

        if result["status"] == "success":
            logger.info(f"[MUTEX-CS] Voto processado com sucesso: {vote.option}")
            self._replicate_vote(
                vote.vote_id, vote.option, vote.lamport_ts, vote.voter_id
            )
        else:
            logger.warning(f"[MUTEX-CS] Voto rejeitado: {result.get('message')}")

        return result

    def _release_critical_section(self) -> None:
        """Libera a secao critica e envia REPLYs adiados."""
        try:
            deferred_replies = self.mutex_manager.release_cs()
            self._stats["cs_releases"] += 1

            logger.info(
                f"[MUTEX] *** SAIU DA SECAO CRITICA *** "
                f"(REPLYs adiados a enviar: {len(deferred_replies)})"
            )

            for reply in deferred_replies:
                peer_id = reply["to"]
                if peer_id in self.peers:
                    host, port = self.peers[peer_id]
                    msg = self._create_reply_message(peer_id)
                    threading.Thread(
                        target=self._send_safe,
                        args=(host, port, msg),
                        daemon=True
                    ).start()
                    logger.info(f"[MUTEX] Enviando REPLY adiado para {peer_id}")

        except RuntimeError as e:
            logger.error(f"[MUTEX] Erro ao liberar CS: {e}")

    def _process_next_pending_vote(self) -> None:
        """Processa o proximo voto na fila de pendentes."""
        try:
            vote = self._pending_votes.get_nowait()
            logger.info(f"[MUTEX] Processando proximo voto da fila: {vote.vote_id}")
            self._request_critical_section(vote)
        except:
            pass

    def _create_reply_message(self, to_id: str) -> Message:
        """Cria mensagem REPLY para exclusao mutua."""
        return Message(
            msg_type=MessageType.EVENT,
            name=DistributedMessageName.REPLY,
            sender=f"TallyServer-{self.server_id}",
            payload={
                "from_id": self.server_id,
                "timestamp": self.get_timestamp()
            }
        )
