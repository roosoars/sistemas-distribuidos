"""
Servidor de Votacao Distribuido com Eleicao de Lider e Exclusao Mutua.

Este modulo e o orquestrador principal que coordena todos os componentes:
- MessageBroker: comunicacao TCP entre servidores
- LeaderElector: eleicao de lider (Bully)
- MutualExclusionManager: exclusao mutua (Ricart-Agrawala)
- VoteProcessor: processamento de votos com mutex
- HeartbeatManager: deteccao de falhas
- ServiceRegistry: registro no NameServer

Uso:
    python tally_server.py --id s1 --port 8001 --http-port 9001 --peers "s2:localhost:8002,s3:localhost:8003"
"""

import time
import argparse
import threading
import logging
import sys
from http.server import HTTPServer
from typing import Dict, List, Optional, Tuple, Any

from core.messaging import MessageBroker
from core.protocol import Message, MessageType, Command, Event, DistributedMessageName
from core.vote_state import VoteState
from core.vote_processor import VoteProcessor
from core.service_registry import ServiceRegistry
from distributed.leader_election import LeaderElector, ElectionMessage, MessageType as ElectionMessageType
from distributed.mutual_exclusion import MutualExclusionManager
from distributed.heartbeat import HeartbeatManager
from handlers.tally_handler import TallyHTTPHandler

def setup_logging(name: str) -> logging.Logger:
    """Configura logging pandronizado."""
    logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s [{name.upper()}] %(levelname)s: %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(name)

logger = setup_logging("TALLY_SERVER")

def parse_peers(peers_str: str) -> List[Tuple[str, str, int]]:
    """Parse da string de configuracao de peers."""
    if not peers_str:
        return []

    peers = []
    for peer in peers_str.split(","):
        parts = peer.strip().split(":")
        if len(parts) == 3:
            peer_id, host, port = parts
            peers.append((peer_id, host, int(port)))
    return peers

class TallyServer:
    """
    Servidor de votacao distribuido - Orquestrador principal.

    Coordena todos os componentes do sistema distribuido:
    - Comunicacao TCP via MessageBroker
    - Eleicao de lider via LeaderElector
    - Exclusao mutua via MutualExclusionManager
    - Processamento de votos via VoteProcessor
    - Deteccao de falhas via HeartbeatManager
    - Registro de servicos via ServiceRegistry
    """

    ELECTION_TIMEOUT = 1.0
    COORDINATOR_TIMEOUT = 1.5
    SYNC_DELAY = 0.5

    def __init__(
        self,
        server_id: str,
        host: str,
        port: int,
        peers: List[Tuple[str, str, int]],
        http_port: int = None,
        room_id: str = "default",
        nameserver_host: str = None,
        nameserver_port: int = None,
        advertised_host: str = None
    ):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.http_port = http_port or (port + 1000)
        self.room_id = room_id
        self.advertised_host = advertised_host or host
        self.peers: Dict[str, Tuple[str, int]] = {
            peer_id: (peer_host, peer_port)
            for peer_id, peer_host, peer_port in peers
        }
        self.is_leader = False
        self.current_leader: Optional[str] = None
        self.running = True
        self.simulated_failure = False
        self.vote_state = VoteState()
        all_process_ids = [server_id] + list(self.peers.keys())
        self.broker = MessageBroker(
            host=host,
            port=port,
            component_id=f"TallyServer-{server_id}",
            known_peers=list(self.peers.keys())
        )
        self.leader_elector = LeaderElector(
            process_id=server_id,
            known_processes=list(self.peers.keys())
        )
        self.leader_elector.on_leader_elected = self._on_leader_elected
        self.leader_elector.on_election_started = self._on_election_started
        self.mutex_manager = MutualExclusionManager(
            process_id=server_id,
            all_processes=all_process_ids,
            clock=self.broker.clock
        )
        self.vote_processor = VoteProcessor(
            server_id=server_id,
            mutex_manager=self.mutex_manager,
            peers=self.peers,
            send_message=self.broker.send_message,
            process_vote=self.vote_state.process_vote,
            replicate_vote=self._replicate_to_backups,
            get_timestamp=lambda: self.broker.current_timestamp
        )
        self.heartbeat_manager = HeartbeatManager(
            server_id=server_id,
            peers=self.peers,
            send_message=self.broker.send_message,
            on_leader_failure=self._on_leader_failure
        )
        self.service_registry = ServiceRegistry(
            server_id=server_id,
            host=advertised_host or host,
            port=port,
            http_port=self.http_port,
            room_id=room_id,
            nameserver_host=nameserver_host,
            nameserver_port=nameserver_port,
            send_message=self.broker.send_message,
            component_id=self.broker.component_id
        )
        self._election_timer: Optional[threading.Timer] = None
        self._coordinator_timer: Optional[threading.Timer] = None
        self.http_server: Optional[HTTPServer] = None
        self._register_handlers()

        logger.info(
            f"TallyServer initialized: id={server_id}, tcp_port={port}, "
            f"http_port={self.http_port}, peers={list(self.peers.keys())}"
        )

    @property
    def mutex_enabled(self) -> bool:
        return self.vote_processor.enabled

    @mutex_enabled.setter
    def mutex_enabled(self, value: bool):
        self.vote_processor.enabled = value

    def get_mutex_stats(self) -> Dict[str, Any]:
        return self.vote_processor.get_stats()

    def process_vote(self, vote_id: str, option: str, voter_id: str) -> Dict:
        """Processa um voto (chamado pelo HTTP handler)."""
        lamport_ts = int(time.time() * 1000)
        return self.vote_processor.process_vote(vote_id, option, lamport_ts, voter_id)

    def start_election(self):
        """Inicia uma nova eleicao de lider."""
        logger.info("Starting leader election...")
        election_msgs = self.leader_elector.start_election()
        for msg in election_msgs:
            self._send_election_message(msg)
        self._start_election_timeout()

    def simulate_failure(self):
        """Simula falha do servidor."""
        self.simulated_failure = True
        self.is_leader = False
        self.heartbeat_manager.simulated_failure = True
        self.heartbeat_manager.is_leader = False
        if self.service_registry.enabled:
            self.service_registry.unregister()

    def recover_from_failure(self):
        """Recupera de falha simulada."""
        self.simulated_failure = False
        self.heartbeat_manager.simulated_failure = False
        self.current_leader = None
        self.start_election()

    def reset(self):
        """Reseta o servidor."""
        self.vote_state.reset()
        self.simulated_failure = False
        self.is_leader = False
        self.current_leader = None
        self.start_election()

    def _register_handlers(self) -> None:
        """Registra handlers para todos os tipos de mensagem."""
        self.broker.register_handler(DistributedMessageName.ELECTION, self._handle_election)
        self.broker.register_handler(DistributedMessageName.OK, self._handle_ok)
        self.broker.register_handler(DistributedMessageName.COORDINATOR, self._handle_coordinator)
        self.broker.register_handler(DistributedMessageName.REQUEST, self._handle_request)
        self.broker.register_handler(DistributedMessageName.REPLY, self._handle_reply)
        self.broker.register_handler(DistributedMessageName.HEARTBEAT, self._handle_heartbeat)
        self.broker.register_handler("VoteCommand", self._handle_vote)
        self.broker.register_handler("GetResultsQuery", self._handle_get_results)
        self.broker.register_handler("ReplicateVote", self._handle_replication)
        self.broker.register_handler("SyncRequest", self._handle_sync_request)
        self.broker.register_handler("SyncResponse", self._handle_sync_response)

    def _handle_election(self, message: Message) -> Optional[Message]:
        """Processa mensagem ELECTION."""
        if self.simulated_failure:
            return None

        from_id = message.payload.get("from_id")
        timestamp = message.payload.get("timestamp", 0)

        logger.info(f"Received ELECTION from {from_id} (ts={timestamp})")
        responses = self.leader_elector.handle_election_message(from_id, timestamp)

        for msg in responses:
            self._send_election_message(msg)

        for msg in responses:
            if msg.msg_type == ElectionMessageType.OK:
                self._start_election_timeout()
                break

        return None

    def _handle_ok(self, message: Message) -> Optional[Message]:
        """Processa mensagem OK."""
        from_id = message.payload.get("from_id")
        timestamp = message.payload.get("timestamp", 0)

        logger.info(f"Received OK from {from_id} (ts={timestamp})")
        self._cancel_election_timeout()
        self.leader_elector.handle_ok_message(from_id, timestamp)
        self._start_coordinator_timeout()

        return None

    def _handle_coordinator(self, message: Message) -> Optional[Message]:
        """Processa mensagem COORDINATOR."""
        from_id = message.payload.get("from_id")
        timestamp = message.payload.get("timestamp", 0)

        logger.info(f"Received COORDINATOR from {from_id} (ts={timestamp})")
        self._cancel_election_timeout()
        self._cancel_coordinator_timeout()

        self.leader_elector.handle_coordinator_message(from_id, timestamp)

        if from_id == self.server_id:
            self._become_leader()
        else:
            if self.simulated_failure:
                self.simulated_failure = False
                self.heartbeat_manager.simulated_failure = False
            self._become_backup(from_id)

        return None

    def _handle_request(self, message: Message) -> Optional[Message]:
        """Processa REQUEST de secao critica."""
        from_id = message.payload.get("from_id")
        timestamp = message.payload.get("timestamp", 0)
        self.vote_processor.handle_request(from_id, timestamp)
        return None

    def _handle_reply(self, message: Message) -> Optional[Message]:
        """Processa REPLY de permissao para CS."""
        from_id = message.payload.get("from_id")
        self.vote_processor.handle_reply(from_id)
        return None

    def _handle_heartbeat(self, message: Message) -> Optional[Message]:
        """Processa heartbeat do lider."""
        from_id = message.payload.get("from_id")
        self.heartbeat_manager.handle_heartbeat(from_id)
        return None

    def _handle_vote(self, message: Message) -> Message:
        """Processa requisicao de voto via TCP."""
        if not self.is_leader:
            return Message(
                MessageType.RESPONSE, "VoteRejected", self.broker.component_id,
                {"error": "Not leader", "leader_id": self.current_leader}
            )

        payload = message.payload
        vote_id = payload.get("voteId")
        option = payload.get("option")
        voter_id = payload.get("voterId")

        if not vote_id or not option:
            return Message(
                MessageType.RESPONSE, "VoteError", self.broker.component_id,
                {"error": "Invalid vote data"}
            )

        result = self.vote_processor.process_vote(vote_id, option, message.lamport_ts, voter_id)
        result["mutex_stats"] = self.get_mutex_stats()

        name = "VoteAccepted" if result["status"] == "success" else "VoteRejected"
        return Message(MessageType.RESPONSE, name, self.broker.component_id, result)

    def _handle_get_results(self, message: Message) -> Message:
        """Processa consulta de resultados."""
        results = self.vote_state.get_results()
        results["is_leader"] = self.is_leader
        results["leader_id"] = self.current_leader
        return Message(MessageType.RESPONSE, "ResultsData", self.broker.component_id, results)

    def _handle_replication(self, message: Message) -> Message:
        """Processa replicacao de voto do lider."""
        payload = message.payload
        if self.vote_state.apply_replication(
            payload.get("voteId"),
            payload.get("option"),
            payload.get("lamportTs", message.lamport_ts),
            payload.get("voterId")
        ):
            return Message(MessageType.RESPONSE, "Ack", self.broker.component_id, {"status": "ok"})
        return Message(MessageType.RESPONSE, "Ack", self.broker.component_id, {"status": "already_exists"})

    def _handle_sync_request(self, message: Message) -> Message:
        """Processa requisicao de sincronizacao."""
        snapshot = self.vote_state.get_state_snapshot()
        return Message(MessageType.RESPONSE, "SyncResponse", self.broker.component_id, snapshot)

    def _handle_sync_response(self, message: Message) -> Optional[Message]:
        """Processa resposta de sincronizacao."""
        vote_log = message.payload.get("vote_log", [])
        for vote_data in vote_log:
            self.vote_state.apply_replication(
                vote_id=vote_data["vote_id"],
                option=vote_data["option"],
                lamport_ts=vote_data["lamport_ts"],
                voter_id=vote_data.get("voter_id")
            )
        return None

    def _send_election_message(self, msg: ElectionMessage) -> None:
        """Envia mensagem de eleicao para um peer."""
        if msg.to_id not in self.peers:
            return

        host, port = self.peers[msg.to_id]
        message = Message(
            msg_type=MessageType.EVENT,
            name=msg.msg_type.value,
            sender=self.broker.component_id,
            payload={
                "from_id": msg.from_id,
                "to_id": msg.to_id,
                "timestamp": msg.timestamp
            }
        )

        threading.Thread(
            target=self._send_safe,
            args=(host, port, message),
            daemon=True
        ).start()

    def _send_safe(self, host: str, port: int, message: Message) -> None:
        """Envia mensagem com tratamento de erro."""
        try:
            self.broker.send_message(host, port, message)
        except Exception as e:
            logger.warning(f"Failed to send message to {host}:{port}: {e}")

    def _start_election_timeout(self) -> None:
        self._cancel_election_timeout()
        self._election_timer = threading.Timer(self.ELECTION_TIMEOUT, self._on_election_timeout)
        self._election_timer.daemon = True
        self._election_timer.start()

    def _cancel_election_timeout(self) -> None:
        if self._election_timer:
            self._election_timer.cancel()
            self._election_timer = None

    def _on_election_timeout(self) -> None:
        logger.info("Election timeout - no OK received")
        coordinator_msgs = self.leader_elector.handle_election_timeout()
        for msg in coordinator_msgs:
            self._send_election_message(msg)
        if self.leader_elector.is_leader():
            self._become_leader()

    def _start_coordinator_timeout(self) -> None:
        self._cancel_coordinator_timeout()
        self._coordinator_timer = threading.Timer(self.COORDINATOR_TIMEOUT, self._on_coordinator_timeout)
        self._coordinator_timer.daemon = True
        self._coordinator_timer.start()

    def _cancel_coordinator_timeout(self) -> None:
        if self._coordinator_timer:
            self._coordinator_timer.cancel()
            self._coordinator_timer = None

    def _on_coordinator_timeout(self) -> None:
        logger.warning("Coordinator timeout - restarting election")
        election_msgs = self.leader_elector.handle_coordinator_timeout()
        for msg in election_msgs:
            self._send_election_message(msg)
        if election_msgs:
            self._start_election_timeout()

    def _on_leader_elected(self, leader_id: str) -> None:
        logger.info(f"Leader elected: {leader_id}")
        self.current_leader = leader_id

    def _on_election_started(self) -> None:
        logger.info("Election started")

    def _on_leader_failure(self) -> None:
        """Callback quando lider falha."""
        self.current_leader = None
        election_msgs = self.leader_elector.on_leader_failure()
        for msg in election_msgs:
            self._send_election_message(msg)
        if election_msgs:
            self._start_election_timeout()

    def _become_leader(self) -> None:
        """Assume papel de lider."""
        self.is_leader = True
        self.current_leader = self.server_id
        self.simulated_failure = False

        self.heartbeat_manager.is_leader = True
        self.heartbeat_manager.simulated_failure = False
        self.service_registry.is_leader = True

        logger.info(f"*** BECAME LEADER *** (id={self.server_id})")

        threading.Thread(target=self._sync_state_from_peers, daemon=True).start()

        if self.service_registry.enabled:
            self.service_registry.register("primary")

    def _become_backup(self, leader_id: str) -> None:
        """Assume papel de backup."""
        self.is_leader = False
        self.current_leader = leader_id

        self.heartbeat_manager.is_leader = False
        self.heartbeat_manager.current_leader = leader_id
        self.service_registry.is_leader = False

        logger.info(f"Became BACKUP (leader={leader_id})")

        if self.service_registry.enabled:
            self.service_registry.register("backup")

    def _sync_state_from_peers(self) -> None:
        """Sincroniza estado dos peers."""
        time.sleep(self.SYNC_DELAY)

        for peer_id, (peer_host, peer_port) in self.peers.items():
            try:
                msg = Command("SyncRequest", self.broker.component_id, {
                    "from_id": self.server_id,
                    "timestamp": time.time()
                })
                response = self.broker.send_message(peer_host, peer_port, msg)
                if response and response.payload:
                    vote_log = response.payload.get("vote_log", [])
                    applied = 0
                    for vote_data in vote_log:
                        if self.vote_state.apply_replication(
                            vote_id=vote_data["vote_id"],
                            option=vote_data["option"],
                            lamport_ts=vote_data["lamport_ts"],
                            voter_id=vote_data.get("voter_id")
                        ):
                            applied += 1
                    if applied > 0:
                        logger.info(f"Synced {applied} votes from {peer_id}")
            except Exception as e:
                logger.warning(f"Failed to sync from {peer_id}: {e}")

    def _replicate_to_backups(self, vote_id: str, option: str, lamport_ts: int, voter_id: str = None) -> None:
        """Replica voto para todos os servidores backup."""
        for peer_id, (peer_host, peer_port) in self.peers.items():
            cmd = Command("ReplicateVote", self.broker.component_id, {
                "voteId": vote_id,
                "option": option,
                "lamportTs": lamport_ts,
                "voterId": voter_id
            })
            threading.Thread(
                target=self.broker.send_message,
                args=(peer_host, peer_port, cmd),
                daemon=True
            ).start()

    def start(self) -> None:
        """Inicia o servidor."""
        logger.info(f"Starting TallyServer {self.server_id} on TCP:{self.port} HTTP:{self.http_port}...")

        self.broker.start()

        self.http_server = HTTPServer((self.host, self.http_port), TallyHTTPHandler)
        self.http_server.tally_server = self
        http_thread = threading.Thread(target=self.http_server.serve_forever, daemon=True)
        http_thread.start()
        logger.info(f"HTTP API running on port {self.http_port}")

        time.sleep(1.0)

        self.heartbeat_manager.start()
        self.service_registry.start()

        logger.info("Initiating leader election...")
        self.start_election()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.stop()

    def stop(self) -> None:
        """Para o servidor."""
        self.running = False
        self._cancel_election_timeout()
        self._cancel_coordinator_timeout()
        self.heartbeat_manager.stop()
        self.service_registry.stop()
        if self.http_server:
            self.http_server.shutdown()
        self.broker.stop()
        logger.info(f"TallyServer {self.server_id} stopped")

def main():
    parser = argparse.ArgumentParser(description="Servidor de Votacao Distribuido")
    parser.add_argument('--id', required=True, help='Server ID (e.g., s1, s2, s3)')
    parser.add_argument('--port', type=int, required=True, help='TCP port')
    parser.add_argument('--http-port', type=int, default=None, help='HTTP API port')
    parser.add_argument('--peers', default='', help='Peer list: "id:host:port,..."')
    parser.add_argument('--room', default='default', help='Room ID')
    parser.add_argument('--nameserver-host', default=None, help='Nameserver host')
    parser.add_argument('--nameserver-port', type=int, default=None, help='Nameserver port')
    parser.add_argument('--advertised-host', default=None, help='Advertised host')

    args = parser.parse_args()
    peers = parse_peers(args.peers)

    server = TallyServer(
        server_id=args.id,
        host='0.0.0.0',
        port=args.port,
        http_port=args.http_port,
        peers=peers,
        room_id=args.room,
        nameserver_host=args.nameserver_host,
        nameserver_port=args.nameserver_port,
        advertised_host=args.advertised_host
    )

    server.start()

if __name__ == '__main__':
    main()
