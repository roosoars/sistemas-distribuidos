"""
Broker de mensagens com suporte a relógios lógicos distribuídos.

Este módulo implementa um MessageBroker que gerencia comunicação TCP entre nós
distribuídos, integrando Lamport Clock e Vector Clock para ordenação causal de
eventos e detecção automática de concorrência.
"""
import socket
import threading
import json
import logging
from typing import Callable, Dict, Optional, List, Any

from core.protocol import Message, MessageType
from core.state_manager import StateManager

logger = logging.getLogger("MESSAGING")


class MessageBroker:
    """
    Broker de mensagens com relógios lógicos para sistemas distribuídos.

    Gerencia comunicação TCP entre nós, mantendo Lamport Clock para ordenação
    total de eventos e Vector Clock para detectar concorrência. Permite registro
    de handlers customizados por tipo de mensagem.
    """

    def __init__(self, host: str, port: int, component_id: str,
                 known_peers: List[str] = None):
        """
        Inicializa o broker de mensagens.

        Args:
            host: Endereço IP para escutar conexões
            port: Porta TCP para escutar
            component_id: ID único deste nó no sistema distribuído
            known_peers: Lista opcional de IDs de peers conhecidos
        """
        self.host = host
        self.port = port
        self.component_id = component_id
        self.running = False
        self.server_socket = None
        self.handlers: Dict[str, Callable[[Message], Optional[Message]]] = {}
        self.default_handler: Optional[Callable[[Message], Optional[Message]]] = None

        # StateManager gerencia tanto Lamport quanto Vector Clock
        all_nodes = [component_id] + (known_peers or [])
        self.state_manager = StateManager(component_id, all_nodes)

        # Callback opcional chamado quando evento concorrente é detectado
        self.on_concurrent_event: Optional[Callable[[Message, Dict], None]] = None

    @property
    def clock(self):
        """Acesso direto ao Lamport Clock."""
        return self.state_manager.lamport_clock

    @property
    def current_timestamp(self) -> int:
        """Timestamp atual do Lamport Clock."""
        return self.state_manager.current_lamport_ts

    @property
    def current_vector(self) -> Dict[str, int]:
        """Estado atual do Vector Clock."""
        return self.state_manager.current_vector

    def add_peer(self, peer_id: str) -> None:
        """Adiciona novo peer ao Vector Clock dinamicamente."""
        self.state_manager.add_known_node(peer_id)

    def start(self) -> None:
        """Inicia servidor TCP e começa a aceitar conexões."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)

        threading.Thread(target=self._accept_loop, daemon=True).start()

        lamport_ts, vector_ts = self.state_manager.on_local_event()
        logger.info(
            f"Broker escutando em {self.host}:{self.port} como {self.component_id} "
            f"(lamport_ts={lamport_ts}, vector_ts={vector_ts})"
        )

    def stop(self) -> None:
        """Para o servidor e fecha socket."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        logger.info(f"Broker {self.component_id} parado")

    def register_handler(
        self,
        msg_name: str,
        handler: Callable[[Message], Optional[Message]]
    ) -> None:
        """
        Registra handler para tipo específico de mensagem.

        Args:
            msg_name: Nome do tipo de mensagem
            handler: Função que processa a mensagem e retorna resposta opcional
        """
        self.handlers[msg_name] = handler

    def set_default_handler(
        self,
        handler: Callable[[Message], Optional[Message]]
    ) -> None:
        """Define handler padrão para mensagens sem handler específico."""
        self.default_handler = handler

    def send_message(
        self,
        target_host: str,
        target_port: int,
        message: Message
    ) -> Optional[Message]:
        """
        Envia mensagem para nó remoto com timestamps dos relógios lógicos.

        Anexa automaticamente Lamport e Vector timestamps à mensagem antes de enviar.
        Aguarda resposta por até 5 segundos e atualiza relógios ao receber.

        Args:
            target_host: IP do destinatário
            target_port: Porta TCP do destinatário
            message: Mensagem a ser enviada

        Returns:
            Mensagem de resposta se recebida, None caso contrário
        """
        try:
            lamport_ts, vector_ts = self.state_manager.on_send()
            message.lamport_ts = lamport_ts
            message.vector_ts = vector_ts

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((target_host, target_port))
            sock.sendall(message.to_json().encode('utf-8'))

            logger.debug(
                f"Enviado {message.name} para {target_host}:{target_port} "
                f"(lamport_ts={lamport_ts})"
            )

            try:
                response_data = sock.recv(4096).decode('utf-8')
                if response_data:
                    response = Message.from_json(response_data)
                    self.state_manager.on_receive(
                        response.lamport_ts,
                        response.vector_ts,
                        {"sender": response.sender, "message": response.name}
                    )
                    return response
            except socket.timeout:
                pass

            sock.close()
            return None

        except Exception as e:
            logger.error(
                f"Falha ao enviar mensagem para {target_host}:{target_port} - {e}"
            )
            return None

    def _accept_loop(self) -> None:
        """Loop principal que aceita conexões TCP de entrada."""
        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(client_sock,),
                    daemon=True
                ).start()
            except OSError:
                break

    def _handle_connection(self, sock: socket.socket) -> None:
        """
        Processa conexão recebida, atualiza relógios e detecta concorrência.

        Ao receber mensagem, atualiza Lamport e Vector Clocks. Se Vector Clock
        detectar evento concorrente, chama callback on_concurrent_event se definido.
        """
        try:
            data = sock.recv(4096).decode('utf-8')
            if not data:
                return

            try:
                message = Message.from_json(data)

                event_details = {
                    "sender": message.sender,
                    "message": message.name,
                    "payload_size": len(str(message.payload))
                }

                # Atualiza relógios e verifica se evento é causalmente relacionado
                is_causal = self.state_manager.on_receive(
                    message.lamport_ts,
                    message.vector_ts,
                    event_details
                )

                if not is_causal:
                    logger.warning(
                        f"EVENTO CONCORRENTE detectado de {message.sender}: {message.name}"
                    )
                    if self.on_concurrent_event:
                        self.on_concurrent_event(message, event_details)

                self._dispatch(message, sock, is_causal)

            except json.JSONDecodeError:
                logger.error("Recebido JSON inválido")

        except Exception as e:
            logger.error(f"Erro de conexão: {e}")
        finally:
            sock.close()

    def _dispatch(self, message: Message, sock: socket.socket,
                  is_causal: bool = True) -> None:
        """
        Despacha mensagem para handler apropriado e envia resposta.

        Args:
            message: Mensagem recebida
            sock: Socket para enviar resposta
            is_causal: True se evento foi causalmente relacionado
        """
        log_suffix = "" if is_causal else " [CONCURRENT]"
        logger.info(
            f"Recebido {message} (clock local agora={self.current_timestamp}){log_suffix}"
        )

        handler = self.handlers.get(message.name, self.default_handler)

        if handler:
            response = handler(message)
            if response and isinstance(response, Message):
                try:
                    lamport_ts, vector_ts = self.state_manager.on_send()
                    response.lamport_ts = lamport_ts
                    response.vector_ts = vector_ts
                    sock.sendall(response.to_json().encode('utf-8'))
                except Exception as e:
                    logger.error(f"Erro ao enviar resposta: {e}")
        else:
            logger.warning(f"Sem handler para {message.name}")

    def get_pending_conflicts(self) -> List[Dict[str, Any]]:
        """Retorna lista de conflitos pendentes detectados pelo Vector Clock."""
        return [c.to_dict() for c in self.state_manager.get_pending_conflicts()]

    def has_pending_conflicts(self) -> bool:
        """Verifica se existem conflitos pendentes."""
        return self.state_manager.has_pending_conflicts()

    def get_clock_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas dos relógios lógicos."""
        return self.state_manager.get_stats()
