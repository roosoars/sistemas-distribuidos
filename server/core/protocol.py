"""
Protocolo de mensagens para sistema distribuído.

Define tipos de mensagens e estruturas para comunicação entre nós do sistema,
incluindo suporte a relógios lógicos (Lamport e Vector) para ordenação causal.
Implementa mensagens para algoritmos distribuídos: eleição de líder (Bully),
exclusão mútua (Ricart-Agrawala) e monitoramento de falhas (Heartbeat).
"""

import json
import uuid
import time
from typing import Dict, Any, Optional
from enum import Enum


class MessageType(Enum):
    """Tipos de mensagens do protocolo"""
    COMMAND = "COMMAND"
    EVENT = "EVENT"
    QUERY = "QUERY"
    RESPONSE = "RESPONSE"


class DistributedMessageName:
    """Nomes das mensagens usadas em algoritmos distribuídos"""

    # Eleição de líder (algoritmo Bully)
    ELECTION = "ELECTION"
    OK = "OK"
    COORDINATOR = "COORDINATOR"

    # Exclusão mútua (algoritmo Ricart-Agrawala)
    REQUEST = "REQUEST"
    REPLY = "REPLY"

    # Monitoramento de falhas
    HEARTBEAT = "HEARTBEAT"


class Message:
    """
    Mensagem base do protocolo com suporte a relógios lógicos.

    Carrega três tipos de timestamp:
    - Físico: tempo real da criação
    - Lamport: para ordenação total de eventos
    - Vector: para rastreamento de causalidade entre nós
    """

    def __init__(self,
                 msg_type: MessageType,
                 name: str,
                 sender: str,
                 payload: Dict[str, Any] = None,
                 msg_id: str = None,
                 timestamp: float = None,
                 lamport_ts: int = None,
                 vector_ts: Dict[str, int] = None):
        self.id = msg_id or str(uuid.uuid4())
        self.type = msg_type
        self.name = name
        self.sender = sender
        self.payload = payload or {}
        self.timestamp = timestamp or time.time()
        self.lamport_ts: int = lamport_ts if lamport_ts is not None else 0
        self.vector_ts: Dict[str, int] = vector_ts if vector_ts is not None else {}

    def to_json(self) -> str:
        """Serializa mensagem para JSON"""
        return json.dumps({
            "id": self.id,
            "type": self.type.value,
            "name": self.name,
            "sender": self.sender,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "lamport_ts": self.lamport_ts,
            "vector_ts": self.vector_ts
        })

    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        """Deserializa mensagem do JSON"""
        data = json.loads(json_str)
        return cls(
            msg_type=MessageType(data["type"]),
            name=data["name"],
            sender=data["sender"],
            payload=data["payload"],
            msg_id=data["id"],
            timestamp=data["timestamp"],
            lamport_ts=data.get("lamport_ts", 0),
            vector_ts=data.get("vector_ts", {})
        )

    def __repr__(self) -> str:
        return (
            f"Message({self.type.name}, {self.name}, "
            f"de={self.sender}, lamport_ts={self.lamport_ts})"
        )


class Command(Message):
    """Comando - solicita que o destinatário execute uma ação"""

    def __init__(self, name: str, sender: str, payload: Dict[str, Any] = None,
                 lamport_ts: int = None, vector_ts: Dict[str, int] = None):
        super().__init__(MessageType.COMMAND, name, sender, payload,
                        lamport_ts=lamport_ts, vector_ts=vector_ts)


class Event(Message):
    """Evento - notifica que algo aconteceu no sistema"""

    def __init__(self, name: str, sender: str, payload: Dict[str, Any] = None,
                 lamport_ts: int = None, vector_ts: Dict[str, int] = None):
        super().__init__(MessageType.EVENT, name, sender, payload,
                        lamport_ts=lamport_ts, vector_ts=vector_ts)


class Query(Message):
    """Query - consulta informação sem causar efeitos colaterais"""

    def __init__(self, name: str, sender: str, payload: Dict[str, Any] = None,
                 lamport_ts: int = None, vector_ts: Dict[str, int] = None):
        super().__init__(MessageType.QUERY, name, sender, payload,
                        lamport_ts=lamport_ts, vector_ts=vector_ts)


class Response(Message):
    """Resposta - retorna o resultado de um comando ou query"""

    def __init__(self, name: str, sender: str, payload: Dict[str, Any] = None,
                 lamport_ts: int = None, vector_ts: Dict[str, int] = None):
        super().__init__(MessageType.RESPONSE, name, sender, payload,
                        lamport_ts=lamport_ts, vector_ts=vector_ts)
