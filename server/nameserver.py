"""
NameServer - Descoberta de Serviços com API HTTP

Servidor central de registro para sistema de votação distribuído.
Clientes consultam este servidor para descobrir quem é o líder atual.
Servidores de contagem se registram aqui quando eleitos como líderes.

Uso:
    python nameserver.py --port 8000 --http-port 8080
"""

import logging
import time
import json
import threading
from typing import Dict, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from core.messaging import MessageBroker
from core.protocol import Message, MessageType, Command, Event

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [NAMESERVER] %(levelname)s: %(message)s'
)
logger = logging.getLogger("NAMESERVER")

class NameServerHTTPHandler(BaseHTTPRequestHandler):
    """Handler de requisições HTTP para a API REST do NameServer."""

    def log_message(self, format, *args):
        """Suprime logs padrão do HTTP server."""
        pass

    def _send_json(self, data: dict, status: int = 200):
        """Envia resposta JSON com headers CORS."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_OPTIONS(self):
        """Responde preflight CORS."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        """Processa requisições GET da API."""
        parsed = urlparse(self.path)
        path = parsed.path

        if path == '/health':
            self._send_json({'status': 'ok', 'time': time.time()})

        elif path == '/api/leader':
            leader = self.server.nameserver.get_leader()
            if leader:
                self._send_json({
                    'found': True,
                    'leader': leader
                })
            else:
                self._send_json({
                    'found': False,
                    'error': 'No leader registered'
                }, 404)

        elif path == '/api/services':
            services = self.server.nameserver.get_all_services()
            self._send_json({'services': services})

        elif path == '/api/status':
            leader = self.server.nameserver.get_leader()
            services = self.server.nameserver.get_all_services()
            self._send_json({
                'role': 'nameserver',
                'leader': leader,
                'services_count': len(services),
                'uptime': time.time() - self.server.nameserver.start_time
            })

        else:
            self._send_json({'error': 'Not found'}, 404)

class NameServer:
    """
    Servidor de descoberta de serviços com interfaces TCP e HTTP.

    Mantém registro de todos os serviços ativos no sistema,
    permitindo que clientes descubram o líder atual através
    de consultas via TCP ou HTTP.
    """

    def __init__(
        self,
        host: str = '0.0.0.0',
        port: int = 8000,
        http_port: int = 8080
    ):
        self.host = host
        self.port = port
        self.http_port = http_port
        self.start_time = time.time()
        self.registry: Dict[str, Dict] = {}
        self.registry_lock = threading.Lock()
        self.broker = MessageBroker(host, port, "NameServer")
        self.broker.register_handler("RegisterService", self.handle_registration)
        self.broker.register_handler("QueryService", self.handle_query)
        self.broker.register_handler("ListServices", self.handle_list)
        self.broker.register_handler("Ping", self.handle_ping)
        self.broker.register_handler("UnregisterService", self.handle_unregister)

        self.http_server: Optional[HTTPServer] = None

    def get_leader(self) -> Optional[Dict]:
        """
        Retorna informações do líder atual.

        Procura no registry por um serviço com role=primary
        e verifica se ainda está ativo (heartbeat < 30s).
        """
        with self.registry_lock:
            for name, info in self.registry.items():
                meta = info.get('meta', {})
                if meta.get('role') == 'primary':
                    if time.time() - info.get('last_seen', 0) < 30:
                        return {
                            'service_name': name,
                            'address': info.get('address'),
                            'port': info.get('port'),
                            'http_port': meta.get('http_port'),
                            'server_id': meta.get('serverId'),
                            'last_seen': info.get('last_seen')
                        }
            return None

    def get_all_services(self) -> Dict:
        """Retorna cópia do registro completo de serviços."""
        with self.registry_lock:
            return dict(self.registry)

    def start(self):
        """Inicia servidores TCP e HTTP em threads separadas."""
        logger.info(f"Starting NameServer on TCP:{self.port} HTTP:{self.http_port}")

        self.broker.start()
        self.http_server = HTTPServer((self.host, self.http_port), NameServerHTTPHandler)
        self.http_server.nameserver = self
        http_thread = threading.Thread(target=self.http_server.serve_forever, daemon=True)
        http_thread.start()
        logger.info(f"HTTP API running on port {self.http_port}")
        cleanup_thread = threading.Thread(target=self._cleanup_stale_services, daemon=True)
        cleanup_thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down NameServer...")
            self.stop()

    def stop(self):
        """Para todos os servidores."""
        if self.http_server:
            self.http_server.shutdown()
        self.broker.stop()

    def _cleanup_stale_services(self):
        """Remove serviços que não deram sinal de vida há mais de 60s."""
        while True:
            time.sleep(10)
            now = time.time()
            with self.registry_lock:
                stale = [
                    name for name, info in self.registry.items()
                    if now - info.get('last_seen', 0) > 60
                ]
                for name in stale:
                    logger.info(f"Removing stale service: {name}")
                    del self.registry[name]

    def handle_registration(self, message: Message) -> Message:
        """Registra um novo serviço no nameserver."""
        payload = message.payload
        service_name = payload.get("serviceName")
        address = payload.get("address")
        port = payload.get("port")

        if not all([service_name, address, port]):
            return Message(
                MessageType.RESPONSE, "Error", "NameServer",
                {"error": "Missing required fields"}
            )

        with self.registry_lock:
            self.registry[service_name] = {
                "address": address,
                "port": port,
                "meta": payload.get("meta", {}),
                "last_seen": time.time()
            }

        logger.info(f"Service registered: {service_name} at {address}:{port}")
        return Message(
            MessageType.RESPONSE, "RegistrationSuccess", "NameServer",
            {"status": "ok", "service_name": service_name}
        )

    def handle_unregister(self, message: Message) -> Message:
        """Remove um serviço do registro."""
        service_name = message.payload.get("serviceName")

        with self.registry_lock:
            if service_name in self.registry:
                del self.registry[service_name]
                logger.info(f"Service unregistered: {service_name}")
                return Message(
                    MessageType.RESPONSE, "UnregisterSuccess", "NameServer",
                    {"status": "ok"}
                )

        return Message(
            MessageType.RESPONSE, "Error", "NameServer",
            {"error": "Service not found"}
        )

    def handle_query(self, message: Message) -> Message:
        """Busca um serviço específico por nome."""
        service_name = message.payload.get("serviceName")

        with self.registry_lock:
            service = self.registry.get(service_name)

        if service:
            return Message(
                MessageType.RESPONSE, "ServiceFound", "NameServer",
                service
            )
        else:
            return Message(
                MessageType.RESPONSE, "ServiceNotFound", "NameServer",
                {"error": "Service not found"}
            )

    def handle_list(self, message: Message) -> Message:
        """Lista todos os serviços registrados."""
        with self.registry_lock:
            services = dict(self.registry)

        return Message(
            MessageType.RESPONSE, "ServiceList", "NameServer",
            {"services": services}
        )

    def handle_ping(self, message: Message) -> Message:
        """Health check básico."""
        return Message(
            MessageType.RESPONSE, "Pong", "NameServer",
            {"time": time.time()}
        )

def main():
    import argparse
    parser = argparse.ArgumentParser(description="NameServer - Service Discovery")
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind')
    parser.add_argument('--port', type=int, default=8000, help='TCP port')
    parser.add_argument('--http-port', type=int, default=8080, help='HTTP API port')
    args = parser.parse_args()

    server = NameServer(
        host=args.host,
        port=args.port,
        http_port=args.http_port
    )
    server.start()

if __name__ == "__main__":
    main()
