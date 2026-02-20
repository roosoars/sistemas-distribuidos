"""
Handler HTTP para API REST do servidor de votacao.

Responsabilidade unica: processar requisicoes HTTP e delegar para o servidor.
"""

import json
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tally_server import TallyServer


class TallyHTTPHandler(BaseHTTPRequestHandler):
    """Handler HTTP para a API REST do servidor de votacao."""

    server: 'TallyServer'

    def log_message(self, format, *args):
        """Suprime logging padrao do HTTP."""
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
        """Handle CORS preflight requests."""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def do_GET(self):
        """Processa requisicoes GET."""
        parsed = urlparse(self.path)
        path = parsed.path
        server = self.server.tally_server

        if path == '/health':
            self._handle_health(server)
        elif path == '/api/status':
            self._handle_status(server)
        elif path == '/api/results':
            self._handle_results(server)
        elif path == '/api/mutex/stats':
            self._handle_mutex_stats(server)
        else:
            self._send_json({'error': 'Not found'}, 404)

    def do_POST(self):
        """Processa requisicoes POST."""
        parsed = urlparse(self.path)
        path = parsed.path
        server = self.server.tally_server

        data = self._parse_body()
        if data is None:
            return

        if path == '/api/vote':
            self._handle_vote(server, data)
        elif path == '/api/election':
            self._handle_election(server)
        elif path == '/api/server/stop':
            self._handle_stop(server)
        elif path == '/api/server/recover':
            self._handle_recover(server)
        elif path == '/api/reset':
            self._handle_reset(server)
        elif path == '/api/mutex/toggle':
            self._handle_mutex_toggle(server, data)
        else:
            self._send_json({'error': 'Not found'}, 404)

    def _parse_body(self) -> dict:
        """Parse request body as JSON."""
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode() if content_length > 0 else '{}'
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            self._send_json({'error': 'Invalid JSON'}, 400)
            return None

    # GET handlers

    def _handle_health(self, server: 'TallyServer'):
        self._send_json({
            'status': 'ok',
            'server_id': server.server_id,
            'is_leader': server.is_leader,
            'simulated_failure': server.simulated_failure
        })

    def _handle_status(self, server: 'TallyServer'):
        self._send_json({
            'server_id': server.server_id,
            'is_leader': server.is_leader,
            'role': 'failed' if server.simulated_failure else ('leader' if server.is_leader else 'backup'),
            'current_leader': server.current_leader,
            'port': server.port,
            'http_port': server.http_port,
            'simulated_failure': server.simulated_failure
        })

    def _handle_results(self, server: 'TallyServer'):
        results = server.vote_state.get_results()
        results['server_id'] = server.server_id
        results['is_leader'] = server.is_leader
        self._send_json(results)

    def _handle_mutex_stats(self, server: 'TallyServer'):
        self._send_json({
            'server_id': server.server_id,
            'is_leader': server.is_leader,
            'mutex_enabled': server.mutex_enabled,
            'mutex_stats': server.get_mutex_stats(),
            'mutex_manager': {
                'state': server.mutex_manager.state.value,
                'request_timestamp': server.mutex_manager.request_timestamp,
                'peers': list(server.mutex_manager.peers),
                'replies_received': list(server.mutex_manager._replies_received),
                'deferred_replies': list(server.mutex_manager._deferred_replies)
            }
        })

    # POST handlers

    def _handle_vote(self, server: 'TallyServer', data: dict):
        if not server.is_leader:
            self._send_json({
                'error': 'Not leader',
                'leader_id': server.current_leader
            }, 400)
            return

        vote_id = data.get('voteId', f'vote-{time.time()}')
        option = data.get('option')
        voter_id = data.get('voterId', 'anonymous')

        if not option:
            self._send_json({'error': 'Missing option'}, 400)
            return

        result = server.process_vote(vote_id, option, voter_id)
        result['mutex_stats'] = server.get_mutex_stats()

        if result['status'] == 'success':
            self._send_json(result)
        else:
            self._send_json(result, 400)

    def _handle_election(self, server: 'TallyServer'):
        server.start_election()
        self._send_json({'status': 'election_started'})

    def _handle_stop(self, server: 'TallyServer'):
        if server.is_leader:
            server.simulate_failure()
            self._send_json({
                'status': 'failure_simulated',
                'message': 'Server will not send heartbeats'
            })
        else:
            self._send_json({
                'status': 'not_leader',
                'message': 'Only leader can simulate failure'
            }, 400)

    def _handle_recover(self, server: 'TallyServer'):
        if server.simulated_failure:
            server.recover_from_failure()
            self._send_json({
                'status': 'recovering',
                'message': 'Starting election'
            })
        else:
            self._send_json({
                'status': 'not_failed',
                'message': 'Server is not in failed state'
            }, 400)

    def _handle_reset(self, server: 'TallyServer'):
        server.reset()
        self._send_json({
            'status': 'reset',
            'message': 'Votes cleared and election started'
        })

    def _handle_mutex_toggle(self, server: 'TallyServer', data: dict):
        enable = data.get('enable', not server.mutex_enabled)
        server.mutex_enabled = enable
        self._send_json({
            'status': 'ok',
            'mutex_enabled': server.mutex_enabled,
            'message': f"Mutual exclusion {'enabled' if enable else 'disabled'}"
        })
