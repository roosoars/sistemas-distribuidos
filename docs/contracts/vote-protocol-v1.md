# Contratos Ativos v1

## Submit de voto
Subject: `svc.vote.submit.<room>`

Payload:
```json
{
  "schema": "vote.submit.v1",
  "vote_id": "uuid",
  "room_id": "default",
  "candidate_id": "candidate1",
  "voter_id": "device-id",
  "client_ts": "2026-02-22T18:10:00Z",
  "trace_id": "uuid"
}
```

## Resultado de voto
Subject: reply do submit/status

Payload:
```json
{
  "schema": "vote.result.v1",
  "status": "success|duplicate|pending|error",
  "vote_id": "uuid",
  "room_id": "default",
  "candidate_id": "candidate1",
  "total_votes": 123,
  "counts": {"candidate1": 70, "candidate2": 53},
  "idempotent_replay": false,
  "trace_id": "uuid"
}
```

## Monitor status
Subject: `svc.vote.monitor.status.<room>`

Payload de resposta:
```json
{
  "schema": "vote.monitor.status.v1",
  "status": "success|error",
  "room_id": "default",
  "counts": {"candidate1": 10},
  "total_votes": 10,
  "leader": {"server_id": "s1"},
  "servers": []
}
```

## Monitor reset
Subject: `svc.vote.monitor.reset.<room>`

## Monitor server control
Subject: `svc.vote.monitor.server.<room>`

Payload de request:
```json
{
  "schema": "vote.monitor.server.v1",
  "room_id": "default",
  "service_name": "tally-s1-default",
  "server_id": "s1",
  "action": "stop|recover",
  "trace_id": "uuid"
}
```

## APIs Removidas
1. `/api/vote`
2. `/api/results`
3. `/api/mutex/stats`
4. `/api/mutex/toggle`
5. `/api/election`
6. comandos TCP legados de nameserver (`QueryService`, `ListServices`, `Ping`)
