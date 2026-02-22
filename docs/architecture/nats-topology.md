# NATS-Only Topologia

## Visao Geral
A arquitetura final usa somente NATS, JetStream e KV.

Componentes ativos:
1. `nats-1`, `nats-2`, `nats-3` (cluster + JetStream + WebSocket)
2. `vote-api` (request/reply para clientes e painel)
3. `vote-processor` (consumo de comandos de voto)
4. `vote-admin` (reset e controle operacional)
5. `monitor-s1`, `monitor-s2`, `monitor-s3` (eleicao e estado dos servidores do painel)
6. `web` (painel de acompanhamento)

Componentes removidos:
1. `nameserver`
2. `tally_server` legado (`s1`, `s2`, `s3` antigos)
3. stack de clocks/distributed legado

## Subjects NATS
### Votacao
1. `svc.vote.submit.<room>`
2. `svc.vote.status.<room>`
3. `svc.vote.results.<room>`
4. `vote.cmd.<room>`
5. `vote.event.accepted.<room>`
6. `vote.result.<room>.<vote_id>`

### Monitoracao e Operacao
1. `svc.vote.monitor.status.<room>`
2. `svc.vote.monitor.reset.<room>`
3. `svc.vote.monitor.server.<room>`
4. `svc.vote.admin.reset.<room>`
5. `svc.vote.admin.server.<room>`

## Streams e Buckets
1. `VOTE_CMD` (stream)
2. `VOTE_EVT` (stream)
3. `KV_VOTE_STATE`
4. `KV_VOTE_COUNT`
5. `KV_CONTROL`
6. `KV_MONITOR_STATE`
7. `KV_MONITOR_LEADER`
8. `KV_MONITOR_CONTROL`

## Invariantes Criticos
1. Idempotencia por `vote_id` via `Nats-Msg-Id` e `KV_VOTE_STATE`.
2. Lider de monitoracao deve existir quando houver ao menos um monitor ativo sem falha simulada.
3. `N/A` de lider apenas quando `s1`, `s2` e `s3` estiverem simultaneamente indisponiveis/falha simulada.
