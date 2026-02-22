# Migracao para NATS-Only

## Estado Final
1. Votacao e monitoracao operam somente por NATS.
2. iOS opera somente por NATS request/reply.
3. Web opera como painel (sem voto pela web).
4. Nomeacao/eleicao legada por TCP/HTTP foi removida.

## Flags e Compatibilidade
1. Nao existe mais fallback legado por socket no iOS.
2. Nao existe mais `nameserver`/`tally_server` legado no compose final.
3. Monitoracao usa `monitor-s1/s2/s3` em NATS/KV.

## Sequencia de Cutover
1. Subir cluster NATS e servicos `vote-*`.
2. Subir `monitor-s1/s2/s3`.
3. Validar `svc.vote.monitor.status.default` com lider definido.
4. Subir painel web.
5. Validar operacoes: reset, stop e recover.

## Rollback
1. Reverter por commit (`git revert`) da fase afetada.
2. Subir novamente com `docker compose up -d --build`.
