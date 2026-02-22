# Runbook Operacional

## Subida do Ambiente
```bash
cd /Users/rsoares/Desktop/VOTE-5
docker compose up -d --build
```

## Health Basico
```bash
docker compose ps
docker compose logs --tail=120 vote-api vote-processor vote-admin monitor-s1 monitor-s2 monitor-s3 web
```

## Verificar Lider via NATS
```bash
docker compose exec -T vote-api python - <<'PY'
import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    await nc.connect(servers=["nats://nats-1:4222"], user="app_backend", password="app_backend_dev")
    msg = await nc.request("svc.vote.monitor.status.default", b"{}", timeout=3)
    print(msg.data.decode())
    await nc.close()

asyncio.run(main())
PY
```

## Operacoes do Painel
1. Reiniciar votacao: usa `svc.vote.monitor.reset.<room>`.
2. Derrubar servidor: usa `svc.vote.monitor.server.<room>` com `action=stop`.
3. Reativar servidor: usa `svc.vote.monitor.server.<room>` com `action=recover`.

## Incidentes Comuns
1. Lider ausente com servidores ativos: verificar `KV_MONITOR_LEADER` e logs dos `monitor-s*`.
2. Painel sem atualizar: validar credenciais `app_web` no NATS WebSocket.
3. Reset sem efeito: validar `svc.vote.admin.reset.*` no `vote-admin`.

## Reiniciar Tudo
```bash
docker compose down
docker compose up -d --build
```
