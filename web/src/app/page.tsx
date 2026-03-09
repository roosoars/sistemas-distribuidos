'use client'

import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  MonitorServerStatus,
  controlMonitorServer,
  getMonitorStatus,
  resetMonitor,
} from '@/lib/nats'

const ROOM_ID = 'default'
const POLL_INTERVAL_MS = 2500

const CANDIDATE_LABELS: Record<string, string> = {
  candidate1: 'Maria Silva',
  candidate2: 'João Santos',
  candidate3: 'Ana Costa',
}

const BAR_COLORS = ['bg-blue-500', 'bg-green-500', 'bg-orange-500', 'bg-rose-500', 'bg-cyan-500']

function totalVotes(counts: Record<string, number>): number {
  return Object.values(counts).reduce((sum, value) => sum + value, 0)
}

type ServerCard = {
  serviceName: string
  serverId: string
  address: string
  httpPort: number
  role: string
  currentLeader: string
  simulatedFailure: boolean
  healthy: boolean
  lastSeen: number
  error?: string
}

function candidateLabel(candidateId: string): string {
  return CANDIDATE_LABELS[candidateId] || candidateId
}

function roleBadge(role: string): string {
  if (role === 'leader' || role === 'primary') return 'bg-emerald-100 text-emerald-800 border-emerald-200'
  if (role === 'failed') return 'bg-red-100 text-red-700 border-red-200'
  return 'bg-slate-100 text-slate-700 border-slate-200'
}

function toServerCard(server: MonitorServerStatus): ServerCard {
  return {
    serviceName: server.service_name,
    serverId: server.server_id,
    address: server.address,
    httpPort: server.http_port,
    role: server.role,
    currentLeader: server.current_leader,
    simulatedFailure: server.simulated_failure,
    healthy: server.healthy,
    lastSeen: server.last_seen,
    error: server.error,
  }
}

export default function Page() {
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [counts, setCounts] = useState<Record<string, number>>({})
  const [servers, setServers] = useState<ServerCard[]>([])
  const [connectionLabel, setConnectionLabel] = useState('CARREGANDO...')
  const [lastUpdate, setLastUpdate] = useState('')
  const [actionLoading, setActionLoading] = useState(false)
  const [serverActionKey, setServerActionKey] = useState('')

  const total = useMemo(() => totalVotes(counts), [counts])
  const candidateRows = useMemo(
    () =>
      Object.entries(counts).sort((a, b) => {
        if (b[1] !== a[1]) return b[1] - a[1]
        return a[0].localeCompare(b[0])
      }),
    [counts],
  )

  const refreshDashboard = useCallback(async () => {
    try {
      const snapshot = await getMonitorStatus(ROOM_ID)
      if (snapshot.status !== 'success') {
        throw new Error(snapshot.error || 'monitor_status_failed')
      }

      const leaderId = snapshot.leader?.server_id || ''
      const cards = (snapshot.servers || []).map(toServerCard)
      cards.sort((a, b) => {
        const aKey = (a.serverId || a.serviceName).toLowerCase()
        const bKey = (b.serverId || b.serviceName).toLowerCase()
        return aKey.localeCompare(bKey)
      })

      setServers(cards)
      setCounts(snapshot.counts || {})
      setConnectionLabel(leaderId ? leaderId.toUpperCase() : 'SEM LÍDER')
      setError('')
      setLastUpdate(new Date().toLocaleTimeString('pt-BR'))
    } catch (refreshError) {
      setConnectionLabel('INDISPONÍVEL')
      setError(refreshError instanceof Error ? refreshError.message : 'Falha ao atualizar painel')
    }
  }, [])

  useEffect(() => {
    refreshDashboard()
    const timer = setInterval(refreshDashboard, POLL_INTERVAL_MS)
    return () => clearInterval(timer)
  }, [refreshDashboard])

  const triggerReset = useCallback(async () => {
    setActionLoading(true)
    setError('')
    setNotice('')
    try {
      const response = await resetMonitor(ROOM_ID)
      if (response.status !== 'success') {
        throw new Error(response.error || 'monitor_reset_failed')
      }

      const resetRequested = response.monitor_reset?.requested || 0
      const resetSuccess = response.monitor_reset?.success_count || 0
      const natsAdminStatus = String(response.nats_admin?.status || 'unknown')
      setNotice(
        `Reinício solicitado via NATS. Servidores atualizados: ${resetSuccess}/${resetRequested}. Admin: ${natsAdminStatus}.`,
      )
      await refreshDashboard()
    } catch (actionError) {
      setError(actionError instanceof Error ? actionError.message : 'Falha ao reiniciar votação')
    } finally {
      setActionLoading(false)
    }
  }, [refreshDashboard])

  const triggerServerAction = useCallback(
    async (server: ServerCard, action: 'stop' | 'recover') => {
      const actionKey = `${server.serviceName}:${action}`
      setServerActionKey(actionKey)
      setError('')
      setNotice('')
      try {
        const response = await controlMonitorServer({
          room_id: ROOM_ID,
          service_name: server.serviceName,
          server_id: server.serverId,
          action,
        })
        if (response.status !== 'success') {
          throw new Error(response.error || `monitor_server_${action}_failed`)
        }
        const actionLabel = action === 'stop' ? 'desativação' : 'reativação'
        setNotice(`Solicitação de ${actionLabel} enviada para ${server.serverId.toUpperCase()} via NATS.`)
        await refreshDashboard()
      } catch (actionError) {
        setError(actionError instanceof Error ? actionError.message : 'Falha ao controlar servidor')
      } finally {
        setServerActionKey('')
      }
    },
    [refreshDashboard],
  )

  return (
    <div className="min-h-screen bg-slate-50 text-slate-900">
      <main className="mx-auto max-w-6xl px-6 py-10 space-y-8">
        <section className="rounded-xl border border-slate-200 bg-white p-6">
          <h1 className="text-2xl font-semibold">SISTEMA DE VOTAÇÃO</h1>
          <p className="mt-2 text-sm text-slate-600">CONEXÃO: {connectionLabel}</p>
          <p className="text-xs text-slate-500">Última atualização: {lastUpdate || '-'}</p>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex items-center justify-between gap-4">
            <div>
              <h2 className="text-lg font-medium">Ações operacionais</h2>
            </div>
            <button
              onClick={triggerReset}
              disabled={actionLoading}
              className="rounded-lg bg-slate-900 px-5 py-3 text-sm font-medium text-white disabled:opacity-60"
            >
              {actionLoading ? 'Reiniciando...' : 'Reiniciar votação'}
            </button>
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-medium">Servidores registrados</h2>
            <span className="text-sm text-slate-600">Total: {servers.length}</span>
          </div>
          <div className="mt-4 grid gap-3 md:grid-cols-2 lg:grid-cols-3">
            {servers.length === 0 && <p className="text-sm text-slate-600">Nenhum servidor encontrado.</p>}
            {servers.map((server) => (
              <article key={server.serviceName} className="rounded-lg border border-slate-200 p-4">
                <div className="flex items-start justify-between">
                  <div>
                    <p className="text-sm text-slate-500">{server.serviceName}</p>
                    <p className="text-base font-medium">{server.serverId.toUpperCase()}</p>
                  </div>
                  <span className={`rounded-full border px-2 py-0.5 text-xs ${roleBadge(server.role)}`}>
                    {server.role}
                  </span>
                </div>
                <div className="mt-3 space-y-1 text-sm text-slate-600">
                  <p>
                    Endpoint: {server.address}:{server.httpPort}
                  </p>
                  <p>Líder atual: {server.currentLeader || '-'}</p>
                  <p>Falha simulada: {server.simulatedFailure ? 'sim' : 'não'}</p>
                  <p>Status: {server.healthy ? 'online' : `offline (${server.error || 'sem resposta'})`}</p>
                </div>
                <div className="mt-4">
                  {server.simulatedFailure ? (
                    <button
                      onClick={() => triggerServerAction(server, 'recover')}
                      disabled={Boolean(serverActionKey) || actionLoading}
                      className="rounded-lg border border-emerald-300 bg-emerald-50 px-3 py-2 text-sm font-medium text-emerald-700 disabled:opacity-60"
                    >
                      {serverActionKey === `${server.serviceName}:recover`
                        ? 'Reativando...'
                        : 'Reativar servidor'}
                    </button>
                  ) : (
                    <button
                      onClick={() => triggerServerAction(server, 'stop')}
                      disabled={Boolean(serverActionKey) || actionLoading}
                      className="rounded-lg border border-red-300 bg-red-50 px-3 py-2 text-sm font-medium text-red-700 disabled:opacity-60"
                    >
                      {serverActionKey === `${server.serviceName}:stop`
                        ? 'Desativando...'
                        : 'Derrubar servidor'}
                    </button>
                  )}
                </div>
              </article>
            ))}
          </div>
        </section>

        <section className="rounded-xl border border-slate-200 bg-white p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-medium">Resultados</h2>
            <span className="text-sm text-slate-600">Total: {total}</span>
          </div>
          <div className="mt-4 space-y-3">
            {candidateRows.length === 0 && <p className="text-sm text-slate-600">Sem votos no momento.</p>}
            {candidateRows.map(([candidateId, count], index) => {
              const percentage = total > 0 ? Math.round((count / total) * 100) : 0
              const color = BAR_COLORS[index % BAR_COLORS.length]
              return (
                <div key={candidateId} className="rounded-lg border border-slate-200 p-3">
                  <div className="mb-1 flex items-center justify-between text-sm">
                    <span>{candidateLabel(candidateId)}</span>
                    <span>
                      {count} ({percentage}%)
                    </span>
                  </div>
                  <div className="h-2 rounded bg-slate-100">
                    <div className={`h-2 rounded ${color}`} style={{ width: `${percentage}%` }} />
                  </div>
                </div>
              )
            })}
          </div>
        </section>

        {notice && (
          <section className="rounded-xl border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-700">
            {notice}
          </section>
        )}

        {error && (
          <section className="rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">{error}</section>
        )}
      </main>
    </div>
  )
}
