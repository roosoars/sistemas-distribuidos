
'use client'

import { useState, useEffect, useCallback } from 'react'
interface LeaderInfo {
  service_name: string
  address: string
  port: number
  http_port: number
  server_id: string
}
interface ServiceInfo {
  address: string
  port: number
  meta: {
    role: string
    serverId: string
    http_port: number
  }
  last_seen: number
}
interface ServerStatus {
  server_id: string
  is_leader: boolean
  role: string
  simulated_failure: boolean
}
interface VoteResults {
  counts: Record<string, number>
  total_votes: number
}
const NAMESERVER_URL = process.env.NEXT_PUBLIC_NAMESERVER_URL || 'http://localhost:9080'

export default function Dashboard() {
  const [leader, setLeader] = useState<LeaderInfo | null>(null)
  const [services, setServices] = useState<Record<string, ServiceInfo>>({})
  const [serverStatuses, setServerStatuses] = useState<Record<string, ServerStatus>>({})
  const [results, setResults] = useState<VoteResults>({ counts: {}, total_votes: 0 })
  const [loading, setLoading] = useState(false)
  const [lastUpdate, setLastUpdate] = useState<string>('')
  const [error, setError] = useState<string>('')
  const fetchLeader = useCallback(async () => {
    try {
      const res = await fetch(`${NAMESERVER_URL}/api/leader`, {
        signal: AbortSignal.timeout(3000)
      })
      if (res.ok) {
        const data = await res.json()
        if (data.found) {
          setLeader(data.leader)
          setError('')
        } else {
          setLeader(null)
        }
      }
    } catch (e) {
      setError('NameServer indisponivel')
      setLeader(null)
    }
  }, [])
  const fetchServices = useCallback(async () => {
    try {
      const res = await fetch(`${NAMESERVER_URL}/api/services`, {
        signal: AbortSignal.timeout(3000)
      })
      if (res.ok) {
        const data = await res.json()
        const servicesData = data.services || {}
        setServices(servicesData)
        const statuses: Record<string, ServerStatus> = {}
        for (const [, info] of Object.entries(servicesData) as [string, ServiceInfo][]) {
          try {
            const statusRes = await fetch(`http://${info.address}:${info.meta?.http_port}/api/status`, {
              signal: AbortSignal.timeout(2000)
            })
            if (statusRes.ok) {
              const statusData = await statusRes.json()
              statuses[info.meta?.serverId] = statusData
            }
          } catch {
          }
        }
        setServerStatuses(statuses)
      }
    } catch {
    }
  }, [])
  const fetchResults = useCallback(async () => {
    if (!leader) return

    try {
      const httpPort = leader.http_port || (leader.port + 1000)
      const url = `http://${leader.address}:${httpPort}/api/results`

      const res = await fetch(url, {
        signal: AbortSignal.timeout(3000)
      })
      if (res.ok) {
        const data = await res.json()
        setResults({
          counts: data.counts || {},
          total_votes: data.totalVotes || data.total_votes || 0
        })
      }
    } catch {
      fetchLeader()
    }
  }, [leader, fetchLeader])
  const refreshAll = useCallback(async () => {
    setLoading(true)
    await Promise.all([fetchLeader(), fetchServices()])
    setLastUpdate(new Date().toLocaleTimeString('pt-BR'))
    setLoading(false)
  }, [fetchLeader, fetchServices])
  useEffect(() => {
    refreshAll()
    const interval = setInterval(refreshAll, 1500)
    return () => clearInterval(interval)
  }, [refreshAll])
  useEffect(() => {
    if (leader) {
      fetchResults()
      const interval = setInterval(fetchResults, 500)
      return () => clearInterval(interval)
    }
  }, [leader, fetchResults])
  const resetSystem = async () => {
    if (!confirm('Tem certeza que deseja reiniciar o sistema e zerar todos os votos?')) {
      return
    }

    for (const [, info] of Object.entries(services) as [string, ServiceInfo][]) {
      try {
        await fetch(`http://${info.address}:${info.meta?.http_port}/api/reset`, {
          method: 'POST'
        })
      } catch {
      }
    }

    setTimeout(refreshAll, 3000)
    setResults({ counts: {}, total_votes: 0 })
  }
  const stopServer = async (address: string, httpPort: number) => {
    try {
      await fetch(`http://${address}:${httpPort}/api/server/stop`, {
        method: 'POST'
      })
      setTimeout(refreshAll, 1000)
    } catch {
    }
  }
  const recoverServer = async (address: string, httpPort: number) => {
    try {
      await fetch(`http://${address}:${httpPort}/api/server/recover`, {
        method: 'POST'
      })
      setTimeout(refreshAll, 2000)
    } catch {
    }
  }
  const servers = Object.entries(services).map(([name, info]) => {
    const serverId = info.meta?.serverId
    const status = serverStatuses[serverId]
    return {
      name,
      ...info,
      isLeader: info.meta?.role === 'primary',
      isOnline: (Date.now() / 1000 - info.last_seen) < 30,
      isFailed: status?.simulated_failure || false
    }
  })

  const onlineCount = servers.filter(s => s.isOnline).length

  return (
    <div className="min-h-screen bg-white">
      <header className="border-b border-gray-200">
        <div className="max-w-5xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-6">
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${onlineCount > 0 ? 'bg-green-500' : 'bg-red-500'}`} />
              <span className="text-sm text-gray-600">{onlineCount} servidores</span>
            </div>
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${leader ? 'bg-blue-500' : 'bg-gray-300'}`} />
              <span className="text-sm text-gray-600">
                Lider: {leader ? leader.server_id?.toUpperCase() : 'Nenhum'}
              </span>
            </div>
            {loading && (
              <div className="flex items-center gap-2">
                <div className="w-3 h-3 border-2 border-gray-300 border-t-blue-500 rounded-full animate-spin" />
                <span className="text-xs text-gray-400">Sincronizando</span>
              </div>
            )}
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs text-gray-400">{lastUpdate}</span>
            <button
              onClick={resetSystem}
              className="px-3 py-1.5 text-xs border border-red-200 text-red-600 rounded hover:bg-red-50"
            >
              Reiniciar
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-5xl mx-auto px-6 py-8 space-y-8">
        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
            {error}
          </div>
        )}
        <section>
          <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wide mb-4">NameServer</h2>
          <div className="border border-gray-200 rounded-lg p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="font-medium text-gray-900">Directory Service</p>
                <p className="text-sm text-gray-500">{NAMESERVER_URL}</p>
              </div>
              <div className={`px-2 py-1 rounded text-xs ${error ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'}`}>
                {error ? 'Offline' : 'Online'}
              </div>
            </div>
          </div>
        </section>
        <section>
          <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wide mb-4">Servidores Registrados</h2>
          {servers.length === 0 ? (
            <p className="text-gray-400 text-sm">Nenhum servidor registrado</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {servers.map(server => (
                <div
                  key={server.name}
                  className={`border rounded-lg p-4 ${
                    server.isFailed
                      ? 'border-red-300 bg-red-50'
                      : server.isLeader
                        ? 'border-blue-300 bg-blue-50'
                        : server.isOnline
                          ? 'border-gray-200'
                          : 'border-gray-200 bg-gray-50'
                  }`}
                >
                  <div className="flex items-center justify-between mb-3">
                    <div className="flex items-center gap-2">
                      <div className={`w-2 h-2 rounded-full ${
                        server.isFailed ? 'bg-red-500' : server.isOnline ? 'bg-green-500' : 'bg-gray-400'
                      }`} />
                      <span className="font-medium text-gray-900">
                        {server.meta?.serverId?.toUpperCase() || server.name}
                      </span>
                    </div>
                    {server.isFailed ? (
                      <span className="text-xs px-2 py-0.5 bg-red-100 text-red-700 rounded">
                        FALHA
                      </span>
                    ) : server.isLeader && (
                      <span className="text-xs px-2 py-0.5 bg-blue-100 text-blue-700 rounded">
                        LIDER
                      </span>
                    )}
                  </div>

                  <div className="text-xs text-gray-500 space-y-1 mb-3">
                    <p>{server.address}:{server.port}</p>
                    <p>HTTP: {server.meta?.http_port}</p>
                    <p>Papel: {server.isFailed ? 'Falha Simulada' : server.meta?.role === 'primary' ? 'Lider' : 'Backup'}</p>
                  </div>

                  <div className="flex gap-2">
                    {server.isFailed ? (
                      <button
                        onClick={() => recoverServer(server.address, server.meta?.http_port)}
                        className="flex-1 text-xs py-1.5 border border-green-300 text-green-600 rounded hover:bg-green-50"
                      >
                        Recuperar
                      </button>
                    ) : (
                      <button
                        onClick={() => stopServer(server.address, server.meta?.http_port)}
                        disabled={!server.isOnline || !server.isLeader}
                        className="flex-1 text-xs py-1.5 border border-red-200 text-red-600 rounded hover:bg-red-50 disabled:opacity-30 disabled:cursor-not-allowed"
                      >
                        Parar
                      </button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
        <section>
          <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wide mb-4">Resultados em Tempo Real</h2>
          <div className="border border-gray-200 rounded-lg p-6">
            <div className="text-center mb-6">
              <span className="text-4xl font-bold text-gray-900">{results.total_votes}</span>
              <span className="text-lg text-gray-500 ml-2">votos</span>
            </div>

            {Object.entries(results.counts).length > 0 ? (
              <div className="space-y-3">
                {Object.entries(results.counts).map(([option, count]) => {
                  const percentage = results.total_votes > 0
                    ? Math.round((count / results.total_votes) * 100)
                    : 0
                  return (
                    <div key={option}>
                      <div className="flex justify-between text-sm mb-1">
                        <span className="font-medium">{option}</span>
                        <span className="text-gray-500">{count} ({percentage}%)</span>
                      </div>
                      <div className="h-3 bg-gray-100 rounded overflow-hidden">
                        <div
                          className="h-full bg-blue-500 transition-all duration-500"
                          style={{ width: `${percentage}%` }}
                        />
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <p className="text-sm text-gray-400 text-center py-8">Aguardando votos do app iOS...</p>
            )}
          </div>
        </section>
      </main>
    </div>
  )
}
