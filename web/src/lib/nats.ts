'use client'

import { connect, JSONCodec, NatsConnection } from 'nats.ws'

const jc = JSONCodec()
let ncPromise: Promise<NatsConnection> | null = null

export async function getNatsConnection(): Promise<NatsConnection> {
  if (!ncPromise) {
    const servers = (process.env.NEXT_PUBLIC_NATS_WS_URL || 'ws://localhost:9222')
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean)

    ncPromise = connect({
      servers,
      user: process.env.NEXT_PUBLIC_NATS_USER || 'app_web',
      pass: process.env.NEXT_PUBLIC_NATS_PASSWORD || 'app_web_dev',
      name: 'vote-web-monitor-client',
      timeout: 3000,
      maxReconnectAttempts: -1,
      reconnectTimeWait: 1000,
    })
  }

  return ncPromise
}

export type MonitorServerStatus = {
  service_name: string
  server_id: string
  address: string
  port: number
  http_port: number
  role: string
  current_leader: string
  simulated_failure: boolean
  healthy: boolean
  last_seen: number
  error?: string
}

export type MonitorStatus = {
  schema: 'vote.monitor.status.v1'
  status: 'success' | 'error'
  room_id: string
  trace_id?: string
  counts?: Record<string, number>
  total_votes?: number
  leader?: {
    service_name: string
    address: string
    port: number
    http_port?: number
    server_id?: string
    last_seen?: number
  }
  servers?: MonitorServerStatus[]
  error?: string
}

export type MonitorReset = {
  schema: 'vote.monitor.reset.v1'
  status: 'success' | 'error'
  room_id?: string
  trace_id?: string
  nats_admin?: {
    status?: string
    [key: string]: unknown
  }
  monitor_reset?: {
    requested: number
    success_count: number
    results: Array<Record<string, unknown>>
  }
  error?: string
}

export type MonitorServerActionRequest = {
  schema: 'vote.monitor.server.v1'
  room_id: string
  service_name?: string
  server_id?: string
  action: 'stop' | 'recover'
  trace_id: string
}

export type MonitorServerActionResponse = {
  schema: 'vote.monitor.server.v1'
  status: 'success' | 'error'
  room_id?: string
  trace_id?: string
  action?: 'stop' | 'recover'
  service_name?: string
  server_id?: string
  result?: Record<string, unknown>
  server?: MonitorServerStatus
  error?: string
}

export async function getMonitorStatus(roomId: string): Promise<MonitorStatus> {
  const nc = await getNatsConnection()
  const msg = await nc.request(
    `svc.vote.monitor.status.${roomId}`,
    jc.encode({
      schema: 'vote.monitor.status.v1',
      room_id: roomId,
      trace_id: crypto.randomUUID(),
    }),
    { timeout: 3500 },
  )
  return jc.decode(msg.data) as MonitorStatus
}

export async function resetMonitor(roomId: string): Promise<MonitorReset> {
  const nc = await getNatsConnection()
  const msg = await nc.request(
    `svc.vote.monitor.reset.${roomId}`,
    jc.encode({
      schema: 'vote.monitor.reset.v1',
      room_id: roomId,
      trace_id: crypto.randomUUID(),
    }),
    { timeout: 5000 },
  )
  return jc.decode(msg.data) as MonitorReset
}

export async function controlMonitorServer(
  req: Omit<MonitorServerActionRequest, 'schema' | 'trace_id'>,
): Promise<MonitorServerActionResponse> {
  const nc = await getNatsConnection()
  const msg = await nc.request(
    `svc.vote.monitor.server.${req.room_id}`,
    jc.encode({
      schema: 'vote.monitor.server.v1',
      room_id: req.room_id,
      service_name: req.service_name,
      server_id: req.server_id,
      action: req.action,
      trace_id: crypto.randomUUID(),
    }),
    { timeout: 5000 },
  )
  return jc.decode(msg.data) as MonitorServerActionResponse
}
