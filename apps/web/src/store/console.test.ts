import { beforeEach, describe, expect, it } from 'vitest'

import type { RuntimeSnapshot, SessionSummary } from '@/transport/types'

import { mergeSessions, reconcileRuntimeSnapshot, useConsoleStore } from './console'

function session(id: string, updatedAt: string): SessionSummary {
  return {
    id,
    nativeSessionId: `native-${id}`,
    title: id,
    projectDisplay: 'workspace',
    branch: 'main',
    updatedAt,
    deviceId: `device-${id}`,
    deviceConnection: 'ONLINE',
    controlMode: 'LIMITED_CONTROL',
    compatibility: 'VERIFIED',
    phase: 'IDLE',
    lastOutcome: 'COMPLETED',
    attentionCount: 0,
    queueStatus: 'EMPTY',
    pinned: false,
    muted: false,
    archived: false,
  }
}

describe('session list snapshots', () => {
  const { state } = useConsoleStore()

  beforeEach(() => {
    state.sessions.splice(0)
    for (const sessionId of Object.keys(state.runtimes)) delete state.runtimes[sessionId]
    for (const sessionId of Object.keys(state.historyCursors)) delete state.historyCursors[sessionId]
    for (const sessionId of Object.keys(state.historyLoading)) delete state.historyLoading[sessionId]
  })

  it('removes sessions omitted from an authoritative WebSocket snapshot', () => {
    state.sessions.push(
      session('session-current', '2026-09-05T02:00:00Z'),
      session('session-stale', '2026-09-05T01:00:00Z'),
    )

    mergeSessions([session('session-current', '2026-09-05T03:00:00Z')], true)

    expect(state.sessions.map((item) => item.id)).toEqual(['session-current'])
  })

  it('keeps prior sessions when merging a non-authoritative HTTP page', () => {
    state.sessions.push(session('session-prior', '2026-09-05T01:00:00Z'))

    mergeSessions([session('session-page', '2026-09-05T02:00:00Z')], false)

    expect(state.sessions.map((item) => item.id)).toEqual(['session-page', 'session-prior'])
  })

  it('does not let a list summary overwrite an opened detail runtime', () => {
    const current = session('session-current', '2026-09-05T01:00:00Z')
    state.sessions.push(current)
    state.runtimes[current.id] = {
      sessionId: current.id,
      runtimeRevision: 12,
      phase: 'RUNNING',
      activeTurnId: 'turn-current',
      timeline: [],
    } as RuntimeSnapshot

    mergeSessions([session('session-current', '2026-09-05T03:00:00Z')], false)
    expect(state.runtimes[current.id]?.phase).toBe('RUNNING')
    expect(state.runtimes[current.id]?.activeTurnId).toBe('turn-current')
  })

  it.each([11, 12])(
    'does not let a revision %s runtime snapshot roll back current detail state',
    (incomingRevision) => {
      const current = {
        sessionId: 'session-current',
        runtimeRevision: 12,
        phase: 'RUNNING',
        activeTurnId: 'turn-current',
        timeline: [],
      } as RuntimeSnapshot
      const stale = {
        sessionId: 'session-current',
        runtimeRevision: incomingRevision,
        phase: 'IDLE',
        timeline: [],
      } as RuntimeSnapshot

      const reconciled = reconcileRuntimeSnapshot(current, stale, false)

      expect(reconciled.runtimeRevision).toBe(12)
      expect(reconciled.phase).toBe('RUNNING')
      expect(reconciled.activeTurnId).toBe('turn-current')
    },
  )

  it('accepts a strictly newer authoritative runtime snapshot', () => {
    const current = {
      sessionId: 'session-current',
      runtimeRevision: 12,
      phase: 'RUNNING',
      activeTurnId: 'turn-current',
      timeline: [],
    } as RuntimeSnapshot
    const incoming = {
      sessionId: 'session-current',
      runtimeRevision: 13,
      phase: 'IDLE',
      timeline: [],
    } as RuntimeSnapshot

    const reconciled = reconcileRuntimeSnapshot(current, incoming, false)

    expect(reconciled.runtimeRevision).toBe(13)
    expect(reconciled.phase).toBe('IDLE')
    expect(reconciled.activeTurnId).toBeUndefined()
  })
})
