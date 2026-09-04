import { beforeEach, describe, expect, it } from 'vitest'

import type { SessionSummary } from '@/transport/types'

import { mergeSessions, useConsoleStore } from './console'

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
})
