import { beforeEach, describe, expect, it } from 'vitest'

import type { RuntimeSnapshot, SessionSummary } from '@/transport/types'

import {
  commandRuntimeSettled,
  mergeSessions,
  prepareDeferredOutputs,
  reconcileRuntimeSnapshot,
  useConsoleStore,
} from './console'

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

  it('keeps an opened detail when a concurrent list snapshot omits it', () => {
    const opened = session('session-opened', '2026-09-05T01:00:00Z')
    state.sessions.push(opened)
    state.runtimes[opened.id] = {
      sessionId: opened.id,
      runtimeRevision: 12,
      phase: 'IDLE',
      timeline: [],
    } as RuntimeSnapshot

    mergeSessions([], true)

    expect(state.sessions.map((item) => item.id)).toEqual(['session-opened'])
    expect(state.runtimes[opened.id]?.runtimeRevision).toBe(12)
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

  it('clears the history-only fallback after a live snapshot succeeds at the same revision', () => {
    const current = {
      sessionId: 'session-current',
      unavailable: { code: 'SESSION_NOT_FOUND', message: 'not open' },
      runtimeRevision: 0,
      phase: 'IDLE',
      timeline: [],
    } as RuntimeSnapshot
    const incoming = {
      sessionId: 'session-current',
      runtimeRevision: 0,
      phase: 'IDLE',
      timeline: [],
    } as RuntimeSnapshot

    const reconciled = reconcileRuntimeSnapshot(current, incoming, true)

    expect(reconciled.unavailable).toBeUndefined()
  })
})

describe('detail reconciliation', () => {
  it('marks historical final output for explicit on-demand loading', () => {
    const runtime = {
      sessionId: 'session-current',
      runtimeRevision: 12,
      phase: 'IDLE',
      timeline: [
        {
          id: 'command-1',
          type: 'command',
          createdAt: '2026-09-05T01:00:00Z',
          command: 'fixture',
          cwdDisplay: '',
          status: 'COMPLETED',
          elapsed: '1s',
          output: {
            itemId: 'command-1',
            revision: 0,
            text: '',
            byteLength: 0,
            isFinal: true,
            authority: 'AUTHORITATIVE_FINAL',
            hasGap: false,
          },
        },
      ],
      outputCursors: [
        { itemId: 'command-1', revision: 12, byteLength: 2048, isFinal: true },
      ],
    } as RuntimeSnapshot

    prepareDeferredOutputs(runtime)

    const item = runtime.timeline[0]
    expect(item?.type).toBe('command')
    if (item?.type !== 'command') throw new Error('expected command')
    expect(item.output).toMatchObject({
      revision: 12,
      byteLength: 2048,
      isFinal: false,
      loadState: 'DEFERRED',
    })
  })

  it('keeps received command output when a terminal status entry replaces the timeline item', () => {
    const receivedOutput = {
      itemId: 'item-1',
      revision: 3,
      text: 'line1\nline2\n',
      byteLength: 14,
      isFinal: true,
      authority: 'AUTHORITATIVE_FINAL',
      hasGap: false,
    } as const
    const current = {
      sessionId: 'session-output',
      runtimeRevision: 10,
      phase: 'RUNNING',
      timeline: [
        {
          id: 'item-1',
          type: 'command',
          createdAt: '2026-09-05T01:00:00Z',
          command: 'echo',
          cwdDisplay: '',
          status: 'RUNNING',
          elapsed: '',
          output: { ...receivedOutput },
        },
      ],
    } as RuntimeSnapshot
    // 命令结束后刷新快照:历史条目重新映射,输出是未取得的空占位。
    const incoming = {
      ...current,
      runtimeRevision: 11,
      timeline: [
        {
          id: 'item-1',
          type: 'command',
          createdAt: '2026-09-05T01:00:00Z',
          command: 'echo',
          cwdDisplay: '',
          status: 'COMPLETED',
          elapsed: '2s',
          output: {
            itemId: 'item-1',
            revision: 0,
            text: '',
            byteLength: 0,
            isFinal: false,
            authority: 'LIVE_PREVIEW',
            hasGap: false,
          },
        },
      ],
    } as RuntimeSnapshot

    const merged = reconcileRuntimeSnapshot(current, incoming, false)
    const item = merged.timeline.find((entry) => entry.id === 'item-1')
    expect(item).toMatchObject({
      type: 'command',
      status: 'COMPLETED',
      elapsed: '2s',
      output: receivedOutput,
    })
  })

  it('keeps interrupt reconciliation active until the authoritative turn is idle', () => {
    const context = { operation: 'INTERRUPT' as const, baselineRevision: 12 }
    expect(
      commandRuntimeSettled(context, {
        runtimeRevision: 13,
        phase: 'RUNNING',
        activeTurnId: 'turn-current',
      } as RuntimeSnapshot),
    ).toBe(false)
    expect(
      commandRuntimeSettled(context, {
        runtimeRevision: 14,
        phase: 'IDLE',
      } as RuntimeSnapshot),
    ).toBe(true)
  })
})
