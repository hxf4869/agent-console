import { describe, expect, it } from 'vitest'

import { useConsoleStore } from './console'
import { mergeSessions } from './console'
import { agentKindDisplay, capabilitySourceLabel } from '@/lib/presentation'
import type { SessionSummary } from '@/transport/types'

function session(overrides: Partial<SessionSummary> & { id: string }): SessionSummary {
  return {
    nativeSessionId: `native-${overrides.id}`,
    title: overrides.id,
    projectDisplay: 'workspace',
    branch: 'main',
    updatedAt: '2026-09-05T02:00:00Z',
    deviceId: 'device-1',
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
    ...overrides,
  }
}

describe('dual-agent session identity (ZC-02)', () => {
  it('keeps same-native-id sessions of different agent kinds distinct', () => {
    const { state } = useConsoleStore()
    mergeSessions(
      [session({ id: 's-codex', nativeSessionId: 'dup-1', agentKind: 'CODEX_DESKTOP' })],
      true,
    )
    mergeSessions(
      [session({ id: 's-zcode', nativeSessionId: 'dup-1', agentKind: 'ZCODE_DESKTOP' })],
      false,
    )
    const dup = state.sessions.filter(
      (item) => item.deviceId === 'device-1' && item.nativeSessionId === 'dup-1',
    )
    expect(dup).toHaveLength(2)
    expect(dup.map((item) => item.agentKind).sort()).toEqual(['CODEX_DESKTOP', 'ZCODE_DESKTOP'])
  })

  it('updates the matching agent-kind session in place, not the other agent', () => {
    const { state } = useConsoleStore()
    mergeSessions(
      [session({ id: 's-codex', nativeSessionId: 'dup-2', agentKind: 'CODEX_DESKTOP' })],
      false,
    )
    mergeSessions(
      [session({ id: 's-zcode', nativeSessionId: 'dup-2', agentKind: 'ZCODE_DESKTOP' })],
      false,
    )
    // ZCode 摘要增量(同 ID)只应更新 ZCode 条目。
    mergeSessions(
      [
        session({
          id: 's-zcode',
          nativeSessionId: 'dup-2',
          agentKind: 'ZCODE_DESKTOP',
          title: 'ZCode 更新后的标题',
        }),
      ],
      false,
    )
    const codex = state.sessions.find((item) => item.id === 's-codex')
    const zcode = state.sessions.find((item) => item.id === 's-zcode')
    expect(codex?.title).not.toBe('ZCode 更新后的标题')
    expect(zcode?.title).toBe('ZCode 更新后的标题')
  })
})

describe('capability source presentation (ZC-02)', () => {
  it('labels zcode sessions as official hook and unknown kinds honestly', () => {
    expect(capabilitySourceLabel('CODEX_DESKTOP')).toBe('能力来源：原生 IPC')
    expect(capabilitySourceLabel('ZCODE_DESKTOP')).toBe('能力来源：官方 Hook')
    expect(capabilitySourceLabel('AGENT_KIND_9')).toBe('能力来源：未知')
    expect(agentKindDisplay('ZCODE_DESKTOP')).toBe('ZCode Desktop')
    expect(agentKindDisplay('AGENT_KIND_9')).toBe('未知 Agent')
  })
})
