import { describe, expect, it } from 'vitest'

import {
  applyExecutionFailureToApprovals,
  attentionAddedReminder,
  attentionClosureReminder,
  outcomeReminder,
  projectAttentionInbox,
  useConsoleStore,
  type AnswerState,
} from './console'
import type { AttentionItem, RuntimeSnapshot, SessionSummary } from '@/transport/types'

function session(overrides: Partial<SessionSummary> & { id: string }): SessionSummary {
  return {
    nativeSessionId: `native-${overrides.id}`,
    title: overrides.id,
    projectDisplay: 'workspace',
    branch: 'main',
    updatedAt: '2026-09-05T02:00:00Z',
    deviceId: `device-${overrides.id}`,
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

function attention(overrides: Partial<AttentionItem> & { id: string; sessionId: string }): AttentionItem {
  return {
    kind: 'USER_QUESTION',
    turnId: `turn-${overrides.id}`,
    title: `标题 ${overrides.id}`,
    description: '描述',
    createdAt: '2026-09-05T01:00:00Z',
    valid: true,
    options: [{ id: 'opt-1', label: '选项', emphasis: 'secondary' }],
    ...overrides,
  }
}

function runtime(sessionId: string, attentionItems: AttentionItem[]): RuntimeSnapshot {
  return {
    sessionId,
    runtimeRevision: 4,
    phase: attentionItems.length ? 'RUNNING' : 'IDLE',
    attention: attentionItems,
    queue: { status: 'EMPTY' },
    capabilities: {
      revision: 1,
      operations: {
        START_TURN: true,
        SET_QUEUE: true,
        REPLACE_QUEUE: true,
        CANCEL_QUEUE: true,
        STEER: true,
        INTERRUPT: true,
        ANSWER_QUESTION: true,
        ANSWER_APPROVAL: true,
        UPDATE_SETTINGS: true,
        STOP_BACKGROUND_COMMAND: true,
        STOP_ALL_BACKGROUND_COMMANDS: true,
        OPEN_FILE: true,
        READ_GIT_DIFF: true,
      },
      models: [],
      thinkingDepths: [],
      serviceTiers: [],
      permissionModes: [],
      collaborationModes: [],
    },
    settings: { model: '', thinkingDepth: '', serviceTier: '', permissionMode: '', collaborationMode: '' },
    contextUsed: 0,
    contextWindow: 1,
    timeline: [],
    backgroundCommandCount: 0,
    outputCursors: [],
  }
}

describe('attention inbox projection (UX-01)', () => {
  it('ranks approvals ahead of questions and longest waiting first, without entering details', () => {
    const approval = attention({ id: 'a-1', sessionId: 's-2', kind: 'RISK_APPROVAL', createdAt: '2026-09-05T01:30:00Z' })
    const questionOld = attention({ id: 'q-1', sessionId: 's-1', createdAt: '2026-09-05T01:00:00Z' })
    const questionNew = attention({ id: 'q-2', sessionId: 's-3', createdAt: '2026-09-05T01:45:00Z' })

    const result = projectAttentionInbox({
      sessions: [session({ id: 's-1' }), session({ id: 's-2' }), session({ id: 's-3' })],
      runtimes: {
        's-1': runtime('s-1', [questionOld]),
        's-2': runtime('s-2', [approval]),
        's-3': runtime('s-3', [questionNew]),
      },
      resolvedAttentionIds: [],
      nowMs: Date.parse('2026-09-05T02:00:00Z'),
    })

    expect(result.active.map((entry) => entry.attention.id)).toEqual(['a-1', 'q-1', 'q-2'])
    // 每项含设备、项目、等待时长,不进详情即可判断优先级。
    expect(result.active[0]).toMatchObject({
      kind: 'RISK_APPROVAL',
      projectDisplay: 'workspace',
      waitingMs: 30 * 60_000,
    })
  })

  it('deduplicates the same request by its stable native id across runtimes', () => {
    const duplicated = attention({ id: 'q-native', sessionId: 's-1' })
    const result = projectAttentionInbox({
      sessions: [session({ id: 's-1' })],
      runtimes: {
        's-1': runtime('s-1', [duplicated]),
        's-1-dup': runtime('s-1-dup', [{ ...duplicated }]),
      },
      resolvedAttentionIds: [],
      nowMs: Date.parse('2026-09-05T02:00:00Z'),
    })
    expect(result.active).toHaveLength(1)
  })

  it('surfaces offline sessions with last-known info and marks submission unavailable', () => {
    const offlineSession = session({
      id: 's-off',
      deviceConnection: 'OFFLINE',
      attentionCount: 2,
      deviceLastSeenAt: '2026-09-05T00:30:00Z',
    })
    const result = projectAttentionInbox({
      sessions: [offlineSession],
      runtimes: {},
      resolvedAttentionIds: [],
      nowMs: Date.parse('2026-09-05T02:00:00Z'),
    })

    expect(result.offlineDegraded).toHaveLength(1)
    const entry = result.offlineDegraded[0]!
    expect(entry.offlineDegraded).toBe(true)
    expect(entry.attention.title).toContain('2 项待处理')
    expect(entry.attention.valid).toBe(false)
    expect(entry.waitingMs).toBe(90 * 60_000)
  })

  it('keeps the pending count equal to the number of unique active items', () => {
    const result = projectAttentionInbox({
      sessions: [session({ id: 's-1' })],
      runtimes: { 's-1': runtime('s-1', [attention({ id: 'q-1', sessionId: 's-1' })]) },
      resolvedAttentionIds: ['q-1'],
      nowMs: Date.parse('2026-09-05T02:00:00Z'),
    })
    // 已答复的请求被本地标记后不复活,计数与详情一致。
    expect(result.active).toHaveLength(0)
    expect(result.offlineDegraded).toHaveLength(0)
  })
})

describe('approval execution outcome (UX-05)', () => {
  it('marks an allowed approval as execution-failed when its turn failed', () => {
    const answerStates: Record<string, AnswerState> = {
      'approval-1': { requestId: 'req-1', submittedStatus: 'ACCEPTED_BY_BRIDGE', at: '2026-09-05T01:00:00Z' },
      'approval-2': { requestId: 'req-2', submittedStatus: 'ACCEPTED_BY_BRIDGE', at: '2026-09-05T01:00:00Z' },
    }
    const approvals = new Map([
      ['approval-1', 'session-x'],
      ['approval-2', 'session-y'],
    ])

    applyExecutionFailureToApprovals(answerStates, approvals, 'session-x')

    expect(answerStates['approval-1']?.execution).toBe('FAILED')
    expect(answerStates['approval-2']?.execution).toBeUndefined()
  })
})

describe('in-app reminders (UX-06 minimal set)', () => {
  it('priority reminder carries only a generic summary plus an in-app route', () => {
    const reminder = attentionAddedReminder('session-x', false)
    expect(reminder).toBeDefined()
    expect(reminder!.message).not.toContain('删除')
    expect(reminder!.message).not.toContain('/Users/')
    expect(reminder!.linkTo).toBe('/s/session-x')
    expect(attentionAddedReminder('session-x', true)).toBeUndefined()
  })

  it('failure reminders are always on; success reminders follow the user toggle', () => {
    expect(outcomeReminder('s', 'FAILED', false, false)).toBeDefined()
    expect(outcomeReminder('s', 'FAILED', true, true)).toBeUndefined()
    expect(outcomeReminder('s', 'COMPLETED', false, false)).toBeUndefined()
    expect(outcomeReminder('s', 'COMPLETED', false, true)).toMatchObject({ linkTo: '/s/s' })
  })

  it('explains a closed request instead of linking an approvable card', () => {
    const reminder = attentionClosureReminder('s')
    expect(reminder.message).toContain('已过期或已在本机处理')
    // 点击后进入会话详情走正常认证与原生 pending 核对,不是可批准卡片。
    expect(reminder.linkTo).toBe('/s/s')
  })
})

describe('relay/toolbox version consumption (IN-01 follow-up)', () => {
  it('uses fetched relay version and falls back to empty when absent', () => {
    const { componentVersions, state } = useConsoleStore()

    expect(componentVersions().relay).toBe('')
    expect(componentVersions().relayProtocol).toBe('')

    state.relayVersions = { relay: '0.4.0', protocol: '1' }
    const versions = componentVersions()
    expect(versions.relay).toBe('0.4.0')
    expect(versions.relayProtocol).toBe('1')
    // 网页侧协议版本仍以本地常量为权威,用于比对 Relay 上报值。
    expect(versions.protocol).toMatch(/^\d+$/)
    // Toolbox 未取到时为空串(展示层回退"未知"),占位值不进入状态。
    expect(versions.toolbox).toBe('')
  })

  it('uses fetched toolbox version with commit and falls back to empty when absent', () => {
    const { componentVersions, state } = useConsoleStore()

    expect(componentVersions().toolbox).toBe('')

    state.toolboxVersions = { app: 'v3.2.1', commit: 'a1b2c3d4e5f6789' }
    const versions = componentVersions()
    expect(versions.toolbox).toBe('v3.2.1')
    expect(versions.toolboxCommit).toBe('a1b2c3d4e5f6789')
  })
})
