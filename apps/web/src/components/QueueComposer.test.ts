import { flushPromises, mount } from '@vue/test-utils'
import { h } from 'vue'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { RuntimeSnapshot, SessionSummary } from '@/transport/types'

function session(id: string, updatedAt: string): SessionSummary {
  return {
    id,
    nativeSessionId: `native-${id}`,
    title: id,
    projectDisplay: 'workspace',
    branch: 'main',
    updatedAt,
    deviceId: 'device-shared',
    deviceConnection: 'ONLINE',
    controlMode: 'LIMITED_CONTROL',
    compatibility: 'VERIFIED',
    phase: 'RUNNING',
    lastOutcome: 'COMPLETED',
    attentionCount: 0,
    queueStatus: 'EMPTY',
    pinned: false,
    muted: false,
    archived: false,
  }
}

function runtime(sessionId: string, queueStatus: 'EMPTY' | 'QUEUED' | 'PAUSED' = 'EMPTY'): RuntimeSnapshot {
  return {
    sessionId,
    runtimeRevision: 8,
    activeTurnId: `turn-${sessionId}`,
    phase: 'RUNNING',
    attention: [],
    queue: { status: queueStatus },
    capabilities: {
      revision: 2,
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

async function loadFixtureModules() {
  window.history.replaceState({}, '', '/agent-console/?fixture=1')
  vi.resetModules()
  const [{ default: QueueComposer }, consoleStore] = await Promise.all([
    import('./QueueComposer.vue'),
    import('@/store/console'),
  ])
  return { QueueComposer, useConsoleStore: consoleStore.useConsoleStore }
}

async function mountComposer(
  sessionId: string,
  setup: (store: ReturnType<typeof use>['useConsoleStore']) => void = () => undefined,
  queueStatus: 'EMPTY' | 'QUEUED' | 'PAUSED' = 'EMPTY',
) {
  const { QueueComposer, useConsoleStore } = await loadFixtureModules()
  const store = useConsoleStore()
  store.state.connection = 'ONLINE'
  setup(store)
  const wrapper = mount(QueueComposer, {
    props: { sessionId, queue: { status: queueStatus }, phase: 'RUNNING' as const },
  })
  await wrapper.vm.$nextTick()
  return { wrapper, store }
}

type use = typeof import('@/store/console')

beforeEach(() => {
  vi.useFakeTimers()
})

describe('queue composer closed loop (UX-03)', () => {
  it('recomputes the submitted operation after an accepted queue update', async () => {
    const { QueueComposer, useConsoleStore } = await loadFixtureModules()
    const { FixtureConsoleTransport } = await import('@/transport/fixture-transport')
    const store = useConsoleStore()
    const id = 'session-queue-replace'
    store.state.connection = 'ONLINE'
    store.state.sessions.push(session(id, '2026-09-05T01:00:00Z'))
    store.state.runtimes[id] = runtime(id)
    const queued = { ...runtime(id, 'QUEUED'), runtimeRevision: 10 }
    const snapshot = vi.spyOn(FixtureConsoleTransport.prototype, 'getRuntimeSnapshot')
      .mockResolvedValueOnce({ ...runtime(id), runtimeRevision: 9 })
      .mockResolvedValueOnce(queued)
    const send = vi.spyOn(FixtureConsoleTransport.prototype, 'sendCommand')
      .mockImplementation(async (request) => ({ requestId: request.requestId, status: 'ACCEPTED_BY_BRIDGE' }))
    const wrapper = mount({
      render: () => h(QueueComposer, {
        sessionId: id,
        queue: store.state.runtimes[id]!.queue,
        phase: store.state.runtimes[id]!.phase,
      }),
    })
    try {
      await wrapper.get('textarea').setValue('original')
      await wrapper.get('.queue-composer__footer .ui-button--primary').trigger('click')
      await flushPromises()
      expect(wrapper.text()).toContain('替换队列')
      await wrapper.get('textarea').setValue('replacement')
      await wrapper.get('.queue-composer__footer .ui-button--primary').trigger('click')
      await flushPromises()
      expect(send.mock.calls.map(([request]) => request.operation)).toEqual(['SET_QUEUE', 'REPLACE_QUEUE'])
      expect(wrapper.get('textarea').element.value).toBe('')
    } finally {
      wrapper.unmount()
      snapshot.mockRestore()
      send.mockRestore()
    }
  })

  it('keeps per-session drafts isolated and preserved across session switches', async () => {
    const { wrapper } = await mountComposer('session-a', (store) => {
      store.state.sessions.push(session('session-a', '2026-09-05T01:00:00Z'), session('session-b', '2026-09-05T02:00:00Z'))
      store.state.runtimes['session-a'] = runtime('session-a')
      store.state.runtimes['session-b'] = runtime('session-b')
    })

    const textarea = wrapper.get('textarea')
    await textarea.setValue('给会话 A 的草稿')
    expect(textarea.element.value).toBe('给会话 A 的草稿')

    await wrapper.setProps({ sessionId: 'session-b' })
    expect(wrapper.get('textarea').element.value).toBe('')

    await wrapper.setProps({ sessionId: 'session-a' })
    expect(wrapper.get('textarea').element.value).toBe('给会话 A 的草稿')
  })

  it('blocks duplicate submits while a command is in flight and clears only after acceptance', async () => {
    // session-sync:fixture 强制刷新后仍返回 RUNNING 且队列为空的会话。
    const { wrapper, store } = await mountComposer('session-sync', (draft) => {
      draft.state.sessions.push(session('session-sync', '2026-09-05T01:00:00Z'))
      draft.state.runtimes['session-sync'] = runtime('session-sync')
    })

    await wrapper.get('textarea').setValue('只发一次')
    const desktopSubmit = wrapper.findAll('button').find((button) => button.text().includes('排入下一轮'))!

    await desktopSubmit.trigger('click')
    await wrapper.vm.$nextTick()

    // 在途回执未返回:按钮进入提交中并禁用,文本仍在。
    expect(desktopSubmit.attributes('disabled')).toBeDefined()
    expect(wrapper.get('textarea').element.value).toBe('只发一次')

    const receiptsBefore = store.state.receipts.length
    await desktopSubmit.trigger('click')
    await wrapper.vm.$nextTick()
    expect(store.state.receipts.length).toBe(receiptsBefore)

    await vi.advanceTimersByTimeAsync(500)
    // 回执 ACCEPTED_BY_BRIDGE 后才清空输入。
    expect(wrapper.get('textarea').element.value).toBe('')
  })

  it('offers verification for an unknown outcome and never clears input while verifying', async () => {
    const { wrapper, store } = await mountComposer('session-sync', (draft) => {
      draft.state.sessions.push(session('session-sync', '2026-09-05T01:00:00Z'))
      draft.state.runtimes['session-sync'] = runtime('session-sync')
    })

    await wrapper.get('textarea').setValue('可能已经发出')
    const desktopSubmit = wrapper.findAll('button').find((button) => button.text().includes('排入下一轮'))!
    await desktopSubmit.trigger('click')
    await vi.advanceTimersByTimeAsync(500)

    // 模拟回执等待超时后只拿到 OUTCOME_UNKNOWN(AC-06 传输层已保证该回执路径)。
    const requestId = store.state.receipts[0]!.requestId
    store.state.receipts.splice(0, store.state.receipts.length, {
      requestId,
      status: 'OUTCOME_UNKNOWN',
    })
    await wrapper.vm.$nextTick()

    // 阶段标签从"Bridge 已接收"切换为"结果未知",并出现核对入口。
    expect(wrapper.text()).toContain('结果未知')
    expect(wrapper.text()).not.toContain('Bridge 已接收')

    // 用户重试前的核对不清空输入、不自动重发。
    await wrapper.get('textarea').setValue('重试前的草稿')
    await wrapper.get('.queue-composer__verify').trigger('click')
    await vi.advanceTimersByTimeAsync(50)
    expect(wrapper.get('textarea').element.value).toBe('重试前的草稿')
    expect(wrapper.text()).toContain('结果未知')
    expect(store.state.receipts).toHaveLength(1)
  })

  it('keeps an explicitly cleared draft empty instead of falling back to the queued text', async () => {
    const { wrapper } = await mountComposer(
      'session-queued',
      (store) => {
        store.state.sessions.push(session('session-queued', '2026-09-05T01:00:00Z'))
        store.state.runtimes['session-queued'] = runtime('session-queued', 'QUEUED')
      },
      'QUEUED',
    )
    await wrapper.setProps({ queue: { status: 'QUEUED', text: '旧的队列正文' } })
    await wrapper.vm.$nextTick()

    // 尚未建立草稿时预填当前队列正文,便于整体替换。
    expect(wrapper.get('textarea').element.value).toBe('旧的队列正文')

    // 用户明确清空草稿:输入保持为空、提交禁用,不得回退旧正文提交旧指令。
    await wrapper.get('textarea').setValue('')
    await wrapper.vm.$nextTick()
    expect(wrapper.get('textarea').element.value).toBe('')
    const desktopSubmit = wrapper.findAll('button').find((button) => button.text().includes('替换队列'))!
    expect(desktopSubmit.attributes('disabled')).toBeDefined()
  })

  it('explains a paused queue as requiring manual confirmation', async () => {
    const { wrapper } = await mountComposer('session-paused', () => undefined, 'PAUSED')
    expect(wrapper.text()).toContain('队列已暂停')
    expect(wrapper.text()).toContain('不会自动发送')
  })
})
