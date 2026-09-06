import { mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { AnswerState } from '@/transport/types'

function stubMobileViewport(): void {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: query.includes('759'),
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })),
  })
}

beforeEach(() => {
  vi.useFakeTimers()
  stubMobileViewport()
})

afterEach(() => {
  vi.useRealTimers()
  window.history.replaceState({}, '', '/')
  vi.restoreAllMocks()
})

async function mountAttentionView() {
  window.history.replaceState({}, '', '/agent-console/?fixture=1')
  vi.resetModules()
  const [{ default: AttentionView }, consoleStore] = await Promise.all([
    import('./AttentionView.vue'),
    import('@/store/console'),
  ])
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/', component: AttentionView },
      { path: '/tasks', component: { template: '<div />' } },
      { path: '/tasks/:sessionId', component: { template: '<div />' } },
    ],
  })
  await router.push('/')
  const store = consoleStore.useConsoleStore()
  const wrapper = mount(AttentionView, { global: { plugins: [router] } })
  void store.initialize()
  await vi.advanceTimersByTimeAsync(700)
  return { wrapper, store }
}

describe('attention view (UX-01 / UX-05, mobile viewport simulated)', () => {
  it('projects priority, project, device and waiting duration without entering details', async () => {
    const { wrapper } = await mountAttentionView()

    const rows = wrapper.findAll('.inbox-row')
    expect(rows.length).toBe(2)
    // 审批排在提问之前。
    expect(rows[0]!.text()).toContain('等待风险审批 · Codex Desktop')
    expect(rows[0]!.text()).toContain('sync-engine')
    expect(rows[1]!.text()).toContain('agent-console')
    // 等待时长以"等待 N 天"呈现(fixture 时间早于当前)。
    expect(rows[0]!.text()).toMatch(/等待 \d+ 天/)
    // 头部计数与列表一致。
    expect(wrapper.get('.inbox-header span').text()).toBe('2')
  })

  it('opens the approval sheet with action, workspace and one-shot native options only', async () => {
    const { wrapper } = await mountAttentionView()

    await wrapper.findAll('.inbox-row')[0]!.trigger('click')
    await vi.advanceTimersByTimeAsync(0)

    const sheet = wrapper.get('#decision-sheet')
    expect(sheet.classes()).toContain('is-open')
    expect(sheet.text()).toContain('删除主索引中的冗余事件')
    expect(sheet.text()).toContain('sync-engine')
    const options = sheet.findAll('button').filter((button) => ['仅本次允许', '拒绝'].includes(button.text()))
    expect(options).toHaveLength(2)
    // 原生不支持"永久允许":不得出现扩大范围的选项。
    expect(sheet.text()).not.toContain('永久允许')
  })

  it('keeps submitted feedback on the page after answering, not toast-only', async () => {
    const { wrapper, store } = await mountAttentionView()

    await wrapper.findAll('.inbox-row')[0]!.trigger('click')
    await vi.advanceTimersByTimeAsync(0)
    const allow = wrapper
      .get('#decision-sheet')
      .findAll('button')
      .find((button) => button.text() === '仅本次允许')!
    await allow.trigger('click')
    await vi.advanceTimersByTimeAsync(300)

    const strip = wrapper.get('.submitted-strip')
    expect(strip.text()).toContain('已提交回复，等待原生处理结果。')
    expect(store.state.answerStates['approval-9b4d']?.submittedStatus).toBe('ACCEPTED_BY_BRIDGE')

    // 所属轮次失败后:同一反馈升级为"已允许，执行失败"。
    store.state.answerStates['approval-9b4d'] = {
      ...(store.state.answerStates['approval-9b4d'] as AnswerState),
      execution: 'FAILED',
    }
    await wrapper.vm.$nextTick()
    expect(wrapper.get('.submitted-strip').text()).toContain('已允许，执行失败')
  })

  it('copies approval info without credentials or tokens', async () => {
    const writeText = vi.fn()
    Object.assign(navigator, { clipboard: { writeText } })
    const { wrapper } = await mountAttentionView()

    await wrapper.findAll('.inbox-row')[0]!.trigger('click')
    await vi.advanceTimersByTimeAsync(0)
    const copyButton = wrapper
      .get('#decision-sheet')
      .findAll('button')
      .find((button) => button.text().includes('复制审批信息'))!
    await copyButton.trigger('click')

    expect(writeText).toHaveBeenCalledTimes(1)
    const copied = String(writeText.mock.calls[0]![0])
    expect(copied).toContain('风险审批')
    expect(copied).toContain('sync-engine')
    expect(copied.toLowerCase()).not.toContain('token')
    expect(copied).not.toMatch(/bearer|cookie/i)
  })

  it('explains that offline sessions only show last-known info', async () => {
    const { wrapper, store } = await mountAttentionView()
    // 设备离线且摘要有待处理:投影出最后已知卡片并解释不可提交。
    store.state.sessions.push({
      ...store.state.sessions.find((session) => session.id === 'session-ui')!,
      id: 'session-offline-degraded',
      deviceConnection: 'OFFLINE',
      attentionCount: 3,
      deviceLastSeenAt: '2026-09-04T10:00:00+08:00',
    })
    await wrapper.vm.$nextTick()

    const degradedRow = wrapper.findAll('.inbox-row').find((row) => row.text().includes('3 项待处理'))
    expect(degradedRow).toBeDefined()
    expect(degradedRow!.text()).toContain('离线 · 最后已知信息')
  })

  it('shows the selected session agent kind in the inspector instead of a hardcoded label', async () => {
    const { wrapper, store } = await mountAttentionView()
    const session = store.state.sessions.find((item) => item.id === 'session-sync')!
    expect(session.agentKind).toBe('CODEX_DESKTOP')

    await wrapper.findAll('.inbox-row')[0]!.trigger('click')
    await vi.advanceTimersByTimeAsync(0)
    expect(wrapper.get('.decision-inspector').text()).toContain('Codex Desktop')

    // 同一界面切到 ZCode 会话:inspector 的 Agent 字段必须随会话变化,不得硬编码。
    session.agentKind = 'ZCODE_DESKTOP'
    await wrapper.vm.$nextTick()
    expect(wrapper.get('.decision-inspector').text()).toContain('ZCode Desktop')
  })

  it('renders an explicit placeholder without decision buttons for approvals with empty decisions', async () => {
    const { wrapper, store } = await mountAttentionView()
    // 0.153.4 审批投影 decisions 为空:卡片可见,但没有可决定按钮,需要解释而非死卡片。
    const approval = store.state.runtimes['session-sync']!.attention.find(
      (item) => item.kind === 'RISK_APPROVAL',
    )!
    expect(approval.id).toBe('approval-9b4d')
    approval.options = []
    await wrapper.vm.$nextTick()

    await wrapper.findAll('.inbox-row')[0]!.trigger('click')
    await vi.advanceTimersByTimeAsync(0)

    const sheet = wrapper.get('#decision-sheet')
    expect(sheet.text()).toContain('此版本尚未验证远程审批回复，请在桌面端处理')
    const decisionButtons = sheet
      .findAll('button')
      .filter((button) => ['仅本次允许', '拒绝'].includes(button.text()))
    expect(decisionButtons).toHaveLength(0)
    // 不为空 decisions 臆造按钮:未验证能力不开放。
    expect(sheet.text()).not.toContain('永久允许')
  })
})
