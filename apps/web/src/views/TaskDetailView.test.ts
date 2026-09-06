import { mount } from '@vue/test-utils'
import { nextTick } from 'vue'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { Component } from 'vue'

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

async function mountTaskDetail() {
  window.history.replaceState({}, '', '/agent-console/?fixture=1')
  vi.resetModules()
  // 与被测视图同一模块图解析 SaveToToolboxDialog:resetModules 后静态 import 会拿到不同实例。
  const [{ default: TaskDetailView }, consoleStore, { default: SaveToToolboxDialog }] = await Promise.all([
    import('./TaskDetailView.vue'),
    import('@/store/console'),
    import('@/components/SaveToToolboxDialog.vue'),
  ])
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: '/', component: { template: '<div />' } },
      { path: '/tasks', component: { template: '<div />' } },
      { path: '/tasks/:sessionId', component: TaskDetailView },
      { path: '/tasks/:sessionId/git', component: { template: '<div />' } },
    ],
  })
  await router.push('/tasks/session-ui')
  await router.isReady()
  const store = consoleStore.useConsoleStore()
  const wrapper = mount(TaskDetailView, { global: { plugins: [router] } })
  await vi.advanceTimersByTimeAsync(700)
  await nextTick()
  return { wrapper, store, SaveToToolboxDialog: SaveToToolboxDialog as Component }
}

describe('task detail view (UX-09 save-to-toolbox source label)', () => {
  it('labels the saved snippet source with the session agent kind, not a hardcoded Codex', async () => {
    const { wrapper, store, SaveToToolboxDialog } = await mountTaskDetail()

    const saveButton = wrapper.get('[aria-label="保存该条消息到工具箱"]')
    // Codex 会话:来源显示 Codex Desktop(既有行为)。
    await saveButton.trigger('click')
    const dialog = wrapper.getComponent(SaveToToolboxDialog)
    expect(dialog.props('open')).toBe(true)
    expect(dialog.props('snippet')).toMatchObject({ agentDisplay: 'Codex Desktop' })

    // ZCode 会话:来源必须跟随会话的 agentKind,不得固定显示 Codex Desktop。
    const session = store.state.sessions.find((item) => item.id === 'session-ui')!
    session.agentKind = 'ZCODE_DESKTOP'
    await nextTick()
    await wrapper.get('[aria-label="保存该条消息到工具箱"]').trigger('click')
    expect(wrapper.getComponent(SaveToToolboxDialog).props('snippet')).toMatchObject({
      agentDisplay: 'ZCode Desktop',
    })
  })
})
