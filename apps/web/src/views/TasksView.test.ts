import { mount } from '@vue/test-utils'
import { nextTick } from 'vue'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, describe, expect, it, vi } from 'vitest'

afterEach(() => {
  vi.useRealTimers()
  window.history.replaceState({}, '', '/')
})

describe('tasks search across pages', () => {
  it('keeps loading remaining pages while searching instead of concluding no match early', async () => {
    vi.useFakeTimers()
    window.history.replaceState({}, '', '/agent-console/tasks?fixture=1')
    vi.resetModules()

    const [{ default: TasksView }, { useConsoleStore }] = await Promise.all([
      import('./TasksView.vue'),
      import('@/store/console'),
    ])
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [
        { path: '/tasks', component: TasksView },
        { path: '/tasks/:sessionId', component: { template: '<div />' } },
      ],
    })
    // 搜索词只存在于第二页的 fixture 会话("失败结果状态")。
    await router.push('/tasks?q=失败')
    await router.isReady()
    const store = useConsoleStore()
    const wrapper = mount(TasksView, { global: { plugins: [router] } })
    void store.initialize()

    // 第一页(4 个)加载完、仍无匹配且还有下一页:不得断言"没有匹配"。
    await vi.advanceTimersByTimeAsync(120)
    await nextTick()
    expect(wrapper.text()).not.toContain('没有匹配的任务')
    expect(wrapper.text()).toContain('正在搜索更多任务')

    // 搜索期间的链式加载把第二页也取回,匹配项出现。
    await vi.advanceTimersByTimeAsync(300)
    await nextTick()
    expect(wrapper.text()).toContain('失败结果状态')
    expect(wrapper.text()).not.toContain('没有匹配的任务')

    store.dispose()
    wrapper.unmount()
  })
})
