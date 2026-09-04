import { mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { afterEach, describe, expect, it, vi } from 'vitest'

afterEach(() => {
  vi.useRealTimers()
  window.history.replaceState({}, '', '/')
})

describe('pairing deep link', () => {
  it('opens the pairing panel and looks up its six-digit code once online', async () => {
    vi.useFakeTimers()
    window.history.replaceState({}, '', '/agent-console/pair?code=123456&fixture=1')
    vi.resetModules()

    const [{ default: DevicesView }, { useConsoleStore }] = await Promise.all([
      import('./DevicesView.vue'),
      import('@/store/console'),
    ])
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [{ path: '/pair', component: DevicesView }],
    })
    await router.push('/pair?code=123456&fixture=1')
    const store = useConsoleStore()
    const wrapper = mount(DevicesView, { global: { plugins: [router] } })
    void store.initialize()

    await vi.advanceTimersByTimeAsync(200)

    expect(wrapper.get('input[aria-label="6 位设备绑定码"]').element).toHaveProperty(
      'value',
      '123456',
    )
    expect(wrapper.text()).toContain('Fixture Bridge')

    store.dispose()
    wrapper.unmount()
  })
})
