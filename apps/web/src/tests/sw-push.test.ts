import { beforeEach, describe, expect, it, vi } from 'vitest'

import swSource from '../../public/sw.js?raw'

/**
 * sw.js 推送逻辑的行为级测试:以 stub self/caches 求值脚本,
 * 驱动 push / notificationclick / message 事件,断言通知内容与导航目标。
 */

const APP_ORIGIN = 'https://console.example'
const APP_BASE = `${APP_ORIGIN}/agent-console/`

interface WaitUntilEvent {
  waitUntil: (promise: Promise<unknown>) => void
}

function loadServiceWorker() {
  const listeners = new Map<string, Array<(event: never) => void>>()
  const showNotification = vi.fn(async () => {})
  const openWindow = vi.fn(async () => null)
  const clients: Array<Record<string, unknown>> = []
  const cacheStore = new Map<string, string>()
  const cache = {
    put: async (url: URL, response: Response) => {
      cacheStore.set(url.toString(), await response.text())
    },
    match: async (url: URL) => {
      const text = cacheStore.get(url.toString())
      return text === undefined ? undefined : new Response(text)
    },
    delete: async (url: URL) => cacheStore.delete(url.toString()),
  }
  const cachesStub = { open: async () => cache }
  const selfStub = {
    addEventListener: (type: string, handler: (event: never) => void) => {
      const arr = listeners.get(type) ?? []
      arr.push(handler)
      listeners.set(type, arr)
    },
    location: { origin: APP_ORIGIN },
    registration: { showNotification },
    clients: {
      matchAll: async () => clients,
      openWindow,
    },
  }
  new Function('self', 'caches', swSource)(selfStub, cachesStub)
  const dispatch = async (type: string, extra: Record<string, unknown>): Promise<void> => {
    const waiters: Promise<unknown>[] = []
    const event = {
      waitUntil: (promise: Promise<unknown>) => waiters.push(promise),
      ...extra,
    } as never
    for (const handler of listeners.get(type) ?? []) handler(event)
    await Promise.all(waiters)
  }
  return { listeners, showNotification, openWindow, clients, cacheStore, dispatch }
}

function pushEvent(payload: unknown): { data: { text: () => string } } & WaitUntilEvent {
  const raw = typeof payload === 'string' ? payload : JSON.stringify(payload)
  return { data: { text: () => raw } }
}

describe('service worker push handling', () => {
  let sw: ReturnType<typeof loadServiceWorker>

  beforeEach(() => {
    sw = loadServiceWorker()
  })

  describe('push event', () => {
    it('shows a notification with the relay summary and routing data only', async () => {
      await sw.dispatch('push', {
        ...pushEvent({
          sessionId: '11111111-1111-1111-1111-111111111111',
          kind: 'waitingApproval',
          deepLink: '/agent-console/s/11111111-1111-1111-1111-111111111111',
          title: null,
          body: '任务在等待风险审批',
        }),
      })
      expect(sw.showNotification).toHaveBeenCalledTimes(1)
      const [title, options] = sw.showNotification.mock.calls[0] as [string, Record<string, unknown>]
      expect(title).toBe('Agent Console') // showTitle 关闭时不带会话标题
      expect(options.body).toBe('任务在等待风险审批')
      expect(options.tag).toBe('agent-console-11111111-1111-1111-1111-111111111111-waitingApproval')
      expect(options.data).toEqual({
        sessionId: '11111111-1111-1111-1111-111111111111',
        kind: 'waitingApproval',
        deepLink: '/agent-console/s/11111111-1111-1111-1111-111111111111',
      })
      expect(JSON.stringify(options.data)).not.toContain('ticket')
    })

    it('falls back to a generic visible notification for malformed payloads', async () => {
      await sw.dispatch('push', { ...pushEvent('not-json') })
      const [title, options] = sw.showNotification.mock.calls[0] as [string, Record<string, unknown>]
      expect(title).toBe('Agent Console')
      expect(options.body).toBe('收到新的任务提醒')
      expect(options.data).toEqual({ sessionId: '', kind: '', deepLink: '' })
    })

    it('falls back to the kind copy when body is missing', async () => {
      await sw.dispatch('push', { ...pushEvent({ sessionId: 's-1', kind: 'turnFailed' }) })
      const [, options] = sw.showNotification.mock.calls[0] as [string, Record<string, unknown>]
      expect(options.body).toBe('任务未完成,已失败')
    })
  })

  describe('notificationclick', () => {
    it('opens a new window on the deep link and stores the pending open for waiting kinds', async () => {
      const notify = { close: vi.fn(), data: { sessionId: 's-1', kind: 'waitingQuestion', deepLink: '/agent-console/s/s-1' } }
      await sw.dispatch('notificationclick', { notification: notify })
      expect(notify.close).toHaveBeenCalledTimes(1)
      expect(sw.openWindow).toHaveBeenCalledWith(`${APP_BASE}s/s-1`)
      expect(sw.clients.find(() => true)).toBeUndefined()
      // 页面启动后经 push-open-query 拉取暂存(取后即清)。
      const replies: unknown[] = []
      await sw.dispatch('message', {
        data: { type: 'agent-console/push-open-query' },
        source: { postMessage: (data: unknown) => replies.push(data) },
      })
      expect(replies[0]).toMatchObject({ type: 'agent-console/push-open', sessionId: 's-1', kind: 'waitingQuestion' })
      const again: unknown[] = []
      await sw.dispatch('message', {
        data: { type: 'agent-console/push-open-query' },
        source: { postMessage: (data: unknown) => again.push(data) },
      })
      expect(again).toEqual([])
    })

    it('navigates an existing window without posting to the old document', async () => {
      const client = {
        url: `${APP_BASE}tasks/other`,
        navigate: vi.fn(async () => ({})),
        focus: vi.fn(),
        postMessage: vi.fn(),
      }
      sw.clients.push(client)
      await sw.dispatch('notificationclick', {
        notification: { close: vi.fn(), data: { sessionId: 's-2', kind: 'turnCompleted', deepLink: '/agent-console/s/s-2' } },
      })
      expect(client.navigate).toHaveBeenCalledWith(`${APP_BASE}s/s-2`)
      expect(client.postMessage).not.toHaveBeenCalled()
      expect(client.focus).not.toHaveBeenCalled()
    })

    it('focuses and messages the client when already on the target route', async () => {
      const target = `${APP_BASE}s/s-3`
      const client = {
        url: target,
        navigate: vi.fn(async () => ({})),
        focus: vi.fn(),
        postMessage: vi.fn(),
      }
      sw.clients.push(client)
      await sw.dispatch('notificationclick', {
        notification: { close: vi.fn(), data: { sessionId: 's-3', kind: 'waitingApproval', deepLink: '' } },
      })
      expect(client.navigate).not.toHaveBeenCalled()
      expect(client.focus).toHaveBeenCalledTimes(1)
      expect(client.postMessage).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'agent-console/push-open', sessionId: 's-3', kind: 'waitingApproval' }),
      )
    })

    it('rejects unsafe deep links and falls back to the in-app session route', async () => {
      // push 阶段解析:非相对路径的 deepLink 被丢弃。
      await sw.dispatch('push', {
        ...pushEvent({ sessionId: 's-4', kind: 'turnFailed', deepLink: 'https://evil.example/x', body: 'b' }),
      })
      const [, options] = sw.showNotification.mock.calls[0] as [string, Record<string, unknown>]
      expect((options.data as Record<string, unknown>).deepLink).toBe('')
      sw.clients.push({
        url: `${APP_BASE}tasks/other`,
        navigate: vi.fn(async () => ({})),
        focus: vi.fn(),
        postMessage: vi.fn(),
      })
      await sw.dispatch('notificationclick', { notification: { close: vi.fn(), data: options.data } })
      const navigate = (sw.clients[0] as { navigate: ReturnType<typeof vi.fn> }).navigate
      expect(navigate).toHaveBeenCalledWith(`${APP_BASE}s/s-4`)
    })

    it('rejects backslash-prefixed deep links that WHATWG URL treats as scheme-relative', async () => {
      // WHATWG URL 把 "\" 归一为 "/":'/\\evil.com' 解析为跨源 'https://evil.com/'。
      await sw.dispatch('push', {
        ...pushEvent({ sessionId: 's-5', kind: 'turnFailed', deepLink: '/\\evil.example/x', body: 'b' }),
      })
      const [, options] = sw.showNotification.mock.calls[0] as [string, Record<string, unknown>]
      expect((options.data as Record<string, unknown>).deepLink).toBe('')
      // 丢弃后点击回退站内既有路由,而不是导航到外部源。
      sw.clients.push({
        url: `${APP_BASE}tasks/other`,
        navigate: vi.fn(async () => ({})),
        focus: vi.fn(),
        postMessage: vi.fn(),
      })
      await sw.dispatch('notificationclick', { notification: { close: vi.fn(), data: options.data } })
      const navigate = (sw.clients[0] as { navigate: ReturnType<typeof vi.fn> }).navigate
      expect(navigate).toHaveBeenCalledWith(`${APP_BASE}s/s-5`)
    })

    it('keeps rejecting protocol-relative and out-of-scope links while accepting in-app routes', async () => {
      await sw.dispatch('push', {
        ...pushEvent({ sessionId: 's-7', kind: 'turnFailed', deepLink: '//evil.example/x', body: 'b' }),
      })
      const [, protocolRelative] = sw.showNotification.mock.calls[0] as [string, Record<string, unknown>]
      expect((protocolRelative.data as Record<string, unknown>).deepLink).toBe('')

      await sw.dispatch('push', {
        ...pushEvent({ sessionId: 's-7', kind: 'turnFailed', deepLink: '/outside/route', body: 'b' }),
      })
      const [, outOfScope] = sw.showNotification.mock.calls[1] as [string, Record<string, unknown>]
      expect((outOfScope.data as Record<string, unknown>).deepLink).toBe('')

      await sw.dispatch('push', {
        ...pushEvent({
          sessionId: 's-7',
          kind: 'turnFailed',
          deepLink: '/agent-console/s/s-7?from=push',
          body: 'b',
        }),
      })
      const [, inApp] = sw.showNotification.mock.calls[2] as [string, Record<string, unknown>]
      expect((inApp.data as Record<string, unknown>).deepLink).toBe('/agent-console/s/s-7?from=push')
    })
  })
})
