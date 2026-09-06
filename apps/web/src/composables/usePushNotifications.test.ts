import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  ATTENTION_CLOSED_MESSAGE,
  AUTH_SESSION_PATH,
  PUSH_CONFIG_PATH,
  PUSH_SUBSCRIPTIONS_PATH,
  buildSubscriptionPayload,
  disablePush,
  enablePush,
  explainPushOpenIfResolved,
  initPushOpenHandling,
  parsePushOpenHint,
  readPushToggleState,
  urlBase64ToUint8Array,
  usePushNotifications,
  type PushBrowserDeps,
} from './usePushNotifications'

interface RecordedRequest {
  path: string
  init: RequestInit
}

interface JsonBody {
  [key: string]: unknown
}

function fetchMock(routes: Array<{ path: string; body: JsonBody }>) {
  const requests: RecordedRequest[] = []
  const fetchImpl = vi.fn(async (path: string, init: RequestInit = {}) => {
    requests.push({ path, init })
    const route = routes.find((item) => path === item.path)
    if (!route) return { ok: false, status: 404, json: async () => ({}) }
    return { ok: true, status: 200, json: async () => route.body }
  }) as unknown as typeof fetch
  return { requests, fetchImpl }
}

function subscriptionStub(overrides: Partial<PushBrowserDeps> = {}): PushBrowserDeps {
  return {
    requestPermission: vi.fn(async () => 'granted' as NotificationPermission),
    permission: vi.fn(() => 'default' as NotificationPermission),
    subscribe: vi.fn(async () => ({
      endpoint: 'https://push.example/ep-1',
      getKey: (name: 'p256dh' | 'auth') =>
        new TextEncoder().encode(name === 'p256dh' ? 'p256dh-key' : 'auth-key').buffer as ArrayBuffer,
      unsubscribe: vi.fn(async () => true),
    })),
    getSubscription: vi.fn(async () => null),
    fetch: fetchMock([
      { path: PUSH_CONFIG_PATH, body: { enabled: true, publicKey: 'dGVzdC1wdWJsaWMta2V5' } },
      { path: PUSH_SUBSCRIPTIONS_PATH, body: { subscriptions: [] } },
    ]).fetchImpl,
    ...overrides,
  }
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe('urlBase64ToUint8Array', () => {
  it('decodes base64url with or without padding', () => {
    expect(Array.from(urlBase64ToUint8Array('AQID'))).toEqual([1, 2, 3])
    expect(Array.from(urlBase64ToUint8Array('-_8'))).toEqual([251, 255])
  })
})

describe('buildSubscriptionPayload', () => {
  it('builds the relay subscription body with endpoint and keys only', () => {
    const payload = buildSubscriptionPayload({
      endpoint: 'https://push.example/ep',
      getKey: (name) => new TextEncoder().encode(`raw-${name}`).buffer as ArrayBuffer,
      unsubscribe: async () => true,
    })
    expect(payload).toEqual({
      endpoint: 'https://push.example/ep',
      keys: { p256dh: 'cmF3LXAyNTZkaA', auth: 'cmF3LWF1dGg' },
    })
  })

  it('rejects subscriptions without keys', () => {
    const payload = buildSubscriptionPayload({
      endpoint: 'https://push.example/ep',
      getKey: () => null,
      unsubscribe: async () => true,
    })
    expect(payload).toBeNull()
  })
})

describe('enablePush', () => {
  it('submits the browser subscription to the relay endpoint with csrf', async () => {
    const { requests, fetchImpl } = fetchMock([
      { path: AUTH_SESSION_PATH, body: { csrfToken: 'csrf-1' } },
      { path: PUSH_CONFIG_PATH, body: { enabled: true, publicKey: 'dGVzdC1rZXk' } },
      { path: PUSH_SUBSCRIPTIONS_PATH, body: { id: 'sub-1' } },
    ])
    const result = await enablePush(subscriptionStub({ fetch: fetchImpl }))
    expect(result).toEqual({ ok: true, status: 'subscribed' })
    const post = requests.find((item) => item.path === PUSH_SUBSCRIPTIONS_PATH)
    expect(post?.init.method).toBe('POST')
    expect(post?.init.headers).toMatchObject({ 'X-CSRF-Token': 'csrf-1' })
    const body = JSON.parse(String(post?.init.body)) as {
      endpoint: string
      keys: { p256dh: string; auth: string }
    }
    expect(Object.keys(body)).toEqual(['endpoint', 'keys'])
    expect(body.endpoint).toBe('https://push.example/ep-1')
    expect(Object.keys(body.keys).sort()).toEqual(['auth', 'p256dh'])
    expect(JSON.stringify(body)).not.toContain('ticket')
  })

  it('requests notification permission before subscribing', async () => {
    const { fetchImpl } = fetchMock([
      { path: AUTH_SESSION_PATH, body: { csrfToken: 'c' } },
      { path: PUSH_CONFIG_PATH, body: { enabled: true, publicKey: 'dGVzdC1rZXk' } },
      { path: PUSH_SUBSCRIPTIONS_PATH, body: {} },
    ])
    const order: string[] = []
    const requestPermission = vi.fn(async () => {
      order.push('permission')
      return 'granted' as NotificationPermission
    })
    const subscribe = vi.fn(async () => {
      order.push('subscribe')
      return {
        endpoint: 'https://push.example/ep',
        getKey: (name: 'p256dh' | 'auth') => new Uint8Array([1]).buffer as ArrayBuffer,
        unsubscribe: async () => true,
      }
    })
    await enablePush(subscriptionStub({ requestPermission, subscribe, fetch: fetchImpl }))
    expect(order).toEqual(['permission', 'subscribe'])
  })

  it('reports unconfigured when the relay push is disabled', async () => {
    const { fetchImpl } = fetchMock([
      { path: PUSH_CONFIG_PATH, body: { enabled: false, publicKey: null } },
    ])
    const result = await enablePush(subscriptionStub({ fetch: fetchImpl }))
    expect(result).toEqual({ ok: false, status: 'unconfigured' })
  })

  it('does not subscribe when permission is denied', async () => {
    const { fetchImpl } = fetchMock([
      { path: PUSH_CONFIG_PATH, body: { enabled: true, publicKey: 'dGVzdC1rZXk' } },
    ])
    const subscribe = vi.fn()
    const result = await enablePush(
      subscriptionStub({
        fetch: fetchImpl,
        requestPermission: vi.fn(async () => 'denied' as NotificationPermission),
        subscribe,
      }),
    )
    expect(result).toEqual({ ok: false, status: 'denied' })
    expect(subscribe).not.toHaveBeenCalled()
  })
})

describe('disablePush', () => {
  it('unsubscribes locally and deletes the matching relay record', async () => {
    const { requests, fetchImpl } = fetchMock([
      { path: AUTH_SESSION_PATH, body: { csrfToken: 'c' } },
      {
        path: PUSH_SUBSCRIPTIONS_PATH,
        body: {
          subscriptions: [
            { id: 'other', endpoint: 'https://push.example/other' },
            { id: 'sub-9', endpoint: 'https://push.example/ep-1' },
          ],
        },
      },
      { path: `${PUSH_SUBSCRIPTIONS_PATH}/sub-9`, body: {} },
    ])
    const unsubscribe = vi.fn(async () => true)
    const result = await disablePush(
      subscriptionStub({
        fetch: fetchImpl,
        getSubscription: vi.fn(async () => ({
          endpoint: 'https://push.example/ep-1',
          getKey: () => new Uint8Array([1]).buffer as ArrayBuffer,
          unsubscribe,
        })),
      }),
    )
    expect(result).toEqual({ ok: true, status: 'unsubscribed' })
    expect(unsubscribe).toHaveBeenCalledTimes(1)
    expect(requests.some((item) => item.path === `${PUSH_SUBSCRIPTIONS_PATH}/sub-9` && item.init.method === 'DELETE')).toBe(true)
  })

  it('succeeds without a local subscription', async () => {
    const result = await disablePush(subscriptionStub())
    expect(result).toEqual({ ok: true, status: 'unsubscribed' })
  })
})

describe('readPushToggleState', () => {
  const base = { fetch: fetchMock([{ path: PUSH_CONFIG_PATH, body: { enabled: true, publicKey: 'dGVzdC1rZXk' } }]).fetchImpl }

  it('maps missing browser api to unsupported', async () => {
    const state = await readPushToggleState({ ...base, pushSupported: false })
    expect(state).toBe('unsupported')
  })

  it('maps disabled relay to unconfigured', async () => {
    const state = await readPushToggleState({
      pushSupported: true,
      permission: () => 'default',
      getSubscription: async () => null,
      fetch: fetchMock([{ path: PUSH_CONFIG_PATH, body: { enabled: false, publicKey: null } }]).fetchImpl,
    })
    expect(state).toBe('unconfigured')
  })

  it('maps existing subscription to subscribed', async () => {
    const state = await readPushToggleState({
      ...base,
      pushSupported: true,
      permission: () => 'granted',
      getSubscription: async () => ({
        endpoint: 'https://push.example/ep',
        getKey: () => new Uint8Array([1]).buffer as ArrayBuffer,
        unsubscribe: async () => true,
      }),
    })
    expect(state).toBe('subscribed')
  })
})

describe('explainPushOpenIfResolved', () => {
  const store = (overrides: Partial<Parameters<typeof explainPushOpenIfResolved>[0]['store']> = {}) => ({
    isOnline: () => true,
    isSessionKnown: () => true,
    hasValidAttention: () => false,
    notifyClosed: vi.fn(),
    ...overrides,
  })

  it('explains a closed waiting request once the session is known without it', async () => {
    const notifyClosed = vi.fn()
    const done = await explainPushOpenIfResolved({
      hint: { sessionId: 's-1', kind: 'waitingApproval', openedAt: 1 },
      store: store({ notifyClosed }),
      delay: async () => {},
    })
    expect(done).toBe(true)
    expect(notifyClosed).toHaveBeenCalledWith(ATTENTION_CLOSED_MESSAGE)
  })

  it('stays silent while the waiting request is still valid', async () => {
    const notifyClosed = vi.fn()
    const done = await explainPushOpenIfResolved({
      hint: { sessionId: 's-1', kind: 'waitingQuestion', openedAt: 1 },
      store: store({ hasValidAttention: () => true, notifyClosed }),
      delay: async () => {},
    })
    expect(done).toBe(false)
    expect(notifyClosed).not.toHaveBeenCalled()
  })

  it('ignores turn outcome pushes', async () => {
    const notifyClosed = vi.fn()
    const done = await explainPushOpenIfResolved({
      hint: { sessionId: 's-1', kind: 'turnCompleted', openedAt: 1 },
      store: store({ notifyClosed }),
      delay: async () => {},
    })
    expect(done).toBe(false)
    expect(notifyClosed).not.toHaveBeenCalled()
  })

  it('never reports closed when the session never becomes known', async () => {
    const notifyClosed = vi.fn()
    let calls = 0
    const done = await explainPushOpenIfResolved({
      hint: { sessionId: 's-1', kind: 'waitingQuestion', openedAt: 1 },
      store: store({ isOnline: () => false, notifyClosed }),
      delay: async () => {
        calls += 1
      },
      attempts: 3,
    })
    expect(done).toBe(false)
    expect(calls).toBe(3)
    expect(notifyClosed).not.toHaveBeenCalled()
  })
})

describe('parsePushOpenHint', () => {
  it('accepts waiting-kind hints from the service worker', () => {
    expect(
      parsePushOpenHint({ type: 'agent-console/push-open', sessionId: 's-1', kind: 'waitingApproval', openedAt: 42 }),
    ).toEqual({ sessionId: 's-1', kind: 'waitingApproval', openedAt: 42 })
  })

  it('rejects other message types and turn kinds', () => {
    expect(parsePushOpenHint({ type: 'other', sessionId: 's-1', kind: 'waitingQuestion' })).toBeNull()
    expect(parsePushOpenHint({ type: 'agent-console/push-open', sessionId: 's-1', kind: 'turnCompleted' })).toBeNull()
    expect(parsePushOpenHint({ type: 'agent-console/push-open', kind: 'waitingQuestion' })).toBeNull()
  })
})

describe('initPushOpenHandling', () => {
  function withServiceWorker() {
    const listeners = new Map<string, Array<(event: MessageEvent) => void>>()
    const posted: unknown[] = []
    const controller = { postMessage: (data: unknown) => posted.push(data) }
    const sw = {
      addEventListener: (type: string, handler: (event: MessageEvent) => void) => {
        const arr = listeners.get(type) ?? []
        arr.push(handler)
        listeners.set(type, arr)
      },
      removeEventListener: (type: string, handler: (event: MessageEvent) => void) => {
        listeners.set(type, (listeners.get(type) ?? []).filter((item) => item !== handler))
      },
      controller,
    }
    Object.defineProperty(navigator, 'serviceWorker', { configurable: true, value: sw })
    return { listeners, posted, controller }
  }

  it('queries the controller once and forwards push-open messages to the store', async () => {
    const sw = withServiceWorker()
    const notifyClosed = vi.fn()
    const store = {
      isOnline: () => true,
      isSessionKnown: () => true,
      hasValidAttention: () => false,
      notifyClosed,
    }
    const dispose = initPushOpenHandling(store)
    expect(sw.posted).toEqual([{ type: 'agent-console/push-open-query' }])
    const handlers = sw.listeners.get('message') ?? []
    handlers.forEach((handler) =>
      handler({ data: { type: 'agent-console/push-open', sessionId: 's-1', kind: 'waitingQuestion', openedAt: 7 } } as MessageEvent),
    )
    await vi.waitFor(() => expect(notifyClosed).toHaveBeenCalledWith(ATTENTION_CLOSED_MESSAGE))
    dispose()
    expect((sw.listeners.get('message') ?? []).length).toBe(0)
  })
})

describe('usePushNotifications (jsdom, no service worker)', () => {
  it('reports unsupported and blocks toggling', async () => {
    const { pushState, pushMessage, refreshPushState, toggleBrowserPush } = usePushNotifications()
    await refreshPushState()
    expect(pushState.value).toBe('unsupported')
    expect(pushMessage.value).toContain('不支持')
    await toggleBrowserPush()
    expect(pushState.value).toBe('unsupported')
  })
})
