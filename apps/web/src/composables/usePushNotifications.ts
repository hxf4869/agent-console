/**
 * 浏览器后台推送(UX-06):复用 Relay 既有 Web Push 后端,补齐"关闭页面后"的提醒。
 * - 订阅开关位于设备页;浏览器通知授权必须由用户真实点击触发;
 * - 订阅本体提交到 Relay 既有 /push/subscriptions;本机不保存订阅密钥副本;
 * - 推送点击只做导航,经正常认证后才可操作,payload 不含任何批准凭据;
 * - 等待类推送打开后若无对应有效待处理请求,给出与站内提醒一致的"已关闭"解释。
 * 浏览器 API(Notification/pushManager/service worker)一律依赖注入,便于 mock 验证。
 */

import { ref } from 'vue'

export const PUSH_CONFIG_PATH = '/api/v1/agent-console/push/vapid-public-key'
export const PUSH_SUBSCRIPTIONS_PATH = '/api/v1/agent-console/push/subscriptions'
export const AUTH_SESSION_PATH = '/api/v1/auth/session'
export const PUSH_OPEN_MESSAGE_TYPE = 'agent-console/push-open'
export const PUSH_OPEN_QUERY_MESSAGE_TYPE = 'agent-console/push-open-query'

/** 与 Relay PushEventKind::name() 对齐(通知设置 JSON 同键)。 */
export type PushEventKind =
  | 'turnCompleted'
  | 'turnFailed'
  | 'turnInterrupted'
  | 'waitingQuestion'
  | 'waitingApproval'

const WAITING_KINDS: ReadonlySet<string> = new Set(['waitingQuestion', 'waitingApproval'])

export interface PushConfig {
  enabled: boolean
  publicKey: string | null
}

/** Relay 侧订阅接口请求体(与 POST /push/subscriptions 契约一致)。 */
export interface PushSubscriptionBody {
  endpoint: string
  keys: { p256dh: string; auth: string }
}

/** pushManager.subscribe 结果的最小投影(便于测试注入)。 */
export interface PushSubscriptionLike {
  endpoint: string
  getKey(name: 'p256dh' | 'auth'): ArrayBuffer | null
  unsubscribe(): Promise<boolean>
}

/** 推送开关的派生状态(设备页展示)。 */
export type PushToggleState =
  | 'unsupported'
  | 'unconfigured'
  | 'denied'
  | 'subscribed'
  | 'off'

export interface PushOpenHint {
  sessionId: string
  kind: PushEventKind
  openedAt: number
}

// ---------------------------------------------------------------------------
// 纯函数(base64 / payload 构造)
// ---------------------------------------------------------------------------

/** VAPID 公钥 base64url → applicationServerKey 所需字节。 */
export function urlBase64ToUint8Array(base64: string): Uint8Array<ArrayBuffer> {
  const padded =
    base64.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - (base64.length % 4)) % 4)
  const binary = atob(padded)
  const bytes = new Uint8Array(new ArrayBuffer(binary.length))
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i)
  return bytes
}

function bufferToBase64Url(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

/** 浏览器订阅 → Relay 订阅请求体;缺 key 视为无效订阅。 */
export function buildSubscriptionPayload(sub: PushSubscriptionLike): PushSubscriptionBody | null {
  const p256dh = sub.getKey('p256dh')
  const auth = sub.getKey('auth')
  if (!sub.endpoint || !p256dh || !auth) return null
  return {
    endpoint: sub.endpoint,
    keys: { p256dh: bufferToBase64Url(p256dh), auth: bufferToBase64Url(auth) },
  }
}

// ---------------------------------------------------------------------------
// Relay API(fetch 注入;与站内请求一致的同域 Cookie + CSRF 语义)
// ---------------------------------------------------------------------------

async function pushApi<T>(
  fetchImpl: typeof fetch,
  path: string,
  init: RequestInit = {},
  mutation = false,
): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json' }
  if (init.body) headers['Content-Type'] = 'application/json'
  if (mutation) {
    const csrf = await fetchCsrf(fetchImpl)
    if (csrf) headers['X-CSRF-Token'] = csrf
  }
  const response = await fetchImpl(path, {
    ...init,
    headers: { ...headers, ...(init.headers as Record<string, string> | undefined) },
    credentials: 'same-origin',
    cache: 'no-store',
  })
  if (!response.ok) throw new Error(`请求失败（${response.status}）。`)
  if (response.status === 204) return undefined as T
  return (await response.json()) as T
}

async function fetchCsrf(fetchImpl: typeof fetch): Promise<string> {
  try {
    const body = await pushApi<{ csrfToken?: string }>(fetchImpl, AUTH_SESSION_PATH)
    return typeof body.csrfToken === 'string' ? body.csrfToken : ''
  } catch {
    return ''
  }
}

/** 查询 Relay 推送能力(未配置 VAPID 时前端隐藏订阅入口)。 */
export async function fetchPushConfig(fetchImpl: typeof fetch = window.fetch): Promise<PushConfig> {
  const body = await pushApi<Partial<PushConfig>>(fetchImpl, PUSH_CONFIG_PATH)
  return {
    enabled: body.enabled === true,
    publicKey:
      typeof body.publicKey === 'string' && body.publicKey ? body.publicKey : null,
  }
}

async function listSubscriptionIds(
  fetchImpl: typeof fetch,
): Promise<Array<{ id: string; endpoint: string }>> {
  const body = await pushApi<{ subscriptions?: Array<{ id: string; endpoint: string }> }>(
    fetchImpl,
    PUSH_SUBSCRIPTIONS_PATH,
  )
  return Array.isArray(body.subscriptions) ? body.subscriptions : []
}

// ---------------------------------------------------------------------------
// 订阅 / 退订(浏览器依赖注入)
// ---------------------------------------------------------------------------

export interface PushBrowserDeps {
  /** 浏览器通知授权;必须来自用户真实点击的调用栈。 */
  requestPermission: () => Promise<NotificationPermission>
  /** 当前通知授权状态。 */
  permission: () => NotificationPermission
  /** pushManager.subscribe 注入。 */
  subscribe: (applicationServerKey: Uint8Array<ArrayBuffer>) => Promise<PushSubscriptionLike>
  /** 当前已存订阅(无则 null)。 */
  getSubscription: () => Promise<PushSubscriptionLike | null>
  fetch: typeof fetch
}

export type PushEnableResult =
  | { ok: true; status: 'subscribed' }
  | { ok: false; status: 'unconfigured' | 'denied' | 'dismissed' | 'invalid' | 'error'; message?: string }

export type PushDisableResult =
  | { ok: true; status: 'unsubscribed' }
  | { ok: false; status: 'error'; message?: string }

/**
 * 开启后台推送:请求授权 → pushManager.subscribe → 提交 Relay。
 * requestPermission 必须在用户点击处理栈内被调用(浏览器强制)。
 */
export async function enablePush(deps: PushBrowserDeps): Promise<PushEnableResult> {
  let config: PushConfig
  try {
    config = await fetchPushConfig(deps.fetch)
  } catch (error) {
    return { ok: false, status: 'error', message: error instanceof Error ? error.message : '操作未完成，请稍后重试。' }
  }
  if (!config.enabled || !config.publicKey) return { ok: false, status: 'unconfigured' }
  const permission = await deps.requestPermission()
  if (permission !== 'granted') {
    return { ok: false, status: permission === 'denied' ? 'denied' : 'dismissed' }
  }
  let subscription: PushSubscriptionLike
  try {
    subscription = await deps.subscribe(urlBase64ToUint8Array(config.publicKey))
  } catch (error) {
    return { ok: false, status: 'error', message: error instanceof Error ? error.message : '操作未完成，请稍后重试。' }
  }
  const payload = buildSubscriptionPayload(subscription)
  if (!payload) return { ok: false, status: 'invalid' }
  try {
    await pushApi(deps.fetch, PUSH_SUBSCRIPTIONS_PATH, {
      method: 'POST',
      body: JSON.stringify(payload),
    }, true)
  } catch (error) {
    return { ok: false, status: 'error', message: error instanceof Error ? error.message : '操作未完成，请稍后重试。' }
  }
  return { ok: true, status: 'subscribed' }
}

/**
 * 关闭后台推送:退订浏览器订阅,并按 endpoint 对称删除 Relay 记录。
 * Relay 记录查不到(已清理)视为成功;删除失败如实上报。
 */
export async function disablePush(deps: PushBrowserDeps): Promise<PushDisableResult> {
  let subscription: PushSubscriptionLike | null
  try {
    subscription = await deps.getSubscription()
  } catch (error) {
    return { ok: false, status: 'error', message: error instanceof Error ? error.message : '操作未完成，请稍后重试。' }
  }
  if (!subscription) return { ok: true, status: 'unsubscribed' }
  const endpoint = subscription.endpoint
  let removedLocally = false
  try {
    removedLocally = await subscription.unsubscribe()
  } catch (error) {
    return { ok: false, status: 'error', message: error instanceof Error ? error.message : '操作未完成，请稍后重试。' }
  }
  if (!removedLocally) return { ok: false, status: 'error', message: '浏览器订阅退订失败。' }
  try {
    const records = await listSubscriptionIds(deps.fetch)
    const record = records.find((item) => item.endpoint === endpoint)
    if (record) {
      await pushApi(deps.fetch, `${PUSH_SUBSCRIPTIONS_PATH}/${encodeURIComponent(record.id)}`, {
        method: 'DELETE',
      }, true)
    }
  } catch (error) {
    return { ok: false, status: 'error', message: error instanceof Error ? error.message : '操作未完成，请稍后重试。' }
  }
  return { ok: true, status: 'unsubscribed' }
}

/** 设备页开关当前应显示的状态(浏览器 API 缺失或 Relay 未配置时禁用开关)。 */
export async function readPushToggleState(
  deps: Pick<PushBrowserDeps, 'permission' | 'getSubscription' | 'fetch'> & {
    pushSupported: boolean
  },
): Promise<PushToggleState> {
  if (!deps.pushSupported) return 'unsupported'
  try {
    const config = await fetchPushConfig(deps.fetch)
    if (!config.enabled || !config.publicKey) return 'unconfigured'
  } catch {
    return 'unconfigured'
  }
  if (deps.permission() === 'denied') return 'denied'
  try {
    const subscription = await deps.getSubscription()
    return subscription ? 'subscribed' : 'off'
  } catch {
    return 'off'
  }
}

// ---------------------------------------------------------------------------
// 推送打开后的"已关闭"解释(等待类事件;与站内 attentionClosureReminder 同措辞)
// ---------------------------------------------------------------------------

export const ATTENTION_CLOSED_MESSAGE = '一项待处理请求已关闭：可能已过期或已在本机处理。'

export interface PushOpenStore {
  isOnline(): boolean
  isSessionKnown(sessionId: string): boolean
  hasValidAttention(sessionId: string, kind: PushEventKind): boolean
  notifyClosed(message: string): void
}

/**
 * 等待类推送打开后的核对:等会话数据就绪,若已无对应有效待处理请求,
 * 给出"已过期或已在本机处理"解释;始终无法确认时不误报。
 */
export async function explainPushOpenIfResolved(input: {
  hint: PushOpenHint
  store: PushOpenStore
  delay?: (ms: number) => Promise<void>
  attempts?: number
}): Promise<boolean> {
  const { hint, store } = input
  const wait = input.delay ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)))
  if (!WAITING_KINDS.has(hint.kind)) return false
  const attempts = input.attempts ?? 20 // 20 × 500ms:连接与会话列表就绪窗口。
  for (let i = 0; i < attempts; i += 1) {
    if (store.isOnline() && store.isSessionKnown(hint.sessionId)) {
      if (!store.hasValidAttention(hint.sessionId, hint.kind)) {
        store.notifyClosed(ATTENTION_CLOSED_MESSAGE)
        return true
      }
      return false
    }
    await wait(500)
  }
  return false
}

/** 校验 SW 消息中的推送打开提示(仅接受同源 SW 投递的受限形状)。 */
export function parsePushOpenHint(data: unknown): PushOpenHint | null {
  if (!data || typeof data !== 'object') return null
  const record = data as Record<string, unknown>
  if (record.type !== PUSH_OPEN_MESSAGE_TYPE) return null
  const sessionId = record.sessionId
  const kind = record.kind
  if (typeof sessionId !== 'string' || !sessionId) return null
  if (typeof kind !== 'string' || !WAITING_KINDS.has(kind)) return null
  const openedAt = typeof record.openedAt === 'number' && Number.isFinite(record.openedAt)
    ? record.openedAt
    : Date.now()
  return { sessionId, kind: kind as PushEventKind, openedAt }
}

/**
 * 全局接线(应用启动时调用):监听 SW 的推送打开消息,并主动查询 SW 暂存的
 * 未读打开记录(通知点击新开窗口的场景)。返回清理函数。
 */
export function initPushOpenHandling(store: PushOpenStore): () => void {
  if (!('serviceWorker' in navigator)) return () => {}
  const handled = new Set<string>()
  const deliver = (hint: PushOpenHint): void => {
    const key = `${hint.sessionId}:${hint.kind}:${hint.openedAt}`
    if (handled.has(key)) return
    handled.add(key)
    void explainPushOpenIfResolved({ hint, store })
  }
  const onMessage = (event: MessageEvent): void => {
    const hint = parsePushOpenHint(event.data)
    if (hint) deliver(hint)
  }
  navigator.serviceWorker.addEventListener('message', onMessage)
  // 通知点击新开窗口:页面就绪后向 SW 查询暂存的打开记录(取后即清)。
  void navigator.serviceWorker.controller?.postMessage({ type: PUSH_OPEN_QUERY_MESSAGE_TYPE })
  return () => navigator.serviceWorker.removeEventListener('message', onMessage)
}

// ---------------------------------------------------------------------------
// 设备页接线(响应式封装;默认绑定真实浏览器 API,测试可注入)
// ---------------------------------------------------------------------------

async function pushRegistration(): Promise<ServiceWorkerRegistration> {
  const scope = import.meta.env.BASE_URL
  const existing = await navigator.serviceWorker.getRegistration(scope)
  return existing ?? navigator.serviceWorker.register(`${scope}sw.js`, { scope })
}

function realBrowserDeps(): PushBrowserDeps | null {
  if (!('serviceWorker' in navigator) || typeof Notification === 'undefined') return null
  return {
    requestPermission: () => Notification.requestPermission(),
    permission: () => Notification.permission,
    subscribe: async (key) =>
      (await pushRegistration()).pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: key }),
    getSubscription: async () => (await pushRegistration()).pushManager.getSubscription(),
    fetch: window.fetch,
  }
}

function pushStateHint(state: PushToggleState): string {
  switch (state) {
    case 'unsupported':
      return '当前浏览器不支持后台推送。'
    case 'unconfigured':
      return '推送服务未配置，暂时不可用。'
    case 'denied':
      return '通知权限已被拒绝；需在浏览器设置中恢复后再开启。'
    case 'subscribed':
      return '已开启；关闭页面后仍会收到任务提醒。'
    default:
      return ''
  }
}

function enableFailureMessage(result: Extract<PushEnableResult, { ok: false }>): string {
  switch (result.status) {
    case 'unconfigured':
    case 'denied':
      return pushStateHint(result.status)
    case 'dismissed':
      return '未授予通知权限，可再次点击开启。'
    case 'invalid':
      return '推送订阅无效，请重试。'
    default:
      return result.message ?? '开启失败，请稍后重试。'
  }
}

/** 设备页推送开关状态与操作(授权弹窗必须由用户点击的 change 事件触发)。 */
export function usePushNotifications() {
  const pushState = ref<PushToggleState>('off')
  const pushBusy = ref(false)
  const pushMessage = ref('')

  async function refreshPushState(): Promise<void> {
    const deps = realBrowserDeps()
    pushState.value = await readPushToggleState({
      pushSupported: deps !== null,
      permission: deps?.permission ?? (() => 'default'),
      getSubscription: deps?.getSubscription ?? (async () => null),
      fetch: window.fetch,
    })
    pushMessage.value = pushStateHint(pushState.value)
  }

  async function enableBrowserPush(): Promise<boolean> {
    const deps = realBrowserDeps()
    if (!deps) {
      pushState.value = 'unsupported'
      pushMessage.value = pushStateHint('unsupported')
      return false
    }
    pushBusy.value = true
    const result = await enablePush(deps)
    pushBusy.value = false
    if (result.ok) {
      pushState.value = 'subscribed'
      pushMessage.value = pushStateHint('subscribed')
      return true
    }
    if (result.status === 'denied') pushState.value = 'denied'
    pushMessage.value = enableFailureMessage(result)
    return false
  }

  async function disableBrowserPush(): Promise<boolean> {
    const deps = realBrowserDeps()
    if (!deps) return false
    pushBusy.value = true
    const result = await disablePush(deps)
    pushBusy.value = false
    if (result.ok) {
      pushState.value = 'off'
      pushMessage.value = '已关闭后台推送。'
      return true
    }
    pushMessage.value = result.message ?? '关闭失败，请稍后重试。'
    return false
  }

  async function toggleBrowserPush(): Promise<void> {
    if (pushBusy.value) return
    if (pushState.value === 'subscribed') await disableBrowserPush()
    else await enableBrowserPush()
  }

  return { pushState, pushBusy, pushMessage, refreshPushState, toggleBrowserPush }
}
