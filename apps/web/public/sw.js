const CACHE_NAME = 'agent-console-shell-v1'
const APP_SCOPE = '/agent-console/'
const SHELL = [APP_SCOPE, `${APP_SCOPE}manifest.webmanifest`, `${APP_SCOPE}icon.svg`]

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL)))
  self.skipWaiting()
})

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key)))),
  )
  self.clients.claim()
})

self.addEventListener('fetch', (event) => {
  const request = event.request
  const url = new URL(request.url)

  if (request.method !== 'GET' || url.origin !== self.location.origin || url.pathname.includes('/api/')) return

  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then((response) => response)
        .catch(() => caches.match(APP_SCOPE)),
    )
    return
  }

  if (!url.pathname.startsWith(APP_SCOPE)) return

  event.respondWith(
    caches.match(request).then(
      (cached) =>
        cached ||
        fetch(request).then((response) => {
          if (response.ok && ['script', 'style', 'font', 'image'].includes(request.destination)) {
            const copy = response.clone()
            void caches.open(CACHE_NAME).then((cache) => cache.put(request, copy))
          }
          return response
        }),
    ),
  )
})

// ---------------------------------------------------------------------------
// Web Push(UX-06):Relay 既有推送后端的浏览器端落地。
// payload 契约与 Relay /agent-console/api/push 一致:
//   { sessionId, kind, deepLink, title, body }
// 不含 ticket/批准参数;点击仅导航,打开后经正常认证与待处理状态核对才可操作。
// ---------------------------------------------------------------------------

const PUSH_APP_BASE = '/agent-console/'
const PUSH_KIND_BODIES = {
  turnCompleted: '任务已完成',
  turnFailed: '任务未完成,已失败',
  turnInterrupted: '任务已中断',
  waitingQuestion: '任务在等待你的回答',
  waitingApproval: '任务在等待风险审批',
}
const PUSH_OPEN_CACHE = 'agent-console-push-open-v1'
const PUSH_OPEN_TTL_MS = 5 * 60 * 1000
const PUSH_OPEN_MESSAGE_TYPE = 'agent-console/push-open'
const PUSH_OPEN_QUERY_MESSAGE_TYPE = 'agent-console/push-open-query'

/**
 * 仅接受解析后仍落在应用 scope 内的同源相对 deep link。
 * 用同源比较而非字符串前缀:WHATWG URL 会把 "\" 归一为 "/",`/\\evil.com`
 * 这类输入可绕过 `startsWith('/') && !startsWith('//')` 构造 scheme-relative
 * 跨源地址。解析失败或跨源/scope 外一律拒绝,调用方回退站内既有路由。
 */
function safePushDeepLink(raw) {
  if (typeof raw !== 'string' || !raw) return ''
  let url
  try {
    url = new URL(raw, self.location.origin)
  } catch (error) {
    return ''
  }
  if (url.origin !== self.location.origin) return ''
  if (!url.pathname.startsWith(APP_SCOPE)) return ''
  return `${url.pathname}${url.search}${url.hash}`
}

/** 解析 Relay payload;非法输入返回 null(调用方展示兜底通知)。 */
function parsePushPayload(raw) {
  let data
  try {
    data = typeof raw === 'string' ? JSON.parse(raw) : raw
  } catch (error) {
    return null
  }
  if (!data || typeof data !== 'object' || Array.isArray(data)) return null
  if (typeof data.sessionId !== 'string' || !data.sessionId) return null
  const kind =
    typeof data.kind === 'string' && Object.prototype.hasOwnProperty.call(PUSH_KIND_BODIES, data.kind)
      ? data.kind
      : ''
  const deepLink = safePushDeepLink(data.deepLink)
  const body =
    typeof data.body === 'string' && data.body
      ? data.body
      : kind
        ? PUSH_KIND_BODIES[kind]
        : '收到新的任务提醒'
  const title = typeof data.title === 'string' && data.title ? data.title : 'Agent Console'
  return { sessionId: data.sessionId, kind, deepLink, title, body }
}

/** 推送点击目标:安全相对 deep link,缺省回退到站内既有路由 /s/{sessionId}。 */
function pushTargetUrl(payload) {
  const path = payload.sessionId
    ? payload.deepLink || `${PUSH_APP_BASE}s/${encodeURIComponent(payload.sessionId)}`
    : PUSH_APP_BASE
  return new URL(path, self.location.origin).toString()
}

function showPushNotification(payload) {
  if (typeof self.registration.showNotification !== 'function') {
    // 无通知 API 环境:无法展示,仅记录(不阻塞 push 事件)。
    return Promise.resolve()
  }
  return self.registration.showNotification(payload.title, {
    body: payload.body,
    tag: `agent-console-${payload.sessionId}-${payload.kind || 'event'}`,
    data: { sessionId: payload.sessionId, kind: payload.kind, deepLink: payload.deepLink },
  })
}

self.addEventListener('push', (event) => {
  let payload = null
  try {
    payload = parsePushPayload(event.data ? event.data.text() : null)
  } catch (error) {
    payload = null
  }
  // 解析失败也必须可见(push userVisibleOnly 契约):展示不含任何内容的兜底通知。
  const safe =
    payload || { sessionId: '', kind: '', deepLink: '', title: 'Agent Console', body: '收到新的任务提醒' }
  event.waitUntil(showPushNotification(safe))
})

/** 等待类事件的打开提示暂存(供新开窗口的页面查询;取后即清,5 分钟过期)。 */
async function rememberPushOpen(payload) {
  if (!payload.sessionId) return
  if (payload.kind !== 'waitingQuestion' && payload.kind !== 'waitingApproval') return
  try {
    const cache = await caches.open(PUSH_OPEN_CACHE)
    const url = new URL(PUSH_APP_BASE, self.location.origin)
    await cache.put(url, new Response(JSON.stringify({
      sessionId: payload.sessionId,
      kind: payload.kind,
      openedAt: Date.now(),
    })))
  } catch (error) {
    // 暂存失败不影响导航本身。
  }
}

async function takePendingPushOpen() {
  try {
    const cache = await caches.open(PUSH_OPEN_CACHE)
    const url = new URL(PUSH_APP_BASE, self.location.origin)
    const cached = await cache.match(url)
    if (!cached) return null
    await cache.delete(url)
    const hint = await cached.json()
    if (hint && hint.sessionId && Date.now() - hint.openedAt <= PUSH_OPEN_TTL_MS) return hint
  } catch (error) {
    // 无 Cache API 或缓存异常:视为无未读提示。
  }
  return null
}

async function forgetPushOpen() {
  try {
    const cache = await caches.open(PUSH_OPEN_CACHE)
    await cache.delete(new URL(PUSH_APP_BASE, self.location.origin))
  } catch (error) {
    // 忽略:暂存有 TTL 兜底。
  }
}

/**
 * 通知点击:只做导航/聚焦,不带任何批准凭据。
 * - 新开窗口:暂存等待类提示,新页面启动后经 push-open-query 拉取;
 * - 已有窗口需跳转:同样暂存后导航(新页面查询),避免向旧文档投递;
 * - 已在目标页/无法导航:聚焦并直接投递消息。
 * 打开后由页面经正常认证与待处理状态核对,等待类请求已关闭时给出解释。
 */
async function openPushTarget(payload) {
  const target = pushTargetUrl(payload)
  const appBase = new URL(PUSH_APP_BASE, self.location.origin).toString()
  const clientList = await self.clients.matchAll({ type: 'window', includeUncontrolled: true })
  const existing = clientList.find((client) => client.url.startsWith(appBase))
  if (!existing) {
    await rememberPushOpen(payload)
    await self.clients.openWindow(target)
    return
  }
  const message = {
    type: PUSH_OPEN_MESSAGE_TYPE,
    sessionId: payload.sessionId,
    kind: payload.kind,
    openedAt: Date.now(),
  }
  if (existing.url !== target && typeof existing.navigate === 'function') {
    await rememberPushOpen(payload)
    try {
      await existing.navigate(target)
      return
    } catch (error) {
      await forgetPushOpen()
    }
  }
  existing.focus()
  existing.postMessage(message)
}

self.addEventListener('notificationclick', (event) => {
  const data = event.notification.data && typeof event.notification.data === 'object'
    ? event.notification.data
    : {}
  const payload = {
    sessionId: typeof data.sessionId === 'string' ? data.sessionId : '',
    kind: typeof data.kind === 'string' ? data.kind : '',
    deepLink: typeof data.deepLink === 'string' ? data.deepLink : '',
  }
  event.notification.close()
  event.waitUntil(openPushTarget(payload))
})

self.addEventListener('message', (event) => {
  const data = event.data
  if (!data || typeof data !== 'object' || data.type !== PUSH_OPEN_QUERY_MESSAGE_TYPE) return
  const source = event.source
  event.waitUntil(
    takePendingPushOpen().then((hint) => {
      if (hint && source && typeof source.postMessage === 'function') {
        source.postMessage({ type: PUSH_OPEN_MESSAGE_TYPE, ...hint })
      }
    }),
  )
})
