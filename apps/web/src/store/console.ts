import { computed, reactive } from 'vue'

import {
  consoleProtocolVersion,
  consoleWebCommit,
  loadRelayVersion,
  loadToolboxVersion,
} from '@/lib/build-info'
import { getOperationAvailability } from '@/transport/capabilities'
import { FixtureConsoleTransport } from '@/transport/fixture-transport'
import {
  AuthRequiredError,
  loginUrl,
  RealConsoleTransport,
  RuntimeSnapshotUnavailableError,
} from '@/transport/real-transport'
import { reduceOutput, shouldHydrateFinalOutput } from '@/transport/output-reducer'
import {
  isTerminalReceipt,
  shouldApplyReceipt,
  shouldRefreshRuntimeAfterReceipt,
} from '@/transport/receipt-policy'
import type {
  AttentionItem,
  AttentionKind,
  CapabilitySnapshot,
  CommandReceipt,
  ConsoleEvent,
  ConsoleTransport,
  ControlOperation,
  DeviceConnection,
  DevicePresence,
  DeviceSummary,
  FileMetadata,
  GitFileDiff,
  GitSummary,
  OutputState,
  PairingChallenge,
  ReceiptStatus,
  RelayLinkState,
  RuntimeSettings,
  RuntimeSnapshot,
  SessionSummary,
  TimelineItem,
  UploadResult,
} from '@/transport/types'
import type { AnswerState } from '@/transport/types'

export type { AnswerState } from '@/transport/types'

interface ToastMessage {
  id: string
  message: string
  tone: 'info' | 'success' | 'warning' | 'danger'
  /** 站内路由标识:点击提醒经正常认证进入会话,不直接携带可批准卡片(UX-06)。 */
  linkTo?: string
}

interface PendingCommandContext {
  sessionId: string
  operation: ControlOperation
  baselineRevision: number
}

export interface ExpiredAttentionNotice {
  id: string
  sessionId: string
  at: string
}

interface ConsoleState {
  initialized: boolean
  connection: DeviceConnection
  link: RelayLinkState
  sessions: SessionSummary[]
  sessionsCursor?: string
  sessionsLoading: boolean
  devices: DeviceSummary[]
  devicesLoading: boolean
  runtimes: Record<string, RuntimeSnapshot>
  historyCursors: Record<string, string | undefined>
  historyLoading: Record<string, boolean>
  receipts: CommandReceipt[]
  toasts: ToastMessage[]
  resolvedAttentionIds: string[]
  answerStates: Record<string, AnswerState>
  expiredNotices: ExpiredAttentionNotice[]
  drafts: Record<string, string>
  notifySuccessEnabled: boolean
  /** Relay 版本端点结果(IN-01):空串表示尚未取到,展示为"未知"。 */
  relayVersions: { relay: string; protocol: string }
  /** Toolbox 版本端点结果(IN-01):空串表示尚未取到或占位值,展示为"未知"。 */
  toolboxVersions: { app: string; commit: string }
  fixtureMode: boolean
  startupError?: string
}

function fixtureRequested(): boolean {
  const query = new URLSearchParams(window.location.search)
  return import.meta.env.VITE_AGENT_CONSOLE_FIXTURE === 'true' || query.get('fixture') === '1'
}

const fixtureMode = fixtureRequested()
const transport: ConsoleTransport = fixtureMode
  ? new FixtureConsoleTransport()
  : new RealConsoleTransport()

/** 成功完成提醒的用户开关(UX-06);只存布尔偏好,不存任何内容。 */
const NOTIFY_SUCCESS_KEY = 'agent-console.notify-success'

function readNotifySuccessSetting(): boolean {
  try {
    return window.localStorage.getItem(NOTIFY_SUCCESS_KEY) === '1'
  } catch {
    return false
  }
}

function writeNotifySuccessSetting(enabled: boolean): void {
  try {
    if (enabled) window.localStorage.setItem(NOTIFY_SUCCESS_KEY, '1')
    else window.localStorage.removeItem(NOTIFY_SUCCESS_KEY)
  } catch {
    // 存储不可用时开关只在当前会话内生效。
  }
}

const state = reactive<ConsoleState>({
  initialized: false,
  connection: 'CONNECTING',
  link: { state: 'CONNECTING', stage: 'TICKET', updatedAt: '' },
  sessions: [],
  sessionsLoading: false,
  devices: [],
  devicesLoading: false,
  runtimes: {},
  historyCursors: {},
  historyLoading: {},
  receipts: [],
  toasts: [],
  resolvedAttentionIds: [],
  answerStates: {},
  expiredNotices: [],
  drafts: {},
  notifySuccessEnabled: readNotifySuccessSetting(),
  relayVersions: { relay: '', protocol: '' },
  toolboxVersions: { app: '', commit: '' },
  fixtureMode,
})

let disconnectTransport: (() => void) | undefined
const pendingCommandContexts = new Map<string, PendingCommandContext>()
/** 已提交回复的审批 → 所属会话:所属轮次失败时标记"已允许，执行失败"(UX-05)。 */
const answeredApprovalSessions = new Map<string, string>()
/** 详情视图对会话订阅的引用计数:归零才释放 transport 订阅。 */
const detailRefs = new Map<string, number>()
/** 在途输出补全的取消句柄(key: `${sessionId}:${itemId}`),离开详情时取消。 */
const outputLoads = new Map<string, AbortController>()
const reconcileTimers = new Map<number, () => void>()
const START_RECONCILE_DELAYS_MS = [0, 150, 300, 600, 1_200] as const
const INTERRUPT_RECONCILE_DELAYS_MS = [0, 250, 500, 1_000, 2_000, 4_000, 8_000, 12_000] as const

async function initialize(): Promise<void> {
  if (state.initialized) return
  state.initialized = true
  delete state.startupError
  // Relay/Toolbox 版本端点与会话/WS 无关:各自独立拉取一次,失败静默保持"未知"(IN-01)。
  void refreshRelayVersion()
  void refreshToolboxVersion()
  try {
    disconnectTransport = await transport.connect(handleEvent)
    await Promise.all([loadMoreSessions(), loadDevices()])
    // Attention 正文和原生 option ID 只在 RuntimeSnapshot 中；仅对摘要明确
    // 有待处理且在线的会话读取一次，不扫描无关注会话历史。
    await Promise.all(
      state.sessions
        .filter((session) => session.attentionCount > 0 && session.deviceConnection === 'ONLINE')
        .map((session) => ensureRuntime(session.id).catch(() => undefined)),
    )
  } catch (error) {
    state.initialized = false
    if (error instanceof AuthRequiredError) {
      redirectToLogin()
      return
    }
    state.startupError = error instanceof Error ? error.message : 'Agent Console 初始化失败。'
    pushToast(state.startupError, 'danger')
  }
}

function dispose(): void {
  disconnectTransport?.()
  disconnectTransport = undefined
  pendingCommandContexts.clear()
  for (const [timer, resolve] of reconcileTimers) {
    window.clearTimeout(timer)
    resolve()
  }
  reconcileTimers.clear()
  state.initialized = false
}

async function loadMoreSessions(): Promise<void> {
  if (state.sessionsLoading || (state.sessions.length > 0 && !state.sessionsCursor)) return
  state.sessionsLoading = true
  try {
    const page = await transport.listSessions(state.sessionsCursor)
    mergeSessions(page.items, false)
    if (page.nextCursor) state.sessionsCursor = page.nextCursor
    else delete state.sessionsCursor
  } finally {
    state.sessionsLoading = false
  }
}

async function loadDevices(): Promise<void> {
  if (state.devicesLoading) return
  state.devicesLoading = true
  try {
    state.devices = await transport.getDevices()
  } finally {
    state.devicesLoading = false
  }
}

async function ensureRuntime(
  sessionId: string,
  force = false,
  includeHistory = true,
): Promise<RuntimeSnapshot> {
  const existing = state.runtimes[sessionId]
  if (existing && !force) return existing
  const summary = state.sessions.find((session) => session.id === sessionId)
  if (summary && effectiveConnection(summary) === 'OFFLINE') {
    const runtime = offlineRuntime(summary)
    state.runtimes[sessionId] = runtime
    delete state.historyCursors[sessionId]
    return runtime
  }
  let incoming: RuntimeSnapshot
  try {
    incoming = await transport.getRuntimeSnapshot(sessionId, { includeHistory })
  } catch (error) {
    if (!(includeHistory && summary && error instanceof RuntimeSnapshotUnavailableError)) {
      throw error
    }
    const runtime = offlineRuntime(summary)
    runtime.unavailable = { code: error.code, message: error.message }
    runtime.timeline = error.history.items
    if (error.history.nextCursor) runtime.historyNextCursor = error.history.nextCursor
    state.runtimes[sessionId] = runtime
    if (runtime.historyNextCursor) state.historyCursors[sessionId] = runtime.historyNextCursor
    else delete state.historyCursors[sessionId]
    return runtime
  }
  const runtime = reconcileRuntimeSnapshot(state.runtimes[sessionId], incoming, includeHistory)
  state.runtimes[sessionId] = runtime
  if (includeHistory) {
    if (runtime.historyNextCursor) state.historyCursors[sessionId] = runtime.historyNextCursor
    else delete state.historyCursors[sessionId]
    prepareDeferredOutputs(runtime)
  }
  // 后续回执会修改此运行态；返回 store 中的代理，确保队列等计算属性同步更新。
  return state.runtimes[sessionId]!
}

/** 详情视图进入:计数持有该会话订阅。 */
function acquireRuntime(sessionId: string): void {
  detailRefs.set(sessionId, (detailRefs.get(sessionId) ?? 0) + 1)
}

/** 详情视图离开:引用归零后释放订阅;有待处理注意项的会话由后台任务继续持有。 */
function releaseRuntime(sessionId: string): void {
  const refs = (detailRefs.get(sessionId) ?? 0) - 1
  if (refs > 0) {
    detailRefs.set(sessionId, refs)
    return
  }
  detailRefs.delete(sessionId)
  const summary = state.sessions.find((item) => item.id === sessionId)
  if (summary && summary.attentionCount > 0 && summary.deviceConnection === 'ONLINE') return
  for (const [key, controller] of outputLoads) {
    if (!key.startsWith(`${sessionId}:`)) continue
    controller.abort()
    outputLoads.delete(key)
  }
  transport.releaseRuntime(sessionId)
}

async function loadOlderHistory(sessionId: string): Promise<void> {
  const runtime = await ensureRuntime(sessionId)
  const cursor = state.historyCursors[sessionId]
  if (!cursor || state.historyLoading[sessionId]) return
  state.historyLoading[sessionId] = true
  try {
    const page = await transport.getHistory(sessionId, cursor)
    const known = new Set(runtime.timeline.map((item) => item.id))
    runtime.timeline.unshift(...page.items.filter((item) => !known.has(item.id)))
    if (page.nextCursor) state.historyCursors[sessionId] = page.nextCursor
    else delete state.historyCursors[sessionId]
  } finally {
    state.historyLoading[sessionId] = false
  }
}

function presenceFor(sessionId?: string): DevicePresence {
  const summary = state.sessions.find((session) => session.id === sessionId)
  const device = state.devices.find((item) => item.id === summary?.deviceId)
  if (!summary) {
    return (
      device ?? {
        id: '',
        displayName: 'Bridge',
        connection: state.connection,
        controlMode: 'UNAVAILABLE',
        compatibility: 'DEGRADED',
        lastSeenAt: '',
        bridgeVersion: '',
        platform: '',
        architecture: '',
      }
    )
  }
  return {
    id: summary.deviceId,
    displayName: device?.displayName ?? 'Bridge',
    connection: effectiveConnection(summary),
    controlMode: summary.controlMode,
    compatibility: summary.compatibility,
    lastSeenAt: summary.deviceLastSeenAt ?? device?.lastSeenAt ?? '',
    bridgeVersion: device?.bridgeVersion ?? '',
    platform: device?.platform ?? '',
    architecture: device?.architecture ?? '',
    ...(summary.degradedReason ? { degradedReason: summary.degradedReason } : {}),
  }
}

function availability(sessionId: string, operation: ControlOperation) {
  const runtime = state.runtimes[sessionId]
  const presence = presenceFor(sessionId)
  if (!runtime) return { enabled: false, reason: '正在读取当前能力。' }
  if (operation === 'OPEN_FILE' || operation === 'READ_GIT_DIFF') {
    return presence.connection === 'ONLINE'
      ? { enabled: true }
      : { enabled: false, reason: '设备未在线，无法读取本机数据。', code: 'DEVICE_OFFLINE' as const }
  }
  const capability = getOperationAvailability(
    operation,
    presence.connection,
    presence.controlMode,
    presence.compatibility,
    runtime.capabilities,
  )
  return capability.enabled ? runtimePrecondition(runtime, operation) : capability
}

function runtimePrecondition(runtime: RuntimeSnapshot, operation: ControlOperation) {
  const validAttention = runtime.attention.some((item) => item.valid)
  if (operation === 'START_TURN' && (runtime.phase !== 'IDLE' || validAttention)) {
    return { enabled: false, reason: validAttention ? '请先处理当前问题或审批。' : '当前轮次尚未结束。' }
  }
  if (operation === 'SET_QUEUE' && (runtime.phase !== 'RUNNING' || runtime.queue.status !== 'EMPTY')) {
    return { enabled: false, reason: '只有运行中且队列为空时才能排入下一轮。' }
  }
  if (
    (operation === 'REPLACE_QUEUE' || operation === 'CANCEL_QUEUE') &&
    runtime.queue.status === 'EMPTY'
  ) {
    return { enabled: false, reason: '当前没有下一轮队列。' }
  }
  if ((operation === 'STEER' || operation === 'INTERRUPT') && runtime.phase !== 'RUNNING') {
    return { enabled: false, reason: '当前没有正在运行的轮次。' }
  }
  if (
    operation === 'ANSWER_QUESTION' &&
    !runtime.attention.some((item) => item.kind === 'USER_QUESTION' && item.valid)
  ) {
    return { enabled: false, reason: '问题已失效或不存在。' }
  }
  if (
    operation === 'ANSWER_APPROVAL' &&
    !runtime.attention.some((item) => item.kind === 'RISK_APPROVAL' && item.valid)
  ) {
    return { enabled: false, reason: '审批已失效或不存在。' }
  }
  return { enabled: true }
}

async function sendCommand(
  sessionId: string,
  operation: ControlOperation,
  payload: Record<string, unknown> = {},
): Promise<CommandReceipt | undefined> {
  // 写入必须基于发送时的权威 revision；运行中输出会持续推进修订，不能
  // 复用打开详情页时缓存的快照，否则 Steer/Interrupt 会稳定变成 STALE_TURN。
  const runtime = await ensureRuntime(sessionId, true, false)
  const allowed = availability(sessionId, operation)
  if (!allowed.enabled) {
    pushToast(allowed.reason ?? '当前操作不可用。', 'warning')
    return undefined
  }
  const requestId = crypto.randomUUID()
  pendingCommandContexts.set(requestId, {
    sessionId,
    operation,
    baselineRevision: runtime.runtimeRevision,
  })
  try {
    const receipt = await transport.sendCommand({
      requestId,
      operation,
      sessionId,
      ...(runtime.activeTurnId ? { expectedTurnId: runtime.activeTurnId } : {}),
      expectedRuntimeRevision: runtime.runtimeRevision,
      payload,
    })
    recordReceipt(receipt)
    if (shouldApplyReceipt(receipt.status)) {
      applyLocalCommand(runtime, operation, payload)
      pushToast(
        receipt.status === 'ACCEPTED_BY_BRIDGE' ? 'Bridge 已接受操作。' : '操作状态已更新。',
        'success',
      )
    } else {
      pushToast(receiptMessage(receipt), 'warning')
    }
    return receipt
  } catch (error) {
    pendingCommandContexts.delete(requestId)
    if (error instanceof AuthRequiredError) redirectToLogin()
    pushToast(error instanceof Error ? error.message : '操作未发送。', 'danger')
    return undefined
  }
}

async function updateSetting(
  sessionId: string,
  key: keyof RuntimeSettings,
  value: string,
): Promise<void> {
  const runtime = state.runtimes[sessionId]
  if (!runtime) return
  const receipt = await sendCommand(sessionId, 'UPDATE_SETTINGS', { key, optionId: key, value })
  if (receipt && shouldApplyReceipt(receipt.status)) runtime.settings[key] = value
}

function toggleConnection(): void {
  if (!state.fixtureMode) return
  state.connection = state.connection === 'ONLINE' ? 'OFFLINE' : 'ONLINE'
  pushToast(
    state.connection === 'ONLINE' ? 'Fixture 已模拟 Bridge 重连。' : 'Fixture 已模拟设备断线。',
    state.connection === 'ONLINE' ? 'success' : 'warning',
  )
}

async function answerAttention(
  attentionId: string,
  optionId: string,
  freeText?: string,
): Promise<void> {
  const attention = pendingAttention.value.find((item) => item.id === attentionId)
  if (!attention || !attention.valid) return
  const operation = attention.kind === 'RISK_APPROVAL' ? 'ANSWER_APPROVAL' : 'ANSWER_QUESTION'
  const payload: Record<string, unknown> = { attentionId, optionId }
  // 自由文本只按原生 allowFreeText 传递;secret 型答案只驻内存,不落任何持久化。
  if (freeText?.trim()) payload.freeText = freeText.trim()
  const receipt = await sendCommand(attention.sessionId, operation, payload)
  if (receipt) {
    state.answerStates[attentionId] = {
      requestId: receipt.requestId,
      submittedStatus: receipt.status,
      ...(receipt.errorCode ? { errorCode: receipt.errorCode } : {}),
      at: new Date().toISOString(),
    }
    if (attention.kind === 'RISK_APPROVAL') {
      answeredApprovalSessions.set(attentionId, attention.sessionId)
    }
  }
  if (
    receipt &&
    shouldApplyReceipt(receipt.status) &&
    !state.resolvedAttentionIds.includes(attentionId)
  ) {
    state.resolvedAttentionIds.push(attentionId)
  }
}

async function lookupPairing(shortCode: string): Promise<PairingChallenge | undefined> {
  try {
    return await transport.lookupPairing(shortCode)
  } catch (error) {
    pushToast(error instanceof Error ? error.message : '绑定码无效。', 'danger')
    return undefined
  }
}

async function approvePairing(shortCode: string, challengeId?: string): Promise<boolean> {
  try {
    await transport.approvePairing(shortCode, challengeId)
    pushToast('已批准设备绑定，等待 Bridge 上线。', 'success')
    window.setTimeout(() => void loadDevices(), 350)
    return true
  } catch (error) {
    pushToast(error instanceof Error ? error.message : '批准绑定失败。', 'danger')
    return false
  }
}

async function revokeDevice(deviceId: string): Promise<boolean> {
  try {
    await transport.revokeDevice(deviceId)
    await loadDevices()
    pushToast('设备已撤销。', 'success')
    return true
  } catch (error) {
    pushToast(error instanceof Error ? error.message : '撤销设备失败。', 'danger')
    return false
  }
}

const getGitSummary = (sessionId: string): Promise<GitSummary> => transport.getGitSummary(sessionId)
const getGitDiff = (sessionId: string, path: string, staged = false): Promise<GitFileDiff> =>
  transport.getGitDiff(sessionId, path, staged)
const getFileMetadata = (sessionId: string, handle: string): Promise<FileMetadata> =>
  transport.getFileMetadata(sessionId, handle)
const previewFile = (sessionId: string, handle: string, fileName?: string): Promise<Response> =>
  transport.previewFile(sessionId, handle, fileName)
const downloadFile = (sessionId: string, handle: string, fileName?: string): Promise<Response> =>
  transport.downloadFile(sessionId, handle, fileName)
const uploadFile = (sessionId: string, file: File): Promise<UploadResult> =>
  transport.uploadFile(sessionId, file)

function applyLocalCommand(
  runtime: RuntimeSnapshot,
  operation: ControlOperation,
  payload: Record<string, unknown>,
): void {
  if (operation === 'SET_QUEUE' || operation === 'REPLACE_QUEUE') {
    runtime.queue.status = 'QUEUED'
    runtime.queue.text = String(payload.text ?? '')
    if (runtime.activeTurnId) runtime.queue.afterTurnId = runtime.activeTurnId
  }
  if (operation === 'CANCEL_QUEUE') runtime.queue = { status: 'EMPTY' }
  if (operation === 'ANSWER_QUESTION' || operation === 'ANSWER_APPROVAL') {
    const attentionId = String(payload.attentionId ?? '')
    runtime.attention = runtime.attention.filter((item) => item.id !== attentionId)
    runtime.timeline = runtime.timeline.filter(
      (item) => item.type !== 'attention' || item.attention.id !== attentionId,
    )
  }
}

function handleEvent(event: ConsoleEvent): void {
  if (event.type === 'connection') {
    state.connection = event.state
    return
  }
  if (event.type === 'link') {
    state.link = event.link
    if (event.link.state === 'ONLINE') {
      delete state.startupError
      // 首连失败后经重试恢复:HTTP 设备列表在此补一次,任务列表由 WS 列表快照提供。
      if (!state.devices.length) void loadDevices().catch(() => undefined)
    }
    return
  }
  if (event.type === 'auth-expired') {
    redirectToLogin()
    return
  }
  if (event.type === 'sessions') {
    mergeSessions(event.sessions, event.snapshot)
    // 列表快照到达证明至少一台 Bridge 已上线;设备列表仍为空时(首次
    // 配对后 presence 帧可能被 Relay 的首快照守卫跳过)顺带补拉一次,
    // 设备无需手动刷新即可出现。
    if (event.snapshot && event.sessions.length > 0 && !state.devices.length) {
      void loadDevices().catch(() => undefined)
    }
    return
  }
  if (event.type === 'receipt') {
    recordReceipt(event.receipt)
    return
  }
  if (event.type === 'resync-required') {
    pushToast(`需要重新同步：${event.reason}`, 'warning')
    return
  }
  if (event.type === 'device-presence') {
    for (const session of state.sessions) {
      if (session.deviceId !== event.deviceId) continue
      session.deviceConnection = event.connection
      if (event.lastSeenAt) session.deviceLastSeenAt = event.lastSeenAt
      if (event.degradedReason) session.degradedReason = event.degradedReason
      else delete session.degradedReason
    }
    const device = state.devices.find((item) => item.id === event.deviceId)
    if (device) {
      device.connection = event.connection
      if (event.lastSeenAt) device.lastSeenAt = event.lastSeenAt
      if (event.degradedReason) device.degradedReason = event.degradedReason
      else delete device.degradedReason
    } else if (event.connection === 'ONLINE') {
      // 尚未进入本地设备列表的设备出现上线 presence(首次配对后 Bridge
      // 上线):拉一次列表让设备自动出现,不依赖手动刷新页面。
      void loadDevices().catch(() => undefined)
    }
    return
  }

  const runtime = state.runtimes[event.sessionId]
  if (event.type === 'runtime-snapshot') {
    state.runtimes[event.sessionId] = reconcileRuntimeSnapshot(runtime, event.runtime, false)
    return
  }
  if (!runtime) return
  if (event.type === 'timeline-upsert') {
    runtime.timeline = mergeTimeline(runtime.timeline, [event.item])
    return
  }
  if (event.type === 'turn-lifecycle') {
    runtime.phase = event.phase
    if (event.turnId && event.phase !== 'IDLE') runtime.activeTurnId = event.turnId
    if (event.phase === 'IDLE') delete runtime.activeTurnId
    const session = state.sessions.find((item) => item.id === event.sessionId)
    if (session) {
      session.phase = event.phase
      if (event.outcome) session.lastOutcome = event.outcome
    }
    if (event.outcome === 'FAILED') {
      applyExecutionFailureToApprovals(state.answerStates, answeredApprovalSessions, event.sessionId)
      notifyOutcome(event.sessionId, 'FAILED')
    } else if (event.outcome === 'COMPLETED') {
      notifyOutcome(event.sessionId, 'COMPLETED')
    }
    return
  }
  if (event.type === 'attention-added') {
    runtime.attention = [
      ...runtime.attention.filter((item) => item.id !== event.attention.id),
      event.attention,
    ]
    runtime.timeline = mergeTimeline(runtime.timeline, [
      {
        id: `attention-${event.attention.id}`,
        type: 'attention',
        createdAt: event.attention.createdAt,
        attention: event.attention,
      },
    ])
    notifyAttentionAdded(event.sessionId)
    return
  }
  if (event.type === 'attention-removed') {
    runtime.attention = runtime.attention.filter((item) => item.id !== event.attentionId)
    runtime.timeline = runtime.timeline.filter(
      (item) => item.type !== 'attention' || item.attention.id !== event.attentionId,
    )
    recordAttentionClosure(event.sessionId, event.attentionId)
    return
  }
  if (event.type === 'queue-changed') {
    runtime.queue = event.queue
    return
  }
  if (event.type === 'capabilities-changed') {
    runtime.capabilities = event.capabilities
    return
  }
  if (event.type === 'output') {
    const hasOutputItem = runtime.timeline.some(
      (item) =>
        (item.type === 'command' || item.type === 'outcome-unknown') &&
        item.output.itemId === event.event.itemId,
    )
    if (!hasOutputItem) {
      runtime.timeline.push({
        id: `output-${event.event.itemId}`,
        type: 'command',
        createdAt: '',
        command: '命令输出',
        cwdDisplay: '',
        status: event.event.type === 'final' ? 'COMPLETED' : 'RUNNING',
        elapsed: '',
        output: {
          itemId: event.event.itemId,
          revision: 0,
          text: '',
          byteLength: 0,
          isFinal: false,
          authority: 'LIVE_PREVIEW',
          hasGap: false,
        },
      })
    }
    runtime.timeline = runtime.timeline.map((item) => reduceTimelineOutput(item, event.event))
    if (event.event.type === 'final') {
      runtime.timeline = runtime.timeline.map((item) =>
        item.type === 'command' && item.output.itemId === event.event.itemId
          ? { ...item, status: 'COMPLETED' }
          : item,
      )
    }
    const output = runtime.timeline
      .filter((item) => item.type === 'command' || item.type === 'outcome-unknown')
      .map((item) => item.output)
      .find((item) => item.itemId === event.event.itemId)
    if (output?.isFinal && !output.hasGap) delete output.loadState
    if (!output?.hasGap) return
    if (event.event.type === 'final') {
      const finalEvent = event.event
      const controller = new AbortController()
      const loadKey = `${event.sessionId}:${finalEvent.itemId}`
      outputLoads.set(loadKey, controller)
      output.loadState = 'LOADING'
      void transport
        .getOutputText(event.sessionId, finalEvent.itemId, { signal: controller.signal })
        .then((text) => {
          handleEvent({
            type: 'output',
            sessionId: event.sessionId,
            event: {
              type: 'replace',
              itemId: finalEvent.itemId,
              revision: finalEvent.revision,
              text,
            },
          })
          handleEvent(event)
        })
        .catch(() => {
          // 取消(已离开详情)不是失败:保持 DEFERRED 等待再次进入;真失败保留已有内容。
          output.loadState = controller.signal.aborted ? 'DEFERRED' : 'FAILED'
        })
        .finally(() => {
          if (outputLoads.get(loadKey) === controller) outputLoads.delete(loadKey)
        })
    } else {
      transport.requestResync(event.sessionId)
    }
  }
}

export function mergeSessions(incoming: SessionSummary[], snapshot: boolean): void {
  const seenIds = new Set<string>()
  for (const session of incoming) {
    // 归属去重必须含 agentKind:同机同 nativeSessionId 的双 Agent 会话
    // (Codex/ZCode)是两个不同对象,不得互相覆盖(ZC-02)。
    const existing = state.sessions.find(
      (item) =>
        item.id === session.id ||
        (item.deviceId === session.deviceId &&
          item.agentKind === session.agentKind &&
          item.nativeSessionId === session.nativeSessionId),
    )
    let targetId = session.id
    if (existing) {
      const oldId = existing.id
      const preferredId = isUuid(session.id) ? session.id : existing.id
      targetId = preferredId
      Object.assign(existing, session, { id: preferredId })
      if (oldId !== preferredId) {
        if (state.runtimes[oldId]) {
          state.runtimes[preferredId] = state.runtimes[oldId]
          state.runtimes[preferredId]!.sessionId = preferredId
          delete state.runtimes[oldId]
        }
        if (oldId in state.historyCursors) {
          state.historyCursors[preferredId] = state.historyCursors[oldId]
          delete state.historyCursors[oldId]
        }
        if (oldId in state.historyLoading) {
          state.historyLoading[preferredId] = state.historyLoading[oldId] ?? false
          delete state.historyLoading[oldId]
        }
      }
    }
    else state.sessions.push(session)
    seenIds.add(targetId)
    if (
      session.attentionCount > 0 &&
      session.deviceConnection === 'ONLINE' &&
      !state.runtimes[targetId]
    ) {
      queueMicrotask(() => void ensureRuntime(targetId).catch(() => undefined))
    }
  }
  if (snapshot) {
    const removedIds = state.sessions
      // 列表 snapshot 与详情首屏可能并发到达；已打开详情的 runtime
      // 不能因分页/ID 归一化时暂未出现在本批列表里而被一起删除。
      .filter((session) => !seenIds.has(session.id) && !state.runtimes[session.id])
      .map((session) => session.id)
    state.sessions = state.sessions.filter(
      (session) => seenIds.has(session.id) || Boolean(state.runtimes[session.id]),
    )
    for (const sessionId of removedIds) {
      delete state.runtimes[sessionId]
      delete state.historyCursors[sessionId]
      delete state.historyLoading[sessionId]
    }
  }
  state.sessions.sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt))
}

export function reconcileRuntimeSnapshot(
  current: RuntimeSnapshot | undefined,
  incoming: RuntimeSnapshot,
  historyAuthoritative: boolean,
): RuntimeSnapshot {
  if (!current) return incoming

  // TurnLifecycle 不携带 runtime revision；当前详情可能已经应用了同一
  // revision 之后到达的生命周期事件，因此只有严格更新的快照才能替换状态。
  const incomingIsStale = incoming.runtimeRevision <= current.runtimeRevision
  const runtime = {
    ...(incomingIsStale ? current : incoming),
    timeline: incomingIsStale
      ? mergeTimeline(incoming.timeline, current.timeline)
      : mergeTimeline(current.timeline, incoming.timeline),
  }
  // 只要本次已取得实时快照，就清除历史回退态；revision 相同也表示恢复成功。
  delete runtime.unavailable

  if (historyAuthoritative) {
    if (incoming.historyNextCursor) runtime.historyNextCursor = incoming.historyNextCursor
    else delete runtime.historyNextCursor
  } else if (current.historyNextCursor) {
    runtime.historyNextCursor = current.historyNextCursor
  }

  return runtime
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
}

function mergeTimeline(current: TimelineItem[], incoming: TimelineItem[]): TimelineItem[] {
  const byId = new Map(current.map((item) => [item.id, item]))
  for (const item of incoming) {
    const existing = byId.get(item.id)
    byId.set(item.id, existing ? mergeTimelineItem(existing, item) : item)
  }
  return [...byId.values()].sort((left, right) => {
    const leftTime = Date.parse(left.createdAt) || 0
    const rightTime = Date.parse(right.createdAt) || 0
    return leftTime - rightTime
  })
}

/**
 * 同 id 条目合并:状态/时长等展示字段以较新一方为准;命令输出例外——
 * 历史/状态条目不携带输出正文,若较新条目是空占位而已有条目已收到
 * 实时输出或权威校正,保留已有输出,避免轮次结束的状态更新清空输出。
 */
function mergeTimelineItem(current: TimelineItem, incoming: TimelineItem): TimelineItem {
  if (
    (current.type === 'command' || current.type === 'outcome-unknown') &&
    incoming.type === current.type &&
    current.output.itemId === incoming.output.itemId &&
    isPlaceholderOutput(incoming.output)
  ) {
    return { ...incoming, output: current.output }
  }
  return incoming
}

/** 空占位输出:无文本、零字节、未定稿且无补全状态(见 commandTimelineItem)。 */
function isPlaceholderOutput(output: OutputState): boolean {
  return !output.text && output.byteLength === 0 && !output.isFinal && !output.loadState
}

function reduceTimelineOutput(item: TimelineItem, event: Parameters<typeof reduceOutput>[1]): TimelineItem {
  if (item.type === 'command') return { ...item, output: reduceOutput(item.output, event) }
  if (item.type === 'outcome-unknown') return { ...item, output: reduceOutput(item.output, event) }
  return item
}

function offlineRuntime(summary: SessionSummary): RuntimeSnapshot {
  return {
    sessionId: summary.id,
    runtimeRevision: 0,
    phase: summary.phase,
    attention: [],
    queue: { status: summary.queueStatus },
    capabilities: emptyReadOnlyCapabilities(),
    settings: {
      model: '',
      thinkingDepth: '',
      serviceTier: '',
      permissionMode: '',
      collaborationMode: '',
    },
    contextUsed: 0,
    contextWindow: 1,
    timeline: [],
    backgroundCommandCount: 0,
    outputCursors: [],
  }
}

export function prepareDeferredOutputs(runtime: RuntimeSnapshot): void {
  for (const cursor of runtime.outputCursors) {
    let item = runtime.timeline.find(
      (
        entry,
      ): entry is Extract<TimelineItem, { type: 'command' }> | Extract<TimelineItem, { type: 'outcome-unknown' }> =>
        (entry.type === 'command' || entry.type === 'outcome-unknown') &&
        entry.output.itemId === cursor.itemId,
    )
    if (!item) {
      const created: TimelineItem = {
        id: `output-${cursor.itemId}`,
        type: 'command',
        createdAt: '',
        command: cursor.itemId,
        cwdDisplay: '',
        status: cursor.isFinal ? 'COMPLETED' : 'RUNNING',
        elapsed: '',
        output: {
          itemId: cursor.itemId,
          revision: 0,
          text: '',
          byteLength: 0,
          isFinal: false,
          authority: 'LIVE_PREVIEW',
          hasGap: false,
        },
      }
      runtime.timeline.push(created)
      item = created
    }
    if (!shouldHydrateFinalOutput(item.output, cursor)) continue
    item.output.revision = cursor.revision
    item.output.byteLength = cursor.byteLength
    item.output.isFinal = false
    item.output.authority = cursor.finalUnavailable ? 'FINAL_OUTPUT_UNAVAILABLE' : 'AUTHORITATIVE_FINAL'
    item.output.hasGap = Boolean(cursor.finalUnavailable)
    item.output.loadState = cursor.finalUnavailable ? 'FAILED' : 'DEFERRED'
  }
}

async function loadOutput(sessionId: string, itemId: string): Promise<void> {
  const runtime = state.runtimes[sessionId]
  const cursor = runtime?.outputCursors.find((item) => item.itemId === itemId)
  const item = runtime?.timeline.find(
    (
      entry,
    ): entry is Extract<TimelineItem, { type: 'command' }> | Extract<TimelineItem, { type: 'outcome-unknown' }> =>
      (entry.type === 'command' || entry.type === 'outcome-unknown') &&
      entry.output.itemId === itemId,
  )
  if (!runtime || !cursor || !item || item.output.loadState === 'LOADING') return
  item.output.loadState = 'LOADING'
  const controller = new AbortController()
  const loadKey = `${sessionId}:${itemId}`
  outputLoads.set(loadKey, controller)
  try {
    const text = await transport.getOutputText(sessionId, itemId, { signal: controller.signal })
    let output = reduceOutput(item.output, {
      type: 'replace',
      itemId,
      revision: cursor.revision,
      text,
    })
    output = reduceOutput(output, {
      type: 'final',
      itemId,
      revision: cursor.revision,
      byteLength: cursor.byteLength,
    })
    delete output.loadState
    item.output = output
  } catch (error) {
    if (controller.signal.aborted) {
      item.output.loadState = 'DEFERRED'
    } else {
      item.output.loadState = 'FAILED'
      pushToast(error instanceof Error ? error.message : '最终输出读取失败，可重试。', 'warning')
    }
  } finally {
    if (outputLoads.get(loadKey) === controller) outputLoads.delete(loadKey)
  }
}

function emptyReadOnlyCapabilities(): CapabilitySnapshot {
  return {
    revision: 0,
    operations: {
      START_TURN: false,
      SET_QUEUE: false,
      REPLACE_QUEUE: false,
      CANCEL_QUEUE: false,
      STEER: false,
      INTERRUPT: false,
      ANSWER_QUESTION: false,
      ANSWER_APPROVAL: false,
      UPDATE_SETTINGS: false,
      STOP_BACKGROUND_COMMAND: false,
      STOP_ALL_BACKGROUND_COMMANDS: false,
      OPEN_FILE: false,
      READ_GIT_DIFF: false,
    },
    models: [],
    thinkingDepths: [],
    serviceTiers: [],
    permissionModes: [],
    collaborationModes: [],
  }
}

function effectiveConnection(session: SessionSummary): DeviceConnection {
  if (session.deviceConnection === 'OFFLINE') return 'OFFLINE'
  if (state.connection !== 'ONLINE') return state.connection
  return session.deviceConnection
}

function upsertReceipt(receipt: CommandReceipt): void {
  const index = state.receipts.findIndex((item) => item.requestId === receipt.requestId)
  if (index >= 0) state.receipts.splice(index, 1)
  state.receipts.unshift(receipt)
}

function recordReceipt(receipt: CommandReceipt): void {
  upsertReceipt(receipt)
  if (!isTerminalReceipt(receipt.status)) return

  const context = pendingCommandContexts.get(receipt.requestId)
  pendingCommandContexts.delete(receipt.requestId)
  if (!context || !shouldRefreshRuntimeAfterReceipt(receipt.status)) return

  void reconcileRuntimeAfterReceipt(context)
}

export function commandRuntimeSettled(
  context: Pick<PendingCommandContext, 'operation' | 'baselineRevision'>,
  runtime: RuntimeSnapshot,
): boolean {
  if (context.operation === 'INTERRUPT') {
    return runtime.phase === 'IDLE' && !runtime.activeTurnId
  }
  if (context.operation === 'START_TURN') {
    return runtime.phase === 'RUNNING' || runtime.runtimeRevision > context.baselineRevision
  }
  return true
}

async function reconcileRuntimeAfterReceipt(context: PendingCommandContext): Promise<void> {
  const delays =
    context.operation === 'INTERRUPT'
      ? INTERRUPT_RECONCILE_DELAYS_MS
      : context.operation === 'START_TURN'
        ? START_RECONCILE_DELAYS_MS
        : ([0] as const)
  for (const delay of delays) {
    if (delay > 0) await waitForReconcile(delay)
    if (!state.initialized) return
    try {
      const runtime = await ensureRuntime(context.sessionId, true, false)
      if (commandRuntimeSettled(context, runtime)) return
    } catch {
      // 后续有界尝试仍会重读；只在全部失败后提示用户。
    }
  }
  transport.requestResync(context.sessionId)
  pushToast('操作已返回，但 Desktop 运行态仍在同步；已请求重建详情流。', 'warning')
}

function waitForReconcile(delay: number): Promise<void> {
  return new Promise((resolve) => {
    const done = () => {
      reconcileTimers.delete(timer)
      resolve()
    }
    const timer = window.setTimeout(done, delay)
    reconcileTimers.set(timer, done)
  })
}

function receiptMessage(receipt: CommandReceipt): string {
  if (receipt.status === 'OUTCOME_UNKNOWN') return '连接中断，当前无法确认操作结果。'
  if (receipt.status === 'REJECTED') return `操作被拒绝${receipt.errorCode ? `：${receipt.errorCode}` : '。'}`
  return '请求已接收，等待 Bridge 确认。'
}

function pushToast(message: string, tone: ToastMessage['tone'] = 'info', linkTo?: string): void {
  const id = crypto.randomUUID()
  state.toasts.push({ id, message, tone, ...(linkTo ? { linkTo } : {}) })
  // 可点击的站内提醒停留更久;普通提示保持原节奏。
  window.setTimeout(() => dismissToast(id), linkTo ? 8000 : 4200)
}

function dismissToast(id: string): void {
  const index = state.toasts.findIndex((toast) => toast.id === id)
  if (index >= 0) state.toasts.splice(index, 1)
}

/**
 * 站内待办提醒(UX-06 最小集;Console 无 Web Push 模块,不接新通知供应商):
 * payload 只带不敏感摘要与站内路由标识,不含标题正文/命令/路径。
 */
export interface ReminderMessage {
  message: string
  linkTo: string
}

export function attentionAddedReminder(sessionId: string, muted: boolean): ReminderMessage | undefined {
  if (muted) return undefined
  return { message: '有新的待处理请求，请查看待处理中心。', linkTo: `/s/${sessionId}` }
}

export function outcomeReminder(
  sessionId: string,
  outcome: 'FAILED' | 'COMPLETED',
  muted: boolean,
  notifySuccessEnabled: boolean,
): ReminderMessage | undefined {
  if (muted) return undefined
  if (outcome === 'COMPLETED' && !notifySuccessEnabled) return undefined
  return {
    message:
      outcome === 'FAILED' ? '一个任务的本轮执行失败，请查看详情。' : '一个任务已完成。',
    linkTo: `/s/${sessionId}`,
  }
}

/**
 * 待处理请求关闭(到期/本机已处理)后的短提示:只解释事实,
 * 不再提供可批准卡片;本机已处理与过期使用同一句用户可读解释。
 */
export function attentionClosureReminder(sessionId: string): ReminderMessage {
  return {
    message: '一项待处理请求已关闭：可能已过期或已在本机处理。',
    linkTo: `/s/${sessionId}`,
  }
}

function notifyAttentionAdded(sessionId: string): void {
  const session = state.sessions.find((item) => item.id === sessionId)
  const reminder = attentionAddedReminder(sessionId, session?.muted ?? false)
  if (reminder) pushToast(reminder.message, 'warning', reminder.linkTo)
}

function notifyOutcome(sessionId: string, outcome: 'FAILED' | 'COMPLETED'): void {
  const session = state.sessions.find((item) => item.id === sessionId)
  const reminder = outcomeReminder(sessionId, outcome, session?.muted ?? false, state.notifySuccessEnabled)
  if (!reminder) return
  pushToast(reminder.message, outcome === 'FAILED' ? 'warning' : 'success', reminder.linkTo)
}

function recordAttentionClosure(sessionId: string, attentionId: string): void {
  if (state.resolvedAttentionIds.includes(attentionId)) return
  state.expiredNotices.unshift({ id: attentionId, sessionId, at: new Date().toISOString() })
  if (state.expiredNotices.length > 5) state.expiredNotices.length = 5
  const reminder = attentionClosureReminder(sessionId)
  pushToast(reminder.message, 'info', reminder.linkTo)
}

/** 审批所属轮次失败时,把该会话已允许的审批标记为"已允许，执行失败"(UX-05)。 */
export function applyExecutionFailureToApprovals(
  answerStates: Record<string, AnswerState>,
  approvalSessionByAttention: ReadonlyMap<string, string>,
  sessionId: string,
): void {
  for (const [attentionId, ownerSessionId] of approvalSessionByAttention) {
    if (ownerSessionId !== sessionId) continue
    const answerState = answerStates[attentionId]
    if (answerState && answerState.submittedStatus !== 'REJECTED') answerState.execution = 'FAILED'
  }
}

function retryConnection(): void {
  delete state.startupError
  transport.retryLink()
}

/** 供视图推送本地提示(如保存结果);不承载命令语义。 */
function notify(message: string, tone: ToastMessage['tone'] = 'info'): void {
  pushToast(message, tone)
}

async function verifyRequest(requestId: string): Promise<CommandReceipt | undefined> {
  try {
    const receipt = await transport.getReceipt(requestId)
    if (receipt) recordReceipt(receipt)
    return receipt
  } catch (error) {
    if (error instanceof AuthRequiredError) redirectToLogin()
    return undefined
  }
}

function toggleNotifySuccess(): void {
  state.notifySuccessEnabled = !state.notifySuccessEnabled
  writeNotifySuccessSetting(state.notifySuccessEnabled)
}

/**
 * 页面内草稿键(UX-03):按 (device, agentKind, nativeSession) 绑定,切会话不串内容。
 * 默认内存保存;审批凭据等 secret 型答案从不进入任何持久化存储。
 */
export function draftKeyFor(deviceId: string, agentKind: string, nativeSessionId: string): string {
  return `${deviceId}:${agentKind}:${nativeSessionId}`
}

function draftKey(sessionId: string): string {
  const summary = state.sessions.find((item) => item.id === sessionId)
  return draftKeyFor(
    summary?.deviceId ?? '',
    summary?.agentKind ?? 'AGENT_KIND_UNSPECIFIED',
    summary?.nativeSessionId || sessionId,
  )
}

/** 未建立草稿返回 undefined;返回 '' 表示用户明确清空(不能回退到旧队列正文)。 */
function getDraft(sessionId: string): string | undefined {
  const key = draftKey(sessionId)
  return key in state.drafts ? state.drafts[key] : undefined
}

function setDraft(sessionId: string, text: string): void {
  state.drafts[draftKey(sessionId)] = text
}

/** 版本组合可观察(IN-01):后端未提供的字段如实显示"未知",不强造数据。 */
function componentVersions() {
  const device = state.devices[0]
  const codexVersion = Object.values(state.runtimes)
    .map((runtime) => runtime.capabilities.codexVersion)
    .find((value) => Boolean(value))
  return {
    protocol: consoleProtocolVersion,
    web: consoleWebCommit,
    // Relay 版本来自 GET /agent-console/api/version;Toolbox 来自 GET /api/v1/version。
    relay: state.relayVersions.relay,
    relayProtocol: state.relayVersions.protocol,
    toolbox: state.toolboxVersions.app,
    toolboxCommit: state.toolboxVersions.commit,
    bridge: device?.bridgeVersion ?? '',
    codex: codexVersion ?? '',
  }
}

/** Relay 版本端点失败/缺失/占位值时保持空串,由展示层回退为"未知"。 */
async function refreshRelayVersion(): Promise<void> {
  const info = await loadRelayVersion()
  if (info) state.relayVersions = { relay: info.relayVersion, protocol: info.protocolVersion }
}

async function refreshToolboxVersion(): Promise<void> {
  const info = await loadToolboxVersion()
  if (info) state.toolboxVersions = { app: info.appVersion, commit: info.commit }
}

function redirectToLogin(): void {
  if (!state.fixtureMode) window.location.assign(loginUrl(window.location))
}

const pendingAttention = computed<AttentionItem[]>(() => {
  const seen = new Set<string>()
  return Object.values(state.runtimes)
    .flatMap((runtime) => runtime.attention)
    .filter((item) => {
      if (!item.valid || state.resolvedAttentionIds.includes(item.id) || seen.has(item.id)) return false
      seen.add(item.id)
      return true
    })
})

export interface AttentionInboxEntry {
  attention: AttentionItem
  kind: AttentionKind
  session?: SessionSummary | undefined
  deviceName: string
  projectDisplay: string
  waitingMs: number
  /** 会话摘要显示有待处理但设备离线:只有最后已知信息,无法提交回复(UX-01)。 */
  offlineDegraded: boolean
}

/**
 * 待处理中心投影(UX-01):从 SessionSummary、PendingAttention 与连接状态推导。
 * 同一请求按稳定原生 ID 去重;审批优先于提问,同类按等待时间最久优先。
 * 不建任务调度系统,只重组既有事实。
 */
export function projectAttentionInbox(input: {
  sessions: SessionSummary[]
  runtimes: Record<string, RuntimeSnapshot>
  resolvedAttentionIds: string[]
  nowMs: number
}): { active: AttentionInboxEntry[]; offlineDegraded: AttentionInboxEntry[] } {
  const seen = new Set<string>()
  const active: AttentionInboxEntry[] = []
  for (const runtime of Object.values(input.runtimes)) {
    for (const attention of runtime.attention) {
      if (!attention.valid || seen.has(attention.id)) continue
      if (input.resolvedAttentionIds.includes(attention.id)) continue
      seen.add(attention.id)
      const session = input.sessions.find((item) => item.id === attention.sessionId)
      active.push({
        attention,
        kind: attention.kind,
        session,
        deviceName: session ? `设备 ${session.deviceId}` : '未知设备',
        projectDisplay: session?.projectDisplay ?? '未知项目',
        waitingMs: Math.max(0, input.nowMs - Date.parse(attention.createdAt)),
        offlineDegraded: (session?.deviceConnection ?? 'OFFLINE') === 'OFFLINE',
      })
    }
  }
  active.sort((left, right) => {
    if (left.kind !== right.kind) return left.kind === 'RISK_APPROVAL' ? -1 : 1
    return Date.parse(left.attention.createdAt) - Date.parse(right.attention.createdAt)
  })

  // 会话摘要声明有待处理但运行时为空(通常设备离线):显示最后已知信息并解释不可提交。
  const offlineDegraded: AttentionInboxEntry[] = []
  for (const session of input.sessions) {
    if (session.attentionCount <= 0) continue
    if (session.deviceConnection !== 'OFFLINE') continue
    if (active.some((entry) => entry.attention.sessionId === session.id)) continue
    offlineDegraded.push({
      attention: {
        id: `offline-${session.id}`,
        kind: 'USER_QUESTION',
        sessionId: session.id,
        turnId: '',
        title: `${session.attentionCount} 项待处理（最后已知）`,
        description: '设备离线，无法读取请求正文；恢复连接后自动同步。',
        createdAt: session.deviceLastSeenAt ?? session.updatedAt,
        valid: false,
        options: [],
      },
      kind: 'USER_QUESTION',
      session,
      deviceName: `设备 ${session.deviceId}`,
      projectDisplay: session.projectDisplay,
      waitingMs: Math.max(0, input.nowMs - Date.parse(session.deviceLastSeenAt ?? session.updatedAt)),
      offlineDegraded: true,
    })
  }
  return { active, offlineDegraded }
}

export function useConsoleStore() {
  return {
    state,
    pendingAttention,
    initialize,
    dispose,
    loadMoreSessions,
    loadDevices,
    ensureRuntime,
    acquireRuntime,
    releaseRuntime,
    loadOlderHistory,
    loadOutput,
    presenceFor,
    availability,
    sendCommand,
    updateSetting,
    toggleConnection,
    answerAttention,
    lookupPairing,
    approvePairing,
    revokeDevice,
    getGitSummary,
    getGitDiff,
    getFileMetadata,
    previewFile,
    downloadFile,
    uploadFile,
    dismissToast,
    retryConnection,
    verifyRequest,
    getDraft,
    setDraft,
    toggleNotifySuccess,
    componentVersions,
    notify,
  }
}
