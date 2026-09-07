import { create } from '@bufbuild/protobuf'
import {
  AckSchema,
  ActiveTurnPhase as PbActiveTurnPhase,
  AgentKind,
  AnswerApprovalPayloadSchema,
  AnswerQuestionPayloadSchema,
  BackgroundCommandState as PbBackgroundCommandState,
  ClientHelloSchema,
  ClientKind,
  CommandReceiptStatus,
  CommandRequestSchema,
  CompatibilityState as PbCompatibilityState,
  ControlMode as PbControlMode,
  decodeEnvelope,
  encodeEnvelope,
  EnvelopeSchema,
  HeartbeatSchema,
  InterruptPayloadSchema,
  LastTurnOutcome as PbLastTurnOutcome,
  newMessageId,
  Operation,
  PROTOCOL_VERSION,
  QueueCancelPayloadSchema,
  QueueReplacePayloadSchema,
  QueueSetPayloadSchema,
  QueueState as PbQueueState,
  ResyncRequestSchema,
  SessionKeySchema,
  SessionListSchema,
  SettingKind,
  StartTurnPayloadSchema,
  SteerPayloadSchema,
  StopAllBackgroundCommandsPayloadSchema,
  StopBackgroundCommandPayloadSchema,
  SubscribeSchema,
  TurnIdSchema,
  UnsubscribeSchema,
  UpdateSettingsPayloadSchema,
  SettingUpdateSchema,
  type CapabilitySnapshot as PbCapabilitySnapshot,
  type CommandRequest as PbCommandRequest,
  type DevicePresence as PbDevicePresence,
  type DomainEvent,
  type Envelope,
  type Item as PbItem,
  type PendingAttentionApproval,
  type PendingAttentionQuestion,
  type RuntimeSnapshot as PbRuntimeSnapshot,
  type SessionSummary as PbSessionSummary,
} from '@agent-console/protocol/source'

import { utf8Length } from './output-reducer'
import type {
  ActiveTurnPhase,
  AttentionItem,
  BackgroundCommandState,
  CapabilitySnapshot,
  CommandReceipt,
  CommandRequest,
  CompatibilityState,
  ConsoleEvent,
  ConsoleTransport,
  ControlMode,
  ControlOperation,
  DeviceConnection,
  DeviceSummary,
  FileMetadata,
  GitFileDiff,
  GitSummary,
  LastTurnOutcome,
  Page,
  PairingChallenge,
  QueueState,
  QueueStatus,
  ReceiptStatus,
  RelayLinkState,
  RelayLinkStage,
  RuntimeSettings,
  RuntimeSnapshot,
  SelectOption,
  SessionSummary,
  TimelineItem,
  TransferLimits,
  UploadResult,
} from './types'

const API_ROOT = '/agent-console/api'
const AUTH_SESSION_PATH = '/api/v1/auth/session'
const TICKET_PATH = '/api/v1/agent-console/ws-tickets'
const WS_PROTOCOL = 'agent-console.v1'
const HEARTBEAT_MS = 15_000
const HEARTBEAT_TIMEOUT_MS = 45_000
const SUBSCRIBE_TIMEOUT_MS = 15_000
// WS 握手(open→ServerHello)独立超时:握不死时断开重连,不永久卡在 CONNECTING。
const HANDSHAKE_TIMEOUT_MS = 10_000
// HTTP 取票(WS ticket)超时,使用 AbortSignal 让浏览器取消旧请求。
const TICKET_TIMEOUT_MS = 10_000
// 回执等待超时后的一次性 receipt 查询超时。
const RECEIPT_QUERY_TIMEOUT_MS = 5_000
// 受控恢复(ResyncRequest)看门狗:在途恢复超时未完成才断开重连(§17.5)。
const RESYNC_TIMEOUT_MS = 10_000
// Browser WebSocket.close 只允许 1000 或 3000–4999；内部主动关闭使用私有码。
const WS_CLOSE_HEARTBEAT_TIMEOUT = 4001
const WS_CLOSE_PROTOCOL_MISMATCH = 4002
const WS_CLOSE_RESYNC_TIMEOUT = 4003
const WS_CLOSE_HANDSHAKE_TIMEOUT = 4004

type SubscriptionTarget = { kind: 'list' } | { kind: 'session'; sessionId: string }

interface StreamState {
  target: SubscriptionTarget
  epoch: bigint
  lastSequence: bigint
}

interface PendingCommand {
  envelope: Envelope
  accepted: boolean
}

interface CommandWaiter {
  resolve: (receipt: CommandReceipt) => void
  reject: (error: Error) => void
  timer: number
}

export class ConsoleApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message)
    this.name = 'ConsoleApiError'
  }
}

export class AuthRequiredError extends ConsoleApiError {
  constructor(message = '登录状态已失效。') {
    super(401, 'AUTH_REQUIRED', message)
    this.name = 'AuthRequiredError'
  }
}

/**
 * dev-toolbox 返回的 ticket 自身已经是 Base64URL 文本；WS 子协议合同要求
 * 再对这段明文做一次 Base64URL 编码，避免 ticket 中任何字节被协议解析。
 */
export function ticketSubprotocol(ticket: string): string {
  const bytes = new TextEncoder().encode(ticket)
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return `agent-console.ticket-${btoa(binary)
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/, '')}`
}

export function safeAgentConsoleReturnPath(locationLike: Pick<Location, 'origin' | 'pathname' | 'search' | 'hash'>): string {
  const raw = `${locationLike.pathname}${locationLike.search}${locationLike.hash}`
  const parsed = new URL(raw, locationLike.origin)
  if (parsed.origin !== locationLike.origin || !parsed.pathname.startsWith('/agent-console/')) {
    return '/agent-console/'
  }
  return `${parsed.pathname}${parsed.search}${parsed.hash}`
}

export function loginUrl(locationLike: Pick<Location, 'origin' | 'pathname' | 'search' | 'hash'>): string {
  return `/?returnTo=${encodeURIComponent(safeAgentConsoleReturnPath(locationLike))}`
}

export class RealConsoleTransport implements ConsoleTransport {
  #listener: ((event: ConsoleEvent) => void) | undefined
  #socket: WebSocket | undefined
  #closed = false
  #csrf = ''
  #reconnectAttempt = 0
  #reconnectTimer: number | undefined
  #heartbeatTimer: number | undefined
  #lastHeartbeatAck = 0
  #sessionKeys = new Map<string, { deviceId: string; agentKind: number; nativeSessionId: string }>()
  #wantedSessions = new Set<string>()
  #streamStates = new Map<string, StreamState>()
  #pendingSubscription: SubscriptionTarget | undefined
  #pendingSubscriptionStream = ''
  #pendingSubscriptionDone: (() => void) | undefined
  #subscriptionChain: Promise<void> = Promise.resolve()
  /** 已排队尚未开始执行的订阅目标:防止同一目标重复排队。 */
  #queuedSubscriptionKeys = new Set<string>()
  #pendingCommands = new Map<string, PendingCommand>()
  #commandWaiters = new Map<string, CommandWaiter>()
  #canonicalSessionRefresh: Promise<void> | undefined
  /** 在途受控恢复(streamId → 看门狗 timer):同一流不重复发送 ResyncRequest。 */
  #resyncInFlight = new Map<string, number>()
  /** 取票(经 Toolbox 网关)是否成功过:区分"Relay 不可达但 Toolbox 可用"(IN-01)。 */
  #toolboxReachable: boolean | undefined
  /** openSocket 流程(取票+握手)在途标志:任何时刻至多一个连接建立流程。 */
  #handshakeInFlight = false

  async connect(onEvent: (event: ConsoleEvent) => void): Promise<() => void> {
    this.#listener = onEvent
    this.#closed = false
    onEvent({ type: 'connection', state: 'CONNECTING' })
    this.#emitLink({ state: 'CONNECTING', stage: 'TICKET' })
    const auth = await this.#json<{ csrfToken: string }>(AUTH_SESSION_PATH)
    this.#csrf = auth.csrfToken
    await this.#openSocket()
    return () => this.#disconnect()
  }

  /** 用户手动重试:清退避并立即重连;协议终态升级后由此恢复。 */
  retryLink(): void {
    if (this.#closed) return
    // 取票/握手在途时不并发第二个 openSocket:旧流程超时会自行走重连自愈,
    // 此时再点重试只会产生多余的第三次连接尝试。
    if (this.#handshakeInFlight) return
    if (this.#socket?.readyState === WebSocket.OPEN) return
    if (this.#reconnectTimer !== undefined) {
      window.clearTimeout(this.#reconnectTimer)
      this.#reconnectTimer = undefined
    }
    this.#reconnectAttempt = 0
    void this.#openSocket()
      .then(() => this.#resendPendingCommands())
      .catch((error: unknown) => {
        if (error instanceof AuthRequiredError) this.#listener?.({ type: 'auth-expired' })
        else this.#scheduleReconnect()
      })
  }

  /** 按 requestId 只读查询持久化回执(UX-03"核对结果");无记录返回 undefined。 */
  async getReceipt(requestId: string): Promise<CommandReceipt | undefined> {
    try {
      const receipt = await this.#json<Record<string, unknown>>(
        `${API_ROOT}/requests/${encodeURIComponent(requestId)}`,
      )
      const status = receiptStatusFromProto(receiptStatusToProto(stringOrEmpty(receipt.status)))
      return {
        requestId,
        status,
        ...(stringOrEmpty(receipt.errorCode) ? { errorCode: stringOrEmpty(receipt.errorCode) } : {}),
      }
    } catch (error) {
      if (error instanceof AuthRequiredError) throw error
      return undefined
    }
  }

  #emitLink(
    overrides: Partial<Omit<RelayLinkState, 'updatedAt'>> & {
      stage: RelayLinkState['stage']
      state: RelayLinkState['state']
    },
  ): void {
    this.#listener?.({
      type: 'link',
      link: {
        ...overrides,
        ...(this.#toolboxReachable === undefined ? {} : { toolboxReachable: this.#toolboxReachable }),
        updatedAt: new Date().toISOString(),
      },
    })
  }

  async listSessions(cursor?: string): Promise<Page<SessionSummary>> {
    const query = new URLSearchParams({ limit: '50' })
    if (cursor) query.set('cursor', cursor)
    const body = await this.#json<{ sessions: unknown[]; nextCursor?: string | null }>(
      `${API_ROOT}/sessions?${query}`,
    )
    const items = body.sessions.map((value) => mapSessionSummaryJson(value))
    this.#rememberSessions(items)
    return body.nextCursor ? { items, nextCursor: body.nextCursor } : { items }
  }

  async getRuntimeSnapshot(
    sessionId: string,
    options?: { includeHistory?: boolean },
  ): Promise<RuntimeSnapshot> {
    this.#wantedSessions.add(sessionId)
    this.#queueSubscription({ kind: 'session', sessionId })
    const historyRequest = options?.includeHistory === false
      ? Promise.resolve<Page<TimelineItem>>({ items: [] })
      : this.getHistory(sessionId).catch((): Page<TimelineItem> => ({ items: [] }))
    const [runtimeBody, history] = await Promise.all([
      this.#json<{ runtimeSnapshot: unknown }>(`${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/runtime`),
      // RuntimeSnapshot 是详情首屏事实；HistoryPage 是可选分页补充。fake owner
      // 或旧 Desktop 暂时不能提供历史时，不得把已成功的 runtime 一并丢弃。
      historyRequest,
    ])
    const runtime = mapRuntimeSnapshotJson(sessionId, runtimeBody.runtimeSnapshot)
    runtime.timeline = history.items
    if (history.nextCursor) runtime.historyNextCursor = history.nextCursor
    applyContextUsage(runtime)
    return runtime
  }

  /** 视图等明确消费者声明需要某会话详情流:声明需要(重连恢复),无活跃流则订阅。 */
  retainRuntime(sessionId: string): void {
    this.#wantedSessions.add(sessionId)
    this.#queueSubscription({ kind: 'session', sessionId })
  }

  /** 释放详情订阅:不再随重连恢复,并向服务端发送 Unsubscribe。 */
  releaseRuntime(sessionId: string): void {
    this.#wantedSessions.delete(sessionId)
    for (const [streamId, stream] of this.#streamStates) {
      if (stream.target.kind !== 'session' || stream.target.sessionId !== sessionId) continue
      this.#streamStates.delete(streamId)
      this.#clearResyncInFlight(streamId)
      this.#send(
        baseEnvelope({
          case: 'unsubscribe',
          value: create(UnsubscribeSchema, { streamId }),
        }),
      )
    }
  }

  async getHistory(sessionId: string, cursor?: string): Promise<Page<TimelineItem>> {
    const query = new URLSearchParams({ pageSize: '50' })
    if (cursor) query.set('cursor', cursor)
    const body = await this.#json<{ historyPage: Record<string, unknown> }>(
      `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/history?${query}`,
    )
    const entries = Array.isArray(body.historyPage.entries) ? body.historyPage.entries : []
    const items = entries.flatMap((entry) => {
      const item = mapHistoryEntryJson(entry, sessionId)
      return item ? [item] : []
    })
    const nextCursor = stringOrEmpty(body.historyPage.nextCursor)
    return nextCursor ? { items, nextCursor } : { items }
  }

  async getDevices(): Promise<DeviceSummary[]> {
    const body = await this.#json<{ devices: unknown[] }>(`${API_ROOT}/devices`)
    return body.devices.map(mapDeviceJson)
  }

  async lookupPairing(shortCode: string): Promise<PairingChallenge> {
    const body = await this.#json<Record<string, unknown>>(
      `${API_ROOT}/pairing/lookup`,
      { method: 'POST', body: JSON.stringify({ shortCode: shortCode.trim() }) },
      true,
    )
    return {
      challengeId: stringOrEmpty(body.challengeId),
      deviceName: stringOrEmpty(body.deviceName),
      platform: stringOrEmpty(body.platform),
      architecture: stringOrEmpty(body.arch),
      bridgeVersion: stringOrEmpty(body.bridgeVersion),
      expiresAt: stringOrEmpty(body.expiresAt),
    }
  }

  async approvePairing(shortCode: string, challengeId?: string): Promise<void> {
    await this.#json(
      `${API_ROOT}/pairing/approve`,
      {
        method: 'POST',
        body: JSON.stringify({
          shortCode: shortCode.trim(),
          ...(challengeId ? { challengeId } : {}),
        }),
      },
      true,
    )
  }

  async revokeDevice(deviceId: string): Promise<void> {
    await this.#raw(`${API_ROOT}/devices/${encodeURIComponent(deviceId)}`, { method: 'DELETE' }, true)
  }

  async getGitSummary(sessionId: string): Promise<GitSummary> {
    const body = await this.#json<{ gitSummary: unknown }>(
      `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/git`,
    )
    return mapGitSummaryJson(body.gitSummary)
  }

  async getGitDiff(sessionId: string, path: string, staged = false): Promise<GitFileDiff> {
    const query = new URLSearchParams({ path, staged: String(staged) })
    const body = await this.#json<{ gitFileDiff: unknown }>(
      `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/git/diff?${query}`,
    )
    return mapGitDiffJson(body.gitFileDiff)
  }

  async getFileMetadata(sessionId: string, handle: string): Promise<FileMetadata> {
    const query = new URLSearchParams({ handle })
    const body = await this.#json<{ fileMetadata: unknown }>(
      `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/files/metadata?${query}`,
    )
    return mapFileMetadataJson(body.fileMetadata)
  }

  previewFile(sessionId: string, handle: string, fileName?: string): Promise<Response> {
    return this.#raw(
      `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/files/preview`,
      {
        method: 'POST',
        body: JSON.stringify({ fileHandle: handle, ...(fileName ? { fileName } : {}) }),
      },
      true,
    )
  }

  downloadFile(sessionId: string, handle: string, fileName?: string): Promise<Response> {
    return this.#raw(
      `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/files/download`,
      {
        method: 'POST',
        body: JSON.stringify({ fileHandle: handle, ...(fileName ? { fileName } : {}) }),
      },
      true,
    )
  }

  async uploadFile(sessionId: string, file: File): Promise<UploadResult> {
    const declared = await this.#json<{ transferId: string; uploadUrl: string }>(
      `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/files/upload`,
      {
        method: 'POST',
        body: JSON.stringify({
          fileName: file.name,
          mime: file.type || 'application/octet-stream',
          length: file.size,
        }),
      },
      true,
    )
    return this.#json<UploadResult>(
      declared.uploadUrl,
      {
        method: 'PUT',
        body: file,
        headers: { 'Content-Type': 'application/octet-stream' },
      },
      true,
    )
  }

  async getOutputText(
    sessionId: string,
    itemId: string,
    options?: { cursor?: string; signal?: AbortSignal },
  ): Promise<string> {
    let next = options?.cursor ?? ''
    let output = ''
    for (let page = 0; page < 100; page += 1) {
      const query = new URLSearchParams({ itemId, pageSize: String(256 * 1024) })
      if (next) query.set('cursor', next)
      const body = await this.#json<{ commandOutputPage: Record<string, unknown> }>(
        `${API_ROOT}/sessions/${encodeURIComponent(sessionId)}/output?${query}`,
        options?.signal ? { signal: options.signal } : {},
      )
      output += decodeBase64Utf8(stringOrEmpty(body.commandOutputPage.bytesBase64))
      next = stringOrEmpty(body.commandOutputPage.nextCursor)
      if (!next) return output
    }
    throw new Error('输出分页超过安全上限。')
  }

  requestResync(sessionId: string): void {
    for (const [streamId, state] of this.#streamStates) {
      if (state.target.kind === 'session' && state.target.sessionId === sessionId) {
        this.#requestStreamResync(streamId)
        return
      }
    }
  }

  async sendCommand(request: CommandRequest): Promise<CommandReceipt> {
    const envelope = await this.#commandEnvelope(request)
    const socket = this.#socket
    if (!socket || socket.readyState !== WebSocket.OPEN) throw new Error('实时连接尚未就绪。')

    const receipt = new Promise<CommandReceipt>((resolve, reject) => {
      const timer = window.setTimeout(() => {
        this.#commandWaiters.delete(request.requestId)
        // 回执等待超时:不推定执行失败。先查询已有 request receipt,
        // 仍未知则保持可解释的 OUTCOME_UNKNOWN(断开不推定原生操作未执行)。
        void this.#settleReceiptAfterTimeout(request.requestId, resolve)
      }, 15_000)
      this.#commandWaiters.set(request.requestId, { resolve, reject, timer })
    })
    this.#pendingCommands.set(request.requestId, { envelope, accepted: false })
    this.#send(envelope)
    return receipt
  }

  async #commandEnvelope(request: CommandRequest): Promise<Envelope> {
    const key = this.#sessionKeys.get(request.sessionId)
    if (!key) throw new Error('会话键尚未加载。')
    const payload = commandPayload(request)
    const digest = await sha256Hex(stableJson(request.payload))
    const command = create(CommandRequestSchema, {
      requestId: request.requestId,
      operation: operationToProto(request.operation),
      sessionKey: create(SessionKeySchema, {
        deviceId: key.deviceId,
        agentKind: key.agentKind,
        nativeSessionId: key.nativeSessionId,
        relaySessionUuid: request.sessionId,
      }),
      ...(request.expectedTurnId
        ? { expectedTurnId: create(TurnIdSchema, { id: request.expectedTurnId, synthetic: false }) }
        : {}),
      expectedRuntimeRevision: BigInt(request.expectedRuntimeRevision),
      payloadDigest: digest,
      payload,
    })
    return baseEnvelope({ case: 'commandRequest', value: command })
  }

  async #openSocket(): Promise<void> {
    this.#handshakeInFlight = true
    try {
      await this.#openSocketInner()
    } finally {
      this.#handshakeInFlight = false
    }
  }

  async #openSocketInner(): Promise<void> {
    // HTTP 取票带超时;取消后旧 Promise 不再创建新连接。
    this.#emitLink({ state: 'CONNECTING', stage: 'TICKET' })
    let ticket: string
    try {
      const body = await withTimeoutSignal(TICKET_TIMEOUT_MS, (signal) =>
        this.#json<{ ticket: string }>(
          TICKET_PATH,
          { method: 'POST', body: '{}', signal },
          true,
        ),
      )
      ticket = body.ticket
      // 取票成功说明 Toolbox 网关与会话都可用;失败是 Relay 侧链路问题(IN-01)。
      this.#toolboxReachable = true
    } catch (error) {
      if (error instanceof AuthRequiredError) throw error
      const reachable = error instanceof ConsoleApiError
      this.#toolboxReachable = reachable
      this.#emitLink({
        state: 'OFFLINE',
        stage: 'TICKET',
        ...(error instanceof ConsoleApiError ? { errorCode: error.code } : { errorCode: 'NETWORK_ERROR' }),
      })
      this.#scheduleReconnect()
      throw error
    }
    if (this.#closed) return
    const scheme = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const socket = new WebSocket(
      `${scheme}//${window.location.host}/agent-console/ws`,
      [WS_PROTOCOL, ticketSubprotocol(ticket)],
    )
    socket.binaryType = 'arraybuffer'
    this.#socket = socket
    this.#emitLink({ state: 'CONNECTING', stage: 'HANDSHAKE' })

    await new Promise<void>((resolve, reject) => {
      let greeted = false
      const fail = () => {
        window.clearTimeout(handshakeTimer)
        if (!greeted) reject(new Error('WebSocket 在握手完成前关闭。'))
      }
      const handshakeTimer = window.setTimeout(() => {
        socket.close(WS_CLOSE_HANDSHAKE_TIMEOUT, 'HANDSHAKE_TIMEOUT')
        fail()
      }, HANDSHAKE_TIMEOUT_MS)
      socket.addEventListener('open', () => {
        this.#send(
          baseEnvelope({
            case: 'clientHello',
            value: create(ClientHelloSchema, {
              protocolVersion: PROTOCOL_VERSION,
              clientKind: ClientKind.CLIENT_BROWSER,
              capabilities: ['list', 'session', 'query', 'command'],
            }),
          }),
        )
      })
      socket.addEventListener('message', (event) => {
        void this.#handleMessage(event, socket, () => {
          if (greeted) return
          greeted = true
          window.clearTimeout(handshakeTimer)
          resolve()
        })
      })
      socket.addEventListener('error', fail, { once: true })
      socket.addEventListener('close', (event) => {
        fail()
        this.#handleClose(event, socket)
      })
    })

    // 连接代次检查:握手期间已断开或 socket 已被替换时,旧握手回调不得发布 ONLINE。
    if (this.#closed || this.#socket !== socket) {
      if (socket.readyState === WebSocket.OPEN) socket.close(1000, 'CLIENT_CLOSE')
      return
    }
    this.#reconnectAttempt = 0
    this.#listener?.({ type: 'connection', state: 'ONLINE' })
    this.#emitLink({ state: 'ONLINE', stage: 'CONNECTED' })
    this.#lastHeartbeatAck = Date.now()
    this.#startHeartbeat()
    this.#queueSubscription({ kind: 'list' })
    for (const sessionId of this.#wantedSessions) this.#queueSubscription({ kind: 'session', sessionId })
    await this.#reconcilePendingReceipts()
  }

  async #handleMessage(event: MessageEvent, socket: WebSocket, onHello: () => void): Promise<void> {
    // 迟到消息守卫:非当前连接代次的消息不处理,不向新连接回放旧流状态。
    if (this.#socket !== socket) return
    const data = event.data instanceof Blob ? await event.data.arrayBuffer() : event.data
    if (!(data instanceof ArrayBuffer)) return
    let envelope: Envelope
    try {
      envelope = decodeEnvelope(new Uint8Array(data))
    } catch {
      this.#socket?.close(WS_CLOSE_PROTOCOL_MISMATCH, 'PROTOCOL_VERSION_MISMATCH')
      return
    }
    const payload = envelope.payload
    switch (payload.case) {
      case 'serverHello':
        if (payload.value.acceptedProtocolVersion !== PROTOCOL_VERSION) {
          this.#socket?.close(WS_CLOSE_PROTOCOL_MISMATCH, 'PROTOCOL_VERSION_MISMATCH')
          return
        }
        onHello()
        return
      case 'subscribed': {
        // 已知流先重绑定(服务端重发 Subscribed / 合法切换 stream epoch):
        // 接受新 epoch 并以 baseSequence(已应用水位)重建坐标:服务端有两种
        // 合法窗口形态——快照帧开头时首帧 seq=base(#acceptSnapshot 无门控,
        // 直接生效),事件帧开头时首帧 seq=base+1(#sequencedStream 门控恰好
        // 放行),两种形态均自洽;不依赖新 TCP 连接(R2-AC01)。
        const existing = this.#streamStates.get(payload.value.streamId)
        if (existing) {
          existing.epoch = payload.value.streamEpoch
          existing.lastSequence = payload.value.baseSequence
          return
        }
        // 仅新流才依赖待建立订阅信息;未知或无法关联的响应不绑定到
        // 当前另一个会话。
        const target = this.#pendingSubscription
        if (!target) return
        this.#pendingSubscriptionStream = payload.value.streamId
        this.#streamStates.set(payload.value.streamId, {
          target,
          epoch: payload.value.streamEpoch,
          lastSequence: payload.value.baseSequence,
        })
        return
      }
      case 'sessionSummaryBatch': {
        const stream = payload.value.snapshot
          ? this.#acceptSnapshot(envelope)
          : this.#sequencedStream(envelope)
        if (!stream) return
        const sessions = payload.value.summaries.map(mapSessionSummaryProto)
        try {
          this.#rememberSessions(sessions)
          this.#listener?.({ type: 'sessions', sessions, snapshot: payload.value.snapshot })
        } catch {
          // 应用失败不得把未应用数据记为成功:不提交序号,走受控恢复。
          this.#requestStreamResync(envelope.streamId)
          return
        }
        // 应用成功后才提交已应用水位并 ACK(§17.4 步骤 6)。
        stream.lastSequence = envelope.sequence
        if (sessions.some((session) => !isUuid(session.id))) this.#refreshCanonicalSessions()
        this.#ack(envelope)
        this.#completeSubscription(envelope.streamId)
        return
      }
      case 'runtimeSnapshot': {
        const stream = this.#acceptSnapshot(envelope)
        if (!stream || stream.target.kind !== 'session') return
        try {
          const runtime = mapRuntimeSnapshotProto(stream.target.sessionId, payload.value)
          this.#listener?.({ type: 'runtime-snapshot', sessionId: stream.target.sessionId, runtime })
        } catch {
          // 应用失败不得提交序号:保留恢复状态,走受控恢复。
          this.#requestStreamResync(envelope.streamId)
          return
        }
        stream.lastSequence = envelope.sequence
        this.#ack(envelope)
        this.#completeSubscription(envelope.streamId)
        return
      }
      case 'eventBatch': {
        const stream = this.#sequencedStream(envelope)
        if (!stream) return
        try {
          if (stream.target.kind === 'list') {
            // 列表流:只应用确有合同的事件(摘要/设备状态),按事件自身的
            // session_key/device_id 定位对象,不套用当前打开的详情会话 ID。
            for (const domainEvent of payload.value.events) {
              this.#applyListEvent(domainEvent)
            }
          } else {
            for (const domainEvent of payload.value.events) {
              this.#emitDomainEvent(stream.target.sessionId, domainEvent)
            }
          }
        } catch {
          // 应用失败不得把未应用数据记为成功:不推进水位,走受控恢复。
          this.#requestStreamResync(envelope.streamId)
          return
        }
        // 合法批次应用完成后才推进已应用水位并 ACK(§17.4 步骤 6)。
        stream.lastSequence = envelope.sequence
        this.#ack(envelope)
        return
      }
      case 'resyncRequired': {
        const stream = this.#streamStates.get(payload.value.streamId)
        if (!stream) return
        if (stream.target.kind === 'session') {
          this.#listener?.({
            type: 'resync-required',
            sessionId: stream.target.sessionId,
            reason: 'RESYNC_REQUIRED',
          })
        }
        // 列表与详情都发起恢复;同一流已有恢复在途时不重复发送。
        this.#requestStreamResync(payload.value.streamId)
        return
      }
      case 'commandAccepted':
        this.#handleReceipt(payload.value.requestId, payload.value.status, 0)
        return
      case 'commandResult':
        this.#handleReceipt(payload.value.requestId, payload.value.status, payload.value.errorCode)
        return
      case 'heartbeatAck':
        this.#lastHeartbeatAck = Date.now()
        return
      case 'protocolError':
        if (payload.value.streamId) {
          const stream = this.#streamStates.get(payload.value.streamId)
          if (stream?.target.kind === 'session') {
            this.#listener?.({
              type: 'resync-required',
              sessionId: stream.target.sessionId,
              reason: stableErrorName(payload.value.errorCode),
            })
          }
        }
        return
      default:
        return
    }
  }

  #emitDomainEvent(sessionId: string, domain: DomainEvent): void {
    const event = domain.event
    switch (event.case) {
      case 'turnLifecycle':
        this.#listener?.({
          type: 'turn-lifecycle',
          sessionId,
          phase: phaseFromProto(event.value.phase),
          ...(event.value.outcome === PbLastTurnOutcome.LAST_TURN_OUTCOME_UNSPECIFIED
            ? {}
            : { outcome: outcomeFromProto(event.value.outcome) }),
          ...(event.value.turn?.id ? { turnId: event.value.turn.id } : {}),
        })
        return
      case 'itemUpsert': {
        const item = event.value.item ? mapItemProto(event.value.item, sessionId) : undefined
        if (item) this.#listener?.({ type: 'timeline-upsert', sessionId, item })
        return
      }
      case 'outputAppend':
        if (event.value.itemId?.id) {
          this.#listener?.({
            type: 'output',
            sessionId,
            event: {
              type: 'append',
              itemId: event.value.itemId.id,
              expectedOffset: safeNumber(event.value.expectedOffset),
              text: new TextDecoder().decode(event.value.bytes),
            },
          })
        }
        return
      case 'outputReplace':
        if (!event.value.itemId?.id) return
        if (event.value.content.case === 'bytes') {
          this.#listener?.({
            type: 'output',
            sessionId,
            event: {
              type: 'replace',
              itemId: event.value.itemId.id,
              revision: safeNumber(event.value.revision),
              text: new TextDecoder().decode(event.value.content.value),
            },
          })
        } else if (event.value.content.case === 'pageCursor') {
          const itemId = event.value.itemId.id
          const revision = safeNumber(event.value.revision)
          void this.getOutputText(sessionId, itemId, { cursor: event.value.content.value })
            .then((text) => {
              // 补全到达前会话已退订:迟到数据不再回放到界面。
              if (!this.#sessionStreamActive(sessionId)) return
              this.#listener?.({
                type: 'output',
                sessionId,
                event: { type: 'replace', itemId, revision, text },
              })
            })
            .catch(() => {
              // 捕获失败:保持已有部分内容,标记完整输出暂不可用,不清空会话。
              if (!this.#sessionStreamActive(sessionId)) return
              this.#listener?.({
                type: 'output',
                sessionId,
                event: { type: 'unavailable', itemId, revision },
              })
            })
        }
        return
      case 'outputFinal':
        if (event.value.itemId?.id) {
          this.#listener?.({
            type: 'output',
            sessionId,
            event: {
              type: 'final',
              itemId: event.value.itemId.id,
              revision: safeNumber(event.value.revision),
              byteLength: safeNumber(event.value.byteLength),
            },
          })
        }
        return
      case 'sessionSummaryChanged': {
        const summary = mapSessionSummaryProto(event.value)
        this.#rememberSessions([summary])
        this.#listener?.({ type: 'sessions', sessions: [summary], snapshot: false })
        return
      }
      case 'pendingAttentionAdded': {
        const value = event.value.attention
        const attention =
          value.case === 'question'
            ? mapQuestionProto(value.value, sessionId)
            : value.case === 'approval'
              ? mapApprovalProto(value.value, sessionId)
              : undefined
        if (attention) this.#listener?.({ type: 'attention-added', sessionId, attention })
        return
      }
      case 'pendingAttentionRemoved':
        this.#listener?.({
          type: 'attention-removed',
          sessionId,
          attentionId: event.value.nativeId,
        })
        return
      case 'queueStateChanged':
        this.#listener?.({
          type: 'queue-changed',
          sessionId,
          queue: mapQueueProto(event.value.queue),
        })
        return
      case 'backgroundCommandChanged': {
        const command = event.value.command
        if (!command) return
        const item: TimelineItem = {
          id: `background-${command.commandId}`,
          type: 'background-command',
          createdAt: timestampToIso(command.startedAt),
          commandId: command.commandId,
          command: command.commandId,
          status: backgroundStateFromProto(command.state),
          elapsed: '',
        }
        this.#listener?.({ type: 'timeline-upsert', sessionId, item })
        return
      }
      case 'devicePresenceChanged':
        this.#emitPresenceChanged(event.value.presence)
        return
      case 'capabilityChanged':
        if (event.value.capabilities) {
          this.#listener?.({
            type: 'capabilities-changed',
            sessionId,
            capabilities: mapCapabilitiesProto(event.value.capabilities, 0),
          })
        }
        return
      default:
        return
    }
  }

  #emitPresenceChanged(presence: PbDevicePresence | undefined): void {
    if (!presence) return
    this.#listener?.({
      type: 'device-presence',
      deviceId: presence.deviceId,
      connection: connectionFromProto(presence.connection),
      ...(presence.lastSeenAt ? { lastSeenAt: timestampToIso(presence.lastSeenAt) } : {}),
      ...(presence.degradedReason ? { degradedReason: presence.degradedReason } : {}),
    })
  }

  #sessionStreamActive(sessionId: string): boolean {
    for (const stream of this.#streamStates.values()) {
      if (stream.target.kind === 'session' && stream.target.sessionId === sessionId) return true
    }
    return false
  }

  #handleReceipt(requestId: string, statusCode: CommandReceiptStatus, errorCode: number): void {
    const receipt: CommandReceipt = {
      requestId,
      status: receiptStatusFromProto(statusCode),
      ...(errorCode ? { errorCode: stableErrorName(errorCode) } : {}),
    }
    const pending = this.#pendingCommands.get(requestId)
    if (pending && receipt.status === 'ACCEPTED_BY_BRIDGE') pending.accepted = true
    if (isTerminalReceipt(receipt.status)) this.#pendingCommands.delete(requestId)
    this.#listener?.({ type: 'receipt', receipt })
    const waiter = this.#commandWaiters.get(requestId)
    if (waiter) {
      window.clearTimeout(waiter.timer)
      this.#commandWaiters.delete(requestId)
      waiter.resolve(receipt)
    }
  }

  /**
   * 快照接受校验(不提交水位):同 stream+epoch 才接受,成功快照重建坐标、
   * 解除该流的恢复在途标记;序号由调用方在应用成功后提交并 ACK,
   * 应用失败保留恢复状态走受控恢复(R2-AC01)。
   */
  #acceptSnapshot(envelope: Envelope): StreamState | undefined {
    const stream = this.#streamStates.get(envelope.streamId)
    if (!stream || stream.epoch !== envelope.streamEpoch) return undefined
    this.#clearResyncInFlight(envelope.streamId)
    return stream
  }

  /**
   * 序号校验(不提交水位):同 stream+epoch 内只接受 lastSequence+1;
   * 重复幂等忽略;缺口对列表与详情都发起一次受控恢复(§17.5),
   * 同一流已有恢复在途时不重复发送。调用方在应用完成后自行提交水位并 ACK。
   */
  #sequencedStream(envelope: Envelope): StreamState | undefined {
    const stream = this.#streamStates.get(envelope.streamId)
    if (!stream || stream.epoch !== envelope.streamEpoch) return undefined
    if (envelope.sequence <= stream.lastSequence) return undefined
    if (envelope.sequence !== stream.lastSequence + 1n) {
      if (stream.target.kind === 'session') {
        this.#listener?.({
          type: 'resync-required',
          sessionId: stream.target.sessionId,
          reason: 'sequence gap',
        })
      }
      this.#requestStreamResync(envelope.streamId)
      return undefined
    }
    return stream
  }

  /** 发送受控恢复请求;在途去重 + 看门狗超时才断开重连。 */
  #requestStreamResync(streamId: string): void {
    if (this.#resyncInFlight.has(streamId)) return
    if (this.#socket?.readyState !== WebSocket.OPEN) return
    const timer = window.setTimeout(() => {
      this.#resyncInFlight.delete(streamId)
      // 恢复超时:断开重连,用新快照重建坐标,不形成无限 Resync 循环。
      this.#socket?.close(WS_CLOSE_RESYNC_TIMEOUT, 'RESYNC_TIMEOUT')
    }, RESYNC_TIMEOUT_MS)
    this.#resyncInFlight.set(streamId, timer)
    this.#send(
      baseEnvelope({
        case: 'resyncRequest',
        value: create(ResyncRequestSchema, { streamId }),
      }),
    )
  }

  #clearResyncInFlight(streamId: string): void {
    const timer = this.#resyncInFlight.get(streamId)
    if (timer !== undefined) {
      window.clearTimeout(timer)
      this.#resyncInFlight.delete(streamId)
    }
  }

  /**
   * 列表流事件应用:只接受会话摘要与设备状态等确有合同的事件;按事件自身的
   * session_key/device_id 定位对象。其余类型不属于列表合同,不臆造字段、
   * 不套用当前详情会话 ID。
   */
  #applyListEvent(domain: DomainEvent): void {
    const event = domain.event
    switch (event.case) {
      case 'sessionSummaryChanged': {
        const summary = mapSessionSummaryProto(event.value)
        this.#rememberSessions([summary])
        this.#listener?.({ type: 'sessions', sessions: [summary], snapshot: false })
        return
      }
      case 'devicePresenceChanged':
        this.#emitPresenceChanged(event.value.presence)
        return
      default:
        return
    }
  }

  #ack(envelope: Envelope): void {
    this.#send(
      baseEnvelope({
        case: 'ack',
        value: create(AckSchema, { streamId: envelope.streamId, sequence: envelope.sequence }),
      }),
    )
  }

  #queueSubscription(target: SubscriptionTarget): void {
    const key = targetKey(target)
    if (this.#pendingSubscription && targetEqual(this.#pendingSubscription, target)) return
    if (this.#queuedSubscriptionKeys.has(key)) return
    const already = [...this.#streamStates.values()].some((stream) => targetEqual(stream.target, target))
    if (already) return
    this.#queuedSubscriptionKeys.add(key)
    this.#subscriptionChain = this.#subscriptionChain
      .catch(() => undefined)
      .then(() => {
        this.#queuedSubscriptionKeys.delete(key)
        return this.#subscribe(target)
      })
  }

  async #subscribe(target: SubscriptionTarget): Promise<void> {
    const socket = this.#socket
    if (!socket || socket.readyState !== WebSocket.OPEN) return
    // 排队期间目标已被释放:不再发出订阅,避免已离开的详情流被重新建立。
    if (target.kind === 'session' && !this.#wantedSessions.has(target.sessionId)) return
    const subscribe =
      target.kind === 'list'
        ? create(SubscribeSchema, {
            target: { case: 'list', value: create(SessionListSchema) },
          })
        : this.#sessionSubscribe(target.sessionId)
    if (!subscribe) return
    this.#pendingSubscription = target
    this.#pendingSubscriptionStream = ''
    await new Promise<void>((resolve) => {
      let completed = false
      const done = () => {
        if (completed) return
        completed = true
        window.clearTimeout(timer)
        this.#pendingSubscriptionDone = undefined
        this.#pendingSubscription = undefined
        this.#pendingSubscriptionStream = ''
        resolve()
      }
      const timer = window.setTimeout(done, SUBSCRIBE_TIMEOUT_MS)
      this.#pendingSubscriptionDone = done
      this.#send(baseEnvelope({ case: 'subscribe', value: subscribe }))
    })
  }

  #sessionSubscribe(sessionId: string) {
    const key = this.#sessionKeys.get(sessionId)
    if (!key) return undefined
    return create(SubscribeSchema, {
      target: {
        case: 'session',
        value: create(SessionKeySchema, {
          deviceId: key.deviceId,
          agentKind: key.agentKind,
          nativeSessionId: key.nativeSessionId,
          relaySessionUuid: sessionId,
        }),
      },
    })
  }

  #completeSubscription(streamId: string): void {
    if (streamId === this.#pendingSubscriptionStream) this.#pendingSubscriptionDone?.()
  }

  #rememberSessions(sessions: SessionSummary[]): void {
    for (const session of sessions) {
      this.#sessionKeys.set(session.id, {
        deviceId: session.deviceId,
        agentKind: agentKindValueFromName(session.agentKind),
        nativeSessionId: session.nativeSessionId,
      })
    }
  }

  #refreshCanonicalSessions(): void {
    if (this.#canonicalSessionRefresh) return
    this.#canonicalSessionRefresh = this.listSessions()
      .then((page) => this.#listener?.({ type: 'sessions', sessions: page.items, snapshot: false }))
      .catch(() => undefined)
      .finally(() => {
        this.#canonicalSessionRefresh = undefined
      })
  }

  #send(envelope: Envelope): void {
    if (this.#socket?.readyState === WebSocket.OPEN) {
      const bytes = encodeEnvelope(envelope)
      const frame = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
      this.#socket.send(frame)
    }
  }

  #startHeartbeat(): void {
    if (this.#heartbeatTimer) window.clearInterval(this.#heartbeatTimer)
    this.#heartbeatTimer = window.setInterval(() => {
      if (Date.now() - this.#lastHeartbeatAck > HEARTBEAT_TIMEOUT_MS) {
        this.#socket?.close(WS_CLOSE_HEARTBEAT_TIMEOUT, 'HEARTBEAT_TIMEOUT')
        return
      }
      this.#send(baseEnvelope({ case: 'heartbeat', value: create(HeartbeatSchema) }))
    }, HEARTBEAT_MS)
  }

  #handleClose(event: CloseEvent, socket?: WebSocket): void {
    if (socket && this.#socket !== socket) return
    if (this.#heartbeatTimer) window.clearInterval(this.#heartbeatTimer)
    this.#heartbeatTimer = undefined
    for (const timer of this.#resyncInFlight.values()) window.clearTimeout(timer)
    this.#resyncInFlight.clear()
    this.#socket = undefined
    this.#streamStates.clear()
    this.#pendingSubscriptionDone?.()
    this.#listener?.({ type: 'connection', state: 'OFFLINE' })
    if (this.#closed) return
    if (event.code === 1008 && event.reason === 'AUTH_EXPIRED') {
      this.#listener?.({ type: 'auth-expired' })
      return
    }
    // 协议不匹配是需要人工升级的终态:停止自动重连,由用户在升级后手动重试(IN-01)。
    if (event.code === WS_CLOSE_PROTOCOL_MISMATCH) {
      this.#emitLink({
        state: 'OFFLINE',
        stage: 'HELLO',
        errorCode: 'PROTOCOL_VERSION_MISMATCH',
        terminal: true,
      })
      return
    }
    const failure = closeFailure(event)
    if (failure) this.#emitLink({ state: 'OFFLINE', stage: failure.stage, errorCode: failure.errorCode })
    this.#scheduleReconnect()
  }

  #scheduleReconnect(): void {
    if (this.#closed || this.#reconnectTimer !== undefined) return
    const delay = Math.min(10_000, 500 * 2 ** this.#reconnectAttempt)
    this.#reconnectAttempt += 1
    this.#listener?.({ type: 'connection', state: 'CONNECTING' })
    this.#emitLink({
      state: 'CONNECTING',
      stage: 'RECONNECT',
      reconnectAttempt: this.#reconnectAttempt,
    })
    this.#reconnectTimer = window.setTimeout(() => {
      this.#reconnectTimer = undefined
      void this.#openSocket()
        .then(() => this.#resendPendingCommands())
        .catch((error) => {
          if (error instanceof AuthRequiredError) this.#listener?.({ type: 'auth-expired' })
          else this.#scheduleReconnect()
        })
    }, delay)
  }

  /** 连接就绪后重发尚未被 Bridge 接受的命令(幂等 requestId 由服务端去重)。 */
  #resendPendingCommands(): void {
    for (const pending of this.#pendingCommands.values()) {
      if (!pending.accepted) this.#send(pending.envelope)
    }
  }

  async #reconcilePendingReceipts(): Promise<void> {
    for (const [requestId, pending] of this.#pendingCommands) {
      if (!pending.accepted) continue
      try {
        const receipt = await this.#json<Record<string, unknown>>(
          `${API_ROOT}/requests/${encodeURIComponent(requestId)}`,
        )
        this.#handleReceipt(
          requestId,
          receiptStatusToProto(stringOrEmpty(receipt.status)),
          stableErrorToProto(stringOrEmpty(receipt.errorCode)),
        )
      } catch {
        // 一次只读核对失败不触发轮询；连接事件仍会给出最终结果或 OUTCOME_UNKNOWN。
      }
    }
  }

  /** 回执等待超时后的一次性查询:已有终态回执则返回;仍未知则保持 OUTCOME_UNKNOWN。 */
  async #settleReceiptAfterTimeout(
    requestId: string,
    resolve: (receipt: CommandReceipt) => void,
  ): Promise<void> {
    try {
      const receipt = await withTimeoutSignal(RECEIPT_QUERY_TIMEOUT_MS, (signal) =>
        this.#json<Record<string, unknown>>(
          `${API_ROOT}/requests/${encodeURIComponent(requestId)}`,
          { signal },
        ),
      )
      const status = receiptStatusFromProto(receiptStatusToProto(stringOrEmpty(receipt.status)))
      if (status !== 'RECEIVED') {
        resolve({
          requestId,
          status,
          ...(stringOrEmpty(receipt.errorCode) ? { errorCode: stringOrEmpty(receipt.errorCode) } : {}),
        })
        return
      }
    } catch {
      // 查询失败:保持未知,不推定失败。
    }
    resolve({ requestId, status: 'OUTCOME_UNKNOWN' })
  }

  #disconnect(): void {
    this.#closed = true
    if (this.#reconnectTimer !== undefined) window.clearTimeout(this.#reconnectTimer)
    this.#reconnectTimer = undefined
    if (this.#heartbeatTimer) window.clearInterval(this.#heartbeatTimer)
    this.#socket?.close(1000, 'CLIENT_CLOSE')
    for (const waiter of this.#commandWaiters.values()) {
      window.clearTimeout(waiter.timer)
      waiter.reject(new Error('实时连接已关闭。'))
    }
    this.#commandWaiters.clear()
    // 终局断开:清空未决命令,重新登录后的新连接不重发、不回填旧请求回执。
    this.#pendingCommands.clear()
  }

  async #json<T = unknown>(path: string, init: RequestInit = {}, mutation = false): Promise<T> {
    const response = await this.#raw(path, init, mutation)
    if (response.status === 204) return undefined as T
    return (await response.json()) as T
  }

  async #raw(path: string, init: RequestInit = {}, mutation = false): Promise<Response> {
    const headers = new Headers(init.headers)
    headers.set('Accept', 'application/json')
    if (init.body && !(init.body instanceof File) && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json')
    }
    if (mutation && this.#csrf) headers.set('X-CSRF-Token', this.#csrf)
    const response = await fetch(path, {
      ...init,
      headers,
      credentials: 'same-origin',
      cache: 'no-store',
    })
    if (response.ok) return response
    let code = response.status === 401 ? 'AUTH_REQUIRED' : `HTTP_${response.status}`
    let message = `请求失败（${response.status}）。`
    try {
      const body = (await response.json()) as {
        error?: { code?: string; message?: string }
      }
      code = body.error?.code ?? code
      message = body.error?.message ?? message
    } catch {
      // 非 JSON 错误由状态码表达。
    }
    if (response.status === 401 || code === 'AUTH_REQUIRED' || code === 'AUTH_EXPIRED') {
      throw new AuthRequiredError(message)
    }
    throw new ConsoleApiError(response.status, code, message)
  }
}

function baseEnvelope(payload: Envelope['payload']): Envelope {
  // Envelope.agent_kind 是设备级元数据(路由不依赖它;会话级身份在
  // SessionKey.agent_kind,ZC-02),沿用主 Agent 值。
  return create(EnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: newMessageId(),
    agentKind: AgentKind.CODEX_DESKTOP,
    payload,
  })
}

function commandPayload(request: CommandRequest): PbCommandRequest['payload'] {
  const turn = request.expectedTurnId
    ? create(TurnIdSchema, { id: request.expectedTurnId, synthetic: false })
    : undefined
  const text = stringOrEmpty(request.payload.text)
  switch (request.operation) {
    case 'START_TURN':
      return { case: 'startTurn', value: create(StartTurnPayloadSchema, { prompt: text }) }
    case 'SET_QUEUE':
      return {
        case: 'queueSet',
        value: create(QueueSetPayloadSchema, {
          prompt: text,
          ...(turn ? { afterTurnId: turn } : {}),
          runtimeRevision: BigInt(request.expectedRuntimeRevision),
        }),
      }
    case 'REPLACE_QUEUE':
      return {
        case: 'queueReplace',
        value: create(QueueReplacePayloadSchema, {
          prompt: text,
          ...(turn ? { afterTurnId: turn } : {}),
          runtimeRevision: BigInt(request.expectedRuntimeRevision),
        }),
      }
    case 'CANCEL_QUEUE':
      return { case: 'queueCancel', value: create(QueueCancelPayloadSchema) }
    case 'STEER':
      return {
        case: 'steer',
        value: create(SteerPayloadSchema, { prompt: text, ...(turn ? { expectedTurn: turn } : {}) }),
      }
    case 'INTERRUPT':
      return {
        case: 'interrupt',
        value: create(InterruptPayloadSchema, { ...(turn ? { expectedTurn: turn } : {}) }),
      }
    case 'ANSWER_QUESTION':
      return {
        case: 'answerQuestion',
        value: create(AnswerQuestionPayloadSchema, {
          questionId: stringOrEmpty(request.payload.attentionId),
          optionIds: request.payload.optionId ? [String(request.payload.optionId)] : [],
          freeText: stringOrEmpty(request.payload.freeText),
        }),
      }
    case 'ANSWER_APPROVAL':
      return {
        case: 'answerApproval',
        value: create(AnswerApprovalPayloadSchema, {
          approvalId: stringOrEmpty(request.payload.attentionId),
          decisionId: stringOrEmpty(request.payload.optionId),
        }),
      }
    case 'UPDATE_SETTINGS':
      return {
        case: 'updateSettings',
        value: create(UpdateSettingsPayloadSchema, {
          updates: [
            create(SettingUpdateSchema, {
              optionId: stringOrEmpty(request.payload.optionId ?? request.payload.key),
              value: stringOrEmpty(request.payload.value),
            }),
          ],
        }),
      }
    case 'STOP_BACKGROUND_COMMAND':
      return {
        case: 'stopBackgroundCommand',
        value: create(StopBackgroundCommandPayloadSchema, {
          commandId: stringOrEmpty(request.payload.commandId),
        }),
      }
    case 'STOP_ALL_BACKGROUND_COMMANDS':
      return {
        case: 'stopAllBackgroundCommands',
        value: create(StopAllBackgroundCommandsPayloadSchema),
      }
    default:
      throw new Error(`操作 ${request.operation} 不通过 WebSocket 写命令发送。`)
  }
}

function operationToProto(operation: ControlOperation): Operation {
  const values: Partial<Record<ControlOperation, Operation>> = {
    START_TURN: Operation.START_TURN,
    SET_QUEUE: Operation.QUEUE_SET,
    REPLACE_QUEUE: Operation.QUEUE_REPLACE,
    CANCEL_QUEUE: Operation.QUEUE_CANCEL,
    STEER: Operation.STEER,
    INTERRUPT: Operation.INTERRUPT,
    ANSWER_QUESTION: Operation.ANSWER_QUESTION,
    ANSWER_APPROVAL: Operation.ANSWER_APPROVAL,
    UPDATE_SETTINGS: Operation.UPDATE_SETTINGS,
    STOP_BACKGROUND_COMMAND: Operation.STOP_BACKGROUND_COMMAND,
    STOP_ALL_BACKGROUND_COMMANDS: Operation.STOP_ALL_BACKGROUND_COMMANDS,
  }
  const value = values[operation]
  if (value === undefined) throw new Error(`操作 ${operation} 不是 WS 写操作。`)
  return value
}

function emptyOperations(): CapabilitySnapshot['operations'] {
  return {
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
  }
}

function operationFromProto(operation: Operation): ControlOperation | undefined {
  switch (operation) {
    case Operation.START_TURN:
      return 'START_TURN'
    case Operation.QUEUE_SET:
      return 'SET_QUEUE'
    case Operation.QUEUE_REPLACE:
      return 'REPLACE_QUEUE'
    case Operation.QUEUE_CANCEL:
      return 'CANCEL_QUEUE'
    case Operation.STEER:
      return 'STEER'
    case Operation.INTERRUPT:
      return 'INTERRUPT'
    case Operation.ANSWER_QUESTION:
      return 'ANSWER_QUESTION'
    case Operation.ANSWER_APPROVAL:
      return 'ANSWER_APPROVAL'
    case Operation.UPDATE_SETTINGS:
      return 'UPDATE_SETTINGS'
    case Operation.STOP_BACKGROUND_COMMAND:
      return 'STOP_BACKGROUND_COMMAND'
    case Operation.STOP_ALL_BACKGROUND_COMMANDS:
      return 'STOP_ALL_BACKGROUND_COMMANDS'
    default:
      return undefined
  }
}

function operationFromName(value: string): ControlOperation | undefined {
  const normalized = value.replace(/^OPERATION_/, '')
  const names: Record<string, ControlOperation> = {
    START_TURN: 'START_TURN',
    QUEUE_SET: 'SET_QUEUE',
    QUEUE_REPLACE: 'REPLACE_QUEUE',
    QUEUE_CANCEL: 'CANCEL_QUEUE',
    STEER: 'STEER',
    INTERRUPT: 'INTERRUPT',
    ANSWER_QUESTION: 'ANSWER_QUESTION',
    ANSWER_APPROVAL: 'ANSWER_APPROVAL',
    UPDATE_SETTINGS: 'UPDATE_SETTINGS',
    STOP_BACKGROUND_COMMAND: 'STOP_BACKGROUND_COMMAND',
    STOP_ALL_BACKGROUND_COMMANDS: 'STOP_ALL_BACKGROUND_COMMANDS',
  }
  return names[normalized]
}

/**
 * AgentKind proto 数值 ↔ 枚举名映射(ZC-02)。
 * 未知数值原样保留为 `AGENT_KIND_{n}`;未知名称解析出数值或回退 UNSPECIFIED,
 * 绝不默认当作 CODEX_DESKTOP(由 Bridge/Relay 显式拒绝)。
 */
const AGENT_KIND_NAMES: Record<number, string> = {
  0: 'AGENT_KIND_UNSPECIFIED',
  1: 'CODEX_DESKTOP',
  2: 'ZCODE_DESKTOP',
}

export function agentKindName(value: number): string {
  return AGENT_KIND_NAMES[value] ?? `AGENT_KIND_${value}`
}

export function agentKindValueFromName(name: string): number {
  const known = Object.entries(AGENT_KIND_NAMES).find(([, value]) => value === name)
  if (known) return Number(known[0])
  const match = /^AGENT_KIND_(\d+)$/.exec(name)
  return match ? Number(match[1]) : 0
}

export function mapSessionSummaryJson(value: unknown): SessionSummary {
  const raw = record(value)
  return {
    id: stringOrEmpty(raw.id),
    nativeSessionId: stringOrEmpty(raw.nativeSessionId),
    agentKind: stringOrEmpty(raw.agentKind) || 'AGENT_KIND_UNSPECIFIED',
    title: stringOrEmpty(raw.title) || '未命名任务',
    projectDisplay: stringOrEmpty(raw.projectDisplayName),
    branch: stringOrEmpty(raw.currentBranch),
    updatedAt: stringOrEmpty(raw.lastUpdatedAt),
    deviceId: stringOrEmpty(raw.deviceId),
    deviceConnection: connectionFromName(stringOrEmpty(raw.deviceConnection)),
    ...(stringOrEmpty(raw.deviceLastSeenAt) ? { deviceLastSeenAt: stringOrEmpty(raw.deviceLastSeenAt) } : {}),
    ...(stringOrEmpty(raw.degradedReason) ? { degradedReason: stringOrEmpty(raw.degradedReason) } : {}),
    controlMode: controlFromName(stringOrEmpty(raw.controlMode)),
    compatibility: compatibilityFromName(stringOrEmpty(raw.compatibilityState)),
    phase: phaseFromName(stringOrEmpty(raw.activeTurnPhase)),
    lastOutcome: outcomeFromName(stringOrEmpty(raw.lastTurnOutcome)),
    attentionCount: numberOrZero(raw.pendingAttentionCount),
    queueStatus: queueFromName(stringOrEmpty(raw.queueState)),
    pinned: Boolean(raw.pinned),
    muted: Boolean(raw.muted),
    archived: Boolean(raw.archived),
  }
}

export function mapSessionSummaryProto(summary: PbSessionSummary): SessionSummary {
  const key = summary.sessionKey
  const agentKind = agentKindName(summary.agentKind)
  // 临时 ID 归属必须含 agentKind:relaySessionUuid 缺失时,同机同
  // nativeSessionId 的双 Agent(Codex/ZCode)是两个会话,临时 ID 与
  // store 归一化(deviceId+agentKind+nativeSessionId 元组)保持一致,
  // 不得折叠成一条(ZC-02)。
  const id =
    key?.relaySessionUuid ||
    `${key?.deviceId ?? ''}:${agentKind}:${key?.nativeSessionId ?? ''}`
  return {
    id,
    nativeSessionId: key?.nativeSessionId ?? '',
    agentKind,
    title: summary.title || '未命名任务',
    projectDisplay: summary.projectDisplayName,
    branch: summary.currentBranch,
    updatedAt: timestampToIso(summary.updatedAt),
    deviceId: key?.deviceId ?? '',
    deviceConnection: connectionFromProto(summary.deviceConnection),
    ...(summary.deviceLastSeenAt ? { deviceLastSeenAt: timestampToIso(summary.deviceLastSeenAt) } : {}),
    ...(summary.degradedReason ? { degradedReason: summary.degradedReason } : {}),
    controlMode: controlFromProto(summary.controlMode),
    compatibility: compatibilityFromProto(summary.compatibilityState),
    phase: phaseFromProto(summary.activeTurnPhase),
    lastOutcome: outcomeFromProto(summary.lastTurnOutcome),
    attentionCount: summary.pendingAttentionCount,
    queueStatus: queueFromProto(summary.queueState),
    pinned: summary.pinned,
    muted: summary.muted,
    archived: summary.archived,
  }
}

export function mapRuntimeSnapshotJson(sessionId: string, value: unknown): RuntimeSnapshot {
  const raw = record(value)
  const current = recordOrUndefined(raw.currentTurn)
  const capabilities = mapCapabilitiesJson(raw.capabilities, numberOrZero(raw.runtimeRevision))
  const attention = [
    ...(Array.isArray(raw.pendingQuestions)
      ? raw.pendingQuestions.map((question) => mapQuestionJson(question, sessionId))
      : []),
    ...(Array.isArray(raw.pendingApprovals)
      ? raw.pendingApprovals.map((approval) => mapApprovalJson(approval, sessionId))
      : []),
  ]
  const timeline: TimelineItem[] = []
  const plan = recordOrUndefined(raw.plan)
  const steps = plan && Array.isArray(plan.steps) ? plan.steps.map(mapPlanStepJson) : []
  if (steps.length) timeline.push({ id: `plan-${sessionId}`, type: 'plan', createdAt: '', steps })
  for (const item of attention) {
    timeline.push({ id: `attention-${item.id}`, type: 'attention', createdAt: item.createdAt, attention: item })
  }
  const backgrounds = Array.isArray(raw.backgroundCommands) ? raw.backgroundCommands : []
  for (const value of backgrounds) {
    const command = record(value)
    timeline.push({
      id: `background-${stringOrEmpty(command.commandId)}`,
      type: 'background-command',
      createdAt: stringOrEmpty(command.startedAt),
      commandId: stringOrEmpty(command.commandId),
      command: stringOrEmpty(command.commandId),
      status: backgroundStateFromName(stringOrEmpty(command.state)),
      elapsed: '',
    })
  }
  const settings = settingsFromJson(recordOrUndefined(raw.capabilities))
  return {
    sessionId,
    runtimeRevision: numberOrZero(raw.runtimeRevision),
    ...(current && recordOrUndefined(current.turn)?.id
      ? { activeTurnId: stringOrEmpty(record(current.turn).id) }
      : {}),
    phase: current ? phaseFromName(stringOrEmpty(current.phase)) : 'IDLE',
    attention,
    queue: mapQueueJson(raw.queue),
    capabilities,
    settings,
    contextUsed: 0,
    contextWindow: 1,
    timeline,
    backgroundCommandCount: numberOrZero(raw.backgroundCommandCount),
    outputCursors: (Array.isArray(raw.recentOutputCursors) ? raw.recentOutputCursors : [])
      .map((value) => {
        const cursor = record(value)
        return {
          itemId: stringOrEmpty(recordOrUndefined(cursor.itemId)?.id),
          revision: numberOrZero(cursor.revision),
          byteLength: numberOrZero(cursor.byteLength),
          isFinal: Boolean(cursor.isFinal),
          finalUnavailable: Boolean(cursor.finalUnavailable),
        }
      })
      .filter((cursor) => cursor.itemId),
  }
}

export function mapRuntimeSnapshotProto(sessionId: string, snapshot: PbRuntimeSnapshot): RuntimeSnapshot {
  const attention = [
    ...snapshot.pendingQuestions.map((question) => mapQuestionProto(question, sessionId)),
    ...snapshot.pendingApprovals.map((approval) => mapApprovalProto(approval, sessionId)),
  ]
  const timeline: TimelineItem[] = []
  if (snapshot.plan?.steps.length) {
    timeline.push({
      id: `plan-${sessionId}`,
      type: 'plan',
      createdAt: '',
      steps: snapshot.plan.steps.map((step) => ({
        id: step.stepId,
        label: step.title,
        status:
          step.status === 3 ? 'COMPLETED' : step.status === 2 ? 'RUNNING' : 'PENDING',
      })),
    })
  }
  for (const item of attention) {
    timeline.push({ id: `attention-${item.id}`, type: 'attention', createdAt: item.createdAt, attention: item })
  }
  for (const command of snapshot.backgroundCommands) {
    timeline.push({
      id: `background-${command.commandId}`,
      type: 'background-command',
      createdAt: timestampToIso(command.startedAt),
      commandId: command.commandId,
      command: command.commandId,
      status: backgroundStateFromProto(command.state),
      elapsed: '',
    })
  }
  const capabilities = mapCapabilitiesProto(snapshot.capabilities, safeNumber(snapshot.runtimeRevision))
  return {
    sessionId,
    runtimeRevision: safeNumber(snapshot.runtimeRevision),
    ...(snapshot.currentTurn?.turn?.id ? { activeTurnId: snapshot.currentTurn.turn.id } : {}),
    phase: snapshot.currentTurn ? phaseFromProto(snapshot.currentTurn.phase) : 'IDLE',
    attention,
    queue: mapQueueProto(snapshot.queue),
    capabilities,
    settings: settingsFromProto(snapshot.capabilities),
    contextUsed: 0,
    contextWindow: 1,
    timeline,
    backgroundCommandCount: snapshot.backgroundCommandCount,
    outputCursors: snapshot.recentOutputCursors
      .filter((cursor) => Boolean(cursor.itemId?.id))
      .map((cursor) => ({
        itemId: cursor.itemId!.id,
        revision: safeNumber(cursor.revision),
        byteLength: safeNumber(cursor.byteLength),
        isFinal: cursor.isFinal,
      })),
  }
}

function mapCapabilitiesJson(value: unknown, revision: number): CapabilitySnapshot {
  const raw = record(value)
  const operations = emptyOperations()
  if (Array.isArray(raw.supportedOperations)) {
    for (const name of raw.supportedOperations) {
      const operation = operationFromName(String(name))
      if (operation) operations[operation] = true
    }
  }
  const transferLimits = mapTransferLimits(recordOrUndefined(raw.transferLimits))
  return {
    revision,
    operations,
    ...(stringOrEmpty(raw.codexVersion) ? { codexVersion: stringOrEmpty(raw.codexVersion) } : {}),
    ...settingOptionsFromJson(raw.settings),
    ...(transferLimits ? { transferLimits } : {}),
  }
}

function mapCapabilitiesProto(capability: PbCapabilitySnapshot | undefined, revision: number): CapabilitySnapshot {
  const operations = emptyOperations()
  for (const value of capability?.supportedOperations ?? []) {
    const operation = operationFromProto(value)
    if (operation) operations[operation] = true
  }
  const transferLimits = capability?.transferLimits
    ? {
        textInlineMaxBytes: safeNumber(capability.transferLimits.textInlineMaxBytes),
        imageInlineMaxBytes: safeNumber(capability.transferLimits.imageInlineMaxBytes),
        pdfRangeMaxBytes: safeNumber(capability.transferLimits.pdfRangeMaxBytes),
        downloadMaxBytes: safeNumber(capability.transferLimits.downloadMaxBytes),
        uploadMaxBytes: safeNumber(capability.transferLimits.uploadMaxBytes),
        maxConcurrentPerBrowser: capability.transferLimits.maxConcurrentPerBrowser,
        maxConcurrentPerDevice: capability.transferLimits.maxConcurrentPerDevice,
        previewableMimePrefixes: [...capability.transferLimits.previewableMimePrefixes],
      }
    : undefined
  return {
    revision,
    operations,
    ...(capability?.codexVersion ? { codexVersion: capability.codexVersion } : {}),
    ...settingOptionsFromProto(capability),
    ...(transferLimits ? { transferLimits } : {}),
  }
}

function settingOptionsFromJson(value: unknown): Pick<CapabilitySnapshot, 'models' | 'thinkingDepths' | 'serviceTiers' | 'permissionModes' | 'collaborationModes'> {
  const result = emptySettingOptions()
  for (const entry of Array.isArray(value) ? value : []) {
    const setting = record(entry)
    const options = Array.isArray(setting.availableValues)
      ? setting.availableValues.map((option) => {
          const raw = record(option)
          return {
            id: stringOrEmpty(raw.value),
            label: stringOrEmpty(raw.label) || stringOrEmpty(raw.value),
            disabled: !Boolean(setting.mutable),
          } satisfies SelectOption
        })
      : []
    const current = stringOrEmpty(setting.currentValue)
    if (current && !options.some((option) => option.id === current)) {
      options.push({ id: current, label: current, disabled: true })
    }
    result[settingListKey(stringOrEmpty(setting.kind))].push(...options)
  }
  return result
}

function settingOptionsFromProto(capability?: PbCapabilitySnapshot): Pick<CapabilitySnapshot, 'models' | 'thinkingDepths' | 'serviceTiers' | 'permissionModes' | 'collaborationModes'> {
  const result = emptySettingOptions()
  for (const setting of capability?.settings ?? []) {
    const options: SelectOption[] = setting.availableValues.map((option) => ({
      id: option.value,
      label: option.label || option.value,
      disabled: !setting.mutable,
    }))
    if (setting.currentValue && !options.some((option) => option.id === setting.currentValue)) {
      options.push({ id: setting.currentValue, label: setting.currentValue, disabled: true })
    }
    result[settingListKey(setting.kind)].push(...options)
  }
  return result
}

function settingsFromJson(capabilities: Record<string, unknown> | undefined): RuntimeSettings {
  const settings = emptyRuntimeSettings()
  for (const value of Array.isArray(capabilities?.settings) ? capabilities.settings : []) {
    const setting = record(value)
    settings[settingValueKey(stringOrEmpty(setting.kind))] = stringOrEmpty(setting.currentValue)
  }
  return settings
}

function settingsFromProto(capability?: PbCapabilitySnapshot): RuntimeSettings {
  const settings = emptyRuntimeSettings()
  for (const setting of capability?.settings ?? []) {
    settings[settingValueKey(setting.kind)] = setting.currentValue
  }
  return settings
}

function emptySettingOptions() {
  return {
    models: [] as SelectOption[],
    thinkingDepths: [] as SelectOption[],
    serviceTiers: [] as SelectOption[],
    permissionModes: [] as SelectOption[],
    collaborationModes: [] as SelectOption[],
  }
}

function emptyRuntimeSettings(): RuntimeSettings {
  return { model: '', thinkingDepth: '', serviceTier: '', permissionMode: '', collaborationMode: '' }
}

function settingListKey(kind: string | SettingKind): keyof ReturnType<typeof emptySettingOptions> {
  const normalized = typeof kind === 'number' ? kind : kind.replace(/^SETTING_KIND_/, '')
  if (normalized === SettingKind.MODEL || normalized === 'MODEL') return 'models'
  if (normalized === SettingKind.REASONING_EFFORT || normalized === 'REASONING_EFFORT') return 'thinkingDepths'
  if (normalized === SettingKind.SERVICE_TIER || normalized === 'SERVICE_TIER') return 'serviceTiers'
  if (normalized === SettingKind.PERMISSION_MODE || normalized === 'PERMISSION_MODE') return 'permissionModes'
  return 'collaborationModes'
}

function settingValueKey(kind: string | SettingKind): keyof RuntimeSettings {
  const list = settingListKey(kind)
  return {
    models: 'model',
    thinkingDepths: 'thinkingDepth',
    serviceTiers: 'serviceTier',
    permissionModes: 'permissionMode',
    collaborationModes: 'collaborationMode',
  }[list] as keyof RuntimeSettings
}

function mapTransferLimits(raw: Record<string, unknown> | undefined): TransferLimits | undefined {
  if (!raw) return undefined
  return {
    textInlineMaxBytes: numberOrZero(raw.textInlineMaxBytes),
    imageInlineMaxBytes: numberOrZero(raw.imageInlineMaxBytes),
    pdfRangeMaxBytes: numberOrZero(raw.pdfRangeMaxBytes),
    downloadMaxBytes: numberOrZero(raw.downloadMaxBytes),
    uploadMaxBytes: numberOrZero(raw.uploadMaxBytes),
    maxConcurrentPerBrowser: numberOrZero(raw.maxConcurrentPerBrowser),
    maxConcurrentPerDevice: numberOrZero(raw.maxConcurrentPerDevice),
    previewableMimePrefixes: Array.isArray(raw.previewableMimePrefixes)
      ? raw.previewableMimePrefixes.map(String)
      : [],
  }
}

function mapQuestionJson(value: unknown, sessionId: string): AttentionItem {
  const raw = record(value)
  const turn = recordOrUndefined(raw.turn)
  return {
    id: stringOrEmpty(raw.questionId),
    kind: 'USER_QUESTION',
    sessionId,
    turnId: stringOrEmpty(turn?.id),
    title: stringOrEmpty(raw.title) || 'Codex 提问',
    description: stringOrEmpty(raw.description),
    createdAt: stringOrEmpty(raw.createdAt),
    valid: Boolean(raw.valid),
    allowFreeText: Boolean(raw.allowFreeText),
    options: (Array.isArray(raw.options) ? raw.options : []).map((option) => {
      const item = record(option)
      return { id: stringOrEmpty(item.optionId), label: stringOrEmpty(item.label), emphasis: 'secondary' }
    }),
  }
}

function mapApprovalJson(value: unknown, sessionId: string): AttentionItem {
  const raw = record(value)
  const turn = recordOrUndefined(raw.turn)
  return {
    id: stringOrEmpty(raw.approvalId),
    kind: 'RISK_APPROVAL',
    sessionId,
    turnId: stringOrEmpty(turn?.id),
    title: '风险审批',
    description: stringOrEmpty(raw.riskDescription),
    requestAction: stringOrEmpty(raw.requestedAction),
    risk: stringOrEmpty(raw.riskDescription),
    createdAt: stringOrEmpty(raw.createdAt),
    valid: Boolean(raw.valid),
    options: (Array.isArray(raw.decisions) ? raw.decisions : []).map((decision) => {
      const item = record(decision)
      const label = stringOrEmpty(item.label)
      return {
        id: stringOrEmpty(item.decisionId),
        label,
        emphasis: /拒绝|deny|reject/i.test(label) ? 'secondary' : 'primary',
      }
    }),
  }
}

function mapQuestionProto(value: PendingAttentionQuestion, sessionId: string): AttentionItem {
  return {
    id: value.questionId,
    kind: 'USER_QUESTION',
    sessionId,
    turnId: value.turn?.id ?? '',
    title: value.title || 'Codex 提问',
    description: value.description,
    createdAt: timestampToIso(value.createdAt),
    valid: value.valid,
    allowFreeText: value.allowFreeText,
    options: value.options.map((option) => ({
      id: option.optionId,
      label: option.label,
      emphasis: 'secondary',
    })),
  }
}

function mapApprovalProto(value: PendingAttentionApproval, sessionId: string): AttentionItem {
  return {
    id: value.approvalId,
    kind: 'RISK_APPROVAL',
    sessionId,
    turnId: value.turn?.id ?? '',
    title: '风险审批',
    description: value.riskDescription,
    requestAction: value.requestedAction,
    risk: value.riskDescription,
    createdAt: timestampToIso(value.createdAt),
    valid: value.valid,
    options: value.decisions.map((decision) => ({
      id: decision.decisionId,
      label: decision.label,
      emphasis: /拒绝|deny|reject/i.test(decision.label) ? 'secondary' : 'primary',
    })),
  }
}

function mapQueueJson(value: unknown): QueueState {
  const raw = recordOrUndefined(value)
  if (!raw) return { status: 'EMPTY' }
  const turn = recordOrUndefined(raw.afterTurnId)
  const afterTurnId = stringOrEmpty(turn?.id)
  return {
    status: queueFromName(stringOrEmpty(raw.state)),
    ...(afterTurnId ? { afterTurnId } : {}),
  }
}

function mapQueueProto(
  value: { state: PbQueueState; afterTurnId?: { id: string } | undefined } | undefined,
): QueueState {
  if (!value) return { status: 'EMPTY' }
  return {
    status: queueFromProto(value.state),
    ...(value.afterTurnId?.id ? { afterTurnId: value.afterTurnId.id } : {}),
  }
}

function mapPlanStepJson(value: unknown) {
  const raw = record(value)
  const status = stringOrEmpty(raw.status)
  return {
    id: stringOrEmpty(raw.stepId),
    label: stringOrEmpty(raw.title),
    status: status.endsWith('COMPLETED')
      ? ('COMPLETED' as const)
      : status.endsWith('IN_PROGRESS')
        ? ('RUNNING' as const)
        : ('PENDING' as const),
  }
}

function mapHistoryEntryJson(value: unknown, sessionId: string): TimelineItem | undefined {
  const entry = record(value)
  const item = recordOrUndefined(entry.item)
  if (!item) return undefined
  const content = recordOrUndefined(item.content)
  if (!content) return undefined
  const id = stringOrEmpty(recordOrUndefined(item.itemId)?.id) || crypto.randomUUID()
  const createdAt = stringOrEmpty(item.createdAt)
  if (content.assistantMessage || content.userMessage || content.reasoningSummary) {
    const message = record(content.assistantMessage ?? content.userMessage ?? content.reasoningSummary)
    const author = content.userMessage ? '你' : content.reasoningSummary ? 'Reasoning' : 'Codex'
    return { id, type: 'commentary', createdAt, body: stringOrEmpty(message.text), author }
  }
  if (content.plan) {
    const plan = record(content.plan)
    return {
      id,
      type: 'plan',
      createdAt,
      steps: (Array.isArray(plan.steps) ? plan.steps : []).map(mapPlanStepJson),
    }
  }
  if (content.toolCall) {
    const tool = record(content.toolCall)
    return {
      id,
      type: 'commentary',
      createdAt,
      author: stringOrEmpty(tool.name) || '工具',
      body: stringOrEmpty(tool.summary),
    }
  }
  if (content.commandStatus) {
    const command = record(content.commandStatus)
    return commandTimelineItem(id, createdAt, command)
  }
  if (content.fileChange) {
    const change = record(content.fileChange)
    const paths = (Array.isArray(change.files) ? change.files : [])
      .map((file) => stringOrEmpty(record(file).path))
      .filter(Boolean)
    return { id, type: 'commentary', createdAt, author: '文件变化', body: paths.join('\n') }
  }
  if (content.question) {
    const attention = mapQuestionJson(content.question, sessionId)
    return { id, type: 'attention', createdAt, attention }
  }
  if (content.approval) {
    const attention = mapApprovalJson(content.approval, sessionId)
    return { id, type: 'attention', createdAt, attention }
  }
  if (content.subagentStatus) {
    const subagent = record(content.subagentStatus)
    return {
      id,
      type: 'commentary',
      createdAt,
      author: '子 Agent',
      body: `${stringOrEmpty(subagent.label)} · ${stringOrEmpty(subagent.phase)}`,
    }
  }
  if (content.tokenUsage) {
    const usage = record(content.tokenUsage)
    return {
      id,
      type: 'commentary',
      createdAt,
      author: '上下文',
      body: `${numberOrZero(usage.contextUsedTokens).toLocaleString()} / ${numberOrZero(usage.contextWindowTokens).toLocaleString()}`,
    }
  }
  return undefined
}

function mapItemProto(item: PbItem, sessionId: string): TimelineItem | undefined {
  const id = item.itemId?.id || crypto.randomUUID()
  const createdAt = timestampToIso(item.createdAt)
  const content = item.content
  switch (content.case) {
    case 'assistantMessage':
      return { id, type: 'commentary', createdAt, body: content.value.text, author: 'Codex' }
    case 'userMessage':
      return { id, type: 'commentary', createdAt, body: content.value.text, author: '你' }
    case 'reasoningSummary':
      return { id, type: 'commentary', createdAt, body: content.value.text, author: 'Reasoning' }
    case 'plan':
      return {
        id,
        type: 'plan',
        createdAt,
        steps: content.value.steps.map((step) => ({
          id: step.stepId,
          label: step.title,
          status: step.status === 3 ? 'COMPLETED' : step.status === 2 ? 'RUNNING' : 'PENDING',
        })),
      }
    case 'toolCall':
      return { id, type: 'commentary', createdAt, body: content.value.summary, author: content.value.name }
    case 'commandStatus':
      return commandTimelineItem(id, createdAt, {
        commandId: content.value.commandId,
        state: backgroundStateFromProto(content.value.state),
        durationMs: safeNumber(content.value.durationMs),
      })
    case 'fileChange':
      return {
        id,
        type: 'commentary',
        createdAt,
        author: '文件变化',
        body: content.value.files.map((file) => file.path).join('\n'),
      }
    case 'question': {
      const attention = mapQuestionProto(content.value, sessionId)
      return { id, type: 'attention', createdAt, attention }
    }
    case 'approval': {
      const attention = mapApprovalProto(content.value, sessionId)
      return { id, type: 'attention', createdAt, attention }
    }
    case 'subagentStatus':
      return {
        id,
        type: 'commentary',
        createdAt,
        author: '子 Agent',
        body: `${content.value.label} · ${phaseFromProto(content.value.phase)}`,
      }
    case 'tokenUsage':
      return {
        id,
        type: 'commentary',
        createdAt,
        author: '上下文',
        body: `${safeNumber(content.value.contextUsedTokens).toLocaleString()} / ${safeNumber(content.value.contextWindowTokens).toLocaleString()}`,
      }
    default:
      return undefined
  }
}

function commandTimelineItem(id: string, createdAt: string, value: Record<string, unknown>): TimelineItem {
  const commandId = stringOrEmpty(value.commandId)
  const status = backgroundStateFromName(stringOrEmpty(value.state))
  const duration = numberOrZero(value.durationMs)
  return {
    id,
    type: 'command',
    createdAt,
    command: commandId,
    cwdDisplay: '',
    status: status === 'UNKNOWN' ? 'RUNNING' : status,
    elapsed: duration ? `${Math.round(duration / 100) / 10}s` : '',
    output: {
      itemId: id,
      revision: 0,
      text: '',
      byteLength: 0,
      isFinal: status !== 'RUNNING',
      authority: status === 'RUNNING' ? 'LIVE_PREVIEW' : 'AUTHORITATIVE_FINAL',
      hasGap: false,
    },
  }
}

export function mapDeviceJson(value: unknown): DeviceSummary {
  const raw = record(value)
  return {
    id: stringOrEmpty(raw.id),
    displayName: stringOrEmpty(raw.displayName),
    connection: connectionFromName(stringOrEmpty(raw.connection)),
    controlMode: controlFromName(stringOrEmpty(raw.controlMode)),
    compatibility: compatibilityFromName(stringOrEmpty(raw.compatibilityState)),
    lastSeenAt: stringOrEmpty(raw.lastSeenAt),
    bridgeVersion: stringOrEmpty(raw.bridgeVersion),
    platform: stringOrEmpty(raw.platform),
    architecture: stringOrEmpty(raw.arch),
    pairedAt: stringOrEmpty(raw.pairedAt),
    revoked: Boolean(raw.revoked),
    privacyHideTitles: Boolean(raw.privacyHideTitles),
  }
}

export function mapGitSummaryJson(value: unknown): GitSummary {
  const raw = record(value)
  return {
    branch: stringOrEmpty(raw.branch),
    detachedHead: Boolean(raw.detachedHead),
    headShort: stringOrEmpty(raw.headShort),
    headFull: stringOrEmpty(raw.headFull),
    rootDisplayName: stringOrEmpty(raw.rootDisplayName),
    entries: (Array.isArray(raw.entries) ? raw.entries : []).map((entry) => {
      const item = record(entry)
      return {
        relativePath: stringOrEmpty(item.relativePath),
        status: stringOrEmpty(item.status),
        staged: Boolean(item.staged),
      }
    }),
    insertions: numberOrZero(raw.insertions),
    deletions: numberOrZero(raw.deletions),
    binaryFiles: Array.isArray(raw.binaryFiles) ? raw.binaryFiles.map(String) : [],
  }
}

export function mapGitDiffJson(value: unknown): GitFileDiff {
  const raw = record(value)
  return {
    relativePath: stringOrEmpty(raw.relativePath),
    staged: Boolean(raw.staged),
    patchText: stringOrEmpty(raw.patchText),
    truncated: Boolean(raw.truncated),
    totalBytes: numberOrZero(raw.totalBytes),
    binary: Boolean(raw.binary),
  }
}

export function mapFileMetadataJson(value: unknown): FileMetadata {
  const raw = record(value)
  const previewKind = stringOrEmpty(raw.previewKind)
  return {
    displayName: stringOrEmpty(raw.displayName),
    mimeType: stringOrEmpty(raw.mimeType),
    sizeBytes: numberOrZero(raw.sizeBytes),
    previewKind: ['text', 'image', 'pdf'].includes(previewKind)
      ? (previewKind as FileMetadata['previewKind'])
      : 'none',
    ...(stringOrEmpty(raw.notPreviewableReason)
      ? { notPreviewableReason: stringOrEmpty(raw.notPreviewableReason) }
      : {}),
    fileHandle: stringOrEmpty(raw.fileHandle),
  }
}

function applyContextUsage(runtime: RuntimeSnapshot): void {
  for (let i = runtime.timeline.length - 1; i >= 0; i -= 1) {
    const item = runtime.timeline[i]
    if (item?.type !== 'commentary' || item.author !== '上下文') continue
    const match = item.body.match(/([\d,]+)\s*\/\s*([\d,]+)/)
    if (!match) continue
    runtime.contextUsed = Number(match[1]?.replace(/,/g, '')) || 0
    runtime.contextWindow = Number(match[2]?.replace(/,/g, '')) || 1
    return
  }
}

function connectionFromName(value: string): DeviceConnection {
  if (value.endsWith('ONLINE')) return 'ONLINE'
  if (value.endsWith('DEGRADED')) return 'DEGRADED'
  if (value.endsWith('CONNECTING')) return 'CONNECTING'
  return 'OFFLINE'
}

function connectionFromProto(value: number): DeviceConnection {
  if (value === 2) return 'ONLINE'
  if (value === 3) return 'DEGRADED'
  if (value === 1) return 'CONNECTING'
  return 'OFFLINE'
}

function controlFromName(value: string): ControlMode {
  if (value.endsWith('FULL_CONTROL')) return 'FULL_CONTROL'
  if (value.endsWith('LIMITED_CONTROL')) return 'LIMITED_CONTROL'
  if (value.endsWith('READ_ONLY')) return 'READ_ONLY'
  return 'UNAVAILABLE'
}

function controlFromProto(value: number): ControlMode {
  if (value === PbControlMode.FULL_CONTROL) return 'FULL_CONTROL'
  if (value === PbControlMode.LIMITED_CONTROL) return 'LIMITED_CONTROL'
  if (value === PbControlMode.READ_ONLY) return 'READ_ONLY'
  return 'UNAVAILABLE'
}

function compatibilityFromName(value: string): CompatibilityState {
  if (value.endsWith('VERIFIED')) return 'VERIFIED'
  if (value.endsWith('UNSUPPORTED')) return 'UNSUPPORTED'
  return 'DEGRADED'
}

function compatibilityFromProto(value: number): CompatibilityState {
  if (value === PbCompatibilityState.COMPATIBILITY_VERIFIED) return 'VERIFIED'
  if (value === PbCompatibilityState.COMPATIBILITY_UNSUPPORTED) return 'UNSUPPORTED'
  return 'DEGRADED'
}

function phaseFromName(value: string): ActiveTurnPhase {
  if (value.endsWith('RUNNING')) return 'RUNNING'
  if (value.endsWith('FINISHING')) return 'FINISHING'
  return 'IDLE'
}

function phaseFromProto(value: number): ActiveTurnPhase {
  if (value === PbActiveTurnPhase.TURN_PHASE_RUNNING) return 'RUNNING'
  if (value === PbActiveTurnPhase.TURN_PHASE_FINISHING) return 'FINISHING'
  return 'IDLE'
}

function queueFromName(value: string): QueueStatus {
  if (value.endsWith('QUEUED')) return 'QUEUED'
  if (value.endsWith('PAUSED')) return 'PAUSED'
  return 'EMPTY'
}

function queueFromProto(value: number): QueueStatus {
  if (value === PbQueueState.QUEUED) return 'QUEUED'
  if (value === PbQueueState.PAUSED) return 'PAUSED'
  return 'EMPTY'
}

function outcomeFromName(value: string): LastTurnOutcome {
  if (value.endsWith('COMPLETED')) return 'COMPLETED'
  if (value.endsWith('FAILED')) return 'FAILED'
  if (value.endsWith('INTERRUPTED')) return 'INTERRUPTED'
  return 'UNKNOWN'
}

function outcomeFromProto(value: number): LastTurnOutcome {
  if (value === PbLastTurnOutcome.TURN_OUTCOME_COMPLETED) return 'COMPLETED'
  if (value === PbLastTurnOutcome.TURN_OUTCOME_FAILED) return 'FAILED'
  if (value === PbLastTurnOutcome.TURN_OUTCOME_INTERRUPTED) return 'INTERRUPTED'
  return 'UNKNOWN'
}

function backgroundStateFromName(value: string): BackgroundCommandState {
  if (value.endsWith('COMPLETED')) return 'COMPLETED'
  if (value.endsWith('FAILED')) return 'FAILED'
  if (value.endsWith('STOPPED')) return 'STOPPED'
  if (value.endsWith('RUNNING')) return 'RUNNING'
  return 'UNKNOWN'
}

function backgroundStateFromProto(value: number): BackgroundCommandState {
  if (value === PbBackgroundCommandState.BACKGROUND_CMD_COMPLETED) return 'COMPLETED'
  if (value === PbBackgroundCommandState.BACKGROUND_CMD_FAILED) return 'FAILED'
  if (value === PbBackgroundCommandState.BACKGROUND_CMD_STOPPED) return 'STOPPED'
  if (value === PbBackgroundCommandState.BACKGROUND_CMD_RUNNING) return 'RUNNING'
  return 'UNKNOWN'
}

function receiptStatusFromProto(value: number): ReceiptStatus {
  if (value === CommandReceiptStatus.RECEIPT_ACCEPTED_BY_BRIDGE) return 'ACCEPTED_BY_BRIDGE'
  if (value === CommandReceiptStatus.RECEIPT_DISPATCHED_TO_CODEX) return 'DISPATCHED_TO_CODEX'
  if (value === CommandReceiptStatus.RECEIPT_COMPLETED) return 'COMPLETED'
  if (value === CommandReceiptStatus.RECEIPT_REJECTED) return 'REJECTED'
  if (value === CommandReceiptStatus.RECEIPT_OUTCOME_UNKNOWN) return 'OUTCOME_UNKNOWN'
  return 'RECEIVED'
}

function receiptStatusToProto(value: string): CommandReceiptStatus {
  if (value === 'ACCEPTED_BY_BRIDGE') return CommandReceiptStatus.RECEIPT_ACCEPTED_BY_BRIDGE
  if (value === 'DISPATCHED_TO_CODEX') return CommandReceiptStatus.RECEIPT_DISPATCHED_TO_CODEX
  if (value === 'COMPLETED') return CommandReceiptStatus.RECEIPT_COMPLETED
  if (value === 'REJECTED') return CommandReceiptStatus.RECEIPT_REJECTED
  if (value === 'OUTCOME_UNKNOWN') return CommandReceiptStatus.RECEIPT_OUTCOME_UNKNOWN
  return CommandReceiptStatus.RECEIPT_RECEIVED
}

function stableErrorName(value: number): string {
  const names = [
    '', 'AUTH_REQUIRED', 'AUTH_EXPIRED', 'CSRF_INVALID', 'WS_TICKET_INVALID',
    'WS_TICKET_EXPIRED', 'WS_TICKET_CONSUMED', 'DEVICE_OFFLINE', 'DEVICE_REVOKED',
    'CODEX_UNAVAILABLE', 'CODEX_VERSION_UNVERIFIED', 'CONTROL_READ_ONLY',
    'CAPABILITY_UNSUPPORTED', 'SESSION_NOT_FOUND', 'STALE_TURN',
    'DUPLICATE_REQUEST_MISMATCH', 'OUTCOME_UNKNOWN', 'RESYNC_REQUIRED',
    'QUEUE_ALREADY_EXISTS', 'QUEUE_PAUSED', 'QUESTION_EXPIRED', 'APPROVAL_EXPIRED',
    'SETTING_COMBINATION_UNSUPPORTED', 'FILE_HANDLE_INVALID', 'FILE_OUTSIDE_SCOPE',
    'FILE_CHANGED', 'FILE_TYPE_NOT_PREVIEWABLE', 'TRANSFER_EXPIRED',
    'TRANSFER_TOO_LARGE', 'TRANSFER_RANGE_INVALID', 'DIFF_TOO_LARGE', 'RATE_LIMITED',
    'INTERNAL_ERROR',
  ]
  return names[value] ?? `ERROR_${value}`
}

function stableErrorToProto(value: string): number {
  for (let index = 1; index <= 32; index += 1) if (stableErrorName(index) === value) return index
  return 0
}

function isTerminalReceipt(value: ReceiptStatus): boolean {
  return value === 'COMPLETED' || value === 'REJECTED' || value === 'OUTCOME_UNKNOWN'
}

/** 把主动关闭码映射为可解释的失败阶段与稳定错误码(IN-01/UX-02)。 */
function closeFailure(event: CloseEvent): { stage: RelayLinkStage; errorCode: string } | undefined {
  switch (event.code) {
    case WS_CLOSE_HEARTBEAT_TIMEOUT:
      return { stage: 'HEARTBEAT', errorCode: 'HEARTBEAT_TIMEOUT' }
    case WS_CLOSE_RESYNC_TIMEOUT:
      return { stage: 'STREAM', errorCode: 'RESYNC_TIMEOUT' }
    case WS_CLOSE_HANDSHAKE_TIMEOUT:
      return { stage: 'HANDSHAKE', errorCode: 'HANDSHAKE_TIMEOUT' }
    default:
      return event.code === 1006 || event.code === 1001
        ? { stage: 'RECONNECT', errorCode: 'CONNECTION_LOST' }
        : undefined
  }
}

function targetEqual(left: SubscriptionTarget, right: SubscriptionTarget): boolean {
  return left.kind === right.kind && (left.kind === 'list' || left.sessionId === (right as { sessionId: string }).sessionId)
}

function targetKey(target: SubscriptionTarget): string {
  return target.kind === 'list' ? 'list' : `session:${target.sessionId}`
}

/** 带超时的 AbortSignal:请求结束(含失败)即清理定时器,不在事件循环里留悬挂 timer。 */
function withTimeoutSignal<T>(milliseconds: number, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), milliseconds)
  return run(controller.signal).finally(() => window.clearTimeout(timer))
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
}

function timestampToIso(value: { seconds: bigint; nanos: number } | undefined): string {
  if (!value) return ''
  return new Date(Number(value.seconds) * 1000 + value.nanos / 1_000_000).toISOString()
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' ? (value as Record<string, unknown>) : {}
}

function recordOrUndefined(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' ? (value as Record<string, unknown>) : undefined
}

function stringOrEmpty(value: unknown): string {
  return typeof value === 'string' ? value : value === undefined || value === null ? '' : String(value)
}

function numberOrZero(value: unknown): number {
  if (typeof value === 'bigint') return safeNumber(value)
  const number = Number(value)
  return Number.isFinite(number) ? number : 0
}

function safeNumber(value: bigint): number {
  const number = Number(value)
  return Number.isSafeInteger(number) ? number : Number.MAX_SAFE_INTEGER
}

function decodeBase64Utf8(value: string): string {
  if (!value) return ''
  const binary = atob(value)
  const bytes = Uint8Array.from(binary, (character) => character.charCodeAt(0))
  return new TextDecoder().decode(bytes)
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`
  if (value && typeof value === 'object') {
    const object = value as Record<string, unknown>
    return `{${Object.keys(object)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(object[key])}`)
      .join(',')}}`
  }
  return JSON.stringify(value) ?? 'null'
}

async function sha256Hex(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value))
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('')
}
