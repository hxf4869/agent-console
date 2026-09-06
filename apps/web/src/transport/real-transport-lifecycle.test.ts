import { create } from '@bufbuild/protobuf'
import {
  decodeEnvelope,
  DomainEventSchema,
  encodeEnvelope,
  EnvelopeSchema,
  EventBatchSchema,
  ItemIdSchema,
  OutputChannel,
  OutputReplaceSchema,
  PROTOCOL_VERSION,
  RuntimeSnapshotSchema,
  ServerHelloSchema,
  SessionKeySchema,
  SessionSummaryBatchSchema,
  SessionSummarySchema,
  SubscribedSchema,
  type Envelope,
} from '@agent-console/protocol/source'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { RealConsoleTransport } from './real-transport'
import type { ConsoleEvent } from './types'

const LIST_STREAM = 'st-list'
const SESSION_STREAM = 'st-session'
const DEVICE_A = 'device-aaaa-aaaa'
const TICKET_PATH = '/api/v1/agent-console/ws-tickets'

function toFrame(envelope: Envelope): ArrayBuffer {
  const bytes = encodeEnvelope(envelope)
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}

function frame(payload: Envelope['payload'], streamId: string, sequence: bigint, epoch = 1n): Envelope {
  return create(EnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: '00000000-0000-4000-8000-0000000000aa',
    streamId,
    streamEpoch: epoch,
    sequence,
    payload,
  })
}

function summaryProto(relayId: string, deviceId: string, title: string) {
  return create(SessionSummarySchema, {
    sessionKey: create(SessionKeySchema, {
      deviceId,
      agentKind: 1,
      nativeSessionId: `${relayId}-native`,
      relaySessionUuid: relayId,
    }),
    title,
    agentKind: 1,
  })
}

function helloEnvelope(): Envelope {
  return create(EnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: '00000000-0000-4000-8000-0000000000ff',
    payload: {
      case: 'serverHello',
      value: create(ServerHelloSchema, { acceptedProtocolVersion: PROTOCOL_VERSION }),
    },
  })
}

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static instances: FakeWebSocket[] = []
  /** 设为 false 可模拟服务端握手后不回 ServerHello。 */
  static greetOnOpen = true

  readyState = FakeWebSocket.CONNECTING
  binaryType: BinaryType = 'blob'
  sent: ArrayBuffer[] = []
  closeCode = 0
  closeReason = ''

  constructor(_url: string | URL, _protocols?: string | string[]) {
    super()
    FakeWebSocket.instances.push(this)
    // 用真实微任务模拟异步 open:fake timers 下 tick 循环不会执行
    // "fire 期间在微任务链中注册的 0ms timer",queueMicrotask 语义也更贴近浏览器。
    queueMicrotask(() => {
      this.readyState = FakeWebSocket.OPEN
      this.dispatchEvent(new Event('open'))
      if (FakeWebSocket.greetOnOpen) {
        this.dispatchEvent(new MessageEvent('message', { data: toFrame(helloEnvelope()) }))
      }
    })
  }

  send(data: ArrayBuffer): void {
    this.sent.push(data)
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState === FakeWebSocket.CLOSED) return
    this.readyState = FakeWebSocket.CLOSED
    this.closeCode = code
    this.closeReason = reason
    this.dispatchEvent(new CloseEvent('close', { code, reason }))
  }

  push(envelope: Envelope): void {
    this.dispatchEvent(new MessageEvent('message', { data: toFrame(envelope) }))
  }

  sentPayloads(): Array<Envelope['payload']> {
    return this.sent.map((data) => decodeEnvelope(new Uint8Array(data)).payload)
  }

  payloadCount(caseName: string): number {
    return this.sentPayloads().filter((payload) => payload.case === caseName).length
  }
}

function defaultFetchMock(): (input: RequestInfo | URL) => Promise<Response> {
  return async (input: RequestInfo | URL) => {
    const path = String(input)
    let body: unknown = { ticket: 'ticket-test' }
    if (path.endsWith('/api/v1/auth/session')) {
      body = { csrfToken: 'csrf-test' }
    } else if (path.includes('/runtime')) {
      body = {
        runtimeSnapshot: {
          runtimeRevision: 1,
          pendingQuestions: [],
          pendingApprovals: [],
          backgroundCommands: [],
          backgroundCommandCount: 0,
        },
      }
    } else if (path.includes('/history')) {
      body = { historyPage: { entries: [] } }
    } else if (path.includes('/output')) {
      body = {
        commandOutputPage: {
          bytesBase64: btoa('late output'),
          nextCursor: null,
        },
      }
    }
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }
}

async function startTransport(
  events: ConsoleEvent[],
  fetchMock?: (input: RequestInfo | URL) => Promise<Response>,
) {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.stubGlobal('fetch', vi.fn(fetchMock ?? defaultFetchMock()))
  const transport = new RealConsoleTransport()
  const connecting = transport.connect((event) => events.push(event))
  await vi.advanceTimersByTimeAsync(0)
  const disconnect = await connecting
  await vi.advanceTimersByTimeAsync(0)
  const socket = FakeWebSocket.instances.at(-1)!
  // 完成 list 订阅:Subscribed(base=1) → 快照帧 seq=1,写入 session 键。
  socket.push(
    frame(
      {
        case: 'subscribed',
        value: create(SubscribedSchema, { streamId: LIST_STREAM, streamEpoch: 1n, baseSequence: 1n }),
      },
      LIST_STREAM,
      0n,
    ),
  )
  socket.push(
    frame(
      {
        case: 'sessionSummaryBatch',
        value: create(SessionSummaryBatchSchema, {
          snapshot: true,
          summaries: [summaryProto('session-1', DEVICE_A, '任务一')],
        }),
      },
      LIST_STREAM,
      1n,
    ),
  )
  await vi.advanceTimersByTimeAsync(0)
  return { transport, socket, disconnect }
}

/** 完成一次详情订阅(Subscribed + RuntimeSnapshot),返回 getRuntimeSnapshot 的 Promise。 */
async function openDetail(
  transport: RealConsoleTransport,
  socket: FakeWebSocket,
  sessionId = 'session-1',
): Promise<Promise<unknown>> {
  transport.retainRuntime(sessionId)
  const runtimePromise = transport.getRuntimeSnapshot(sessionId)
  await vi.advanceTimersByTimeAsync(0)
  socket.push(
    frame(
      {
        case: 'subscribed',
        value: create(SubscribedSchema, { streamId: SESSION_STREAM, streamEpoch: 1n, baseSequence: 1n }),
      },
      SESSION_STREAM,
      0n,
    ),
  )
  socket.push(
    frame({ case: 'runtimeSnapshot', value: create(RuntimeSnapshotSchema, {}) }, SESSION_STREAM, 1n),
  )
  await vi.advanceTimersByTimeAsync(0)
  return runtimePromise
}

/** 构造含 outputReplace(pageCursor) 的 EventBatch payload(详情流)。 */
function outputReplacePageCursorEvent(itemId: string, cursor: string): Envelope['payload'] {
  return {
    case: 'eventBatch',
    value: create(EventBatchSchema, {
      streamId: SESSION_STREAM,
      events: [
        create(DomainEventSchema, {
          event: {
            case: 'outputReplace',
            value: create(OutputReplaceSchema, {
              itemId: create(ItemIdSchema, { id: itemId, synthetic: false }),
              revision: 3n,
              content: { case: 'pageCursor', value: cursor },
              channel: OutputChannel.COMBINED,
            }),
          },
        }),
      ],
    }),
  }
}

beforeEach(() => {
  // fake timers 会阻塞 Node webcrypto 的内部完成调度;digest 值不影响被测行为,
  // 用同步 resolve 的替身保持 sendCommand 可测。
  vi.spyOn(crypto.subtle, 'digest').mockResolvedValue(new ArrayBuffer(32))
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
  FakeWebSocket.instances = []
  FakeWebSocket.greetOnOpen = true
})

describe('real transport lifecycle (AC-06)', () => {
  it('T21: releases browsed detail subscriptions; reconnect restores only the live list', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events)

    const runtimePromise = openDetail(transport, socket)
    await runtimePromise

    // 浏览 99 个其他详情再回列表(无 session 键,不建立流),逐个释放。
    for (let index = 0; index < 99; index += 1) {
      const sessionId = `session-browse-${index}`
      transport.retainRuntime(sessionId)
      await transport.getRuntimeSnapshot(sessionId)
      transport.releaseRuntime(sessionId)
    }

    // 离开当前详情:活跃详情订阅回落,发送 Unsubscribe。
    transport.releaseRuntime('session-1')
    await vi.advanceTimersByTimeAsync(0)
    expect(socket.payloadCount('unsubscribe')).toBe(1)
    const unsubscribed = socket
      .sentPayloads()
      .filter((payload) => payload.case === 'unsubscribe')
      .at(-1)
    expect(unsubscribed && 'streamId' in unsubscribed.value && unsubscribed.value.streamId).toBe(
      SESSION_STREAM,
    )

    // 释放后,该流上的迟到事件不再应用。
    socket.push(
      frame(outputReplacePageCursorEvent('item-1', 'cursor-1'), SESSION_STREAM, 2n),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(events.filter((event) => event.type === 'output')).toHaveLength(0)

    // 断线重连:只恢复全局列表订阅,不重放 100 条浏览历史。
    socket.close(1006, 'NETWORK')
    await vi.advanceTimersByTimeAsync(500)
    const reconnected = FakeWebSocket.instances.at(-1)!
    expect(reconnected).not.toBe(socket)
    await vi.advanceTimersByTimeAsync(0)
    const subscribes = reconnected.sentPayloads().filter((payload) => payload.case === 'subscribe')
    expect(subscribes).toHaveLength(1)
    expect(subscribes[0] && 'target' in subscribes[0].value && subscribes[0].value.target.case).toBe(
      'list',
    )
    disconnect()
  })

  it('T22: a still-subscribed detail catches a failed late output completion as unavailable', async () => {
    vi.useFakeTimers()
    let failOutput: ((error: Error) => void) | undefined
    const fetchMock = async (input: RequestInfo | URL): Promise<Response> => {
      if (String(input).includes('/output')) {
        return new Promise<Response>((_, reject) => {
          failOutput = reject
        })
      }
      return defaultFetchMock()(input)
    }
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events, fetchMock)

    await openDetail(transport, socket)
    socket.push(frame(outputReplacePageCursorEvent('item-1', 'cursor-1'), SESSION_STREAM, 2n))
    await vi.advanceTimersByTimeAsync(0)
    expect(failOutput).toBeDefined()

    failOutput!(new Error('HTTP_500'))
    await vi.advanceTimersByTimeAsync(0)

    // 捕获失败:标记完整输出暂不可用,而不是清空或悬挂。
    const unavailable = events.find(
      (event) => event.type === 'output' && event.event.type === 'unavailable',
    )
    expect(unavailable).toMatchObject({
      sessionId: 'session-1',
      event: { type: 'unavailable', itemId: 'item-1' },
    })
    disconnect()
  })

  it('T22: a released detail no longer replays a late output completion', async () => {
    vi.useFakeTimers()
    let settleOutput: ((response: Response) => void) | undefined
    const fetchMock = async (input: RequestInfo | URL): Promise<Response> => {
      if (String(input).includes('/output')) {
        return new Promise<Response>((resolve) => {
          settleOutput = resolve
        })
      }
      return defaultFetchMock()(input)
    }
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events, fetchMock)

    await openDetail(transport, socket)
    socket.push(frame(outputReplacePageCursorEvent('item-1', 'cursor-1'), SESSION_STREAM, 2n))
    await vi.advanceTimersByTimeAsync(0)
    expect(settleOutput).toBeDefined()

    // 离开详情后再让补全返回:迟到数据不回放(unavailable 同样不发)。
    transport.releaseRuntime('session-1')
    settleOutput!(
      new Response(JSON.stringify({ commandOutputPage: { bytesBase64: btoa('late output') } }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(events.filter((event) => event.type === 'output')).toHaveLength(0)
    disconnect()
  })

  it('T23: disconnect during a pending reconnect handshake never publishes ONLINE', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    let gateSecondTicket: (() => void) | undefined
    const secondTicket = new Promise<void>((resolve) => {
      gateSecondTicket = resolve
    })
    let ticketRequests = 0
    const fetchMock = async (input: RequestInfo | URL): Promise<Response> => {
      const path = String(input)
      if (path.endsWith(TICKET_PATH)) {
        ticketRequests += 1
        if (ticketRequests === 2) await secondTicket
      }
      return defaultFetchMock()(input)
    }
    const { transport, socket, disconnect } = await startTransport(events, fetchMock)

    // 网络断开 → 重连调度 → 取票请求挂起期间执行 logout(disconnect)。
    socket.close(1006, 'NETWORK')
    await vi.advanceTimersByTimeAsync(500)
    expect(ticketRequests).toBe(2)
    disconnect()
    gateSecondTicket!()
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(0)

    // 旧取票 Promise 完成后:不创建新连接,不重新发布 ONLINE。
    expect(FakeWebSocket.instances).toHaveLength(1)
    // 事件序列:CONNECTING → ONLINE → OFFLINE → CONNECTING;此后无 ONLINE。
    // (IN-01 起 link 事件与会话事件交错;按最后一条 CONNECTING 切片保持同一断言意图。)
    expect(events.filter((event) => event.type === 'connection')).toHaveLength(4)
    const lastConnectingIndex = events.reduce(
      (found, event, index) => (event.type === 'connection' && event.state === 'CONNECTING' ? index : found),
      -1,
    )
    expect(
      events
        .slice(lastConnectingIndex + 1)
        .filter((event) => event.type === 'connection' && event.state === 'ONLINE'),
    ).toHaveLength(0)
  })

  it('T23: disconnect clears pending commands; a fresh connect neither replays nor backfills them', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { transport, disconnect } = await startTransport(events)

    const commandPromise = transport.sendCommand({
      requestId: 'req-logout-1',
      operation: 'INTERRUPT',
      sessionId: 'session-1',
      expectedRuntimeRevision: 0,
      payload: {},
    })
    commandPromise.catch(() => undefined)
    await vi.advanceTimersByTimeAsync(0)
    const firstSocket = FakeWebSocket.instances.at(-1)!
    expect(firstSocket.payloadCount('commandRequest')).toBe(1)

    disconnect()
    await expect(commandPromise).rejects.toThrow('实时连接已关闭。')

    // 重新登录后的新连接:不重发旧命令,也不查询旧请求回执回填。
    const reconnecting = transport.connect((event) => events.push(event))
    await vi.advanceTimersByTimeAsync(0)
    await reconnecting
    await vi.advanceTimersByTimeAsync(0)
    const secondSocket = FakeWebSocket.instances.at(-1)!
    expect(secondSocket).not.toBe(firstSocket)
    expect(secondSocket.payloadCount('commandRequest')).toBe(0)
    const requestedPaths = (globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls.map((call) =>
      String(call[0]),
    )
    expect(requestedPaths.some((path) => path.includes('/requests/'))).toBe(false)
    disconnect()
  })

  it('closes with a handshake timeout and reconnects when the server never greets', async () => {
    vi.useFakeTimers()
    FakeWebSocket.greetOnOpen = false
    const events: ConsoleEvent[] = []
    vi.stubGlobal('WebSocket', FakeWebSocket)
    vi.stubGlobal('fetch', vi.fn(defaultFetchMock()))
    const transport = new RealConsoleTransport()
    const connecting = transport.connect((event) => events.push(event))
    // 尽早附加 rejection 断言,避免未处理 rejection。
    const rejected = expect(connecting).rejects.toThrow('WebSocket 在握手完成前关闭。')
    await vi.advanceTimersByTimeAsync(0)

    // 握手超时:主动关闭(私有码),并继续调度重连。
    await vi.advanceTimersByTimeAsync(10_000)
    const firstSocket = FakeWebSocket.instances[0]!
    expect(firstSocket.closeCode).toBe(4004)
    expect(firstSocket.closeReason).toBe('HANDSHAKE_TIMEOUT')

    FakeWebSocket.greetOnOpen = true
    await vi.advanceTimersByTimeAsync(500)
    await vi.advanceTimersByTimeAsync(0)
    const secondSocket = FakeWebSocket.instances.at(-1)!
    expect(secondSocket).not.toBe(firstSocket)
    expect(events.filter((event) => event.type === 'connection').at(-1)).toMatchObject({
      state: 'ONLINE',
    })
    await rejected
  })
})
