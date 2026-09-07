import { create } from '@bufbuild/protobuf'
import {
  ActiveTurnPhase,
  decodeEnvelope,
  DomainEventSchema,
  encodeEnvelope,
  EnvelopeSchema,
  EventBatchSchema,
  PROTOCOL_VERSION,
  RuntimeSnapshotSchema,
  ServerHelloSchema,
  SessionKeySchema,
  SessionSummaryBatchSchema,
  SessionSummarySchema,
  SubscribedSchema,
  TurnLifecycleSchema,
  type DomainEvent,
  type Envelope,
} from '@agent-console/protocol/source'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { RealConsoleTransport } from './real-transport'
import type { ConsoleEvent } from './types'

const LIST_STREAM = 'st-list'
const SESSION_STREAM = 'st-session'
const DEVICE_A = 'device-aaaa-aaaa'

function toFrame(envelope: Envelope): ArrayBuffer {
  const bytes = encodeEnvelope(envelope)
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}

function frame(
  payload: Envelope['payload'],
  streamId: string,
  sequence: bigint,
  epoch = 1n,
  correlation = '',
): Envelope {
  return create(EnvelopeSchema, {
    protocolVersion: PROTOCOL_VERSION,
    messageId: '00000000-0000-4000-8000-0000000000aa',
    ...(correlation ? { correlationId: correlation } : {}),
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

function summaryChangedEvent(relayId: string, deviceId: string, title: string): DomainEvent {
  return create(DomainEventSchema, {
    event: {
      case: 'sessionSummaryChanged',
      value: summaryProto(relayId, deviceId, title),
    },
  })
}

function turnLifecycleEvent(phase: ActiveTurnPhase): DomainEvent {
  return create(DomainEventSchema, {
    event: {
      case: 'turnLifecycle',
      value: create(TurnLifecycleSchema, { phase }),
    },
  })
}

function subscribed(streamId: string, epoch: bigint, base: bigint, correlation = ''): Envelope {
  return frame(
    {
      case: 'subscribed',
      value: create(SubscribedSchema, {
        streamId,
        streamEpoch: epoch,
        baseSequence: base,
      }),
    },
    streamId,
    0n,
    1n,
    correlation,
  )
}

class FakeWebSocket extends EventTarget {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static instances: FakeWebSocket[] = []

  readyState = FakeWebSocket.CONNECTING
  binaryType: BinaryType = 'blob'
  sent: ArrayBuffer[] = []
  closeCode = 0
  closeReason = ''

  constructor(_url: string | URL, _protocols?: string | string[]) {
    super()
    FakeWebSocket.instances.push(this)
    queueMicrotask(() => {
      this.readyState = FakeWebSocket.OPEN
      this.dispatchEvent(new Event('open'))
      this.dispatchEvent(
        new MessageEvent('message', {
          data: toFrame(
            create(EnvelopeSchema, {
              protocolVersion: PROTOCOL_VERSION,
              messageId: '00000000-0000-4000-8000-0000000000ff',
              payload: {
                case: 'serverHello',
                value: create(ServerHelloSchema, { acceptedProtocolVersion: PROTOCOL_VERSION }),
              },
            }),
          ),
        }),
      )
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

  sentEnvelopes(): Envelope[] {
    return this.sent.map((data) => decodeEnvelope(new Uint8Array(data)))
  }

  /** 第 n 个(默认最后)会话订阅的 correlation_id,供 Subscribed 回显。 */
  sessionSubscribeCorrelation(index = -1): string {
    const list = this.sentEnvelopes()
      .filter(
        (env) =>
          env.payload.case === 'subscribe' && env.payload.value.target?.case === 'session',
      )
      .map((env) => env.correlationId)
    return list.at(index) ?? ''
  }

  payloadCount(caseName: string): number {
    return this.sentPayloads().filter((payload) => payload.case === caseName).length
  }

  lastAck(streamId: string): bigint | undefined {
    const ack = this
      .sentPayloads()
      .filter((payload) => payload.case === 'ack' && payload.value.streamId === streamId)
      .at(-1)
    return ack && ack.case === 'ack' ? ack.value.sequence : undefined
  }
}

type Listener = (event: ConsoleEvent) => void

async function startTransport(listener: Listener) {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
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
      }
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    }),
  )
  const transport = new RealConsoleTransport()
  const connecting = transport.connect(listener)
  await vi.advanceTimersByTimeAsync(0)
  const disconnect = await connecting
  await vi.advanceTimersByTimeAsync(0)
  const socket = FakeWebSocket.instances.at(-1)!
  // 完成 list 订阅(epoch 1):Subscribed(base=1) → 快照 seq=1。
  socket.push(subscribed(LIST_STREAM, 1n, 1n))
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

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  FakeWebSocket.instances = []
})

describe('real transport snapshot epoch recovery (R2-AC01)', () => {
  it('rebinds a known stream to a new epoch and accepts the fresh snapshot without a new connection', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { socket, disconnect } = await startTransport((event) => events.push(event))

    // 服务端合法切换 stream epoch:重发 Subscribed(epoch 2) + 快照。
    socket.push(subscribed(LIST_STREAM, 2n, 5n))
    socket.push(
      frame(
        {
          case: 'sessionSummaryBatch',
          value: create(SessionSummaryBatchSchema, {
            snapshot: true,
            summaries: [summaryProto('session-1', DEVICE_A, '新纪元快照')],
          }),
        },
        LIST_STREAM,
        5n,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)

    // 新快照必须被应用(不因 epoch 不同被拒绝),序号提交并 ACK。
    const snapshot = events
      .filter((event) => event.type === 'sessions' && event.snapshot)
      .at(-1)
    expect(snapshot && 'sessions' in snapshot && snapshot.sessions[0]).toMatchObject({
      title: '新纪元快照',
    })
    expect(socket.lastAck(LIST_STREAM)).toBe(5n)
    expect(socket.payloadCount('resyncRequest')).toBe(0)

    // epoch 2 的后续增量按新坐标连续应用。
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [summaryChangedEvent('session-1', DEVICE_A, '新纪元增量')],
          }),
        },
        LIST_STREAM,
        6n,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    const delta = events
      .filter((event) => event.type === 'sessions' && !event.snapshot)
      .at(-1)
    expect(delta && 'sessions' in delta && delta.sessions[0]).toMatchObject({
      title: '新纪元增量',
    })
    expect(socket.lastAck(LIST_STREAM)).toBe(6n)
    disconnect()
  })

  it('rebinding a known stream does not hijack a pending new-subscription response', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport((event) => events.push(event))

    // 新详情订阅已发出、Subscribed 未回:pending 订阅在途(list + session 共两帧)。
    const runtimePromise = transport.getRuntimeSnapshot('session-1')
    await vi.advanceTimersByTimeAsync(0)
    const subscribes = socket.sentPayloads().filter((payload) => payload.case === 'subscribe')
    expect(subscribes.length).toBe(2)
    expect(
      subscribes.at(-1) && subscribes.at(-1)!.case === 'subscribe'
        ? subscribes.at(-1)!.value.target.case
        : '',
    ).toBe('session')

    // 先到已知 list 流的重绑定响应(新 epoch),再到新详情流的订阅响应。
    socket.push(subscribed(LIST_STREAM, 2n, 5n))
    socket.push(subscribed(SESSION_STREAM, 1n, 1n, socket.sessionSubscribeCorrelation()))
    socket.push(
      frame(
        {
          case: 'runtimeSnapshot',
          value: create(RuntimeSnapshotSchema, {}),
        },
        SESSION_STREAM,
        1n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    await runtimePromise

    // 详情流按 pending target 建立,不被重绑定抢占;两条流各自 ACK。
    expect(events.some((event) => event.type === 'runtime-snapshot')).toBe(true)
    expect(socket.lastAck(SESSION_STREAM)).toBe(1n)
    // 重绑定的 Subscribed 不会让 list 流提交任何新序号(仅初始快照的 ACK(1))。
    expect(socket.lastAck(LIST_STREAM)).toBe(1n)

    // 重绑定后的 list 流按新 epoch 接受快照。
    socket.push(
      frame(
        {
          case: 'sessionSummaryBatch',
          value: create(SessionSummaryBatchSchema, {
            snapshot: true,
            summaries: [summaryProto('session-2', DEVICE_A, '重绑定后快照')],
          }),
        },
        LIST_STREAM,
        5n,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    const snapshot = events
      .filter((event) => event.type === 'sessions' && event.snapshot)
      .at(-1)
    expect(snapshot && 'sessions' in snapshot && snapshot.sessions[0]).toMatchObject({
      title: '重绑定后快照',
    })
    expect(socket.lastAck(LIST_STREAM)).toBe(5n)
    disconnect()
  })

  it('ignores a Subscribed response for an unknown stream without pending subscription', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { socket, disconnect } = await startTransport((event) => events.push(event))

    // 无 pending、未知 streamId:不建立状态,后续快照不应用、不 ACK。
    socket.push(subscribed('st-unknown', 1n, 1n))
    socket.push(
      frame(
        {
          case: 'sessionSummaryBatch',
          value: create(SessionSummaryBatchSchema, {
            snapshot: true,
            summaries: [summaryProto('session-x', DEVICE_A, '未知流快照')],
          }),
        },
        'st-unknown',
        1n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)

    // 只有 startTransport 的初始快照;未知流的快照未应用。
    const snapshots = events.filter(
      (event) => event.type === 'sessions' && event.snapshot,
    )
    expect(snapshots).toHaveLength(1)
    expect(socket.lastAck('st-unknown')).toBeUndefined()
    disconnect()
  })

  it('keeps resync state when snapshot application fails; commits sequence only after success', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    let failSnapshots = false
    const listener: Listener = (event) => {
      if (failSnapshots && event.type === 'sessions' && event.snapshot) {
        throw new Error('apply failed')
      }
      events.push(event)
    }
    const { socket, disconnect } = await startTransport(listener)

    // 快照应用失败:不发 ACK,发起受控恢复(恢复状态保留)。
    failSnapshots = true
    socket.push(
      frame(
        {
          case: 'sessionSummaryBatch',
          value: create(SessionSummaryBatchSchema, {
            snapshot: true,
            summaries: [summaryProto('session-9', DEVICE_A, '失败快照')],
          }),
        },
        LIST_STREAM,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(socket.payloadCount('resyncRequest')).toBe(1)
    // 失败快照未提交序号:仍停留在初始快照的 ACK(1)。
    expect(socket.lastAck(LIST_STREAM)).toBe(1n)

    // 应用恢复后,同一坐标的快照成功:提交序号并 ACK,恢复标记解除。
    failSnapshots = false
    socket.push(
      frame(
        {
          case: 'sessionSummaryBatch',
          value: create(SessionSummaryBatchSchema, {
            snapshot: true,
            summaries: [summaryProto('session-9', DEVICE_A, '恢复后快照')],
          }),
        },
        LIST_STREAM,
        3n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    const snapshot = events
      .filter((event) => event.type === 'sessions' && event.snapshot)
      .at(-1)
    expect(snapshot && 'sessions' in snapshot && snapshot.sessions[0]).toMatchObject({
      title: '恢复后快照',
    })
    expect(socket.lastAck(LIST_STREAM)).toBe(3n)
    // 后续增量恢复正常应用(在途恢复标记已被成功快照解除)。
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [summaryChangedEvent('session-9', DEVICE_A, '恢复后增量')],
          }),
        },
        LIST_STREAM,
        4n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(socket.lastAck(LIST_STREAM)).toBe(4n)
    disconnect()
  })

  // 服务端 replay_base 有两种合法窗口形态:base 为已应用水位。
  // 快照帧开头:首帧 seq=base;事件帧开头:首帧 seq=base+1(无快照帧)。

  it('applies an event-first window when rebinding a known stream (first event at base + 1)', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { socket, disconnect } = await startTransport((event) => events.push(event))

    // 重绑定 Subscribed(base=5) 后窗口以事件帧开头:首事件 seq=6,无快照帧。
    socket.push(subscribed(LIST_STREAM, 2n, 5n))
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [summaryChangedEvent('session-1', DEVICE_A, '事件窗首帧')],
          }),
        },
        LIST_STREAM,
        6n,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)

    // 首事件必须被应用,不得因 base 语义误判为缺口而触发 resync。
    const delta = events
      .filter((event) => event.type === 'sessions' && !event.snapshot)
      .at(-1)
    expect(delta && 'sessions' in delta && delta.sessions[0]).toMatchObject({
      title: '事件窗首帧',
    })
    expect(socket.lastAck(LIST_STREAM)).toBe(6n)
    expect(socket.payloadCount('resyncRequest')).toBe(0)

    // 后续事件按新坐标连续应用。
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [summaryChangedEvent('session-1', DEVICE_A, '事件窗次帧')],
          }),
        },
        LIST_STREAM,
        7n,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    const next = events
      .filter((event) => event.type === 'sessions' && !event.snapshot)
      .at(-1)
    expect(next && 'sessions' in next && next.sessions[0]).toMatchObject({
      title: '事件窗次帧',
    })
    expect(socket.lastAck(LIST_STREAM)).toBe(7n)
    disconnect()
  })

  it('applies an event-first window on a new session subscription (first event at base + 1)', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport((event) => events.push(event))

    // 新详情订阅在途:pending 订阅为 session 目标。
    const runtimePromise = transport.getRuntimeSnapshot('session-1')
    await vi.advanceTimersByTimeAsync(0)

    // 新流 Subscribed(base=5) 后窗口以事件帧开头:首事件 seq=6。
    socket.push(subscribed(SESSION_STREAM, 1n, 5n, socket.sessionSubscribeCorrelation()))
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: SESSION_STREAM,
            events: [turnLifecycleEvent(ActiveTurnPhase.TURN_PHASE_RUNNING)],
          }),
        },
        SESSION_STREAM,
        6n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    await runtimePromise

    // 首事件被应用为领域事件,无缺口误判。
    const lifecycle = events.find((event) => event.type === 'turn-lifecycle')
    expect(lifecycle).toMatchObject({
      type: 'turn-lifecycle',
      sessionId: 'session-1',
      phase: 'RUNNING',
    })
    expect(socket.lastAck(SESSION_STREAM)).toBe(6n)
    expect(socket.payloadCount('resyncRequest')).toBe(0)

    // 后续事件连续应用。
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: SESSION_STREAM,
            events: [turnLifecycleEvent(ActiveTurnPhase.TURN_PHASE_FINISHING)],
          }),
        },
        SESSION_STREAM,
        7n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    const lifecycles = events.filter((event) => event.type === 'turn-lifecycle')
    expect(lifecycles).toHaveLength(2)
    expect(lifecycles[1]).toMatchObject({ phase: 'FINISHING' })
    expect(socket.lastAck(SESSION_STREAM)).toBe(7n)
    disconnect()
  })

  it('keeps accepting the snapshot-first window on a new subscription (snapshot at base, event at base + 1)', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport((event) => events.push(event))

    // 新详情订阅在途;窗口以快照帧开头:快照 seq=base。
    const runtimePromise = transport.getRuntimeSnapshot('session-1')
    await vi.advanceTimersByTimeAsync(0)
    socket.push(subscribed(SESSION_STREAM, 1n, 5n, socket.sessionSubscribeCorrelation()))
    socket.push(
      frame(
        {
          case: 'runtimeSnapshot',
          value: create(RuntimeSnapshotSchema, {}),
        },
        SESSION_STREAM,
        5n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    await runtimePromise

    // 快照形态不回归:快照应用并 ACK(base),后续事件 seq=base+1 连续应用。
    expect(events.some((event) => event.type === 'runtime-snapshot')).toBe(true)
    expect(socket.lastAck(SESSION_STREAM)).toBe(5n)
    expect(socket.payloadCount('resyncRequest')).toBe(0)
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: SESSION_STREAM,
            events: [turnLifecycleEvent(ActiveTurnPhase.TURN_PHASE_RUNNING)],
          }),
        },
        SESSION_STREAM,
        6n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(events.some((event) => event.type === 'turn-lifecycle')).toBe(true)
    expect(socket.lastAck(SESSION_STREAM)).toBe(6n)
    disconnect()
  })
})
