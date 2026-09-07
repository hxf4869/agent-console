import { create } from '@bufbuild/protobuf'
import {
  decodeEnvelope,
  DeviceConnection,
  DevicePresenceChangedSchema,
  DevicePresenceSchema,
  DomainEventSchema,
  encodeEnvelope,
  EnvelopeSchema,
  EventBatchSchema,
  ItemIdSchema,
  OutputAppendSchema,
  OutputChannel,
  PROTOCOL_VERSION,
  RuntimeSnapshotSchema,
  ServerHelloSchema,
  SessionKeySchema,
  SessionSummaryBatchSchema,
  SessionSummarySchema,
  SubscribedSchema,
  type DomainEvent,
  type Envelope,
} from '@agent-console/protocol/source'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { RealConsoleTransport } from './real-transport'
import type { ConsoleEvent } from './types'

const LIST_STREAM = 'st-list'
const SESSION_STREAM = 'st-session'
const DEVICE_A = 'device-aaaa-aaaa'
const DEVICE_B = 'device-bbbb-bbbb'

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

function presenceEvent(deviceId: string, online: boolean): DomainEvent {
  return create(DomainEventSchema, {
    event: {
      case: 'devicePresenceChanged',
      value: create(DevicePresenceChangedSchema, {
        presence: create(DevicePresenceSchema, {
          deviceId,
          connection: online ? DeviceConnection.CONNECTION_ONLINE : DeviceConnection.CONNECTION_OFFLINE,
        }),
      }),
    },
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
    window.setTimeout(() => {
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
    }, 0)
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

  /** 模拟服务端推送一帧二进制 envelope。 */
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
}

async function startTransport(events: ConsoleEvent[]) {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      let body: unknown = { ticket: 'ticket-test' }
      if (path.endsWith('/api/v1/auth/session')) {
        body = { csrfToken: 'csrf-test' }
      } else if (path.includes('/runtime')) {
        body = { runtimeSnapshot: { runtimeRevision: 1, pendingQuestions: [], pendingApprovals: [], backgroundCommands: [], backgroundCommandCount: 0 } }
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
  const connecting = transport.connect((event) => events.push(event))
  await vi.advanceTimersByTimeAsync(0)
  const disconnect = await connecting
  await vi.advanceTimersByTimeAsync(0)
  const socket = FakeWebSocket.instances.at(-1)!
  // 完成 list 订阅:Subscribed(base=1) → 快照帧 seq=1。
  socket.push(
    frame(
      {
        case: 'subscribed',
        value: create(SubscribedSchema, {
          streamId: LIST_STREAM,
          streamEpoch: 1n,
          baseSequence: 1n,
        }),
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

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  FakeWebSocket.instances = []
})

describe('real transport list stream', () => {
  it('T01: applies presence EventBatch on the list stream and ACKs it', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { socket, disconnect } = await startTransport(events)

    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [presenceEvent(DEVICE_A, false)],
          }),
        },
        LIST_STREAM,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)

    const presence = events.find((event) => event.type === 'device-presence')
    expect(presence).toMatchObject({ deviceId: DEVICE_A, connection: 'OFFLINE' })
    const ack = socket
      .sentPayloads()
      .filter((payload) => payload.case === 'ack' && payload.value.streamId === LIST_STREAM)
      .at(-1)
    expect(ack && payloadSequence(ack)).toBe(2n)
    disconnect()
  })

  it('T02: applies summary delta by the event own session key, independent of open detail', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { socket, disconnect } = await startTransport(events)

    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [summaryChangedEvent('session-2', DEVICE_B, '另一台设备的新任务')],
          }),
        },
        LIST_STREAM,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)

    const update = events
      .filter((event) => event.type === 'sessions' && !event.snapshot)
      .at(-1)
    expect(update && 'sessions' in update && update.sessions[0]).toMatchObject({
      id: 'session-2',
      deviceId: DEVICE_B,
      title: '另一台设备的新任务',
    })
    const ack = socket
      .sentPayloads()
      .filter((payload) => payload.case === 'ack' && payload.value.streamId === LIST_STREAM)
      .at(-1)
    expect(ack && payloadSequence(ack)).toBe(2n)
    disconnect()
  })

  it('T03/T04: one list sequence gap triggers one controlled resync; snapshot clears it; watchdog closes on timeout', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { socket, disconnect } = await startTransport(events)

    // 缺口:期望 2,先到 4。
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [presenceEvent(DEVICE_A, true)],
          }),
        },
        LIST_STREAM,
        4n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(socket.payloadCount('resyncRequest')).toBe(1)
    // 后续缺口不重复发送(同一流恢复在途)。
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [presenceEvent(DEVICE_A, false)],
          }),
        },
        LIST_STREAM,
        5n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(socket.payloadCount('resyncRequest')).toBe(1)

    // 快照到达:恢复完成,水位重建,后续事件正常应用并解除恢复标记。
    socket.push(
      frame(
        {
          case: 'subscribed',
          value: create(SubscribedSchema, {
            streamId: LIST_STREAM,
            streamEpoch: 1n,
            baseSequence: 6n,
          }),
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
            summaries: [summaryProto('session-1', DEVICE_A, '恢复后快照')],
          }),
        },
        LIST_STREAM,
        6n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [summaryChangedEvent('session-1', DEVICE_A, '恢复后的增量')],
          }),
        },
        LIST_STREAM,
        7n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    const delta = events
      .filter((event) => event.type === 'sessions' && !event.snapshot)
      .at(-1)
    expect(delta && 'sessions' in delta && delta.sessions[0]).toMatchObject({
      title: '恢复后的增量',
    })
    // 快照后再次缺口可以再次发起恢复(在途标记已解除)。
    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: LIST_STREAM,
            events: [presenceEvent(DEVICE_A, true)],
          }),
        },
        LIST_STREAM,
        9n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    expect(socket.payloadCount('resyncRequest')).toBe(2)

    // 恢复在途超时:看门狗断开重连(私有 close 码),不形成无限 Resync 循环。
    await vi.advanceTimersByTimeAsync(10_500)
    expect(socket.closeCode).toBe(4003)
    disconnect()
  })

  it('keeps the detail session stream applying sequenced events after the rework', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events)

    // 打开详情订阅(复用列表快照写入的 session 键)。
    const runtimePromise = transport.getRuntimeSnapshot('session-1')
    await vi.advanceTimersByTimeAsync(0)
    socket.push(
      frame(
        {
          case: 'subscribed',
          value: create(SubscribedSchema, {
            streamId: SESSION_STREAM,
            streamEpoch: 1n,
            baseSequence: 1n,
          }),
        },
        SESSION_STREAM,
        0n,
        1n,
        socket.sessionSubscribeCorrelation(),
      ),
    )
    socket.push(
      frame(
        { case: 'runtimeSnapshot', value: create(RuntimeSnapshotSchema, {}) },
        SESSION_STREAM,
        1n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)
    await runtimePromise

    socket.push(
      frame(
        {
          case: 'eventBatch',
          value: create(EventBatchSchema, {
            streamId: SESSION_STREAM,
            events: [
              create(DomainEventSchema, {
                event: {
                  case: 'outputAppend',
                  value: create(OutputAppendSchema, {
                    itemId: create(ItemIdSchema, { id: 'item-1', synthetic: false }),
                    expectedOffset: 0n,
                    bytes: new TextEncoder().encode('hello'),
                    channel: OutputChannel.COMBINED,
                  }),
                },
              }),
            ],
          }),
        },
        SESSION_STREAM,
        2n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)

    const output = events.find((event) => event.type === 'output')
    expect(output).toMatchObject({
      sessionId: 'session-1',
      event: { type: 'append', itemId: 'item-1', expectedOffset: 0, text: 'hello' },
    })
    const ack = socket
      .sentPayloads()
      .filter((payload) => payload.case === 'ack' && payload.value.streamId === SESSION_STREAM)
      .at(-1)
    expect(ack && payloadSequence(ack)).toBe(2n)
    disconnect()
  })

  it('rebinds a late list Subscribed after the subscription wait timed out', async () => {
    vi.useFakeTimers()
    const events: ConsoleEvent[] = []
    // 连接后不回 Subscribed:模拟浏览器先打开、Bridge 尚未配对上线,列表
    // 订阅等待超时后关联信息被清空(问题:迟到快照无人认领)。
    vi.stubGlobal('WebSocket', FakeWebSocket)
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = String(input)
        let body: unknown = { ticket: 'ticket-test' }
        if (path.endsWith('/api/v1/auth/session')) body = { csrfToken: 'csrf-test' }
        return new Response(JSON.stringify(body), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      }),
    )
    const transport = new RealConsoleTransport()
    const connecting = transport.connect((event) => events.push(event))
    await vi.advanceTimersByTimeAsync(0)
    const disconnect = await connecting
    await vi.advanceTimersByTimeAsync(0)
    const socket = FakeWebSocket.instances.at(-1)!

    // 订阅等待超时(15s)。
    await vi.advanceTimersByTimeAsync(16_000)
    expect(events.some((event) => event.type === 'sessions')).toBe(false)

    // Bridge 随后配对上线:迟到的 Subscribed + 列表快照必须仍能建立流并应用。
    socket.push(
      frame(
        {
          case: 'subscribed',
          value: create(SubscribedSchema, {
            streamId: LIST_STREAM,
            streamEpoch: 1n,
            baseSequence: 1n,
          }),
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
            summaries: [summaryProto('session-late', DEVICE_A, '迟到任务')],
          }),
        },
        LIST_STREAM,
        1n,
      ),
    )
    await vi.advanceTimersByTimeAsync(0)

    const snapshot = events.find(
      (event) => event.type === 'sessions' && event.snapshot,
    )
    expect(snapshot).toMatchObject({
      type: 'sessions',
      snapshot: true,
      sessions: [{ id: 'session-late', title: '迟到任务' }],
    })
    const ack = socket
      .sentPayloads()
      .filter((payload) => payload.case === 'ack' && payload.value.streamId === LIST_STREAM)
      .at(-1)
    expect(ack && payloadSequence(ack)).toBe(1n)
    disconnect()
  })
})

function payloadSequence(payload: Envelope['payload']): bigint | undefined {
  return payload.case === 'ack' ? payload.value.sequence : undefined
}
