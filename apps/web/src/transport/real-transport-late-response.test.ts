import { create } from '@bufbuild/protobuf'
import {
  decodeEnvelope,
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
  type Envelope,
} from '@agent-console/protocol/source'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { RealConsoleTransport } from './real-transport'
import type { ConsoleEvent } from './types'

const LIST_STREAM = 'st-list'
const SESSION_STREAM = 'st-session'
const DEVICE_A = 'device-aaaa-aaaa'
const SID = '11111111-1111-4111-8111-111111111111'

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

function subscribed(streamId: string, base: bigint, correlation = ''): Envelope {
  return frame(
    {
      case: 'subscribed',
      value: create(SubscribedSchema, { streamId, streamEpoch: 1n, baseSequence: base }),
    },
    streamId,
    0n,
    1n,
    correlation,
  )
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

function listSnapshot(): Envelope['payload'] {
  return {
    case: 'sessionSummaryBatch',
    value: create(SessionSummaryBatchSchema, {
      snapshot: true,
      summaries: [summaryProto(SID, DEVICE_A, 'Late list task')],
    }),
  }
}

function detailSnapshot(revision: bigint): Envelope['payload'] {
  return {
    case: 'runtimeSnapshot',
    value: create(RuntimeSnapshotSchema, { runtimeRevision: revision }),
  }
}

function outputAppendEvent(text: string): Envelope['payload'] {
  return {
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
              bytes: new TextEncoder().encode(text),
              channel: OutputChannel.COMBINED,
            }),
          },
        }),
      ],
    }),
  }
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
}

/**
 * 建立连接并让列表订阅超时、详情订阅成为当前等待中的订阅:
 * 1. 列表订阅发出,但服务端不回(模拟响应在途延迟)。
 * 2. 列表等待超时(15s)后,详情订阅开始并成为 pendingSubscription。
 * 返回后的 socket 上可按任意顺序推送两条订阅响应。
 */
async function startWithPendingDetail() {
  const events: ConsoleEvent[] = []
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      let body: unknown = { ticket: 'ticket-test' }
      if (path.endsWith('/api/v1/auth/session')) {
        body = { csrfToken: 'csrf-test' }
      } else if (path.includes('/sessions?')) {
        body = {
          sessions: [
            { id: SID, nativeSessionId: `${SID}-native`, deviceId: DEVICE_A, agentKind: 'CODEX_DESKTOP' },
          ],
        }
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
  const connecting = transport.connect((event) => events.push(event))
  await vi.advanceTimersByTimeAsync(0)
  const disconnect = await connecting
  await transport.listSessions()
  await transport.getRuntimeSnapshot(SID)
  // 16s:列表订阅超时(15s)后,详情订阅已发出并等待响应。
  await vi.advanceTimersByTimeAsync(16_000)
  const socket = FakeWebSocket.instances.at(-1)!
  expect(
    socket
      .sentPayloads()
      .filter((p) => p.case === 'subscribe')
      .map((p) => (p.case === 'subscribe' ? p.value.target.case : undefined)),
  ).toEqual(['list', 'session'])
  return { events, socket, disconnect }
}

/** 列表与详情均正确应用的公共断言:快照应用、ACK 归属、后续输出归属。 */
async function expectBothStreamsApplied(events: ConsoleEvent[], socket: FakeWebSocket) {
  const listSnapshot = events.find((event) => event.type === 'sessions' && event.snapshot)
  expect(
    listSnapshot && listSnapshot.type === 'sessions' && listSnapshot.sessions[0],
  ).toMatchObject({ id: SID, title: 'Late list task' })
  const runtime = events.find((event) => event.type === 'runtime-snapshot')
  expect(runtime).toMatchObject({ sessionId: SID, runtime: { runtimeRevision: 9 } })

  // 快照 ACK 属于各自的流。
  const acks = socket.sentPayloads().filter((p) => p.case === 'ack')
  expect(
    acks.some((p) => p.case === 'ack' && p.value.streamId === LIST_STREAM && p.value.sequence === 1n),
  ).toBe(true)
  expect(
    acks.some((p) => p.case === 'ack' && p.value.streamId === SESSION_STREAM && p.value.sequence === 1n),
  ).toBe(true)

  // 后续输出仍属于详情流(正确目标,未被列表流串绑)。
  socket.push(frame(outputAppendEvent('live'), SESSION_STREAM, 2n))
  await vi.advanceTimersByTimeAsync(0)
  const output = events.find((event) => event.type === 'output')
  expect(output).toMatchObject({
    sessionId: SID,
    event: { type: 'append', itemId: 'item-1', expectedOffset: 0, text: 'live' },
  })
  expect(
    socket
      .sentPayloads()
      .some(
        (p) => p.case === 'ack' && p.value.streamId === SESSION_STREAM && p.value.sequence === 2n,
      ),
  ).toBe(true)
}

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  FakeWebSocket.instances = []
})

describe('real transport late subscription responses', () => {
  it('late list response during an active detail subscribe must not swap the streams', async () => {
    vi.useFakeTimers()
    const { events, socket, disconnect } = await startWithPendingDetail()

    // 失败顺序:迟到的列表响应先到,详情响应后到。
    socket.push(subscribed(LIST_STREAM, 1n))
    socket.push(frame(listSnapshot(), LIST_STREAM, 1n))
    await vi.advanceTimersByTimeAsync(0)
    socket.push(subscribed(SESSION_STREAM, 1n, socket.sessionSubscribeCorrelation()))
    socket.push(frame(detailSnapshot(9n), SESSION_STREAM, 1n))
    await vi.advanceTimersByTimeAsync(0)

    await expectBothStreamsApplied(events, socket)
    disconnect()
  })

  it('control: detail response before the late list remains associated', async () => {
    vi.useFakeTimers()
    const { events, socket, disconnect } = await startWithPendingDetail()

    // 通过顺序:详情响应先到,迟到的列表响应后到。
    socket.push(subscribed(SESSION_STREAM, 1n, socket.sessionSubscribeCorrelation()))
    socket.push(frame(detailSnapshot(9n), SESSION_STREAM, 1n))
    await vi.advanceTimersByTimeAsync(0)
    socket.push(subscribed(LIST_STREAM, 1n))
    socket.push(frame(listSnapshot(), LIST_STREAM, 1n))
    await vi.advanceTimersByTimeAsync(0)

    await expectBothStreamsApplied(events, socket)
    disconnect()
  })
})
