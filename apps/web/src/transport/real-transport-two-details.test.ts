import { create } from '@bufbuild/protobuf'
import {
  decodeEnvelope,
  encodeEnvelope,
  EnvelopeSchema,
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
const DEVICE_A = 'device-aaaa-aaaa'
const SID_A = '11111111-1111-4111-8111-111111111111'
const SID_B = '22222222-2222-4222-8222-222222222222'

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

  sentEnvelopes(): Envelope[] {
    return this.sent.map((data) => decodeEnvelope(new Uint8Array(data)))
  }

  /** 第 n 个会话订阅的 correlation_id(index 0 = 任务 A,1 = 任务 B)。 */
  sessionSubscribeCorrelation(index: number): string {
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
 * 建立连接;列表订阅超时后任务 A 的详情订阅发出;列表随后完成,再切换到
 * 任务 B(A 释放,B 的订阅在 A 超时后发出)。返回时 A、B 两条详情响应都
 * 未到达,可按任意顺序推送。
 */
async function startWithSwitchedDetail() {
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
            { id: SID_A, nativeSessionId: `${SID_A}-native`, deviceId: DEVICE_A, agentKind: 'CODEX_DESKTOP' },
            { id: SID_B, nativeSessionId: `${SID_B}-native`, deviceId: DEVICE_A, agentKind: 'CODEX_DESKTOP' },
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
  await transport.getRuntimeSnapshot(SID_A)
  // 16s:列表超时,A 的详情订阅发出。
  await vi.advanceTimersByTimeAsync(16_000)
  const socket = FakeWebSocket.instances.at(-1)!
  // 列表随后完成。
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
          summaries: [summaryProto(SID_A, DEVICE_A, 'Task A')],
        }),
      },
      LIST_STREAM,
      1n,
    ),
  )
  await vi.advanceTimersByTimeAsync(0)
  // 用户切换到任务 B:A 释放,B 的订阅排队;A 超时后 B 的订阅发出。
  transport.releaseRuntime(SID_A)
  await transport.getRuntimeSnapshot(SID_B)
  await vi.advanceTimersByTimeAsync(16_000)
  const sessionSubscribes = socket
    .sentEnvelopes()
    .filter(
      (env) => env.payload.case === 'subscribe' && env.payload.value.target?.case === 'session',
    )
  expect(sessionSubscribes).toHaveLength(2)
  return { events, socket, disconnect }
}

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  FakeWebSocket.instances = []
})

describe('real transport detail target correlation (round3)', () => {
  it.each([true, false])(
    'task A must never be attributed to task B; late A arrives first=%s',
    async (aFirst) => {
      vi.useFakeTimers()
      const { events, socket, disconnect } = await startWithSwitchedDetail()

      // 服务端回显各订阅自己的 correlation_id:迟到 A 的响应仍能正确归属
      // (A 已释放 → 忽略),绝不写进当前等待中的任务 B。
      const pushA = async () => {
        socket.push(
          frame(
            {
              case: 'subscribed',
              value: create(SubscribedSchema, {
                streamId: 'st-task-a',
                streamEpoch: 1n,
                baseSequence: 1n,
              }),
            },
            'st-task-a',
            0n,
            1n,
            socket.sessionSubscribeCorrelation(0),
          ),
        )
        socket.push(
          frame(
            { case: 'runtimeSnapshot', value: create(RuntimeSnapshotSchema, { runtimeRevision: 111n }) },
            'st-task-a',
            1n,
          ),
        )
        await vi.advanceTimersByTimeAsync(0)
      }
      const pushB = async () => {
        socket.push(
          frame(
            {
              case: 'subscribed',
              value: create(SubscribedSchema, {
                streamId: 'st-task-b',
                streamEpoch: 1n,
                baseSequence: 1n,
              }),
            },
            'st-task-b',
            0n,
            1n,
            socket.sessionSubscribeCorrelation(1),
          ),
        )
        socket.push(
          frame(
            { case: 'runtimeSnapshot', value: create(RuntimeSnapshotSchema, { runtimeRevision: 222n }) },
            'st-task-b',
            1n,
          ),
        )
        await vi.advanceTimersByTimeAsync(0)
      }
      if (aFirst) {
        await pushA()
        await pushB()
      } else {
        await pushB()
        await pushA()
      }
      disconnect()
      const got = events
        .filter((event) => event.type === 'runtime-snapshot')
        .map((event) => ({
          sessionId: event.sessionId,
          revision: event.runtime.runtimeRevision,
        }))
      expect(got).toEqual([{ sessionId: SID_B, revision: 222 }])
    },
  )
})
