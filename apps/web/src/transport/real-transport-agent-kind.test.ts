import { create } from '@bufbuild/protobuf'
import {
  decodeEnvelope,
  encodeEnvelope,
  EnvelopeSchema,
  PROTOCOL_VERSION,
  ServerHelloSchema,
  SessionKeySchema,
  RuntimeSnapshotSchema,
  SessionSummaryBatchSchema,
  SessionSummarySchema,
  SubscribedSchema,
  type Envelope,
} from '@agent-console/protocol/source'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { agentKindName, agentKindValueFromName, RealConsoleTransport } from './real-transport'
import type { ConsoleEvent } from './types'

const LIST_STREAM = 'st-list'
const DEVICE_A = 'device-aaaa-aaaa'

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

function summaryProto(relayId: string, agentKind: number) {
  return create(SessionSummarySchema, {
    sessionKey: create(SessionKeySchema, {
      deviceId: DEVICE_A,
      agentKind,
      nativeSessionId: `${relayId}-native`,
      relaySessionUuid: relayId,
    }),
    title: `任务-${agentKind}`,
    agentKind,
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
      this.dispatchEvent(new MessageEvent('message', { data: toFrame(helloEnvelope()) }))
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
}

async function startTransport(
  events: ConsoleEvent[],
  summaries: ReturnType<typeof summaryProto>[],
): Promise<{ transport: RealConsoleTransport; socket: FakeWebSocket; disconnect: () => void }> {
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
  const connecting = transport.connect((event) => events.push(event))
  await vi.advanceTimersByTimeAsync(0)
  const disconnect = await connecting
  await vi.advanceTimersByTimeAsync(0)
  const socket = FakeWebSocket.instances.at(-1)!
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
        value: create(SessionSummaryBatchSchema, { snapshot: true, summaries }),
      },
      LIST_STREAM,
      1n,
    ),
  )
  await vi.advanceTimersByTimeAsync(0)
  return { transport, socket, disconnect }
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

function sessionSubscribeKeys(socket: FakeWebSocket): Array<Record<string, unknown>> {
  return socket
    .sentPayloads()
    .filter((payload) => payload.case === 'subscribe' && payload.value.target?.case === 'session')
    .map((payload) => payload.value.target.value as Record<string, unknown>)
}

describe('agent kind routing on the realtime link (ZC-02)', () => {
  it('carries ZCODE_DESKTOP from the summary cache into the detail subscribe', async () => {
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events, [
      summaryProto('session-z', 2),
      summaryProto('session-c', 1),
    ])

    transport.retainRuntime('session-z')
    await vi.advanceTimersByTimeAsync(0)
    const keys = sessionSubscribeKeys(socket)
    const zcode = keys.at(-1)
    expect(zcode).toBeDefined()
    expect(zcode && 'agentKind' in zcode && zcode.agentKind).toBe(2)
    expect(zcode && 'nativeSessionId' in zcode && zcode.nativeSessionId).toBe('session-z-native')
    // 完成订阅握手(Subscribed 锚定 + 快照到达才回执完成),解锁订阅链。
    socket.push(
      frame(
        {
          case: 'subscribed',
          value: create(SubscribedSchema, { streamId: 'st-z', streamEpoch: 1n, baseSequence: 1n }),
        },
        'st-z',
        0n,
      ),
    )
    socket.push(
      frame({ case: 'runtimeSnapshot', value: create(RuntimeSnapshotSchema, {}) }, 'st-z', 1n),
    )
    await vi.advanceTimersByTimeAsync(0)

    transport.retainRuntime('session-c')
    await vi.advanceTimersByTimeAsync(0)
    const codex = sessionSubscribeKeys(socket).at(-1)
    expect(codex && 'agentKind' in codex && codex.agentKind).toBe(1)
    disconnect()
  })

  it('does not rewrite unknown agent kinds into Codex on subscribe', async () => {
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events, [
      summaryProto('session-u', 9),
    ])

    transport.retainRuntime('session-u')
    await vi.advanceTimersByTimeAsync(0)
    const unknown = sessionSubscribeKeys(socket).at(-1)
    // 未知数值原样透传,由 Bridge/Relay 显式拒绝;绝不默认当作 CODEX_DESKTOP。
    expect(unknown && 'agentKind' in unknown && unknown.agentKind).toBe(9)
    expect(unknown && 'agentKind' in unknown && unknown.agentKind).not.toBe(1)
    disconnect()
  })

  it('carries the cached agent kind into command session keys', async () => {
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events, [
      summaryProto('session-z', 2),
    ])

    void transport
      .sendCommand({
        requestId: '10000000-0000-4000-8000-000000000001',
        operation: 'ANSWER_APPROVAL',
        sessionId: 'session-z',
        expectedRuntimeRevision: 0,
        payload: { attentionId: 'att-1', optionId: 'allow' },
      })
      .catch(() => undefined)
    await vi.advanceTimersByTimeAsync(50)
    await vi.advanceTimersByTimeAsync(50)
    const command = socket
      .sentPayloads()
      .filter((payload) => payload.case === 'commandRequest')
      .at(-1)
    expect(command).toBeDefined()
    const sessionKey = (command!.value as Record<string, unknown>).sessionKey as Record<string, unknown>
    expect(sessionKey.agentKind).toBe(2)
    expect(sessionKey.nativeSessionId).toBe('session-z-native')
    disconnect()
  })

  it('maps agent kind names and values without defaulting unknowns to Codex', () => {
    expect(agentKindName(1)).toBe('CODEX_DESKTOP')
    expect(agentKindName(2)).toBe('ZCODE_DESKTOP')
    expect(agentKindName(9)).toBe('AGENT_KIND_9')
    expect(agentKindValueFromName('ZCODE_DESKTOP')).toBe(2)
    expect(agentKindValueFromName('CODEX_DESKTOP')).toBe(1)
    expect(agentKindValueFromName('AGENT_KIND_9')).toBe(9)
    expect(agentKindValueFromName('AGENT_KIND_UNSPECIFIED')).toBe(0)
    // 未知名称不得默认折叠为 Codex。
    expect(agentKindValueFromName('AGENT_KIND_9')).not.toBe(1)
  })
})
