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
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { RealConsoleTransport } from './real-transport'
import type { ConsoleEvent } from './types'
import { mergeSessions, useConsoleStore } from '@/store/console'

const LIST_STREAM = 'st-list'
const DEVICE = 'device-dup-dup-dup'
const NATIVE_ID = 'native-dup'

/**
 * P1-4 回归:双 Agent(同 deviceId + 同 nativeSessionId,agentKind 不同)且
 * relaySessionUuid 为空的原始 WS 摘要,不得在前端折叠成一条会话。
 * 覆盖完整链路:proto 摘要 → mapSessionSummaryProto → store mergeSessions →
 * 详情订阅目标 → 命令目标,以及后续 canonical UUID 归一化不串号。
 */
function collisionSummaryProto(agentKind: number) {
  return create(SessionSummarySchema, {
    sessionKey: create(SessionKeySchema, {
      deviceId: DEVICE,
      agentKind,
      nativeSessionId: NATIVE_ID,
      relaySessionUuid: '',
    }),
    title: `任务-${agentKind}`,
    agentKind,
  })
}

function canonicalSummaryProto(agentKind: number, relayUuid: string) {
  return create(SessionSummarySchema, {
    sessionKey: create(SessionKeySchema, {
      deviceId: DEVICE,
      agentKind,
      nativeSessionId: NATIVE_ID,
      relaySessionUuid: relayUuid,
    }),
    title: `任务-${agentKind}`,
    agentKind,
  })
}

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
): Promise<{ transport: RealConsoleTransport; socket: FakeWebSocket; disconnect: () => void }> {
  vi.stubGlobal('WebSocket', FakeWebSocket)
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      let body: unknown = { ticket: 'ticket-test' }
      if (path.includes('/auth/session')) {
        body = { csrfToken: 'csrf-test' }
      } else if (path.includes('/sessions?')) {
        body = { sessions: [] }
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
  await vi.advanceTimersByTimeAsync(0)
  return { transport, socket, disconnect }
}

function pushSummaryBatch(socket: FakeWebSocket, summaries: ReturnType<typeof collisionSummaryProto>[]) {
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
}

function sessionSubscribeKeys(socket: FakeWebSocket): Array<Record<string, unknown>> {
  return socket
    .sentPayloads()
    .filter((payload) => payload.case === 'subscribe' && payload.value.target?.case === 'session')
    .map((payload) => payload.value.target.value as Record<string, unknown>)
}

beforeEach(() => {
  vi.useFakeTimers()
})

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('dual-agent collision on empty relay session uuid (ZC-02)', () => {
  it('keeps both agents distinct across map, store, subscribe and command', async () => {
    const events: ConsoleEvent[] = []
    const { transport, socket, disconnect } = await startTransport(events)

    // 1) 原始 WS 摘要 → mapSessionSummaryProto(transport 内部映射)。
    //    注:非 UUID 临时 ID 会触发 canonical 刷新,之后可能追加空的
    //    sessions 事件,这里只取携带摘要的非空批次。
    pushSummaryBatch(socket, [collisionSummaryProto(1), collisionSummaryProto(2)])
    await vi.advanceTimersByTimeAsync(0)
    const batch = events
      .filter((event) => event.type === 'sessions' && event.sessions.length > 0)
      .at(-1)
    expect(batch && batch.type === 'sessions').toBe(true)
    const mapped = batch && batch.type === 'sessions' ? batch.sessions : []
    expect(mapped).toHaveLength(2)

    // 2) 映射后的临时 ID 必须按 agentKind 区分,不得同 ID 互相覆盖。
    const ids = mapped.map((session) => session.id)
    expect(new Set(ids).size).toBe(2)
    for (const session of mapped) {
      expect(session.id).toContain(session.agentKind)
      expect(session.id).toContain(DEVICE)
      expect(session.id).toContain(NATIVE_ID)
    }

    // 3) store mergeSessions:两条会话并存。
    const { state } = useConsoleStore()
    mergeSessions(mapped, true)
    expect(state.sessions).toHaveLength(2)
    expect(state.sessions.map((item) => item.agentKind).sort()).toEqual([
      'CODEX_DESKTOP',
      'ZCODE_DESKTOP',
    ])

    // 4) 详情订阅目标:临时 ID 各自带对 agentKind 与 nativeSessionId。
    const zId = mapped.find((session) => session.agentKind === 'ZCODE_DESKTOP')!.id
    const cId = mapped.find((session) => session.agentKind === 'CODEX_DESKTOP')!.id
    transport.retainRuntime(zId)
    await vi.advanceTimersByTimeAsync(0)
    const zKey = sessionSubscribeKeys(socket).at(-1)
    expect(zKey && 'agentKind' in zKey && zKey.agentKind).toBe(2)
    expect(zKey && 'nativeSessionId' in zKey && zKey.nativeSessionId).toBe(NATIVE_ID)
    expect(zKey && 'deviceId' in zKey && zKey.deviceId).toBe(DEVICE)
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

    transport.retainRuntime(cId)
    await vi.advanceTimersByTimeAsync(0)
    const cKey = sessionSubscribeKeys(socket).at(-1)
    expect(cKey && 'agentKind' in cKey && cKey.agentKind).toBe(1)
    expect(cKey && 'nativeSessionId' in cKey && cKey.nativeSessionId).toBe(NATIVE_ID)

    // 5) 命令目标:同 native ID 的两个 Agent 各达各的目标。
    async function commandAgentKind(sessionId: string): Promise<unknown> {
      void transport
        .sendCommand({
          requestId: '20000000-0000-4000-8000-00000000000' + (sessionId === zId ? '1' : '2'),
          operation: 'ANSWER_APPROVAL',
          sessionId,
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
      const sessionKey = (command!.value as Record<string, unknown>).sessionKey as Record<string, unknown>
      return sessionKey.agentKind
    }
    expect(await commandAgentKind(zId)).toBe(2)
    expect(await commandAgentKind(cId)).toBe(1)

    // 6) canonical UUID 归一化:同 (device, agentKind, native) 元组随后带上
    //    各自真实 relaySessionUuid 到达时,各自升级为自己的 UUID,不互串。
    mergeSessions(
      [
        { ...mapped.find((session) => session.agentKind === 'CODEX_DESKTOP')!, id: '11111111-1111-4111-8111-111111111111' },
        { ...mapped.find((session) => session.agentKind === 'ZCODE_DESKTOP')!, id: '22222222-2222-4222-8222-222222222222' },
      ],
      true,
    )
    expect(state.sessions).toHaveLength(2)
    expect(new Set(state.sessions.map((item) => item.id)).size).toBe(2)
    const codexRow = state.sessions.find((item) => item.agentKind === 'CODEX_DESKTOP')!
    const zcodeRow = state.sessions.find((item) => item.agentKind === 'ZCODE_DESKTOP')!
    expect(codexRow.id).toBe('11111111-1111-4111-8111-111111111111')
    expect(zcodeRow.id).toBe('22222222-2222-4222-8222-222222222222')

    disconnect()
  })
})
