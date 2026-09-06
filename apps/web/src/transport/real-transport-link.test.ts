import { create } from '@bufbuild/protobuf'
import {
  encodeEnvelope,
  EnvelopeSchema,
  PROTOCOL_VERSION,
  ServerHelloSchema,
} from '@agent-console/protocol/source'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { RealConsoleTransport } from './real-transport'
import type { ConsoleEvent } from './types'

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
  window.localStorage.clear()
})

function helloFrame(acceptedProtocolVersion: number): ArrayBuffer {
  const bytes = encodeEnvelope(
    create(EnvelopeSchema, {
      protocolVersion: PROTOCOL_VERSION,
      messageId: '00000000-0000-4000-8000-000000000001',
      payload: {
        case: 'serverHello',
        value: create(ServerHelloSchema, { acceptedProtocolVersion }),
      },
    }),
  )
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
}

function stubFetch(bodies: Record<string, unknown>): void {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input)
      for (const [suffix, body] of Object.entries(bodies)) {
        if (path.endsWith(suffix)) {
          return new Response(JSON.stringify(body), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          })
        }
      }
      return new Response(JSON.stringify({ error: { code: 'NOT_FOUND', message: 'x' } }), { status: 404 })
    }),
  )
}

interface FakeSocketOptions {
  onOpen?: (socket: FakeLinkWebSocket) => void
  /** false 时保持 CONNECTING 且不派发 open:模拟握手挂起(等待 serverHello)。 */
  autoOpen?: boolean
}

class FakeLinkWebSocket extends EventTarget {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static instances: FakeLinkWebSocket[] = []
  static options: FakeSocketOptions = {}

  readyState = FakeLinkWebSocket.CONNECTING
  binaryType: BinaryType = 'blob'

  constructor(_url: string | URL, _protocols?: string | string[]) {
    super()
    FakeLinkWebSocket.instances.push(this)
    if (FakeLinkWebSocket.options.autoOpen === false) return
    queueMicrotask(() => {
      this.readyState = FakeLinkWebSocket.OPEN
      this.dispatchEvent(new Event('open'))
      FakeLinkWebSocket.options.onOpen?.(this)
    })
  }

  send(): void {}

  close(code = 1000, reason = ''): void {
    if (this.readyState === FakeLinkWebSocket.CLOSED) return
    this.readyState = FakeLinkWebSocket.CLOSED
    this.dispatchEvent(new CloseEvent('close', { code, reason }))
  }
}

async function connectTransport(): Promise<{
  transport: RealConsoleTransport
  events: ConsoleEvent[]
  disconnect: () => void
  connectError?: Error
}> {
  const transport = new RealConsoleTransport()
  const events: ConsoleEvent[] = []
  let connectError: Error | undefined
  const connecting = transport.connect((event) => events.push(event)).catch((error: Error) => {
    connectError = error
    return () => undefined
  })
  await vi.advanceTimersByTimeAsync(0)
  const disconnect = await connecting
  return { transport, events, disconnect, connectError }
}

describe('relay link observability (IN-01)', () => {
  it('reports ticket/handshake/connected stages and toolbox reachability', async () => {
    vi.useFakeTimers()
    FakeLinkWebSocket.instances = []
    FakeLinkWebSocket.options = {
      onOpen: (socket) => {
        socket.dispatchEvent(
          new MessageEvent('message', { data: helloFrame(PROTOCOL_VERSION) }),
        )
      },
    }
    vi.stubGlobal('WebSocket', FakeLinkWebSocket)
    stubFetch({
      '/api/v1/auth/session': { csrfToken: 'csrf' },
      '/api/v1/agent-console/ws-tickets': { ticket: 'ticket-1' },
    })

    const { events, disconnect, connectError } = await connectTransport()

    const links = events.filter((event) => event.type === 'link')
    expect(links.map((event) => event.link.stage)).toEqual(['TICKET', 'TICKET', 'HANDSHAKE', 'CONNECTED'])
    expect(links.every((event) => event.link.toolboxReachable !== false)).toBe(true)
    const online = links.at(-1)
    expect(online && online.type === 'link' && online.link.state).toBe('ONLINE')
    expect(connectError).toBeUndefined()
    disconnect()
  })

  it('stops reconnecting on protocol mismatch and marks the link terminal', async () => {
    vi.useFakeTimers()
    FakeLinkWebSocket.instances = []
    FakeLinkWebSocket.options = {
      onOpen: (socket) => {
        // 服务端声明了不同的协议版本:连接必须以终态关闭,不进入无限重连。
        socket.dispatchEvent(new MessageEvent('message', { data: helloFrame(PROTOCOL_VERSION + 1) }))
      },
    }
    vi.stubGlobal('WebSocket', FakeLinkWebSocket)
    stubFetch({
      '/api/v1/auth/session': { csrfToken: 'csrf' },
      '/api/v1/agent-console/ws-tickets': { ticket: 'ticket-1' },
    })

    const { transport, events, disconnect } = await connectTransport()
    await vi.advanceTimersByTimeAsync(30_000)

    // 协议不匹配时首连 Promise 以握手失败结束,这是预期行为。
    expect(FakeLinkWebSocket.instances).toHaveLength(1)
    const terminal = events.find(
      (event) => event.type === 'link' && event.link.terminal,
    )
    expect(terminal).toBeDefined()
    if (terminal?.type === 'link') {
      expect(terminal.link.errorCode).toBe('PROTOCOL_VERSION_MISMATCH')
      expect(terminal.link.stage).toBe('HELLO')
    }

    // 升级后的手动重试应恢复连接尝试。
    FakeLinkWebSocket.options = {
      onOpen: (socket) => {
        socket.dispatchEvent(new MessageEvent('message', { data: helloFrame(PROTOCOL_VERSION) }))
      },
    }
    transport.retryLink()
    await vi.advanceTimersByTimeAsync(0)
    expect(FakeLinkWebSocket.instances).toHaveLength(2)
    disconnect()
  })

  it('does not open a second socket while a handshake is still in flight', async () => {
    vi.useFakeTimers()
    FakeLinkWebSocket.instances = []
    // 握手挂起:socket 停在 CONNECTING,不派发 open、不回 serverHello。
    FakeLinkWebSocket.options = { autoOpen: false }
    vi.stubGlobal('WebSocket', FakeLinkWebSocket)
    stubFetch({
      '/api/v1/auth/session': { csrfToken: 'csrf' },
      '/api/v1/agent-console/ws-tickets': { ticket: 'ticket-1' },
    })

    const transport = new RealConsoleTransport()
    const connecting = transport.connect(() => {})
    // 立即挂 rejection handler:首连会在握手超时(10s)时失败,不能晚于 timer 推进。
    const connectingFailure = connecting.catch((error: Error) => error)
    await vi.advanceTimersByTimeAsync(0)
    expect(FakeLinkWebSocket.instances).toHaveLength(1)

    // 握手在途(最长 10s 窗口)时手动重试:不得并发第二个 openSocket 流程。
    transport.retryLink()
    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(0)
    expect(FakeLinkWebSocket.instances).toHaveLength(1)

    // 互斥不是死锁:握手超时后仍走自动重连自愈路径(首连以握手失败结束)。
    await vi.advanceTimersByTimeAsync(11_000)
    expect(FakeLinkWebSocket.instances.length).toBeGreaterThan(1)
    expect(await connectingFailure).toBeInstanceOf(Error)
  })

  it('reports HTTP_503 at ticket stage while keeping the toolbox gateway reachable', async () => {    vi.useFakeTimers()
    vi.stubGlobal('WebSocket', FakeLinkWebSocket)
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = String(input)
        if (path.endsWith('/api/v1/auth/session')) {
          return new Response(JSON.stringify({ csrfToken: 'csrf' }), { status: 200 })
        }
        return new Response(
          JSON.stringify({ error: { code: 'RELAY_UNAVAILABLE', message: '上游不可用' } }),
          { status: 503 },
        )
      }),
    )

    const events: ConsoleEvent[] = []
    const transport = new RealConsoleTransport()
    const connecting = transport.connect((event) => events.push(event))
    await expect(connecting).rejects.toBeInstanceOf(Error)
    await vi.advanceTimersByTimeAsync(0)
    void transport

    const link = events.find((event) => event.type === 'link' && event.link.stage === 'TICKET' && event.link.state === 'OFFLINE')
    expect(link).toBeDefined()
    if (link?.type === 'link') {
      expect(link.link.errorCode).toBe('RELAY_UNAVAILABLE')
      expect(link.link.toolboxReachable).toBe(true)
      expect(link.link.terminal).toBeUndefined()
    }
  })
})

describe('receipt lookup (UX-03 verify)', () => {
  it('maps a persisted receipt and tolerates missing records', async () => {
    vi.useFakeTimers()
    FakeLinkWebSocket.instances = []
    FakeLinkWebSocket.options = {
      onOpen: (socket) => {
        socket.dispatchEvent(new MessageEvent('message', { data: helloFrame(PROTOCOL_VERSION) }))
      },
    }
    vi.stubGlobal('WebSocket', FakeLinkWebSocket)
    stubFetch({
      '/api/v1/auth/session': { csrfToken: 'csrf' },
      '/api/v1/agent-console/ws-tickets': { ticket: 'ticket-1' },
      '/requests/req-known': { status: 'COMPLETED' },
    })

    const { transport, disconnect } = await connectTransport()

    expect(await transport.getReceipt('req-known')).toEqual({
      requestId: 'req-known',
      status: 'COMPLETED',
    })
    expect(await transport.getReceipt('req-missing')).toBeUndefined()
    disconnect()
  })
})
