import { create } from '@bufbuild/protobuf'
import {
  encodeEnvelope,
  EnvelopeSchema,
  PROTOCOL_VERSION,
  ServerHelloSchema,
} from '@agent-console/protocol/source'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  loginUrl,
  mapFileMetadataJson,
  mapRuntimeSnapshotJson,
  mapSessionSummaryJson,
  RealConsoleTransport,
  ticketSubprotocol,
} from './real-transport'

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('real transport contract mapping', () => {
  it('double-encodes the issued ticket for the WebSocket subprotocol', () => {
    expect(ticketSubprotocol('ticket-value')).toBe('agent-console.ticket-dGlja2V0LXZhbHVl')
  })

  it('keeps only an Agent Console same-origin return path', () => {
    expect(
      loginUrl({
        origin: 'https://toolbox.test',
        pathname: '/agent-console/tasks/abc',
        search: '?tab=files',
        hash: '#output',
      }),
    ).toBe('/?returnTo=%2Fagent-console%2Ftasks%2Fabc%3Ftab%3Dfiles%23output')
    expect(
      loginUrl({
        origin: 'https://toolbox.test',
        pathname: '/other',
        search: '',
        hash: '',
      }),
    ).toBe('/?returnTo=%2Fagent-console%2F')
  })

  it('normalizes prefixed HTTP enums without widening product capability', () => {
    const session = mapSessionSummaryJson({
      id: 'session-1',
      deviceId: 'device-1',
      nativeSessionId: 'native-1',
      title: '真实任务',
      projectDisplayName: 'workspace',
      currentBranch: 'main',
      deviceConnection: 'CONNECTION_ONLINE',
      controlMode: 'CONTROL_MODE_LIMITED_CONTROL',
      compatibilityState: 'COMPATIBILITY_VERIFIED',
      activeTurnPhase: 'TURN_PHASE_RUNNING',
      pendingAttentionCount: 0,
      queueState: 'QUEUE_STATE_EMPTY',
      lastTurnOutcome: 'TURN_OUTCOME_COMPLETED',
      lastUpdatedAt: '2026-09-05T00:00:00Z',
    })
    expect(session).toMatchObject({
      deviceConnection: 'ONLINE',
      controlMode: 'LIMITED_CONTROL',
      compatibility: 'VERIFIED',
      phase: 'RUNNING',
      queueStatus: 'EMPTY',
    })

    const runtime = mapRuntimeSnapshotJson('session-1', {
      runtimeRevision: 8,
      currentTurn: { turn: { id: 'turn-1' }, phase: 'TURN_PHASE_RUNNING' },
      pendingQuestions: [],
      pendingApprovals: [],
      backgroundCommands: [],
      backgroundCommandCount: 0,
      recentOutputCursors: [{
        itemId: { id: 'command-1' },
        revision: 8,
        byteLength: 0,
        isFinal: true,
        finalUnavailable: true,
      }],
      queue: { state: 'QUEUE_STATE_EMPTY' },
      capabilities: {
        controlMode: 'CONTROL_MODE_LIMITED_CONTROL',
        compatibilityState: 'COMPATIBILITY_VERIFIED',
        codexVersion: 'codex-cli 0.153.1',
        supportedOperations: [
          'OPERATION_START_TURN',
          'OPERATION_QUEUE_SET',
          'OPERATION_QUEUE_REPLACE',
          'OPERATION_QUEUE_CANCEL',
          'OPERATION_STEER',
          'OPERATION_INTERRUPT',
        ],
        settings: [],
      },
    })
    expect(runtime.capabilities.operations).toMatchObject({
      START_TURN: true,
      SET_QUEUE: true,
      REPLACE_QUEUE: true,
      CANCEL_QUEUE: true,
      STEER: true,
      INTERRUPT: true,
      ANSWER_QUESTION: false,
      ANSWER_APPROVAL: false,
      UPDATE_SETTINGS: false,
      STOP_BACKGROUND_COMMAND: false,
      STOP_ALL_BACKGROUND_COMMANDS: false,
    })
    expect(runtime.outputCursors[0]?.finalUnavailable).toBe(true)
  })

  it('keeps the refreshed file handle from metadata', () => {
    expect(
      mapFileMetadataJson({
        displayName: 'upload.txt',
        mimeType: 'text/plain',
        sizeBytes: 12,
        previewKind: 'text',
        notPreviewableReason: null,
        fileHandle: 'refreshed-handle',
      }),
    ).toEqual({
      displayName: 'upload.txt',
      mimeType: 'text/plain',
      sizeBytes: 12,
      previewKind: 'text',
      fileHandle: 'refreshed-handle',
    })
  })

  it('keeps only one reconnect timer when a retry socket also closes', async () => {
    vi.useFakeTimers()
    const helloBytes = encodeEnvelope(
      create(EnvelopeSchema, {
        protocolVersion: PROTOCOL_VERSION,
        messageId: '00000000-0000-4000-8000-000000000001',
        payload: {
          case: 'serverHello',
          value: create(ServerHelloSchema, { acceptedProtocolVersion: PROTOCOL_VERSION }),
        },
      }),
    )
    const helloFrame = helloBytes.buffer.slice(
      helloBytes.byteOffset,
      helloBytes.byteOffset + helloBytes.byteLength,
    ) as ArrayBuffer

    class FakeWebSocket extends EventTarget {
      static readonly CONNECTING = 0
      static readonly OPEN = 1
      static readonly CLOSING = 2
      static readonly CLOSED = 3
      static readonly instances: FakeWebSocket[] = []

      readyState = FakeWebSocket.CONNECTING
      binaryType: BinaryType = 'blob'

      constructor(_url: string | URL, _protocols?: string | string[]) {
        super()
        const index = FakeWebSocket.instances.push(this) - 1
        window.setTimeout(() => {
          if (index === 0) {
            this.readyState = FakeWebSocket.OPEN
            this.dispatchEvent(new Event('open'))
            this.dispatchEvent(new MessageEvent('message', { data: helloFrame }))
            return
          }
          this.readyState = FakeWebSocket.CLOSED
          this.dispatchEvent(new CloseEvent('close', { code: 1006, reason: 'NETWORK' }))
        }, 0)
      }

      send(): void {}

      close(code = 1000, reason = ''): void {
        if (this.readyState === FakeWebSocket.CLOSED) return
        this.readyState = FakeWebSocket.CLOSED
        this.dispatchEvent(new CloseEvent('close', { code, reason }))
      }
    }

    vi.stubGlobal('WebSocket', FakeWebSocket)
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const path = String(input)
        const body = path.endsWith('/api/v1/auth/session')
          ? { csrfToken: 'csrf-test' }
          : { ticket: 'ticket-test' }
        return new Response(JSON.stringify(body), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      }),
    )

    const transport = new RealConsoleTransport()
    const connecting = transport.connect(() => undefined)
    await vi.advanceTimersByTimeAsync(0)
    const disconnect = await connecting

    FakeWebSocket.instances[0]!.close(1006, 'NETWORK')
    await vi.advanceTimersByTimeAsync(500)

    expect(FakeWebSocket.instances).toHaveLength(2)
    expect(vi.getTimerCount()).toBe(1)
    disconnect()
  })
})
