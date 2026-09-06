import {
  approvalAttention,
  offlineRuntime,
  olderHistory,
  onlineRuntime,
  sessionsFixture,
} from '@/fixtures/console-fixture'

import { utf8Length } from './output-reducer'
import type {
  CommandReceipt,
  CommandRequest,
  ConsoleEvent,
  ConsoleTransport,
  DeviceSummary,
  FileMetadata,
  GitFileDiff,
  GitSummary,
  Page,
  PairingChallenge,
  RuntimeSnapshot,
  SessionSummary,
  TimelineItem,
  UploadResult,
} from './types'

export class FixtureConsoleTransport implements ConsoleTransport {
  #listener: ((event: ConsoleEvent) => void) | undefined
  #timers: number[] = []

  async connect(onEvent: (event: ConsoleEvent) => void): Promise<() => void> {
    this.#listener = onEvent
    onEvent({ type: 'connection', state: 'CONNECTING' })
    onEvent({ type: 'link', link: { state: 'CONNECTING', stage: 'TICKET', toolboxReachable: true, updatedAt: new Date().toISOString() } })

    this.#timers.push(
      window.setTimeout(() => onEvent({ type: 'connection', state: 'ONLINE' }), 180),
      window.setTimeout(() => {
        onEvent({
          type: 'link',
          link: { state: 'ONLINE', stage: 'CONNECTED', toolboxReachable: true, updatedAt: new Date().toISOString() },
        })
      }, 180),
      window.setTimeout(() => {
        const command = onlineRuntime.timeline.find((item) => item.type === 'command')
        if (!command || command.type !== 'command') return
        onEvent({
          type: 'output',
          sessionId: onlineRuntime.sessionId,
          event: {
            type: 'append',
            itemId: command.output.itemId,
            expectedOffset: command.output.byteLength,
            text: '\n✓ 已加载响应式壳层',
          },
        })
      }, 800),
      window.setTimeout(() => {
        const text = [
          'VITE v8.2.2  ready in 345 ms',
          '➜  Local:   http://localhost:4174/',
          '✓ 已加载响应式壳层',
          '✓ HMR connection established',
        ].join('\n')
        onEvent({
          type: 'output',
          sessionId: onlineRuntime.sessionId,
          event: { type: 'replace', itemId: 'command-vite', revision: 2, text },
        })
      }, 1500),
      window.setTimeout(() => {
        const text = [
          'VITE v8.2.2  ready in 345 ms',
          '➜  Local:   http://localhost:4174/',
          '✓ 已加载响应式壳层',
          '✓ HMR connection established',
        ].join('\n')
        onEvent({
          type: 'output',
          sessionId: onlineRuntime.sessionId,
          event: { type: 'final', itemId: 'command-vite', revision: 2, byteLength: utf8Length(text) },
        })
      }, 1750),
    )

    return () => {
      this.#timers.forEach((timer) => window.clearTimeout(timer))
      this.#timers = []
      this.#listener = undefined
    }
  }

  async listSessions(cursor?: string): Promise<Page<SessionSummary>> {
    await delay(80)
    if (cursor === 'sessions:2') return { items: structuredClone(sessionsFixture.slice(4)) }
    return { items: structuredClone(sessionsFixture.slice(0, 4)), nextCursor: 'sessions:2' }
  }

  async getRuntimeSnapshot(
    sessionId: string,
    _options?: { includeHistory?: boolean },
  ): Promise<RuntimeSnapshot> {
    await delay(90)
    if (sessionId === 'session-offline') return structuredClone(offlineRuntime)
    if (sessionId === 'session-sync') {
      const runtime = structuredClone(onlineRuntime)
      runtime.sessionId = sessionId
      runtime.activeTurnId = approvalAttention.turnId
      runtime.attention = [structuredClone(approvalAttention)]
      runtime.queue = { status: 'EMPTY' }
      runtime.timeline = [
        {
          id: 'timeline-approval',
          type: 'attention',
          createdAt: approvalAttention.createdAt,
          attention: structuredClone(approvalAttention),
        },
        ...runtime.timeline.filter((item) => item.type !== 'attention'),
      ]
      return runtime
    }
    if (sessionId === 'session-api' || sessionId === 'session-failed') {
      const runtime = structuredClone(onlineRuntime)
      runtime.sessionId = sessionId
      delete runtime.activeTurnId
      runtime.phase = 'IDLE'
      runtime.attention = []
      runtime.queue = { status: 'EMPTY' }
      runtime.backgroundCommandCount = 0
      runtime.outputCursors = []
      runtime.timeline = runtime.timeline
        .filter((item) => item.type !== 'attention' && item.type !== 'background-command')
        .map((item) => {
          if (item.type !== 'command') return item
          const failed = sessionId === 'session-failed'
          return {
            ...item,
            status: failed ? ('FAILED' as const) : ('COMPLETED' as const),
            output: {
              ...item.output,
              text: failed ? 'fixture command failed' : 'fixture command completed',
              byteLength: utf8Length(failed ? 'fixture command failed' : 'fixture command completed'),
              isFinal: true,
              authority: 'AUTHORITATIVE_FINAL' as const,
            },
          }
        })
      return runtime
    }
    return { ...structuredClone(onlineRuntime), sessionId }
  }

  async getHistory(sessionId: string, cursor?: string): Promise<Page<TimelineItem>> {
    await delay(100)
    if (sessionId === 'session-offline' || cursor === 'history:end') return { items: [] }
    return { items: structuredClone(olderHistory), nextCursor: 'history:end' }
  }

  async getDevices(): Promise<DeviceSummary[]> {
    const device = sessionsFixture[0]
    return [
      {
        id: device?.deviceId ?? 'device-mac-main',
        displayName: 'MacBook Pro 14',
        connection: 'ONLINE',
        controlMode: 'LIMITED_CONTROL',
        compatibility: 'VERIFIED',
        lastSeenAt: '2026-09-04T11:42:16+08:00',
        bridgeVersion: '0.1.0-dev',
        platform: 'macOS 15.6',
        architecture: 'arm64',
        pairedAt: '2026-09-04T10:00:00+08:00',
        revoked: false,
        privacyHideTitles: false,
      },
    ]
  }

  async lookupPairing(shortCode: string): Promise<PairingChallenge> {
    return {
      challengeId: `fixture-${shortCode}`,
      deviceName: 'Fixture Bridge',
      platform: 'macOS',
      architecture: 'arm64',
      bridgeVersion: '0.1.0-fixture',
      expiresAt: new Date(Date.now() + 5 * 60_000).toISOString(),
    }
  }

  async approvePairing(): Promise<void> {
    await delay(80)
  }

  async revokeDevice(): Promise<void> {
    await delay(80)
  }

  async getGitSummary(): Promise<GitSummary> {
    return {
      branch: 'fixture/branch',
      detachedHead: false,
      headShort: 'f17e123',
      headFull: 'f17e123000000000000000000000000000000000',
      rootDisplayName: 'fixture-workspace',
      entries: [
        { relativePath: 'src/main.ts', status: 'modified', staged: false },
        { relativePath: 'src/new.ts', status: 'untracked', staged: false },
      ],
      insertions: 12,
      deletions: 3,
      binaryFiles: [],
    }
  }

  async getGitDiff(_sessionId: string, path: string, staged = false): Promise<GitFileDiff> {
    return {
      relativePath: path,
      staged,
      patchText: `--- a/${path}\n+++ b/${path}\n@@ -1 +1 @@\n-old\n+fixture`,
      truncated: false,
      totalBytes: 64,
      binary: false,
    }
  }

  async getFileMetadata(_sessionId: string, handle: string): Promise<FileMetadata> {
    return {
      displayName: 'fixture.txt',
      mimeType: 'text/plain',
      sizeBytes: 15,
      previewKind: 'text',
      fileHandle: handle,
    }
  }

  async previewFile(): Promise<Response> {
    return new Response('fixture preview', {
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    })
  }

  async downloadFile(): Promise<Response> {
    return new Response('fixture download', {
      headers: { 'Content-Type': 'application/octet-stream' },
    })
  }

  async uploadFile(_sessionId: string, file: File): Promise<UploadResult> {
    await file.arrayBuffer()
    return {
      transferId: 'fixture-transfer',
      outcome: 'TRANSFER_OUTCOME_COMPLETED',
      uploadFileHandle: 'fixture-upload-handle',
    }
  }

  async getOutputText(
    _sessionId: string,
    _itemId: string,
    _options?: { cursor?: string; signal?: AbortSignal },
  ): Promise<string> {
    return 'fixture authoritative output'
  }

  retainRuntime(_sessionId: string): void {}

  releaseRuntime(_sessionId: string): void {}

  retryLink(): void {}

  async getReceipt(_requestId: string): Promise<CommandReceipt | undefined> {
    return undefined
  }

  requestResync(sessionId: string): void {
    this.#listener?.({ type: 'resync-required', sessionId, reason: 'fixture resync' })
  }

  async sendCommand(request: CommandRequest): Promise<CommandReceipt> {
    await delay(180)
    const receipt: CommandReceipt = {
      requestId: request.requestId,
      status: 'ACCEPTED_BY_BRIDGE',
    }
    this.#listener?.({ type: 'receipt', receipt })
    return receipt
  }
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds))
}
