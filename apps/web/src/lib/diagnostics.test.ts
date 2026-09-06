import { describe, expect, it } from 'vitest'

import { consoleProtocolVersion, consoleWebCommit } from './build-info'
import { buildDiagnosticsReport, connectionExplanation } from './diagnostics'
import type { CapabilitySnapshot, CommandReceipt, RelayLinkState } from '@/transport/types'

function link(overrides: Partial<RelayLinkState> = {}): RelayLinkState {
  return {
    state: 'ONLINE',
    stage: 'CONNECTED',
    updatedAt: '2026-09-05T08:00:00Z',
    ...overrides,
  }
}

const fullOperations = Object.fromEntries(
  ['START_TURN', 'ANSWER_QUESTION', 'ANSWER_APPROVAL', 'STEER', 'INTERRUPT'].map((operation) => [
    operation,
    true,
  ]),
) as CapabilitySnapshot['operations']

const capabilities: CapabilitySnapshot = {
  revision: 3,
  operations: fullOperations,
  codexVersion: 'codex-cli 0.153.1',
  models: [],
  thinkingDepths: [],
  serviceTiers: [],
  permissionModes: [],
  collaborationModes: [],
}

describe('connection explanation', () => {
  it('explains a terminal protocol mismatch and that auto reconnect stopped', () => {
    const text = connectionExplanation({
      link: link({ state: 'OFFLINE', stage: 'HELLO', terminal: true, errorCode: 'PROTOCOL_VERSION_MISMATCH' }),
      deviceConnection: 'ONLINE',
      controlMode: 'FULL_CONTROL',
      compatibility: 'VERIFIED',
      supportsNativeQuestion: true,
    })
    expect(text).toContain('协议版本不匹配')
    expect(text).toContain('升级')
    expect(text).toContain('停止自动重连')
  })

  it('distinguishes relay unreachable while toolbox gateway is reachable', () => {
    const text = connectionExplanation({
      link: link({ state: 'OFFLINE', stage: 'TICKET', errorCode: 'HTTP_503', toolboxReachable: true }),
      deviceConnection: 'OFFLINE',
      controlMode: 'READ_ONLY',
      compatibility: 'DEGRADED',
      supportsNativeQuestion: true,
    })
    expect(text).toContain('实时通道未接通')
    expect(text).toContain('HTTP_503')
    expect(text).not.toContain('登录')
  })

  it('does not fall back to the login flow for a gateway network failure', () => {
    const text = connectionExplanation({
      link: link({ state: 'OFFLINE', stage: 'TICKET', toolboxReachable: false }),
      deviceConnection: 'OFFLINE',
      controlMode: 'READ_ONLY',
      compatibility: 'DEGRADED',
      supportsNativeQuestion: true,
    })
    expect(text).toContain('无法访问 Toolbox 网关')
    expect(text).not.toContain('重新登录')
  })

  it('explains offline devices as last-known read-only', () => {
    const text = connectionExplanation({
      link: link(),
      deviceConnection: 'OFFLINE',
      controlMode: 'READ_ONLY',
      compatibility: 'DEGRADED',
      supportsNativeQuestion: true,
    })
    expect(text).toContain('设备离线')
    expect(text).toContain('不可提交')
  })

  it('explains online device with Codex not started', () => {
    const text = connectionExplanation({
      link: link(),
      deviceConnection: 'ONLINE',
      controlMode: 'UNAVAILABLE',
      compatibility: 'DEGRADED',
      supportsNativeQuestion: false,
    })
    expect(text).toContain('Codex 尚未启动')
  })

  it('explains read-only unverified versions without banning the whole site', () => {
    const text = connectionExplanation({
      link: link(),
      deviceConnection: 'ONLINE',
      controlMode: 'READ_ONLY',
      compatibility: 'DEGRADED',
      supportsNativeQuestion: false,
    })
    expect(text).toContain('只读')
    expect(text).toContain('尚未验证')
  })

  it('shows different text for never-generated native questions vs a failed answer', () => {
    const never = connectionExplanation({
      link: link(),
      deviceConnection: 'ONLINE',
      controlMode: 'LIMITED_CONTROL',
      compatibility: 'VERIFIED',
      supportsNativeQuestion: false,
    })
    expect(never).toContain('从未生成原生问题')

    const failed: CommandReceipt = { requestId: 'req-1', status: 'REJECTED', errorCode: 'QUESTION_EXPIRED' }
    const answered = connectionExplanation({
      link: link(),
      deviceConnection: 'ONLINE',
      controlMode: 'LIMITED_CONTROL',
      compatibility: 'VERIFIED',
      supportsNativeQuestion: true,
      lastAnswerReceipt: failed,
    })
    expect(answered).toContain('被拒绝')
    expect(answered).toContain('QUESTION_EXPIRED')
    expect(answered).not.toBe(never)
  })
})

describe('diagnostics report', () => {
  it('lists version composition with unknown placeholders instead of fabricated data', () => {
    const report = buildDiagnosticsReport({
      link: link({ state: 'OFFLINE', stage: 'TICKET', errorCode: 'HTTP_503' }),
      versions: {
        protocol: consoleProtocolVersion,
        web: consoleWebCommit,
        toolbox: '未知',
        relay: '未知',
        bridge: '0.1.0-dev',
        codex: 'codex-cli 0.153.1',
      },
      device: { displayName: 'MacBook Pro 14', connection: 'OFFLINE', platform: 'macOS 15.6', architecture: 'arm64' },
      controlMode: 'READ_ONLY',
      compatibility: 'DEGRADED',
      capabilities,
      now: '2026-09-05T08:00:00Z',
    })
    expect(report).toContain('协议版本')
    expect(report).toContain('Bridge 版本: 0.1.0-dev')
    expect(report).toContain('Relay 版本: 未知')
    expect(report).toContain('Toolbox 版本: 未知')
    expect(report).toContain('最近错误码: HTTP_503')
    expect(report).toContain('ANSWER_QUESTION=是')
  })

  it('keeps secrets, titles, paths and request ids out of the copied report', () => {
    const report = buildDiagnosticsReport({
      link: link(),
      versions: { protocol: '1', web: 'abc1234', toolbox: '未知', relay: '未知', bridge: '0.1.0', codex: '0.153.1' },
      device: { displayName: ' leaking-token-9f8e7d ', connection: 'ONLINE', platform: 'macOS', architecture: 'arm64' },
      controlMode: 'FULL_CONTROL',
      compatibility: 'VERIFIED',
      capabilities,
      lastReceipt: { requestId: 'req-SECRET-ticket-9f8e7d', status: 'COMPLETED' },
      now: '2026-09-05T08:00:00Z',
      // 白名单外现场字段:注入的密钥样本不得进入报告。
      sessionTitle: '删除数据库 super-secret-9f8e7d',
      sessionNativeId: '/Users/leaky/.codex/sessions/secret-9f8e7d',
      attentionText: '批准 token=super-secret-9f8e7d',
    })
    expect(report).not.toContain('super-secret-9f8e7d')
    expect(report).not.toContain('/Users/leaky')
    expect(report).not.toContain('删除数据库')
    expect(report).toContain('最近回执: COMPLETED')
  })
})
