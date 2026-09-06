import { consoleProtocolVersion, consoleWebCommit } from '@/lib/build-info'
import type {
  AnswerState,
  CapabilitySnapshot,
  CommandReceipt,
  CompatibilityState,
  ControlMode,
  DeviceConnection,
  RelayLinkState,
} from '@/transport/types'

const STAGE_LABEL: Record<RelayLinkState['stage'], string> = {
  TICKET: '获取连接凭据',
  HANDSHAKE: '建立实时连接',
  HELLO: '协议握手',
  CONNECTED: '已连接',
  HEARTBEAT: '心跳保活',
  STREAM: '流同步',
  RECONNECT: '等待重连',
}

/** 回复提交后的内联状态反馈(UX-05):不只靠 toast,卡片上保留状态。 */
export function answerStateFeedback(answerState: AnswerState | undefined): {
  text: string
  canVerify: boolean
} {
  if (!answerState) return { text: '', canVerify: false }
  if (answerState.execution === 'FAILED') {
    return { text: '已允许，执行失败：所属轮次最终失败，请查看任务输出。', canVerify: false }
  }
  switch (answerState.submittedStatus) {
    case 'RECEIVED':
      return { text: '回复已提交到中继，尚未被 Bridge 确认。', canVerify: false }
    case 'ACCEPTED_BY_BRIDGE':
    case 'DISPATCHED_TO_CODEX':
      return { text: '已提交回复，等待原生处理结果。', canVerify: false }
    case 'COMPLETED':
      return { text: '回复已处理完成。', canVerify: false }
    case 'REJECTED':
      return {
        text: `回复被拒绝（${answerState.errorCode ?? '未知原因'}）。`,
        canVerify: false,
      }
    case 'OUTCOME_UNKNOWN':
      return { text: '结果未知：连接中断，无法确认是否已处理。', canVerify: true }
    default:
      return { text: '', canVerify: false }
  }
}

export interface ConnectionExplanationInput {
  link: RelayLinkState
  deviceConnection: DeviceConnection
  controlMode: ControlMode
  compatibility: CompatibilityState
  /** 会话能力中是否声明了原生问题能力(从未生成原生问题时为 false)。 */
  supportsNativeQuestion: boolean
  /** 最近一次问题/审批回复的回执;用于区分"曾生成但回复失败"。 */
  lastAnswerReceipt?: CommandReceipt
}

/**
 * 把多维状态合并为一句用户可读说明(UX-02)。
 * 顺序即优先级:先链路终态,再链路中断,再设备,再能力与兼容性。
 */
export function connectionExplanation(input: ConnectionExplanationInput): string {
  const { link, deviceConnection, controlMode, compatibility } = input
  if (link.terminal) {
    return '网页与 Relay 的协议版本不匹配：需要升级 Agent Console 网页或 Relay 后重试；已停止自动重连。'
  }
  if (link.state !== 'ONLINE') {
    if (link.toolboxReachable === false) {
      return '无法访问 Toolbox 网关：请检查网络后重试；当前不会跳转登录。'
    }
    return `实时通道未接通（${STAGE_LABEL[link.stage]}${link.errorCode ? `：${link.errorCode}` : ''}），正在自动重试。`
  }
  if (deviceConnection === 'OFFLINE') {
    return '设备离线：以下为最后已知信息，操作暂不可提交。'
  }
  if (controlMode === 'UNAVAILABLE') {
    return '设备在线，但 Codex 尚未启动或暂不可用。'
  }
  if (controlMode === 'READ_ONLY') {
    return compatibility === 'VERIFIED'
      ? '只读：当前能力探测未开放控制操作。'
      : '只读：此版本尚未验证审批回复，写操作已关闭。'
  }
  if (compatibility === 'UNSUPPORTED') {
    return '此 Codex Desktop 版本未通过验证：仅开放已验证的读取能力。'
  }
  if (compatibility === 'DEGRADED') {
    return '此版本未验证：未知版本按只读处理，已验证的读取正常显示。'
  }
  const answer = input.lastAnswerReceipt
  if (answer && (answer.status === 'REJECTED' || answer.status === 'OUTCOME_UNKNOWN')) {
    return answer.status === 'REJECTED'
      ? `曾生成回复但被拒绝（${answer.errorCode ?? '未知原因'}）；可核对结果后重试。`
      : '曾生成回复但结果未知；请先核对结果，不要盲目重发。'
  }
  if (!input.supportsNativeQuestion) {
    return '此会话从未生成原生问题，因此问题回复不可用。'
  }
  return '设备在线，连接正常。'
}

export interface DiagnosticsInput {
  link: RelayLinkState
  versions: {
    protocol: string
    web: string
    toolbox: string
    relay: string
    bridge: string
    codex: string
  }
  device: {
    displayName: string
    connection: DeviceConnection
    platform: string
    architecture: string
  }
  controlMode: ControlMode
  compatibility: CompatibilityState
  capabilities?: CapabilitySnapshot
  lastReceipt?: CommandReceipt
  now: string
  /** 以下字段存在于调用现场,但诊断报告刻意不包含(白名单外)。 */
  sessionTitle?: string
  sessionNativeId?: string
  attentionText?: string
}

const OPERATION_ORDER = [
  'START_TURN',
  'SET_QUEUE',
  'REPLACE_QUEUE',
  'CANCEL_QUEUE',
  'STEER',
  'INTERRUPT',
  'ANSWER_QUESTION',
  'ANSWER_APPROVAL',
  'UPDATE_SETTINGS',
  'STOP_BACKGROUND_COMMAND',
  'STOP_ALL_BACKGROUND_COMMANDS',
  'OPEN_FILE',
  'READ_GIT_DIFF',
] as const

/**
 * 生成可复制的诊断报告(UX-02):只包含版本、平台、连接阶段、错误码、
 * 时间与各操作验证状态;不含绝对路径、会话标题正文、命令、ticket、凭据。
 */
export function buildDiagnosticsReport(input: DiagnosticsInput): string {
  const lines: string[] = [
    'Agent Console 诊断',
    `时间: ${input.now}`,
    `网页版本: ${input.versions.web || '未知'}`,
    `协议版本: v${input.versions.protocol || '未知'}`,
    `Relay 版本: ${input.versions.relay || '未知'}`,
    `Toolbox 版本: ${input.versions.toolbox || '未知'}`,
    `Bridge 版本: ${input.versions.bridge || '未知'}`,
    `Codex 版本: ${input.versions.codex || '未知'}`,
    `平台: ${[input.device.platform, input.device.architecture].filter(Boolean).join(' ') || '未知'}`,
    `实时链路: ${input.link.state} · 阶段 ${STAGE_LABEL[input.link.stage]}`,
  ]
  if (input.link.errorCode) lines.push(`最近错误码: ${input.link.errorCode}`)
  lines.push(`链路更新时间: ${input.link.updatedAt || '未知'}`)
  lines.push(`设备状态: ${input.device.connection} · 控制 ${input.controlMode} · 兼容性 ${input.compatibility}`)
  if (input.lastReceipt) {
    lines.push(`最近回执: ${input.lastReceipt.status}${input.lastReceipt.errorCode ? ` (${input.lastReceipt.errorCode})` : ''}`)
  }
  const operations = input.capabilities?.operations
  if (operations) {
    lines.push(
      `操作支持: ${OPERATION_ORDER.map((operation) => `${operation}=${operations[operation] ? '是' : '否'}`).join(' ')}`,
    )
  }
  else {
    lines.push('操作支持: 未知(未读取到能力快照)')
  }
  return lines.join('\n')
}
