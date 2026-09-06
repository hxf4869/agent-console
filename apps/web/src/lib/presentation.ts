import type {
  ActiveTurnPhase,
  DeviceConnection,
  LastTurnOutcome,
  OutputAuthority,
  QueueStatus,
  ReceiptStatus,
} from '@/transport/types'

export const phaseLabel: Record<ActiveTurnPhase, string> = {
  IDLE: '空闲',
  RUNNING: '运行中',
  FINISHING: '收尾中',
}

export const outcomeLabel: Record<LastTurnOutcome, string> = {
  COMPLETED: '已完成',
  FAILED: '失败',
  INTERRUPTED: '已中断',
  UNKNOWN: '结果未知',
}

export const queueLabel: Record<QueueStatus, string> = {
  EMPTY: '队列为空',
  QUEUED: '已排下一轮',
  PAUSED: '队列暂停',
}

export const connectionLabel: Record<DeviceConnection, string> = {
  CONNECTING: '连接中',
  ONLINE: '在线',
  DEGRADED: '连接降级',
  OFFLINE: '离线',
}

export const authorityLabel: Record<OutputAuthority, string> = {
  LIVE_PREVIEW: '实时预览（可能延迟）',
  AUTHORITATIVE_FINAL: '最终输出',
  FINAL_OUTPUT_UNAVAILABLE: '最终输出不可用',
}

export function formatClock(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

export function formatDateTime(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(date)
}

/** 待处理项等待时长(UX-01):失败输入返回空串,由调用方回退到时钟展示。 */
export function formatWaitingDuration(waitingMs: number): string {
  if (!Number.isFinite(waitingMs) || waitingMs < 0) return ''
  const minutes = Math.floor(waitingMs / 60_000)
  if (minutes < 1) return '刚刚'
  if (minutes < 60) return `等待 ${minutes} 分钟`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `等待 ${hours} 小时`
  return `等待 ${Math.floor(hours / 24)} 天`
}

/** 请求 ID 关联的阶段标签(UX-03):RECEIVED 只是 Relay 收到,不算已发送到 Codex。 */
export function requestStatusLabel(
  status: ReceiptStatus,
  errorCode?: string,
): { text: string; verify: boolean } {
  switch (status) {
    case 'RECEIVED':
      return { text: '已发送（Relay 已接收，等待 Bridge 确认）', verify: false }
    case 'ACCEPTED_BY_BRIDGE':
      return { text: 'Bridge 已接收', verify: false }
    case 'DISPATCHED_TO_CODEX':
    case 'COMPLETED':
      return { text: '原生已处理', verify: false }
    case 'REJECTED':
      return { text: `被拒绝（${errorCode ?? '未知原因'}）`, verify: false }
    case 'OUTCOME_UNKNOWN':
      return { text: '结果未知：连接中断，无法确认是否已执行', verify: true }
    default:
      return { text: '', verify: false }
  }
}

// ---------------------------------------------------------------------------
// Agent 种类与能力来源(04 §8.9:外部 capability 与内部探测分开表述)
// ---------------------------------------------------------------------------

export const agentKindLabel: Record<string, string> = {
  CODEX_DESKTOP: 'Codex Desktop',
  ZCODE_DESKTOP: 'ZCode Desktop',
}

/** Agent 种类显示名;未知枚举值如实显示"未知",不默认当作 Codex。 */
export function agentKindDisplay(agentKind: string): string {
  return agentKindLabel[agentKind] ?? '未知 Agent'
}

/**
 * 能力来源标注:ZCode 走官方 Hook(仅审批/问答决定),Codex 走原生 IPC;
 * 未知种类显示"未知"。控制按钮的可用性仍由 capability 快照驱动,这里只
 * 负责如实标注来源。
 */
export function capabilitySourceLabel(agentKind: string): string {
  switch (agentKind) {
    case 'CODEX_DESKTOP':
      return '能力来源：原生 IPC'
    case 'ZCODE_DESKTOP':
      return '能力来源：官方 Hook'
    default:
      return '能力来源：未知'
  }
}
