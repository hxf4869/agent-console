import type {
  ActiveTurnPhase,
  DeviceConnection,
  LastTurnOutcome,
  OutputAuthority,
  QueueStatus,
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
