import type { ReceiptStatus } from './types'

const locallyApplicableStatuses = new Set<ReceiptStatus>([
  'ACCEPTED_BY_BRIDGE',
  'DISPATCHED_TO_CODEX',
  'COMPLETED',
])

const terminalStatuses = new Set<ReceiptStatus>([
  'COMPLETED',
  'REJECTED',
  'OUTCOME_UNKNOWN',
])

const runtimeRefreshStatuses = new Set<ReceiptStatus>(['COMPLETED', 'OUTCOME_UNKNOWN'])

export function shouldApplyReceipt(status: ReceiptStatus): boolean {
  return locallyApplicableStatuses.has(status)
}

export function isTerminalReceipt(status: ReceiptStatus): boolean {
  return terminalStatuses.has(status)
}

export function shouldRefreshRuntimeAfterReceipt(status: ReceiptStatus): boolean {
  return runtimeRefreshStatuses.has(status)
}
