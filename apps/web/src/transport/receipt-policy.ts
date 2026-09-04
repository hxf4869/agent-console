import type { ReceiptStatus } from './types'

const locallyApplicableStatuses = new Set<ReceiptStatus>([
  'ACCEPTED_BY_BRIDGE',
  'DISPATCHED_TO_CODEX',
  'COMPLETED',
])

export function shouldApplyReceipt(status: ReceiptStatus): boolean {
  return locallyApplicableStatuses.has(status)
}
