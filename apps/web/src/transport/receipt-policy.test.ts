import { describe, expect, it } from 'vitest'

import { shouldApplyReceipt } from './receipt-policy'
import type { ReceiptStatus } from './types'

describe('shouldApplyReceipt', () => {
  it.each<ReceiptStatus>(['ACCEPTED_BY_BRIDGE', 'DISPATCHED_TO_CODEX', 'COMPLETED'])(
    'allows local state changes after %s',
    (status) => expect(shouldApplyReceipt(status)).toBe(true),
  )

  it.each<ReceiptStatus>(['RECEIVED', 'REJECTED', 'OUTCOME_UNKNOWN'])(
    'keeps local state unchanged after %s',
    (status) => expect(shouldApplyReceipt(status)).toBe(false),
  )
})
