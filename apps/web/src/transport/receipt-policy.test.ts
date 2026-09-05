import { describe, expect, it } from 'vitest'

import {
  isTerminalReceipt,
  shouldApplyReceipt,
  shouldRefreshRuntimeAfterReceipt,
} from './receipt-policy'
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

describe('terminal receipt reconciliation', () => {
  it.each<ReceiptStatus>(['COMPLETED', 'REJECTED', 'OUTCOME_UNKNOWN'])(
    'recognizes %s as terminal',
    (status) => expect(isTerminalReceipt(status)).toBe(true),
  )

  it.each<ReceiptStatus>(['COMPLETED', 'OUTCOME_UNKNOWN'])(
    'refreshes authoritative runtime after %s',
    (status) => expect(shouldRefreshRuntimeAfterReceipt(status)).toBe(true),
  )

  it.each<ReceiptStatus>(['RECEIVED', 'ACCEPTED_BY_BRIDGE', 'DISPATCHED_TO_CODEX', 'REJECTED'])(
    'does not refresh runtime after %s',
    (status) => expect(shouldRefreshRuntimeAfterReceipt(status)).toBe(false),
  )
})
