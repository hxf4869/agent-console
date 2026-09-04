import { describe, expect, it } from 'vitest'

import { formatClock, formatDateTime } from './presentation'

describe('date presentation', () => {
  it('renders an unavailable marker instead of throwing for an absent timestamp', () => {
    expect(formatClock('')).toBe('—')
    expect(formatDateTime('')).toBe('—')
  })
})
