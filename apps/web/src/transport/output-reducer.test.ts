import { describe, expect, it } from 'vitest'

import { reduceOutput, shouldHydrateFinalOutput, utf8Length } from './output-reducer'
import type { OutputState } from './types'

function output(overrides: Partial<OutputState> = {}): OutputState {
  return {
    itemId: 'output-1',
    revision: 1,
    text: 'A',
    byteLength: 1,
    isFinal: false,
    authority: 'LIVE_PREVIEW',
    hasGap: false,
    ...overrides,
  }
}

describe('reduceOutput', () => {
  it('appends only at the expected UTF-8 byte offset', () => {
    const next = reduceOutput(output(), {
      type: 'append',
      itemId: 'output-1',
      expectedOffset: 1,
      text: '中',
    })

    expect(next.text).toBe('A中')
    expect(next.byteLength).toBe(4)
    expect(next.hasGap).toBe(false)
  })

  it('marks a gap instead of applying an out-of-order append', () => {
    const next = reduceOutput(output(), {
      type: 'append',
      itemId: 'output-1',
      expectedOffset: 7,
      text: 'late',
    })

    expect(next.text).toBe('A')
    expect(next.hasGap).toBe(true)
  })

  it('uses replace as the recovery point for a live preview', () => {
    const text = 'recovered\npreview'
    const next = reduceOutput(output({ hasGap: true }), {
      type: 'replace',
      itemId: 'output-1',
      revision: 3,
      text,
    })

    expect(next).toMatchObject({
      revision: 3,
      text,
      byteLength: utf8Length(text),
      authority: 'LIVE_PREVIEW',
      hasGap: false,
    })
  })

  it('marks a byte-complete output as authoritative final', () => {
    const next = reduceOutput(output(), {
      type: 'final',
      itemId: 'output-1',
      revision: 2,
      byteLength: 1,
    })

    expect(next).toMatchObject({
      revision: 2,
      isFinal: true,
      authority: 'AUTHORITATIVE_FINAL',
      hasGap: false,
    })
  })

  it('does not claim final authority when bytes are missing', () => {
    const next = reduceOutput(output(), {
      type: 'final',
      itemId: 'output-1',
      revision: 2,
      byteLength: 8,
    })

    expect(next.isFinal).toBe(false)
    expect(next.authority).toBe('LIVE_PREVIEW')
    expect(next.hasGap).toBe(true)
  })

  it('records when the final output cannot be recovered', () => {
    const next = reduceOutput(output(), {
      type: 'unavailable',
      itemId: 'output-1',
      revision: 4,
    })

    expect(next).toMatchObject({
      revision: 4,
      isFinal: false,
      authority: 'FINAL_OUTPUT_UNAVAILABLE',
      hasGap: true,
    })
  })

  it('ignores a stale replace after a newer authoritative final', () => {
    const state = output({
      revision: 2,
      text: 'authoritative',
      byteLength: utf8Length('authoritative'),
      isFinal: true,
      authority: 'AUTHORITATIVE_FINAL',
    })
    const next = reduceOutput(state, {
      type: 'replace',
      itemId: 'output-1',
      revision: 1,
      text: 'late preview',
    })

    expect(next).toBe(state)
  })

  it('ignores stale final metadata behind the current revision', () => {
    const state = output({ revision: 3, text: 'new preview', byteLength: utf8Length('new preview') })
    const next = reduceOutput(state, {
      type: 'final',
      itemId: 'output-1',
      revision: 2,
      byteLength: state.byteLength,
    })

    expect(next).toBe(state)
  })

  it('keeps an authoritative final stable when a late append arrives', () => {
    const state = output({
      revision: 2,
      isFinal: true,
      authority: 'AUTHORITATIVE_FINAL',
    })
    const next = reduceOutput(state, {
      type: 'append',
      itemId: 'output-1',
      expectedOffset: 1,
      text: 'late',
    })

    expect(next).toBe(state)
  })
})

describe('shouldHydrateFinalOutput', () => {
  it('hydrates an equal-length live preview before claiming final authority', () => {
    expect(
      shouldHydrateFinalOutput(output(), { revision: 2, byteLength: 1, isFinal: true }),
    ).toBe(true)
  })

  it('skips an already authoritative final at the same or newer revision', () => {
    expect(
      shouldHydrateFinalOutput(
        output({ revision: 3, isFinal: true, authority: 'AUTHORITATIVE_FINAL' }),
        { revision: 2, byteLength: 1, isFinal: true },
      ),
    ).toBe(false)
  })

  it('does not hydrate a non-final cursor', () => {
    expect(
      shouldHydrateFinalOutput(output(), { revision: 2, byteLength: 1, isFinal: false }),
    ).toBe(false)
  })

  it('does not query a final output the Bridge marked unavailable', () => {
    expect(
      shouldHydrateFinalOutput(output(), {
        revision: 2,
        byteLength: 0,
        isFinal: true,
        finalUnavailable: true,
      }),
    ).toBe(false)
  })
})
