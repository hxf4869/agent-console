import type { OutputEvent, OutputState } from './types'

export function utf8Length(value: string): number {
  return new TextEncoder().encode(value).byteLength
}

export function shouldHydrateFinalOutput(
  state: OutputState,
  cursor: { revision: number; byteLength: number; isFinal: boolean; finalUnavailable?: boolean },
): boolean {
  if (!cursor.isFinal || cursor.finalUnavailable) return false
  return !(
    state.isFinal &&
    state.authority === 'AUTHORITATIVE_FINAL' &&
    state.byteLength === cursor.byteLength &&
    state.revision >= cursor.revision
  )
}

export function reduceOutput(state: OutputState, event: OutputEvent): OutputState {
  if (event.itemId !== state.itemId) return state
  if ('revision' in event && event.revision < state.revision) return state
  if (state.isFinal && event.type === 'append') return state
  if (state.isFinal && 'revision' in event && event.revision <= state.revision) return state

  if (event.type === 'append') {
    if (event.expectedOffset !== state.byteLength) {
      return { ...state, hasGap: true }
    }
    const text = `${state.text}${event.text}`
    return {
      ...state,
      text,
      byteLength: utf8Length(text),
      authority: 'LIVE_PREVIEW',
    }
  }

  if (event.type === 'replace') {
    return {
      ...state,
      revision: event.revision,
      text: event.text,
      byteLength: utf8Length(event.text),
      isFinal: false,
      authority: 'LIVE_PREVIEW',
      hasGap: false,
    }
  }

  if (event.type === 'final') {
    if (event.byteLength !== state.byteLength) {
      return {
        ...state,
        revision: event.revision,
        isFinal: false,
        authority: 'LIVE_PREVIEW',
        hasGap: true,
      }
    }
    return {
      ...state,
      revision: event.revision,
      byteLength: event.byteLength,
      isFinal: true,
      authority: 'AUTHORITATIVE_FINAL',
      hasGap: false,
    }
  }

  return {
    ...state,
    revision: event.revision,
    isFinal: false,
    authority: 'FINAL_OUTPUT_UNAVAILABLE',
    hasGap: true,
  }
}
