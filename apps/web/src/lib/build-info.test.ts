import { describe, expect, it, vi } from 'vitest'

import {
  consoleProtocolVersion,
  createRelayVersionLoader,
  createToolboxVersionLoader,
  RELAY_VERSION_PATH,
  TOOLBOX_VERSION_PATH,
} from './build-info'

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('relay version loader (IN-01)', () => {
  it('consumes relayVersion and protocolVersion from the version endpoint', async () => {
    const fetchMock = vi.fn<FetchLike>(async (input, init) => {
      expect(input).toBe(RELAY_VERSION_PATH)
      expect(init?.credentials).toBe('same-origin')
      expect(init?.cache).toBe('no-store')
      return jsonResponse(200, { relayVersion: '0.4.0', protocolVersion: 1 })
    })

    const loader = createRelayVersionLoader(fetchMock)
    await expect(loader()).resolves.toEqual({ relayVersion: '0.4.0', protocolVersion: '1' })
  })

  it('degrades to undefined on non-2xx without throwing', async () => {
    const fetchMock = vi.fn<FetchLike>(async () => jsonResponse(404, { error: { code: 'NOT_FOUND' } }))
    const loader = createRelayVersionLoader(fetchMock)
    await expect(loader()).resolves.toBeUndefined()
  })

  it('degrades to undefined on network failure and malformed payloads', async () => {
    const failing = createRelayVersionLoader(
      vi.fn<FetchLike>(async () => {
        throw new TypeError('network down')
      }),
    )
    await expect(failing()).resolves.toBeUndefined()

    const garbage = createRelayVersionLoader(
      vi.fn<FetchLike>(async () => jsonResponse(200, { hello: 'world' })),
    )
    await expect(garbage()).resolves.toBeUndefined()
  })

  it('caches the first result and does not refetch on subsequent calls', async () => {
    const fetchMock = vi.fn<FetchLike>(async () =>
      jsonResponse(200, { relayVersion: '0.4.0', protocolVersion: consoleProtocolVersion }),
    )
    const loader = createRelayVersionLoader(fetchMock)
    const first = await loader()
    const second = await loader()
    expect(first).toEqual(second)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('a fresh loader retries independently (new page load semantics)', async () => {
    const failing = createRelayVersionLoader(vi.fn<FetchLike>(async () => jsonResponse(503, {})))
    await expect(failing()).resolves.toBeUndefined()

    const next = createRelayVersionLoader(
      vi.fn<FetchLike>(async () => jsonResponse(200, { relayVersion: '0.5.0', protocolVersion: 1 })),
    )
    await expect(next()).resolves.toEqual({ relayVersion: '0.5.0', protocolVersion: '1' })
  })
})

describe('toolbox version loader (IN-01 follow-up)', () => {
  it('consumes appVersion and commit from the toolbox version endpoint', async () => {
    const fetchMock = vi.fn<FetchLike>(async (input, init) => {
      expect(input).toBe(TOOLBOX_VERSION_PATH)
      expect(init?.credentials).toBe('same-origin')
      expect(init?.cache).toBe('no-store')
      return jsonResponse(200, { apiVersion: 'v1', appVersion: 'v3.2.1', commit: 'a1b2c3d4e5f6789' })
    })

    const loader = createToolboxVersionLoader(fetchMock)
    await expect(loader()).resolves.toEqual({ appVersion: 'v3.2.1', commit: 'a1b2c3d4e5f6789' })
  })

  it('treats unknown/development placeholders as absent (falls back to 未知)', async () => {
    const development = createToolboxVersionLoader(
      vi.fn<FetchLike>(async () => jsonResponse(200, { apiVersion: 'v1', appVersion: 'development', commit: 'unknown' })),
    )
    await expect(development()).resolves.toBeUndefined()

    const unknownCommit = createToolboxVersionLoader(
      vi.fn<FetchLike>(async () => jsonResponse(200, { apiVersion: 'v1', appVersion: 'v3.2.1', commit: 'unknown' })),
    )
    await expect(unknownCommit()).resolves.toEqual({ appVersion: 'v3.2.1', commit: '' })
  })

  it('degrades to undefined on non-2xx and network failure, and caches per loader', async () => {
    const notFound = createToolboxVersionLoader(vi.fn<FetchLike>(async () => jsonResponse(404, {})))
    await expect(notFound()).resolves.toBeUndefined()

    const failing = createToolboxVersionLoader(
      vi.fn<FetchLike>(async () => {
        throw new TypeError('network down')
      }),
    )
    await expect(failing()).resolves.toBeUndefined()

    const fetchMock = vi.fn<FetchLike>(async () =>
      jsonResponse(200, { apiVersion: 'v1', appVersion: 'v3.2.1', commit: 'a1b2c3d' }),
    )
    const loader = createToolboxVersionLoader(fetchMock)
    await loader()
    await loader()
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })
})
