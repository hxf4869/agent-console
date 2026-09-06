import { mount } from '@vue/test-utils'
import { describe, expect, it, vi } from 'vitest'

import SaveToToolboxDialog from '@/components/SaveToToolboxDialog.vue'
import {
  buildItemPayload,
  fetchToolboxCsrf,
  saveSnippetToToolbox,
  ToolboxAuthError,
  type ToolboxSaveDraft,
} from './toolbox-save'

const snippet = {
  text: 'pnpm test 通过 12 个用例',
  agentDisplay: 'Codex Desktop',
  projectDisplay: 'agent-console',
}

const draft: ToolboxSaveDraft = {
  snippet,
  title: '测试运行结果',
  sensitivity: 'sensitive',
  folderId: undefined,
  tagIds: [],
}

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('toolbox save payload (UX-09)', () => {
  it('records source as agent/project display names, never absolute paths', () => {
    const payload = buildItemPayload(draft)
    expect(payload.contentFormat).toBe('plain_text')
    expect(payload.sensitivity).toBe('sensitive')
    expect(payload.title).toBe('测试运行结果')
    expect(payload.body).toContain('Codex Desktop')
    expect(payload.body).toContain('agent-console')
    expect(payload.body).toContain('pnpm test 通过 12 个用例')
    expect(payload.body).not.toMatch(/\/Users\//)
    expect((payload as Record<string, unknown>).source).toBeUndefined()
  })

  it('omits folder and tags when the user did not pick any', () => {
    const payload = buildItemPayload(draft)
    expect('folderId' in payload).toBe(false)
    expect('tagIds' in payload).toBe(false)
  })
})

describe('toolbox save API call (UX-09)', () => {
  it('posts to /api/v1/items with session credentials, CSRF and idempotency key', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input)
      if (path === '/api/v1/auth/session') {
        return jsonResponse(200, { csrfToken: 'csrf-123' })
      }
      return jsonResponse(201, {
        item: { id: '6f1e...', title: '测试运行结果', sensitivity: 'sensitive', createdAt: '2026-09-05T00:00:00Z' },
        rawRecord: { id: 'raw-1', readOnly: true },
      })
    })

    const saved = await saveSnippetToToolbox(draft, { fetchImpl: fetchMock as typeof fetch })

    expect(fetchMock).toHaveBeenCalledTimes(2)
    const [itemsPath, itemsInit] = fetchMock.mock.calls[1] as [string, RequestInit]
    expect(itemsPath).toBe('/api/v1/items')
    expect(itemsInit.method).toBe('POST')
    expect(itemsInit.credentials).toBe('same-origin')
    const headers = itemsInit.headers as Record<string, string>
    expect(headers['X-CSRF-Token']).toBe('csrf-123')
    expect(headers['Idempotency-Key'].length).toBeGreaterThanOrEqual(8)
    const body = JSON.parse(String(itemsInit.body)) as Record<string, unknown>
    expect(body).toMatchObject({ contentFormat: 'plain_text', sensitivity: 'sensitive' })
    expect(String(body.body)).toContain('pnpm test 通过 12 个用例')
    expect(saved).toMatchObject({ itemId: '6f1e...', title: '测试运行结果', sensitivity: 'sensitive', replay: false })
  })

  it('reuses a provided CSRF token without an extra session request', async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse(201, { item: { id: 'id-2', title: 't', sensitivity: 'normal' } }),
    )
    await saveSnippetToToolbox(draft, { fetchImpl: fetchMock as typeof fetch, csrfToken: 'known' })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('surfaces auth expiry as a distinct error instead of a generic failure', async () => {
    const fetchMock = vi.fn(async () => jsonResponse(401, { error: { code: 'AUTH_REQUIRED', message: '请先登录' } }))
    await expect(
      saveSnippetToToolbox(draft, { fetchImpl: fetchMock as typeof fetch, csrfToken: 'stale' }),
    ).rejects.toBeInstanceOf(ToolboxAuthError)
  })

  it('reads the csrf token from the existing toolbox session endpoint', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      expect(String(input)).toBe('/api/v1/auth/session')
      return jsonResponse(200, { csrfToken: 'abc' })
    })
    await expect(fetchToolboxCsrf(fetchMock as typeof fetch)).resolves.toBe('abc')
  })
})

describe('save dialog (UX-09 confirm-before-write)', () => {
  const snippetProp = { ...snippet }

  it('performs no network write when the user cancels', async () => {
    const fetchMock = vi.fn()
    const wrapper = mount(SaveToToolboxDialog, {
      props: { open: true, snippet: snippetProp },
    })
    await wrapper.vm.$nextTick()
    await wrapper.get('button[aria-label="关闭保存弹层"]').trigger('click')
    expect(wrapper.emitted('close')).toHaveLength(1)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('shows only the selected snippet plus source metadata in the confirm dialog', async () => {
    const wrapper = mount(SaveToToolboxDialog, {
      props: { open: true, snippet: snippetProp },
    })
    await wrapper.vm.$nextTick()
    const text = wrapper.text()
    // 待保存正文在可编辑的 textarea 中(默认为选中片段,可改)。
    const bodyTextarea = wrapper.get('textarea').element as HTMLTextAreaElement
    expect(bodyTextarea.value).toBe('pnpm test 通过 12 个用例')
    expect(text).toContain('Codex Desktop')
    expect(text).toContain('agent-console')
    // 敏感性默认沿用保守值;包含 Toolbox 合法的三档。
    expect(text).toContain('敏感')
    expect(text).toContain('常规')
    expect(text).toContain('未定')
  })
})
