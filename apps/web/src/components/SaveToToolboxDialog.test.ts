import { flushPromises, mount } from '@vue/test-utils'
import { afterEach, describe, expect, it, vi } from 'vitest'

import SaveToToolboxDialog from '@/components/SaveToToolboxDialog.vue'

const snippet = {
  text: '第一步：配置 API_KEY=sk-secret-123\n第二步：pnpm test 通过 12 个用例',
  agentDisplay: 'Codex Desktop',
  projectDisplay: 'agent-console',
}

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

interface ItemCall {
  path: string
  init: RequestInit
}

/**
 * 拦截组件会用到的全部 Toolbox 端点;记录发往 /api/v1/items 的真实请求。
 * itemsHandler 可注入网络错误/HTTP 失败等响应行为。
 */
function stubToolboxFetch(
  itemsHandler: () => Promise<Response> | Response = () =>
    jsonResponse(201, { item: { id: 'id-1', title: 't', sensitivity: 'sensitive' } }),
): { fetchMock: ReturnType<typeof vi.fn>; itemsCalls: ItemCall[] } {
  const itemsCalls: ItemCall[] = []
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    if (path === '/api/v1/auth/session') return jsonResponse(200, { csrfToken: 'csrf-test' })
    if (path === '/api/v1/folders/tree' || path === '/api/v1/tags') {
      return jsonResponse(200, { items: [] })
    }
    if (path === '/api/v1/items') {
      itemsCalls.push({ path, init: init ?? {} })
      return itemsHandler()
    }
    throw new Error(`unexpected fetch: ${path}`)
  })
  vi.stubGlobal('fetch', fetchMock)
  return { fetchMock, itemsCalls }
}

function itemsBody(call: ItemCall): Record<string, unknown> {
  return JSON.parse(String(call.init.body)) as Record<string, unknown>
}

async function mountOpenDialog(): Promise<ReturnType<typeof mount>> {
  const wrapper = mount(SaveToToolboxDialog, {
    props: { open: true, snippet: { ...snippet } },
  })
  await flushPromises()
  return wrapper
}

function saveButton(wrapper: ReturnType<typeof mount>) {
  return wrapper.get('footer .ui-button--primary')
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('save dialog submits the edited body (UX-09)', () => {
  it('sends the edited textarea content, never the original snippet', async () => {
    const { itemsCalls } = stubToolboxFetch()
    const wrapper = await mountOpenDialog()

    // 用户删掉含密钥的那一行,只保留可公开内容。
    const edited = '第二步：pnpm test 通过 12 个用例'
    await wrapper.get('textarea').setValue(edited)
    await saveButton(wrapper).trigger('click')
    await flushPromises()

    expect(itemsCalls).toHaveLength(1)
    const body = itemsBody(itemsCalls[0]!)
    const sentText = String(body.body)
    expect(sentText).not.toContain('sk-secret-123')
    expect(sentText).toContain('pnpm test 通过 12 个用例')
    // 来源标识(显示名)仍按契约保留在正文首部。
    expect(sentText).toContain('Codex Desktop')
    expect(wrapper.emitted('saved')).toHaveLength(1)
  })
})

describe('save retry keeps one idempotency key per logical save (P2-9)', () => {
  it('retries the same key and payload after a lost response; edited content mints a new key', async () => {
    let itemsFailure: Error | undefined = new TypeError('网络错误：服务端已提交但响应丢失')
    const { itemsCalls } = stubToolboxFetch(() => {
      if (itemsFailure) return Promise.reject(itemsFailure)
      return jsonResponse(201, { item: { id: 'id-9', title: 't', sensitivity: 'sensitive' } })
    })
    const wrapper = await mountOpenDialog()

    const keyOf = (call: ItemCall): string =>
      (call.init.headers as Record<string, string>)['Idempotency-Key']

    // 第一次保存:响应丢失,但请求必须已携带幂等 key X。
    await saveButton(wrapper).trigger('click')
    await flushPromises()
    expect(itemsCalls).toHaveLength(1)
    const keyX = keyOf(itemsCalls[0]!)
    const payloadX = String(itemsCalls[0]!.init.body)
    expect(keyX.length).toBeGreaterThanOrEqual(8)

    // 立即重试相同内容:复用 key X 与同一 payload,不得换新键造成重复写入。
    await saveButton(wrapper).trigger('click')
    await flushPromises()
    expect(itemsCalls).toHaveLength(2)
    expect(keyOf(itemsCalls[1]!)).toBe(keyX)
    expect(String(itemsCalls[1]!.init.body)).toBe(payloadX)

    // 用户编辑内容后再保存:payload 变化视为新保存,必须使用新 key。
    itemsFailure = undefined
    await wrapper.get('textarea').setValue('编辑后的全新内容')
    await saveButton(wrapper).trigger('click')
    await flushPromises()
    expect(itemsCalls).toHaveLength(3)
    expect(keyOf(itemsCalls[2]!)).not.toBe(keyX)
    expect(String(itemsCalls[2]!.init.body)).toContain('编辑后的全新内容')
    expect(wrapper.emitted('saved')).toHaveLength(1)
  })
})
