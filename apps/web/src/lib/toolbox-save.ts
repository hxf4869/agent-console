const AUTH_SESSION_PATH = '/api/v1/auth/session'
const ITEMS_PATH = '/api/v1/items'

export type ToolboxSensitivity = 'normal' | 'sensitive' | 'unknown'

export interface ToolboxSnippet {
  /** 用户选中的正文;未选中的消息绝不进入草稿。 */
  text: string
  /** 用户可见的 Agent 显示名(如 Codex Desktop),不含内部凭据。 */
  agentDisplay: string
  /** 项目显示名(用户可见),不取绝对路径。 */
  projectDisplay: string
}

export interface ToolboxSaveDraft {
  snippet: ToolboxSnippet
  title: string
  sensitivity: ToolboxSensitivity
  folderId?: string | undefined
  tagIds: string[]
}

/**
 * 构建 POST /api/v1/items 的请求体(dev-toolbox 知识条目创建契约)。
 * 会话 Cookie 身份不允许传 source 字段(服务端 strict 解码会拒绝),
 * 来源以正文首部的必要标识记录:仅 Agent 与项目显示名,不含路径(UX-09)。
 */
export function buildItemPayload(draft: ToolboxSaveDraft): {
  body: string
  contentFormat: 'plain_text'
  sensitivity: ToolboxSensitivity
  title: string
  folderId?: string
  tagIds?: string[]
} {
  const sourceHeader = `【来自 Agent Console · ${draft.snippet.agentDisplay}】项目：${draft.snippet.projectDisplay}`
  const body = `${sourceHeader}\n\n${draft.snippet.text}`
  return {
    body,
    contentFormat: 'plain_text',
    sensitivity: draft.sensitivity,
    title: draft.title,
    ...(draft.folderId ? { folderId: draft.folderId } : {}),
    ...(draft.tagIds.length ? { tagIds: draft.tagIds } : {}),
  }
}

export interface SavedItem {
  itemId: string
  title: string
  sensitivity: string
  /** 200 表示幂等重放命中,201 表示新条目。 */
  replay: boolean
}

export class ToolboxAuthError extends Error {
  constructor(message = 'Toolbox 登录状态已失效，请重新登录后保存。') {
    super(message)
    this.name = 'ToolboxAuthError'
  }
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

function defaultFetch(input: string, init?: RequestInit): Promise<Response> {
  return fetch(input, init)
}

/** 读取 Toolbox 会话 CSRF(dev-toolbox 现有 GET /api/v1/auth/session 契约)。 */
export async function fetchToolboxCsrf(
  fetchImpl: FetchLike = defaultFetch,
): Promise<string> {
  const response = await fetchImpl(AUTH_SESSION_PATH, {
    credentials: 'same-origin',
    cache: 'no-store',
  })
  if (response.status === 401) throw new ToolboxAuthError()
  if (!response.ok) throw new Error('无法读取 Toolbox 会话，请稍后重试。')
  const body = (await response.json()) as { csrfToken?: string }
  return body.csrfToken ?? ''
}

/** 幂等键:同一逻辑保存的重试必须复用,防止服务端重复写入(UX-09)。 */
export function newIdempotencyKey(): string {
  return crypto.randomUUID().repeat(2).slice(0, 36)
}

/**
 * 调用 Toolbox 真实保存 API:POST /api/v1/items。
 * 认证:同域会话 Cookie + X-CSRF-Token;带 Idempotency-Key 防重复写入。
 * 成功返回 201(或幂等重放 200)与新建条目摘要。
 * 重试换键会造成重复条目:调用方必须为同一逻辑保存复用同一 idempotencyKey。
 */
export async function saveSnippetToToolbox(
  draft: ToolboxSaveDraft,
  options: {
    csrfToken?: string
    idempotencyKey?: string
    fetchImpl?: FetchLike
  } = {},
): Promise<SavedItem> {
  const fetchImpl = options.fetchImpl ?? defaultFetch
  const csrfToken = options.csrfToken ?? (await fetchToolboxCsrf(fetchImpl))
  const idempotencyKey = options.idempotencyKey ?? newIdempotencyKey()
  const response = await fetchImpl(ITEMS_PATH, {
    method: 'POST',
    credentials: 'same-origin',
    cache: 'no-store',
    headers: {
      'Content-Type': 'application/json',
      'X-CSRF-Token': csrfToken,
      'Idempotency-Key': idempotencyKey,
    },
    body: JSON.stringify(buildItemPayload(draft)),
  })
  if (response.status === 401) throw new ToolboxAuthError()
  if (!response.ok) {
    let message = `保存失败（HTTP ${response.status}）。`
    try {
      const body = (await response.json()) as { error?: { message?: string } }
      if (body.error?.message) message = `保存失败：${body.error.message}`
    } catch {
      // 保留默认错误说明。
    }
    throw new Error(message)
  }
  const body = (await response.json()) as {
    item?: { id?: string; title?: string; sensitivity?: string }
  }
  if (!body.item?.id) throw new Error('保存返回缺少条目 ID。')
  return {
    itemId: body.item.id,
    title: body.item.title ?? draft.title,
    sensitivity: body.item.sensitivity ?? draft.sensitivity,
    replay: response.status === 200,
  }
}

export interface ToolboxFolder {
  id: string
  name: string
  children: ToolboxFolder[]
}

export interface ToolboxTag {
  id: string
  name: string
}

/** 目录树与标签列表用于确认弹层的选择项;读取失败不阻塞保存。 */
export async function fetchToolboxOrganization(
  fetchImpl: FetchLike = defaultFetch,
): Promise<{ folders: ToolboxFolder[]; tags: ToolboxTag[] }> {
  const [foldersResult, tagsResult] = await Promise.all([
    fetchImpl('/api/v1/folders/tree', { credentials: 'same-origin', cache: 'no-store' })
      .then((response) => (response.ok ? response.json() : { items: [] }))
      .catch(() => ({ items: [] })),
    fetchImpl('/api/v1/tags', { credentials: 'same-origin', cache: 'no-store' })
      .then((response) => (response.ok ? response.json() : { items: [] }))
      .catch(() => ({ items: [] })),
  ])
  const folders = (foldersResult as { items?: unknown }).items
  const tags = (tagsResult as { items?: unknown }).items
  return {
    folders: Array.isArray(folders) ? folders.map(mapFolder) : [],
    tags: Array.isArray(tags)
      ? tags
          .map((entry) => {
            const item = entry as { id?: unknown; name?: unknown }
            return { id: String(item.id ?? ''), name: String(item.name ?? '') }
          })
          .filter((tag) => tag.id && tag.name)
      : [],
  }
}

function mapFolder(entry: unknown): ToolboxFolder {
  const item = entry as {
    id?: unknown
    name?: unknown
    children?: unknown
  }
  return {
    id: String(item.id ?? ''),
    name: String(item.name ?? ''),
    children: Array.isArray(item.children) ? item.children.map(mapFolder) : [],
  }
}

/** 保存结果的最小回执(本地展示用),不构成跨产品内容总线。 */
export function savedItemSummary(saved: SavedItem): string {
  return `已保存「${saved.title}」（${saved.sensitivity}${saved.replay ? '，幂等重放' : ''}）：${saved.itemId}`
}
