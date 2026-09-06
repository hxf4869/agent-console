import { PROTOCOL_VERSION } from '@agent-console/protocol/source'

/**
 * Console Web 构建提交:由构建流水线通过 VITE_AGENT_CONSOLE_COMMIT 注入。
 * 本地开发或未注入时显示"未知",不强造数据(IN-01:有则展示、无则未知)。
 */
export const consoleWebCommit: string = import.meta.env.VITE_AGENT_CONSOLE_COMMIT || '未知'

/** 与 Rust 侧 codec::PROTOCOL_VERSION 一致的协议主版本。 */
export const consoleProtocolVersion: string = String(PROTOCOL_VERSION)

/** Relay 无门禁只读版本端点(经浏览器 API 前缀消费;响应 {relayVersion, protocolVersion})。 */
export const RELAY_VERSION_PATH = '/agent-console/api/version'

export interface RelayVersionInfo {
  relayVersion: string
  /** Relay 报告的线上协议版本;与本地不一致时提示"需要升级哪一端"。 */
  protocolVersion: string
}

type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

function defaultFetch(input: string, init?: RequestInit): Promise<Response> {
  return fetch(input, init)
}

/**
 * Relay 版本加载器:每页面只取一次并缓存。
 * 端点不可达/非 2xx/响应异常一律返回 undefined,由调用方按"未知"展示,不报错(IN-01)。
 */
export function createRelayVersionLoader(fetchImpl: FetchLike = defaultFetch): () => Promise<RelayVersionInfo | undefined> {
  let cache: Promise<RelayVersionInfo | undefined> | undefined
  return function loadRelayVersion(): Promise<RelayVersionInfo | undefined> {
    cache ??= (async () => {
      try {
        const response = await fetchImpl(RELAY_VERSION_PATH, {
          credentials: 'same-origin',
          cache: 'no-store',
        })
        if (!response.ok) return undefined
        const body = (await response.json()) as { relayVersion?: unknown; protocolVersion?: unknown }
        const relayVersion = typeof body.relayVersion === 'string' ? body.relayVersion : ''
        if (!relayVersion) return undefined
        return {
          relayVersion,
          protocolVersion: body.protocolVersion === undefined ? '' : String(body.protocolVersion),
        }
      } catch {
        return undefined
      }
    })()
    return cache
  }
}

/** 应用内单例:版本在页面生命周期内不变。 */
export const loadRelayVersion = createRelayVersionLoader()

/** Toolbox 无认证版本端点(同域;响应 {apiVersion, appVersion, commit})。 */
export const TOOLBOX_VERSION_PATH = '/api/v1/version'

export interface ToolboxVersionInfo {
  appVersion: string
  commit: string
}

/**
 * 占位值("unknown"/"development"/空)不携带版本信息:
 * 与"网页 commit 未注入显示未知"同语义,一律回退为未知,不把占位符当版本展示(IN-01)。
 */
const VERSION_PLACEHOLDERS = new Set(['', 'unknown', 'development'])

function meaningfulVersion(value: unknown): string {
  if (typeof value !== 'string') return ''
  return VERSION_PLACEHOLDERS.has(value.toLowerCase()) ? '' : value
}

/** Toolbox 版本加载器:与 Relay loader 相同的缓存与降级语义。 */
export function createToolboxVersionLoader(fetchImpl: FetchLike = defaultFetch): () => Promise<ToolboxVersionInfo | undefined> {
  let cache: Promise<ToolboxVersionInfo | undefined> | undefined
  return function loadToolboxVersion(): Promise<ToolboxVersionInfo | undefined> {
    cache ??= (async () => {
      try {
        const response = await fetchImpl(TOOLBOX_VERSION_PATH, {
          credentials: 'same-origin',
          cache: 'no-store',
        })
        if (!response.ok) return undefined
        const body = (await response.json()) as { appVersion?: unknown; commit?: unknown }
        const appVersion = meaningfulVersion(body.appVersion)
        if (!appVersion) return undefined
        return { appVersion, commit: meaningfulVersion(body.commit) }
      } catch {
        return undefined
      }
    })()
    return cache
  }
}

/** 应用内单例:版本在页面生命周期内不变。 */
export const loadToolboxVersion = createToolboxVersionLoader()
