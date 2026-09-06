/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** 构建流水线注入的 Console Web 提交标识;缺省时 UI 显示"未知"。 */
  readonly VITE_AGENT_CONSOLE_COMMIT?: string
  readonly VITE_AGENT_CONSOLE_FIXTURE?: string
}
