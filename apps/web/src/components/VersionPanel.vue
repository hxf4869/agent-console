<script setup lang="ts">
import { Layers } from 'lucide-vue-next'
import { computed } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import { useConsoleStore } from '@/store/console'
import type { DeviceSummary } from '@/transport/types'

const props = defineProps<{ device?: DeviceSummary }>()
const { componentVersions } = useConsoleStore()

const toolboxValue = computed(() => {
  const versions = componentVersions()
  if (!versions.toolbox) return ''
  const commit = versions.toolboxCommit ? ` (${versions.toolboxCommit.slice(0, 7)})` : ''
  return `${versions.toolbox}${commit}`
})

const rows = computed(() => {
  const versions = componentVersions()
  return [
    { label: '协议版本', value: `v${versions.protocol}`, hint: 'Browser ⇄ Relay 线上协议主版本' },
    { label: 'Console 网页', value: versions.web, hint: '构建提交,由流水线注入' },
    { label: 'Relay', value: versions.relay, hint: '来自 GET /agent-console/api/version' },
    { label: 'Relay 协议', value: versions.relayProtocol ? `v${versions.relayProtocol}` : '', hint: 'Relay 端点上报的协议主版本' },
    { label: 'Toolbox', value: toolboxValue.value, hint: '来自 GET /api/v1/version;占位值按未知展示' },
    { label: 'Bridge', value: props.device?.bridgeVersion || versions.bridge, hint: '设备上报' },
    { label: 'Codex Desktop', value: versions.codex, hint: '能力快照上报' },
  ]
})

/** 普通补丁版本不同但协议主版本一致时不做全局门禁(IN-01)。 */
const protocolGate = computed(() => {
  const versions = componentVersions()
  return `线上协议主版本 v${versions.protocol} 一致即可工作；补丁版本差异只影响展示，不整体禁用。`
})

/** Relay 上报的协议版本与网页不一致:提示需要升级哪一端,不整站禁用(IN-01)。 */
const protocolMismatch = computed(() => {
  const versions = componentVersions()
  return versions.relayProtocol !== '' && versions.relayProtocol !== versions.protocol
})
</script>

<template>
  <section class="detail-card version-card" aria-labelledby="version-title">
    <header>
      <div><Layers :size="17" /><div><h2 id="version-title">版本组成</h2></div></div>
      <StatusBadge v-if="protocolMismatch" tone="warning">
        Relay 协议 v{{ componentVersions().relayProtocol }} ≠ 网页 v{{ componentVersions().protocol }}：需要升级一端
      </StatusBadge>
      <StatusBadge v-else tone="neutral">有则展示 · 无则未知</StatusBadge>
    </header>
    <dl>
      <div v-for="row in rows" :key="row.label" :title="row.hint">
        <dt>{{ row.label }}</dt>
        <dd class="mono">{{ row.value || '未知' }}</dd>
      </div>
    </dl>
    <p class="version-card__note">{{ protocolGate }}</p>
  </section>
</template>

<style scoped>
.detail-card {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.detail-card > header {
  display: flex;
  min-height: 48px;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--border-subtle);
}

.detail-card > header > div {
  display: flex;
  align-items: center;
  gap: 8px;
}

.detail-card > header svg {
  color: var(--accent);
}

.detail-card h2 {
  margin: 0;
  font-size: 13px;
}

.version-card dl {
  padding: 4px 12px 8px;
  margin: 0;
}

.version-card dl > div {
  display: grid;
  min-height: 34px;
  grid-template-columns: 110px minmax(0, 1fr);
  align-items: center;
  gap: 8px;
  border-bottom: 1px solid var(--border-subtle);
}

.version-card dl > div:last-child {
  border-bottom: 0;
}

.version-card dt {
  color: var(--text-muted);
  font-size: 10px;
}

.version-card dd {
  min-width: 0;
  margin: 0;
  color: var(--text-secondary);
  font-size: 10px;
  overflow-wrap: anywhere;
  text-align: right;
}

.version-card__note {
  padding: 0 12px 10px;
  margin: 0;
  color: var(--text-muted);
  font-size: 10px;
}
</style>
