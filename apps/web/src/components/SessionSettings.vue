<script setup lang="ts">
import { Gauge, LockKeyhole, Settings2 } from 'lucide-vue-next'
import { computed } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import { useConsoleStore } from '@/store/console'
import type { RuntimeSettings, RuntimeSnapshot, SelectOption } from '@/transport/types'

const props = defineProps<{ sessionId: string; runtime: RuntimeSnapshot }>()
const { availability, updateSetting } = useConsoleStore()

const control = computed(() => availability(props.sessionId, 'UPDATE_SETTINGS'))

const fields: Array<{
  key: keyof RuntimeSettings
  label: string
  options: keyof Pick<
    RuntimeSnapshot['capabilities'],
    'models' | 'thinkingDepths' | 'serviceTiers' | 'permissionModes' | 'collaborationModes'
  >
}> = [
  { key: 'model', label: '模型', options: 'models' },
  { key: 'thinkingDepth', label: '思考深度', options: 'thinkingDepths' },
  { key: 'serviceTier', label: '服务等级', options: 'serviceTiers' },
  { key: 'permissionMode', label: '权限模式', options: 'permissionModes' },
  { key: 'collaborationMode', label: '协作模式', options: 'collaborationModes' },
]

function onChange(key: keyof RuntimeSettings, event: Event): void {
  void updateSetting(props.sessionId, key, (event.target as HTMLSelectElement).value)
}

function optionsFor(key: (typeof fields)[number]['options']): SelectOption[] {
  return props.runtime.capabilities[key]
}
</script>

<template>
  <section class="settings-card">
    <header>
      <div>
        <Settings2 :size="16" aria-hidden="true" />
        <strong>本轮运行设置</strong>
      </div>
      <StatusBadge :tone="control.enabled ? 'success' : 'warning'">
        {{ control.enabled ? '可控制' : '只读' }}
      </StatusBadge>
    </header>

    <div class="settings-card__fields">
      <label v-for="field in fields" :key="field.key">
        <span>{{ field.label }}</span>
        <select
          :value="runtime.settings[field.key]"
          :disabled="!control.enabled"
          :aria-describedby="!control.enabled ? 'settings-disabled-reason' : undefined"
          @change="onChange(field.key, $event)"
        >
          <option
            v-for="option in optionsFor(field.options)"
            :key="option.id"
            :value="option.id"
            :disabled="option.disabled"
          >
            {{ option.label }}{{ option.disabled ? ' — 不可用' : '' }}
          </option>
        </select>
      </label>
    </div>

    <p v-if="!control.enabled" id="settings-disabled-reason" class="settings-card__reason">
      <LockKeyhole :size="14" aria-hidden="true" />{{ control.reason }}
    </p>

    <div class="context-usage">
      <div>
        <Gauge :size="15" aria-hidden="true" />
        <span>上下文</span>
        <strong>{{ Math.round((runtime.contextUsed / runtime.contextWindow) * 100) }}%</strong>
      </div>
      <progress :value="runtime.contextUsed" :max="runtime.contextWindow">
        {{ runtime.contextUsed }} / {{ runtime.contextWindow }}
      </progress>
      <span class="mono">{{ runtime.contextUsed.toLocaleString() }} / {{ runtime.contextWindow.toLocaleString() }}</span>
    </div>
  </section>
</template>

<style scoped>
.settings-card {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.settings-card > header,
.settings-card > header > div,
.context-usage > div {
  display: flex;
  align-items: center;
}

.settings-card > header {
  min-height: 46px;
  justify-content: space-between;
  gap: 8px;
  padding: 8px 10px 8px 12px;
  border-bottom: 1px solid var(--border-subtle);
}

.settings-card > header > div {
  gap: 8px;
}

.settings-card__fields {
  display: grid;
  gap: 11px;
  padding: 12px;
}

.settings-card label {
  display: grid;
  gap: 5px;
}

.settings-card label > span {
  color: var(--text-muted);
  font-size: 11px;
  font-weight: 650;
}

.settings-card select {
  width: 100%;
  min-height: 34px;
  padding: 5px 28px 5px 8px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-control);
  background: var(--bg-surface);
  color: var(--text-primary);
}

.settings-card select:disabled {
  border-color: var(--border-subtle);
  color: var(--text-muted);
}

.settings-card__reason {
  display: flex;
  align-items: flex-start;
  gap: 7px;
  padding: 0 12px 12px;
  margin: 0;
  color: var(--warning);
  font-size: 11px;
}

.settings-card__reason svg {
  flex: 0 0 auto;
  margin-top: 2px;
}

.context-usage {
  display: grid;
  gap: 7px;
  padding: 12px;
  border-top: 1px solid var(--border-subtle);
}

.context-usage > div {
  gap: 7px;
}

.context-usage > div span {
  flex: 1;
  color: var(--text-secondary);
}

.context-usage progress {
  width: 100%;
  height: 6px;
  overflow: hidden;
  border: 0;
  border-radius: 999px;
  background: var(--bg-surface);
}

.context-usage progress::-webkit-progress-bar {
  background: var(--bg-surface);
}

.context-usage progress::-webkit-progress-value {
  border-radius: 999px;
  background: var(--accent);
}

.context-usage progress::-moz-progress-bar {
  border-radius: 999px;
  background: var(--accent);
}

.context-usage > .mono {
  color: var(--text-muted);
  font-size: 10px;
  text-align: right;
}

@media (max-width: 599px) {
  .settings-card select {
    min-height: 44px;
  }
}
</style>
