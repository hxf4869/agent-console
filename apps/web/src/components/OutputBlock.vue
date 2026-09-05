<script setup lang="ts">
import { Check, Copy, LoaderCircle, Radio, RefreshCw, TriangleAlert } from 'lucide-vue-next'
import { ref } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import { authorityLabel } from '@/lib/presentation'
import type { OutputState } from '@/transport/types'

const props = defineProps<{ output: OutputState }>()
const emit = defineEmits<{ load: [] }>()
const copied = ref(false)

async function copyOutput(): Promise<void> {
  try {
    await navigator.clipboard.writeText(props.output.text)
    copied.value = true
    window.setTimeout(() => (copied.value = false), 1500)
  } catch {
    copied.value = false
  }
}
</script>

<template>
  <section class="output-block" :aria-live="output.isFinal ? 'off' : 'polite'">
    <header>
      <StatusBadge
        :tone="output.authority === 'AUTHORITATIVE_FINAL' ? 'success' : output.hasGap ? 'warning' : 'accent'"
        :dot="output.authority === 'LIVE_PREVIEW'"
      >
        <Radio v-if="output.authority === 'LIVE_PREVIEW'" :size="11" aria-hidden="true" />
        {{ authorityLabel[output.authority] }}
      </StatusBadge>
      <span class="mono">rev {{ output.revision }} · {{ output.byteLength }} B</span>
      <button
        type="button"
        class="output-block__copy"
        :disabled="!output.text || output.loadState === 'LOADING'"
        :aria-label="copied ? '已复制输出' : '复制输出'"
        @click="copyOutput"
      >
        <Check v-if="copied" :size="14" aria-hidden="true" />
        <Copy v-else :size="14" aria-hidden="true" />
      </button>
    </header>
    <div v-if="output.hasGap" class="output-block__gap">
      <TriangleAlert :size="14" aria-hidden="true" />
      输出存在缺口；不要把当前预览视为完整执行结果。
    </div>
    <div v-if="output.loadState" class="output-block__deferred" role="status">
      <span>
        {{
          output.loadState === 'FAILED'
            ? '最终输出暂时不可用。'
            : output.loadState === 'LOADING'
              ? '正在读取最终输出…'
              : '最终输出将在需要时读取。'
        }}
      </span>
      <button
        type="button"
        class="output-block__load"
        :disabled="output.loadState === 'LOADING'"
        @click="emit('load')"
      >
        <LoaderCircle v-if="output.loadState === 'LOADING'" class="spin" :size="14" aria-hidden="true" />
        <RefreshCw v-else :size="14" aria-hidden="true" />
        {{ output.loadState === 'FAILED' ? '重试' : '加载输出' }}
      </button>
    </div>
    <pre v-else><code>{{ output.text || '（暂无输出）' }}</code></pre>
  </section>
</template>

<style scoped>
.output-block {
  overflow: hidden;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-control);
  background: var(--bg-code);
}

.output-block header {
  display: flex;
  min-height: 38px;
  align-items: center;
  gap: 8px;
  padding: 5px 7px 5px 9px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-surface);
}

.output-block header > span:nth-child(2) {
  flex: 1;
  color: var(--text-muted);
  font-size: 10px;
  text-align: right;
}

.output-block__copy {
  display: grid;
  width: 28px;
  height: 28px;
  padding: 0;
  place-items: center;
  border: 0;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--text-muted);
}

.output-block__copy:hover:not(:disabled) {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.output-block__copy:disabled {
  cursor: not-allowed;
  opacity: 0.45;
}

.output-block__deferred {
  display: flex;
  min-height: 58px;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 12px;
  color: var(--text-muted);
  font-size: 12px;
}

.output-block__load {
  display: inline-flex;
  min-height: 32px;
  align-items: center;
  gap: 6px;
  padding: 6px 10px;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-control);
  background: var(--bg-surface);
  color: var(--text-primary);
  font: inherit;
  font-weight: 650;
}

.output-block__load:hover:not(:disabled) {
  border-color: var(--accent);
  color: var(--accent);
}

.output-block__load:disabled {
  cursor: wait;
  opacity: 0.7;
}

.spin {
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.output-block__gap {
  display: flex;
  align-items: center;
  gap: 7px;
  padding: 7px 10px;
  border-bottom: 1px solid color-mix(in srgb, var(--warning), transparent 65%);
  background: color-mix(in srgb, var(--warning), transparent 90%);
  color: var(--warning);
  font-size: 11px;
}

.output-block pre {
  max-height: 340px;
  overflow: auto;
  padding: 12px;
  margin: 0;
  color: var(--text-secondary);
  font-size: 12px;
  line-height: 1.65;
  tab-size: 2;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}

@media (max-width: 599px) {
  .output-block header {
    min-height: 50px;
  }

  .output-block__copy {
    width: 44px;
    height: 44px;
  }

  .output-block__deferred {
    align-items: stretch;
    flex-direction: column;
  }

  .output-block__load {
    min-height: 44px;
    justify-content: center;
  }
}
</style>
