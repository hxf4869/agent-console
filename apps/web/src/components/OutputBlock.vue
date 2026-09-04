<script setup lang="ts">
import { Check, Copy, Radio, TriangleAlert } from 'lucide-vue-next'
import { ref } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import { authorityLabel } from '@/lib/presentation'
import type { OutputState } from '@/transport/types'

const props = defineProps<{ output: OutputState }>()
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
      <button type="button" :aria-label="copied ? '已复制输出' : '复制输出'" @click="copyOutput">
        <Check v-if="copied" :size="14" aria-hidden="true" />
        <Copy v-else :size="14" aria-hidden="true" />
      </button>
    </header>
    <div v-if="output.hasGap" class="output-block__gap">
      <TriangleAlert :size="14" aria-hidden="true" />
      输出存在缺口；不要把当前预览视为完整执行结果。
    </div>
    <pre><code>{{ output.text || '（暂无输出）' }}</code></pre>
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

.output-block button {
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

.output-block button:hover {
  background: var(--bg-elevated);
  color: var(--text-primary);
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

  .output-block button {
    width: 44px;
    height: 44px;
  }
}
</style>
