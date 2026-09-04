<script setup lang="ts">
import { AlertTriangle, Info, WifiOff } from 'lucide-vue-next'

withDefaults(
  defineProps<{
    tone?: 'info' | 'warning' | 'danger'
    title: string
  }>(),
  { tone: 'info' },
)
</script>

<template>
  <section class="notice" :class="`notice--${tone}`" role="status">
    <WifiOff v-if="tone === 'danger'" :size="18" aria-hidden="true" />
    <AlertTriangle v-else-if="tone === 'warning'" :size="18" aria-hidden="true" />
    <Info v-else :size="18" aria-hidden="true" />
    <div>
      <strong>{{ title }}</strong>
      <p><slot /></p>
    </div>
  </section>
</template>

<style scoped>
.notice {
  display: flex;
  align-items: flex-start;
  gap: var(--space-3);
  padding: 12px 14px;
  border: 1px solid color-mix(in srgb, var(--accent), transparent 60%);
  border-radius: var(--radius-card);
  background: color-mix(in srgb, var(--accent), transparent 90%);
  color: var(--text-secondary);
}

.notice > svg {
  flex: 0 0 auto;
  margin-top: 2px;
  color: var(--accent);
}

.notice strong {
  color: var(--text-primary);
}

.notice p {
  margin: 2px 0 0;
}

.notice--warning {
  border-color: color-mix(in srgb, var(--warning), transparent 55%);
  background: color-mix(in srgb, var(--warning), transparent 90%);
}

.notice--warning > svg {
  color: var(--warning);
}

.notice--danger {
  border-color: color-mix(in srgb, var(--danger), transparent 55%);
  background: color-mix(in srgb, var(--danger), transparent 90%);
}

.notice--danger > svg {
  color: var(--danger);
}
</style>
