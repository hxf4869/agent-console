<script setup lang="ts">
withDefaults(
  defineProps<{
    tone?: 'neutral' | 'accent' | 'success' | 'warning' | 'danger'
    dot?: boolean
  }>(),
  { tone: 'neutral', dot: false },
)
</script>

<template>
  <span class="status-badge" :class="`status-badge--${tone}`">
    <span v-if="dot" class="status-badge__dot" aria-hidden="true" />
    <slot />
  </span>
</template>

<style scoped>
.status-badge {
  display: inline-flex;
  min-height: 24px;
  align-items: center;
  gap: 6px;
  padding: 2px 8px;
  border: 1px solid var(--border-subtle);
  border-radius: 999px;
  background: var(--bg-surface);
  color: var(--text-secondary);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.02em;
  line-height: 1.2;
  white-space: nowrap;
}

.status-badge__dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: currentColor;
}

.status-badge--accent {
  border-color: color-mix(in srgb, var(--accent), transparent 58%);
  background: var(--accent-soft);
  color: var(--accent);
}

.status-badge--success {
  color: var(--success);
}

.status-badge--warning {
  color: var(--warning);
}

.status-badge--danger {
  color: var(--danger);
}
</style>
