<script setup lang="ts">
withDefaults(
  defineProps<{
    variant?: 'primary' | 'secondary' | 'danger' | 'quiet'
    size?: 'small' | 'medium'
    disabled?: boolean
  }>(),
  {
    variant: 'secondary',
    size: 'medium',
    disabled: false,
  },
)
</script>

<template>
  <button
    class="ui-button"
    :class="[`ui-button--${variant}`, `ui-button--${size}`]"
    :disabled="disabled"
  >
    <slot name="icon" />
    <span><slot /></span>
  </button>
</template>

<style scoped>
.ui-button {
  display: inline-flex;
  min-width: 0;
  align-items: center;
  justify-content: center;
  gap: var(--space-2);
  border: 1px solid transparent;
  border-radius: var(--radius-control);
  font-weight: 650;
  line-height: 1.2;
  white-space: nowrap;
  transition:
    background var(--motion-fast) var(--ease-standard),
    border-color var(--motion-fast) var(--ease-standard),
    color var(--motion-fast) var(--ease-standard);
}

.ui-button--medium {
  min-height: 36px;
  padding: 8px 12px;
}

.ui-button--small {
  min-height: 30px;
  padding: 5px 9px;
  font-size: 12px;
}

.ui-button--primary {
  background: var(--accent);
  color: var(--accent-contrast);
}

.ui-button--primary:hover:not(:disabled) {
  filter: brightness(1.08);
}

.ui-button--secondary {
  border-color: var(--border-strong);
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.ui-button--secondary:hover:not(:disabled),
.ui-button--quiet:hover:not(:disabled) {
  background: var(--accent-soft);
  color: var(--text-primary);
}

.ui-button--danger {
  border-color: color-mix(in srgb, var(--danger), transparent 45%);
  background: color-mix(in srgb, var(--danger), transparent 84%);
  color: var(--danger);
}

.ui-button--quiet {
  background: transparent;
  color: var(--text-secondary);
}

.ui-button:disabled {
  border-color: var(--border-subtle);
  background: var(--bg-surface);
  color: var(--text-muted);
  opacity: 0.62;
}

.ui-button :deep(svg) {
  width: 16px;
  height: 16px;
  flex: 0 0 auto;
}

@media (max-width: 599px) {
  .ui-button {
    min-width: 44px;
    min-height: 44px;
  }
}
</style>
