<script setup lang="ts">
import { CheckCircle2, CircleAlert, Info, X } from 'lucide-vue-next'
import { useRouter } from 'vue-router'

import { useConsoleStore } from '@/store/console'

const router = useRouter()
const { state, dismissToast } = useConsoleStore()

/** 站内提醒点击:经正常认证进入会话,不在推送/提醒里内嵌任何批准动作(UX-06)。 */
function openReminder(linkTo: string): void {
  void router.push(linkTo)
}
</script>

<template>
  <div class="toast-region" aria-live="polite" aria-label="操作通知">
    <div v-for="toast in state.toasts" :key="toast.id" class="toast" :class="`toast--${toast.tone}`">
      <CheckCircle2 v-if="toast.tone === 'success'" :size="17" aria-hidden="true" />
      <CircleAlert v-else-if="toast.tone === 'warning' || toast.tone === 'danger'" :size="17" aria-hidden="true" />
      <Info v-else :size="17" aria-hidden="true" />
      <button
        v-if="toast.linkTo"
        type="button"
        class="toast__link"
        @click="openReminder(toast.linkTo)"
      >
        <span>{{ toast.message }}</span>
        <span class="toast__open">查看</span>
      </button>
      <span v-else>{{ toast.message }}</span>
      <button type="button" aria-label="关闭通知" @click="dismissToast(toast.id)">
        <X :size="15" aria-hidden="true" />
      </button>
    </div>
  </div>
</template>

<style scoped>
.toast-region {
  position: fixed;
  z-index: var(--layer-toast);
  right: var(--space-5);
  bottom: var(--space-5);
  display: grid;
  width: min(360px, calc(100vw - 32px));
  gap: var(--space-2);
  pointer-events: none;
}

.toast {
  display: grid;
  grid-template-columns: auto 1fr auto;
  align-items: center;
  gap: var(--space-2);
  padding: 10px 12px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
  box-shadow: var(--shadow-overlay);
  color: var(--text-primary);
  pointer-events: auto;
}

.toast--success > svg {
  color: var(--success);
}

.toast--warning > svg,
.toast--danger > svg {
  color: var(--warning);
}

.toast--danger > svg {
  color: var(--danger);
}

.toast__link {
  display: flex;
  min-width: 0;
  flex: 1;
  align-items: baseline;
  justify-content: space-between;
  gap: var(--space-2);
  padding: 0;
  border: 0;
  background: transparent;
  color: inherit;
  font: inherit;
  text-align: left;
  cursor: pointer;
}

.toast__link > span:first-child {
  min-width: 0;
}

.toast__open {
  flex: 0 0 auto;
  color: var(--accent);
  font-weight: 700;
  white-space: nowrap;
}

.toast button {
  display: grid;
  width: 30px;
  height: 30px;
  padding: 0;
  place-items: center;
  border: 0;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--text-muted);
}

.toast button:hover {
  background: var(--bg-surface);
  color: var(--text-primary);
}

@media (max-width: 599px) {
  .toast-region {
    right: 16px;
    bottom: 16px;
  }

  .toast button {
    width: 44px;
    height: 44px;
  }
}
</style>
