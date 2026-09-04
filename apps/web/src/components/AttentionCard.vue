<script setup lang="ts">
import { AlertTriangle, ChevronRight, CircleHelp, Clock3, ShieldAlert } from 'lucide-vue-next'
import { computed } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import { formatClock } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'
import type { AttentionItem } from '@/transport/types'

const props = withDefaults(
  defineProps<{
    item: AttentionItem
    selected?: boolean
    expanded?: boolean
  }>(),
  { selected: false, expanded: false },
)

const emit = defineEmits<{
  select: [id: string]
  answer: [attentionId: string, optionId: string]
}>()

const { availability } = useConsoleStore()
const operation = computed(() =>
  props.item.kind === 'RISK_APPROVAL' ? 'ANSWER_APPROVAL' : 'ANSWER_QUESTION',
)
const operationAvailability = computed(() => availability(props.item.sessionId, operation.value))
</script>

<template>
  <article
    class="attention-card"
    :class="{
      'attention-card--selected': selected,
      'attention-card--risk': item.kind === 'RISK_APPROVAL',
    }"
  >
    <button class="attention-card__select" type="button" @click="emit('select', item.id)">
      <span class="attention-card__icon" aria-hidden="true">
        <ShieldAlert v-if="item.kind === 'RISK_APPROVAL'" :size="18" />
        <CircleHelp v-else :size="18" />
      </span>
      <span class="attention-card__summary">
        <span class="attention-card__eyebrow">
          <span>{{ item.kind === 'RISK_APPROVAL' ? '风险审批' : 'Codex 提问' }}</span>
          <time :datetime="item.createdAt"><Clock3 :size="11" aria-hidden="true" />{{ formatClock(item.createdAt) }}</time>
        </span>
        <strong>{{ item.title }}</strong>
        <span class="visually-clamped">{{ item.description }}</span>
      </span>
      <ChevronRight :size="16" aria-hidden="true" />
    </button>

    <div v-if="expanded" class="attention-card__details">
      <div v-if="item.requestAction" class="attention-card__fact">
        <span>请求操作</span>
        <p>{{ item.requestAction }}</p>
      </div>
      <div v-if="item.risk" class="attention-card__risk-note">
        <AlertTriangle :size="16" aria-hidden="true" />
        <div><strong>风险说明</strong><p>{{ item.risk }}</p></div>
      </div>
      <p v-if="!item.requestAction" class="attention-card__question">{{ item.description }}</p>

      <div class="attention-card__context">
        <StatusBadge>{{ item.sessionId }}</StatusBadge>
        <span class="mono">turn {{ item.turnId.replace('turn-', '') }}</span>
      </div>

      <div class="attention-card__actions" :aria-describedby="`${item.id}-disabled-reason`">
        <UiButton
          v-for="option in item.options"
          :key="option.id"
          :variant="option.emphasis === 'primary' ? 'primary' : option.emphasis === 'danger' ? 'danger' : 'secondary'"
          :disabled="!operationAvailability.enabled"
          @click="emit('answer', item.id, option.id)"
        >
          {{ option.label }}
        </UiButton>
      </div>
      <p
        v-if="!operationAvailability.enabled"
        :id="`${item.id}-disabled-reason`"
        class="attention-card__disabled"
      >
        {{ operationAvailability.reason }}
      </p>
    </div>
  </article>
</template>

<style scoped>
.attention-card {
  overflow: hidden;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.attention-card--selected {
  border-color: color-mix(in srgb, var(--accent), transparent 38%);
  box-shadow: inset 3px 0 var(--accent);
}

.attention-card--risk:not(.attention-card--selected) {
  border-color: color-mix(in srgb, var(--warning), transparent 65%);
}

.attention-card__select {
  display: grid;
  width: 100%;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 11px;
  padding: 13px;
  border: 0;
  background: transparent;
  color: inherit;
  text-align: left;
}

.attention-card__select:hover {
  background: color-mix(in srgb, var(--accent-soft), transparent 35%);
}

.attention-card__icon {
  display: grid;
  width: 32px;
  height: 32px;
  place-items: center;
  border-radius: var(--radius-control);
  background: var(--accent-soft);
  color: var(--accent);
}

.attention-card--risk .attention-card__icon {
  background: color-mix(in srgb, var(--warning), transparent 84%);
  color: var(--warning);
}

.attention-card__summary {
  display: grid;
  min-width: 0;
  gap: 3px;
}

.attention-card__summary > strong {
  overflow: hidden;
  color: var(--text-primary);
  font-size: 13px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.attention-card__summary > span:last-child {
  color: var(--text-secondary);
  font-size: 12px;
}

.attention-card__eyebrow {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 750;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.attention-card__eyebrow time {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  letter-spacing: 0;
}

.attention-card__select > svg {
  color: var(--text-muted);
}

.attention-card__details {
  display: grid;
  gap: 14px;
  padding: 0 14px 14px;
  border-top: 1px solid var(--border-subtle);
}

.attention-card__fact {
  display: grid;
  gap: 5px;
  padding-top: 14px;
}

.attention-card__fact > span {
  color: var(--text-muted);
  font-size: 11px;
  font-weight: 750;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.attention-card__fact p,
.attention-card__risk-note p,
.attention-card__question {
  margin: 0;
  color: var(--text-secondary);
}

.attention-card__risk-note {
  display: flex;
  align-items: flex-start;
  gap: 9px;
  padding: 10px;
  border: 1px solid color-mix(in srgb, var(--warning), transparent 60%);
  border-radius: var(--radius-control);
  background: color-mix(in srgb, var(--warning), transparent 90%);
}

.attention-card__risk-note > svg {
  flex: 0 0 auto;
  margin-top: 2px;
  color: var(--warning);
}

.attention-card__risk-note strong {
  color: var(--warning);
  font-size: 12px;
}

.attention-card__risk-note p {
  margin-top: 2px;
}

.attention-card__question {
  padding-top: 14px;
  font-size: 15px;
  color: var(--text-primary);
}

.attention-card__context,
.attention-card__actions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}

.attention-card__context .mono {
  color: var(--text-muted);
  font-size: 11px;
}

.attention-card__actions {
  justify-content: flex-end;
}

.attention-card__disabled {
  margin: -6px 0 0;
  color: var(--warning);
  font-size: 11px;
  text-align: right;
}

@media (max-width: 599px) {
  .attention-card__select {
    align-items: start;
    padding: 14px 12px;
  }

  .attention-card__icon {
    width: 30px;
    height: 30px;
  }

  .attention-card__eyebrow {
    align-items: flex-start;
  }

  .attention-card__actions :deep(.ui-button) {
    flex: 1 1 140px;
  }
}
</style>
