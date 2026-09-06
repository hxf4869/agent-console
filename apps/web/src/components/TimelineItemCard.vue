<script setup lang="ts">
import {
  Check,
  Circle,
  CircleAlert,
  CircleStop,
  Clock3,
  LoaderCircle,
  MessageSquareText,
  Save,
  SquareTerminal,
} from 'lucide-vue-next'
import { computed } from 'vue'

import AttentionCard from '@/components/AttentionCard.vue'
import OutputBlock from '@/components/OutputBlock.vue'
import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import { formatClock } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'
import type { TimelineItem } from '@/transport/types'

const props = defineProps<{ item: TimelineItem; sessionId: string }>()
const emit = defineEmits<{ save: [payload: { source: string; text: string }] }>()
const { availability, answerAttention, loadOutput, sendCommand } = useConsoleStore()
const stopAvailability = computed(() => availability(props.sessionId, 'STOP_BACKGROUND_COMMAND'))

function savePayload(source: string, text: string) {
  emit('save', { source, text })
}
</script>

<template>
  <article v-if="item.type === 'commentary'" class="timeline-card timeline-card--commentary">
    <header>
      <span><MessageSquareText :size="15" aria-hidden="true" />{{ item.author ?? 'Codex' }}</span>
      <div class="timeline-card__tools">
        <time :datetime="item.createdAt">{{ formatClock(item.createdAt) }}</time>
        <button
          v-if="item.body"
          type="button"
          class="timeline-card__save"
          aria-label="保存该条消息到工具箱"
          @click="savePayload(item.author ?? 'Codex', item.body)"
        >
          <Save :size="13" aria-hidden="true" />
        </button>
      </div>
    </header>
    <p>{{ item.body }}</p>
  </article>

  <article v-else-if="item.type === 'plan'" class="timeline-card timeline-card--plan">
    <header>
      <span><CircleAlert :size="15" aria-hidden="true" />执行计划</span>
      <time :datetime="item.createdAt">{{ formatClock(item.createdAt) }}</time>
    </header>
    <ol class="plan-steps">
      <li v-for="step in item.steps" :key="step.id" :class="`plan-step--${step.status.toLowerCase()}`">
        <Check v-if="step.status === 'COMPLETED'" :size="14" aria-hidden="true" />
        <LoaderCircle v-else-if="step.status === 'RUNNING'" class="spin" :size="14" aria-hidden="true" />
        <Circle v-else :size="12" aria-hidden="true" />
        <span>{{ step.label }}</span>
        <small>{{ step.status === 'COMPLETED' ? '完成' : step.status === 'RUNNING' ? '进行中' : '等待' }}</small>
      </li>
    </ol>
  </article>

  <article v-else-if="item.type === 'command'" class="timeline-card timeline-card--command">
    <header>
      <span><SquareTerminal :size="15" aria-hidden="true" />命令</span>
      <div>
        <StatusBadge :tone="item.status === 'FAILED' ? 'danger' : item.status === 'RUNNING' ? 'accent' : 'success'">
          {{ item.status }}
        </StatusBadge>
        <time :datetime="item.createdAt">{{ formatClock(item.createdAt) }}</time>
      </div>
    </header>
    <div class="command-line">
      <code>{{ item.command }}</code>
      <span class="mono">{{ item.cwdDisplay }} · {{ item.elapsed }}</span>
    </div>
    <OutputBlock
      :output="item.output"
      default-collapsed
      @load="loadOutput(sessionId, item.output.itemId)"
      @save="(payload: string) => savePayload('命令输出', payload)"
    />
  </article>

  <article v-else-if="item.type === 'background-command'" class="timeline-card timeline-card--background">
    <header>
      <span><Clock3 :size="15" aria-hidden="true" />后台命令</span>
      <StatusBadge :tone="item.status === 'RUNNING' ? 'accent' : 'neutral'" dot>{{ item.status }}</StatusBadge>
    </header>
    <div class="background-command">
      <div>
        <code>{{ item.command }}</code>
        <span class="mono">{{ item.commandId }} · {{ item.elapsed }}</span>
      </div>
      <UiButton
        variant="quiet"
        size="small"
        :disabled="item.status !== 'RUNNING' || !stopAvailability.enabled"
        :title="stopAvailability.reason"
        @click="sendCommand(sessionId, 'STOP_BACKGROUND_COMMAND', { commandId: item.commandId })"
      >
        <template #icon><CircleStop aria-hidden="true" /></template>
        停止
      </UiButton>
    </div>
  </article>

  <AttentionCard
    v-else-if="item.type === 'attention'"
    :item="item.attention"
    expanded
    @answer="answerAttention"
  />

  <article v-else-if="item.type === 'outcome-unknown'" class="timeline-card timeline-card--unknown">
    <header>
      <span><CircleAlert :size="15" aria-hidden="true" />结果状态未知</span>
      <time :datetime="item.createdAt">{{ formatClock(item.createdAt) }}</time>
    </header>
    <p>{{ item.description }}</p>
    <div class="request-id mono">request {{ item.requestId }}</div>
    <OutputBlock
      :output="item.output"
      default-collapsed
      @load="loadOutput(sessionId, item.output.itemId)"
      @save="(payload) => savePayload('未确认指令输出', payload)"
    />
  </article>
</template>

<style scoped>
.timeline-card {
  display: grid;
  gap: 11px;
  padding: 13px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.timeline-card > header,
.timeline-card > header > span,
.timeline-card > header > div,
.background-command {
  display: flex;
  align-items: center;
}

.timeline-card > header {
  justify-content: space-between;
  gap: 8px;
  color: var(--text-primary);
  font-size: 12px;
  font-weight: 700;
}

.timeline-card > header > span,
.timeline-card > header > div {
  gap: 7px;
}

.timeline-card > header svg {
  color: var(--accent);
}

.timeline-card > header time {
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 500;
}

.timeline-card__tools {
  display: flex;
  align-items: center;
  gap: 6px;
}

.timeline-card__save {
  display: grid;
  width: 26px;
  height: 26px;
  padding: 0;
  place-items: center;
  border: 0;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--text-muted);
}

.timeline-card__save:hover {
  background: var(--bg-elevated);
  color: var(--accent);
}

.timeline-card p {
  margin: 0;
  color: var(--text-secondary);
}

.timeline-card--commentary {
  background: color-mix(in srgb, var(--accent-soft), var(--bg-elevated) 72%);
}

.timeline-card--unknown {
  border-color: color-mix(in srgb, var(--warning), transparent 48%);
}

.timeline-card--unknown > header > span,
.timeline-card--unknown > header svg {
  color: var(--warning);
}

.request-id {
  color: var(--text-muted);
  font-size: 10px;
}

.plan-steps {
  display: grid;
  gap: 8px;
  padding: 0;
  margin: 0;
  list-style: none;
}

.plan-steps li {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  color: var(--text-secondary);
  font-size: 12px;
}

.plan-steps li > svg {
  color: var(--text-muted);
}

.plan-steps li small {
  color: var(--text-muted);
}

.plan-steps .plan-step--completed > svg {
  color: var(--success);
}

.plan-steps .plan-step--running {
  color: var(--text-primary);
  font-weight: 650;
}

.plan-steps .plan-step--running > svg {
  color: var(--accent);
}

.command-line {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
}

.command-line code,
.background-command code {
  color: var(--text-primary);
  overflow-wrap: anywhere;
}

.command-line > span,
.background-command span {
  color: var(--text-muted);
  font-size: 10px;
}

.background-command {
  justify-content: space-between;
  gap: 12px;
}

.background-command > div {
  display: grid;
  min-width: 0;
  gap: 2px;
}

.spin {
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 759px) {
  .timeline-card {
    gap: 9px;
    padding: 12px 2px;
    border: 0;
    border-bottom: 1px solid var(--border-subtle);
    border-radius: 0;
    background: transparent;
  }

  .timeline-card--commentary {
    background: transparent;
  }

  .timeline-card--unknown {
    border-bottom-color: color-mix(in srgb, var(--warning), transparent 48%);
  }

  .command-line {
    display: grid;
  }

  .background-command {
    align-items: flex-start;
  }
}
</style>
