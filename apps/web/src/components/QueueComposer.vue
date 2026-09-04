<script setup lang="ts">
import { CornerDownLeft, ListPlus, Trash2 } from 'lucide-vue-next'
import { computed, ref, watch } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import { queueLabel } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'
import type { ActiveTurnPhase, QueueState } from '@/transport/types'

const props = defineProps<{ sessionId: string; queue: QueueState; phase: ActiveTurnPhase }>()
const { availability, sendCommand } = useConsoleStore()
const draft = ref(props.queue.text ?? '')
const sendMode = ref<'queue' | 'steer'>('queue')

watch(
  () => props.queue.text,
  (value) => (draft.value = value ?? ''),
)

const operation = computed(() => {
  if (props.phase === 'RUNNING' && sendMode.value === 'steer') return 'STEER'
  if (props.queue.status !== 'EMPTY') return 'REPLACE_QUEUE'
  return props.phase === 'IDLE' ? 'START_TURN' : 'SET_QUEUE'
})
const submitAvailability = computed(() => availability(props.sessionId, operation.value))
const cancelAvailability = computed(() => availability(props.sessionId, 'CANCEL_QUEUE'))

async function submit(): Promise<void> {
  const text = draft.value.trim()
  if (!text) return
  const receipt = await sendCommand(props.sessionId, operation.value, { text })
  if (receipt?.status === 'ACCEPTED_BY_BRIDGE' || receipt?.status === 'COMPLETED') draft.value = ''
}

async function cancel(): Promise<void> {
  await sendCommand(props.sessionId, 'CANCEL_QUEUE')
  draft.value = ''
}

function onKeydown(event: KeyboardEvent): void {
  if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) {
    event.preventDefault()
    void submit()
  }
}

function onMobileKeydown(event: KeyboardEvent): void {
  if (event.key === 'Enter') {
    event.preventDefault()
    void submit()
  }
}
</script>

<template>
  <section class="queue-composer" aria-labelledby="queue-title">
    <header>
      <div>
        <ListPlus :size="16" aria-hidden="true" />
        <strong id="queue-title">{{ phase === 'IDLE' && queue.status === 'EMPTY' ? '发送指令' : '下一轮队列' }}</strong>
        <StatusBadge :tone="queue.status === 'QUEUED' ? 'accent' : queue.status === 'PAUSED' ? 'warning' : 'neutral'">
          {{ queueLabel[queue.status] }}
        </StatusBadge>
      </div>
      <span>{{ phase === 'RUNNING' ? '默认排入下一轮' : '由 Desktop 开始新轮次' }}</span>
    </header>
    <textarea
      v-model="draft"
      rows="2"
      :disabled="!submitAvailability.enabled"
      :placeholder="phase === 'IDLE' && queue.status === 'EMPTY' ? '给 Codex Desktop 发送下一轮指令…' : sendMode === 'steer' ? '补充当前轮次的方向…' : queue.status === 'EMPTY' ? '当前轮次完成后要继续做什么？' : '替换已排队的下一轮内容…'"
      :aria-describedby="!submitAvailability.enabled ? 'queue-disabled-reason' : 'queue-shortcut'"
      @keydown="onKeydown"
    />
    <div class="queue-composer__footer">
      <UiButton
        v-if="phase === 'RUNNING' && queue.status === 'EMPTY'"
        variant="quiet"
        size="small"
        :disabled="!availability(sessionId, 'STEER').enabled && sendMode !== 'steer'"
        @click="sendMode = sendMode === 'queue' ? 'steer' : 'queue'"
      >
        {{ sendMode === 'steer' ? '改为下一轮' : 'Steer 当前轮' }}
      </UiButton>
      <span v-if="!submitAvailability.enabled" id="queue-disabled-reason" class="queue-composer__reason">
        {{ submitAvailability.reason }}
      </span>
      <span v-else id="queue-shortcut" class="queue-composer__shortcut">⌘/Ctrl + Enter 提交</span>
      <UiButton
        v-if="queue.status !== 'EMPTY'"
        variant="quiet"
        size="small"
        :disabled="!cancelAvailability.enabled"
        @click="cancel"
      >
        <template #icon><Trash2 aria-hidden="true" /></template>
        取消队列
      </UiButton>
      <UiButton
        variant="primary"
        :disabled="!submitAvailability.enabled || !draft.trim()"
        @click="submit"
      >
        <template #icon><CornerDownLeft aria-hidden="true" /></template>
        {{ operation === 'START_TURN' ? '开始新轮次' : operation === 'STEER' ? 'Steer 当前轮' : queue.status === 'EMPTY' ? '排入下一轮' : '替换队列' }}
      </UiButton>
    </div>
    <input
      v-model="draft"
      class="queue-composer__mobile-input"
      type="text"
      :disabled="!submitAvailability.enabled"
      :placeholder="operation === 'START_TURN' ? '发送下一轮…' : operation === 'STEER' ? 'Steer 当前轮…' : queue.status === 'EMPTY' ? '追加下一轮…' : '替换下一轮…'"
      aria-label="下一轮指令"
      @keydown="onMobileKeydown"
    />
    <button
      v-if="queue.status !== 'EMPTY'"
      class="queue-composer__mobile-cancel"
      type="button"
      :disabled="!cancelAvailability.enabled"
      aria-label="取消下一轮队列"
      @click="cancel"
    >
      <Trash2 :size="17" aria-hidden="true" />
    </button>
    <button
      class="queue-composer__mobile-submit"
      type="button"
      :disabled="!submitAvailability.enabled || !draft.trim()"
      :aria-label="operation === 'START_TURN' ? '开始新轮次' : operation === 'STEER' ? 'Steer 当前轮' : queue.status === 'EMPTY' ? '排入下一轮' : '替换下一轮队列'"
      @click="submit"
    >
      <CornerDownLeft :size="18" aria-hidden="true" />
    </button>
  </section>
</template>

<style scoped>
.queue-composer {
  display: grid;
  gap: 9px;
  padding: 11px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
  box-shadow: 0 -10px 32px rgb(0 0 0 / 8%);
}

.queue-composer header,
.queue-composer header > div,
.queue-composer__footer {
  display: flex;
  align-items: center;
}

.queue-composer header {
  justify-content: space-between;
  gap: 8px;
}

.queue-composer header > div {
  min-width: 0;
  gap: 7px;
}

.queue-composer header > span {
  color: var(--text-muted);
  font-size: 10px;
}

.queue-composer textarea {
  width: 100%;
  min-height: 64px;
  resize: vertical;
  padding: 9px 10px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-control);
  outline: 0;
  background: var(--bg-surface);
  color: var(--text-primary);
  line-height: 1.5;
}

.queue-composer textarea:focus {
  border-color: var(--focus-ring);
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--focus-ring), transparent 78%);
}

.queue-composer textarea:disabled {
  border-color: var(--border-subtle);
  background: var(--bg-app);
}

.queue-composer__footer {
  justify-content: flex-end;
  gap: 7px;
}

.queue-composer__reason,
.queue-composer__shortcut {
  flex: 1;
  color: var(--text-muted);
  font-size: 10px;
}

.queue-composer__reason {
  color: var(--warning);
}

.queue-composer__mobile-input,
.queue-composer__mobile-cancel,
.queue-composer__mobile-submit {
  display: none;
}

@media (max-width: 1100px) {
  .queue-composer {
    min-height: 56px;
    grid-template-columns: minmax(0, 1fr) auto auto;
    align-items: center;
    gap: 6px;
    padding: 6px 10px;
    border-right: 0;
    border-bottom: 0;
    border-left: 0;
    border-radius: 0;
    box-shadow: none;
  }

  .queue-composer > header,
  .queue-composer > textarea,
  .queue-composer__footer {
    display: none;
  }

  .queue-composer__mobile-input {
    display: block;
    width: 100%;
    min-width: 0;
    height: 44px;
    padding: 0 10px;
    border: 1px solid var(--border-strong);
    border-radius: var(--radius-control);
    outline: 0;
    background: var(--bg-surface);
    color: var(--text-primary);
  }

  .queue-composer__mobile-input:focus {
    border-color: var(--focus-ring);
  }

  .queue-composer__mobile-cancel,
  .queue-composer__mobile-submit {
    width: 44px;
    height: 44px;
    padding: 0;
    place-items: center;
    border-radius: var(--radius-control);
  }

  .queue-composer__mobile-cancel {
    border: 1px solid var(--border-subtle);
    background: transparent;
    color: var(--text-muted);
  }

  .queue-composer__mobile-submit {
    display: grid;
    border: 0;
    background: var(--accent);
    color: var(--accent-contrast);
  }

  .queue-composer__mobile-cancel:not([hidden]) {
    display: grid;
  }

  .queue-composer__mobile-cancel:disabled,
  .queue-composer__mobile-submit:disabled {
    opacity: 0.48;
  }
}
</style>
