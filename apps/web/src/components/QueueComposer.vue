<script setup lang="ts">
import { CornerDownLeft, ListPlus, RefreshCw, Trash2 } from 'lucide-vue-next'
import { computed, ref } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import { queueLabel, requestStatusLabel } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'
import type { ActiveTurnPhase, QueueState } from '@/transport/types'

const props = defineProps<{ sessionId: string; queue: QueueState; phase: ActiveTurnPhase }>()
const { availability, sendCommand, getDraft, setDraft, verifyRequest, state } = useConsoleStore()

/** 草稿按 (device, agentKind, nativeSession) 存于 store(UX-03):切会话不串、状态更新不清空。 */
const draft = computed<string>({
  get: () => getDraft(props.sessionId) || props.queue.text || '',
  set: (value) => setDraft(props.sessionId, value),
})
const sendMode = ref<'queue' | 'steer'>('queue')
/** 发送中防重复:一条命令在途回执前按钮与输入全部禁用。 */
const submitting = ref(false)
/** 本次输入框最近一次发送的请求 ID;用于关联回执阶段展示。 */
const lastRequestId = ref('')

const operation = computed(() => {
  if (props.phase === 'RUNNING' && sendMode.value === 'steer') return 'STEER'
  if (props.queue.status !== 'EMPTY') return 'REPLACE_QUEUE'
  return props.phase === 'IDLE' ? 'START_TURN' : 'SET_QUEUE'
})
const submitAvailability = computed(() => availability(props.sessionId, operation.value))
const cancelAvailability = computed(() => availability(props.sessionId, 'CANCEL_QUEUE'))

const lastReceipt = computed(() =>
  lastRequestId.value
    ? state.receipts.find((receipt) => receipt.requestId === lastRequestId.value)
    : undefined,
)
const lastReceiptLabel = computed(() =>
  lastReceipt.value ? requestStatusLabel(lastReceipt.value.status, lastReceipt.value.errorCode) : undefined,
)

async function submit(): Promise<void> {
  const text = draft.value.trim()
  if (!text || submitting.value) return
  submitting.value = true
  try {
    const receipt = await sendCommand(props.sessionId, operation.value, { text })
    if (receipt) lastRequestId.value = receipt.requestId
    // 只有确认被接受才清空输入;超时/未知/拒绝都保留用户输入,不自动新 ID 重发(UX-03)。
    if (receipt?.status === 'ACCEPTED_BY_BRIDGE' || receipt?.status === 'COMPLETED') draft.value = ''
  } finally {
    submitting.value = false
  }
}

async function verifyLastRequest(): Promise<void> {
  if (!lastRequestId.value) return
  await verifyRequest(lastRequestId.value)
}

async function cancel(): Promise<void> {
  if (submitting.value) return
  submitting.value = true
  try {
    await sendCommand(props.sessionId, 'CANCEL_QUEUE')
    draft.value = ''
  } finally {
    submitting.value = false
  }
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
    <p v-if="queue.status === 'PAUSED'" class="queue-composer__paused" role="note">
      队列已暂停：不会自动发送；需重新确认或取消后才会执行。
    </p>
    <textarea
      v-model="draft"
      rows="2"
      :disabled="!submitAvailability.enabled || submitting"
      :placeholder="phase === 'IDLE' && queue.status === 'EMPTY' ? '给 Codex Desktop 发送下一轮指令…' : sendMode === 'steer' ? '补充当前轮次的方向…' : queue.status === 'EMPTY' ? '当前轮次完成后要继续做什么？' : '替换已排队的下一轮内容…'"
      :aria-describedby="!submitAvailability.enabled ? 'queue-disabled-reason' : 'queue-shortcut'"
      @keydown="onKeydown"
    />
    <div class="queue-composer__footer">
      <UiButton
        v-if="phase === 'RUNNING' && queue.status === 'EMPTY'"
        variant="quiet"
        size="small"
        :disabled="submitting || (!availability(sessionId, 'STEER').enabled && sendMode !== 'steer')"
        @click="sendMode = sendMode === 'queue' ? 'steer' : 'queue'"
      >
        {{ sendMode === 'steer' ? '改为下一轮' : 'Steer 当前轮' }}
      </UiButton>
      <span v-if="lastReceiptLabel" class="queue-composer__receipt" role="status">
        {{ lastReceiptLabel.text }}
        <button
          v-if="lastReceiptLabel.verify"
          type="button"
          class="queue-composer__verify"
          @click="verifyLastRequest"
        >
          <RefreshCw :size="12" aria-hidden="true" />核对结果
        </button>
      </span>
      <span v-if="!submitAvailability.enabled" id="queue-disabled-reason" class="queue-composer__reason">
        {{ submitAvailability.reason }}
      </span>
      <span v-else id="queue-shortcut" class="queue-composer__shortcut">⌘/Ctrl + Enter 提交</span>
      <UiButton
        v-if="queue.status !== 'EMPTY'"
        variant="quiet"
        size="small"
        :disabled="submitting || !cancelAvailability.enabled"
        @click="cancel"
      >
        <template #icon><Trash2 aria-hidden="true" /></template>
        取消队列
      </UiButton>
      <UiButton
        variant="primary"
        :disabled="submitting || !submitAvailability.enabled || !draft.trim()"
        @click="submit"
      >
        <template #icon><CornerDownLeft aria-hidden="true" /></template>
        {{ submitting ? '提交中…' : operation === 'START_TURN' ? '开始新轮次' : operation === 'STEER' ? 'Steer 当前轮' : queue.status === 'EMPTY' ? '排入下一轮' : '替换队列' }}
      </UiButton>
    </div>
    <input
      v-model="draft"
      class="queue-composer__mobile-input"
      type="text"
      :disabled="!submitAvailability.enabled || submitting"
      :placeholder="operation === 'START_TURN' ? '发送下一轮…' : operation === 'STEER' ? 'Steer 当前轮…' : queue.status === 'EMPTY' ? '追加下一轮…' : '替换下一轮…'"
      aria-label="下一轮指令"
      @keydown="onMobileKeydown"
    />
    <button
      v-if="phase === 'RUNNING' && queue.status === 'EMPTY'"
      class="queue-composer__mobile-mode"
      type="button"
      :disabled="submitting || (!availability(sessionId, 'STEER').enabled && sendMode !== 'steer')"
      :aria-label="sendMode === 'steer' ? '改为下一轮' : 'Steer 当前轮'"
      :aria-pressed="sendMode === 'steer'"
      @click="sendMode = sendMode === 'queue' ? 'steer' : 'queue'"
    >
      {{ sendMode === 'steer' ? '队列' : 'Steer' }}
    </button>
    <button
      v-if="queue.status !== 'EMPTY'"
      class="queue-composer__mobile-cancel"
      type="button"
      :disabled="submitting || !cancelAvailability.enabled"
      aria-label="取消下一轮队列"
      @click="cancel"
    >
      <Trash2 :size="17" aria-hidden="true" />
    </button>
    <button
      class="queue-composer__mobile-submit"
      type="button"
      :disabled="submitting || !submitAvailability.enabled || !draft.trim()"
      :aria-label="submitting ? '提交中' : operation === 'START_TURN' ? '开始新轮次' : operation === 'STEER' ? 'Steer 当前轮' : queue.status === 'EMPTY' ? '排入下一轮' : '替换下一轮队列'"
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

.queue-composer__paused {
  margin: 0;
  padding: 6px 9px;
  border: 1px solid color-mix(in srgb, var(--warning), transparent 60%);
  border-radius: var(--radius-control);
  background: color-mix(in srgb, var(--warning), transparent 92%);
  color: var(--text-secondary);
  font-size: 11px;
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

.queue-composer__receipt {
  display: inline-flex;
  flex: 1;
  align-items: center;
  gap: 6px;
  color: var(--text-muted);
  font-size: 10px;
}

.queue-composer__verify {
  display: inline-flex;
  min-height: 24px;
  align-items: center;
  gap: 4px;
  padding: 2px 7px;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-control);
  background: var(--bg-surface);
  color: var(--accent);
  font-size: 10px;
  font-weight: 700;
}

.queue-composer__mobile-input,
.queue-composer__mobile-mode,
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
  .queue-composer > .queue-composer__paused,
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

  .queue-composer__mobile-mode,
  .queue-composer__mobile-cancel,
  .queue-composer__mobile-submit {
    height: 44px;
    padding: 0;
    place-items: center;
    border-radius: var(--radius-control);
  }

  .queue-composer__mobile-mode {
    display: grid;
    min-width: 58px;
    padding: 0 9px;
    border: 1px solid var(--border-strong);
    background: var(--bg-elevated);
    color: var(--text-secondary);
    font-size: 11px;
    font-weight: 700;
  }

  .queue-composer__mobile-mode[aria-pressed='true'] {
    border-color: var(--accent);
    background: var(--accent-soft);
    color: var(--accent);
  }

  .queue-composer__mobile-cancel {
    width: 44px;
    border: 1px solid var(--border-subtle);
    background: transparent;
    color: var(--text-muted);
  }

  .queue-composer__mobile-submit {
    width: 44px;
    display: grid;
    border: 0;
    background: var(--accent);
    color: var(--accent-contrast);
  }

  .queue-composer__mobile-cancel:not([hidden]) {
    display: grid;
  }

  .queue-composer__mobile-mode:disabled,
  .queue-composer__mobile-cancel:disabled,
  .queue-composer__mobile-submit:disabled {
    opacity: 0.48;
  }
}
</style>
