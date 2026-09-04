<script setup lang="ts">
import {
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  CircleHelp,
  Clock3,
  ShieldAlert,
  X,
} from 'lucide-vue-next'
import { computed, onMounted, ref, watch } from 'vue'

import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import { useBottomSheetFocus } from '@/composables/useBottomSheetFocus'
import { formatClock } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'
import type { ControlOperation } from '@/transport/types'

const { pendingAttention, state, ensureRuntime, availability, answerAttention, presenceFor } = useConsoleStore()
const selectedId = ref('approval-9b4d')
const filter = ref<'all' | 'risk' | 'question'>('all')
const inboxPane = ref<HTMLElement | null>(null)
const { sheetElement, sheetOpen, openSheet, closeSheet, onSheetKeydown } = useBottomSheetFocus(
  () => inboxPane.value,
  '#decision-sheet',
)

const visibleAttention = computed(() =>
  pendingAttention.value.filter((item) => {
    if (filter.value === 'risk') return item.kind === 'RISK_APPROVAL'
    if (filter.value === 'question') return item.kind === 'USER_QUESTION'
    return true
  }),
)
const selected = computed(() =>
  visibleAttention.value.find((item) => item.id === selectedId.value) ?? visibleAttention.value[0],
)
const selectedSession = computed(() =>
  state.sessions.find((session) => session.id === selected.value?.sessionId),
)
const selectedRuntime = computed(() =>
  selected.value ? state.runtimes[selected.value.sessionId] : undefined,
)
const selectedPresence = computed(() => presenceFor(selected.value?.sessionId))
const selectedOperation = computed<ControlOperation>(() =>
  selected.value?.kind === 'RISK_APPROVAL' ? 'ANSWER_APPROVAL' : 'ANSWER_QUESTION',
)
const selectedAvailability = computed(() =>
  selected.value
    ? availability(selected.value.sessionId, selectedOperation.value)
    : { enabled: false, reason: '没有待处理项。' },
)
const riskCount = computed(() => pendingAttention.value.filter((item) => item.kind === 'RISK_APPROVAL').length)
const questionCount = computed(() => pendingAttention.value.filter((item) => item.kind === 'USER_QUESTION').length)
const runningSessions = computed(() => state.sessions.filter((session) => session.phase === 'RUNNING').slice(0, 2))
const recentSessions = computed(() => state.sessions.filter((session) => session.phase !== 'RUNNING').slice(0, 2))

onMounted(() => {
  void Promise.all(pendingAttention.value.map((item) => ensureRuntime(item.sessionId)))
})

watch(visibleAttention, (items) => {
  if (!items.some((item) => item.id === selectedId.value)) selectedId.value = items[0]?.id ?? ''
  if (!items.length) void closeSheet()
})

function selectAttention(id: string, event: MouseEvent): void {
  selectedId.value = id
  if (window.matchMedia('(max-width: 759px)').matches) void openSheet(event.currentTarget)
}

async function respond(attentionId: string, optionId: string): Promise<void> {
  await answerAttention(attentionId, optionId)
  await closeSheet()
}
</script>

<template>
  <div class="attention-page">
    <div v-if="pendingAttention.length" class="attention-layout">
      <section ref="inboxPane" class="inbox-pane" aria-label="待处理列表" tabindex="-1">
        <header class="inbox-header">
          <div><h1>待处理</h1><span>{{ pendingAttention.length }}</span></div>
        </header>

        <div class="inbox-filters" aria-label="待处理分类">
          <button type="button" :aria-pressed="filter === 'all'" @click="filter = 'all'">全部 {{ pendingAttention.length }}</button>
          <button type="button" :aria-pressed="filter === 'risk'" @click="filter = 'risk'">审批 {{ riskCount }}</button>
          <button type="button" :aria-pressed="filter === 'question'" @click="filter = 'question'">提问 {{ questionCount }}</button>
        </div>

        <div class="inbox-list">
          <button
            v-for="item in visibleAttention"
            :key="item.id"
            type="button"
            class="inbox-row"
            :class="{ 'is-selected': selected?.id === item.id }"
            @click="selectAttention(item.id, $event)"
          >
            <span class="inbox-row__icon" :class="{ 'is-risk': item.kind === 'RISK_APPROVAL' }">
              <ShieldAlert v-if="item.kind === 'RISK_APPROVAL'" :size="17" aria-hidden="true" />
              <CircleHelp v-else :size="17" aria-hidden="true" />
            </span>
            <span class="inbox-row__body">
              <strong>{{ item.title }}</strong>
              <small>{{ item.kind === 'RISK_APPROVAL' ? '等待风险审批' : item.description }}</small>
            </span>
            <time :datetime="item.createdAt">{{ formatClock(item.createdAt) }}</time>
            <ChevronRight :size="15" aria-hidden="true" />
          </button>
        </div>

        <section v-if="runningSessions.length" class="running-list" aria-label="运行中的任务">
          <header><strong>运行中</strong><RouterLink to="/tasks">查看全部</RouterLink></header>
          <RouterLink v-for="session in runningSessions" :key="session.id" :to="`/tasks/${session.id}`">
            <span class="running-dot" aria-hidden="true" />
            <span><strong>{{ session.title }}</strong><small>{{ session.projectDisplay }}</small></span>
            <ChevronRight :size="14" aria-hidden="true" />
          </RouterLink>
        </section>

        <section v-if="recentSessions.length" class="running-list recent-list" aria-label="最近任务">
          <header><strong>最近</strong><RouterLink to="/tasks">查看全部</RouterLink></header>
          <RouterLink v-for="session in recentSessions" :key="session.id" :to="`/tasks/${session.id}`">
            <Clock3 :size="13" aria-hidden="true" />
            <span><strong>{{ session.title }}</strong><small>{{ session.projectDisplay }}</small></span>
            <ChevronRight :size="14" aria-hidden="true" />
          </RouterLink>
        </section>
      </section>

      <button
        v-if="sheetOpen"
        class="sheet-scrim"
        type="button"
        aria-label="关闭决策详情"
        @click="closeSheet"
      />

      <section
        v-if="selected"
        ref="sheetElement"
        id="decision-sheet"
        class="decision-workspace"
        :class="{ 'is-open': sheetOpen }"
        :role="sheetOpen ? 'dialog' : 'region'"
        :aria-modal="sheetOpen ? 'true' : undefined"
        aria-labelledby="decision-sheet-title"
        tabindex="-1"
        @keydown="onSheetKeydown"
      >
        <header class="decision-header">
          <div>
            <span :class="{ 'is-risk': selected.kind === 'RISK_APPROVAL' }">
              <ShieldAlert v-if="selected.kind === 'RISK_APPROVAL'" :size="16" />
              <CircleHelp v-else :size="16" />
              {{ selected.kind === 'RISK_APPROVAL' ? '风险审批' : 'Codex 提问' }}
            </span>
            <h2 id="decision-sheet-title">{{ selected.title }}</h2>
          </div>
          <button class="decision-close" type="button" aria-label="关闭决策详情" @click="closeSheet">
            <X :size="18" />
          </button>
          <RouterLink class="open-task" :to="`/tasks/${selected.sessionId}`">打开任务<ChevronRight :size="14" /></RouterLink>
        </header>

        <div class="decision-body">
          <section class="decision-request">
            <span>{{ selected.requestAction ? '请求操作' : '问题' }}</span>
            <p>{{ selected.requestAction ?? selected.description }}</p>
          </section>

          <details v-if="selected.risk" class="risk-disclosure">
            <summary>
              <AlertTriangle :size="16" aria-hidden="true" />
              <span>{{ selected.risk }}</span>
              <ChevronDown :size="16" aria-hidden="true" />
            </summary>
            <p>影响范围需要在执行前核对；当前审批只对本次请求生效。</p>
          </details>

          <section v-if="state.fixtureMode && selected.kind === 'RISK_APPROVAL'" class="evidence-panel">
            <header><strong>影响范围</strong><span>4 / 12 条预览</span></header>
            <div class="evidence-table" role="table" aria-label="受影响事件预览">
              <div role="row"><span role="cell" class="mono">evt_84b0</span><span role="cell">重复签名</span><StatusBadge tone="danger">DELETE</StatusBadge></div>
              <div role="row"><span role="cell" class="mono">evt_91ad</span><span role="cell">重复签名</span><StatusBadge tone="danger">DELETE</StatusBadge></div>
              <div role="row"><span role="cell" class="mono">evt_a3c2</span><span role="cell">格式待核对</span><StatusBadge tone="warning">REVIEW</StatusBadge></div>
              <div role="row"><span role="cell" class="mono">evt_b110</span><span role="cell">重复签名</span><StatusBadge tone="danger">DELETE</StatusBadge></div>
            </div>
          </section>

          <div class="decision-context mono">{{ selected.sessionId }} · {{ selected.turnId }}</div>
        </div>

        <footer class="decision-actions">
          <span v-if="!selectedAvailability.enabled">{{ selectedAvailability.reason }}</span>
          <UiButton
            v-for="option in selected.options"
            :key="option.id"
            :variant="option.emphasis === 'primary' ? 'primary' : 'secondary'"
            :disabled="!selectedAvailability.enabled"
            @click="respond(selected.id, option.id)"
          >
            {{ option.label }}
          </UiButton>
        </footer>
      </section>

      <aside v-if="selected" class="decision-inspector" aria-label="任务上下文">
        <div class="inspector-tabs">
          <strong>状态</strong>
          <RouterLink :to="`/tasks/${selected.sessionId}`">任务设置</RouterLink>
        </div>
        <dl>
          <div><dt>项目</dt><dd>{{ selectedSession?.projectDisplay || '—' }}</dd></div>
          <div><dt>轮次</dt><dd class="mono">{{ selected.turnId }}</dd></div>
          <div><dt>设备</dt><dd>{{ selectedPresence.displayName }}</dd></div>
          <div><dt>连接</dt><dd><span class="online-dot" />{{ selectedPresence.connection }}</dd></div>
          <div><dt>控制</dt><dd class="mono">{{ selectedPresence.controlMode }}</dd></div>
          <div><dt>模型</dt><dd>{{ selectedRuntime?.settings.model || '只读/未提供' }}</dd></div>
          <div><dt>权限</dt><dd>{{ selectedRuntime?.settings.permissionMode || '只读/未提供' }}</dd></div>
        </dl>
        <section v-if="state.fixtureMode" class="recent-activity">
          <header>最近活动</header>
          <div><Clock3 :size="13" /><span>识别重复事件签名</span><time>11:36</time></div>
          <div><Clock3 :size="13" /><span>生成删除查询</span><time>11:38</time></div>
          <div><ShieldAlert :size="13" /><span>暂停并请求审批</span><time>11:39</time></div>
        </section>
      </aside>
    </div>

    <section v-else class="attention-empty">
      <CheckCircle2 :size="32" aria-hidden="true" />
      <h1>已全部处理</h1>
      <p>新的问题或审批会出现在这里。</p>
      <UiButton variant="secondary" @click="$router.push('/tasks')">查看任务</UiButton>
    </section>
  </div>
</template>

<style scoped>
.attention-page,
.attention-layout {
  width: 100%;
  height: 100%;
  min-height: 0;
}

.attention-layout {
  display: grid;
  grid-template-columns: 326px minmax(480px, 1fr) 286px;
  overflow: hidden;
}

.inbox-pane,
.decision-workspace,
.decision-inspector {
  min-width: 0;
  min-height: 0;
  overflow: auto;
}

.inbox-pane {
  border-right: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.inbox-header,
.running-list > header,
.decision-header,
.decision-actions,
.evidence-panel > header,
.recent-activity > div {
  display: flex;
  align-items: center;
}

.inbox-header {
  min-height: 58px;
  justify-content: space-between;
  padding: 8px 12px 8px 16px;
  border-bottom: 1px solid var(--border-subtle);
}

.inbox-header > div {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.inbox-header h1 {
  margin: 0;
  font-size: 18px;
  letter-spacing: -0.02em;
}

.inbox-header span {
  color: var(--warning);
  font: 11px var(--font-mono);
}

.decision-close {
  display: grid;
  width: 36px;
  height: 36px;
  padding: 0;
  place-items: center;
  border: 0;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--text-muted);
}

.decision-close:hover {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.inbox-filters,
.inspector-tabs {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 2px;
  padding: 6px;
  border-bottom: 1px solid var(--border-subtle);
}

.inbox-filters button,
.inspector-tabs > * {
  min-height: 32px;
  border: 0;
  border-radius: 5px;
  background: transparent;
  color: var(--text-muted);
  font-size: 11px;
  font-weight: 650;
}

.inbox-filters button[aria-pressed='true'],
.inspector-tabs > strong {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.inbox-list {
  border-bottom: 1px solid var(--border-subtle);
}

.inbox-row {
  display: grid;
  width: 100%;
  min-height: 66px;
  grid-template-columns: auto minmax(0, 1fr) auto auto;
  align-items: center;
  gap: 9px;
  padding: 8px 10px 8px 12px;
  border: 0;
  border-bottom: 1px solid var(--border-subtle);
  background: transparent;
  color: var(--text-secondary);
  text-align: left;
}

.inbox-row:last-child {
  border-bottom: 0;
}

.inbox-row:hover {
  background: var(--bg-elevated);
}

.inbox-row.is-selected {
  background: var(--accent-soft);
  box-shadow: inset 1px 0 var(--accent);
}

.inbox-row__icon {
  display: grid;
  width: 30px;
  height: 30px;
  place-items: center;
  border-radius: var(--radius-control);
  background: var(--accent-soft);
  color: var(--accent);
}

.inbox-row__icon.is-risk {
  background: color-mix(in srgb, var(--warning), transparent 86%);
  color: var(--warning);
}

.inbox-row__body {
  display: grid;
  min-width: 0;
  gap: 2px;
}

.inbox-row__body strong,
.inbox-row__body small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.inbox-row__body strong {
  color: var(--text-primary);
  font-size: 12px;
}

.inbox-row__body small,
.inbox-row time {
  color: var(--text-muted);
  font-size: 10px;
}

.inbox-row > svg {
  color: var(--text-muted);
}

.running-list {
  padding-top: 10px;
}

.running-list > header {
  min-height: 32px;
  justify-content: space-between;
  padding: 0 13px;
  color: var(--text-muted);
  font-size: 10px;
}

.running-list > header a {
  color: var(--text-muted);
  text-decoration: none;
}

.running-list > a {
  display: grid;
  min-height: 54px;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 9px;
  padding: 7px 12px;
  color: var(--text-secondary);
  text-decoration: none;
}

.running-list > a:hover {
  background: var(--bg-elevated);
}

.recent-list {
  padding-top: 2px;
}

.recent-list > a > svg:first-child {
  color: var(--text-muted);
}

.running-list > a > span:nth-child(2) {
  display: grid;
  min-width: 0;
}

.running-list strong {
  overflow: hidden;
  color: var(--text-primary);
  font-size: 11px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.running-list small {
  color: var(--text-muted);
  font-size: 9px;
}

.running-dot,
.online-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--success);
}

.decision-workspace {
  display: grid;
  grid-template-rows: auto minmax(0, 1fr) auto;
  background: var(--bg-app);
}

.decision-header {
  min-height: 68px;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 18px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-surface);
}

.decision-header > div {
  min-width: 0;
}

.decision-header span {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--accent);
  font-size: 10px;
  font-weight: 700;
}

.decision-header span.is-risk {
  color: var(--warning);
}

.decision-header h2 {
  overflow: hidden;
  margin: 3px 0 0;
  font-size: 16px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.decision-close {
  display: none;
}

.open-task {
  display: inline-flex;
  min-height: 34px;
  align-items: center;
  gap: 3px;
  padding: 6px 8px;
  border-radius: var(--radius-control);
  color: var(--text-secondary);
  font-size: 11px;
  text-decoration: none;
}

.open-task:hover {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.decision-body {
  overflow: auto;
  padding: clamp(18px, 2.4vw, 34px);
}

.decision-request > span {
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 700;
}

.decision-request p {
  max-width: 760px;
  margin: 7px 0 18px;
  color: var(--text-primary);
  font-size: clamp(18px, 1.6vw, 24px);
  font-weight: 620;
  line-height: 1.45;
}

.risk-disclosure {
  max-width: 760px;
  border-top: 1px solid color-mix(in srgb, var(--warning), transparent 50%);
  border-bottom: 1px solid color-mix(in srgb, var(--warning), transparent 50%);
  background: color-mix(in srgb, var(--warning), transparent 92%);
}

.risk-disclosure summary {
  display: grid;
  min-height: 48px;
  grid-template-columns: auto 1fr auto;
  align-items: center;
  gap: 9px;
  padding: 9px 11px;
  color: var(--warning);
  list-style: none;
}

.risk-disclosure summary::-webkit-details-marker {
  display: none;
}

.risk-disclosure[open] summary > svg:last-child {
  transform: rotate(180deg);
}

.risk-disclosure p {
  padding: 0 36px 12px;
  margin: 0;
  color: var(--text-secondary);
  font-size: 12px;
}

.evidence-panel {
  max-width: 860px;
  margin-top: 28px;
  border-top: 1px solid var(--border-subtle);
  border-bottom: 1px solid var(--border-subtle);
}

.evidence-panel > header {
  min-height: 42px;
  justify-content: space-between;
  color: var(--text-secondary);
}

.evidence-panel > header span {
  color: var(--text-muted);
  font-size: 10px;
}

.evidence-table > div {
  display: grid;
  min-height: 40px;
  grid-template-columns: 110px 1fr auto;
  align-items: center;
  gap: 12px;
  border-top: 1px solid var(--border-subtle);
  color: var(--text-secondary);
  font-size: 11px;
}

.decision-context {
  margin-top: 16px;
  color: var(--text-muted);
  font-size: 10px;
}

.decision-actions {
  min-height: 62px;
  justify-content: flex-end;
  gap: 8px;
  padding: 9px 14px;
  border-top: 1px solid var(--border-subtle);
  background: var(--bg-surface);
}

.decision-actions > span {
  flex: 1;
  color: var(--warning);
  font-size: 10px;
}

.decision-inspector {
  border-left: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.inspector-tabs {
  grid-template-columns: repeat(2, 1fr);
}

.inspector-tabs > * {
  display: flex;
  align-items: center;
  justify-content: center;
  text-decoration: none;
}

.decision-inspector dl {
  padding: 8px 12px 14px;
  margin: 0;
}

.decision-inspector dl > div {
  display: grid;
  min-height: 38px;
  grid-template-columns: 76px minmax(0, 1fr);
  align-items: center;
  gap: 8px;
  border-bottom: 1px solid var(--border-subtle);
}

.decision-inspector dt,
.decision-inspector dd {
  font-size: 10px;
}

.decision-inspector dt {
  color: var(--text-muted);
}

.decision-inspector dd {
  display: flex;
  min-width: 0;
  align-items: center;
  justify-content: flex-end;
  gap: 6px;
  margin: 0;
  color: var(--text-secondary);
  overflow-wrap: anywhere;
  text-align: right;
}

.recent-activity {
  margin-top: 10px;
  border-top: 1px solid var(--border-subtle);
}

.recent-activity > header {
  padding: 12px;
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 700;
}

.recent-activity > div {
  min-height: 38px;
  gap: 7px;
  padding: 6px 12px;
  color: var(--text-secondary);
  font-size: 10px;
}

.recent-activity > div svg {
  color: var(--text-muted);
}

.recent-activity > div span {
  min-width: 0;
  flex: 1;
}

.recent-activity time {
  color: var(--text-muted);
}

.attention-empty {
  display: grid;
  max-width: 360px;
  min-height: 100%;
  align-content: center;
  justify-items: center;
  padding: 24px;
  margin: auto;
  text-align: center;
}

.attention-empty svg {
  color: var(--success);
}

.attention-empty h1 {
  margin: 10px 0 2px;
  font-size: 18px;
}

.attention-empty p {
  margin: 0 0 16px;
  color: var(--text-muted);
}

.sheet-scrim {
  display: none;
}

@media (max-width: 1120px) {
  .attention-layout {
    grid-template-columns: 310px minmax(440px, 1fr);
  }

  .decision-inspector {
    display: none;
  }
}

@media (max-width: 759px) {
  .attention-page {
    min-height: calc(100dvh - 56px - 62px - env(safe-area-inset-bottom));
  }

  .attention-layout {
    display: block;
    overflow: visible;
  }

  .inbox-pane {
    overflow: visible;
    border-right: 0;
  }

  .inbox-header {
    min-height: 50px;
  }

  .decision-close {
    width: 44px;
    height: 44px;
  }

  .inbox-filters {
    position: sticky;
    z-index: 2;
    top: 56px;
    background: var(--bg-nav);
  }

  .inbox-filters button {
    min-height: 44px;
  }

  .inbox-row {
    min-height: 68px;
    padding-right: 12px;
  }

  .running-list {
    padding: 10px 0 18px;
  }

  .sheet-scrim {
    position: fixed;
    z-index: calc(var(--layer-overlay) - 1);
    inset: 56px 0 calc(58px + env(safe-area-inset-bottom));
    display: block;
    width: 100%;
    border: 0;
    background: var(--scrim);
  }

  .decision-workspace {
    position: fixed;
    z-index: var(--layer-overlay);
    right: 0;
    bottom: calc(58px + env(safe-area-inset-bottom));
    left: 0;
    display: grid;
    max-height: min(68dvh, 610px);
    overflow: hidden;
    border-top: 1px solid var(--border-strong);
    border-radius: 14px 14px 0 0;
    box-shadow: 0 -12px 36px rgb(0 0 0 / 24%);
    visibility: hidden;
    transform: translateY(calc(100% + 12px));
    transition: transform 180ms cubic-bezier(0.2, 0.8, 0.2, 1);
  }

  .decision-workspace.is-open {
    visibility: visible;
    transform: translateY(0);
  }

  .decision-header {
    min-height: 64px;
    padding: 8px 8px 8px 14px;
  }

  .decision-header h2 {
    max-width: calc(100vw - 88px);
    font-size: 15px;
  }

  .decision-close {
    display: grid;
  }

  .open-task {
    display: none;
  }

  .decision-body {
    min-height: 0;
    overflow: auto;
    padding: 16px 14px;
  }

  .decision-request p {
    margin: 5px 0 13px;
    font-size: 16px;
    line-height: 1.45;
  }

  .risk-disclosure summary {
    min-height: 46px;
  }

  .risk-disclosure p {
    padding: 0 34px 10px;
  }

  .evidence-panel {
    margin-top: 16px;
  }

  .evidence-table > div {
    min-height: 36px;
    grid-template-columns: 86px 1fr auto;
    gap: 7px;
  }

  .decision-actions {
    min-height: 66px;
    padding: 9px 10px;
  }

  .decision-actions > .ui-button {
    flex: 1;
  }

  .decision-actions > span {
    flex-basis: 100%;
  }
}
</style>
