<script setup lang="ts">
import {
  Activity,
  ArrowLeft,
  ChevronRight,
  CircleAlert,
  CircleStop,
  FileCode2,
  Files,
  GitBranch,
  History,
  ListChecks,
  LoaderCircle,
  MessageCircleQuestion,
  MonitorDot,
  Settings2,
  X,
} from 'lucide-vue-next'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import NoticeBanner from '@/components/NoticeBanner.vue'
import QueueComposer from '@/components/QueueComposer.vue'
import SessionSettings from '@/components/SessionSettings.vue'
import StatusBadge from '@/components/StatusBadge.vue'
import TimelineItemCard from '@/components/TimelineItemCard.vue'
import UiButton from '@/components/UiButton.vue'
import { useBottomSheetFocus } from '@/composables/useBottomSheetFocus'
import { connectionLabel, phaseLabel, queueLabel } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'
import type { ControlOperation, RuntimeSnapshot } from '@/transport/types'

type MobileSection = 'activity' | 'plan' | 'files' | 'settings'

const route = useRoute()
const {
  state,
  initialize,
  ensureRuntime,
  loadOlderHistory,
  presenceFor,
  availability,
  sendCommand,
  answerAttention,
} = useConsoleStore()
const loading = ref(true)
const errorMessage = ref('')
const mobileSection = ref<MobileSection>('activity')
const mobileMedia = window.matchMedia('(max-width: 759px)')
const isMobileViewport = ref(mobileMedia.matches)
const contentPane = ref<HTMLElement | null>(null)
const attentionTrigger = ref<HTMLElement | null>(null)
const { sheetElement, sheetOpen, openSheet, closeSheet, onSheetKeydown } = useBottomSheetFocus(
  () => contentPane.value,
  '#attention-sheet',
)

const sessionId = computed(() => String(route.params.sessionId))
const session = computed(() => state.sessions.find((item) => item.id === sessionId.value))
const runtime = computed<RuntimeSnapshot | undefined>(() => state.runtimes[sessionId.value])
const presence = computed(() => presenceFor(sessionId.value))
const interruptAvailability = computed(() => availability(sessionId.value, 'INTERRUPT'))
const activityItems = computed(() =>
  runtime.value?.timeline.filter((item) => item.type !== 'plan' && item.type !== 'attention') ?? [],
)
const planItems = computed(() => runtime.value?.timeline.filter((item) => item.type === 'plan') ?? [])
const selectedAttention = computed(() => runtime.value?.attention[0])
const attentionOperation = computed<ControlOperation>(() =>
  selectedAttention.value?.kind === 'RISK_APPROVAL' ? 'ANSWER_APPROVAL' : 'ANSWER_QUESTION',
)
const attentionAvailability = computed(() =>
  selectedAttention.value
    ? availability(sessionId.value, attentionOperation.value)
    : { enabled: false, reason: '没有待处理项。' },
)
const backgroundCommands = computed(() =>
  runtime.value?.timeline.filter((item) => item.type === 'background-command') ?? [],
)
const lastReceipt = computed(() => state.receipts[0])

function syncMobileViewport(event: MediaQueryListEvent): void {
  isMobileViewport.value = event.matches
}

onMounted(() => mobileMedia.addEventListener('change', syncMobileViewport))
onBeforeUnmount(() => mobileMedia.removeEventListener('change', syncMobileViewport))

watch(
  sessionId,
  async (id) => {
    loading.value = true
    errorMessage.value = ''
    mobileSection.value = 'activity'
    sheetOpen.value = false
    try {
      await initialize()
      await ensureRuntime(id)
    } catch (error) {
      errorMessage.value = error instanceof Error ? error.message : '无法读取任务运行态。'
    } finally {
      loading.value = false
    }
  },
  { immediate: true },
)

async function answerCurrentAttention(optionId: string): Promise<void> {
  if (!selectedAttention.value) return
  await answerAttention(selectedAttention.value.id, optionId)
  await closeSheet()
}

function openAttentionSheet(): void {
  void openSheet(attentionTrigger.value)
}

function interruptTurn(): void {
  if (
    window.confirm(
      '确认中断当前 Codex 轮次？已经产生的输出会保留，但 Desktop 已启动的本机命令进程不一定同时停止。',
    )
  ) {
    void sendCommand(sessionId.value, 'INTERRUPT')
  }
}
</script>

<template>
  <div v-if="loading" class="task-loading" role="status">
    <LoaderCircle class="spin" :size="22" aria-hidden="true" />正在读取运行态…
  </div>

  <div v-else-if="errorMessage || !runtime" class="task-loading task-loading--error" role="alert">
    <CircleAlert :size="22" aria-hidden="true" />{{ errorMessage || '没有可用的任务快照。' }}
  </div>

  <div v-else class="task-detail-page">
    <header class="task-header">
      <div class="task-header__title">
        <RouterLink class="back-link" to="/tasks" aria-label="返回任务列表">
          <ArrowLeft :size="17" aria-hidden="true" />
        </RouterLink>
        <div>
          <div class="task-header__state">
            <span class="phase-dot" :class="{ 'is-running': runtime.phase === 'RUNNING' }" />
            {{ phaseLabel[runtime.phase] }}
            <span class="mono">{{ session?.nativeSessionId ?? sessionId }}</span>
          </div>
          <h1>{{ session?.title ?? sessionId }}</h1>
          <p><GitBranch :size="12" aria-hidden="true" />{{ session?.branch ?? 'unknown' }}</p>
        </div>
      </div>
      <div class="task-header__actions">
        <RouterLink class="git-link" :to="`/tasks/${sessionId}/git`" aria-label="查看文件与 Git">
          <FileCode2 :size="16" aria-hidden="true" /><span>文件与 Git</span>
        </RouterLink>
        <UiButton
          variant="danger"
          size="small"
          aria-label="中断当前 Codex 轮次"
          :disabled="runtime.phase !== 'RUNNING' || !interruptAvailability.enabled"
          :title="interruptAvailability.reason"
          @click="interruptTurn"
        >
          <template #icon><CircleStop aria-hidden="true" /></template>
          中断轮次
        </UiButton>
      </div>
    </header>

    <NoticeBanner
      v-if="presence.connection !== 'ONLINE'"
      class="connection-notice"
      tone="danger"
      title="设备离线"
    >
      当前为只读快照，恢复连接后重新同步。
    </NoticeBanner>

    <NoticeBanner
      v-else-if="presence.compatibility !== 'VERIFIED' || presence.controlMode === 'READ_ONLY'"
      class="connection-notice"
      tone="warning"
      title="当前版本仅支持读取"
    >
      写操作由 Bridge capability 关闭；不会根据版本字符串猜测开放。
    </NoticeBanner>

    <nav class="mobile-task-tabs" aria-label="任务工作区">
      <button type="button" :aria-pressed="mobileSection === 'activity'" @click="mobileSection = 'activity'">
        <Activity :size="16" />动态
      </button>
      <button type="button" :aria-pressed="mobileSection === 'plan'" @click="mobileSection = 'plan'">
        <ListChecks :size="16" />计划
      </button>
      <button type="button" :aria-pressed="mobileSection === 'files'" @click="mobileSection = 'files'">
        <Files :size="16" />文件
      </button>
      <button type="button" :aria-pressed="mobileSection === 'settings'" @click="mobileSection = 'settings'">
        <Settings2 :size="16" />设置
      </button>
    </nav>

    <div class="task-workbench">
      <section ref="contentPane" class="content-pane" aria-label="任务内容" tabindex="-1">
        <div class="content-toolbar">
          <strong>动态</strong>
          <UiButton
            v-if="state.historyCursors[sessionId]"
            variant="quiet"
            size="small"
            :disabled="Boolean(state.historyLoading[sessionId])"
            @click="loadOlderHistory(sessionId)"
          >
            <template #icon>
              <LoaderCircle v-if="state.historyLoading[sessionId]" class="spin" aria-hidden="true" />
              <History v-else aria-hidden="true" />
            </template>
            {{ state.historyLoading[sessionId] ? '读取中' : '更早记录' }}
          </UiButton>
        </div>

        <div v-if="!isMobileViewport" class="desktop-timeline">
          <TimelineItemCard
            v-for="item in runtime.timeline"
            :key="item.id"
            :item="item"
            :session-id="sessionId"
          />
        </div>

        <div v-if="isMobileViewport" v-show="mobileSection === 'activity'" class="mobile-section mobile-activity">
          <TimelineItemCard
            v-for="item in activityItems"
            :key="item.id"
            :item="item"
            :session-id="sessionId"
          />
        </div>

        <div v-if="isMobileViewport" v-show="mobileSection === 'plan'" class="mobile-section mobile-plan">
          <TimelineItemCard
            v-for="item in planItems"
            :key="item.id"
            :item="item"
            :session-id="sessionId"
          />
          <p v-if="!planItems.length" class="section-empty">当前没有计划。</p>
        </div>

        <div v-if="isMobileViewport" v-show="mobileSection === 'files'" class="mobile-section mobile-files">
          <RouterLink v-if="state.fixtureMode" :to="`/tasks/${sessionId}/git`">
            <FileCode2 :size="17" /><span><strong>TaskDetailView.vue</strong><small>已修改 · +286</small></span><ChevronRight :size="15" />
          </RouterLink>
          <RouterLink v-if="state.fixtureMode" :to="`/tasks/${sessionId}/git`">
            <FileCode2 :size="17" /><span><strong>output-reducer.ts</strong><small>已修改 · +31 −5</small></span><ChevronRight :size="15" />
          </RouterLink>
          <RouterLink v-if="state.fixtureMode" :to="`/tasks/${sessionId}/git`">
            <FileCode2 :size="17" /><span><strong>tokens.css</strong><small>新增 · +78</small></span><ChevronRight :size="15" />
          </RouterLink>
          <RouterLink v-if="!state.fixtureMode" :to="`/tasks/${sessionId}/git`">
            <FileCode2 :size="17" /><span><strong>打开文件与 Git</strong><small>在线按需读取，不缓存本机正文</small></span><ChevronRight :size="15" />
          </RouterLink>
        </div>

        <div v-if="isMobileViewport" v-show="mobileSection === 'settings'" class="mobile-section mobile-settings">
          <section class="mobile-runtime">
            <div><span>连接</span><strong>{{ connectionLabel[presence.connection] }}</strong></div>
            <div><span>控制</span><strong class="mono">{{ presence.controlMode }}</strong></div>
            <div><span>修订</span><strong class="mono">{{ runtime.runtimeRevision }}</strong></div>
            <div><span>下一轮</span><strong>{{ queueLabel[runtime.queue.status] }}</strong></div>
          </section>
          <SessionSettings :session-id="sessionId" :runtime="runtime" />
        </div>

        <button
          v-if="selectedAttention && mobileSection === 'activity'"
          ref="attentionTrigger"
          class="mobile-attention-strip"
          type="button"
          @click="openAttentionSheet"
        >
          <MessageCircleQuestion :size="16" />
          <span>{{ selectedAttention.description }}</span>
          <ChevronRight :size="16" />
        </button>

        <QueueComposer
          v-if="mobileSection === 'activity'"
          :session-id="sessionId"
          :queue="runtime.queue"
          :phase="runtime.phase"
        />
      </section>

      <aside class="task-inspector" aria-label="任务检查器">
        <section class="runtime-overview">
          <header><MonitorDot :size="16" aria-hidden="true" /><strong>运行态</strong></header>
          <dl>
            <div><dt>设备</dt><dd>{{ presence.displayName }}</dd></div>
            <div><dt>连接</dt><dd><StatusBadge :tone="presence.connection === 'ONLINE' ? 'success' : 'danger'" dot>{{ connectionLabel[presence.connection] }}</StatusBadge></dd></div>
            <div><dt>控制</dt><dd class="mono">{{ presence.controlMode }}</dd></div>
            <div><dt>兼容性</dt><dd class="mono">{{ presence.compatibility }}</dd></div>
            <div><dt>运行修订</dt><dd class="mono">{{ runtime.runtimeRevision }}</dd></div>
            <div><dt>下一轮</dt><dd>{{ queueLabel[runtime.queue.status] }}</dd></div>
          </dl>
        </section>

        <SessionSettings :session-id="sessionId" :runtime="runtime" />

        <section class="inspector-rows">
          <header><Activity :size="16" /><strong>进程与回执</strong></header>
          <div><span>后台命令</span><strong>{{ backgroundCommands.length }}</strong></div>
          <div><span>最近回执</span><strong class="mono">{{ lastReceipt?.status ?? '—' }}</strong></div>
        </section>
      </aside>
    </div>

    <button
      v-if="sheetOpen"
      class="attention-sheet-scrim"
      type="button"
      aria-label="关闭提问"
      @click="closeSheet"
    />
    <section
      v-if="selectedAttention"
      ref="sheetElement"
      id="attention-sheet"
      class="attention-sheet"
      :class="{ 'is-open': sheetOpen }"
      role="dialog"
      aria-modal="true"
      :aria-hidden="sheetOpen ? undefined : 'true'"
      aria-labelledby="attention-sheet-title"
      tabindex="-1"
      @keydown="onSheetKeydown"
    >
      <header><strong id="attention-sheet-title">Codex 提问</strong><button type="button" aria-label="关闭提问" @click="closeSheet"><X :size="18" /></button></header>
      <p>{{ selectedAttention.description }}</p>
      <footer>
        <UiButton
          v-for="option in selectedAttention.options"
          :key="option.id"
          variant="secondary"
          :disabled="!attentionAvailability.enabled"
          @click="answerCurrentAttention(option.id)"
        >{{ option.label }}</UiButton>
      </footer>
    </section>
  </div>
</template>

<style scoped>
.task-loading {
  display: flex;
  min-height: 100%;
  align-items: center;
  justify-content: center;
  gap: 10px;
  color: var(--text-secondary);
}

.task-loading--error {
  color: var(--danger);
}

.task-detail-page {
  display: grid;
  height: 100%;
  min-height: 0;
  grid-template-rows: auto auto auto minmax(0, 1fr);
}

.task-header,
.task-header__title,
.task-header__actions,
.task-header__state,
.task-header__title p,
.git-link,
.content-toolbar,
.runtime-overview > header,
.inspector-rows > header,
.inspector-rows > div,
.attention-sheet > header,
.attention-sheet footer {
  display: flex;
  align-items: center;
}

.task-header {
  min-height: 70px;
  justify-content: space-between;
  gap: 16px;
  padding: 9px 16px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-surface);
}

.task-header__title {
  min-width: 0;
  gap: 9px;
}

.back-link {
  display: none;
  flex: 0 0 auto;
  place-items: center;
  border-radius: var(--radius-control);
  color: var(--text-secondary);
}

.task-header__title > div {
  min-width: 0;
}

.task-header__state {
  gap: 6px;
  color: var(--text-secondary);
  font-size: 10px;
}

.task-header__state .mono {
  color: var(--text-muted);
}

.phase-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--text-muted);
}

.phase-dot.is-running {
  background: var(--success);
}

.task-header h1 {
  overflow: hidden;
  margin: 2px 0 0;
  color: var(--text-primary);
  font-size: 15px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-header__title p {
  gap: 4px;
  margin: 2px 0 0;
  color: var(--text-muted);
  font-size: 10px;
}

.task-header__actions {
  flex: 0 0 auto;
  gap: 6px;
}

.git-link {
  min-height: 32px;
  gap: 6px;
  padding: 5px 9px;
  border-radius: var(--radius-control);
  color: var(--text-secondary);
  font-size: 11px;
  font-weight: 650;
  text-decoration: none;
}

.git-link:hover {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.connection-notice {
  margin: 10px 12px 0;
}

.mobile-task-tabs {
  display: none;
}

.task-workbench {
  display: grid;
  min-height: 0;
  grid-template-columns: minmax(420px, 1fr) var(--inspector-width);
}

.content-pane,
.task-inspector {
  min-width: 0;
  min-height: 0;
  overflow: auto;
}

.content-pane {
  position: relative;
  display: grid;
  grid-template-rows: auto minmax(0, 1fr) auto;
  overflow: hidden;
  background: var(--bg-app);
}

.content-toolbar {
  min-height: 46px;
  justify-content: space-between;
  padding: 6px 14px 6px 18px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.content-toolbar strong {
  font-size: 12px;
}

.desktop-timeline {
  display: grid;
  align-content: start;
  gap: 8px;
  padding: 12px clamp(12px, 1.6vw, 24px);
  overflow: auto;
}

.content-pane > .queue-composer {
  z-index: var(--layer-sticky);
  margin: 0 14px 12px;
}

.mobile-section,
.mobile-attention-strip {
  display: none;
}

.task-inspector {
  display: grid;
  align-content: start;
  gap: 8px;
  padding: 10px;
  overflow-x: hidden;
  border-left: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.runtime-overview,
.inspector-rows {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.runtime-overview > header,
.inspector-rows > header {
  min-height: 40px;
  gap: 7px;
  padding: 7px 10px;
  border-bottom: 1px solid var(--border-subtle);
}

.runtime-overview > header svg,
.inspector-rows > header svg {
  color: var(--accent);
}

.runtime-overview dl {
  padding: 5px 10px 10px;
  margin: 0;
}

.runtime-overview dl > div,
.inspector-rows > div {
  display: grid;
  min-height: 34px;
  grid-template-columns: 82px minmax(0, 1fr);
  align-items: center;
  gap: 8px;
  border-bottom: 1px solid var(--border-subtle);
}

.runtime-overview dl > div:last-child,
.inspector-rows > div:last-child {
  border-bottom: 0;
}

.runtime-overview dt,
.runtime-overview dd,
.inspector-rows > div {
  font-size: 10px;
}

.runtime-overview dt,
.inspector-rows span {
  color: var(--text-muted);
}

.runtime-overview dd,
.inspector-rows strong {
  min-width: 0;
  margin: 0;
  color: var(--text-secondary);
  overflow-wrap: anywhere;
  text-align: right;
}

.inspector-rows > div {
  display: flex;
  justify-content: space-between;
  padding: 0 10px;
}

.attention-sheet,
.attention-sheet-scrim {
  display: none;
}

.spin {
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

@media (max-width: 759px) {
  .task-detail-page {
    height: auto;
    min-height: calc(100dvh - 56px - 62px - env(safe-area-inset-bottom));
  }

  .task-workbench {
    display: block;
  }

  .task-inspector,
  .desktop-timeline,
  .content-toolbar {
    display: none;
  }

  .mobile-task-tabs {
    position: sticky;
    z-index: 5;
    top: 56px;
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    border-bottom: 1px solid var(--border-subtle);
    background: var(--bg-nav);
  }

  .mobile-task-tabs button {
    display: flex;
    min-height: 46px;
    align-items: center;
    justify-content: center;
    gap: 5px;
    border: 0;
    border-bottom: 2px solid transparent;
    background: transparent;
    color: var(--text-muted);
    font-size: 11px;
  }

  .mobile-task-tabs button[aria-pressed='true'] {
    border-bottom-color: var(--accent);
    color: var(--text-primary);
  }

  .content-pane {
    display: block;
    min-height: calc(100dvh - 172px - env(safe-area-inset-bottom));
    overflow: visible;
  }

  .mobile-section {
    display: grid;
    align-content: start;
    min-height: 0;
  }

  .mobile-activity,
  .mobile-plan {
    padding: 0 10px 126px;
  }

  .mobile-files > a {
    display: grid;
    min-height: 64px;
    grid-template-columns: auto minmax(0, 1fr) auto;
    align-items: center;
    gap: 10px;
    padding: 8px 14px;
    border-bottom: 1px solid var(--border-subtle);
    color: var(--text-secondary);
    text-decoration: none;
  }

  .mobile-files > a > svg:first-child {
    color: var(--accent);
  }

  .mobile-files > a > span {
    display: grid;
    min-width: 0;
  }

  .mobile-files strong {
    overflow: hidden;
    color: var(--text-primary);
    font-size: 12px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .mobile-files small {
    color: var(--text-muted);
    font-size: 10px;
  }

  .mobile-settings {
    gap: 10px;
    padding: 10px 10px 24px;
  }

  .mobile-runtime {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    border-top: 1px solid var(--border-subtle);
    border-bottom: 1px solid var(--border-subtle);
  }

  .mobile-runtime > div {
    display: flex;
    min-height: 44px;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 0 10px;
    border-right: 1px solid var(--border-subtle);
    border-bottom: 1px solid var(--border-subtle);
  }

  .mobile-runtime span,
  .mobile-runtime strong {
    font-size: 10px;
  }

  .mobile-runtime span { color: var(--text-muted); }
  .mobile-runtime strong { color: var(--text-secondary); }

  .mobile-attention-strip {
    position: fixed;
    z-index: calc(var(--layer-sticky) + 1);
    right: 0;
    bottom: calc(112px + env(safe-area-inset-bottom));
    left: 0;
    display: grid;
    min-height: 44px;
    grid-template-columns: auto minmax(0, 1fr) auto;
    align-items: center;
    gap: 8px;
    padding: 7px 12px;
    border: 0;
    border-top: 1px solid color-mix(in srgb, var(--warning), transparent 48%);
    border-bottom: 1px solid color-mix(in srgb, var(--warning), transparent 48%);
    background: color-mix(in srgb, var(--warning), var(--bg-nav) 88%);
    color: var(--warning);
    text-align: left;
  }

  .mobile-attention-strip span {
    overflow: hidden;
    color: var(--text-primary);
    font-size: 11px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .content-pane > .queue-composer {
    position: fixed;
    z-index: var(--layer-sticky);
    right: 0;
    bottom: calc(58px + env(safe-area-inset-bottom));
    left: 0;
    margin: 0;
  }

  .section-empty {
    padding: 32px 14px;
    margin: 0;
    color: var(--text-muted);
    text-align: center;
  }

  .attention-sheet-scrim {
    position: fixed;
    z-index: calc(var(--layer-overlay) - 1);
    inset: 56px 0 calc(58px + env(safe-area-inset-bottom));
    display: block;
    width: 100%;
    border: 0;
    background: var(--scrim);
  }

  .attention-sheet {
    position: fixed;
    z-index: var(--layer-overlay);
    right: 0;
    bottom: calc(58px + env(safe-area-inset-bottom));
    left: 0;
    display: grid;
    padding: 0 14px 12px;
    border-top: 1px solid var(--border-strong);
    border-radius: 14px 14px 0 0;
    background: var(--bg-elevated);
    box-shadow: 0 -12px 36px rgb(0 0 0 / 24%);
    visibility: hidden;
    transform: translateY(calc(100% + 12px));
    transition: transform 180ms cubic-bezier(0.2, 0.8, 0.2, 1);
  }

  .attention-sheet.is-open {
    visibility: visible;
    transform: translateY(0);
  }

  .attention-sheet > header {
    min-height: 54px;
    justify-content: space-between;
    border-bottom: 1px solid var(--border-subtle);
  }

  .attention-sheet > header button {
    display: grid;
    width: 44px;
    height: 44px;
    padding: 0;
    place-items: center;
    border: 0;
    border-radius: var(--radius-control);
    background: transparent;
    color: var(--text-muted);
  }

  .attention-sheet > p {
    margin: 16px 0 18px;
    color: var(--text-primary);
    font-size: 16px;
  }

  .attention-sheet footer {
    gap: 8px;
  }

  .attention-sheet footer > .ui-button {
    flex: 1;
  }
}

@media (max-width: 599px) {
  .task-header {
    min-height: 64px;
    padding: 8px 10px;
  }

  .back-link,
  .git-link,
  .task-header__actions > .ui-button {
    display: grid;
    width: 44px;
    height: 44px;
    padding: 0;
    place-items: center;
  }

  .git-link span,
  .task-header__actions > .ui-button :deep(span) {
    display: none;
  }

  .task-header h1 {
    max-width: calc(100vw - 184px);
  }
}
</style>
