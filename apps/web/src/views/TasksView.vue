<script setup lang="ts">
import { Bell, ChevronRight, GitBranch, LoaderCircle, Pin } from 'lucide-vue-next'
import { computed, watch } from 'vue'
import { useRoute } from 'vue-router'

import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import { formatDateTime, outcomeLabel, phaseLabel, queueLabel } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'

const route = useRoute()
const { state, loadMoreSessions } = useConsoleStore()
const query = computed(() => String(route.query.q ?? '').trim().toLocaleLowerCase('zh-CN'))
const sessions = computed(() =>
  state.sessions.filter((session) => {
    if (!query.value) return true
    return [session.title, session.projectDisplay, session.branch].some((value) =>
      value.toLocaleLowerCase('zh-CN').includes(query.value),
    )
  }),
)
/** 搜索尚未覆盖全部页:此时"没有匹配"会是误导性结论,继续静默取下一页。 */
const searchingMore = computed(() => Boolean(query.value) && Boolean(state.sessionsCursor))
const runningCount = computed(() => state.sessions.filter((session) => session.phase === 'RUNNING').length)
const attentionCount = computed(() => state.sessions.reduce((total, session) => total + session.attentionCount, 0))

// 搜索只过滤已加载数据,未加载页可能仍有匹配:搜索期间按游标链式加载
// 剩余页(每次状态变化拉一页,loading 守卫防止并发重入),直到游标耗尽。
watch(
  () => [query.value, state.sessionsCursor] as const,
  ([currentQuery]) => {
    if (currentQuery && state.sessionsCursor && !state.sessionsLoading) {
      void loadMoreSessions().catch(() => undefined)
    }
  },
  { immediate: true },
)
</script>

<template>
  <div class="tasks-page">
    <header class="tasks-page__header">
      <div>
        <h1>任务</h1>
        <span v-if="query">“{{ route.query.q }}”</span>
      </div>
      <div class="task-summary">
        <span><i class="is-running" />{{ runningCount }} 运行中</span>
        <span><i class="needs-attention" />{{ attentionCount }} 待处理</span>
        <span>{{ state.sessions.length }} 个任务</span>
      </div>
    </header>

    <section class="tasks-table-wrap" aria-labelledby="task-list-title">
      <div class="tasks-table-heading">
        <h2 id="task-list-title">最近任务</h2>
        <span>{{ sessions.length }} 项</span>
      </div>

      <div class="tasks-table" aria-label="最近任务">
        <div class="tasks-table__row tasks-table__head">
          <span>任务</span>
          <span>运行状态</span>
          <span>下一轮</span>
          <span>最后结果</span>
          <span>更新</span>
          <span aria-hidden="true" />
        </div>
        <RouterLink
          v-for="session in sessions"
          :key="session.id"
          :to="`/tasks/${session.id}`"
          class="tasks-table__row"
        >
          <span class="task-name">
            <strong>{{ session.title }}<Pin v-if="session.pinned" :size="12" aria-label="已固定" /></strong>
            <small><GitBranch :size="12" aria-hidden="true" />{{ session.projectDisplay }} · {{ session.branch }}</small>
          </span>
          <span class="task-meta">
            <span class="task-state">
              <StatusBadge :tone="session.phase === 'RUNNING' ? 'success' : 'neutral'" dot>{{ phaseLabel[session.phase] }}</StatusBadge>
              <StatusBadge v-if="session.attentionCount" tone="warning"><Bell :size="11" />{{ session.attentionCount }}</StatusBadge>
            </span>
            <span><StatusBadge :tone="session.queueStatus === 'PAUSED' ? 'warning' : 'neutral'">{{ queueLabel[session.queueStatus] }}</StatusBadge></span>
            <span><StatusBadge :tone="session.lastOutcome === 'UNKNOWN' ? 'warning' : session.lastOutcome === 'FAILED' ? 'danger' : 'neutral'">{{ outcomeLabel[session.lastOutcome] }}</StatusBadge></span>
            <time :datetime="session.updatedAt">{{ formatDateTime(session.updatedAt) }}</time>
          </span>
          <ChevronRight :size="16" aria-hidden="true" />
        </RouterLink>
      </div>

      <div v-if="!sessions.length && searchingMore" class="tasks-empty">
        正在搜索更多任务…
      </div>
      <div v-else-if="!sessions.length" class="tasks-empty">没有匹配的任务。</div>

      <UiButton
        v-if="state.sessionsCursor"
        class="load-more"
        variant="secondary"
        :disabled="state.sessionsLoading"
        @click="loadMoreSessions"
      >
        <template #icon><LoaderCircle v-if="state.sessionsLoading" class="spin" aria-hidden="true" /></template>
        {{ state.sessionsLoading ? '读取中' : '加载更多任务' }}
      </UiButton>
    </section>
  </div>
</template>

<style scoped>
.tasks-page {
  width: min(100%, var(--content-max));
  min-height: 100%;
  padding: 0 var(--page-gutter) 40px;
  margin: 0 auto;
}

.tasks-page__header {
  display: flex;
  min-height: 66px;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  border-bottom: 1px solid var(--border-subtle);
}

.tasks-page__header > div:first-child {
  display: flex;
  align-items: center;
  gap: 10px;
}

.tasks-page__header h1 {
  margin: 0;
  font-size: 18px;
}

.tasks-page__header > div:first-child > span {
  color: var(--text-muted);
  font-size: 11px;
}

.task-summary {
  display: flex;
  align-items: center;
  gap: 16px;
}

.task-summary span {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: var(--text-muted);
  font-size: 11px;
}

.task-summary i {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--text-muted);
}

.task-summary i.is-running {
  background: var(--success);
}

.task-summary i.needs-attention {
  background: var(--warning);
}

.tasks-table-wrap {
  padding-top: 22px;
}

.tasks-table-heading {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  margin-bottom: 10px;
}

.tasks-table-heading h2 {
  margin: 0;
  font-size: 15px;
}

.tasks-table-heading span {
  color: var(--text-muted);
  font-size: 11px;
}

.tasks-table {
  overflow: hidden;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.tasks-table__row {
  display: grid;
  min-height: 66px;
  grid-template-columns: minmax(240px, 1.6fr) minmax(150px, 0.75fr) minmax(130px, 0.65fr) minmax(120px, 0.6fr) 106px 20px;
  align-items: center;
  gap: 12px;
  padding: 9px 13px;
  border-bottom: 1px solid var(--border-subtle);
  color: var(--text-secondary);
  text-decoration: none;
}

.tasks-table__row:last-child {
  border-bottom: 0;
}

a.tasks-table__row:hover {
  background: var(--bg-surface);
}

.tasks-table__head {
  min-height: 36px;
  background: var(--bg-nav);
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 750;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.task-meta {
  display: contents;
}

.task-meta > span {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}

.task-name {
  display: grid;
  min-width: 0;
  gap: 5px;
}

.task-name strong,
.task-name small {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 6px;
}

.task-name strong {
  overflow: hidden;
  color: var(--text-primary);
  font-size: 13px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-name small,
.tasks-table__row time {
  overflow: hidden;
  color: var(--text-muted);
  font-size: 10px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tasks-table__row > svg {
  color: var(--text-muted);
}

.tasks-empty {
  padding: 48px;
  color: var(--text-muted);
  text-align: center;
}

.load-more {
  display: flex;
  margin: 16px auto 0;
}

.spin {
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 900px) {
  .tasks-table__head {
    display: none;
  }

  .tasks-table {
    display: grid;
    gap: 0;
    border-right: 0;
    border-left: 0;
    border-radius: 0;
    background: transparent;
  }

  .tasks-table__row {
    min-height: 0;
    grid-template-columns: 1fr auto;
    gap: 9px;
    padding: 12px 2px;
    border: 0;
    border-bottom: 1px solid var(--border-subtle);
    border-radius: 0;
    background: transparent;
  }

  .task-name {
    grid-column: 1;
  }

  .task-meta {
    display: flex;
    min-width: 0;
    grid-column: 1;
    align-items: center;
    gap: 5px;
    overflow: hidden;
  }

  .task-meta > span {
    flex: 0 0 auto;
    flex-wrap: nowrap;
  }

  .task-meta time {
    min-width: 0;
    margin-left: auto;
    text-overflow: ellipsis;
  }

  .tasks-table__row > svg {
    grid-column: 2;
    grid-row: 1 / 3;
  }
}

@media (max-width: 759px) {
  .tasks-page {
    padding: 0 12px 32px;
  }

  .tasks-page__header {
    min-height: 56px;
    padding: 0 4px;
  }

  .task-summary span:last-child {
    display: none;
  }

  .tasks-table-wrap {
    padding-top: 16px;
  }

  .tasks-table__row {
    min-height: 76px;
    row-gap: 7px;
    padding: 9px 2px;
  }

  .task-meta :deep(.status-badge) {
    padding-right: 6px;
    padding-left: 6px;
    font-size: 9px;
  }
}
</style>
