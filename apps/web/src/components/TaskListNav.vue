<script setup lang="ts">
import { Archive, Bell, GitBranch, LoaderCircle, Pin, Plus } from 'lucide-vue-next'

import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import { formatClock, phaseLabel } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'

const { state, loadMoreSessions } = useConsoleStore()
</script>

<template>
  <nav class="task-nav" aria-label="任务列表">
    <div class="task-nav__heading">
      <div>
        <span class="task-nav__eyebrow">工作区</span>
        <RouterLink to="/tasks">任务</RouterLink>
      </div>
      <UiButton variant="quiet" size="small" title="新任务由 Codex Desktop 创建" disabled>
        <template #icon><Plus aria-hidden="true" /></template>
        <span class="sr-only">新任务</span>
      </UiButton>
    </div>

    <div class="task-nav__list">
      <RouterLink
        v-for="session in state.sessions"
        :key="session.id"
        :to="`/tasks/${session.id}`"
        class="task-link"
      >
        <div class="task-link__topline">
          <span class="visually-clamped">{{ session.title }}</span>
          <Pin v-if="session.pinned" :size="12" aria-label="已固定" />
          <Archive v-if="session.archived" :size="12" aria-label="已归档" />
        </div>
        <div class="task-link__meta">
          <span><GitBranch :size="12" aria-hidden="true" />{{ session.branch }}</span>
          <time :datetime="session.updatedAt">{{ formatClock(session.updatedAt) }}</time>
        </div>
        <div class="task-link__state">
          <StatusBadge :tone="session.phase === 'RUNNING' ? 'success' : 'neutral'" dot>
            {{ phaseLabel[session.phase] }}
          </StatusBadge>
          <span v-if="session.attentionCount" class="task-link__attention">
            <Bell :size="12" aria-hidden="true" />{{ session.attentionCount }}
          </span>
        </div>
      </RouterLink>
    </div>

    <UiButton
      v-if="state.sessionsCursor"
      class="task-nav__more"
      variant="quiet"
      size="small"
      :disabled="state.sessionsLoading"
      @click="loadMoreSessions"
    >
      <template #icon>
        <LoaderCircle v-if="state.sessionsLoading" class="spin" aria-hidden="true" />
      </template>
      {{ state.sessionsLoading ? '读取中' : '加载更多任务' }}
    </UiButton>
  </nav>
</template>

<style scoped>
.task-nav {
  display: flex;
  min-height: 0;
  flex: 1;
  flex-direction: column;
}

.task-nav__heading {
  display: flex;
  min-height: 66px;
  align-items: center;
  justify-content: space-between;
  padding: 12px 14px;
  border-bottom: 1px solid var(--border-subtle);
}

.task-nav__heading > div {
  display: grid;
}

.task-nav__heading a {
  color: var(--text-primary);
  font-size: 17px;
  font-weight: 700;
  text-decoration: none;
}

.task-nav__eyebrow {
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.task-nav__list {
  min-height: 0;
  flex: 1;
  overflow-y: auto;
  padding: 6px;
}

.task-link {
  display: grid;
  gap: 7px;
  margin-bottom: 2px;
  padding: 10px;
  border: 1px solid transparent;
  border-radius: var(--radius-control);
  color: var(--text-secondary);
  text-decoration: none;
}

.task-link:hover {
  background: var(--bg-elevated);
}

.task-link.router-link-active {
  border-color: var(--border-subtle);
  background: var(--accent-soft);
}

.task-link__topline {
  display: flex;
  min-width: 0;
  align-items: flex-start;
  gap: 6px;
  color: var(--text-primary);
  font-size: 13px;
  font-weight: 650;
}

.task-link__topline > span {
  flex: 1;
  -webkit-line-clamp: 1;
}

.task-link__topline svg {
  flex: 0 0 auto;
  color: var(--text-muted);
}

.task-link__meta,
.task-link__state {
  display: flex;
  min-width: 0;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  color: var(--text-muted);
  font-size: 11px;
}

.task-link__meta span {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 4px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.task-link__attention {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  color: var(--warning);
  font-weight: 700;
}

.task-nav__more {
  margin: 8px;
}

.spin {
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
