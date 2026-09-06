<script setup lang="ts">
import {
  Bell,
  ListTodo,
  Menu,
  MonitorSmartphone,
  Moon,
  Search,
  Sun,
  TerminalSquare,
  Wifi,
  WifiOff,
  X,
} from 'lucide-vue-next'
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import DeviceSettingsNav from '@/components/DeviceSettingsNav.vue'
import NoticeBanner from '@/components/NoticeBanner.vue'
import StatusBadge from '@/components/StatusBadge.vue'
import TaskListNav from '@/components/TaskListNav.vue'
import UiButton from '@/components/UiButton.vue'
import { useTheme } from '@/composables/useTheme'
import { connectionExplanation } from '@/lib/diagnostics'
import { connectionLabel } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'

const route = useRoute()
const router = useRouter()
const { theme, toggleTheme } = useTheme()
const { state, pendingAttention, toggleConnection, retryConnection } = useConsoleStore()
const navDialog = ref<HTMLDialogElement | null>(null)
const mainElement = ref<HTMLElement | null>(null)
const searchInput = ref<HTMLInputElement | null>(null)
const searchQuery = ref('')

const section = computed<'tasks' | 'devices' | undefined>(() => {
  if (route.path.startsWith('/tasks')) return 'tasks'
  if (route.path.startsWith('/devices')) return 'devices'
  return undefined
})

const connectionTone = computed(() => {
  if (state.connection === 'ONLINE') return 'success'
  if (state.connection === 'CONNECTING') return 'warning'
  return 'danger'
})

/** 浏览器→Relay 链路的用户可读说明(IN-01/UX-02);正常在线时不打扰。 */
const linkBanner = computed(() => {
  if (state.link.state === 'ONLINE' && !state.startupError) return undefined
  return connectionExplanation({
    link: state.link,
    deviceConnection: 'ONLINE',
    controlMode: 'LIMITED_CONTROL',
    compatibility: 'VERIFIED',
    supportsNativeQuestion: true,
  })
})

function openNavigation(): void {
  navDialog.value?.showModal()
}

function closeNavigation(): void {
  navDialog.value?.close()
}

function submitSearch(): void {
  const query = searchQuery.value.trim()
  void router.push(query ? { path: '/tasks', query: { q: query } } : '/tasks')
}

function handleGlobalShortcut(event: KeyboardEvent): void {
  if (event.key.toLocaleLowerCase() === 'k' && (event.metaKey || event.ctrlKey)) {
    event.preventDefault()
    searchInput.value?.focus()
  }
}

onMounted(() => window.addEventListener('keydown', handleGlobalShortcut))
onBeforeUnmount(() => window.removeEventListener('keydown', handleGlobalShortcut))

watch(
  () => route.fullPath,
  async () => {
    closeNavigation()
    await nextTick()
    mainElement.value?.focus({ preventScroll: true })
  },
)
</script>

<template>
  <a class="skip-link" href="#main-content">跳到主要内容</a>
  <div class="app-shell">
    <header class="global-header">
      <div class="brand-lockup">
        <button v-if="section" class="menu-button" type="button" aria-label="打开任务导航" @click="openNavigation">
          <Menu :size="20" aria-hidden="true" />
        </button>
        <RouterLink class="brand-mark" to="/" aria-label="Agent Console 首页">
          <TerminalSquare :size="20" aria-hidden="true" />
          <span>Agent Console</span>
        </RouterLink>
        <StatusBadge :tone="state.fixtureMode ? 'warning' : 'success'">
          {{ state.fixtureMode ? 'Fixture only' : '真实连接' }}
        </StatusBadge>
      </div>

      <nav class="global-nav" aria-label="主导航">
        <RouterLink to="/" exact-active-class="is-active">
          <Bell :size="16" aria-hidden="true" />待处理
          <span v-if="pendingAttention.length" class="nav-count">{{ pendingAttention.length }}</span>
        </RouterLink>
        <RouterLink to="/tasks" active-class="is-active">
          <ListTodo :size="16" aria-hidden="true" />任务
        </RouterLink>
        <RouterLink to="/devices" active-class="is-active">
          <MonitorSmartphone :size="16" aria-hidden="true" />设备
        </RouterLink>
      </nav>

      <div class="header-actions">
        <form class="global-search" role="search" @submit.prevent="submitSearch">
          <Search :size="15" aria-hidden="true" />
          <label class="sr-only" for="global-search">搜索任务</label>
          <input id="global-search" ref="searchInput" v-model="searchQuery" type="search" placeholder="搜索任务…" />
          <kbd>⌘K</kbd>
        </form>
        <button
          class="icon-button"
          type="button"
          :aria-label="theme === 'dark' ? '切换到浅色模式' : '切换到深色模式'"
          @click="toggleTheme"
        >
          <Sun v-if="theme === 'dark'" :size="18" aria-hidden="true" />
          <Moon v-else :size="18" aria-hidden="true" />
        </button>
        <button
          class="connection-button"
          type="button"
          :disabled="!state.fixtureMode"
          :title="state.fixtureMode ? '切换 fixture 连接状态' : undefined"
          @click="toggleConnection"
        >
          <Wifi v-if="state.connection === 'ONLINE'" :size="15" aria-hidden="true" />
          <WifiOff v-else :size="15" aria-hidden="true" />
          <StatusBadge :tone="connectionTone" dot>{{ connectionLabel[state.connection] }}</StatusBadge>
        </button>
      </div>
    </header>

    <div class="app-frame">
      <nav class="desktop-icon-rail" aria-label="主导航">
        <RouterLink to="/" exact-active-class="is-active" aria-label="待处理" title="待处理">
          <Bell :size="19" aria-hidden="true" />
          <b v-if="pendingAttention.length">{{ pendingAttention.length }}</b>
        </RouterLink>
        <RouterLink to="/tasks" active-class="is-active" aria-label="任务" title="任务">
          <ListTodo :size="19" aria-hidden="true" />
        </RouterLink>
        <RouterLink to="/devices" active-class="is-active" aria-label="设备" title="设备">
          <MonitorSmartphone :size="19" aria-hidden="true" />
        </RouterLink>
      </nav>

      <div class="app-body" :class="{ 'app-body--full': !section }">
        <aside v-if="section" class="module-sidebar">
          <TaskListNav v-if="section === 'tasks'" />
          <DeviceSettingsNav v-else />
        </aside>

        <main id="main-content" ref="mainElement" class="app-main" tabindex="-1">
          <div v-if="linkBanner" class="link-banner">
            <NoticeBanner tone="danger" title="实时连接未接通">
              {{ linkBanner }}
            </NoticeBanner>
            <UiButton variant="secondary" size="small" class="link-banner__retry" @click="retryConnection">
              重试连接
            </UiButton>
          </div>
          <slot />
        </main>
      </div>
    </div>

    <nav class="mobile-bottom-nav" aria-label="移动端主导航">
      <RouterLink to="/" exact-active-class="is-active">
        <span class="mobile-bottom-nav__icon">
          <Bell :size="20" aria-hidden="true" />
          <b v-if="pendingAttention.length">{{ pendingAttention.length }}</b>
        </span>
        <span>待处理</span>
      </RouterLink>
      <RouterLink to="/tasks" active-class="is-active">
        <ListTodo :size="20" aria-hidden="true" />
        <span>任务</span>
      </RouterLink>
      <RouterLink to="/devices" active-class="is-active">
        <MonitorSmartphone :size="20" aria-hidden="true" />
        <span>设备</span>
      </RouterLink>
    </nav>
  </div>

  <dialog ref="navDialog" class="nav-dialog" aria-label="导航菜单" @click.self="closeNavigation">
    <div class="nav-dialog__header">
      <strong>Agent Console</strong>
      <button class="icon-button" type="button" aria-label="关闭导航" @click="closeNavigation">
        <X :size="19" aria-hidden="true" />
      </button>
    </div>
    <nav class="mobile-primary-nav" aria-label="移动端主导航">
      <RouterLink to="/"><Bell :size="17" aria-hidden="true" />待处理</RouterLink>
      <RouterLink to="/tasks"><ListTodo :size="17" aria-hidden="true" />任务</RouterLink>
      <RouterLink to="/devices"><MonitorSmartphone :size="17" aria-hidden="true" />设备</RouterLink>
    </nav>
    <div v-if="section" class="nav-dialog__module">
      <TaskListNav v-if="section === 'tasks'" />
      <DeviceSettingsNav v-else />
    </div>
  </dialog>
</template>

<style scoped>
.app-shell {
  display: grid;
  width: 100%;
  height: 100dvh;
  grid-template-rows: var(--global-header-height) minmax(0, 1fr);
  background: var(--bg-app);
}

.global-header {
  position: relative;
  z-index: var(--layer-sticky);
  display: grid;
  grid-template-columns: minmax(240px, 1fr) auto minmax(240px, 1fr);
  align-items: center;
  gap: var(--space-4);
  padding: 0 var(--page-gutter);
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.brand-lockup,
.header-actions,
.global-nav,
.global-nav a,
.brand-mark,
.connection-button {
  display: flex;
  align-items: center;
}

.brand-lockup {
  min-width: 0;
  gap: var(--space-3);
}

.brand-mark {
  min-width: 0;
  gap: 9px;
  color: var(--text-primary);
  font-size: 15px;
  font-weight: 750;
  letter-spacing: -0.01em;
  text-decoration: none;
}

.brand-mark svg {
  color: var(--accent);
}

.global-nav {
  align-self: stretch;
  gap: 2px;
}

.app-frame {
  display: grid;
  min-width: 0;
  min-height: 0;
  grid-template-columns: 56px minmax(0, 1fr);
  overflow: hidden;
}

.desktop-icon-rail {
  display: grid;
  min-height: 0;
  align-content: start;
  justify-items: center;
  gap: 4px;
  padding: 8px 6px;
  border-right: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.desktop-icon-rail a {
  position: relative;
  display: grid;
  width: 44px;
  height: 44px;
  place-items: center;
  border-radius: var(--radius-control);
  color: var(--text-muted);
  text-decoration: none;
}

.desktop-icon-rail a:hover,
.desktop-icon-rail a.is-active {
  background: var(--accent-soft);
  color: var(--accent);
}

.desktop-icon-rail b {
  position: absolute;
  top: 5px;
  right: 4px;
  display: grid;
  min-width: 15px;
  height: 15px;
  place-items: center;
  border-radius: 999px;
  background: var(--warning);
  color: #17120a;
  font-size: 9px;
  line-height: 1;
}

.global-nav a {
  position: relative;
  gap: 7px;
  padding: 0 14px;
  color: var(--text-secondary);
  font-size: 13px;
  font-weight: 650;
  text-decoration: none;
}

.global-nav a::after {
  position: absolute;
  right: 14px;
  bottom: -1px;
  left: 14px;
  height: 2px;
  background: transparent;
  content: '';
}

.global-nav a:hover,
.global-nav a.is-active {
  color: var(--text-primary);
}

.global-nav a.is-active::after {
  background: var(--accent);
}

.nav-count {
  display: grid;
  min-width: 18px;
  height: 18px;
  place-items: center;
  border-radius: 999px;
  background: color-mix(in srgb, var(--warning), transparent 84%);
  color: var(--warning);
  font-size: 10px;
}

.header-actions {
  min-width: 0;
  justify-content: flex-end;
  gap: var(--space-2);
}

.global-search {
  display: flex;
  width: min(250px, 22vw);
  height: 34px;
  align-items: center;
  gap: 7px;
  padding: 0 8px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-control);
  background: var(--bg-surface);
  color: var(--text-muted);
}

.global-search:focus-within {
  border-color: var(--focus-ring);
}

.global-search input {
  width: 100%;
  min-width: 0;
  border: 0;
  outline: 0;
  background: transparent;
  color: var(--text-primary);
  font-size: 12px;
}

.global-search kbd {
  padding: 1px 4px;
  border: 1px solid var(--border-subtle);
  border-radius: 4px;
  color: var(--text-muted);
  font: 10px var(--font-ui);
}

.icon-button,
.menu-button,
.connection-button {
  border: 0;
  background: transparent;
  color: var(--text-secondary);
}

.icon-button,
.menu-button {
  display: grid;
  width: 36px;
  height: 36px;
  padding: 0;
  place-items: center;
  border-radius: var(--radius-control);
}

.icon-button:hover,
.menu-button:hover,
.connection-button:hover {
  background: var(--bg-elevated);
  color: var(--text-primary);
}

.menu-button {
  display: none;
}

.connection-button {
  gap: 6px;
  padding: 3px;
  border-radius: 999px;
}

.connection-button > svg {
  margin-left: 5px;
}

.app-body {
  display: grid;
  min-height: 0;
  grid-template-columns: var(--task-list-width) minmax(0, 1fr);
}

.app-body:has(.settings-nav) {
  grid-template-columns: var(--module-nav-width) minmax(0, 1fr);
}

.app-body--full {
  grid-template-columns: minmax(0, 1fr);
}

.module-sidebar {
  display: flex;
  min-width: 0;
  min-height: 0;
  border-right: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.app-main {
  min-width: 0;
  min-height: 0;
  overflow: auto;
  outline: 0;
}

.link-banner {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 10px var(--page-gutter) 0;
}

.link-banner > .notice {
  flex: 1;
  min-width: 0;
}

.link-banner__retry {
  flex: 0 0 auto;
}

.nav-dialog {
  width: min(380px, calc(100vw - 24px));
  height: calc(100dvh - 24px);
  max-height: none;
  padding: 0;
  margin: 12px 12px 12px auto;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-dialog);
  background: var(--bg-nav);
  box-shadow: var(--shadow-overlay);
  color: var(--text-primary);
}

.mobile-bottom-nav {
  display: none;
}

@media (min-width: 1101px) {
  .global-nav {
    display: none;
  }
}

.nav-dialog::backdrop {
  background: var(--scrim);
}

.nav-dialog[open] {
  display: grid;
  grid-template-rows: auto auto minmax(0, 1fr);
}

.nav-dialog__header {
  display: flex;
  min-height: 56px;
  align-items: center;
  justify-content: space-between;
  padding: 0 12px 0 16px;
  border-bottom: 1px solid var(--border-subtle);
}

.mobile-primary-nav {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  padding: 8px;
  border-bottom: 1px solid var(--border-subtle);
}

.mobile-primary-nav a {
  display: flex;
  min-height: 42px;
  align-items: center;
  justify-content: center;
  gap: 6px;
  border-radius: var(--radius-control);
  color: var(--text-secondary);
  text-decoration: none;
}

.mobile-primary-nav a.router-link-active {
  background: var(--accent-soft);
  color: var(--text-primary);
}

.nav-dialog__module {
  display: flex;
  min-height: 0;
  overflow: hidden;
}

@media (max-width: 1250px) {
  .global-search {
    display: none;
  }
}

@media (max-width: 1100px) {
  .app-frame {
    grid-template-columns: minmax(0, 1fr);
  }

  .desktop-icon-rail {
    display: none;
  }

  .global-header {
    grid-template-columns: 1fr auto auto;
  }

  .menu-button {
    display: grid;
  }

  .module-sidebar {
    display: none;
  }

  .app-body {
    grid-template-columns: minmax(0, 1fr);
  }

  .app-body:has(.settings-nav) {
    grid-template-columns: minmax(0, 1fr);
  }
}

@media (max-width: 759px) {
  .app-shell {
    min-height: 100dvh;
    height: auto;
    grid-template-rows: 56px minmax(0, 1fr);
  }

  .global-header {
    position: sticky;
    top: 0;
    grid-template-columns: minmax(0, 1fr) auto;
    padding: 0 12px;
  }

  .global-nav,
  .connection-button,
  .brand-lockup > .status-badge {
    display: none;
  }

  .brand-lockup {
    gap: 8px;
  }

  .brand-mark span {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .brand-mark {
    min-height: 44px;
  }

  .app-body,
  .app-main {
    overflow: visible;
  }

  .app-frame {
    overflow: visible;
  }

  .app-main {
    padding-bottom: calc(62px + env(safe-area-inset-bottom));
  }

  .header-actions {
    gap: 0;
  }

  .icon-button,
  .menu-button {
    width: 44px;
    height: 44px;
  }

  .mobile-primary-nav a {
    min-height: 44px;
  }

  .mobile-bottom-nav {
    position: fixed;
    z-index: var(--layer-sticky);
    right: 0;
    bottom: 0;
    left: 0;
    display: grid;
    height: calc(58px + env(safe-area-inset-bottom));
    grid-template-columns: repeat(3, 1fr);
    padding: 4px max(8px, env(safe-area-inset-right)) env(safe-area-inset-bottom)
      max(8px, env(safe-area-inset-left));
    border-top: 1px solid var(--border-subtle);
    background: color-mix(in srgb, var(--bg-nav), transparent 3%);
  }

  .mobile-bottom-nav a {
    position: relative;
    display: flex;
    min-width: 64px;
    min-height: 50px;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 2px;
    border-radius: var(--radius-control);
    color: var(--text-muted);
    font-size: 10px;
    font-weight: 650;
    text-decoration: none;
  }

  .mobile-bottom-nav a.is-active {
    color: var(--accent);
  }

  .mobile-bottom-nav__icon {
    position: relative;
    display: grid;
    place-items: center;
  }

  .mobile-bottom-nav__icon b {
    position: absolute;
    top: -5px;
    right: -9px;
    display: grid;
    min-width: 15px;
    height: 15px;
    place-items: center;
    border-radius: 999px;
    background: var(--warning);
    color: #17120a;
    font-size: 9px;
    line-height: 1;
  }
}
</style>
