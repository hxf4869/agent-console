<script setup lang="ts">
import {
  Cable,
  CheckCircle2,
  ChevronRight,
  CircleAlert,
  Clock3,
  Laptop,
  Link2,
  QrCode,
  RefreshCw,
  ShieldCheck,
  Smartphone,
  WifiOff,
  X,
} from 'lucide-vue-next'
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import NoticeBanner from '@/components/NoticeBanner.vue'
import StatusBadge from '@/components/StatusBadge.vue'
import UiButton from '@/components/UiButton.vue'
import VersionPanel from '@/components/VersionPanel.vue'
import { useBottomSheetFocus } from '@/composables/useBottomSheetFocus'
import { usePushNotifications } from '@/composables/usePushNotifications'
import { buildDiagnosticsReport } from '@/lib/diagnostics'
import { connectionLabel, formatDateTime } from '@/lib/presentation'
import { useConsoleStore } from '@/store/console'

const {
  state,
  toggleConnection,
  loadDevices,
  lookupPairing,
  approvePairing,
  revokeDevice,
  toggleNotifySuccess,
  componentVersions,
} = useConsoleStore()
const route = useRoute()
const selectedDevice = ref('')
const bindingVisible = ref(false)
const pairingCode = ref('')
const pairingChallenge = ref<Awaited<ReturnType<typeof lookupPairing>>>()
const pairingBusy = ref(false)
const lastAutoLookupCode = ref('')
const deviceListElement = ref<HTMLElement | null>(null)
const { sheetElement, sheetOpen, openSheet, closeSheet, onSheetKeydown } = useBottomSheetFocus(
  () => deviceListElement.value,
  '#device-sheet',
)
const { pushState, pushBusy, pushMessage, refreshPushState, toggleBrowserPush } =
  usePushNotifications()

const currentDevice = computed(() =>
  state.devices.find((device) => device.id === selectedDevice.value) ?? state.devices[0],
)
const deviceRuntime = computed(() => {
  const session = state.sessions.find((item) => item.deviceId === currentDevice.value?.id)
  return session ? state.runtimes[session.id] : undefined
})
const capabilities = computed(() => [
  { label: '读取任务与历史', status: '支持', tone: 'success' as const },
  {
    label: '开始 / Steer / Interrupt',
    status: ['START_TURN', 'STEER', 'INTERRUPT'].every(
      (operation) => deviceRuntime.value?.capabilities.operations[operation as 'START_TURN'],
    )
      ? '已验证'
      : '按会话能力',
    tone: 'accent' as const,
  },
  { label: '问题与风险审批', status: state.fixtureMode ? 'Fixture only' : '未开放', tone: 'warning' as const },
  { label: '模型 / 思考 / Fast / 权限写入', status: '未开放', tone: 'warning' as const },
  { label: '文件预览与 Git Diff', status: '只读', tone: 'neutral' as const },
  { label: '新建 / 改名 / 归档 / Fork / 后台停止', status: '不支持', tone: 'warning' as const },
])

onMounted(() => {
  void loadDevices()
  void refreshPushState()
})

watch(
  () => state.devices.map((device) => device.id),
  (ids) => {
    if (!ids.includes(selectedDevice.value)) selectedDevice.value = ids[0] ?? ''
  },
  { immediate: true },
)

watch(
  [() => route.query.code, () => state.connection],
  ([rawCode, connection]) => {
    const queryCode = Array.isArray(rawCode) ? rawCode[0] : rawCode
    const code = String(queryCode ?? '').replace(/\D/g, '').slice(0, 6)
    if (code.length !== 6) return
    pairingCode.value = code
    bindingVisible.value = true
    if (connection !== 'ONLINE' || lastAutoLookupCode.value === code) return
    lastAutoLookupCode.value = code
    void findPairing()
  },
  { immediate: true },
)

function selectDevice(id: string, event: MouseEvent): void {
  selectedDevice.value = id
  if (window.matchMedia('(max-width: 759px)').matches) void openSheet(event.currentTarget)
}

async function findPairing(): Promise<void> {
  const code = pairingCode.value.replace(/\D/g, '').slice(0, 6)
  if (code.length !== 6) return
  pairingBusy.value = true
  pairingChallenge.value = await lookupPairing(code)
  pairingBusy.value = false
}

async function approveCurrentPairing(): Promise<void> {
  if (!pairingChallenge.value) return
  pairingBusy.value = true
  const ok = await approvePairing(pairingCode.value, pairingChallenge.value.challengeId)
  pairingBusy.value = false
  if (ok) {
    pairingChallenge.value = undefined
    pairingCode.value = ''
    bindingVisible.value = false
  }
}

async function revokeCurrentDevice(): Promise<void> {
  const device = currentDevice.value
  if (!device || device.revoked) return
  if (!window.confirm(`确认撤销设备“${device.displayName}”？Bridge 会立即断开。`)) return
  await revokeDevice(device.id)
}

/** 诊断报告(UX-02):白名单字段;不含路径/标题正文/命令/凭据。 */
const diagnosticsCopied = ref(false)

async function copyDiagnostics(): Promise<void> {
  const device = currentDevice.value
  const runtimeCapability = deviceRuntime.value?.capabilities
  try {
    await navigator.clipboard.writeText(
      buildDiagnosticsReport({
        link: state.link,
        versions: componentVersions(),
        device: {
          displayName: '',
          connection: device?.connection ?? state.connection,
          platform: device?.platform ?? '',
          architecture: device?.architecture ?? '',
        },
        controlMode: device?.controlMode ?? 'UNAVAILABLE',
        compatibility: device?.compatibility ?? 'DEGRADED',
        ...(runtimeCapability ? { capabilities: runtimeCapability } : {}),
        ...(state.receipts[0] ? { lastReceipt: state.receipts[0] } : {}),
        now: new Date().toISOString(),
      }),
    )
    diagnosticsCopied.value = true
    window.setTimeout(() => (diagnosticsCopied.value = false), 1600)
  } catch {
    diagnosticsCopied.value = false
  }
}
</script>

<template>
  <div id="device-settings" class="devices-page">
    <header class="devices-header">
      <h1>设备</h1>
      <UiButton
        variant="primary"
        :aria-label="bindingVisible ? '收起新设备绑定码' : '显示新设备绑定码'"
        @click="bindingVisible = !bindingVisible"
      >
        <template #icon><Link2 aria-hidden="true" /></template>
        {{ bindingVisible ? '收起绑定码' : '绑定新设备' }}
      </UiButton>
    </header>

    <NoticeBanner v-if="state.connection !== 'ONLINE'" tone="danger" title="实时连接不可用">
      Browser WebSocket 正在重连；写操作已暂停。
    </NoticeBanner>

    <section v-if="bindingVisible" class="binding-panel" aria-labelledby="binding-title">
      <div class="binding-visual" aria-hidden="true"><QrCode :size="46" /></div>
      <div>
        <span>Bridge-first 一次性绑定码</span>
        <h2 id="binding-title">输入 Bridge 显示的 6 位短码</h2>
        <input
          v-model="pairingCode"
          class="mono"
          inputmode="numeric"
          autocomplete="one-time-code"
          maxlength="6"
          placeholder="000000"
          aria-label="6 位设备绑定码"
          @input="pairingCode = pairingCode.replace(/\D/g, '').slice(0, 6)"
          @keydown.enter.prevent="findPairing"
        />
        <p v-if="pairingChallenge">
          {{ pairingChallenge.deviceName }} · {{ pairingChallenge.platform }} / {{ pairingChallenge.architecture }}
        </p>
        <p v-else>短码由未绑定 Bridge 主动注册；浏览器只负责核对并批准。</p>
      </div>
      <UiButton
        v-if="!pairingChallenge"
        variant="secondary"
        :disabled="pairingBusy || pairingCode.length !== 6"
        @click="findPairing"
      >
        核对设备
      </UiButton>
      <UiButton v-else variant="primary" :disabled="pairingBusy" @click="approveCurrentPairing">
        批准绑定
      </UiButton>
    </section>

    <div class="devices-layout">
      <section ref="deviceListElement" class="device-list" aria-labelledby="device-list-title" tabindex="-1">
        <header><h2 id="device-list-title">已绑定设备</h2><span>{{ state.devices.length }} 台</span></header>
        <button
          v-for="device in state.devices"
          :key="device.id"
          type="button"
          :class="{ 'is-selected': selectedDevice === device.id }"
          @click="selectDevice(device.id, $event)"
        >
          <span class="device-icon"><Laptop :size="19" /></span>
          <span>
            <strong>{{ device.displayName }}</strong>
            <small>{{ device.platform }} · {{ device.architecture }}</small>
          </span>
          <StatusBadge :tone="device.connection === 'ONLINE' ? 'success' : 'danger'" dot>
            {{ device.revoked ? '已撤销' : connectionLabel[device.connection] }}
          </StatusBadge>
          <ChevronRight :size="15" aria-hidden="true" />
        </button>
        <div v-if="!state.devices.length" class="pwa-binding">
          <WifiOff :size="17" aria-hidden="true" />
          <div><strong>尚无设备</strong><span>请先在 Mac 上运行 Bridge pair。</span></div>
        </div>
      </section>

      <section v-if="currentDevice" class="device-detail" aria-label="设备详情">
        <section class="detail-card detail-card--identity">
          <header>
            <div>
              <span class="device-icon"><Laptop :size="22" /></span>
              <div>
                <h2>{{ currentDevice.displayName }}</h2>
                <p class="mono">{{ currentDevice.id }}</p>
              </div>
            </div>
            <StatusBadge
              :tone="currentDevice.connection === 'ONLINE' ? 'success' : 'danger'"
              dot
            >
              {{ currentDevice.revoked ? '已撤销' : connectionLabel[currentDevice.connection] }}
            </StatusBadge>
          </header>
          <dl>
            <div><dt>Bridge 版本</dt><dd class="mono">{{ currentDevice.bridgeVersion || 'unknown' }}</dd></div>
            <div><dt>平台</dt><dd>{{ currentDevice.platform }} / {{ currentDevice.architecture }}</dd></div>
            <div><dt>控制模式</dt><dd class="mono">{{ currentDevice.controlMode }}</dd></div>
            <div><dt>兼容状态</dt><dd class="mono">{{ currentDevice.compatibility }}</dd></div>
            <div><dt>最后在线</dt><dd>{{ currentDevice.lastSeenAt ? formatDateTime(currentDevice.lastSeenAt) : '尚未上线' }}</dd></div>
          </dl>
          <footer>
            <UiButton
              v-if="state.fixtureMode"
              variant="secondary"
              @click="toggleConnection"
            >
              <template #icon><WifiOff v-if="state.connection === 'ONLINE'" /><RefreshCw v-else /></template>
              {{ state.connection === 'ONLINE' ? '模拟断开' : '模拟重新连接' }}
            </UiButton>
            <UiButton
              v-if="!currentDevice.revoked"
              variant="danger"
              @click="revokeCurrentDevice"
            >
              撤销设备
            </UiButton>
          </footer>
        </section>

        <section class="detail-card capability-card">
          <header>
            <div><ShieldCheck :size="17" /><div><h2>能力与权限</h2></div></div>
            <StatusBadge tone="accent">{{ deviceRuntime ? `rev ${deviceRuntime.capabilities.revision}` : '设备级摘要' }}</StatusBadge>
          </header>
          <div class="capability-list">
            <div v-for="capability in capabilities" :key="capability.label">
              <CheckCircle2 v-if="capability.tone === 'success'" :size="15" />
              <CircleAlert v-else-if="capability.tone === 'warning'" :size="15" />
              <Cable v-else :size="15" />
              <span>{{ capability.label }}</span>
              <StatusBadge :tone="capability.tone">{{ capability.status }}</StatusBadge>
            </div>
          </div>
        </section>

        <VersionPanel :device="currentDevice" />

        <section class="detail-card diagnostics-card" aria-labelledby="diagnostics-title">
          <header>
            <div><h2 id="diagnostics-title">诊断</h2></div>
            <StatusBadge tone="neutral">仅含白名单字段</StatusBadge>
          </header>
          <p>复制版本、平台、连接阶段、错误码、时间与各操作验证状态；不含路径、标题正文、命令与凭据。</p>
          <footer>
            <UiButton variant="secondary" size="small" @click="copyDiagnostics">
              {{ diagnosticsCopied ? '已复制诊断' : '复制诊断信息' }}
            </UiButton>
            <label class="notify-toggle">
              <input
                type="checkbox"
                :checked="state.notifySuccessEnabled"
                @change="toggleNotifySuccess"
              />
              任务完成也提醒（默认关闭；失败与待处理始终提醒）
            </label>
            <label class="notify-toggle">
              <input
                type="checkbox"
                :checked="pushState === 'subscribed'"
                :disabled="pushBusy || pushState === 'unsupported' || pushState === 'unconfigured' || pushState === 'denied'"
                @change="toggleBrowserPush"
              />
              后台推送（关闭页面后经系统通知提醒）
            </label>
            <p v-if="pushMessage" class="push-hint">{{ pushMessage }}</p>
          </footer>
        </section>

      </section>
    </div>

    <button
      v-if="sheetOpen"
      class="device-sheet-scrim"
      type="button"
      aria-label="关闭设备详情"
      @click="closeSheet"
    />
    <section
      ref="sheetElement"
      id="device-sheet"
      class="mobile-device-sheet"
      :class="{ 'is-open': sheetOpen }"
      role="dialog"
      aria-modal="true"
      :aria-hidden="sheetOpen ? undefined : 'true'"
      aria-labelledby="device-sheet-title"
      tabindex="-1"
      @keydown="onSheetKeydown"
    >
      <header>
        <div><span class="device-icon"><Laptop :size="20" /></span><strong id="device-sheet-title">{{ currentDevice?.displayName ?? '设备' }}</strong></div>
        <button type="button" aria-label="关闭设备详情" @click="closeSheet"><X :size="18" /></button>
      </header>
      <dl>
        <div><dt>连接</dt><dd>{{ currentDevice ? connectionLabel[currentDevice.connection] : '离线' }}</dd></div>
        <div><dt>平台</dt><dd>{{ currentDevice?.platform }} / {{ currentDevice?.architecture }}</dd></div>
        <div><dt>控制</dt><dd class="mono">{{ currentDevice?.controlMode ?? 'UNAVAILABLE' }}</dd></div>
        <div><dt>兼容性</dt><dd class="mono">{{ currentDevice?.compatibility ?? 'DEGRADED' }}</dd></div>
      </dl>
      <footer v-if="currentDevice && !currentDevice.revoked">
        <UiButton variant="danger" @click="revokeCurrentDevice">
          撤销设备
        </UiButton>
      </footer>
    </section>
  </div>
</template>

<style scoped>
.devices-page {
  display: grid;
  width: min(100%, var(--configuration-max));
  min-height: 100%;
  align-content: start;
  gap: 12px;
  padding: 0 var(--page-gutter) 42px;
  margin: 0 auto;
}

.devices-header,
.binding-panel,
.device-list > header,
.device-list > button,
.pwa-binding,
.detail-card > header,
.detail-card > header > div,
.detail-card--identity > footer,
.capability-list > div,
.device-context section > header,
.device-context li {
  display: flex;
  align-items: center;
}

.devices-header {
  min-height: 66px;
  justify-content: space-between;
  gap: 16px;
  border-bottom: 1px solid var(--border-subtle);
}

.devices-header h1 {
  margin: 0;
  font-size: 18px;
}

.binding-panel {
  gap: 14px;
  padding: 13px;
  border: 1px solid color-mix(in srgb, var(--accent), transparent 55%);
  border-radius: var(--radius-card);
  background: color-mix(in srgb, var(--accent), transparent 91%);
}

.binding-visual,
.device-icon {
  display: grid;
  flex: 0 0 auto;
  place-items: center;
  border-radius: var(--radius-control);
  background: var(--accent-soft);
  color: var(--accent);
}

.binding-visual {
  width: 62px;
  height: 62px;
}

.binding-panel > div:nth-child(2) {
  min-width: 0;
  flex: 1;
}

.binding-panel span:not(.status-badge) {
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 750;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.binding-panel h2 {
  margin: 1px 0;
  font-size: 20px;
  letter-spacing: 0.08em;
}

.binding-panel p {
  margin: 0;
  color: var(--text-secondary);
  font-size: 11px;
}

.binding-panel input {
  width: min(100%, 180px);
  min-height: 36px;
  padding: 6px 9px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-control);
  outline: 0;
  background: var(--bg-surface);
  color: var(--text-primary);
  font-size: 16px;
  letter-spacing: 0.16em;
}

.binding-panel input:focus {
  border-color: var(--focus-ring);
}

.devices-layout {
  display: grid;
  min-height: 0;
  grid-template-columns: minmax(260px, 320px) minmax(440px, 1fr);
  gap: 12px;
}

.mobile-device-sheet,
.device-sheet-scrim {
  display: none;
}

.device-list,
.detail-card,
.device-context section {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.device-list {
  align-self: start;
  overflow: hidden;
}

.device-list > header {
  min-height: 44px;
  justify-content: space-between;
  padding: 8px 12px;
  border-bottom: 1px solid var(--border-subtle);
}

.device-list h2,
.detail-card h2 {
  margin: 0;
  font-size: 14px;
}

.device-list > header span {
  color: var(--text-muted);
  font-size: 10px;
}

.device-list > button {
  width: 100%;
  min-height: 68px;
  gap: 9px;
  padding: 9px 10px;
  border: 0;
  border-bottom: 1px solid var(--border-subtle);
  background: transparent;
  color: var(--text-secondary);
  text-align: left;
}

.device-list > button:hover,
.device-list > button.is-selected {
  background: var(--accent-soft);
}

.device-icon {
  width: 34px;
  height: 34px;
}

.device-list button > span:nth-child(2) {
  display: grid;
  min-width: 0;
  flex: 1;
}

.device-list button strong {
  color: var(--text-primary);
  font-size: 12px;
}

.device-list button small,
.pwa-binding span {
  color: var(--text-muted);
  font-size: 10px;
}

.device-list button > svg {
  color: var(--text-muted);
}

.pwa-binding {
  min-height: 60px;
  gap: 9px;
  padding: 10px;
}

.pwa-binding > svg {
  color: var(--accent);
}

.pwa-binding > div {
  display: grid;
  min-width: 0;
  flex: 1;
}

.pwa-binding strong {
  font-size: 11px;
}

.device-detail,
.device-context {
  display: grid;
  align-content: start;
  gap: 12px;
  min-width: 0;
}

.detail-card > header {
  min-height: 54px;
  justify-content: space-between;
  gap: 10px;
  padding: 9px 12px;
  border-bottom: 1px solid var(--border-subtle);
}

.detail-card > header > div {
  min-width: 0;
  gap: 9px;
}

.detail-card > header p {
  margin: 1px 0 0;
  color: var(--text-muted);
  font-size: 10px;
}

.detail-card--identity dl {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  padding: 3px 12px 12px;
  margin: 0;
}

.detail-card--identity dl > div {
  display: grid;
  gap: 2px;
  padding: 9px 0;
  border-bottom: 1px solid var(--border-subtle);
}

.detail-card--identity dl > div:nth-last-child(-n + 2) {
  border-bottom: 0;
}

.detail-card dt {
  color: var(--text-muted);
  font-size: 10px;
}

.detail-card dd {
  margin: 0;
  color: var(--text-secondary);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.detail-card--identity > footer {
  justify-content: space-between;
  gap: 10px;
  padding: 10px 12px;
  border-top: 1px solid var(--border-subtle);
}

.detail-card--identity > footer > span {
  color: var(--text-muted);
  font-size: 10px;
}

.capability-list {
  display: grid;
  padding: 5px 12px 10px;
}

.capability-list > div {
  min-height: 38px;
  gap: 8px;
  border-bottom: 1px solid var(--border-subtle);
}

.capability-list > div:last-child {
  border: 0;
}

.diagnostics-card > p {
  padding: 0 12px;
  margin: 10px 0;
  color: var(--text-muted);
  font-size: 11px;
}

.diagnostics-card > footer {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  padding: 0 12px 12px;
}

.notify-toggle {
  display: inline-flex;
  min-height: 32px;
  align-items: center;
  gap: 7px;
  color: var(--text-secondary);
  font-size: 11px;
}

.notify-toggle input {
  width: 15px;
  height: 15px;
  accent-color: var(--accent);
}

.push-hint {
  margin: 0;
  color: var(--text-secondary);
  font-size: 11px;
}

.capability-list > div > svg {
  color: var(--accent);
}

.capability-list > div:nth-last-child(1) > svg {
  color: var(--warning);
}

.capability-list > div > span:nth-child(2) {
  min-width: 0;
  flex: 1;
  color: var(--text-secondary);
  font-size: 11px;
}

.security-card > p {
  padding: 11px 12px;
  margin: 0;
  color: var(--text-secondary);
  font-size: 11px;
}

.device-context section {
  overflow: hidden;
}

.device-context section > header {
  min-height: 42px;
  gap: 7px;
  padding: 8px 10px;
  border-bottom: 1px solid var(--border-subtle);
}

.device-context section > header svg {
  color: var(--accent);
}

.device-context ol,
.device-context ul {
  padding: 10px;
  margin: 0;
  list-style: none;
}

.device-context li {
  position: relative;
  gap: 9px;
  min-height: 48px;
  color: var(--text-muted);
}

.device-context ol li:not(:last-child)::after {
  position: absolute;
  top: 34px;
  bottom: -5px;
  left: 13px;
  width: 1px;
  background: var(--border-strong);
  content: '';
}

.device-context ol li > span {
  display: grid;
  z-index: 1;
  width: 26px;
  height: 26px;
  flex: 0 0 auto;
  place-items: center;
  border: 1px solid var(--border-strong);
  border-radius: 50%;
  background: var(--bg-surface);
  font: 10px var(--font-mono);
}

.device-context ol li.is-done > span {
  border-color: var(--success);
  color: var(--success);
}

.device-context li > div {
  display: grid;
}

.device-context li strong {
  color: var(--text-secondary);
  font-size: 11px;
}

.device-context li small {
  font-size: 10px;
}

.device-context ul li {
  min-height: 30px;
  padding-left: 15px;
  font-size: 11px;
}

.device-context ul li::before {
  position: absolute;
  left: 2px;
  color: var(--text-muted);
  content: '·';
}

@media (max-width: 1250px) {
  .devices-layout {
    grid-template-columns: minmax(230px, 300px) minmax(420px, 1fr);
  }
}

@media (max-width: 759px) {
  .devices-page {
    padding: 0 12px 32px;
  }

  .devices-header {
    min-height: 56px;
    align-items: center;
    padding: 0 4px;
  }

  .devices-header > .ui-button :deep(span) {
    display: none;
  }

  .binding-panel {
    align-items: flex-start;
    flex-wrap: wrap;
  }

  .binding-panel .status-badge {
    margin-left: 76px;
  }

  .devices-layout {
    display: block;
  }

  .device-list {
    border-right: 0;
    border-left: 0;
    border-radius: 0;
  }

  .device-detail {
    display: none;
  }

  .device-sheet-scrim {
    position: fixed;
    z-index: calc(var(--layer-overlay) - 1);
    inset: 56px 0 calc(58px + env(safe-area-inset-bottom));
    display: block;
    width: 100%;
    border: 0;
    background: var(--scrim);
  }

  .mobile-device-sheet {
    position: fixed;
    z-index: var(--layer-overlay);
    right: 0;
    bottom: calc(58px + env(safe-area-inset-bottom));
    left: 0;
    display: grid;
    overflow: hidden;
    border-top: 1px solid var(--border-strong);
    border-radius: 14px 14px 0 0;
    background: var(--bg-elevated);
    box-shadow: 0 -12px 36px rgb(0 0 0 / 24%);
    visibility: hidden;
    transform: translateY(calc(100% + 12px));
    transition: transform 180ms cubic-bezier(0.2, 0.8, 0.2, 1);
  }

  .mobile-device-sheet.is-open {
    visibility: visible;
    transform: translateY(0);
  }

  .mobile-device-sheet > header,
  .mobile-device-sheet > header > div,
  .mobile-device-sheet footer {
    display: flex;
    align-items: center;
  }

  .mobile-device-sheet > header {
    min-height: 60px;
    justify-content: space-between;
    padding: 8px 10px 8px 14px;
    border-bottom: 1px solid var(--border-subtle);
  }

  .mobile-device-sheet > header > div {
    gap: 9px;
  }

  .mobile-device-sheet > header button {
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

  .mobile-device-sheet dl {
    padding: 4px 14px;
    margin: 0;
  }

  .mobile-device-sheet dl > div {
    display: flex;
    min-height: 44px;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    border-bottom: 1px solid var(--border-subtle);
  }

  .mobile-device-sheet dl > div:last-child {
    border-bottom: 0;
  }

  .mobile-device-sheet dt,
  .mobile-device-sheet dd {
    font-size: 11px;
  }

  .mobile-device-sheet dt {
    color: var(--text-muted);
  }

  .mobile-device-sheet dd {
    margin: 0;
    color: var(--text-secondary);
  }

  .mobile-device-sheet footer {
    justify-content: flex-end;
    padding: 10px 14px;
    border-top: 1px solid var(--border-subtle);
  }
}
</style>
