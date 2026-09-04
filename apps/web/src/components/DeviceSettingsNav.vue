<script setup lang="ts">
import { BellRing, Cable, KeyRound, MonitorSmartphone, Palette, ShieldCheck } from 'lucide-vue-next'

const items = [
  { label: '设备与绑定', icon: MonitorSmartphone, active: true },
  { label: 'Bridge 连接', icon: Cable },
  { label: '权限边界', icon: ShieldCheck },
  { label: '通知', icon: BellRing },
  { label: '密钥与会话', icon: KeyRound },
  { label: '外观', icon: Palette },
]
</script>

<template>
  <nav class="settings-nav" aria-label="设置导航">
    <div class="settings-nav__heading">
      <span>控制台</span>
      <strong>设置</strong>
    </div>
    <ul>
      <li v-for="item in items" :key="item.label">
        <a href="#device-settings" :aria-current="item.active ? 'page' : undefined" @click.prevent>
          <component :is="item.icon" :size="16" aria-hidden="true" />
          {{ item.label }}
        </a>
      </li>
    </ul>
    <p>所有控制均由当前设备上的 Codex Desktop owner 执行。</p>
  </nav>
</template>

<style scoped>
.settings-nav {
  display: flex;
  min-height: 0;
  flex: 1;
  flex-direction: column;
  padding: 14px 10px;
}

.settings-nav__heading {
  display: grid;
  padding: 5px 8px 16px;
}

.settings-nav__heading span {
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.settings-nav__heading strong {
  font-size: 18px;
}

.settings-nav ul {
  display: grid;
  gap: 2px;
  padding: 0;
  margin: 0;
  list-style: none;
}

.settings-nav a {
  display: flex;
  min-height: 38px;
  align-items: center;
  gap: 10px;
  padding: 8px 10px;
  border-radius: var(--radius-control);
  color: var(--text-secondary);
  text-decoration: none;
}

@media (max-width: 599px) {
  .settings-nav a {
    min-height: 44px;
  }
}

.settings-nav a:hover {
  background: var(--bg-elevated);
}

.settings-nav a[aria-current='page'] {
  background: var(--accent-soft);
  color: var(--text-primary);
  font-weight: 650;
}

.settings-nav p {
  margin: auto 8px 0;
  color: var(--text-muted);
  font-size: 11px;
}
</style>
