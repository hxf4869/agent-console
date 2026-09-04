<script setup lang="ts">
import { onBeforeUnmount, onMounted } from 'vue'

import AppShell from '@/components/AppShell.vue'
import ToastRegion from '@/components/ToastRegion.vue'
import { useConsoleStore } from '@/store/console'

const consoleStore = useConsoleStore()

onMounted(() => void consoleStore.initialize())
onBeforeUnmount(() => consoleStore.dispose())
</script>

<template>
  <AppShell>
    <RouterView />
  </AppShell>
  <ToastRegion />
</template>
