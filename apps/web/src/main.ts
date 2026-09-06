import { createApp } from 'vue'

import App from './App.vue'
import './design/tokens.css'
import './design/global.css'
import router from './router'
import { initPushOpenHandling, type PushOpenStore } from './composables/usePushNotifications'
import { useConsoleStore } from './store/console'

createApp(App).use(router).mount('#app')

if (import.meta.env.PROD && 'serviceWorker' in navigator) {
  window.addEventListener('load', () => {
    void navigator.serviceWorker.register(`${import.meta.env.BASE_URL}sw.js`, {
      scope: import.meta.env.BASE_URL,
    })
  })
}

// 后台推送打开(UX-06):SW 消息 → 会话数据就绪后核对等待类请求是否已关闭。
// 授权与订阅在设备页由用户点击触发;这里只负责打开后的解释性提示。
const pushOpenStore: PushOpenStore = (() => {
  const store = useConsoleStore()
  return {
    isOnline: () => store.state.connection === 'ONLINE',
    isSessionKnown: (sessionId) => store.state.sessions.some((item) => item.id === sessionId),
    hasValidAttention: (sessionId, kind) => {
      const expected = kind === 'waitingQuestion' ? 'USER_QUESTION' : 'RISK_APPROVAL'
      return (store.state.runtimes[sessionId]?.attention ?? []).some(
        (item) => item.valid && item.kind === expected,
      )
    },
    notifyClosed: (message) => store.notify(message),
  }
})()
initPushOpenHandling(pushOpenStore)
