import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'

export const routes: RouteRecordRaw[] = [
  {
    path: '/',
    name: 'attention',
    component: () => import('@/views/AttentionView.vue'),
  },
  {
    path: '/tasks',
    name: 'tasks',
    component: () => import('@/views/TasksView.vue'),
  },
  {
    path: '/tasks/:sessionId',
    name: 'task-detail',
    component: () => import('@/views/TaskDetailView.vue'),
  },
  {
    path: '/tasks/:sessionId/git',
    name: 'task-git',
    component: () => import('@/views/GitWorkspaceView.vue'),
  },
  {
    path: '/devices',
    name: 'devices',
    component: () => import('@/views/DevicesView.vue'),
  },
  {
    path: '/s/:sessionId',
    redirect: (to) => ({
      name: 'task-detail',
      params: { sessionId: to.params.sessionId },
      query: to.query,
      hash: to.hash,
    }),
  },
  {
    path: '/pair',
    redirect: (to) => ({ name: 'devices', query: to.query, hash: to.hash }),
  },
  { path: '/settings', redirect: '/devices' },
  { path: '/:pathMatch(.*)*', redirect: '/' },
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
  scrollBehavior: () => ({ top: 0 }),
})

export default router
