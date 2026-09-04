import { createMemoryHistory, createRouter } from 'vue-router'
import { describe, expect, it } from 'vitest'

import { routes } from './index'

function testRouter() {
  return createRouter({ history: createMemoryHistory('/agent-console/'), routes })
}

describe('documented deep links', () => {
  it('routes a pairing code to the devices page without dropping the query', async () => {
    const router = testRouter()
    await router.push('/pair?code=123456#confirm')

    expect(router.currentRoute.value).toMatchObject({
      name: 'devices',
      path: '/devices',
      query: { code: '123456' },
      hash: '#confirm',
    })
  })

  it('routes a compact session link to the task detail', async () => {
    const router = testRouter()
    await router.push('/s/session-123?tab=output')

    expect(router.currentRoute.value).toMatchObject({
      name: 'task-detail',
      path: '/tasks/session-123',
      params: { sessionId: 'session-123' },
      query: { tab: 'output' },
    })
  })
})
