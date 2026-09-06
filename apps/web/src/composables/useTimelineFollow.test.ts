import { describe, expect, it } from 'vitest'

import { createTimelineFollow, isNearBottom } from './useTimelineFollow'

describe('timeline follow (UX-04)', () => {
  const pinned = { scrollTop: 900, clientHeight: 300, scrollHeight: 1200 }
  const readingUp = { scrollTop: 120, clientHeight: 300, scrollHeight: 1200 }

  it('treats positions near the bottom as pinned and higher positions as reading', () => {
    expect(isNearBottom(pinned)).toBe(true)
    expect(isNearBottom(readingUp)).toBe(false)
    expect(isNearBottom({ scrollTop: 0, clientHeight: 300, scrollHeight: 0 })).toBe(true)
  })

  it('auto-scrolls only while pinned and counts missed output while reading up', () => {
    const follow = createTimelineFollow()

    // 贴底时新输出到达:自动跟随(调用方执行滚动)。
    expect(follow.onArrival({ ...pinned })).toBe(true)

    // 用户向上阅读:持续输出期间不被拉回底部,只累计提示。
    follow.onScroll({ ...readingUp })
    expect(follow.onArrival({ ...readingUp })).toBe(false)
    expect(follow.onArrival({ ...readingUp })).toBe(false)
    expect(follow.newOutputCount.value).toBe(2)
    expect(follow.pinnedToBottom.value).toBe(false)
  })

  it('jumping back to the bottom clears the new-output indicator', () => {
    const follow = createTimelineFollow()
    follow.onScroll({ ...readingUp })
    follow.onArrival({ ...readingUp })
    expect(follow.newOutputCount.value).toBe(1)

    const view = { ...readingUp }
    follow.jumpToBottom(view)
    expect(view.scrollTop).toBe(1200)
    expect(follow.pinnedToBottom.value).toBe(true)
    expect(follow.newOutputCount.value).toBe(0)
  })

  it('scrolling back near the bottom re-pins and clears the counter', () => {
    const follow = createTimelineFollow()
    follow.onScroll({ ...readingUp })
    follow.onArrival({ ...readingUp })
    follow.onScroll({ scrollTop: 1100, clientHeight: 300, scrollHeight: 1200 })
    expect(follow.pinnedToBottom.value).toBe(true)
    expect(follow.newOutputCount.value).toBe(0)
  })
})
