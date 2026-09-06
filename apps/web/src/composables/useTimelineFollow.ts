import { ref, type Ref } from 'vue'

/** 滚动位置是否接近底部(UX-04):接近才自动跟随,向上阅读不拉回。 */
export function isNearBottom(
  view: { scrollTop: number; clientHeight: number; scrollHeight: number },
  threshold = 80,
): boolean {
  if (view.scrollHeight <= 0) return true
  return view.scrollHeight - view.scrollTop - view.clientHeight <= threshold
}

export interface ScrollView {
  scrollTop: number
  clientHeight: number
  scrollHeight: number
}

export interface TimelineFollow {
  pinnedToBottom: Ref<boolean>
  newOutputCount: Ref<number>
  onScroll: (view: ScrollView) => void
  /** 新内容到达:贴底时由调用方滚动,离开底部时累计未读输出。返回是否自动滚动了。 */
  onArrival: (view?: ScrollView) => boolean
  jumpToBottom: (view: ScrollView) => void
}

/**
 * 时间线跟随状态机:只有用户接近底部时新输出才自动跟随;
 * 向上阅读后显示"有新输出 · 回到底部",由用户自行决定跳回(UX-04)。
 */
export function createTimelineFollow(): TimelineFollow {
  const pinnedToBottom = ref(true)
  const newOutputCount = ref(0)

  function onScroll(view: ScrollView): void {
    pinnedToBottom.value = isNearBottom(view)
    if (pinnedToBottom.value) newOutputCount.value = 0
  }

  function onArrival(view?: ScrollView): boolean {
    if (pinnedToBottom.value) {
      if (view) view.scrollTop = view.scrollHeight
      newOutputCount.value = 0
      return true
    }
    newOutputCount.value += 1
    return false
  }

  function jumpToBottom(view: ScrollView): void {
    view.scrollTop = view.scrollHeight
    pinnedToBottom.value = true
    newOutputCount.value = 0
  }

  return { pinnedToBottom, newOutputCount, onScroll, onArrival, jumpToBottom }
}
