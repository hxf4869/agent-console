import { mount } from '@vue/test-utils'
import { defineComponent, h, nextTick, ref } from 'vue'
import { afterEach, describe, expect, it } from 'vitest'

import { useBottomSheetFocus } from '@/composables/useBottomSheetFocus'

const Harness = defineComponent({
  setup() {
    const fallback = ref<HTMLElement | null>(null)
    const sheet = useBottomSheetFocus(() => fallback.value)

    return () =>
      h('div', [
        h('button', { ref: fallback, onClick: (event: MouseEvent) => sheet.openSheet(event.currentTarget) }, '打开'),
        h(
          'section',
          {
            ref: sheet.sheetElement,
            tabindex: -1,
            role: 'dialog',
            'aria-modal': 'true',
            onKeydown: sheet.onSheetKeydown,
          },
          '操作层',
        ),
      ])
  },
})

describe('useBottomSheetFocus', () => {
  afterEach(() => {
    document.body.innerHTML = ''
  })

  it('打开后把焦点移入操作层，Escape 关闭后恢复到触发按钮', async () => {
    const wrapper = mount(Harness, { attachTo: document.body })
    const trigger = wrapper.get('button')
    const sheet = wrapper.get('[role="dialog"]')

    await trigger.trigger('click')
    await nextTick()
    expect(document.activeElement).toBe(sheet.element)

    await sheet.trigger('keydown', { key: 'Escape' })
    await nextTick()
    expect(document.activeElement).toBe(trigger.element)

    wrapper.unmount()
  })
})
