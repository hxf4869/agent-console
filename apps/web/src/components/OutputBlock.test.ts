import { mount } from '@vue/test-utils'
import { describe, expect, it, vi } from 'vitest'

import OutputBlock from './OutputBlock.vue'
import { utf8Length } from '@/transport/output-reducer'
import type { OutputState } from '@/transport/types'

function output(overrides: Partial<OutputState> = {}): OutputState {
  return {
    itemId: 'item-1',
    revision: 1,
    text: 'a line of output',
    byteLength: utf8Length('a line of output'),
    isFinal: true,
    authority: 'AUTHORITATIVE_FINAL',
    hasGap: false,
    ...overrides,
  }
}

describe('output block (UX-04 / UX-09 entry)', () => {
  it('starts collapsed by default and expands on demand', async () => {
    const wrapper = mount(OutputBlock, { props: { output: output(), defaultCollapsed: true } })
    expect(wrapper.find('pre').exists()).toBe(false)
    expect(wrapper.text()).toContain('输出已折叠')

    await wrapper.get('button[aria-label="展开输出"]').trigger('click')
    expect(wrapper.find('pre').exists()).toBe(true)
    expect(wrapper.text()).toContain('a line of output')
  })

  it('expands by default when not told to collapse', () => {
    const wrapper = mount(OutputBlock, { props: { output: output() } })
    expect(wrapper.find('pre').exists()).toBe(true)
  })

  it('labels windowed output as partial and complete output as full before copying', () => {
    const windowed = mount(OutputBlock, {
      props: {
        output: output({ isFinal: false, authority: 'LIVE_PREVIEW', byteLength: 10_000 }),
      },
    })
    const windowedCopy = windowed.findAll('button').find((button) => button.text().includes('复制当前显示'))
    expect(windowedCopy).toBeDefined()
    expect(windowed.text()).toContain('仅保留当前窗口内容')

    const complete = mount(OutputBlock, { props: { output: output() } })
    expect(complete.findAll('button').find((button) => button.text().includes('复制完整输出'))).toBeDefined()
    expect(complete.text()).toContain('当前显示即完整可取内容')
  })

  it('emits the selected text to the toolbox save dialog', async () => {
    const wrapper = mount(OutputBlock, { props: { output: output() } })
    await wrapper.get('button[aria-label="保存到工具箱"]').trigger('click')
    expect(wrapper.emitted('save')?.[0]).toEqual(['a line of output'])
  })
})
