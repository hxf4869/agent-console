import { describe, expect, it } from 'vitest'

import { countPatchChanges, parsePatch } from './git-diff'

const PATCH = [
  'diff --git a/app.py b/app.py',
  'index 1a2b3c4..5d6e7f8 100644',
  '--- a/app.py',
  '+++ b/app.py',
  '@@ -8,4 +8,5 @@ def run():',
  ' context line',
  '-old line',
  '+new line',
  '+extra line',
  ' tail context',
].join('\n')

describe('git diff display parsing', () => {
  it('keeps the real hunk header instead of a fabricated one', () => {
    const rows = parsePatch(PATCH)
    expect(rows[0]).toEqual({ kind: 'hunk', text: '@@ -8,4 +8,5 @@ def run():' })
    // 文件头与 ---/+++ 行不进入展示。
    expect(rows.some((row) => row.text.startsWith('diff --git') || row.text.startsWith('+++'))).toBe(false)
  })

  it('numbers add/remove lines from the actual hunk header', () => {
    const rows = parsePatch(PATCH)
    expect(rows.find((row) => row.kind === 'remove')).toMatchObject({ oldLine: 9, text: 'old line' })
    expect(rows.find((row) => row.kind === 'add')).toMatchObject({ newLine: 9, text: 'new line' })
  })

  it('counts per-file additions and deletions from the parsed rows', () => {
    expect(countPatchChanges(parsePatch(PATCH))).toEqual({ additions: 2, deletions: 1 })
    // Diff 未加载(空 patch)时不虚造增删。
    expect(countPatchChanges(parsePatch(''))).toEqual({ additions: 0, deletions: 0 })
  })

  it('keeps in-hunk code lines starting with -- and ++ visible with line numbers', () => {
    // 反例:--counter; 改为 ++counter; 时 hunk 正文为 ---counter;/+++counter;。
    // 文件头过滤必须按上下文区分:hunk 内的 ---/+++ 是合法代码,不是元信息。
    const patch = [
      'diff --git a/counter.cpp b/counter.cpp',
      '--- a/counter.cpp',
      '+++ b/counter.cpp',
      '@@ -1 +1 @@',
      '---counter;',
      '+++counter;',
    ].join('\n')
    const rows = parsePatch(patch)
    expect(rows[0]).toEqual({ kind: 'hunk', text: '@@ -1 +1 @@' })
    expect(rows.find((row) => row.kind === 'remove')).toMatchObject({
      oldLine: 1,
      text: '--counter;',
    })
    expect(rows.find((row) => row.kind === 'add')).toMatchObject({
      newLine: 1,
      text: '++counter;',
    })
    expect(countPatchChanges(rows)).toEqual({ additions: 1, deletions: 1 })
  })

  it('still drops file headers between hunks of a multi-file patch', () => {
    const patch = [
      'diff --git a/one.txt b/one.txt',
      'index 1a2b3c4..5d6e7f8 100644',
      '--- a/one.txt',
      '+++ b/one.txt',
      '@@ -1 +1 @@',
      '-old',
      '+new',
      'diff --git a/two.txt b/two.txt',
      'index 9e8f7a6..5d4c3b2 100644',
      '--- a/two.txt',
      '+++ b/two.txt',
      '@@ -2 +2 @@',
      '-gone',
      '+here',
    ].join('\n')
    const rows = parsePatch(patch)
    expect(rows.filter((row) => row.kind === 'hunk')).toHaveLength(2)
    expect(
      rows.some((row) => row.text.startsWith('diff --git') || row.text.startsWith('index ')),
    ).toBe(false)
    expect(countPatchChanges(rows)).toEqual({ additions: 2, deletions: 2 })
    // 第二个文件的 hunk 头重置行号。
    expect(rows.find((row) => row.kind === 'remove' && row.text === 'gone')).toMatchObject({
      oldLine: 2,
    })
  })

  it('recognizes a second hunk of the same file and resets its line numbers', () => {
    // 同一文件的第二个 hunk:行首 @@ 在 hunk 内也必须按 hunk 头识别,
    // 第二处修改的行号来自新 hunk 头(100),不是延续上一 hunk 的计数。
    const patch = [
      'diff --git a/a.txt b/a.txt',
      '--- a/a.txt',
      '+++ b/a.txt',
      '@@ -1 +1 @@',
      '-old1',
      '+new1',
      '@@ -100 +100 @@',
      '-old100',
      '+new100',
    ].join('\n')
    const rows = parsePatch(patch)
    expect(rows.filter((row) => row.kind === 'hunk')).toHaveLength(2)
    expect(rows.find((row) => row.text === 'new100')?.newLine).toBe(100)
    expect(rows.find((row) => row.text === 'old100')?.oldLine).toBe(100)
    expect(countPatchChanges(rows)).toEqual({ additions: 2, deletions: 2 })
  })
})
