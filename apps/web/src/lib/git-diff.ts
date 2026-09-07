/** Git 统一 Diff 的展示解析:真实 hunk 头与增删行统计(§23.1 只读 Git)。 */

export interface PatchRow {
  kind: 'context' | 'add' | 'remove' | 'hunk'
  oldLine?: number
  newLine?: number
  text: string
}

export function parsePatch(patch: string): PatchRow[] {
  const rows: PatchRow[] = []
  let oldLine = 0
  let newLine = 0
  // 是否已进入 hunk 正文:文件头(diff --git/index/---/+++)只存在于首个
  // hunk 头之前或两个文件头之间;hunk 内以 ---/+++ 开头的行是合法的
  // 删/增代码(如 --counter; → ++counter;),不得当元信息过滤。
  let inHunk = false
  for (const line of patch.split('\n')) {
    // hunk 头在任何位置都识别:同一文件的第二个 @@ 必须重置行号并展示
    // (hunk 正文行恒以空格/+/-/\\ 开头,行首 @@ 只可能是新 hunk 头)。
    if (line.startsWith('@@')) {
      const match = line.match(/@@ -(\d+)(?:,\d+)? \+(\d+)/)
      oldLine = Number(match?.[1] ?? 0)
      newLine = Number(match?.[2] ?? 0)
      inHunk = true
      rows.push({ kind: 'hunk', text: line })
      continue
    }
    if (line.startsWith('diff --git')) {
      inHunk = false
      continue
    }
    if (!inHunk) {
      // index/---/+++ 等文件头元信息不进入展示。
      continue
    }
    if (line.startsWith('+')) {
      rows.push({ kind: 'add', newLine: newLine++, text: line.slice(1) })
    } else if (line.startsWith('-')) {
      rows.push({ kind: 'remove', oldLine: oldLine++, text: line.slice(1) })
    } else {
      rows.push({
        kind: 'context',
        oldLine: oldLine++,
        newLine: newLine++,
        text: line.startsWith(' ') ? line.slice(1) : line,
      })
    }
  }
  return rows
}

/** 单文件增删行数来自实际 Diff 的 +/- 行;Diff 未加载时为 0。 */
export function countPatchChanges(rows: PatchRow[]): { additions: number; deletions: number } {
  let additions = 0
  let deletions = 0
  for (const row of rows) {
    if (row.kind === 'add') additions += 1
    else if (row.kind === 'remove') deletions += 1
  }
  return { additions, deletions }
}
