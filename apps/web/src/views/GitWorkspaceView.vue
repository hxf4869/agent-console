<script setup lang="ts">
import {
  ArrowLeft,
  Braces,
  ChevronRight,
  CircleDot,
  FileCode2,
  Files,
  GitBranch,
  GitCompareArrows,
} from 'lucide-vue-next'
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import NoticeBanner from '@/components/NoticeBanner.vue'
import StatusBadge from '@/components/StatusBadge.vue'
import { useConsoleStore } from '@/store/console'
import type { FileMetadata, GitFileDiff, GitSummary } from '@/transport/types'

interface ChangedFile {
  path: string
  status: 'M' | 'A'
  additions: number
  deletions: number
  language: string
  diff: Array<{ kind: 'context' | 'add' | 'remove'; oldLine?: number; newLine?: number; text: string }>
  preview: string[]
}

const fixtureFiles: ChangedFile[] = [
  {
    path: 'apps/web/src/views/TaskDetailView.vue',
    status: 'A',
    additions: 286,
    deletions: 0,
    language: 'Vue',
    diff: [
      { kind: 'context', oldLine: 0, newLine: 1, text: '<script setup lang="ts">' },
      { kind: 'add', newLine: 2, text: "import { computed, ref, watch } from 'vue'" },
      { kind: 'add', newLine: 3, text: "import { useRoute } from 'vue-router'" },
      { kind: 'context', oldLine: 0, newLine: 4, text: '' },
      { kind: 'add', newLine: 5, text: "import QueueComposer from '@/components/QueueComposer.vue'" },
      { kind: 'add', newLine: 6, text: "import TimelineItemCard from '@/components/TimelineItemCard.vue'" },
      { kind: 'context', oldLine: 0, newLine: 7, text: '' },
      { kind: 'add', newLine: 8, text: 'const route = useRoute()' },
      { kind: 'add', newLine: 9, text: "const sessionId = computed(() => String(route.params.sessionId))" },
      { kind: 'add', newLine: 10, text: 'const loading = ref(true)' },
    ],
    preview: [
      '<script setup lang="ts">',
      "import { computed, ref, watch } from 'vue'",
      "import { useRoute } from 'vue-router'",
      '',
      "import QueueComposer from '@/components/QueueComposer.vue'",
      "import TimelineItemCard from '@/components/TimelineItemCard.vue'",
      '',
      'const route = useRoute()',
      "const sessionId = computed(() => String(route.params.sessionId))",
      'const loading = ref(true)',
      '</' + 'script>',
    ],
  },
  {
    path: 'apps/web/src/transport/output-reducer.ts',
    status: 'M',
    additions: 31,
    deletions: 5,
    language: 'TypeScript',
    diff: [
      { kind: 'context', oldLine: 8, newLine: 8, text: 'if (event.itemId !== state.itemId) return state' },
      { kind: 'context', oldLine: 9, newLine: 9, text: '' },
      { kind: 'remove', oldLine: 10, text: "if (event.type === 'append') return append(state, event.text)" },
      { kind: 'add', newLine: 10, text: "if (event.type === 'append') {" },
      { kind: 'add', newLine: 11, text: '  if (state.isFinal || event.expectedOffset !== state.byteLength) {' },
      { kind: 'add', newLine: 12, text: '    return { ...state, hasGap: true }' },
      { kind: 'add', newLine: 13, text: '  }' },
      { kind: 'add', newLine: 14, text: '  const text = `${state.text}${event.text}`' },
      { kind: 'add', newLine: 15, text: "  return { ...state, text, authority: 'LIVE_PREVIEW' }" },
      { kind: 'add', newLine: 16, text: '}' },
    ],
    preview: [
      "import type { OutputEvent, OutputState } from './types'",
      '',
      'export function reduceOutput(state: OutputState, event: OutputEvent): OutputState {',
      '  if (event.itemId !== state.itemId) return state',
      '',
      "  if (event.type === 'append') {",
      '    if (state.isFinal || event.expectedOffset !== state.byteLength) {',
      '      return { ...state, hasGap: true }',
      '    }',
      '  }',
      '}',
    ],
  },
  {
    path: 'apps/web/src/design/tokens.css',
    status: 'A',
    additions: 78,
    deletions: 0,
    language: 'CSS',
    diff: [
      { kind: 'context', oldLine: 0, newLine: 1, text: ':root,' },
      { kind: 'add', newLine: 2, text: ":root[data-theme='dark'] {" },
      { kind: 'add', newLine: 3, text: '  color-scheme: dark;' },
      { kind: 'add', newLine: 4, text: '  --bg-app: #151716;' },
      { kind: 'add', newLine: 5, text: '  --bg-surface: #1f2220;' },
      { kind: 'add', newLine: 6, text: '  --text-primary: #f1f4f2;' },
      { kind: 'add', newLine: 7, text: '  --accent: #6e9bff;' },
      { kind: 'context', oldLine: 0, newLine: 8, text: '}' },
    ],
    preview: [
      ':root,',
      ":root[data-theme='dark'] {",
      '  color-scheme: dark;',
      '  --bg-app: #151716;',
      '  --bg-surface: #1f2220;',
      '  --text-primary: #f1f4f2;',
      '  --accent: #6e9bff;',
      '}',
    ],
  },
]

const route = useRoute()
const {
  state,
  presenceFor,
  getGitSummary,
  getGitDiff,
  getFileMetadata,
  previewFile,
  downloadFile,
  uploadFile,
} = useConsoleStore()
const sessionId = computed(() => String(route.params.sessionId))
const session = computed(() => state.sessions.find((item) => item.id === sessionId.value))
const presence = computed(() => presenceFor(sessionId.value))
const summary = ref<GitSummary>()
const diffs = ref<Record<string, GitFileDiff>>({})
const loading = ref(false)
const errorMessage = ref('')
const selectedPath = ref('')
const uploaded = ref<FileMetadata>()
const uploadedHandle = ref('')
const transferMessage = ref('')
const uploadedPreview = ref<string[]>([])
const previewObjectUrl = ref('')
const files = computed<ChangedFile[]>(() => {
  if (state.fixtureMode) return fixtureFiles
  return (summary.value?.entries ?? []).map((entry) => {
    const diff = diffs.value[entry.relativePath]
    return {
      path: entry.relativePath,
      status: /added|untracked/i.test(entry.status) ? 'A' : 'M',
      additions: 0,
      deletions: 0,
      language: languageFor(entry.relativePath),
      diff: parsePatch(diff?.patchText ?? ''),
      preview: (diff?.patchText ?? '').split('\n'),
    }
  })
})
const selectedFile = computed(() =>
  files.value.find((file) => file.path === selectedPath.value) ?? files.value[0],
)
const mode = ref<'diff' | 'preview'>('diff')
const additions = computed(() => summary.value?.insertions ?? fixtureFiles.reduce((sum, file) => sum + file.additions, 0))
const deletions = computed(() => summary.value?.deletions ?? fixtureFiles.reduce((sum, file) => sum + file.deletions, 0))

watch(
  sessionId,
  async (id) => {
    if (state.fixtureMode) {
      selectedPath.value = fixtureFiles[0]?.path ?? ''
      return
    }
    loading.value = true
    errorMessage.value = ''
    try {
      summary.value = await getGitSummary(id)
      selectedPath.value = summary.value.entries[0]?.relativePath ?? ''
    } catch (error) {
      errorMessage.value = error instanceof Error ? error.message : '无法读取 Git 状态。'
    } finally {
      loading.value = false
    }
  },
  { immediate: true },
)

watch(selectedPath, async (path) => {
  if (!path || state.fixtureMode || diffs.value[path]) return
  try {
    const entry = summary.value?.entries.find((item) => item.relativePath === path)
    diffs.value = { ...diffs.value, [path]: await getGitDiff(sessionId.value, path, entry?.staged) }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '无法读取 Diff。'
  }
})

async function onUpload(event: Event): Promise<void> {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  transferMessage.value = '正在上传…'
  uploadedPreview.value = []
  try {
    const result = await uploadFile(sessionId.value, file)
    if (!result.uploadFileHandle) throw new Error('Bridge 未返回 upload handle。')
    uploadedHandle.value = result.uploadFileHandle
    uploaded.value = await getFileMetadata(sessionId.value, result.uploadFileHandle)
    uploadedHandle.value = uploaded.value.fileHandle
    transferMessage.value = '上传完成，可预览或下载。'
    mode.value = 'preview'
    await previewUploaded()
  } catch (error) {
    transferMessage.value = error instanceof Error ? error.message : '上传失败。'
  } finally {
    input.value = ''
  }
}

async function previewUploaded(): Promise<void> {
  if (!uploaded.value || !uploadedHandle.value) return
  const response = await previewFile(sessionId.value, uploadedHandle.value, uploaded.value.displayName)
  const blob = await response.blob()
  if (previewObjectUrl.value) URL.revokeObjectURL(previewObjectUrl.value)
  if (uploaded.value.previewKind === 'text') {
    uploadedPreview.value = (await blob.text()).split('\n')
    previewObjectUrl.value = ''
  } else {
    previewObjectUrl.value = URL.createObjectURL(blob)
    uploadedPreview.value = []
  }
  mode.value = 'preview'
}

async function downloadUploaded(): Promise<void> {
  if (!uploaded.value || !uploadedHandle.value) return
  const response = await downloadFile(sessionId.value, uploadedHandle.value, uploaded.value.displayName)
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = uploaded.value.displayName
  anchor.click()
  URL.revokeObjectURL(url)
}

onBeforeUnmount(() => {
  if (previewObjectUrl.value) URL.revokeObjectURL(previewObjectUrl.value)
})

function languageFor(path: string): string {
  return path.split('.').at(-1)?.toUpperCase() ?? 'TEXT'
}

function parsePatch(patch: string): ChangedFile['diff'] {
  let oldLine = 0
  let newLine = 0
  return patch
    .split('\n')
    .filter(
      (line) =>
        !line.startsWith('diff --git') &&
        !line.startsWith('index ') &&
        !line.startsWith('---') &&
        !line.startsWith('+++'),
    )
    .map((line) => {
      if (line.startsWith('@@')) {
        const match = line.match(/@@ -(\d+)(?:,\d+)? \+(\d+)/)
        oldLine = Number(match?.[1] ?? 0)
        newLine = Number(match?.[2] ?? 0)
        return { kind: 'context' as const, text: line }
      }
      if (line.startsWith('+')) return { kind: 'add' as const, newLine: newLine++, text: line.slice(1) }
      if (line.startsWith('-')) return { kind: 'remove' as const, oldLine: oldLine++, text: line.slice(1) }
      return {
        kind: 'context' as const,
        oldLine: oldLine++,
        newLine: newLine++,
        text: line.startsWith(' ') ? line.slice(1) : line,
      }
    })
}
</script>

<template>
  <div class="git-page">
    <header class="git-header">
      <div>
        <RouterLink :to="`/tasks/${sessionId}`" aria-label="返回任务详情"><ArrowLeft :size="17" /></RouterLink>
        <div>
          <span>只读工作区</span>
          <h1>{{ session?.title ?? '任务文件' }}</h1>
          <p><GitBranch :size="13" />{{ session?.branch ?? 'unknown' }}</p>
        </div>
      </div>
      <div class="git-summary">
        <StatusBadge>{{ files.length }} 个文件</StatusBadge>
        <span class="diff-add">+{{ additions }}</span>
        <span class="diff-remove">−{{ deletions }}</span>
      </div>
    </header>

    <NoticeBanner v-if="presence.connection !== 'ONLINE'" class="git-notice" tone="warning" title="设备离线">
      当前仅显示会话摘要；不会请求本机 Git、Diff 或文件内容。
    </NoticeBanner>
    <NoticeBanner v-else-if="errorMessage" class="git-notice" tone="warning" title="读取失败">
      {{ errorMessage }}
    </NoticeBanner>

    <div class="git-toolbar" aria-label="文件视图">
      <button type="button" :aria-pressed="mode === 'diff'" @click="mode = 'diff'">
        <GitCompareArrows :size="15" />逐文件 Diff
      </button>
      <button type="button" :aria-pressed="mode === 'preview'" @click="mode = 'preview'">
        <FileCode2 :size="15" />{{ state.fixtureMode ? '文件预览' : '上传预览' }}
      </button>
      <label class="file-upload-action">
        <span>{{ transferMessage || '上传到当前会话' }}</span>
        <input type="file" :disabled="presence.connection !== 'ONLINE'" @change="onUpload" />
      </label>
      <button v-if="uploaded" type="button" @click="previewUploaded">预览上传</button>
      <button v-if="uploaded" type="button" @click="downloadUploaded">下载上传</button>
      <label class="mobile-file-picker">
        <span class="sr-only">选择文件</span>
        <select v-model="selectedPath" aria-label="选择文件">
          <option v-for="file in files" :key="file.path" :value="file.path">{{ file.path.split('/').at(-1) }}</option>
        </select>
      </label>
    </div>

    <div class="git-workbench">
      <aside class="changed-files" aria-label="变更文件">
        <header><Files :size="15" /><strong>变更</strong><span>{{ files.length }}</span></header>
        <button
          v-for="file in files"
          :key="file.path"
          type="button"
          :class="{ 'is-selected': selectedPath === file.path }"
          @click="selectedPath = file.path"
        >
          <StatusBadge :tone="file.status === 'A' ? 'success' : 'warning'">{{ file.status }}</StatusBadge>
          <span><strong>{{ file.path.split('/').at(-1) }}</strong><small>{{ file.path }}</small></span>
          <ChevronRight :size="14" aria-hidden="true" />
        </button>
      </aside>

      <section v-if="selectedFile" class="code-pane" aria-label="文件内容">
        <header>
          <div>
            <Braces :size="15" aria-hidden="true" />
            <strong>{{ selectedFile.path }}</strong>
          </div>
          <span>{{ selectedFile.language }}</span>
        </header>

        <div v-if="mode === 'diff'" class="diff-view" role="region" aria-label="统一 Diff" tabindex="0">
          <div class="diff-hunk">@@ -8,4 +8,{{ selectedFile.diff.length }} @@</div>
          <code>
            <span v-for="(line, index) in selectedFile.diff" :key="index" :class="`diff-line diff-line--${line.kind}`">
              <b>{{ line.oldLine ?? '' }}</b><b>{{ line.newLine ?? '' }}</b><i>{{ line.kind === 'add' ? '+' : line.kind === 'remove' ? '−' : ' ' }}</i><em>{{ line.text || ' ' }}</em>
            </span>
          </code>
        </div>

        <div v-else class="preview-view" role="region" aria-label="文件预览" tabindex="0">
          <img
            v-if="previewObjectUrl && uploaded?.previewKind === 'image'"
            :src="previewObjectUrl"
            :alt="uploaded.displayName"
          />
          <iframe
            v-else-if="previewObjectUrl && uploaded?.previewKind === 'pdf'"
            :src="previewObjectUrl"
            title="上传 PDF 预览"
            sandbox=""
          />
          <code>
            <span
              v-for="(line, index) in uploadedPreview.length ? uploadedPreview : selectedFile.preview"
              :key="index"
            ><b>{{ index + 1 }}</b><em>{{ line || ' ' }}</em></span>
          </code>
        </div>
      </section>

      <aside v-if="selectedFile" class="file-context" aria-label="文件上下文">
        <section>
          <header><CircleDot :size="15" /><strong>文件状态</strong></header>
          <dl>
            <div><dt>状态</dt><dd>{{ selectedFile.status === 'A' ? '新增' : '修改' }}</dd></div>
            <div><dt>语言</dt><dd>{{ selectedFile.language }}</dd></div>
            <div><dt>新增</dt><dd class="diff-add">+{{ selectedFile.additions }}</dd></div>
            <div><dt>删除</dt><dd class="diff-remove">−{{ selectedFile.deletions }}</dd></div>
          </dl>
        </section>
      </aside>
    </div>
  </div>
</template>

<style scoped>
.git-page {
  display: grid;
  height: 100%;
  min-height: 0;
  grid-template-rows: auto auto auto minmax(0, 1fr);
}

.git-header,
.git-header > div,
.git-summary,
.git-toolbar,
.changed-files > header,
.code-pane > header,
.code-pane > header > div,
.file-context section > header {
  display: flex;
  align-items: center;
}

.git-header {
  min-height: 82px;
  justify-content: space-between;
  gap: 20px;
  padding: 12px 18px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-surface);
}

.git-header > div:first-child {
  min-width: 0;
  gap: 10px;
}

.git-header a {
  display: grid;
  width: 34px;
  height: 34px;
  flex: 0 0 auto;
  place-items: center;
  border-radius: var(--radius-control);
  color: var(--text-secondary);
}

.git-header a:hover {
  background: var(--bg-elevated);
}

.git-header > div > div {
  min-width: 0;
}

.git-header span:not(.status-badge) {
  color: var(--text-muted);
  font-size: 10px;
  font-weight: 750;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.git-header h1 {
  overflow: hidden;
  margin: 1px 0;
  font-size: 16px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.git-header p {
  display: flex;
  align-items: center;
  gap: 5px;
  margin: 0;
  color: var(--text-muted);
  font-size: 11px;
}

.git-summary {
  gap: 9px;
  font: 11px var(--font-mono);
}

.git-notice {
  margin: 10px 12px 0;
}

.diff-add {
  color: var(--success) !important;
}

.diff-remove {
  color: var(--danger) !important;
}

.git-toolbar {
  min-height: 44px;
  gap: 3px;
  padding: 5px 10px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.git-toolbar button {
  display: inline-flex;
  min-height: 32px;
  align-items: center;
  gap: 6px;
  padding: 5px 10px;
  border: 0;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--text-secondary);
  font-size: 12px;
}

.file-upload-action {
  position: relative;
  display: inline-flex;
  min-height: 32px;
  align-items: center;
  padding: 0 9px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-control);
  color: var(--text-secondary);
  cursor: pointer;
  font-size: 11px;
}

.file-upload-action input {
  position: absolute;
  width: 1px;
  height: 1px;
  opacity: 0;
}

.git-toolbar button:hover,
.git-toolbar button[aria-pressed='true'] {
  background: var(--accent-soft);
  color: var(--text-primary);
}

.mobile-file-picker {
  display: none;
}

.git-toolbar > span {
  margin-left: auto;
  color: var(--text-muted);
  font-size: 10px;
}

.git-workbench {
  display: grid;
  min-height: 0;
  grid-template-columns: minmax(210px, 260px) minmax(420px, 1fr) minmax(220px, 280px);
}

.changed-files,
.code-pane,
.file-context {
  min-width: 0;
  min-height: 0;
  overflow: auto;
}

.changed-files {
  padding: 8px;
  border-right: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.changed-files > header {
  min-height: 36px;
  gap: 7px;
  padding: 0 7px;
  color: var(--text-secondary);
}

.changed-files > header span {
  margin-left: auto;
  color: var(--text-muted);
  font-size: 10px;
}

.changed-files > button {
  display: grid;
  width: 100%;
  grid-template-columns: auto minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  padding: 9px 7px;
  border: 1px solid transparent;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--text-secondary);
  text-align: left;
}

.changed-files > button:hover,
.changed-files > button.is-selected {
  background: var(--bg-elevated);
}

.changed-files > button.is-selected {
  border-color: var(--border-subtle);
}

.changed-files button > span:nth-child(2) {
  display: grid;
  min-width: 0;
}

.changed-files button strong,
.changed-files button small {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.changed-files button strong {
  color: var(--text-primary);
  font-size: 12px;
}

.changed-files button small {
  color: var(--text-muted);
  font-size: 9px;
}

.changed-files button > svg {
  color: var(--text-muted);
}

.code-pane {
  display: grid;
  grid-template-rows: auto minmax(0, 1fr);
  background: var(--bg-code);
}

.code-pane > header {
  min-height: 42px;
  justify-content: space-between;
  gap: 10px;
  padding: 7px 12px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-surface);
}

.code-pane > header > div {
  min-width: 0;
  gap: 7px;
}

.code-pane > header strong {
  overflow: hidden;
  font: 11px var(--font-mono);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.code-pane > header > span {
  color: var(--text-muted);
  font-size: 10px;
}

.diff-view,
.preview-view {
  min-height: 0;
  overflow: auto;
  outline: 0;
  color: var(--text-secondary);
  font: 12px/1.65 var(--font-mono);
}

.preview-view > img,
.preview-view > iframe {
  display: block;
  max-width: 100%;
  max-height: 100%;
  margin: auto;
  border: 0;
}

.diff-hunk {
  padding: 6px 12px;
  border-bottom: 1px solid var(--border-subtle);
  background: color-mix(in srgb, var(--accent), transparent 90%);
  color: var(--accent);
}

.diff-view code,
.preview-view code {
  display: block;
  min-width: max-content;
  padding: 8px 0 28px;
}

.diff-line {
  display: grid;
  min-width: 100%;
  grid-template-columns: 42px 42px 24px minmax(480px, 1fr);
}

.diff-line b,
.preview-view b {
  padding: 0 8px;
  color: var(--text-muted);
  font-weight: 400;
  text-align: right;
  user-select: none;
}

.diff-line i {
  font-style: normal;
  text-align: center;
}

.diff-line em,
.preview-view em {
  padding-right: 24px;
  font-style: normal;
  white-space: pre;
}

.diff-line--add {
  background: color-mix(in srgb, var(--success), transparent 88%);
}

.diff-line--add i {
  color: var(--success);
}

.diff-line--remove {
  background: color-mix(in srgb, var(--danger), transparent 88%);
}

.diff-line--remove i {
  color: var(--danger);
}

.preview-view code > span {
  display: grid;
  min-width: 100%;
  grid-template-columns: 52px minmax(520px, 1fr);
}

.file-context {
  display: grid;
  align-content: start;
  gap: 10px;
  padding: 10px;
  border-left: 1px solid var(--border-subtle);
  background: var(--bg-nav);
}

.file-context section {
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-card);
  background: var(--bg-elevated);
}

.file-context section > header {
  min-height: 40px;
  gap: 7px;
  padding: 7px 10px;
  border-bottom: 1px solid var(--border-subtle);
}

.file-context section > header svg {
  color: var(--accent);
}

.file-context dl {
  display: grid;
  gap: 7px;
  padding: 10px;
  margin: 0;
}

.file-context dl > div {
  display: flex;
  justify-content: space-between;
  gap: 8px;
}

.file-context dt,
.file-context dd,
.file-context p {
  color: var(--text-muted);
  font-size: 11px;
}

.file-context dd {
  margin: 0;
  color: var(--text-secondary);
}

.file-context section > p {
  padding: 10px;
  margin: 0;
}

@media (max-width: 900px) {
  .git-workbench {
    grid-template-columns: minmax(190px, 230px) minmax(0, 1fr);
  }

  .file-context {
    display: none;
  }
}

@media (max-width: 759px) {
  .git-page {
    height: calc(100dvh - 56px - 62px - env(safe-area-inset-bottom));
    grid-template-rows: auto auto auto minmax(0, 1fr);
  }

  .git-header {
    min-height: 76px;
    padding: 10px 12px;
  }

  .git-header a {
    width: 44px;
    height: 44px;
  }

  .git-summary .status-badge {
    display: none;
  }

  .git-toolbar button {
    min-height: 44px;
  }

  .mobile-file-picker {
    display: block;
    min-width: 0;
    margin-left: auto;
  }

  .mobile-file-picker select {
    width: min(132px, 34vw);
    height: 44px;
    padding: 0 24px 0 8px;
    border: 1px solid var(--border-subtle);
    border-radius: var(--radius-control);
    background: var(--bg-surface);
    color: var(--text-primary);
    font-size: 12px;
  }

  .git-workbench {
    display: block;
    min-height: 0;
    overflow: hidden;
  }

  .changed-files {
    display: none;
  }

  .code-pane {
    height: 100%;
    min-height: 0;
  }

  .diff-view,
  .preview-view {
    max-width: 100vw;
  }
}
</style>
