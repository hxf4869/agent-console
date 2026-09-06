<script setup lang="ts">
import { AlertTriangle, FolderTree, LoaderCircle, Tags, X } from 'lucide-vue-next'
import { computed, ref, watch } from 'vue'

import UiButton from '@/components/UiButton.vue'
import {
  buildItemPayload,
  fetchToolboxOrganization,
  newIdempotencyKey,
  saveSnippetToToolbox,
  ToolboxAuthError,
  type ToolboxFolder,
  type ToolboxSaveDraft,
  type ToolboxSensitivity,
  type ToolboxTag,
} from '@/lib/toolbox-save'

const props = defineProps<{
  open: boolean
  /** 用户选中的片段;取消时不会发生任何写入。 */
  snippet: { text: string; agentDisplay: string; projectDisplay: string } | null
}>()
const emit = defineEmits<{ close: []; saved: [message: string] }>()

const SENSITIVITY_OPTIONS: Array<{ value: ToolboxSensitivity; label: string; hint: string }> = [
  { value: 'sensitive', label: '敏感', hint: '仅所有者可读（默认，Console 输出建议保持）' },
  { value: 'normal', label: '常规', hint: '可被带 sensitive:read 范围的 Token 检索' },
  { value: 'unknown', label: '未定', hint: '按敏感处理，之后可在 Toolbox 中调整' },
]

const title = ref('')
const body = ref('')
const sensitivity = ref<ToolboxSensitivity>('sensitive')
const folderId = ref('')
const selectedTagIds = ref<string[]>([])
const folders = ref<ToolboxFolder[]>([])
const tags = ref<ToolboxTag[]>([])
const organizationLoading = ref(false)
const saving = ref(false)
const errorMessage = ref('')
/**
 * 同一次逻辑保存的幂等状态:响应丢失后的重试复用原 key 和原 payload,
 * 避免服务端重复写入;用户编辑内容(payload 变化)视为新保存,换新 key。
 * 每次打开弹层(新的逻辑保存)重新发行。
 */
let issuedSave: { idempotencyKey: string; payloadJson: string } | undefined

watch(
  () => props.open,
  (open) => {
    if (!open) return
    issuedSave = undefined
    // 确认前内存草稿:每次打开从选中片段重建,不写 URL/localStorage(UX-09)。
    title.value = firstLine(props.snippet?.text ?? '').slice(0, 60) || 'Console 片段'
    body.value = props.snippet?.text ?? ''
    sensitivity.value = 'sensitive'
    folderId.value = ''
    selectedTagIds.value = []
    errorMessage.value = ''
    void loadOrganization()
  },
  { immediate: true },
)

const canSave = computed(() => Boolean(body.value.trim()) && !saving.value)

function firstLine(text: string): string {
  return text.trim().split('\n')[0]?.trim() ?? ''
}

async function loadOrganization(): Promise<void> {
  organizationLoading.value = true
  try {
    const organization = await fetchToolboxOrganization()
    folders.value = organization.folders
    tags.value = organization.tags
  } finally {
    organizationLoading.value = false
  }
}

function toggleTag(id: string): void {
  const index = selectedTagIds.value.indexOf(id)
  if (index >= 0) selectedTagIds.value.splice(index, 1)
  else if (selectedTagIds.value.length < 30) selectedTagIds.value.push(id)
}

async function save(): Promise<void> {
  if (!props.snippet || !canSave.value) return
  saving.value = true
  errorMessage.value = ''
  const draft: ToolboxSaveDraft = {
    // 提交用户确认弹层里实际编辑后的正文,绝不是原始选中片段:
    // 删除过的内容(如敏感行)不得进入保存请求(UX-09)。
    snippet: { ...props.snippet, text: body.value },
    title: title.value.trim() || 'Console 片段',
    sensitivity: sensitivity.value,
    ...(folderId.value ? { folderId: folderId.value } : {}),
    tagIds: [...selectedTagIds.value],
  }
  // 幂等键绑定确切 payload:重试同内容复用原 key;内容/标题/敏感性等
  // 任一变化都换新 key,不得混用旧键(UX-09)。
  const payloadJson = JSON.stringify(buildItemPayload(draft))
  if (!issuedSave || issuedSave.payloadJson !== payloadJson) {
    issuedSave = { idempotencyKey: newIdempotencyKey(), payloadJson }
  }
  try {
    const saved = await saveSnippetToToolbox(draft, { idempotencyKey: issuedSave.idempotencyKey })
    emit('saved', `已保存「${saved.title}」到工具箱（${saved.sensitivity}）。`)
    emit('close')
  } catch (error) {
    if (error instanceof ToolboxAuthError) errorMessage.value = error.message
    else errorMessage.value = error instanceof Error ? error.message : '保存失败，请稍后重试。'
  } finally {
    saving.value = false
  }
}
</script>

<template>
  <template v-if="open">
    <button class="toolbox-scrim" type="button" aria-label="关闭保存弹层" @click="emit('close')" />
    <section
      class="toolbox-dialog"
      role="dialog"
      aria-modal="true"
      aria-labelledby="toolbox-dialog-title"
    >
      <header>
        <div>
          <strong id="toolbox-dialog-title">保存到工具箱</strong>
          <span>确认前仅驻留内存；取消不会写入任何数据。</span>
        </div>
        <button type="button" aria-label="关闭保存弹层" @click="emit('close')">
          <X :size="18" />
        </button>
      </header>

      <div class="toolbox-dialog__body">
        <label class="toolbox-field">
          <span>标题</span>
          <input v-model="title" type="text" maxlength="200" />
        </label>

        <fieldset class="toolbox-field">
          <legend>敏感性（沿用 Toolbox 规则）</legend>
          <label v-for="option in SENSITIVITY_OPTIONS" :key="option.value" class="toolbox-radio">
            <input v-model="sensitivity" type="radio" name="toolbox-sensitivity" :value="option.value" />
            <span><strong>{{ option.label }}</strong><small>{{ option.hint }}</small></span>
          </label>
        </fieldset>

        <label class="toolbox-field">
          <span><FolderTree :size="13" aria-hidden="true" />目录（可选）</span>
          <select v-model="folderId">
            <option value="">不放入目录</option>
            <option v-for="folder in folders" :key="folder.id" :value="folder.id">{{ folder.name }}</option>
          </select>
        </label>

        <fieldset class="toolbox-field">
          <legend><Tags :size="13" aria-hidden="true" />标签（可选，最多 30 个）</legend>
          <div v-if="organizationLoading" class="toolbox-loading"><LoaderCircle class="spin" :size="14" aria-hidden="true" />读取目录与标签…</div>
          <div v-else-if="!tags.length" class="toolbox-loading">暂无标签，可直接保存。</div>
          <div v-else class="toolbox-tags">
            <button
              v-for="tag in tags"
              :key="tag.id"
              type="button"
              :aria-pressed="selectedTagIds.includes(tag.id)"
              @click="toggleTag(tag.id)"
            >
              {{ tag.name }}
            </button>
          </div>
        </fieldset>

        <label class="toolbox-field">
          <span>待保存内容（仅选中片段）</span>
          <textarea v-model="body" rows="6" />
        </label>

        <p class="toolbox-source">
          来源：{{ snippet?.agentDisplay ?? '—' }} · 项目 {{ snippet?.projectDisplay ?? '—' }}；不含绝对路径与审批凭据。
        </p>

        <p v-if="errorMessage" class="toolbox-error" role="alert">
          <AlertTriangle :size="14" aria-hidden="true" />{{ errorMessage }}
        </p>
      </div>

      <footer>
        <UiButton variant="secondary" :disabled="saving" @click="emit('close')">取消</UiButton>
        <UiButton variant="primary" :disabled="!canSave" @click="save">
          <template #icon><LoaderCircle v-if="saving" class="spin" aria-hidden="true" /></template>
          {{ saving ? '保存中…' : '确认保存' }}
        </UiButton>
      </footer>
    </section>
  </template>
</template>

<style scoped>
.toolbox-scrim {
  position: fixed;
  z-index: calc(var(--layer-overlay) - 1);
  inset: 0;
  display: block;
  width: 100%;
  border: 0;
  background: var(--scrim);
}

.toolbox-dialog {
  position: fixed;
  z-index: var(--layer-overlay);
  top: 50%;
  left: 50%;
  display: grid;
  width: min(560px, calc(100vw - 24px));
  max-height: min(86dvh, 760px);
  grid-template-rows: auto minmax(0, 1fr) auto;
  overflow: hidden;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-dialog);
  background: var(--bg-elevated);
  box-shadow: var(--shadow-overlay);
  transform: translate(-50%, -50%);
}

.toolbox-dialog > header {
  display: flex;
  min-height: 56px;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 8px 10px 8px 16px;
  border-bottom: 1px solid var(--border-subtle);
}

.toolbox-dialog > header strong {
  font-size: 14px;
}

.toolbox-dialog > header span {
  display: block;
  color: var(--text-muted);
  font-size: 10px;
}

.toolbox-dialog > header button {
  display: grid;
  width: 40px;
  height: 40px;
  padding: 0;
  place-items: center;
  border: 0;
  border-radius: var(--radius-control);
  background: transparent;
  color: var(--text-muted);
}

.toolbox-dialog__body {
  display: grid;
  gap: 13px;
  overflow: auto;
  padding: 14px 16px;
}

.toolbox-field {
  display: grid;
  gap: 6px;
}

.toolbox-field > span,
.toolbox-field > legend {
  color: var(--text-muted);
  font-size: 11px;
  font-weight: 700;
}

.toolbox-field > span {
  display: inline-flex;
  align-items: center;
  gap: 5px;
}

.toolbox-field input[type='text'],
.toolbox-field select,
.toolbox-field textarea {
  width: 100%;
  padding: 8px 10px;
  border: 1px solid var(--border-strong);
  border-radius: var(--radius-control);
  outline: 0;
  background: var(--bg-surface);
  color: var(--text-primary);
  font-size: 12px;
  line-height: 1.5;
}

.toolbox-field textarea {
  font-family: var(--font-mono);
  white-space: pre-wrap;
}

.toolbox-field:focus-within input,
.toolbox-field:focus-within select,
.toolbox-field:focus-within textarea {
  border-color: var(--focus-ring);
}

.toolbox-field fieldset {
  display: grid;
  gap: 6px;
  padding: 0;
  margin: 0;
  border: 0;
}

.toolbox-radio {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  align-items: center;
  gap: 8px;
  padding: 6px 8px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-control);
}

.toolbox-radio:has(input:checked) {
  border-color: var(--accent);
  background: var(--accent-soft);
}

.toolbox-radio span {
  display: grid;
  min-width: 0;
}

.toolbox-radio small {
  color: var(--text-muted);
  font-size: 10px;
}

.toolbox-loading {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--text-muted);
  font-size: 11px;
}

.toolbox-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.toolbox-tags button {
  min-height: 28px;
  padding: 3px 10px;
  border: 1px solid var(--border-subtle);
  border-radius: 999px;
  background: var(--bg-surface);
  color: var(--text-secondary);
  font-size: 11px;
}

.toolbox-tags button[aria-pressed='true'] {
  border-color: var(--accent);
  background: var(--accent-soft);
  color: var(--accent);
}

.toolbox-source {
  margin: 0;
  color: var(--text-muted);
  font-size: 10px;
}

.toolbox-error {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0;
  color: var(--danger);
  font-size: 11px;
}

.toolbox-dialog > footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  padding: 10px 16px;
  border-top: 1px solid var(--border-subtle);
}

.spin {
  animation: spin 0.9s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 599px) {
  .toolbox-dialog {
    width: calc(100vw - 16px);
  }

  .toolbox-dialog > footer > .ui-button {
    flex: 1;
    min-height: 44px;
  }
}
</style>
