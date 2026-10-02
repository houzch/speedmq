<script setup lang="ts">
// 策略页：按 vhost 管理策略（pattern 匹配一批队列/交换机，统一施加一组参数）。
//
// pattern 在 RabbitMQ 里是**正则**，但运维大多只想表达"全部对象"或"以某前缀开头"，
// 因此这里与账号权限页同一套做法：先给档位（全部 / 名称前缀 / 自定义正则），
// 正则只作为只读预览展示 —— 不把"写正则"当成必答题。
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { Plus, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import { ApiError } from '@/api/client'
import type { Policy, PolicyRequest } from '@/api/types'
import { useVhostStore } from '@/stores/vhost'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { formatJson } from '@/utils/format'
import { showError } from '@/utils/message'

const vhostStore = useVhostStore()
const refresh = useRefreshStore()
const { t } = useI18n()

const policies = ref<Policy[]>([])
const loading = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    const [list] = await Promise.all([api.policies(), vhostStore.loadVhosts()])
    policies.value = list
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('policies.loadFailed'))
  } finally {
    loading.value = false
  }
}

/** 写操作错误提示：优先展示服务端原因 */
function messageOf(error: unknown): string {
  if (error instanceof ApiError) return error.reason || error.message
  if (error instanceof Error) return error.message
  return t('common.operationFailed')
}

// ---- 作用对象 ----
const APPLY_TO_OPTIONS = computed(() => [
  { value: 'queues', label: t('policies.applyAll') },
  { value: 'classic_queues', label: t('policies.applyClassic') },
  { value: 'quorum_queues', label: t('policies.applyQuorum') },
  { value: 'exchanges', label: t('policies.applyExchanges') },
  { value: 'all', label: t('policies.applyAllObjects') },
])

function applyToLabel(value: string): string {
  return APPLY_TO_OPTIONS.value.find((item) => item.value === value)?.label ?? value
}

// ---- 定义编辑器：可选项按作用对象收敛，避免写出后端必然拒绝的键 ----
const QUEUE_KEYS = [
  'max-length',
  'max-length-bytes',
  'message-ttl',
  'expires',
  'dead-letter-exchange',
  'dead-letter-routing-key',
  'overflow',
]
const EXCHANGE_KEYS = ['alternate-exchange']

/** 值必须是整数的键（其余按字符串提交） */
const NUMERIC_KEYS = ['max-length', 'max-length-bytes', 'message-ttl', 'expires']
const OVERFLOW_OPTIONS = ['drop-head', 'reject-publish', 'reject-publish-dlx']

/** 策略键的本地化名称；未知键原样返回 */
function keyLabel(key: string): string {
  switch (key) {
    case 'max-length':
      return t('policies.keyMaxLength')
    case 'max-length-bytes':
      return t('policies.keyMaxLengthBytes')
    case 'message-ttl':
      return t('policies.keyMessageTtl')
    case 'expires':
      return t('policies.keyExpires')
    case 'dead-letter-exchange':
      return t('policies.keyDlx')
    case 'dead-letter-routing-key':
      return t('policies.keyDlrk')
    case 'overflow':
      return t('policies.keyOverflow')
    case 'alternate-exchange':
      return t('policies.keyAlternateExchange')
    default:
      return key
  }
}

interface DefinitionRow {
  key: string
  value: string
}

// ---- 新建 / 编辑 ----
const dialogVisible = ref(false)
const editing = ref(false)
const submitting = ref(false)
const form = ref({
  vhost: '',
  name: '',
  applyTo: 'queues',
  priority: 0,
  /** 匹配档位：全部对象 / 名称前缀 / 自定义正则 */
  matchMode: 'all' as 'all' | 'prefix' | 'custom',
  prefix: '',
  customPattern: '',
  rows: [] as DefinitionRow[],
})

/** 当前作用对象允许的策略键 */
const allowedKeys = computed<string[]>(() =>
  form.value.applyTo === 'exchanges' ? EXCHANGE_KEYS : QUEUE_KEYS,
)

/** 最终提交的 pattern */
const finalPattern = computed<string>(() => {
  if (form.value.matchMode === 'all') return '.*'
  if (form.value.matchMode === 'custom') return form.value.customPattern
  return `^${escapeRegex(form.value.prefix)}`
})

/** 转义正则元字符，让前缀按字面量匹配 */
function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

/** 把 ^前缀 还原为原始前缀；不是"纯字面量前缀"时返回 null */
function regexToPrefix(regex: string): string | null {
  if (!regex.startsWith('^')) return null
  const body = regex.slice(1)
  const prefix = body.replace(/\\(.)/g, '$1')
  return escapeRegex(prefix) === body ? prefix : null
}

function definitionToRows(definition: Record<string, unknown>): DefinitionRow[] {
  return Object.entries(definition).map(([key, value]) => ({ key, value: String(value) }))
}

function openCreate(): void {
  editing.value = false
  form.value = {
    vhost: vhostStore.current,
    name: '',
    applyTo: 'queues',
    priority: 0,
    matchMode: 'all',
    prefix: '',
    customPattern: '',
    rows: [{ key: 'max-length', value: '' }],
  }
  dialogVisible.value = true
}

function openEdit(row: Policy): void {
  editing.value = true
  const prefix = row.pattern === '.*' ? null : regexToPrefix(row.pattern)
  form.value = {
    vhost: row.vhost,
    name: row.name,
    applyTo: row['apply-to'],
    priority: row.priority,
    matchMode: row.pattern === '.*' ? 'all' : prefix === null ? 'custom' : 'prefix',
    prefix: prefix ?? '',
    customPattern: row.pattern,
    rows: definitionToRows(row.definition),
  }
  dialogVisible.value = true
}

// 切换作用对象时丢掉已经不合法的键，避免提交后才发现被打回
watch(
  () => form.value.applyTo,
  () => {
    const allowed = allowedKeys.value
    form.value.rows = form.value.rows.filter((row) => allowed.includes(row.key))
    if (form.value.rows.length === 0) {
      form.value.rows = [{ key: allowed[0], value: '' }]
    }
  },
)

function addRow(): void {
  form.value.rows.push({ key: allowedKeys.value[0], value: '' })
}

function removeRow(index: number): void {
  form.value.rows.splice(index, 1)
}

/** 把界面上的字符串值转成后端要的类型 */
function buildDefinition(): Record<string, unknown> | null {
  const out: Record<string, unknown> = {}
  for (const row of form.value.rows) {
    if (row.value.trim() === '') continue
    if (NUMERIC_KEYS.includes(row.key)) {
      const parsed = Number(row.value.trim())
      if (!Number.isInteger(parsed)) {
        ElMessage.warning(t('policies.integerRequired', { field: keyLabel(row.key) }))
        return null
      }
      out[row.key] = parsed
      continue
    }
    out[row.key] = row.value.trim()
  }
  if (Object.keys(out).length === 0) {
    ElMessage.warning(t('policies.definitionRequired'))
    return null
  }
  return out
}

async function submit(): Promise<void> {
  const name = form.value.name.trim()
  if (!name) {
    ElMessage.warning(t('policies.nameRequired'))
    return
  }
  if (!form.value.vhost) {
    ElMessage.warning(t('users.vhostRequired'))
    return
  }
  if (form.value.matchMode === 'prefix' && !form.value.prefix.trim()) {
    ElMessage.warning(t('policies.prefixRequired'))
    return
  }
  if (form.value.matchMode === 'custom' && !form.value.customPattern.trim()) {
    ElMessage.warning(t('policies.patternRequired'))
    return
  }
  const definition = buildDefinition()
  if (definition === null) return

  const body: PolicyRequest = {
    pattern: finalPattern.value,
    'apply-to': form.value.applyTo,
    definition,
    priority: form.value.priority,
  }
  submitting.value = true
  try {
    await api.savePolicy(form.value.vhost, name, body)
    ElMessage.success(editing.value ? t('policies.updated', { name }) : t('policies.created', { name }))
    dialogVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  } finally {
    submitting.value = false
  }
}

async function removePolicy(row: Policy): Promise<void> {
  try {
    await ElMessageBox.confirm(t('policies.deleteConfirm', { name: row.name, vhost: row.vhost }), t('policies.deleteTitle'), {
      type: 'warning',
      confirmButtonText: t('common.delete'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.deletePolicy(row.vhost, row.name)
    ElMessage.success(t('policies.deleted', { name: row.name }))
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  }
}

useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">{{ t('policies.title') }}</h2>
        <div class="page-subtitle">{{ t('policies.total', { count: policies.length }) }}</div>
      </div>
      <div>
        <el-button type="primary" :icon="Plus" @click="openCreate">{{ t('policies.create') }}</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="load">{{ t('common.refresh') }}</el-button>
      </div>
    </div>

    <el-table :data="policies" stripe>
      <el-table-column :label="t('policies.colName')" min-width="150">
        <template #default="{ row }"><span class="mono">{{ row.name }}</span></template>
      </el-table-column>
      <el-table-column :label="t('policies.colVhost')" min-width="110">
        <template #default="{ row }"><span class="mono">{{ row.vhost }}</span></template>
      </el-table-column>
      <el-table-column :label="t('policies.colPattern')" min-width="160" show-overflow-tooltip>
        <template #default="{ row }"><span class="mono">{{ row.pattern }}</span></template>
      </el-table-column>
      <el-table-column :label="t('policies.colApplyTo')" width="110">
        <template #default="{ row }">{{ applyToLabel(row['apply-to']) }}</template>
      </el-table-column>
      <el-table-column :label="t('policies.colPriority')" width="90">
        <template #default="{ row }">{{ row.priority }}</template>
      </el-table-column>
      <el-table-column :label="t('policies.colDefinition')" min-width="260">
        <template #default="{ row }">
          <span class="mono policy-definition">{{ formatJson(row.definition) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('common.actions')" width="130" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="openEdit(row as Policy)">{{ t('common.edit') }}</el-button>
          <el-button link type="danger" @click="removePolicy(row as Policy)">{{ t('common.delete') }}</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty :description="t('policies.empty')" :image-size="80" />
      </template>
    </el-table>

    <el-dialog v-model="dialogVisible" :title="editing ? t('policies.editTitle') : t('policies.createTitle')" width="640px">
      <el-form label-width="96px">
        <el-form-item :label="t('policies.colVhost')">
          <el-select v-model="form.vhost" :disabled="editing" :placeholder="t('app.vhostPlaceholder')" style="width: 100%">
            <el-option v-for="item in vhostStore.vhosts" :key="item.name" :label="item.name" :value="item.name" />
          </el-select>
        </el-form-item>
        <el-form-item :label="t('policies.colName')">
          <el-input v-model="form.name" :disabled="editing" :placeholder="t('policies.namePlaceholder')" autocomplete="off" />
        </el-form-item>
        <el-form-item :label="t('policies.matchScope')">
          <div style="width: 100%">
            <el-radio-group v-model="form.matchMode">
              <el-radio value="all">{{ t('policies.matchAll') }}</el-radio>
              <el-radio value="prefix">{{ t('policies.matchPrefix') }}</el-radio>
              <el-radio value="custom">{{ t('policies.matchCustom') }}</el-radio>
            </el-radio-group>
            <el-input
              v-if="form.matchMode === 'prefix'"
              v-model="form.prefix"
              :placeholder="t('policies.prefixPlaceholder')"
              style="margin-top: 8px"
            />
            <el-input
              v-else-if="form.matchMode === 'custom'"
              v-model="form.customPattern"
              :placeholder="t('policies.patternPlaceholder')"
              style="margin-top: 8px"
            />
            <div class="page-subtitle" style="margin-top: 6px">
              {{ t('policies.finalPattern', { pattern: finalPattern || t('policies.emptyPattern') }) }}
            </div>
          </div>
        </el-form-item>
        <el-form-item :label="t('policies.colApplyTo')">
          <el-select v-model="form.applyTo" style="width: 100%">
            <el-option v-for="item in APPLY_TO_OPTIONS" :key="item.value" :label="item.label" :value="item.value" />
          </el-select>
        </el-form-item>
        <el-form-item :label="t('policies.colPriority')">
          <el-input-number v-model="form.priority" :min="0" :max="100" />
          <span class="page-subtitle" style="margin-left: 8px">{{ t('policies.priorityHint') }}</span>
        </el-form-item>
        <el-form-item :label="t('policies.colDefinition')">
          <div style="width: 100%">
            <div v-for="(row, index) in form.rows" :key="index" class="definition-row">
              <el-select v-model="row.key" class="definition-key">
                <el-option
                  v-for="key in allowedKeys"
                  :key="key"
                  :label="keyLabel(key)"
                  :value="key"
                />
              </el-select>
              <el-select v-if="row.key === 'overflow'" v-model="row.value" class="definition-value">
                <el-option v-for="value in OVERFLOW_OPTIONS" :key="value" :label="value" :value="value" />
              </el-select>
              <el-input
                v-else
                v-model="row.value"
                class="definition-value"
                :placeholder="NUMERIC_KEYS.includes(row.key) ? t('policies.integerPlaceholder') : t('policies.valuePlaceholder')"
              />
              <el-button link type="danger" @click="removeRow(index)">{{ t('common.remove') }}</el-button>
            </div>
            <el-button link type="primary" :icon="Plus" @click="addRow">{{ t('policies.addRow') }}</el-button>
          </div>
        </el-form-item>
      </el-form>
      <div class="page-subtitle">{{ t('policies.definitionNote') }}</div>
      <template #footer>
        <el-button @click="dialogVisible = false">{{ t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="submitting" @click="submit">
          {{ editing ? t('common.save') : t('common.create') }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.policy-definition {
  white-space: pre-wrap;
  font-size: 12px;
  line-height: 1.5;
}

.definition-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}

.definition-key {
  width: 200px;
}

.definition-value {
  flex: 1;
}
</style>
