<script setup lang="ts">
// 账号管理页：账号 CRUD（标签/口令/禁用）与按 vhost 的权限（configure/write/read 正则）管理。
//
// 总账号（is_root）由后端强制保护：不可删除、不可禁用、不可移除 administrator 标签，
// 界面对应按钮直接置灰并给出说明，而不是等运维点了才看到 403。
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { Plus, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import { ApiError } from '@/api/client'
import type { Permission, User, UserUpsertRequest, Vhost } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { API_GROUPS, apiGroupDesc, apiGroupLabel } from '@/utils/apiGroups'
import { showError } from '@/utils/message'

/** 内置标签候选（仍允许自定义，标签串以空格分隔） */
const TAG_OPTIONS = ['administrator', 'management', 'monitoring']

const refresh = useRefreshStore()
const { t } = useI18n()

const users = ref<User[]>([])
const vhosts = ref<Vhost[]>([])
const loading = ref(false)

/** 把后端空格分隔的标签串拆成数组 */
function splitTags(tags: string): string[] {
  return tags.split(/\s+/).filter(Boolean)
}

/** 写操作错误提示：优先展示服务端原因 */
function messageOf(error: unknown): string {
  if (error instanceof ApiError) return error.reason || error.message
  if (error instanceof Error) return error.message
  return t('common.operationFailed')
}

async function load(): Promise<void> {
  loading.value = true
  try {
    const [list, vhostList] = await Promise.all([api.users(), api.vhosts()])
    users.value = list
    vhosts.value = vhostList
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('users.loadFailed'))
  } finally {
    loading.value = false
  }
}

// ---- 新建 / 编辑 ----
const dialogVisible = ref(false)
const creating = ref(false)
const submitting = ref(false)
const form = ref({
  name: '',
  tags: [] as string[],
  password: '',
  /** 管理接口权限：不受限（不限制，用标签允许的全部接口） */
  unrestricted: true,
  /** 勾选的管理接口功能组 id（unrestricted 为 false 时生效） */
  apiGroups: [] as string[],
})

function openCreate(): void {
  creating.value = true
  form.value = { name: '', tags: [], password: '', unrestricted: true, apiGroups: [] }
  dialogVisible.value = true
}

function openEdit(row: User): void {
  creating.value = false
  form.value = {
    name: row.name,
    tags: splitTags(row.tags),
    password: '',
    unrestricted: row.api_groups.length === 0,
    apiGroups: [...row.api_groups],
  }
  dialogVisible.value = true
}

async function submitUser(): Promise<void> {
  const name = form.value.name.trim()
  if (!name) {
    ElMessage.warning(t('users.nameRequired'))
    return
  }
  if (creating.value && !form.value.password) {
    ElMessage.warning(t('users.passwordRequired'))
    return
  }
  if (!form.value.unrestricted && form.value.apiGroups.length === 0) {
    ElMessage.warning(t('users.groupRequired'))
    return
  }
  const body: UserUpsertRequest = { tags: form.value.tags }
  if (form.value.password) body.password = form.value.password
  // 不受限 → 空数组；否则提交勾选的功能组（不传则保持原状，这里始终显式提交）
  body.api_groups = form.value.unrestricted ? [] : [...form.value.apiGroups]
  submitting.value = true
  try {
    await api.saveUser(name, body)
    ElMessage.success(creating.value ? t('users.created', { name }) : t('users.updated', { name }))
    dialogVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  } finally {
    submitting.value = false
  }
}

/** 启用/禁用：只切换 disabled，其余字段保持原状 */
async function toggleDisabled(row: User): Promise<void> {
  try {
    await api.saveUser(row.name, { disabled: !row.disabled })
    ElMessage.success(row.disabled ? t('users.enabled', { name: row.name }) : t('users.disabled', { name: row.name }))
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  }
}

async function removeUser(row: User): Promise<void> {
  try {
    await ElMessageBox.confirm(t('users.deleteConfirm', { name: row.name }), t('users.deleteTitle'), {
      type: 'warning',
      confirmButtonText: t('users.deleteButton'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.deleteUser(row.name)
    ElMessage.success(t('users.deleted', { name: row.name }))
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  }
}

// ---- 权限管理（右侧抽屉）----
const permDrawerVisible = ref(false)
const permUser = ref('')
const perms = ref<Permission[]>([])
const permLoading = ref(false)

/** 尚未配置权限的 vhost（新增权限时可选项） */
const availableVhosts = computed<string[]>(() => {
  const used = new Set(perms.value.map((item) => item.vhost))
  return vhosts.value.filter((item) => !used.has(item.name)).map((item) => item.name)
})

async function loadPermissions(): Promise<void> {
  permLoading.value = true
  try {
    const all = await api.permissions()
    perms.value = all.filter((item) => item.user === permUser.value)
  } catch (error) {
    showError(error, t('users.loadPermissionFailed'))
  } finally {
    permLoading.value = false
  }
}

function openPermissions(row: User): void {
  permUser.value = row.name
  perms.value = []
  permDrawerVisible.value = true
  void loadPermissions()
}

const permDialogVisible = ref(false)
const permEditing = ref(false)
const permSubmitting = ref(false)
const permForm = ref({ vhost: '', configure: '', write: '', read: '' })

// ---- 权限档位与资源范围（把 configure/write/read 正则收进预设，避免让运维手写）----

/** 权限档位：三个动作的启用组合；custom 时直接读输入框 */
type PermPreset = 'full' | 'consume' | 'publish' | 'declare' | 'custom'
/** 资源范围：全部资源（.*）或指定前缀（^前缀） */
type PermScope = 'all' | 'prefix'

/** 各预设档位启用哪些动作 */
const PRESET_ACTIONS: Record<Exclude<PermPreset, 'custom'>, { configure: boolean; write: boolean; read: boolean }> = {
  full: { configure: true, write: true, read: true },
  consume: { configure: false, write: false, read: true },
  publish: { configure: false, write: true, read: false },
  declare: { configure: true, write: false, read: false },
}

const permPreset = ref<PermPreset>('full')
const permScope = ref<PermScope>('all')
const permPrefix = ref('')

/** 转义正则元字符，让前缀按字面量匹配 */
function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

/** 把 ^前缀 还原为原始前缀；若非"纯字面量前缀"则返回 null */
function regexToPrefix(regex: string): string | null {
  if (!regex.startsWith('^')) return null
  const body = regex.slice(1)
  const prefix = body.replace(/\\(.)/g, '$1')
  return escapeRegex(prefix) === body ? prefix : null
}

/** 空串与 ^$ 都视为"该动作不允许" */
function isEmptyPattern(value: string): boolean {
  return value === '' || value === '^$'
}

/** 档位 + 范围组合出的最终三项正则 */
function patternsFor(
  preset: Exclude<PermPreset, 'custom'>,
  scope: PermScope,
  prefix: string,
): { configure: string; write: string; read: string } {
  const regex = scope === 'all' ? '.*' : `^${escapeRegex(prefix)}`
  const on = PRESET_ACTIONS[preset]
  return {
    configure: on.configure ? regex : '',
    write: on.write ? regex : '',
    read: on.read ? regex : '',
  }
}

/** 打开编辑时按现有值反推档位与范围；无法归为某个预设则落到自定义 */
function inferPreset(row: Pick<Permission, 'configure' | 'write' | 'read'>): {
  preset: PermPreset
  scope: PermScope
  prefix: string
} {
  const custom = { preset: 'custom' as const, scope: 'all' as const, prefix: '' }
  const enabled = [row.configure, row.write, row.read].filter((value) => !isEmptyPattern(value))
  const sameValue = enabled.length > 0 && enabled.every((value) => value === enabled[0])
  if (!sameValue) return custom
  const allMatched = enabled[0] === '.*'
  const prefix = allMatched ? null : regexToPrefix(enabled[0])
  // 启用维度的值必须一致，且能识别为 .* 或 ^前缀，否则自定义
  if (!allMatched && prefix === null) return custom
  const cOn = !isEmptyPattern(row.configure)
  const wOn = !isEmptyPattern(row.write)
  const rOn = !isEmptyPattern(row.read)
  let preset: Exclude<PermPreset, 'custom'>
  if (cOn && wOn && rOn) preset = 'full'
  else if (!cOn && !wOn && rOn) preset = 'consume'
  else if (!cOn && wOn && !rOn) preset = 'publish'
  else if (cOn && !wOn && !rOn) preset = 'declare'
  else return custom
  return { preset, scope: allMatched ? 'all' : 'prefix', prefix: prefix ?? '' }
}

/** 当前档位 + 范围将保存的三项正则（自定义时直接取输入框） */
const permFinalPatterns = computed<{ configure: string; write: string; read: string }>(() => {
  if (permPreset.value === 'custom') {
    return {
      configure: permForm.value.configure,
      write: permForm.value.write,
      read: permForm.value.read,
    }
  }
  return patternsFor(permPreset.value, permScope.value, permPrefix.value)
})

// 从预设切到自定义时，把当前将保存的值灌进输入框，便于微调
watch(permPreset, (next, prev) => {
  if (next !== 'custom' || prev === 'custom') return
  const seeded = patternsFor(prev, permScope.value, permPrefix.value)
  permForm.value.configure = seeded.configure
  permForm.value.write = seeded.write
  permForm.value.read = seeded.read
})

function openPermCreate(): void {
  permEditing.value = false
  permForm.value = { vhost: availableVhosts.value[0] ?? '', configure: '', write: '', read: '' }
  permPreset.value = 'full'
  permScope.value = 'all'
  permPrefix.value = ''
  permDialogVisible.value = true
}

function openPermEdit(row: Permission): void {
  permEditing.value = true
  permForm.value = {
    vhost: row.vhost,
    configure: row.configure,
    write: row.write,
    read: row.read,
  }
  const inferred = inferPreset(row)
  permPreset.value = inferred.preset
  permScope.value = inferred.scope
  permPrefix.value = inferred.prefix
  permDialogVisible.value = true
}

async function submitPermission(): Promise<void> {
  if (!permForm.value.vhost) {
    ElMessage.warning(t('users.vhostRequired'))
    return
  }
  if (permPreset.value !== 'custom' && permScope.value === 'prefix' && !permPrefix.value.trim()) {
    ElMessage.warning(t('users.prefixRequired'))
    return
  }
  permSubmitting.value = true
  try {
    await api.setPermission(permForm.value.vhost, permUser.value, {
      configure: permFinalPatterns.value.configure,
      write: permFinalPatterns.value.write,
      read: permFinalPatterns.value.read,
    })
    ElMessage.success(t('users.permissionUpdated', { vhost: permForm.value.vhost }))
    permDialogVisible.value = false
    await loadPermissions()
  } catch (error) {
    ElMessage.error(messageOf(error))
  } finally {
    permSubmitting.value = false
  }
}

async function removePermission(row: Permission): Promise<void> {
  try {
    await ElMessageBox.confirm(t('users.deletePermissionConfirm', { user: permUser.value, vhost: row.vhost }), t('users.deletePermissionTitle'), {
      type: 'warning',
      confirmButtonText: t('common.delete'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.deletePermission(row.vhost, permUser.value)
    ElMessage.success(t('users.permissionDeleted'))
    await loadPermissions()
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
        <h2 class="page-title">{{ t('users.title') }}</h2>
        <div class="page-subtitle">{{ t('users.total', { count: users.length }) }}</div>
      </div>
      <div>
        <el-button type="primary" :icon="Plus" @click="openCreate">{{ t('users.create') }}</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="load">{{ t('common.refresh') }}</el-button>
      </div>
    </div>

    <el-table :data="users" stripe>
      <el-table-column :label="t('users.colName')" min-width="180">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('users.colTags')" min-width="220">
        <template #default="{ row }">
          <template v-if="splitTags(row.tags).length > 0">
            <el-tag
              v-for="tag in splitTags(row.tags)"
              :key="tag"
              class="tag-item"
              type="info"
              effect="plain"
              size="small"
            >
              {{ tag }}
            </el-tag>
          </template>
          <span v-else class="page-subtitle">—</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('users.colApiGroups')" min-width="240">
        <template #default="{ row }">
          <el-tag v-if="row.api_groups.length === 0" type="success" size="small" effect="plain">{{ t('common.unlimited') }}</el-tag>
          <template v-else>
            <el-tag
              v-for="id in row.api_groups"
              :key="id"
              class="tag-item"
              type="info"
              effect="plain"
              size="small"
            >
              {{ apiGroupLabel(id) }}
            </el-tag>
          </template>
        </template>
      </el-table-column>
      <el-table-column :label="t('users.colState')" min-width="200">
        <template #default="{ row }">
          <el-tag v-if="row.is_root" class="tag-item" type="danger" size="small">{{ t('users.rootTag') }}</el-tag>
          <el-tag v-if="row.disabled" class="tag-item" type="info" size="small">{{ t('users.disabledTag') }}</el-tag>
          <el-tag v-if="row.must_change_password" class="tag-item" type="warning" size="small">{{ t('users.mustChangeTag') }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('common.actions')" width="260" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="openEdit(row as User)">{{ t('common.edit') }}</el-button>
          <el-button link type="primary" @click="openPermissions(row as User)">{{ t('users.permissions') }}</el-button>
          <el-tooltip :content="t('users.rootProtected')" placement="top" :disabled="!row.is_root">
            <span>
              <el-button link type="warning" :disabled="row.is_root" @click="toggleDisabled(row as User)">
                {{ row.disabled ? t('common.enable') : t('common.disable') }}
              </el-button>
            </span>
          </el-tooltip>
          <el-tooltip :content="t('users.rootProtected')" placement="top" :disabled="!row.is_root">
            <span>
              <el-button link type="danger" :disabled="row.is_root" @click="removeUser(row as User)">{{ t('common.delete') }}</el-button>
            </span>
          </el-tooltip>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty :description="t('users.empty')" :image-size="80" />
      </template>
    </el-table>

    <!-- 新建 / 编辑账号 -->
    <el-dialog v-model="dialogVisible" :title="creating ? t('users.createTitle') : t('users.editTitle')" width="480px">
      <el-form label-width="90px">
        <el-form-item :label="t('users.colName')">
          <el-input
            v-model="form.name"
            :disabled="!creating"
            :placeholder="t('users.namePlaceholder')"
            autocomplete="off"
          />
        </el-form-item>
        <el-form-item :label="t('users.colTags')">
          <el-select
            v-model="form.tags"
            multiple
            filterable
            allow-create
            default-first-option
            :placeholder="t('users.tagsPlaceholder')"
            style="width: 100%"
          >
            <el-option v-for="tag in TAG_OPTIONS" :key="tag" :label="tag" :value="tag" />
          </el-select>
        </el-form-item>
        <el-form-item :label="t('password.new')">
          <el-input
            v-model="form.password"
            type="password"
            show-password
            :placeholder="creating ? t('users.passwordPlaceholderCreate') : t('users.passwordPlaceholderEdit')"
            autocomplete="new-password"
          />
        </el-form-item>
        <el-form-item :label="t('users.colApiGroups')">
          <div style="width: 100%">
            <el-switch
              v-model="form.unrestricted"
              :active-text="t('users.unrestrictedSwitch')"
            />
            <div v-if="!form.unrestricted" class="api-group-list">
              <el-checkbox-group v-model="form.apiGroups">
                <el-checkbox
                  v-for="group in API_GROUPS"
                  :key="group.id"
                  :value="group.id"
                  class="api-group-item"
                >
                  {{ apiGroupLabel(group.id) }}
                  <span class="api-group-desc">{{ apiGroupDesc(group.id) }}</span>
                </el-checkbox>
              </el-checkbox-group>
            </div>
          </div>
        </el-form-item>
      </el-form>
      <div class="page-subtitle">{{ t('users.tagsNote') }}</div>
      <template #footer>
        <el-button @click="dialogVisible = false">{{ t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="submitting" @click="submitUser">
          {{ creating ? t('common.create') : t('common.save') }}
        </el-button>
      </template>
    </el-dialog>

    <!-- 权限管理 -->
    <el-drawer v-model="permDrawerVisible" :title="t('users.permissionsOf', { name: permUser })" size="640px">
      <div class="page-header" style="margin-bottom: 12px">
        <div class="page-subtitle">{{ t('users.permNote') }}</div>
        <el-button
          type="primary"
          size="small"
          :icon="Plus"
          :disabled="availableVhosts.length === 0"
          @click="openPermCreate"
        >
          {{ t('users.addPermission') }}
        </el-button>
      </div>
      <el-table v-loading="permLoading" :data="perms" stripe>
        <el-table-column :label="t('users.permissionVhost')" min-width="120">
          <template #default="{ row }">
            <span class="mono">{{ row.vhost }}</span>
          </template>
        </el-table-column>
        <el-table-column label="configure" min-width="120" show-overflow-tooltip>
          <template #default="{ row }"><span class="mono">{{ row.configure || '—' }}</span></template>
        </el-table-column>
        <el-table-column label="write" min-width="120" show-overflow-tooltip>
          <template #default="{ row }"><span class="mono">{{ row.write || '—' }}</span></template>
        </el-table-column>
        <el-table-column label="read" min-width="120" show-overflow-tooltip>
          <template #default="{ row }"><span class="mono">{{ row.read || '—' }}</span></template>
        </el-table-column>
        <el-table-column :label="t('common.actions')" width="130" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="openPermEdit(row as Permission)">{{ t('common.edit') }}</el-button>
            <el-button link type="danger" @click="removePermission(row as Permission)">{{ t('common.delete') }}</el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty :description="t('users.noPermission')" :image-size="80" />
        </template>
      </el-table>

      <el-dialog
        v-model="permDialogVisible"
        :title="permEditing ? t('users.editPermission') : t('users.addPermission')"
        width="520px"
        append-to-body
      >
        <el-form label-width="90px">
          <el-form-item :label="t('users.permissionVhost')">
            <el-select
              v-model="permForm.vhost"
              :disabled="permEditing"
              :placeholder="t('app.vhostPlaceholder')"
              style="width: 100%"
            >
              <el-option
                v-for="name in permEditing ? [permForm.vhost] : availableVhosts"
                :key="name"
                :label="name"
                :value="name"
              />
            </el-select>
          </el-form-item>
          <el-form-item :label="t('users.permissionPreset')">
            <el-radio-group v-model="permPreset">
              <el-radio value="full">{{ t('users.presetFull') }}</el-radio>
              <el-radio value="consume">{{ t('users.presetConsume') }}</el-radio>
              <el-radio value="publish">{{ t('users.presetPublish') }}</el-radio>
              <el-radio value="declare">{{ t('users.presetDeclare') }}</el-radio>
              <el-radio value="custom">{{ t('users.presetCustom') }}</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="permPreset !== 'custom'" :label="t('users.resourceScope')">
            <div style="width: 100%">
              <el-radio-group v-model="permScope">
                <el-radio value="all">{{ t('users.scopeAll') }}</el-radio>
                <el-radio value="prefix">{{ t('users.scopePrefix') }}</el-radio>
              </el-radio-group>
              <el-input
                v-if="permScope === 'prefix'"
                v-model="permPrefix"
                :placeholder="t('users.prefixPlaceholder')"
                style="margin-top: 8px"
              />
            </div>
          </el-form-item>
          <template v-else>
            <el-form-item label="configure">
              <el-input v-model="permForm.configure" :placeholder="t('users.configurePlaceholder')" />
            </el-form-item>
            <el-form-item label="write">
              <el-input v-model="permForm.write" :placeholder="t('users.writePlaceholder')" />
            </el-form-item>
            <el-form-item label="read">
              <el-input v-model="permForm.read" :placeholder="t('users.readPlaceholder')" />
            </el-form-item>
          </template>
        </el-form>
        <div class="page-subtitle">
          {{
            t('users.savePreview', {
              configure: permFinalPatterns.configure || t('users.emptyPattern'),
              write: permFinalPatterns.write || t('users.emptyPattern'),
              read: permFinalPatterns.read || t('users.emptyPattern'),
            })
          }}
        </div>
        <template #footer>
          <el-button @click="permDialogVisible = false">{{ t('common.cancel') }}</el-button>
          <el-button type="primary" :loading="permSubmitting" @click="submitPermission">{{ t('common.save') }}</el-button>
        </template>
      </el-dialog>
    </el-drawer>
  </div>
</template>

<style scoped>
.tag-item {
  margin-right: 4px;
}

.api-group-list {
  margin-top: 8px;
}

.api-group-item {
  display: flex;
  align-items: flex-start;
  width: 100%;
  height: auto;
  margin: 0 0 6px 0;
}

.api-group-desc {
  margin-left: 8px;
  font-size: 12px;
  line-height: 1.4;
  color: var(--el-text-color-secondary);
}
</style>
