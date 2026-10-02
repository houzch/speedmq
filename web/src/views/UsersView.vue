<script setup lang="ts">
// 账号管理页：账号 CRUD（标签/口令/禁用）与按 vhost 的权限（configure/write/read 正则）管理。
//
// 总账号（is_root）由后端强制保护：不可删除、不可禁用、不可移除 administrator 标签，
// 界面对应按钮直接置灰并给出说明，而不是等运维点了才看到 403。
import { computed, onMounted, ref, watch } from 'vue'
import { Plus, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import { ApiError } from '@/api/client'
import type { Permission, User, UserUpsertRequest, Vhost } from '@/api/types'
import { API_GROUPS, apiGroupLabel } from '@/utils/apiGroups'
import { showError } from '@/utils/message'

/** 内置标签候选（仍允许自定义，标签串以空格分隔） */
const TAG_OPTIONS = ['administrator', 'management', 'monitoring']

const users = ref<User[]>([])
const vhosts = ref<Vhost[]>([])
const loading = ref(false)

/** 把后端空格分隔的标签串拆成数组 */
function splitTags(tags: string): string[] {
  return tags.split(/\s+/).filter(Boolean)
}

/** 写操作错误提示：优先展示服务端中文 reason */
function messageOf(error: unknown): string {
  if (error instanceof ApiError) return error.reason || error.message
  if (error instanceof Error) return error.message
  return '操作失败'
}

async function load(): Promise<void> {
  loading.value = true
  try {
    const [list, vhostList] = await Promise.all([api.users(), api.vhosts()])
    users.value = list
    vhosts.value = vhostList
  } catch (error) {
    showError(error, '获取账号列表失败')
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
    ElMessage.warning('请输入账号名')
    return
  }
  if (creating.value && !form.value.password) {
    ElMessage.warning('新建账号必须设置口令')
    return
  }
  if (!form.value.unrestricted && form.value.apiGroups.length === 0) {
    ElMessage.warning('请至少勾选一个管理接口功能组，或改为「不受限」')
    return
  }
  const body: UserUpsertRequest = { tags: form.value.tags }
  if (form.value.password) body.password = form.value.password
  // 不受限 → 空数组；否则提交勾选的功能组（不传则保持原状，这里始终显式提交）
  body.api_groups = form.value.unrestricted ? [] : [...form.value.apiGroups]
  submitting.value = true
  try {
    await api.saveUser(name, body)
    ElMessage.success(creating.value ? `账号 ${name} 已创建` : `账号 ${name} 已更新`)
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
    ElMessage.success(row.disabled ? `账号 ${row.name} 已启用` : `账号 ${row.name} 已禁用`)
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  }
}

async function removeUser(row: User): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `确定要删除账号「${row.name}」吗？其所有 vhost 权限也会一并移除，且不可恢复。`,
      '删除账号',
      { type: 'warning', confirmButtonText: '删除账号', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    await api.deleteUser(row.name)
    ElMessage.success(`账号 ${row.name} 已删除`)
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
    showError(error, '获取权限列表失败')
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
    ElMessage.warning('请选择虚拟主机')
    return
  }
  if (permPreset.value !== 'custom' && permScope.value === 'prefix' && !permPrefix.value.trim()) {
    ElMessage.warning('请输入资源前缀')
    return
  }
  permSubmitting.value = true
  try {
    await api.setPermission(permForm.value.vhost, permUser.value, {
      configure: permFinalPatterns.value.configure,
      write: permFinalPatterns.value.write,
      read: permFinalPatterns.value.read,
    })
    ElMessage.success(`已更新 vhost「${permForm.value.vhost}」的权限`)
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
    await ElMessageBox.confirm(
      `确定要删除账号「${permUser.value}」在 vhost「${row.vhost}」上的权限吗？`,
      '删除权限',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    await api.deletePermission(row.vhost, permUser.value)
    ElMessage.success('权限已删除')
    await loadPermissions()
  } catch (error) {
    ElMessage.error(messageOf(error))
  }
}

onMounted(() => {
  void load()
})
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">账号</h2>
        <div class="page-subtitle">共 {{ users.length }} 个账号</div>
      </div>
      <div>
        <el-button type="primary" :icon="Plus" @click="openCreate">新建账号</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
      </div>
    </div>

    <el-table :data="users" stripe>
      <el-table-column label="账号名" min-width="180">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column label="标签" min-width="220">
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
      <el-table-column label="管理接口权限" min-width="240">
        <template #default="{ row }">
          <el-tag v-if="row.api_groups.length === 0" type="success" size="small" effect="plain">不受限</el-tag>
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
      <el-table-column label="状态" min-width="200">
        <template #default="{ row }">
          <el-tag v-if="row.is_root" class="tag-item" type="danger" size="small">总账号</el-tag>
          <el-tag v-if="row.disabled" class="tag-item" type="info" size="small">已禁用</el-tag>
          <el-tag v-if="row.must_change_password" class="tag-item" type="warning" size="small">待改密</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="260" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="openEdit(row as User)">编辑</el-button>
          <el-button link type="primary" @click="openPermissions(row as User)">权限</el-button>
          <el-tooltip content="总账号不可删除/禁用/降级" placement="top" :disabled="!row.is_root">
            <span>
              <el-button link type="warning" :disabled="row.is_root" @click="toggleDisabled(row as User)">
                {{ row.disabled ? '启用' : '禁用' }}
              </el-button>
            </span>
          </el-tooltip>
          <el-tooltip content="总账号不可删除/禁用/降级" placement="top" :disabled="!row.is_root">
            <span>
              <el-button link type="danger" :disabled="row.is_root" @click="removeUser(row as User)">删除</el-button>
            </span>
          </el-tooltip>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="没有账号" :image-size="80" />
      </template>
    </el-table>

    <!-- 新建 / 编辑账号 -->
    <el-dialog v-model="dialogVisible" :title="creating ? '新建账号' : '编辑账号'" width="480px">
      <el-form label-width="90px">
        <el-form-item label="账号名">
          <el-input
            v-model="form.name"
            :disabled="!creating"
            placeholder="请输入账号名"
            autocomplete="off"
          />
        </el-form-item>
        <el-form-item label="标签">
          <el-select
            v-model="form.tags"
            multiple
            filterable
            allow-create
            default-first-option
            placeholder="选择或输入标签"
            style="width: 100%"
          >
            <el-option v-for="tag in TAG_OPTIONS" :key="tag" :label="tag" :value="tag" />
          </el-select>
        </el-form-item>
        <el-form-item label="口令">
          <el-input
            v-model="form.password"
            type="password"
            show-password
            :placeholder="creating ? '新建账号必须设置口令' : '留空表示不修改口令'"
            autocomplete="new-password"
          />
        </el-form-item>
        <el-form-item label="管理接口权限">
          <div style="width: 100%">
            <el-switch
              v-model="form.unrestricted"
              active-text="不受限（使用标签允许的全部管理接口）"
            />
            <div v-if="!form.unrestricted" class="api-group-list">
              <el-checkbox-group v-model="form.apiGroups">
                <el-checkbox
                  v-for="group in API_GROUPS"
                  :key="group.id"
                  :value="group.id"
                  class="api-group-item"
                >
                  {{ group.label }}
                  <span class="api-group-desc">{{ group.desc }}</span>
                </el-checkbox>
              </el-checkbox-group>
            </div>
          </div>
        </el-form-item>
      </el-form>
      <div class="page-subtitle">
        标签决定授权：administrator/management 可写，monitoring 只读。涉及管理账号的接口需要 administrator 或 management。
      </div>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitUser">
          {{ creating ? '创建' : '保存' }}
        </el-button>
      </template>
    </el-dialog>

    <!-- 权限管理 -->
    <el-drawer v-model="permDrawerVisible" :title="`账号「${permUser}」的权限`" size="640px">
      <div class="page-header" style="margin-bottom: 12px">
        <div class="page-subtitle">按 vhost 选择权限档位与资源范围；底层仍保存为 configure / write / read 三个正则。</div>
        <el-button
          type="primary"
          size="small"
          :icon="Plus"
          :disabled="availableVhosts.length === 0"
          @click="openPermCreate"
        >
          新增权限
        </el-button>
      </div>
      <el-table v-loading="permLoading" :data="perms" stripe>
        <el-table-column label="虚拟主机" min-width="120">
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
        <el-table-column label="操作" width="130" fixed="right">
          <template #default="{ row }">
            <el-button link type="primary" @click="openPermEdit(row as Permission)">编辑</el-button>
            <el-button link type="danger" @click="removePermission(row as Permission)">删除</el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty description="该账号还没有任何 vhost 权限" :image-size="80" />
        </template>
      </el-table>

      <el-dialog
        v-model="permDialogVisible"
        :title="permEditing ? '编辑权限' : '新增权限'"
        width="520px"
        append-to-body
      >
        <el-form label-width="90px">
          <el-form-item label="虚拟主机">
            <el-select
              v-model="permForm.vhost"
              :disabled="permEditing"
              placeholder="选择 vhost"
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
          <el-form-item label="权限档位">
            <el-radio-group v-model="permPreset">
              <el-radio value="full">完全管理</el-radio>
              <el-radio value="consume">只读（消费）</el-radio>
              <el-radio value="publish">只发布</el-radio>
              <el-radio value="declare">只声明拓扑</el-radio>
              <el-radio value="custom">自定义</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="permPreset !== 'custom'" label="资源范围">
            <div style="width: 100%">
              <el-radio-group v-model="permScope">
                <el-radio value="all">全部资源</el-radio>
                <el-radio value="prefix">指定前缀</el-radio>
              </el-radio-group>
              <el-input
                v-if="permScope === 'prefix'"
                v-model="permPrefix"
                placeholder="例如 app.（按字面量匹配，自动转义）"
                style="margin-top: 8px"
              />
            </div>
          </el-form-item>
          <template v-else>
            <el-form-item label="configure">
              <el-input v-model="permForm.configure" placeholder="配置正则，如 ^swiftmq\." />
            </el-form-item>
            <el-form-item label="write">
              <el-input v-model="permForm.write" placeholder="写入正则，如 ^swiftmq\." />
            </el-form-item>
            <el-form-item label="read">
              <el-input v-model="permForm.read" placeholder="读取正则，如 .*" />
            </el-form-item>
          </template>
        </el-form>
        <div class="page-subtitle">
          将保存为：configure={{ permFinalPatterns.configure || '（空）' }} /
          write={{ permFinalPatterns.write || '（空）' }} / read={{ permFinalPatterns.read || '（空）' }}
        </div>
        <template #footer>
          <el-button @click="permDialogVisible = false">取消</el-button>
          <el-button type="primary" :loading="permSubmitting" @click="submitPermission">保存</el-button>
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
