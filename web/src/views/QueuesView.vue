<script setup lang="ts">
// 队列列表：按名称过滤 + 行内查看详情 / 清空 / 删除
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Delete, Plus, Refresh, Search } from '@element-plus/icons-vue'
import { api } from '@/api'
import { ApiError } from '@/api/client'
import type { Queue, QueueDeclareRequest } from '@/api/types'
import { formatBoolean, formatBytes, formatNumber, formatRate } from '@/utils/format'
import { showError } from '@/utils/message'

const route = useRoute()
const router = useRouter()

/** el-table 插槽行类型（Element Plus 声明为 Record<PropertyKey, any>），放宽以避免强制断言 */
type TableRow = Record<PropertyKey, any>

/** 当前 vhost 来自路由参数（hash 中形如 /queues/%2F） */
const vhost = computed<string>(() => String(route.params.vhost ?? '/'))

const queues = ref<Queue[]>([])
const loading = ref(false)
const filterName = ref('')

let debounceTimer: number | null = null

async function loadQueues(): Promise<void> {
  loading.value = true
  try {
    queues.value = await api.queues({
      vhost: vhost.value,
      name: filterName.value.trim() || undefined,
      use_regex: false,
    })
  } catch (error) {
    showError(error, '获取队列列表失败')
  } finally {
    loading.value = false
  }
}

function goDetail(row: TableRow): void {
  void router.push({ name: 'queue-detail', params: { vhost: String(row.vhost), name: String(row.name) } })
}

async function purgeQueue(row: TableRow): Promise<void> {
  const vhost = String(row.vhost)
  const name = String(row.name)
  try {
    await ElMessageBox.confirm(`确定要清空队列「${name}」中的全部消息吗？该操作不可撤销。`, '清空队列', {
      type: 'warning',
      confirmButtonText: '清空',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await api.purgeQueue(vhost, name)
    ElMessage.success('队列已清空')
    await loadQueues()
  } catch (error) {
    showError(error, '清空队列失败')
  }
}

async function deleteQueue(row: TableRow): Promise<void> {
  const vhost = String(row.vhost)
  const name = String(row.name)
  try {
    await ElMessageBox.confirm(`确定要删除队列「${name}」吗？该操作不可撤销。`, '删除队列', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await api.deleteQueue(vhost, name)
    ElMessage.success('队列已删除')
    await loadQueues()
  } catch (error) {
    showError(error, '删除队列失败')
  }
}

// ---- 声明队列（Add a new queue）----
/** 可选参数的值类型（键固定为 vstring） */
type ArgValueType = 'string' | 'number' | 'boolean'

interface ArgRow {
  key: string
  valueType: ArgValueType
  value: string
}

const declareVisible = ref(false)
const declaring = ref(false)
const declareForm = ref({
  name: '',
  queueType: 'classic' as 'classic' | 'quorum',
  durable: true,
  autoDelete: false,
  args: [] as ArgRow[],
})

function openDeclare(): void {
  declareForm.value = { name: '', queueType: 'classic', durable: true, autoDelete: false, args: [] }
  declareVisible.value = true
}

function addArgRow(): void {
  declareForm.value.args.push({ key: '', valueType: 'string', value: '' })
}

function removeArgRow(index: number): void {
  declareForm.value.args.splice(index, 1)
}

/** 写操作错误提示：优先展示服务端中文 reason */
function messageOf(error: unknown): string {
  if (error instanceof ApiError) return error.reason || error.message
  if (error instanceof Error) return error.message
  return '操作失败'
}

/** 把参数编辑器转成 arguments 对象；键为空的行忽略，数字非法时返回 null */
function buildArguments(): Record<string, unknown> | null {
  const result: Record<string, unknown> = {}
  for (const row of declareForm.value.args) {
    const key = row.key.trim()
    if (!key) continue
    if (row.valueType === 'number') {
      const num = Number(row.value)
      if (row.value.trim() === '' || Number.isNaN(num)) {
        ElMessage.warning(`参数「${key}」的值必须是数字`)
        return null
      }
      result[key] = num
    } else if (row.valueType === 'boolean') {
      result[key] = row.value === 'true'
    } else {
      result[key] = row.value
    }
  }
  return result
}

/** 声明队列：PUT /api/queues/{vhost}/{name}，201 新建与 204 已存在均视为成功 */
async function submitDeclare(): Promise<void> {
  const name = declareForm.value.name.trim()
  if (!name) {
    ElMessage.warning('请输入队列名称')
    return
  }
  const args = buildArguments()
  if (args === null) return
  // 队列类型通过 arguments 表达：仲裁队列写入 x-queue-type=quorum，经典队列不写
  if (declareForm.value.queueType === 'quorum') {
    args['x-queue-type'] = 'quorum'
  }
  const body: QueueDeclareRequest = {
    durable: declareForm.value.durable,
    auto_delete: declareForm.value.autoDelete,
    arguments: args,
  }
  declaring.value = true
  try {
    await api.declareQueue(vhost.value, name, body)
    ElMessage.success(`队列 ${name} 已声明`)
    declareVisible.value = false
    await loadQueues()
  } catch (error) {
    ElMessage.error(messageOf(error))
  } finally {
    declaring.value = false
  }
}

// 过滤输入防抖（服务端按 name 查询）
watch(filterName, () => {
  if (debounceTimer !== null) window.clearTimeout(debounceTimer)
  debounceTimer = window.setTimeout(() => {
    void loadQueues()
  }, 300)
})

// 切换 vhost 时重新加载
watch(vhost, () => {
  void loadQueues()
})

onMounted(() => {
  void loadQueues()
})

onBeforeUnmount(() => {
  if (debounceTimer !== null) window.clearTimeout(debounceTimer)
})
</script>

<template>
  <div>
    <div class="page-header">
      <div>
        <h2 class="page-title">队列</h2>
        <div class="page-subtitle">虚拟主机：{{ vhost }}</div>
      </div>
      <div>
        <el-button type="primary" :icon="Plus" @click="openDeclare">添加队列</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="loadQueues">刷新</el-button>
      </div>
    </div>

    <div class="toolbar">
      <el-input
        v-model="filterName"
        :prefix-icon="Search"
        placeholder="按名称过滤（支持正则请由服务端 use_regex 控制）"
        clearable
        style="width: 320px"
      />
      <span class="page-subtitle">共 {{ queues.length }} 个队列</span>
    </div>

    <el-table v-loading="loading" :data="queues" stripe>
      <el-table-column label="名称" min-width="200">
        <template #default="{ row }">
          <span class="link-text" @click="goDetail(row)">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column label="虚拟主机" prop="vhost" width="120" />
      <el-table-column label="持久化" width="90">
        <template #default="{ row }">
          <el-tag :type="row.durable ? 'success' : 'info'" effect="plain" size="small">
            {{ formatBoolean(row.durable) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag :type="row.state === 'running' ? 'success' : 'danger'" size="small">{{ row.state }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="就绪消息" width="100" align="right">
        <template #default="{ row }">{{ formatNumber(row.messages_ready, 0) }}</template>
      </el-table-column>
      <el-table-column label="未确认消息" width="110" align="right">
        <template #default="{ row }">{{ formatNumber(row.messages_unacknowledged, 0) }}</template>
      </el-table-column>
      <el-table-column label="消费者数" width="100" align="right">
        <template #default="{ row }">{{ formatNumber(row.consumers, 0) }}</template>
      </el-table-column>
      <el-table-column label="消息速率" width="110" align="right">
        <template #default="{ row }">{{ formatRate(row.messages_details) }}</template>
      </el-table-column>
      <el-table-column label="内存" width="110" align="right">
        <template #default="{ row }">{{ formatBytes(row.memory) }}</template>
      </el-table-column>
      <el-table-column label="操作" width="220" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="goDetail(row)">详情</el-button>
          <el-button link type="warning" :icon="Delete" @click="purgeQueue(row)">清空</el-button>
          <el-button link type="danger" :icon="Delete" @click="deleteQueue(row)">删除</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="没有匹配的队列" :image-size="80" />
      </template>
    </el-table>

    <!-- 声明队列（Add a new queue） -->
    <el-dialog v-model="declareVisible" title="添加队列" width="560px">
      <el-form label-width="110px">
        <el-form-item label="名称">
          <el-input v-model="declareForm.name" placeholder="队列名称" autocomplete="off" />
        </el-form-item>
        <el-form-item label="队列类型">
          <el-radio-group v-model="declareForm.queueType">
            <el-radio value="classic">经典队列</el-radio>
            <el-radio value="quorum">仲裁队列</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="持久化">
          <el-switch v-model="declareForm.durable" />
        </el-form-item>
        <el-form-item label="自动删除">
          <el-switch v-model="declareForm.autoDelete" />
        </el-form-item>
        <el-form-item label="可选参数">
          <div style="width: 100%">
            <div v-for="(row, index) in declareForm.args" :key="index" class="arg-row">
              <el-input v-model="row.key" placeholder="键（vstring）" style="flex: 1" />
              <el-select v-model="row.valueType" style="width: 96px">
                <el-option value="string" label="字符串" />
                <el-option value="number" label="数字" />
                <el-option value="boolean" label="布尔" />
              </el-select>
              <el-select v-if="row.valueType === 'boolean'" v-model="row.value" style="width: 110px">
                <el-option value="true" label="true" />
                <el-option value="false" label="false" />
              </el-select>
              <el-input v-else v-model="row.value" placeholder="值" style="flex: 1" />
              <el-button link type="danger" :icon="Delete" @click="removeArgRow(index)" />
            </div>
            <el-button link type="primary" :icon="Plus" @click="addArgRow">添加参数</el-button>
          </div>
        </el-form-item>
      </el-form>
      <div class="page-subtitle">
        这是一个「声明队列」操作，与客户端 queue.declare 同语义：队列已存在且参数等价时不做改动，参数不一致会报错。
      </div>
      <template #footer>
        <el-button @click="declareVisible = false">取消</el-button>
        <el-button type="primary" :loading="declaring" @click="submitDeclare">声明</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.arg-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
</style>
