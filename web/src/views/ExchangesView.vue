<script setup lang="ts">
// 交换机列表：按名称过滤 + 绑定数（由 /api/bindings/{vhost} 聚合）
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Delete, Plus, Refresh, Search } from '@element-plus/icons-vue'
import { api } from '@/api'
import { ApiError } from '@/api/client'
import type { Exchange, ExchangeDeclareRequest } from '@/api/types'
import { formatBoolean, formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

const route = useRoute()
const router = useRouter()

/** el-table 插槽行类型（Element Plus 声明为 Record<PropertyKey, any>），放宽以避免强制断言 */
type TableRow = Record<PropertyKey, any>

const vhost = computed<string>(() => String(route.params.vhost ?? '/'))

const exchanges = ref<Exchange[]>([])
const bindingCounts = ref<Record<string, number>>({})
const loading = ref(false)
const filterName = ref('')

let debounceTimer: number | null = null

/** 每种交换机类型对应的标签颜色 */
function typeTagType(type: string): 'primary' | 'success' | 'warning' | 'info' | 'danger' {
  switch (type) {
    case 'direct':
      return 'primary'
    case 'fanout':
      return 'success'
    case 'topic':
      return 'warning'
    case 'headers':
      return 'danger'
    default:
      return 'info'
  }
}

function bindingCount(name: string): number | null {
  return bindingCounts.value[name] ?? null
}

async function load(): Promise<void> {
  loading.value = true
  try {
    const [exchangeList, bindingList] = await Promise.all([
      api.exchanges({ vhost: vhost.value, name: filterName.value.trim() || undefined }),
      api.bindings(vhost.value),
    ])
    exchanges.value = exchangeList
    const counts: Record<string, number> = {}
    for (const binding of bindingList) {
      counts[binding.source] = (counts[binding.source] ?? 0) + 1
    }
    bindingCounts.value = counts
  } catch (error) {
    showError(error, '获取交换机列表失败')
  } finally {
    loading.value = false
  }
}

function goDetail(row: TableRow): void {
  void router.push({ name: 'exchange-detail', params: { vhost: String(row.vhost), name: String(row.name) } })
}

// ---- 声明交换机（Add a new exchange）----
/** 交换机类型的适用场景说明 */
const EXCHANGE_TYPES: { value: string; hint: string }[] = [
  { value: 'direct', hint: '按路由键精确匹配' },
  { value: 'fanout', hint: '广播到所有绑定队列，忽略路由键' },
  { value: 'topic', hint: '按路由键通配模式匹配（* 匹配一个词，# 匹配多个词）' },
  { value: 'headers', hint: '按消息 headers 匹配，忽略路由键' },
]

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
  type: 'direct',
  durable: true,
  autoDelete: false,
  internal: false,
  args: [] as ArgRow[],
})

function openDeclare(): void {
  declareForm.value = { name: '', type: 'direct', durable: true, autoDelete: false, internal: false, args: [] }
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

/** 声明交换机：PUT /api/exchanges/{vhost}/{name}，201 新建与 204 已存在均视为成功 */
async function submitDeclare(): Promise<void> {
  const name = declareForm.value.name.trim()
  if (!name) {
    ElMessage.warning('请输入交换机名称')
    return
  }
  const args = buildArguments()
  if (args === null) return
  const body: ExchangeDeclareRequest = {
    type: declareForm.value.type,
    durable: declareForm.value.durable,
    auto_delete: declareForm.value.autoDelete,
    internal: declareForm.value.internal,
    arguments: args,
  }
  declaring.value = true
  try {
    await api.declareExchange(vhost.value, name, body)
    ElMessage.success(`交换机 ${name} 已声明`)
    declareVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  } finally {
    declaring.value = false
  }
}

watch(filterName, () => {
  if (debounceTimer !== null) window.clearTimeout(debounceTimer)
  debounceTimer = window.setTimeout(() => {
    void load()
  }, 300)
})

watch(vhost, () => {
  void load()
})

onMounted(() => {
  void load()
})

onBeforeUnmount(() => {
  if (debounceTimer !== null) window.clearTimeout(debounceTimer)
})
</script>

<template>
  <div>
    <div class="page-header">
      <div>
        <h2 class="page-title">交换机</h2>
        <div class="page-subtitle">虚拟主机：{{ vhost }}</div>
      </div>
      <div>
        <el-button type="primary" :icon="Plus" @click="openDeclare">添加交换机</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
      </div>
    </div>

    <div class="toolbar">
      <el-input v-model="filterName" :prefix-icon="Search" placeholder="按名称过滤" clearable style="width: 320px" />
      <span class="page-subtitle">共 {{ exchanges.length }} 个交换机</span>
    </div>

    <el-table v-loading="loading" :data="exchanges" stripe>
      <el-table-column label="名称" min-width="200">
        <template #default="{ row }">
          <span class="link-text" @click="goDetail(row)">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column label="虚拟主机" prop="vhost" width="120" />
      <el-table-column label="类型" width="110">
        <template #default="{ row }">
          <el-tag :type="typeTagType(row.type)" size="small" effect="plain">{{ row.type }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="持久化" width="90">
        <template #default="{ row }">
          <el-tag :type="row.durable ? 'success' : 'info'" effect="plain" size="small">
            {{ formatBoolean(row.durable) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="内部使用" width="100">
        <template #default="{ row }">{{ formatBoolean(row.internal) }}</template>
      </el-table-column>
      <el-table-column label="绑定数" width="100" align="right">
        <template #default="{ row }">
          {{ bindingCount(row.name) === null ? '—' : formatNumber(bindingCount(row.name), 0) }}
        </template>
      </el-table-column>
      <el-table-column label="操作" width="100" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="goDetail(row)">详情</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="没有匹配的交换机" :image-size="80" />
      </template>
    </el-table>

    <!-- 声明交换机（Add a new exchange） -->
    <el-dialog v-model="declareVisible" title="添加交换机" width="620px">
      <el-form label-width="110px">
        <el-form-item label="名称">
          <el-input v-model="declareForm.name" placeholder="交换机名称" autocomplete="off" />
        </el-form-item>
        <el-form-item label="类型">
          <el-select v-model="declareForm.type" style="width: 100%">
            <el-option v-for="item in EXCHANGE_TYPES" :key="item.value" :label="item.value" :value="item.value">
              <span>{{ item.value }}</span>
              <span class="option-hint">{{ item.hint }}</span>
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="持久化">
          <el-switch v-model="declareForm.durable" />
        </el-form-item>
        <el-form-item label="自动删除">
          <el-switch v-model="declareForm.autoDelete" />
        </el-form-item>
        <el-form-item label="内部">
          <div style="width: 100%">
            <el-switch v-model="declareForm.internal" />
            <div class="page-subtitle">内部交换机不接受客户端直接发布，只能作为其他交换机的绑定目标。</div>
          </div>
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
        这是一个「声明交换机」操作，与客户端 exchange.declare 同语义：交换机已存在且参数等价时不做改动，参数不一致会报错。
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

.option-hint {
  float: right;
  margin-left: 16px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
