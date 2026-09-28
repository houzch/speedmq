<script setup lang="ts">
// 队列列表：按名称过滤 + 行内查看详情 / 清空 / 删除
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Delete, Refresh, Search } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Queue } from '@/api/types'
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
      <el-button :icon="Refresh" :loading="loading" @click="loadQueues">刷新</el-button>
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
  </div>
</template>
