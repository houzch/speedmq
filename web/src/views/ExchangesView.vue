<script setup lang="ts">
// 交换机列表：按名称过滤 + 绑定数（由 /api/bindings/{vhost} 聚合）
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { Refresh, Search } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Exchange } from '@/api/types'
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
      <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
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
  </div>
</template>
