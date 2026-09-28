<script setup lang="ts">
// 连接列表：展示连接信息并支持强制关闭
import { onMounted, ref } from 'vue'
import { Delete, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Connection } from '@/api/types'
import { formatElapsedFrom, formatNumber, formatTimestamp } from '@/utils/format'
import { showError } from '@/utils/message'

/** el-table 插槽行类型（Element Plus 声明为 Record<PropertyKey, any>），放宽以避免强制断言 */
type TableRow = Record<PropertyKey, any>

const connections = ref<Connection[]>([])
const loading = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    connections.value = await api.connections()
  } catch (error) {
    showError(error, '获取连接列表失败')
  } finally {
    loading.value = false
  }
}

async function closeConnection(row: TableRow): Promise<void> {
  const name = String(row.name)
  try {
    await ElMessageBox.confirm(
      `确定要强制关闭连接「${name}」吗？该连接上的所有通道都会被终止。`,
      '关闭连接',
      { type: 'warning', confirmButtonText: '关闭连接', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    await api.closeConnection(name)
    ElMessage.success('连接已关闭')
    await load()
  } catch (error) {
    showError(error, '关闭连接失败')
  }
}

onMounted(() => {
  void load()
})
</script>

<template>
  <div>
    <div class="page-header">
      <div>
        <h2 class="page-title">连接</h2>
        <div class="page-subtitle">当前共 {{ connections.length }} 个客户端连接</div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
    </div>

    <el-table v-loading="loading" :data="connections" stripe>
      <el-table-column label="连接名称" prop="name" min-width="260">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column label="用户" prop="user" width="110" />
      <el-table-column label="虚拟主机" prop="vhost" width="110" />
      <el-table-column label="协议" prop="protocol" width="130" />
      <el-table-column label="通道数" width="90" align="right">
        <template #default="{ row }">{{ formatNumber(row.channels, 0) }}</template>
      </el-table-column>
      <el-table-column label="状态" width="100">
        <template #default="{ row }">
          <el-tag :type="row.state === 'running' ? 'success' : 'info'" size="small">{{ row.state }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="连接时长" width="150">
        <template #default="{ row }">{{ formatElapsedFrom(row.connected_at) }}</template>
      </el-table-column>
      <el-table-column label="建立时间" width="180">
        <template #default="{ row }">{{ formatTimestamp(row.connected_at) }}</template>
      </el-table-column>
      <el-table-column label="对端地址" width="180">
        <template #default="{ row }">{{ row.peer_host }}:{{ row.peer_port }}</template>
      </el-table-column>
      <el-table-column label="节点" prop="node" min-width="180" />
      <el-table-column label="操作" width="120" fixed="right">
        <template #default="{ row }">
          <el-button link type="danger" :icon="Delete" @click="closeConnection(row)">关闭</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="当前没有客户端连接" :image-size="80" />
      </template>
    </el-table>
  </div>
</template>
