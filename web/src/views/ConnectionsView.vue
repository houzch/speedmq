<script setup lang="ts">
// 连接列表：展示连接信息并支持强制关闭
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { Delete, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Connection } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { formatElapsedFrom, formatNumber, formatTimestamp } from '@/utils/format'
import { showError } from '@/utils/message'

/** el-table 插槽行类型（Element Plus 声明为 Record<PropertyKey, any>），放宽以避免强制断言 */
type TableRow = Record<PropertyKey, any>

const refresh = useRefreshStore()
const { t } = useI18n()

const connections = ref<Connection[]>([])
const loading = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    connections.value = await api.connections()
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('connections.loadFailed'))
  } finally {
    loading.value = false
  }
}

async function closeConnection(row: TableRow): Promise<void> {
  const name = String(row.name)
  try {
    await ElMessageBox.confirm(t('connections.closeConfirm', { name }), t('connections.closeTitle'), {
      type: 'warning',
      confirmButtonText: t('connections.closeButton'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.closeConnection(name)
    ElMessage.success(t('connections.close'))
    await load()
  } catch (error) {
    showError(error, t('connections.closeFailed'))
  }
}

useAutoRefresh(load)
</script>

<template>
  <div>
    <div class="page-header">
      <div>
        <h2 class="page-title">{{ t('connections.title') }}</h2>
        <div class="page-subtitle">{{ t('connections.total', { count: connections.length }) }}</div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">{{ t('common.refresh') }}</el-button>
    </div>

    <el-table v-loading="loading" :data="connections" stripe>
      <el-table-column :label="t('connections.colName')" prop="name" min-width="260">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('connections.colUser')" prop="user" width="110" />
      <el-table-column :label="t('connections.colVhost')" prop="vhost" width="110" />
      <el-table-column :label="t('connections.colProtocol')" prop="protocol" width="130" />
      <el-table-column :label="t('connections.colChannels')" width="90" align="right">
        <template #default="{ row }">{{ formatNumber(row.channels, 0) }}</template>
      </el-table-column>
      <el-table-column :label="t('connections.colState')" width="100">
        <template #default="{ row }">
          <el-tag :type="row.state === 'running' ? 'success' : 'info'" size="small">{{ row.state }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('connections.colDuration')" width="150">
        <template #default="{ row }">{{ formatElapsedFrom(row.connected_at) }}</template>
      </el-table-column>
      <el-table-column :label="t('connections.colConnectedAt')" width="180">
        <template #default="{ row }">{{ formatTimestamp(row.connected_at) }}</template>
      </el-table-column>
      <el-table-column :label="t('connections.colPeer')" width="180">
        <template #default="{ row }">{{ row.peer_host }}:{{ row.peer_port }}</template>
      </el-table-column>
      <el-table-column :label="t('connections.colNode')" prop="node" min-width="180" />
      <el-table-column :label="t('common.actions')" width="120" fixed="right">
        <template #default="{ row }">
          <el-button link type="danger" :icon="Delete" @click="closeConnection(row)">{{ t('connections.close') }}</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty :description="t('connections.empty')" :image-size="80" />
      </template>
    </el-table>
  </div>
</template>
