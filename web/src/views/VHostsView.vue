<script setup lang="ts">
// 虚拟主机页：列出全部 vhost 的规模与运行状态，并支持新建、删除。
//
// 删除是**级联**的：队列、交换机、绑定、权限、策略、限制都会一并清除（服务端逐条提交），
// 因此确认框里必须把这一点说清楚，而不是只写"确定删除吗"。
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { Plus, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Vhost } from '@/api/types'
import { useVhostStore } from '@/stores/vhost'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

const vhostStore = useVhostStore()
const refresh = useRefreshStore()
const { t } = useI18n()
const loading = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    await vhostStore.loadVhosts()
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('vhosts.loadFailed'))
  } finally {
    loading.value = false
  }
}

/** 该 vhost 在本节点是否处于运行状态（用于标签颜色，不参与文案拼接） */
function isRunning(row: Pick<Vhost, 'cluster_state'>): boolean {
  const states = Object.values(row.cluster_state)
  return states.length > 0 && states.every((state) => state === 'running')
}

/** 运行状态文案：全部 running 显示"运行中"，否则原样罗列各节点状态 */
function clusterStateLabel(row: Pick<Vhost, 'cluster_state'>): string {
  const states = Object.values(row.cluster_state)
  if (states.length === 0) return '—'
  return isRunning(row) ? t('vhosts.running') : states.join(' / ')
}

// ---- 新建 ----
const createVisible = ref(false)
const createName = ref('')
const creating = ref(false)

function openCreate(): void {
  createName.value = ''
  createVisible.value = true
}

async function submitCreate(): Promise<void> {
  const name = createName.value.trim()
  if (!name) {
    ElMessage.warning(t('vhosts.nameRequired'))
    return
  }
  creating.value = true
  try {
    await api.createVHost(name)
    ElMessage.success(t('vhosts.created', { name }))
    createVisible.value = false
    await load()
  } catch (error) {
    showError(error, t('vhosts.createFailed'))
  } finally {
    creating.value = false
  }
}

// ---- 删除 ----
async function removeVHost(name: string): Promise<void> {
  try {
    await ElMessageBox.confirm(t('vhosts.deleteConfirm', { name }), t('vhosts.deleteTitle'), {
      type: 'warning',
      confirmButtonText: t('common.delete'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.deleteVHost(name)
    ElMessage.success(t('vhosts.deleted', { name }))
    await load()
  } catch (error) {
    showError(error, t('vhosts.deleteFailed'))
  }
}

useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">{{ t('vhosts.title') }}</h2>
        <div class="page-subtitle">{{ t('vhosts.total', { count: vhostStore.vhosts.length }) }}</div>
      </div>
      <div>
        <el-button type="primary" :icon="Plus" @click="openCreate">{{ t('vhosts.create') }}</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="load">{{ t('common.refresh') }}</el-button>
      </div>
    </div>

    <el-table :data="vhostStore.vhosts" stripe>
      <el-table-column :label="t('vhosts.colName')" min-width="200">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('vhosts.colMessages')" width="140">
        <template #default="{ row }">{{ formatNumber(row.messages, 0) }}</template>
      </el-table-column>
      <el-table-column :label="t('vhosts.colMessagesReady')" width="140">
        <template #default="{ row }">{{ formatNumber(row.messages_ready, 0) }}</template>
      </el-table-column>
      <el-table-column :label="t('vhosts.colMessagesUnacked')" width="140">
        <template #default="{ row }">{{ formatNumber(row.messages_unacknowledged, 0) }}</template>
      </el-table-column>
      <el-table-column :label="t('vhosts.colState')" width="120">
        <template #default="{ row }">
          <el-tag :type="isRunning(row as Vhost) ? 'success' : 'warning'" size="small">
            {{ clusterStateLabel(row as Vhost) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('common.actions')" width="160" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="vhostStore.setCurrent(row.name)">{{ t('vhosts.setCurrent') }}</el-button>
          <el-button link type="danger" @click="removeVHost(row.name)">{{ t('common.delete') }}</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty :description="t('vhosts.empty')" :image-size="80" />
      </template>
    </el-table>

    <el-dialog v-model="createVisible" :title="t('vhosts.create')" width="460px">
      <el-form label-width="90px">
        <el-form-item :label="t('vhosts.colName')">
          <el-input v-model="createName" :placeholder="t('vhosts.namePlaceholder')" autocomplete="off" />
        </el-form-item>
      </el-form>
      <div class="page-subtitle">{{ t('vhosts.createNote') }}</div>
      <template #footer>
        <el-button @click="createVisible = false">{{ t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="creating" @click="submitCreate">{{ t('common.create') }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>
