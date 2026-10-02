<script setup lang="ts">
// 虚拟主机页：列出全部 vhost 的规模与运行状态，并支持新建、删除。
//
// 删除是**级联**的：队列、交换机、绑定、权限、策略、限制都会一并清除（服务端逐条提交），
// 因此确认框里必须把这一点说清楚，而不是只写"确定删除吗"。
import { ref } from 'vue'
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
const loading = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    await vhostStore.loadVhosts()
    refresh.markRefreshed()
  } catch (error) {
    showError(error, '获取虚拟主机列表失败')
  } finally {
    loading.value = false
  }
}

/** 该 vhost 在本节点是否处于运行状态 */
function clusterStateOf(row: Pick<Vhost, 'cluster_state'>): string {
  const states = Object.values(row.cluster_state)
  if (states.length === 0) return '—'
  return states.every((state) => state === 'running') ? '运行中' : states.join(' / ')
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
    ElMessage.warning('请输入虚拟主机名')
    return
  }
  creating.value = true
  try {
    await api.createVHost(name)
    ElMessage.success(`虚拟主机 ${name} 已创建`)
    createVisible.value = false
    await load()
  } catch (error) {
    showError(error, '新建虚拟主机失败')
  } finally {
    creating.value = false
  }
}

// ---- 删除 ----
async function removeVHost(name: string): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `确定要删除虚拟主机「${name}」吗？它的队列、交换机、绑定、权限、策略与限制都会一并移除，且不可恢复。`,
      '删除虚拟主机',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    await api.deleteVHost(name)
    ElMessage.success(`虚拟主机 ${name} 已删除`)
    await load()
  } catch (error) {
    showError(error, '删除虚拟主机失败')
  }
}

useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">虚拟主机</h2>
        <div class="page-subtitle">共 {{ vhostStore.vhosts.length }} 个虚拟主机</div>
      </div>
      <div>
        <el-button type="primary" :icon="Plus" @click="openCreate">新建虚拟主机</el-button>
        <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
      </div>
    </div>

    <el-table :data="vhostStore.vhosts" stripe>
      <el-table-column label="名称" min-width="200">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column label="消息总数" width="140">
        <template #default="{ row }">{{ formatNumber(row.messages, 0) }}</template>
      </el-table-column>
      <el-table-column label="就绪消息" width="140">
        <template #default="{ row }">{{ formatNumber(row.messages_ready, 0) }}</template>
      </el-table-column>
      <el-table-column label="未确认消息" width="140">
        <template #default="{ row }">{{ formatNumber(row.messages_unacknowledged, 0) }}</template>
      </el-table-column>
      <el-table-column label="运行状态" width="120">
        <template #default="{ row }">
          <el-tag :type="clusterStateOf(row as Vhost) === '运行中' ? 'success' : 'warning'" size="small">
            {{ clusterStateOf(row as Vhost) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="160" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="vhostStore.setCurrent(row.name)">设为当前</el-button>
          <el-button link type="danger" @click="removeVHost(row.name)">删除</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="没有虚拟主机" :image-size="80" />
      </template>
    </el-table>

    <el-dialog v-model="createVisible" title="新建虚拟主机" width="460px">
      <el-form label-width="90px">
        <el-form-item label="名称">
          <el-input v-model="createName" placeholder="例如 /staging" autocomplete="off" />
        </el-form-item>
      </el-form>
      <div class="page-subtitle">
        名称可以是任意路径风格字符串（如 /production）；同名重复提交是幂等的。
      </div>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" :loading="creating" @click="submitCreate">创建</el-button>
      </template>
    </el-dialog>
  </div>
</template>
