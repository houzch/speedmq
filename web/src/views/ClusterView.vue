<script setup lang="ts">
// 集群页：展示元数据层（Raft）的状态与成员划分，并支持运行期增删成员。
//
// 单机模式下"增删成员"由服务端返回 501 NOT_IMPLEMENTED，界面在这里**直接禁用并说明原因**，
// 而不是让运维点了之后才看到报错。
import { computed, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { Plus, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Cluster, ClusterMembers, NodeInfo } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { formatDuration, formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

const refresh = useRefreshStore()
const { t } = useI18n()

const cluster = ref<Cluster | null>(null)
const members = ref<ClusterMembers>({ voters: [], learners: [] })
const nodes = ref<NodeInfo[]>([])
const loading = ref(false)

/** 只有集群模式才允许运行期增删成员（单机模式服务端返回 501） */
const clusterEnabled = computed<boolean>(() => cluster.value?.enabled === true)

/** 成员表：把投票/非投票成员摊平成一张表，便于逐行展示与操作 */
const memberRows = computed<{ id: string; role: 'voter' | 'learner' }[]>(() => [
  ...members.value.voters.map((id) => ({ id, role: 'voter' as const })),
  ...members.value.learners.map((id) => ({ id, role: 'learner' as const })),
])

/** 顶部统计卡片：标签与取值随语言/数据变化 */
const statCards = computed(() => [
  { label: t('cluster.role'), value: cluster.value?.role ?? '—', small: true },
  { label: t('cluster.term'), value: cluster.value ? String(cluster.value.term) : '—', small: true },
  { label: t('cluster.leader'), value: cluster.value?.leader || t('cluster.noLeader'), small: true },
  { label: t('cluster.hasQuorum'), value: cluster.value ? (cluster.value.has_quorum ? t('format.yes') : t('format.no')) : '—', small: true },
  { label: t('cluster.voters'), value: cluster.value ? String(cluster.value.peers.length) : '—', small: false },
  { label: t('cluster.learners'), value: cluster.value ? String(cluster.value.learners.length) : '—', small: false },
])

const addVisible = ref(false)
const addForm = ref({ nodeId: '', addr: '' })
const adding = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    const [cl, mem, nds] = await Promise.all([api.cluster(), api.clusterMembers(), api.nodes()])
    cluster.value = cl
    members.value = mem
    nodes.value = nds
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('cluster.loadFailed'))
  } finally {
    loading.value = false
  }
}

function openAdd(): void {
  if (!clusterEnabled.value) return
  addForm.value = { nodeId: '', addr: '' }
  addVisible.value = true
}

async function submitAdd(): Promise<void> {
  const nodeId = addForm.value.nodeId.trim()
  const addr = addForm.value.addr.trim()
  if (!nodeId || !addr) {
    ElMessage.warning(t('cluster.addRequired'))
    return
  }
  adding.value = true
  try {
    members.value = await api.addClusterMember(nodeId, addr)
    ElMessage.success(t('cluster.addSuccess', { node: nodeId }))
    addVisible.value = false
    await load()
  } catch (error) {
    showError(error, t('cluster.addFailed'))
  } finally {
    adding.value = false
  }
}

async function removeMember(id: string): Promise<void> {
  try {
    await ElMessageBox.confirm(t('cluster.removeConfirm', { id }), t('cluster.removeTitle'), {
      type: 'warning',
      confirmButtonText: t('cluster.removeButton'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    members.value = await api.removeClusterMember(id)
    ElMessage.success(t('cluster.removeSuccess', { id }))
    await load()
  } catch (error) {
    showError(error, t('cluster.removeFailed'))
  }
}

useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">{{ t('cluster.title') }}</h2>
        <div class="page-subtitle">
          {{ cluster ? t('cluster.subtitleWithMode', { mode: cluster.mode, node: cluster.node_id }) : t('cluster.subtitle') }}
        </div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">{{ t('common.refresh') }}</el-button>
    </div>

    <el-alert
      v-if="cluster && !clusterEnabled"
      class="section-card"
      type="info"
      show-icon
      :closable="false"
      :title="t('cluster.singleNodeAlertTitle')"
      :description="t('cluster.singleNodeAlertDesc')"
    />

    <el-alert
      v-else-if="cluster && cluster.paused"
      class="section-card"
      type="error"
      show-icon
      :closable="false"
      :title="t('cluster.pausedAlertTitle')"
      :description="t('cluster.pausedAlertDesc')"
    />

    <el-row :gutter="16">
      <el-col v-for="item in statCards" :key="item.label" :xs="12" :sm="8" :md="4">
        <el-card class="section-card" shadow="never">
          <div class="stat-label">{{ item.label }}</div>
          <div class="stat-value" :class="{ 'stat-value--small': item.small }">{{ item.value }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>{{ t('cluster.metaStatus') }}</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item :label="t('cluster.mode')">
              {{ cluster?.mode ?? '—' }}{{ cluster && !cluster.enabled ? t('cluster.clusterDisabledSuffix') : '' }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.thisNode')">{{ cluster?.node_id ?? '—' }}</el-descriptions-item>
            <el-descriptions-item :label="t('cluster.role')">{{ cluster?.role ?? '—' }}</el-descriptions-item>
            <el-descriptions-item :label="t('cluster.term')">{{ cluster ? formatNumber(cluster.term, 0) : '—' }}</el-descriptions-item>
            <el-descriptions-item :label="t('cluster.leader')">{{ cluster?.leader || t('cluster.noLeader') }}</el-descriptions-item>
            <el-descriptions-item :label="t('cluster.serviceState')">
              <el-tag v-if="cluster?.paused" type="danger" size="small">{{ t('cluster.paused') }}</el-tag>
              <el-tag v-else type="success" size="small">{{ t('cluster.normal') }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.consensus')">
              {{
                t('cluster.consensusValue', {
                  commit: formatNumber(cluster?.commit_index, 0),
                  applied: formatNumber(cluster?.last_applied, 0),
                  records: formatNumber(cluster?.applied_records, 0),
                })
              }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.metaScale')">
              {{
                t('cluster.metaScaleValue', {
                  queues: formatNumber(cluster?.object_totals.queues, 0),
                  exchanges: formatNumber(cluster?.object_totals.exchanges, 0),
                  bindings: formatNumber(cluster?.object_totals.bindings, 0),
                  users: formatNumber(cluster?.object_totals.users, 0),
                })
              }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>{{ t('cluster.forwarding') }}</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item :label="t('cluster.proxyConsumers')">
              {{ formatNumber(cluster?.forwarding.proxy_consumers, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.remoteConsumers')">
              {{ formatNumber(cluster?.forwarding.remote_consumers, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.heldDeliveries')">
              {{ formatNumber(cluster?.forwarding.held_deliveries, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.forwardedOut')">
              {{ formatNumber(cluster?.forwarding.forwarded_out, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.forwardedIn')">
              {{ formatNumber(cluster?.forwarding.forwarded_in, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('cluster.deliveries')">
              {{ formatNumber(cluster?.forwarding.deliveries, 0) }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
    </el-row>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div class="page-header" style="margin-bottom: 0">
          <span>{{ t('cluster.members') }}</span>
          <el-button
            type="primary"
            size="small"
            :icon="Plus"
            :disabled="!clusterEnabled"
            @click="openAdd"
          >
            {{ t('cluster.addMember') }}
          </el-button>
        </div>
      </template>
      <el-table :data="memberRows" stripe>
        <el-table-column :label="t('cluster.colNodeId')" min-width="220">
          <template #default="{ row }">
            <span class="mono">{{ row.id }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="t('cluster.colMemberType')" width="140">
          <template #default="{ row }">
            <el-tag :type="row.role === 'voter' ? 'success' : 'info'" size="small">
              {{ row.role === 'voter' ? t('cluster.memberVoter') : t('cluster.memberLearner') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="t('common.actions')" width="120" fixed="right">
          <template #default="{ row }">
            <el-button
              link
              type="danger"
              :disabled="!clusterEnabled || members.voters.length <= 1"
              @click="removeMember(row.id)"
            >
              {{ t('cluster.remove') }}
            </el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty :description="t('cluster.emptyMembers')" :image-size="80" />
        </template>
      </el-table>
      <div v-if="clusterEnabled" class="page-subtitle" style="margin-top: 8px">
        {{ t('cluster.removeHint') }}
      </div>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>{{ t('cluster.nodes') }}</template>
      <el-table :data="nodes" stripe>
        <el-table-column :label="t('cluster.colNodeName')" prop="name" min-width="200" />
        <el-table-column :label="t('cluster.colRunning')" width="100">
          <template #default="{ row }">
            <el-tag :type="row.running ? 'success' : 'danger'" size="small">
              {{ row.running ? t('cluster.running') : t('cluster.stopped') }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="t('cluster.colUptime')" width="180">
          <template #default="{ row }">{{ formatDuration(row.uptime / 1000) }}</template>
        </el-table-column>
        <el-table-column :label="t('cluster.colMem')" width="140">
          <template #default="{ row }">{{ formatNumber(row.mem_used / 1024 / 1024, 1) }} MiB</template>
        </el-table-column>
        <el-table-column :label="t('cluster.colDisk')" width="140">
          <template #default="{ row }">{{ formatNumber(row.disk_free / 1024 / 1024, 1) }} MiB</template>
        </el-table-column>
        <el-table-column :label="t('cluster.colSockets')" width="90">
          <template #default="{ row }">{{ formatNumber(row.sockets_used, 0) }}</template>
        </el-table-column>
        <el-table-column :label="t('cluster.colOsPid')" width="110">
          <template #default="{ row }">{{ formatNumber(row.os_pid, 0) }}</template>
        </el-table-column>
        <template #empty>
          <el-empty :description="t('cluster.emptyNodes')" :image-size="80" />
        </template>
      </el-table>
    </el-card>

    <el-dialog v-model="addVisible" :title="t('cluster.addTitle')" width="480px">
      <el-form label-width="120px">
        <el-form-item :label="t('cluster.colNodeId')">
          <el-input v-model="addForm.nodeId" :placeholder="t('cluster.nodeIdPlaceholder')" />
        </el-form-item>
        <el-form-item :label="t('cluster.addrLabel')">
          <el-input v-model="addForm.addr" :placeholder="t('cluster.addrPlaceholder')" />
        </el-form-item>
      </el-form>
      <div class="page-subtitle">{{ t('cluster.addNote') }}</div>
      <template #footer>
        <el-button @click="addVisible = false">{{ t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="adding" @click="submitAdd">{{ t('cluster.addButton') }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>
