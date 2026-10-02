<script setup lang="ts">
// 集群页：展示元数据层（Raft）的状态与成员划分，并支持运行期增删成员。
//
// 单机模式下"增删成员"由服务端返回 501 NOT_IMPLEMENTED，界面在这里**直接禁用并说明原因**，
// 而不是让运维点了之后才看到报错。
import { computed, onMounted, ref } from 'vue'
import { Plus, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Cluster, ClusterMembers, NodeInfo } from '@/api/types'
import { formatDuration, formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

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
  } catch (error) {
    showError(error, '获取集群信息失败')
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
    ElMessage.warning('请填写节点 ID 与集群 RPC 地址')
    return
  }
  adding.value = true
  try {
    members.value = await api.addClusterMember(nodeId, addr)
    ElMessage.success(`节点 ${nodeId} 已加入集群`)
    addVisible.value = false
    await load()
  } catch (error) {
    showError(error, '加入集群成员失败')
  } finally {
    adding.value = false
  }
}

async function removeMember(id: string): Promise<void> {
  try {
    await ElMessageBox.confirm(
      `确定要把节点「${id}」移出集群吗？它将不再参与投票，也不再持有仲裁队列的副本。`,
      '移除集群成员',
      { type: 'warning', confirmButtonText: '移出集群', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    members.value = await api.removeClusterMember(id)
    ElMessage.success(`节点 ${id} 已移出集群`)
    await load()
  } catch (error) {
    showError(error, '移除集群成员失败')
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
        <h2 class="page-title">集群</h2>
        <div class="page-subtitle">
          元数据层与成员管理
          <span v-if="cluster">（模式 {{ cluster.mode }}，本节点 {{ cluster.node_id }}）</span>
        </div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
    </div>

    <el-alert
      v-if="cluster && !clusterEnabled"
      class="section-card"
      type="info"
      show-icon
      :closable="false"
      title="当前是单机模式，集群成员增删不可用"
      description="单机部署没有 Raft 成员表可变更（服务端对成员接口返回 501 NOT_IMPLEMENTED）。启用 cluster 后本页会出现成员管理。"
    />

    <el-alert
      v-else-if="cluster && cluster.paused"
      class="section-card"
      type="error"
      show-icon
      :closable="false"
      title="节点已暂停服务（pause_minority：与多数派失联）"
      description="该节点上的客户端连接会被主动断开，请把客户端重连到集群中的其他节点。"
    />

    <el-row :gutter="16">
      <el-col v-for="item in [
        { label: '角色', value: cluster?.role ?? '—', small: true },
        { label: '任期', value: cluster ? String(cluster.term) : '—', small: true },
        { label: '领导者', value: cluster?.leader || '（未选出）', small: true },
        { label: '拥有多数派', value: cluster ? (cluster.has_quorum ? '是' : '否') : '—', small: true },
        { label: '投票成员', value: cluster ? String(cluster.peers.length) : '—', small: false },
        { label: '非投票成员', value: cluster ? String(cluster.learners.length) : '—', small: false },
      ]" :key="item.label" :xs="12" :sm="8" :md="4">
        <el-card class="section-card" shadow="never">
          <div class="stat-label">{{ item.label }}</div>
          <div class="stat-value" :class="{ 'stat-value--small': item.small }">{{ item.value }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>元数据层状态</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="模式">
              {{ cluster?.mode ?? '—' }}{{ cluster && !cluster.enabled ? '（cluster 未启用）' : '' }}
            </el-descriptions-item>
            <el-descriptions-item label="本节点">{{ cluster?.node_id ?? '—' }}</el-descriptions-item>
            <el-descriptions-item label="角色">{{ cluster?.role ?? '—' }}</el-descriptions-item>
            <el-descriptions-item label="任期">{{ cluster ? formatNumber(cluster.term, 0) : '—' }}</el-descriptions-item>
            <el-descriptions-item label="领导者">{{ cluster?.leader || '（未选出）' }}</el-descriptions-item>
            <el-descriptions-item label="服务状态">
              <el-tag v-if="cluster?.paused" type="danger" size="small">已暂停</el-tag>
              <el-tag v-else type="success" size="small">正常</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="共识进度">
              提交 {{ formatNumber(cluster?.commit_index, 0) }} / 已应用
              {{ formatNumber(cluster?.last_applied, 0) }} / 累计 {{ formatNumber(cluster?.applied_records, 0) }} 条
            </el-descriptions-item>
            <el-descriptions-item label="元数据规模">
              队列 {{ formatNumber(cluster?.object_totals.queues, 0) }} ·
              交换机 {{ formatNumber(cluster?.object_totals.exchanges, 0) }} ·
              绑定 {{ formatNumber(cluster?.object_totals.bindings, 0) }} ·
              用户 {{ formatNumber(cluster?.object_totals.users, 0) }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>跨节点转发</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="代理消费者">
              {{ formatNumber(cluster?.forwarding.proxy_consumers, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="远端消费者">
              {{ formatNumber(cluster?.forwarding.remote_consumers, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="持有中的投递">
              {{ formatNumber(cluster?.forwarding.held_deliveries, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="转发出去">
              {{ formatNumber(cluster?.forwarding.forwarded_out, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="转发进来">
              {{ formatNumber(cluster?.forwarding.forwarded_in, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="推回的投递">
              {{ formatNumber(cluster?.forwarding.deliveries, 0) }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
    </el-row>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div class="page-header" style="margin-bottom: 0">
          <span>成员</span>
          <el-button
            type="primary"
            size="small"
            :icon="Plus"
            :disabled="!clusterEnabled"
            @click="openAdd"
          >
            加入成员
          </el-button>
        </div>
      </template>
      <el-table :data="memberRows" stripe>
        <el-table-column label="节点 ID" min-width="220">
          <template #default="{ row }">
            <span class="mono">{{ row.id }}</span>
          </template>
        </el-table-column>
        <el-table-column label="成员类型" width="140">
          <template #default="{ row }">
            <el-tag :type="row.role === 'voter' ? 'success' : 'info'" size="small">
              {{ row.role === 'voter' ? '投票成员' : '非投票成员' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="120" fixed="right">
          <template #default="{ row }">
            <el-button
              link
              type="danger"
              :disabled="!clusterEnabled || members.voters.length <= 1"
              @click="removeMember(row.id)"
            >
              移出
            </el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty description="没有集群成员信息" :image-size="80" />
        </template>
      </el-table>
      <div v-if="clusterEnabled" class="page-subtitle" style="margin-top: 8px">
        移出操作在集群中只剩一个投票成员时被禁用（那样会让集群再也选不出领导者）。
      </div>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>节点</template>
      <el-table :data="nodes" stripe>
        <el-table-column label="节点名称" prop="name" min-width="200" />
        <el-table-column label="运行状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.running ? 'success' : 'danger'" size="small">
              {{ row.running ? '运行中' : '已停止' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="运行时长" width="180">
          <template #default="{ row }">{{ formatDuration(row.uptime / 1000) }}</template>
        </el-table-column>
        <el-table-column label="内存占用" width="140">
          <template #default="{ row }">{{ formatNumber(row.mem_used / 1024 / 1024, 1) }} MiB</template>
        </el-table-column>
        <el-table-column label="磁盘剩余" width="140">
          <template #default="{ row }">{{ formatNumber(row.disk_free / 1024 / 1024, 1) }} MiB</template>
        </el-table-column>
        <el-table-column label="套接字" width="90">
          <template #default="{ row }">{{ formatNumber(row.sockets_used, 0) }}</template>
        </el-table-column>
        <el-table-column label="系统进程号" width="110">
          <template #default="{ row }">{{ formatNumber(row.os_pid, 0) }}</template>
        </el-table-column>
        <template #empty>
          <el-empty description="没有节点信息" :image-size="80" />
        </template>
      </el-table>
    </el-card>

    <el-dialog v-model="addVisible" title="加入集群成员" width="480px">
      <el-form label-width="120px">
        <el-form-item label="节点 ID">
          <el-input v-model="addForm.nodeId" placeholder="例如 swiftmq@node4" />
        </el-form-item>
        <el-form-item label="集群 RPC 地址">
          <el-input v-model="addForm.addr" placeholder="例如 10.0.0.4:25672" />
        </el-form-item>
      </el-form>
      <div class="page-subtitle">
        新节点需先以 learner 身份启动（cluster.join=true），加入后由后台追平日志再提升为投票成员，
        可能耗时几十秒。
      </div>
      <template #footer>
        <el-button @click="addVisible = false">取消</el-button>
        <el-button type="primary" :loading="adding" @click="submitAdd">加入</el-button>
      </template>
    </el-dialog>
  </div>
</template>
