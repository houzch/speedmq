<script setup lang="ts">
// 概览页：对象/消息统计 + 节点信息 + 前端轮询采样的趋势图
import { onBeforeUnmount, onMounted, ref } from 'vue'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { NodeInfo, Overview } from '@/api/types'
import { formatBytes, formatDuration, formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

echarts.use([LineChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer])

/** 轮询间隔（毫秒）：每 5 秒采样一次 /api/overview */
const POLL_INTERVAL = 5_000
/** 内存中保留的最大样本数 */
const MAX_SAMPLES = 60

interface TrendSample {
  time: string
  messages: number
  publish: number
  deliver: number
}

const overview = ref<Overview | null>(null)
const nodes = ref<NodeInfo[]>([])
const nodesLoading = ref(false)
const samples = ref<TrendSample[]>([])

const chartRef = ref<HTMLDivElement | null>(null)
let chart: ReturnType<typeof echarts.init> | null = null
let pollTimer: number | null = null

/** 渲染趋势图（图表数据全部来自前端累积的采样点） */
function renderChart(): void {
  if (!chart) return
  chart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['队列消息总数', '累计发布', '累计投递'] },
    grid: { left: 56, right: 24, top: 40, bottom: 32 },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: samples.value.map((item) => item.time),
    },
    yAxis: { type: 'value' },
    series: [
      {
        name: '队列消息总数',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: samples.value.map((item) => item.messages),
      },
      {
        name: '累计发布',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: samples.value.map((item) => item.publish),
      },
      {
        name: '累计投递',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: samples.value.map((item) => item.deliver),
      },
    ],
  })
}

function resizeChart(): void {
  chart?.resize()
}

/** 拉取概览并追加一个采样点；silent 用于轮询时抑制重复报错 */
async function loadOverview(silent = false): Promise<void> {
  try {
    const data = await api.overview()
    overview.value = data
    samples.value.push({
      time: new Date().toLocaleTimeString('zh-CN', { hour12: false }),
      messages: data.queue_totals.messages,
      publish: data.message_stats.publish ?? 0,
      deliver: data.message_stats.deliver ?? 0,
    })
    if (samples.value.length > MAX_SAMPLES) {
      samples.value.splice(0, samples.value.length - MAX_SAMPLES)
    }
    renderChart()
  } catch (error) {
    if (!silent) showError(error, '获取概览信息失败')
  }
}

async function loadNodes(): Promise<void> {
  nodesLoading.value = true
  try {
    nodes.value = await api.nodes()
  } catch (error) {
    showError(error, '获取节点信息失败')
  } finally {
    nodesLoading.value = false
  }
}

async function refreshAll(): Promise<void> {
  await Promise.all([loadOverview(), loadNodes()])
}

onMounted(() => {
  if (chartRef.value) {
    chart = echarts.init(chartRef.value)
  }
  void refreshAll()
  pollTimer = window.setInterval(() => {
    void loadOverview(true)
  }, POLL_INTERVAL)
  window.addEventListener('resize', resizeChart)
})

onBeforeUnmount(() => {
  if (pollTimer !== null) window.clearInterval(pollTimer)
  window.removeEventListener('resize', resizeChart)
  chart?.dispose()
  chart = null
})
</script>

<template>
  <div>
    <div class="page-header">
      <h2 class="page-title">概览</h2>
      <el-button :icon="Refresh" @click="refreshAll">刷新</el-button>
    </div>

    <el-row :gutter="16">
      <el-col v-for="item in [
        { label: '连接数', value: overview?.object_totals.connections },
        { label: '通道数', value: overview?.object_totals.channels },
        { label: '队列数', value: overview?.object_totals.queues },
        { label: '消费者数', value: overview?.object_totals.consumers },
        { label: '交换机数', value: overview?.object_totals.exchanges },
      ]" :key="item.label" :xs="12" :sm="8" :md="4">
        <el-card class="section-card" shadow="never">
          <div class="stat-label">{{ item.label }}</div>
          <div class="stat-value">{{ formatNumber(item.value, 0) }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>队列消息</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="消息总数">
              {{ formatNumber(overview?.queue_totals.messages, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="就绪消息">
              {{ formatNumber(overview?.queue_totals.messages_ready, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="未确认消息">
              {{ formatNumber(overview?.queue_totals.messages_unacknowledged, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="累计发布">
              {{ formatNumber(overview?.message_stats.publish, 0) }}
            </el-descriptions-item>
            <el-descriptions-item label="累计投递">
              {{ formatNumber(overview?.message_stats.deliver, 0) }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>服务信息</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item label="产品">
              {{ overview?.product_name ?? '—' }} {{ overview?.product_version ?? '' }}
            </el-descriptions-item>
            <el-descriptions-item label="管理版本">{{ overview?.management_version ?? '—' }}</el-descriptions-item>
            <el-descriptions-item label="AMQP 版本">{{ overview?.rabbitmq_version ?? '—' }}</el-descriptions-item>
            <el-descriptions-item label="集群名称">{{ overview?.cluster_name ?? '—' }}</el-descriptions-item>
            <el-descriptions-item label="当前节点">{{ overview?.node ?? '—' }}</el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
    </el-row>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div class="page-header" style="margin-bottom: 0">
          <span>趋势（前端每 5 秒采样一次，仅保留最近 60 个点）</span>
          <span class="page-subtitle">共 {{ samples.length }} 个采样点</span>
        </div>
      </template>
      <div ref="chartRef" class="chart" />
      <el-empty v-if="samples.length === 0" description="暂无采样数据" :image-size="80" />
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div class="page-header" style="margin-bottom: 0">
          <span>节点</span>
          <el-button :icon="Refresh" size="small" :loading="nodesLoading" @click="loadNodes">刷新</el-button>
        </div>
      </template>
      <el-table v-loading="nodesLoading" :data="nodes" stripe>
        <el-table-column label="节点名称" prop="name" min-width="200" />
        <el-table-column label="类型" prop="type" width="90" />
        <el-table-column label="运行状态" width="100">
          <template #default="{ row }">
            <el-tag :type="row.running ? 'success' : 'danger'">{{ row.running ? '运行中' : '已停止' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="运行时长" width="180">
          <template #default="{ row }">{{ formatDuration(row.uptime) }}</template>
        </el-table-column>
        <el-table-column label="内存占用" width="140">
          <template #default="{ row }">{{ formatBytes(row.mem_used) }}</template>
        </el-table-column>
        <el-table-column label="内存上限" width="140">
          <template #default="{ row }">{{ formatBytes(row.mem_limit) }}</template>
        </el-table-column>
        <el-table-column label="磁盘剩余" width="140">
          <template #default="{ row }">{{ formatBytes(row.disk_free) }}</template>
        </el-table-column>
        <el-table-column label="进程数" width="90">
          <template #default="{ row }">{{ formatNumber(row.proc_used, 0) }}</template>
        </el-table-column>
        <el-table-column label="文件描述符" width="110">
          <template #default="{ row }">{{ formatNumber(row.fd_used, 0) }}</template>
        </el-table-column>
        <el-table-column label="套接字" width="90">
          <template #default="{ row }">{{ formatNumber(row.sockets_used, 0) }}</template>
        </el-table-column>
        <el-table-column label="系统进程号" width="110">
          <template #default="{ row }">{{ formatNumber(row.os_pid, 0) }}</template>
        </el-table-column>
        <el-table-column label="启用插件">
          <template #default="{ row }">
            <el-tag v-for="plugin in row.enabled_plugins" :key="plugin" size="small" effect="plain" class="mono">
              {{ plugin }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>监听器</template>
      <el-table :data="overview?.listeners ?? []" stripe>
        <el-table-column label="节点" prop="node" min-width="200" />
        <el-table-column label="协议" prop="protocol" width="120" />
        <el-table-column label="监听地址" prop="ip_address" width="160" />
        <el-table-column label="端口" prop="port" width="100" />
      </el-table>
    </el-card>

  </div>
</template>
