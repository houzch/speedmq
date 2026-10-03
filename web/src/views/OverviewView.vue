<script setup lang="ts">
// 概览页：对象/消息统计 + 节点信息 + 前端轮询采样的趋势图
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import { Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { NodeInfo, Overview } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { formatBytes, formatDuration, formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

echarts.use([LineChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer])

const refresh = useRefreshStore()
const { t, locale } = useI18n()

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

/** 顶部统计卡片：标签随语言切换 */
const statCards = computed(() => [
  { label: t('overview.connections'), value: overview.value?.object_totals.connections },
  { label: t('overview.channels'), value: overview.value?.object_totals.channels },
  { label: t('overview.queues'), value: overview.value?.object_totals.queues },
  { label: t('overview.consumers'), value: overview.value?.object_totals.consumers },
  { label: t('overview.exchanges'), value: overview.value?.object_totals.exchanges },
])

const chartRef = ref<HTMLDivElement | null>(null)
let chart: ReturnType<typeof echarts.init> | null = null

/** 渲染趋势图（图表数据全部来自前端累积的采样点） */
function renderChart(): void {
  if (!chart) return
  const seriesNames = [t('overview.chartMessages'), t('overview.chartPublish'), t('overview.chartDeliver')]
  chart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: seriesNames },
    grid: { left: 56, right: 24, top: 40, bottom: 32 },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: samples.value.map((item) => item.time),
    },
    yAxis: { type: 'value' },
    series: [
      {
        name: seriesNames[0],
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: samples.value.map((item) => item.messages),
      },
      {
        name: seriesNames[1],
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: samples.value.map((item) => item.publish),
      },
      {
        name: seriesNames[2],
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
      time: new Date().toLocaleTimeString(String(locale.value), { hour12: false }),
      messages: data.queue_totals.messages,
      publish: data.message_stats.publish ?? 0,
      deliver: data.message_stats.deliver ?? 0,
    })
    if (samples.value.length > MAX_SAMPLES) {
      samples.value.splice(0, samples.value.length - MAX_SAMPLES)
    }
    renderChart()
  } catch (error) {
    if (!silent) showError(error, t('overview.loadFailed'))
  }
}

async function loadNodes(): Promise<void> {
  nodesLoading.value = true
  try {
    nodes.value = await api.nodes()
  } catch (error) {
    showError(error, t('overview.loadNodesFailed'))
  } finally {
    nodesLoading.value = false
  }
}

/** 组合入口：概览 + 节点；供顶栏刷新按钮与全局自动刷新共用。
 *
 * 采样统一跟随本函数（每次拉取追加一个采样点），不再另开一个定时器重复请求 overview ——
 * 之前"独立采样定时器 + 自动刷新"会让同一周期请求两次、并重复压入两个采样点。 */
async function refreshAll(): Promise<void> {
  await Promise.all([loadOverview(), loadNodes()])
  refresh.markRefreshed()
}

// 切换语言后重绘图例/系列名（图表不参与 Vue 的响应式渲染）
watch(locale, renderChart)

onMounted(() => {
  if (chartRef.value) {
    chart = echarts.init(chartRef.value)
  }
  window.addEventListener('resize', resizeChart)
})

// 首次加载与自动刷新都走 refreshAll；图表已在上面的 onMounted 中初始化
useAutoRefresh(refreshAll)

onBeforeUnmount(() => {
  window.removeEventListener('resize', resizeChart)
  chart?.dispose()
  chart = null
})
</script>

<template>
  <div>
    <div class="page-header">
      <h2 class="page-title">{{ t('overview.title') }}</h2>
      <el-button :icon="Refresh" @click="refreshAll">{{ t('common.refresh') }}</el-button>
    </div>

    <el-row :gutter="16">
      <el-col v-for="item in statCards" :key="item.label" :xs="12" :sm="8" :md="4">
        <el-card class="section-card" shadow="never">
          <div class="stat-label">{{ item.label }}</div>
          <div class="stat-value">{{ formatNumber(item.value, 0) }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="16">
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>{{ t('overview.queueMessages') }}</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item :label="t('overview.messagesTotal')">
              {{ formatNumber(overview?.queue_totals.messages, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('overview.messagesReady')">
              {{ formatNumber(overview?.queue_totals.messages_ready, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('overview.messagesUnacked')">
              {{ formatNumber(overview?.queue_totals.messages_unacknowledged, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('overview.publishTotal')">
              {{ formatNumber(overview?.message_stats.publish, 0) }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('overview.deliverTotal')">
              {{ formatNumber(overview?.message_stats.deliver, 0) }}
            </el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
      <el-col :xs="24" :md="12">
        <el-card class="section-card" shadow="never">
          <template #header>{{ t('overview.serviceInfo') }}</template>
          <el-descriptions :column="1" border>
            <el-descriptions-item :label="t('overview.product')">
              {{ overview?.product_name ?? '—' }} {{ overview?.product_version ?? '' }}
            </el-descriptions-item>
            <el-descriptions-item :label="t('overview.managementVersion')">{{ overview?.management_version ?? '—' }}</el-descriptions-item>
            <el-descriptions-item :label="t('overview.amqpVersion')">{{ overview?.rabbitmq_version ?? '—' }}</el-descriptions-item>
            <el-descriptions-item :label="t('overview.clusterName')">{{ overview?.cluster_name ?? '—' }}</el-descriptions-item>
            <el-descriptions-item :label="t('overview.currentNode')">{{ overview?.node ?? '—' }}</el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
    </el-row>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div class="page-header" style="margin-bottom: 0">
          <span>{{ t('overview.trend') }}</span>
          <span class="page-subtitle">{{ t('overview.samples', { count: samples.length }) }}</span>
        </div>
      </template>
      <div ref="chartRef" class="chart" />
      <el-empty v-if="samples.length === 0" :description="t('overview.noSamples')" :image-size="80" />
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div class="page-header" style="margin-bottom: 0">
          <span>{{ t('overview.nodes') }}</span>
          <el-button :icon="Refresh" size="small" :loading="nodesLoading" @click="loadNodes">{{ t('common.refresh') }}</el-button>
        </div>
      </template>
      <el-table v-loading="nodesLoading" :data="nodes" stripe>
        <el-table-column :label="t('overview.nodeName')" prop="name" min-width="200" />
        <el-table-column :label="t('overview.nodeType')" prop="type" width="90" />
        <el-table-column :label="t('overview.runningState')" width="100">
          <template #default="{ row }">
            <el-tag :type="row.running ? 'success' : 'danger'">{{ row.running ? t('overview.runningOn') : t('overview.stopped') }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column :label="t('overview.uptime')" width="180">
          <template #default="{ row }">{{ formatDuration(row.uptime / 1000) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.memUsed')" width="140">
          <template #default="{ row }">{{ formatBytes(row.mem_used) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.memLimit')" width="140">
          <template #default="{ row }">{{ formatBytes(row.mem_limit) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.diskFree')" width="140">
          <template #default="{ row }">{{ formatBytes(row.disk_free) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.procUsed')" width="90">
          <template #default="{ row }">{{ formatNumber(row.proc_used, 0) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.fdUsed')" width="110">
          <template #default="{ row }">{{ formatNumber(row.fd_used, 0) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.sockets')" width="90">
          <template #default="{ row }">{{ formatNumber(row.sockets_used, 0) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.osPid')" width="110">
          <template #default="{ row }">{{ formatNumber(row.os_pid, 0) }}</template>
        </el-table-column>
        <el-table-column :label="t('overview.enabledPlugins')">
          <template #default="{ row }">
            <el-tag v-for="plugin in row.enabled_plugins" :key="plugin" size="small" effect="plain" class="mono">
              {{ plugin }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>{{ t('overview.listeners') }}</template>
      <el-table :data="overview?.listeners ?? []" stripe>
        <el-table-column :label="t('overview.listenerNode')" prop="node" min-width="200" />
        <el-table-column :label="t('overview.protocol')" prop="protocol" width="120" />
        <el-table-column :label="t('overview.listenAddr')" prop="ip_address" width="160" />
        <el-table-column :label="t('overview.port')" prop="port" width="100" />
      </el-table>
    </el-card>

  </div>
</template>
