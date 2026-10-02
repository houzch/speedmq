<script setup lang="ts">
// 弃用特性页：只读清单。
//
// RabbitMQ 4.x 里弃用特性已经"随版本定型"（per-vhost 的 enable/disable 接口返回 405），
// 因此这里如实做成只读：每条说明本实现对某个旧能力的最终态度，并给出内核里的依据。
import { ref } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { DeprecatedFeature } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { showError } from '@/utils/message'

/** 弃用阶段的中文名 */
const PHASE_LABELS: Record<string, string> = {
  denied_by_default: '默认拒绝',
  permitted_by_default: '默认允许',
  removed: '已移除',
}

const refresh = useRefreshStore()
const features = ref<DeprecatedFeature[]>([])
const loading = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    features.value = await api.deprecatedFeatures()
    refresh.markRefreshed()
  } catch (error) {
    showError(error, '获取弃用特性失败')
  } finally {
    loading.value = false
  }
}

function phaseLabel(phase: string): string {
  return PHASE_LABELS[phase] ?? phase
}

// 清单随版本定型、运行期不会变，但依然走统一的自动刷新入口：
// 它负责"未认证不加载、登录后立刻补一次"，是各页一致的挂载时机。
useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">弃用特性</h2>
        <div class="page-subtitle">
          共 {{ features.length }} 项；这是只读清单，说明本实现对旧能力的最终态度
        </div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
    </div>

    <el-table :data="features" stripe>
      <el-table-column label="名称" min-width="220">
        <template #default="{ row }"><span class="mono">{{ row.name }}</span></template>
      </el-table-column>
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag :type="row.state === 'permitted' ? 'success' : 'info'" size="small">
            {{ row.state === 'permitted' ? '仍允许' : '已拒绝' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="弃用阶段" width="130">
        <template #default="{ row }">
          <el-tag type="info" effect="plain" size="small">{{ phaseLabel(row.deprecation_phase) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="说明" min-width="360">
        <template #default="{ row }">
          <div>{{ row.desc }}</div>
          <a v-if="row.doc_url" :href="row.doc_url" target="_blank" rel="noopener" class="mono doc-link">
            {{ row.doc_url }}
          </a>
        </template>
      </el-table-column>
      <el-table-column label="提供方" width="110">
        <template #default="{ row }"><span class="mono">{{ row.provided_by }}</span></template>
      </el-table-column>
      <template #empty>
        <el-empty description="没有弃用特性信息" :image-size="80" />
      </template>
    </el-table>
  </div>
</template>

<style scoped>
.doc-link {
  font-size: 12px;
  word-break: break-all;
}
</style>
