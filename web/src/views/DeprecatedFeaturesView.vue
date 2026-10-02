<script setup lang="ts">
// 弃用特性页：只读清单。
//
// RabbitMQ 4.x 里弃用特性已经"随版本定型"（per-vhost 的 enable/disable 接口返回 405），
// 因此这里如实做成只读：每条说明本实现对某个旧能力的最终态度，并给出内核里的依据。
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { DeprecatedFeature } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { showError } from '@/utils/message'

const refresh = useRefreshStore()
const { t } = useI18n()
const features = ref<DeprecatedFeature[]>([])
const loading = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    features.value = await api.deprecatedFeatures()
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('deprecatedFeatures.loadFailed'))
  } finally {
    loading.value = false
  }
}

/** 弃用阶段的中文/本地化名称；未知阶段原样返回 */
function phaseLabel(phase: string): string {
  switch (phase) {
    case 'denied_by_default':
      return t('deprecatedFeatures.phaseDenied')
    case 'permitted_by_default':
      return t('deprecatedFeatures.phasePermitted')
    case 'removed':
      return t('deprecatedFeatures.phaseRemoved')
    default:
      return phase
  }
}

// 清单随版本定型、运行期不会变，但依然走统一的自动刷新入口：
// 它负责"未认证不加载、登录后立刻补一次"，是各页一致的挂载时机。
useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">{{ t('deprecatedFeatures.title') }}</h2>
        <div class="page-subtitle">{{ t('deprecatedFeatures.total', { count: features.length }) }}</div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">{{ t('common.refresh') }}</el-button>
    </div>

    <el-table :data="features" stripe>
      <el-table-column :label="t('deprecatedFeatures.colName')" min-width="220">
        <template #default="{ row }"><span class="mono">{{ row.name }}</span></template>
      </el-table-column>
      <el-table-column :label="t('deprecatedFeatures.colState')" width="110">
        <template #default="{ row }">
          <el-tag :type="row.state === 'permitted' ? 'success' : 'info'" size="small">
            {{ row.state === 'permitted' ? t('deprecatedFeatures.permitted') : t('deprecatedFeatures.denied') }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('deprecatedFeatures.colPhase')" width="130">
        <template #default="{ row }">
          <el-tag type="info" effect="plain" size="small">{{ phaseLabel(row.deprecation_phase) }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('deprecatedFeatures.colDesc')" min-width="360">
        <template #default="{ row }">
          <div>{{ row.desc }}</div>
          <a v-if="row.doc_url" :href="row.doc_url" target="_blank" rel="noopener" class="mono doc-link">
            {{ row.doc_url }}
          </a>
        </template>
      </el-table-column>
      <el-table-column :label="t('deprecatedFeatures.colProvidedBy')" width="110">
        <template #default="{ row }"><span class="mono">{{ row.provided_by }}</span></template>
      </el-table-column>
      <template #empty>
        <el-empty :description="t('deprecatedFeatures.empty')" :image-size="80" />
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
