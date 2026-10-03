<script setup lang="ts">
// 插件管理页：列出随内核编译或外部进程（sidecar）接入的插件，支持热启用 / 热停用。
//
// 插件自带操作界面时（部署方在配置里声明了 console_url），这里提供「打开管理界面」直达入口；
// 跳转用**新标签页**而不是内嵌 iframe —— 插件页面自身的 X-Frame-Options / CSP 不受我们控制，
// 内嵌很容易白屏，新开标签页则始终可用。
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Plugin } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { showError } from '@/utils/message'

const refresh = useRefreshStore()
const { t } = useI18n()
const plugins = ref<Plugin[]>([])
const loading = ref(false)
/** 正在启停的插件名（用于禁用按钮，避免连点） */
const pending = ref<string | null>(null)

async function load(): Promise<void> {
  loading.value = true
  try {
    plugins.value = await api.plugins()
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('plugins.loadFailed'))
  } finally {
    loading.value = false
  }
}

/** 状态标签配色：failed/down 需要一眼看出异常 */
function stateTagType(state: string): 'success' | 'warning' | 'danger' | 'info' {
  if (state === 'enabled') return 'success'
  if (state === 'failed') return 'danger'
  if (state === 'down') return 'warning'
  return 'info'
}

/** 状态文案；出现未知状态时原样展示，便于排查前后端不一致 */
function stateLabel(state: string): string {
  const known = ['enabled', 'disabled', 'failed', 'down', 'stopped']
  return known.includes(state) ? t(`plugins.state.${state}`) : state
}

async function toggle(row: Plugin): Promise<void> {
  const next = row.state !== 'enabled'
  const verb = next ? t('common.enable') : t('common.disable')
  try {
    await ElMessageBox.confirm(
      next ? t('plugins.confirmEnable', { name: row.name }) : t('plugins.confirmDisable', { name: row.name }),
      verb,
      { type: 'warning', confirmButtonText: verb, cancelButtonText: t('common.cancel') },
    )
  } catch {
    return
  }
  pending.value = row.name
  try {
    if (next) {
      await api.enablePlugin(row.name)
    } else {
      await api.disablePlugin(row.name)
    }
    ElMessage.success(next ? t('plugins.enabled', { name: row.name }) : t('plugins.disabled', { name: row.name }))
    await load()
  } catch (error) {
    showError(error, t('plugins.toggleFailed'))
  } finally {
    pending.value = null
  }
}

/** 新标签页打开插件自带的管理界面 */
function openConsole(row: Plugin): void {
  if (!row.console_url) return
  window.open(row.console_url, '_blank', 'noopener')
}

useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">{{ t('plugins.title') }}</h2>
        <div class="page-subtitle">{{ t('plugins.subtitle', { count: plugins.length }) }}</div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">{{ t('common.refresh') }}</el-button>
    </div>

    <el-table :data="plugins" stripe>
      <el-table-column :label="t('common.name')" min-width="160">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
          <el-tag v-if="row.required" type="warning" effect="plain" size="small" class="tag-gap">
            {{ t('plugins.required') }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('plugins.colVersion')" width="100">
        <template #default="{ row }"><span class="mono">{{ row.version }}</span></template>
      </el-table-column>
      <el-table-column :label="t('common.type')" width="120">
        <template #default="{ row }">
          <el-tag size="small" effect="plain" type="info">
            {{ row.builtin ? t('plugins.typeBuiltin') : t('plugins.typeExternal') }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column :label="t('common.status')" width="120">
        <template #default="{ row }">
          <!-- 运行期原因（失败原因 / 外部进程断开原因）作为状态标签的悬浮说明 -->
          <el-tooltip :content="row.runtime_note" :disabled="!row.runtime_note" placement="top">
            <el-tag :type="stateTagType(row.state)" size="small">{{ stateLabel(row.state) }}</el-tag>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column :label="t('plugins.colCapabilities')" min-width="170">
        <template #default="{ row }">
          <template v-if="row.capabilities.length">
            <el-tag v-for="cap in row.capabilities" :key="cap" size="small" effect="plain" class="tag-gap">
              {{ cap }}
            </el-tag>
          </template>
          <span v-else>—</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('plugins.colDependencies')" min-width="120">
        <template #default="{ row }">
          <template v-if="row.dependencies.length">
            <el-tag v-for="dep in row.dependencies" :key="dep" size="small" effect="plain" class="tag-gap">
              {{ dep }}
            </el-tag>
          </template>
          <span v-else>—</span>
        </template>
      </el-table-column>
      <el-table-column :label="t('plugins.colDescription')" min-width="240">
        <template #default="{ row }">{{ row.description || '—' }}</template>
      </el-table-column>
      <el-table-column :label="t('common.actions')" width="220" fixed="right">
        <template #default="{ row }">
          <el-button
            link
            :type="row.state === 'enabled' ? 'danger' : 'primary'"
            :loading="pending === row.name"
            @click="toggle(row as Plugin)"
          >
            {{ row.state === 'enabled' ? t('common.disable') : t('common.enable') }}
          </el-button>
          <!-- 禁用的按钮不触发事件，用 span 包一层让 tooltip 仍可悬停 -->
          <el-tooltip :content="t('plugins.noConsole')" :disabled="!!row.console_url" placement="top">
            <span>
              <el-button link type="primary" :disabled="!row.console_url" @click="openConsole(row as Plugin)">
                {{ t('plugins.openConsole') }}
              </el-button>
            </span>
          </el-tooltip>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty :description="t('plugins.empty')" :image-size="80" />
      </template>
    </el-table>
  </div>
</template>

<style scoped>
.tag-gap {
  margin-right: 4px;
}
</style>
