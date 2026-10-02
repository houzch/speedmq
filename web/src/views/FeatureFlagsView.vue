<script setup lang="ts">
// 特性开关页：列出每个开关的当前状态，并支持开启 / 关闭。
//
// 这里的开关不是"页面装饰"：关掉之后对应的内核行为真的会变（并且协议层的能力声明
// 会同步收敛），所以列表里每个条目都带一句"关掉会发生什么"。注册表在后端，
// 前台只负责展示与切换。
import { ref } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { FeatureFlag } from '@/api/types'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { showError } from '@/utils/message'

const refresh = useRefreshStore()
const flags = ref<FeatureFlag[]>([])
const loading = ref(false)
/** 正在切换的开关名（用于禁用按钮，避免连点） */
const pending = ref<string | null>(null)

async function load(): Promise<void> {
  loading.value = true
  try {
    flags.value = await api.featureFlags()
    refresh.markRefreshed()
  } catch (error) {
    showError(error, '获取特性开关失败')
  } finally {
    loading.value = false
  }
}

async function toggle(row: FeatureFlag): Promise<void> {
  const next = row.state !== 'enabled'
  const verb = next ? '开启' : '关闭'
  try {
    // row.desc 里已经写清了"关掉会发生什么"，这里不再重复一句，否则提示会自相重复。
    await ElMessageBox.confirm(
      `确定要${verb}特性「${row.name}」吗？${row.desc}`,
      `${verb}特性`,
      { type: 'warning', confirmButtonText: verb, cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  pending.value = row.name
  try {
    await api.setFeatureFlag(row.name, next)
    ElMessage.success(`特性 ${row.name} 已${verb}`)
    await load()
  } catch (error) {
    showError(error, `${verb}特性失败`)
  } finally {
    pending.value = null
  }
}

useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">特性开关</h2>
        <div class="page-subtitle">
          共 {{ flags.length }} 个开关；只有内核里确有判定点的能力才会出现在这里
        </div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
    </div>

    <el-table :data="flags" stripe>
      <el-table-column label="名称" min-width="220">
        <template #default="{ row }"><span class="mono">{{ row.name }}</span></template>
      </el-table-column>
      <el-table-column label="状态" width="110">
        <template #default="{ row }">
          <el-tag :type="row.state === 'enabled' ? 'success' : 'info'" size="small">
            {{ row.state === 'enabled' ? '已开启' : '已关闭' }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="稳定性" width="110">
        <template #default="{ row }">
          <el-tag type="info" effect="plain" size="small">{{ row.stability }}</el-tag>
        </template>
      </el-table-column>
      <el-table-column label="说明" min-width="340">
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
      <el-table-column label="操作" width="110" fixed="right">
        <template #default="{ row }">
          <el-button
            link
            :type="row.state === 'enabled' ? 'danger' : 'primary'"
            :loading="pending === row.name"
            @click="toggle(row as FeatureFlag)"
          >
            {{ row.state === 'enabled' ? '关闭' : '开启' }}
          </el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="没有可用的特性开关" :image-size="80" />
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
