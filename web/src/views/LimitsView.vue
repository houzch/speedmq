<script setup lang="ts">
// 限制页：按 vhost 配置容量上限（max-connections / max-queues）。
//
// 与 RabbitMQ 的 vhost-limits 对齐，但**只支持正整数**：值为 0 或负数该怎么解释
// 没有公认口径（"一个也不许" 与 "不限制" 都说得通），因此服务端会直接拒绝，
// 界面上就用开关表达"限 / 不限"，不去暴露这个歧义。
import { computed, ref } from 'vue'
import { Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { VHostLimit, VHostLimitName } from '@/api/types'
import { useVhostStore } from '@/stores/vhost'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { showError } from '@/utils/message'

/** 支持的限制项（与后端 broker.LimitNames 一致） */
const LIMIT_NAMES: VHostLimitName[] = ['max-connections', 'max-queues']

const LIMIT_LABELS: Record<VHostLimitName, string> = {
  'max-connections': '最大连接数',
  'max-queues': '最大队列数',
}

const LIMIT_HINTS: Record<VHostLimitName, string> = {
  'max-connections': '该 vhost 同时打开的连接数上限；超出后新连接会被拒（ACCESS_REFUSED）',
  'max-queues': '该 vhost 的队列数上限；超出后新建队列会被拒（ACCESS_REFUSED）',
}

const vhostStore = useVhostStore()
const refresh = useRefreshStore()

const limits = ref<VHostLimit[]>([])
const loading = ref(false)
const saving = ref(false)

/** vhost → （限制名 → 值） */
const limitMap = computed<Record<string, Record<string, number>>>(() => {
  const out: Record<string, Record<string, number>> = {}
  for (const item of limits.value) {
    out[item.vhost] = item.value
  }
  return out
})

async function load(): Promise<void> {
  loading.value = true
  try {
    const [list] = await Promise.all([api.vhostLimits(), vhostStore.loadVhosts()])
    limits.value = list
    refresh.markRefreshed()
  } catch (error) {
    showError(error, '获取虚拟主机限制失败')
  } finally {
    loading.value = false
  }
}

function valueOf(vhost: string, name: VHostLimitName): number | null {
  const value = limitMap.value[vhost]?.[name]
  return value === undefined ? null : value
}

// ---- 编辑 ----
const dialogVisible = ref(false)
const form = ref({
  vhost: '',
  entries: {} as Record<VHostLimitName, { limited: boolean; value: number }>,
})

function openEdit(vhost: string): void {
  const entries = {} as Record<VHostLimitName, { limited: boolean; value: number }>
  for (const name of LIMIT_NAMES) {
    const current = valueOf(vhost, name)
    entries[name] = {
      limited: current !== null,
      // 未配置时给一个便于起手的默认值，避免用户面对 0 无从下手
      value: current ?? (name === 'max-connections' ? 100 : 1000),
    }
  }
  form.value = { vhost, entries }
  dialogVisible.value = true
}

async function submit(): Promise<void> {
  const { vhost, entries } = form.value
  for (const name of LIMIT_NAMES) {
    const entry = entries[name]
    if (entry.limited && (!Number.isInteger(entry.value) || entry.value <= 0)) {
      ElMessage.warning(`「${LIMIT_LABELS[name]}」必须是大于 0 的整数`)
      return
    }
  }
  saving.value = true
  try {
    for (const name of LIMIT_NAMES) {
      const entry = entries[name]
      if (entry.limited) {
        await api.setVHostLimit(vhost, name, entry.value)
      } else {
        // 未开启即"不限制"：删掉这条限制（不存在也返回 204，幂等）
        await api.deleteVHostLimit(vhost, name)
      }
    }
    ElMessage.success(`虚拟主机 ${vhost} 的限制已保存`)
    dialogVisible.value = false
    await load()
  } catch (error) {
    showError(error, '保存限制失败')
  } finally {
    saving.value = false
  }
}

useAutoRefresh(load)
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <h2 class="page-title">限制</h2>
        <div class="page-subtitle">按虚拟主机限制连接数与队列数；未配置表示不限制</div>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="load">刷新</el-button>
    </div>

    <el-table :data="vhostStore.vhosts" stripe>
      <el-table-column label="虚拟主机" min-width="200">
        <template #default="{ row }">
          <span class="mono">{{ row.name }}</span>
        </template>
      </el-table-column>
      <el-table-column v-for="name in LIMIT_NAMES" :key="name" :label="LIMIT_LABELS[name]" min-width="160">
        <template #default="{ row }">
          <span v-if="valueOf(row.name, name) !== null" class="mono">{{ valueOf(row.name, name) }}</span>
          <span v-else class="page-subtitle">不限制</span>
        </template>
      </el-table-column>
      <el-table-column label="操作" width="120" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="openEdit(row.name)">配置</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <el-empty description="没有虚拟主机" :image-size="80" />
      </template>
    </el-table>

    <el-dialog v-model="dialogVisible" :title="`配置限制：${form.vhost}`" width="520px">
      <el-form label-width="110px">
        <el-form-item v-for="name in LIMIT_NAMES" :key="name" :label="LIMIT_LABELS[name]">
          <div style="width: 100%">
            <el-switch
              v-model="form.entries[name].limited"
              active-text="限制"
              inactive-text="不限制"
            />
            <el-input-number
              v-if="form.entries[name].limited"
              v-model="form.entries[name].value"
              :min="1"
              style="margin-left: 12px"
            />
            <div class="page-subtitle" style="margin-top: 4px">{{ LIMIT_HINTS[name] }}</div>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="submit">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>
