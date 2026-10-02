<script setup lang="ts">
// 交换机详情：基本信息 + source 绑定 + 发布测试消息
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Delete, Plus, Promotion, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import { ApiError } from '@/api/client'
import type { Binding, Exchange, Queue } from '@/api/types'
import BindingArgsEditor, { buildArguments, type ArgRow } from '@/components/BindingArgsEditor.vue'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { formatBoolean, formatJson, formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

const route = useRoute()
const router = useRouter()

const refresh = useRefreshStore()

const vhost = computed<string>(() => String(route.params.vhost ?? '/'))
const exchangeName = computed<string>(() => String(route.params.name ?? ''))

const exchange = ref<Exchange | null>(null)
const bindings = ref<Binding[]>([])
const queues = ref<Queue[]>([])
const exchanges = ref<Exchange[]>([])
const loading = ref(false)

/** 添加绑定的表单（目标类型 + 目标名称 + 路由键 + 可选参数） */
const bindingVisible = ref(false)
const bindingSubmitting = ref(false)
const bindingForm = ref({
  destinationType: 'queue' as 'queue' | 'exchange',
  destination: '',
  routingKey: '',
  args: [] as ArgRow[],
})

const publishForm = reactive({
  routing_key: '',
  content_type: 'text/plain',
  delivery_mode: 2,
  headersText: '{}',
  payload: '{"hello":"swiftmq"}',
  mandatory: true,
})
const publishing = ref(false)

async function load(): Promise<void> {
  loading.value = true
  try {
    const [exchangeData, bindingList, queueList, exchangeList] = await Promise.all([
      api.exchange(vhost.value, exchangeName.value),
      api.exchangeSourceBindings(vhost.value, exchangeName.value),
      api.queues({ vhost: vhost.value }),
      api.exchanges({ vhost: vhost.value }),
    ])
    exchange.value = exchangeData
    bindings.value = bindingList
    queues.value = queueList
    exchanges.value = exchangeList
    refresh.markRefreshed()
  } catch (error) {
    showError(error, '加载交换机详情失败')
  } finally {
    loading.value = false
  }
}

/** 写操作错误提示：优先展示服务端中文 reason */
function messageOf(error: unknown): string {
  if (error instanceof ApiError) return error.reason || error.message
  if (error instanceof Error) return error.message
  return '操作失败'
}

/** 打开「添加绑定」对话框并重置表单 */
function openBinding(): void {
  bindingForm.value = { destinationType: 'queue', destination: '', routingKey: '', args: [] }
  bindingVisible.value = true
}

/**
 * 建立「本交换机 → 目标」绑定：
 * - 目标为队列：POST /api/bindings/{vhost}/e/{source}/q/{destination}  （bindQueue，源为本交换机）
 * - 目标为交换机：POST /api/bindings/{vhost}/e/{source}/e/{destination}
 */
async function submitBinding(): Promise<void> {
  const destination = bindingForm.value.destination.trim()
  if (!destination) {
    ElMessage.warning('请选择目标')
    return
  }
  const built = buildArguments(bindingForm.value.args)
  if (!built.ok) {
    ElMessage.warning(built.error)
    return
  }
  const body = { routing_key: bindingForm.value.routingKey, arguments: built.arguments }
  bindingSubmitting.value = true
  try {
    if (bindingForm.value.destinationType === 'queue') {
      await api.bindQueue(vhost.value, exchangeName.value, destination, body)
    } else {
      await api.bindExchange(vhost.value, exchangeName.value, destination, body)
    }
    ElMessage.success('绑定已建立')
    bindingVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  } finally {
    bindingSubmitting.value = false
  }
}

/**
 * 解绑：按目标类型选择删除路径，properties_key 直接用列表返回值。
 * - 目标为队列：DELETE /api/bindings/{vhost}/e/{source}/q/{destination}/{properties_key}
 * - 目标为交换机：DELETE /api/bindings/{vhost}/e/{source}/e/{destination}/{properties_key}
 */
async function unbindBinding(rawRow: Record<PropertyKey, unknown>): Promise<void> {
  const destination = String(rawRow.destination ?? '')
  const destinationType = String(rawRow.destination_type ?? '')
  const props = String(rawRow.properties_key ?? '')
  const routingKey = String(rawRow.routing_key ?? '')
  const typeLabel = destinationType === 'queue' ? '队列' : '交换机'
  try {
    await ElMessageBox.confirm(
      `确定要删除从本交换机到${typeLabel}「${destination}」的绑定（路由键「${routingKey || '~'}」）吗？`,
      '解绑',
      { type: 'warning', confirmButtonText: '解绑', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    if (destinationType === 'queue') {
      await api.unbindQueue(vhost.value, exchangeName.value, destination, props)
    } else {
      await api.unbindExchange(vhost.value, exchangeName.value, destination, props)
    }
    ElMessage.success('绑定已删除')
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  }
}

/** 切换目标类型时清空已选目标 */
watch(
  () => bindingForm.value.destinationType,
  () => {
    bindingForm.value.destination = ''
  },
)

/** 发布测试消息到当前交换机 */
async function publishMessage(): Promise<void> {
  let headers: Record<string, unknown> = {}
  if (publishForm.headersText.trim()) {
    try {
      headers = JSON.parse(publishForm.headersText) as Record<string, unknown>
    } catch {
      ElMessage.error('Headers 必须是合法的 JSON 对象')
      return
    }
  }

  publishing.value = true
  try {
    const result = await api.publish(vhost.value, exchangeName.value, {
      properties: {
        content_type: publishForm.content_type,
        delivery_mode: publishForm.delivery_mode,
        headers,
      },
      routing_key: publishForm.routing_key,
      payload: publishForm.payload,
      payload_encoding: 'string',
      mandatory: publishForm.mandatory,
    })
    if (result.routed) {
      ElMessage.success('消息已成功路由')
    } else {
      ElMessage.warning('消息已发布，但未路由到任何队列')
    }
  } catch (error) {
    showError(error, '发布消息失败')
  } finally {
    publishing.value = false
  }
}

function goBack(): void {
  void router.push({ name: 'exchanges', params: { vhost: vhost.value } })
}

/** 删除交换机：二次确认后删除，成功返回交换机列表 */
async function deleteExchange(): Promise<void> {
  try {
    await ElMessageBox.confirm(`确定要删除交换机「${exchangeName.value}」吗？该操作不可撤销。`, '删除交换机', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await api.deleteExchange(vhost.value, exchangeName.value)
    ElMessage.success('交换机已删除')
    await router.push({ name: 'exchanges', params: { vhost: vhost.value } })
  } catch (error) {
    showError(error, '删除交换机失败')
  }
}

useAutoRefresh(load)

watch([vhost, exchangeName], () => {
  publishForm.routing_key = ''
  void load()
})
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <el-button link :icon="ArrowLeft" @click="goBack">返回交换机列表</el-button>
        <h2 class="page-title">交换机详情：{{ exchangeName }}</h2>
        <div class="page-subtitle">虚拟主机：{{ vhost }}</div>
      </div>
      <div class="toolbar" style="margin-bottom: 0">
        <el-button :icon="Refresh" @click="load">刷新</el-button>
        <el-button type="danger" :icon="Delete" @click="deleteExchange">删除交换机</el-button>
      </div>
    </div>

    <el-card class="section-card" shadow="never">
      <template #header>基本信息</template>
      <el-descriptions :column="3" border>
        <el-descriptions-item label="名称">{{ exchange?.name ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="虚拟主机">{{ exchange?.vhost ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="类型">{{ exchange?.type ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="持久化">{{ formatBoolean(exchange?.durable) }}</el-descriptions-item>
        <el-descriptions-item label="自动删除">{{ formatBoolean(exchange?.auto_delete) }}</el-descriptions-item>
        <el-descriptions-item label="内部使用">{{ formatBoolean(exchange?.internal) }}</el-descriptions-item>
        <el-descriptions-item label="累计发布">
          {{ formatNumber(exchange?.message_stats?.publish, 0) }}
        </el-descriptions-item>
        <el-descriptions-item label="策略">{{ exchange?.policy ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="参数（arguments）">
          <span class="mono">{{ formatJson(exchange?.arguments ?? {}) }}</span>
        </el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>发布测试消息</template>
      <el-form label-width="110px">
        <el-row :gutter="16">
          <el-col :xs="24" :md="8">
            <el-form-item label="路由键">
              <el-input v-model="publishForm.routing_key" placeholder="routing key" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item label="内容类型">
              <el-input v-model="publishForm.content_type" placeholder="text/plain" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item label="投递模式">
              <el-select v-model="publishForm.delivery_mode" style="width: 100%">
                <el-option :value="1" label="1 - 非持久化" />
                <el-option :value="2" label="2 - 持久化" />
              </el-select>
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item label="Headers（JSON）">
          <el-input v-model="publishForm.headersText" placeholder="{}" />
        </el-form-item>
        <el-form-item label="消息内容">
          <el-input v-model="publishForm.payload" type="textarea" :rows="4" placeholder="消息 payload" />
        </el-form-item>
        <el-form-item label="mandatory">
          <el-checkbox v-model="publishForm.mandatory">无可路由队列时返回未路由提示</el-checkbox>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :icon="Promotion" :loading="publishing" @click="publishMessage">
            发布消息
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card shadow="never">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>源绑定（bindings/source）</span>
          <el-button type="primary" size="small" :icon="Plus" @click="openBinding">添加绑定</el-button>
        </div>
      </template>
      <el-table :data="bindings" stripe>
        <el-table-column label="目标类型" width="110">
          <template #default="{ row }">{{ row.destination_type === 'queue' ? '队列' : '交换机' }}</template>
        </el-table-column>
        <el-table-column label="目标" prop="destination" min-width="180" />
        <el-table-column label="路由键" prop="routing_key" min-width="140" />
        <el-table-column label="properties_key" prop="properties_key" min-width="140" />
        <el-table-column label="参数" min-width="160">
          <template #default="{ row }">
            <span class="mono">{{ formatJson(row.arguments) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{ row }">
            <el-button link type="danger" @click="unbindBinding(row)">解绑</el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty description="该交换机没有源绑定" :image-size="80" />
        </template>
      </el-table>
    </el-card>

    <!-- 添加绑定：把本交换机绑定到某个队列 / 交换机 -->
    <el-dialog v-model="bindingVisible" title="添加绑定" width="600px">
      <el-form label-width="110px">
        <el-form-item label="目标类型">
          <el-radio-group v-model="bindingForm.destinationType">
            <el-radio value="queue">队列</el-radio>
            <el-radio value="exchange">交换机</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="目标名称">
          <el-select
            v-if="bindingForm.destinationType === 'queue'"
            v-model="bindingForm.destination"
            filterable
            placeholder="请选择目标队列"
            style="width: 100%"
          >
            <el-option v-for="queue in queues" :key="queue.name" :label="queue.name" :value="queue.name" />
          </el-select>
          <el-select v-else v-model="bindingForm.destination" filterable placeholder="请选择目标交换机" style="width: 100%">
            <el-option
              v-for="ex in exchanges"
              :key="ex.name || 'amq.default'"
              :label="ex.name || 'amq.default'"
              :value="ex.name || 'amq.default'"
              :disabled="ex.name === ''"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="路由键">
          <el-input v-model="bindingForm.routingKey" placeholder="fanout 交换机可留空" />
        </el-form-item>
        <el-form-item label="可选参数">
          <BindingArgsEditor v-model="bindingForm.args" />
        </el-form-item>
      </el-form>
      <div class="page-subtitle">默认交换机（amq.default）不能作为目标；其余交换机与队列均可被绑定。</div>
      <template #footer>
        <el-button @click="bindingVisible = false">取消</el-button>
        <el-button type="primary" :loading="bindingSubmitting" @click="submitBinding">绑定</el-button>
      </template>
    </el-dialog>
  </div>
</template>
