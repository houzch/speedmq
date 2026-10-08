<script setup lang="ts">
// 交换机详情：基本信息 + source 绑定 + 发布测试消息
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
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
const { t } = useI18n()

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
  payload: '{"hello":"speedmq"}',
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
    showError(error, t('exchangeDetail.loadFailed'))
  } finally {
    loading.value = false
  }
}

/** 写操作错误提示：优先展示服务端原因 */
function messageOf(error: unknown): string {
  if (error instanceof ApiError) return error.reason || error.message
  if (error instanceof Error) return error.message
  return t('common.operationFailed')
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
    ElMessage.warning(t('exchangeDetail.targetRequired'))
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
    ElMessage.success(t('exchangeDetail.bound'))
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
  const typeLabel = destinationType === 'queue' ? t('common.queue') : t('common.exchange')
  try {
    await ElMessageBox.confirm(
      t('exchangeDetail.unbindConfirm', { type: typeLabel, destination, routingKey: routingKey || '~' }),
      t('exchangeDetail.unbindTitle'),
      { type: 'warning', confirmButtonText: t('exchangeDetail.unbindButton'), cancelButtonText: t('common.cancel') },
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
    ElMessage.success(t('exchangeDetail.unbound'))
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
      ElMessage.error(t('exchangeDetail.headersInvalid'))
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
      ElMessage.success(t('exchangeDetail.publishRouted'))
    } else {
      ElMessage.warning(t('exchangeDetail.publishUnrouted'))
    }
  } catch (error) {
    showError(error, t('exchangeDetail.publishFailed'))
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
    await ElMessageBox.confirm(t('exchangeDetail.deleteConfirm', { name: exchangeName.value }), t('exchangeDetail.deleteTitle'), {
      type: 'warning',
      confirmButtonText: t('common.delete'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.deleteExchange(vhost.value, exchangeName.value)
    ElMessage.success(t('exchangeDetail.deleted'))
    await router.push({ name: 'exchanges', params: { vhost: vhost.value } })
  } catch (error) {
    showError(error, t('exchangeDetail.deleteFailed'))
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
        <el-button link :icon="ArrowLeft" @click="goBack">{{ t('exchangeDetail.back') }}</el-button>
        <h2 class="page-title">{{ t('exchangeDetail.title', { name: exchangeName }) }}</h2>
        <div class="page-subtitle">{{ t('common.vhostSubtitle', { vhost }) }}</div>
      </div>
      <div class="toolbar" style="margin-bottom: 0">
        <el-button :icon="Refresh" @click="load">{{ t('common.refresh') }}</el-button>
        <el-button type="danger" :icon="Delete" @click="deleteExchange">{{ t('exchangeDetail.deleteButton') }}</el-button>
      </div>
    </div>

    <el-card class="section-card" shadow="never">
      <template #header>{{ t('exchangeDetail.basicInfo') }}</template>
      <el-descriptions :column="3" border>
        <el-descriptions-item :label="t('common.name')">{{ exchange?.name ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.vhost')">{{ exchange?.vhost ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.type')">{{ exchange?.type ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.durable')">{{ formatBoolean(exchange?.durable) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.autoDelete')">{{ formatBoolean(exchange?.auto_delete) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.internalUse')">{{ formatBoolean(exchange?.internal) }}</el-descriptions-item>
        <el-descriptions-item :label="t('overview.publishTotal')">
          {{ formatNumber(exchange?.message_stats?.publish, 0) }}
        </el-descriptions-item>
        <el-descriptions-item :label="t('common.policy')">{{ exchange?.policy ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.arguments')">
          <span class="mono">{{ formatJson(exchange?.arguments ?? {}) }}</span>
        </el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>{{ t('exchangeDetail.publishTitle') }}</template>
      <el-form label-width="110px">
        <el-row :gutter="16">
          <el-col :xs="24" :md="8">
            <el-form-item :label="t('common.routingKey')">
              <el-input v-model="publishForm.routing_key" :placeholder="t('exchangeDetail.routingKeyPlaceholder')" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item :label="t('queueDetail.contentType')">
              <el-input v-model="publishForm.content_type" placeholder="text/plain" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item :label="t('queueDetail.deliveryMode')">
              <el-select v-model="publishForm.delivery_mode" style="width: 100%">
                <el-option :value="1" :label="t('queueDetail.deliveryMode1')" />
                <el-option :value="2" :label="t('queueDetail.deliveryMode2')" />
              </el-select>
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item label="Headers（JSON）">
          <el-input v-model="publishForm.headersText" placeholder="{}" />
        </el-form-item>
        <el-form-item :label="t('queueDetail.payload')">
          <el-input v-model="publishForm.payload" type="textarea" :rows="4" :placeholder="t('queueDetail.payloadPlaceholder')" />
        </el-form-item>
        <el-form-item label="mandatory">
          <el-checkbox v-model="publishForm.mandatory">{{ t('queueDetail.mandatoryLabel') }}</el-checkbox>
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :icon="Promotion" :loading="publishing" @click="publishMessage">
            {{ t('queueDetail.publishButton') }}
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <el-card shadow="never">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>{{ t('exchangeDetail.sourceBindingsTitle') }}</span>
          <el-button type="primary" size="small" :icon="Plus" @click="openBinding">{{ t('common.addBinding') }}</el-button>
        </div>
      </template>
      <el-table :data="bindings" stripe>
        <el-table-column :label="t('queueDetail.colDestinationType')" width="110">
          <template #default="{ row }">{{ row.destination_type === 'queue' ? t('common.queue') : t('common.exchange') }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.colDestination')" prop="destination" min-width="180" />
        <el-table-column :label="t('common.routingKey')" prop="routing_key" min-width="140" />
        <el-table-column :label="t('queueDetail.colPropertiesKey')" prop="properties_key" min-width="140" />
        <el-table-column :label="t('common.parameters')" min-width="160">
          <template #default="{ row }">
            <span class="mono">{{ formatJson(row.arguments) }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="t('common.actions')" width="100" fixed="right">
          <template #default="{ row }">
            <el-button link type="danger" @click="unbindBinding(row)">{{ t('common.unbind') }}</el-button>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty :description="t('exchangeDetail.noBindings')" :image-size="80" />
        </template>
      </el-table>
    </el-card>

    <!-- 添加绑定：把本交换机绑定到某个队列 / 交换机 -->
    <el-dialog v-model="bindingVisible" :title="t('exchangeDetail.addBindingTitle')" width="600px">
      <el-form label-width="110px">
        <el-form-item :label="t('common.targetType')">
          <el-radio-group v-model="bindingForm.destinationType">
            <el-radio value="queue">{{ t('common.queue') }}</el-radio>
            <el-radio value="exchange">{{ t('common.exchange') }}</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item :label="t('common.target')">
          <el-select
            v-if="bindingForm.destinationType === 'queue'"
            v-model="bindingForm.destination"
            filterable
            :placeholder="t('exchangeDetail.destinationPlaceholderQueue')"
            style="width: 100%"
          >
            <el-option v-for="queue in queues" :key="queue.name" :label="queue.name" :value="queue.name" />
          </el-select>
          <el-select v-else v-model="bindingForm.destination" filterable :placeholder="t('exchangeDetail.destinationPlaceholderExchange')" style="width: 100%">
            <el-option
              v-for="ex in exchanges"
              :key="ex.name || 'amq.default'"
              :label="ex.name || 'amq.default'"
              :value="ex.name || 'amq.default'"
              :disabled="ex.name === ''"
            />
          </el-select>
        </el-form-item>
        <el-form-item :label="t('common.routingKey')">
          <el-input v-model="bindingForm.routingKey" :placeholder="t('queueDetail.bindingRoutingKeyPlaceholder')" />
        </el-form-item>
        <el-form-item :label="t('common.optionalArgs')">
          <BindingArgsEditor v-model="bindingForm.args" />
        </el-form-item>
      </el-form>
      <div class="page-subtitle">{{ t('exchangeDetail.bindingNote') }}</div>
      <template #footer>
        <el-button @click="bindingVisible = false">{{ t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="bindingSubmitting" @click="submitBinding">{{ t('common.bind') }}</el-button>
      </template>
    </el-dialog>
  </div>
</template>
