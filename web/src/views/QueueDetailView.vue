<script setup lang="ts">
// 队列详情：基本信息 + 绑定 + 消费者 + 发布测试消息 + 取消息（get）+ 清空/删除
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { ArrowLeft, Delete, Plus, Promotion, Refresh, Search } from '@element-plus/icons-vue'
import { api } from '@/api'
import { ApiError } from '@/api/client'
import type {
  AckMode,
  Binding,
  Consumer,
  Exchange,
  GetMessage,
  GetMessagesRequest,
  MessageProperties,
  Queue,
} from '@/api/types'
import BindingArgsEditor, { buildArguments, type ArgRow } from '@/components/BindingArgsEditor.vue'
import { useAutoRefresh } from '@/composables/useAutoRefresh'
import { useRefreshStore } from '@/stores/refresh'
import { formatBoolean, formatBytes, formatJson, formatNumber, formatRate, formatTimestamp } from '@/utils/format'
import { showError } from '@/utils/message'

const route = useRoute()
const router = useRouter()
const { t } = useI18n()

const refresh = useRefreshStore()

const vhost = computed<string>(() => String(route.params.vhost ?? '/'))
const queueName = computed<string>(() => String(route.params.name ?? ''))

const queue = ref<Queue | null>(null)
const bindings = ref<Binding[]>([])
const consumers = ref<Consumer[]>([])
const exchanges = ref<Exchange[]>([])
const loading = ref(false)

/** 添加绑定的表单（来源交换机 + 路由键 + 可选参数） */
const bindingVisible = ref(false)
const bindingSubmitting = ref(false)
const bindingForm = ref({
  source: '',
  routingKey: '',
  args: [] as ArgRow[],
})

/** 常用的内置交换机，允许自行输入 */
const builtinExchanges = ['amq.default', 'amq.direct', 'amq.fanout', 'amq.topic', 'amq.headers']

/** ackmode 选项：值与说明随语言切换 */
const ackModeOptions = computed<{ value: AckMode; label: string; hint: string }[]>(() => [
  {
    value: 'ack_requeue_true',
    label: t('queueDetail.ackAckRequeueTrue'),
    hint: t('queueDetail.ackAckRequeueTrueHint'),
  },
  { value: 'ack_requeue_false', label: t('queueDetail.ackAckRequeueFalse'), hint: t('queueDetail.ackAckRequeueFalseHint') },
  {
    value: 'reject_requeue_true',
    label: t('queueDetail.ackRejectRequeueTrue'),
    hint: t('queueDetail.ackRejectRequeueTrueHint'),
  },
  { value: 'reject_requeue_false', label: t('queueDetail.ackRejectRequeueFalse'), hint: t('queueDetail.ackRejectRequeueFalseHint') },
])

const publishForm = reactive({
  exchange: 'amq.default',
  routing_key: '',
  content_type: 'text/plain',
  delivery_mode: 2,
  headersText: '{}',
  payload: '{"hello":"swiftmq"}',
  mandatory: true,
})
const publishing = ref(false)

const getForm = reactive<GetMessagesRequest>({
  count: 10,
  ackmode: 'ack_requeue_true',
  encoding: 'auto',
  truncate: 50000,
})
const messages = ref<GetMessage[]>([])
const getting = ref(false)
const fetched = ref(false)

const currentAckHint = computed(() => ackModeOptions.value.find((item) => item.value === getForm.ackmode)?.hint ?? '')

/** 消息属性键 → 本地化名称 */
function propertyLabel(key: string): string {
  switch (key) {
    case 'content_type':
      return t('queueDetail.propertyContentType')
    case 'delivery_mode':
      return t('queueDetail.propertyDeliveryMode')
    case 'headers':
      return t('queueDetail.propertyHeaders')
    case 'priority':
      return t('queueDetail.propertyPriority')
    case 'correlation_id':
      return t('queueDetail.propertyCorrelationId')
    case 'reply_to':
      return t('queueDetail.propertyReplyTo')
    case 'expiration':
      return t('queueDetail.propertyExpiration')
    case 'message_id':
      return t('queueDetail.propertyMessageId')
    case 'timestamp':
      return t('queueDetail.propertyTimestamp')
    case 'type':
      return t('queueDetail.propertyType')
    case 'user_id':
      return t('queueDetail.propertyUserId')
    case 'app_id':
      return t('queueDetail.propertyAppId')
    default:
      return key
  }
}

interface PropertyEntry {
  key: string
  label: string
  value: string
}

/** 消息属性转成可渲染的键值对 */
function propertyEntries(properties: MessageProperties | undefined): PropertyEntry[] {
  if (!properties) return []
  return Object.entries(properties).map(([key, raw]) => ({
    key,
    label: propertyLabel(key),
    value:
      typeof raw === 'object' && raw !== null
        ? formatJson(raw)
        : typeof raw === 'string'
          ? raw || '—'
          : String(raw),
  }))
}

/** 重置与当前队列相关的表单默认值 */
function resetForms(): void {
  publishForm.exchange = 'amq.default'
  publishForm.routing_key = queueName.value
  messages.value = []
  fetched.value = false
}

async function load(): Promise<void> {
  loading.value = true
  try {
    const [queueData, bindingList, consumerList, exchangeList] = await Promise.all([
      api.queue(vhost.value, queueName.value),
      api.queueBindings(vhost.value, queueName.value),
      api.consumers(vhost.value),
      api.exchanges({ vhost: vhost.value }),
    ])
    queue.value = queueData
    bindings.value = bindingList
    consumers.value = consumerList.filter(
      (item) => item.queue.name === queueName.value && item.queue.vhost === vhost.value,
    )
    exchanges.value = exchangeList
    refresh.markRefreshed()
  } catch (error) {
    showError(error, t('queueDetail.loadFailed'))
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
  bindingForm.value = { source: '', routingKey: '', args: [] }
  bindingVisible.value = true
}

/** 建立「交换机 → 本队列」绑定：POST /api/bindings/{vhost}/e/{source}/q/{destination} */
async function submitBinding(): Promise<void> {
  const source = bindingForm.value.source.trim()
  if (!source) {
    ElMessage.warning(t('queueDetail.sourceRequired'))
    return
  }
  const built = buildArguments(bindingForm.value.args)
  if (!built.ok) {
    ElMessage.warning(built.error)
    return
  }
  bindingSubmitting.value = true
  try {
    await api.bindQueue(vhost.value, source, queueName.value, {
      routing_key: bindingForm.value.routingKey,
      arguments: built.arguments,
    })
    ElMessage.success(t('queueDetail.bound'))
    bindingVisible.value = false
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  } finally {
    bindingSubmitting.value = false
  }
}

/**
 * 解绑：DELETE /api/bindings/{vhost}/e/{source}/q/{destination}/{properties_key}
 *
 * properties_key 直接用列表返回值，避免自行拼接出错（空 routing key 时为 `~`）。
 */
async function unbindBinding(rawRow: Record<PropertyKey, unknown>): Promise<void> {
  const source = String(rawRow.source ?? '')
  const props = String(rawRow.properties_key ?? '')
  const routingKey = String(rawRow.routing_key ?? '')
  const sourceLabel = source || 'amq.default'
  try {
    await ElMessageBox.confirm(
      t('queueDetail.unbindConfirm', { source: sourceLabel, routingKey: routingKey || '~' }),
      t('queueDetail.unbindTitle'),
      { type: 'warning', confirmButtonText: t('queueDetail.unbindButton'), cancelButtonText: t('common.cancel') },
    )
  } catch {
    return
  }
  try {
    await api.unbindQueue(vhost.value, source, queueName.value, props)
    ElMessage.success(t('queueDetail.unbound'))
    await load()
  } catch (error) {
    ElMessage.error(messageOf(error))
  }
}

/** 发布测试消息到默认交换机（或指定交换机） */
async function publishMessage(): Promise<void> {
  let headers: Record<string, unknown> = {}
  if (publishForm.headersText.trim()) {
    try {
      headers = JSON.parse(publishForm.headersText) as Record<string, unknown>
    } catch {
      ElMessage.error(t('queueDetail.headersInvalid'))
      return
    }
  }
  if (!publishForm.exchange.trim()) {
    ElMessage.error(t('queueDetail.exchangeRequired'))
    return
  }

  publishing.value = true
  try {
    const result = await api.publish(vhost.value, publishForm.exchange.trim(), {
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
      ElMessage.success(t('queueDetail.publishRouted'))
    } else {
      ElMessage.warning(t('queueDetail.publishUnrouted'))
    }
    await load()
  } catch (error) {
    showError(error, t('queueDetail.publishFailed'))
  } finally {
    publishing.value = false
  }
}

/** 从队列取消息（仅用于查看） */
async function loadMessages(): Promise<void> {
  getting.value = true
  try {
    messages.value = await api.getMessages(vhost.value, queueName.value, { ...getForm })
    fetched.value = true
    if (messages.value.length === 0) {
      ElMessage.info(t('queueDetail.noMessagesInQueue'))
    } else {
      await load()
    }
  } catch (error) {
    showError(error, t('queueDetail.getFailed'))
  } finally {
    getting.value = false
  }
}

async function purgeQueue(): Promise<void> {
  try {
    await ElMessageBox.confirm(t('queues.purgeConfirm', { name: queueName.value }), t('queues.purgeTitle'), {
      type: 'warning',
      confirmButtonText: t('queues.purgeButton'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.purgeQueue(vhost.value, queueName.value)
    ElMessage.success(t('queues.purged'))
    await load()
  } catch (error) {
    showError(error, t('queues.purgeFailed'))
  }
}

async function deleteQueue(): Promise<void> {
  try {
    await ElMessageBox.confirm(t('queues.deleteConfirm', { name: queueName.value }), t('queues.deleteTitle'), {
      type: 'warning',
      confirmButtonText: t('common.delete'),
      cancelButtonText: t('common.cancel'),
    })
  } catch {
    return
  }
  try {
    await api.deleteQueue(vhost.value, queueName.value)
    ElMessage.success(t('queues.deleted'))
    await router.push({ name: 'queues', params: { vhost: vhost.value } })
  } catch (error) {
    showError(error, t('queues.deleteFailed'))
  }
}

function goBack(): void {
  void router.push({ name: 'queues', params: { vhost: vhost.value } })
}

onMounted(() => {
  resetForms()
})

useAutoRefresh(load)

watch([vhost, queueName], () => {
  resetForms()
  void load()
})
</script>

<template>
  <div v-loading="loading">
    <div class="page-header">
      <div>
        <el-button link :icon="ArrowLeft" @click="goBack">{{ t('queueDetail.back') }}</el-button>
        <h2 class="page-title">{{ t('queueDetail.title', { name: queueName }) }}</h2>
        <div class="page-subtitle">{{ t('common.vhostSubtitle', { vhost }) }}</div>
      </div>
      <div class="toolbar" style="margin-bottom: 0">
        <el-button :icon="Refresh" @click="load">{{ t('common.refresh') }}</el-button>
        <el-button type="warning" :icon="Delete" @click="purgeQueue">{{ t('queueDetail.purgeButton') }}</el-button>
        <el-button type="danger" :icon="Delete" @click="deleteQueue">{{ t('queueDetail.deleteButton') }}</el-button>
      </div>
    </div>

    <el-card class="section-card" shadow="never">
      <template #header>{{ t('queueDetail.basicInfo') }}</template>
      <el-descriptions :column="3" border>
        <el-descriptions-item :label="t('common.name')">{{ queue?.name ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.vhost')">{{ queue?.vhost ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.type')">{{ queue?.type ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.status')">
          <el-tag :type="queue?.state === 'running' ? 'success' : 'danger'" size="small">
            {{ queue?.state ?? '—' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.node')">{{ queue?.node ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.durable')">{{ formatBoolean(queue?.durable) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.autoDelete')">{{ formatBoolean(queue?.auto_delete) }}</el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.exclusive')">{{ formatBoolean(queue?.exclusive) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.consumers')">{{ formatNumber(queue?.consumers, 0) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.messages')">{{ formatNumber(queue?.messages, 0) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.messagesReady')">{{ formatNumber(queue?.messages_ready, 0) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.messagesUnacked')">
          {{ formatNumber(queue?.messages_unacknowledged, 0) }}
        </el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.consumeRate')">{{ formatRate(queue?.messages_details) }}</el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.readyRate')">{{ formatRate(queue?.messages_ready_details) }}</el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.unackedRate')">
          {{ formatRate(queue?.messages_unacknowledged_details) }}
        </el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.memUsed')">{{ formatBytes(queue?.memory) }}</el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.idleSince')">{{ queue?.idle_since ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.policy')">{{ queue?.policy ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.exclusiveConsumer')">{{ queue?.exclusive_consumer_tag ?? '—' }}</el-descriptions-item>
        <el-descriptions-item :label="t('queueDetail.reductions')">{{ formatNumber(queue?.reductions, 0) }}</el-descriptions-item>
        <el-descriptions-item :label="t('common.arguments')">
          <span class="mono">{{ formatJson(queue?.arguments ?? {}) }}</span>
        </el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>{{ t('queueDetail.publishTitle') }}</template>
      <el-form label-width="110px">
        <el-row :gutter="16">
          <el-col :xs="24" :md="8">
            <el-form-item :label="t('queueDetail.targetExchange')">
              <el-select
                v-model="publishForm.exchange"
                filterable
                allow-create
                default-first-option
                :placeholder="t('queueDetail.defaultExchange')"
                style="width: 100%"
              >
                <el-option v-for="name in builtinExchanges" :key="name" :label="name" :value="name" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item :label="t('common.routingKey')">
              <el-input v-model="publishForm.routing_key" :placeholder="t('queueDetail.routingKeyPlaceholder')" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item :label="t('queueDetail.contentType')">
              <el-input v-model="publishForm.content_type" placeholder="text/plain" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="16">
          <el-col :xs="24" :md="8">
            <el-form-item :label="t('queueDetail.deliveryMode')">
              <el-select v-model="publishForm.delivery_mode" style="width: 100%">
                <el-option :value="1" :label="t('queueDetail.deliveryMode1')" />
                <el-option :value="2" :label="t('queueDetail.deliveryMode2')" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="16">
            <el-form-item label="Headers（JSON）">
              <el-input v-model="publishForm.headersText" placeholder="{}" />
            </el-form-item>
          </el-col>
        </el-row>
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

    <el-card class="section-card" shadow="never">
      <template #header>{{ t('queueDetail.getTitle') }}</template>
      <el-form label-width="110px">
        <el-row :gutter="16">
          <el-col :xs="24" :md="6">
            <el-form-item :label="t('queueDetail.count')">
              <el-input-number v-model="getForm.count" :min="1" :max="500" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="6">
            <el-form-item :label="t('queueDetail.truncate')">
              <el-input-number v-model="getForm.truncate" :min="1" :max="1000000" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="12">
            <el-form-item :label="t('queueDetail.encoding')">
              <el-select v-model="getForm.encoding" style="width: 100%">
                <el-option value="auto" label="auto" />
                <el-option value="base64" label="base64" />
              </el-select>
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item label="ackmode">
          <el-radio-group v-model="getForm.ackmode">
            <el-radio-button v-for="option in ackModeOptions" :key="option.value" :value="option.value">
              {{ option.label }}
            </el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item>
          <div>
            <el-button type="primary" :icon="Search" :loading="getting" @click="loadMessages">{{ t('queueDetail.getButton') }}</el-button>
            <div class="page-subtitle" style="margin-top: 6px">{{ currentAckHint }}</div>
          </div>
        </el-form-item>
      </el-form>

      <el-alert
        v-if="fetched && messages.length > 0"
        type="info"
        :closable="false"
        show-icon
        :title="t('queueDetail.gotCount', { count: messages.length })"
        style="margin-bottom: 12px"
      />

      <el-table v-if="messages.length > 0" :data="messages" border>
        <el-table-column :label="t('queueDetail.colIndex')" type="index" width="70" />
        <el-table-column :label="t('queueDetail.colRoutingKey')" prop="routing_key" min-width="120" />
        <el-table-column :label="t('queueDetail.colExchange')" width="130">
          <template #default="{ row }">{{ row.exchange || 'amq.default' }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.colSize')" width="90" align="right">
          <template #default="{ row }">{{ formatNumber(row.payload_bytes, 0) }} B</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.colRedelivered')" width="90">
          <template #default="{ row }">{{ formatBoolean(row.redelivered) }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.colRemaining')" width="100" align="right">
          <template #default="{ row }">{{ formatNumber(row.message_count, 0) }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.colPreview')" min-width="240">
          <template #default="{ row }">
            <span class="mono">{{ row.payload }}</span>
          </template>
        </el-table-column>
        <el-table-column type="expand" width="60">
          <template #default="{ row }">
            <pre class="payload">{{ row.payload }}</pre>
            <el-descriptions :column="2" border style="margin-top: 12px">
              <el-descriptions-item v-for="entry in propertyEntries(row.properties)" :key="entry.key" :label="entry.label">
                <span class="mono">{{ entry.value }}</span>
              </el-descriptions-item>
            </el-descriptions>
            <div class="page-subtitle" style="margin-top: 8px">
              {{ t('queueDetail.timestampLine', { time: formatTimestamp(row.properties?.timestamp), encoding: row.payload_encoding }) }}
            </div>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-else-if="fetched" :description="t('queueDetail.noMessages')" :image-size="80" />
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>{{ t('queueDetail.bindingsTitle') }}</span>
          <el-button type="primary" size="small" :icon="Plus" @click="openBinding">{{ t('common.addBinding') }}</el-button>
        </div>
      </template>
      <el-table :data="bindings" stripe>
        <el-table-column :label="t('queueDetail.colSource')" min-width="160">
          <template #default="{ row }">{{ row.source || 'amq.default' }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.colDestinationType')" width="110">
          <template #default="{ row }">{{ row.destination_type === 'queue' ? t('common.queue') : t('common.exchange') }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.colDestination')" prop="destination" min-width="160" />
        <el-table-column :label="t('common.routingKey')" prop="routing_key" min-width="140" />
        <el-table-column :label="t('queueDetail.colPropertiesKey')" prop="properties_key" min-width="140" />
        <el-table-column :label="t('common.parameters')" min-width="160">
          <template #default="{ row }">
            <span class="mono">{{ formatJson(row.arguments) }}</span>
          </template>
        </el-table-column>
        <el-table-column :label="t('common.actions')" width="100" fixed="right">
          <template #default="{ row }">
            <el-button v-if="row.source !== ''" link type="danger" @click="unbindBinding(row)">{{ t('common.unbind') }}</el-button>
            <span v-else class="page-subtitle" :title="t('queueDetail.defaultBindingHint')">—</span>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty :description="t('queueDetail.noBindings')" :image-size="80" />
        </template>
      </el-table>
    </el-card>

    <!-- 添加绑定：把某个交换机绑定到本队列 -->
    <el-dialog v-model="bindingVisible" :title="t('queueDetail.addBindingTitle')" width="600px">
      <el-form label-width="110px">
        <el-form-item :label="t('common.sourceExchange')">
          <el-select v-model="bindingForm.source" filterable :placeholder="t('queueDetail.sourcePlaceholder')" style="width: 100%">
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
      <div class="page-subtitle">{{ t('queueDetail.bindingNote') }}</div>
      <template #footer>
        <el-button @click="bindingVisible = false">{{ t('common.cancel') }}</el-button>
        <el-button type="primary" :loading="bindingSubmitting" @click="submitBinding">{{ t('common.bind') }}</el-button>
      </template>
    </el-dialog>

    <el-card shadow="never">
      <template #header>{{ t('queueDetail.consumersTitle') }}</template>
      <el-table :data="consumers" stripe>
        <el-table-column :label="t('queueDetail.consumerTag')" prop="consumer_tag" min-width="200" />
        <el-table-column :label="t('common.queue')" min-width="140">
          <template #default="{ row }">{{ row.queue.name }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.ackRequired')" width="110">
          <template #default="{ row }">{{ formatBoolean(row.ack_required) }}</template>
        </el-table-column>
        <el-table-column label="prefetch" width="100" align="right">
          <template #default="{ row }">{{ formatNumber(row.prefetch_count, 0) }}</template>
        </el-table-column>
        <el-table-column :label="t('queueDetail.exclusiveShort')" width="90">
          <template #default="{ row }">{{ formatBoolean(row.exclusive) }}</template>
        </el-table-column>
        <el-table-column :label="t('common.parameters')" min-width="160">
          <template #default="{ row }">
            <span class="mono">{{ formatJson(row.arguments) }}</span>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty :description="t('queueDetail.noConsumers')" :image-size="80" />
        </template>
      </el-table>
    </el-card>
  </div>
</template>
