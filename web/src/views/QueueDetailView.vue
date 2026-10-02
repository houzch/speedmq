<script setup lang="ts">
// 队列详情：基本信息 + 绑定 + 消费者 + 发布测试消息 + 取消息（get）+ 清空/删除
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
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

const ackModeOptions: { value: AckMode; label: string; hint: string }[] = [
  {
    value: 'ack_requeue_true',
    label: 'ack + 重新入队',
    hint: '取出后立即确认并重新入队，队列内容不变（只读查看，推荐）',
  },
  { value: 'ack_requeue_false', label: 'ack + 移出队列', hint: '取出后确认并从队列删除该消息' },
  { value: 'reject_requeue_true', label: 'reject + 重新入队', hint: '拒绝消息并重新入队，队列内容不变' },
  { value: 'reject_requeue_false', label: 'reject + 丢弃', hint: '拒绝消息并丢弃，不会进入死信队列' },
]

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

const currentAckHint = computed(() => ackModeOptions.find((item) => item.value === getForm.ackmode)?.hint ?? '')

const PROPERTY_LABELS: Record<string, string> = {
  content_type: '内容类型',
  delivery_mode: '投递模式',
  headers: 'Headers',
  priority: '优先级',
  correlation_id: '关联 ID',
  reply_to: '回复地址',
  expiration: '过期时间',
  message_id: '消息 ID',
  timestamp: '时间戳',
  type: '类型',
  user_id: '用户 ID',
  app_id: '应用 ID',
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
    label: PROPERTY_LABELS[key] ?? key,
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
    showError(error, '加载队列详情失败')
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
  bindingForm.value = { source: '', routingKey: '', args: [] }
  bindingVisible.value = true
}

/** 建立「交换机 → 本队列」绑定：POST /api/bindings/{vhost}/e/{source}/q/{destination} */
async function submitBinding(): Promise<void> {
  const source = bindingForm.value.source.trim()
  if (!source) {
    ElMessage.warning('请选择来源交换机')
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
 * 解绑：DELETE /api/bindings/{vhost}/e/{source}/q/{destination}/{properties_key}
 *
 * properties_key 直接用列表返回值，避免自行拼接出错（空 routing key 时为 `~`）。
 */
async function unbindBinding(rawRow: Record<PropertyKey, unknown>): Promise<void> {
  const source = String(rawRow.source ?? '')
  const props = String(rawRow.properties_key ?? '')
  const routingKey = String(rawRow.routing_key ?? '')
  const sourceLabel = source || '(default)'
  try {
    await ElMessageBox.confirm(
      `确定要删除交换机「${sourceLabel}」到本队列的绑定（路由键「${routingKey || '~'}」）吗？`,
      '解绑',
      { type: 'warning', confirmButtonText: '解绑', cancelButtonText: '取消' },
    )
  } catch {
    return
  }
  try {
    await api.unbindQueue(vhost.value, source, queueName.value, props)
    ElMessage.success('绑定已删除')
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
      ElMessage.error('Headers 必须是合法的 JSON 对象')
      return
    }
  }
  if (!publishForm.exchange.trim()) {
    ElMessage.error('请填写目标交换机')
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
      ElMessage.success('消息已成功路由到队列')
    } else {
      ElMessage.warning('消息已发布，但未路由到任何队列')
    }
    await load()
  } catch (error) {
    showError(error, '发布消息失败')
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
      ElMessage.info('队列中没有可取出的消息')
    } else {
      await load()
    }
  } catch (error) {
    showError(error, '获取消息失败')
  } finally {
    getting.value = false
  }
}

async function purgeQueue(): Promise<void> {
  try {
    await ElMessageBox.confirm(`确定要清空队列「${queueName.value}」中的全部消息吗？该操作不可撤销。`, '清空队列', {
      type: 'warning',
      confirmButtonText: '清空',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await api.purgeQueue(vhost.value, queueName.value)
    ElMessage.success('队列已清空')
    await load()
  } catch (error) {
    showError(error, '清空队列失败')
  }
}

async function deleteQueue(): Promise<void> {
  try {
    await ElMessageBox.confirm(`确定要删除队列「${queueName.value}」吗？该操作不可撤销。`, '删除队列', {
      type: 'warning',
      confirmButtonText: '删除',
      cancelButtonText: '取消',
    })
  } catch {
    return
  }
  try {
    await api.deleteQueue(vhost.value, queueName.value)
    ElMessage.success('队列已删除')
    await router.push({ name: 'queues', params: { vhost: vhost.value } })
  } catch (error) {
    showError(error, '删除队列失败')
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
        <el-button link :icon="ArrowLeft" @click="goBack">返回队列列表</el-button>
        <h2 class="page-title">队列详情：{{ queueName }}</h2>
        <div class="page-subtitle">虚拟主机：{{ vhost }}</div>
      </div>
      <div class="toolbar" style="margin-bottom: 0">
        <el-button :icon="Refresh" @click="load">刷新</el-button>
        <el-button type="warning" :icon="Delete" @click="purgeQueue">清空消息</el-button>
        <el-button type="danger" :icon="Delete" @click="deleteQueue">删除队列</el-button>
      </div>
    </div>

    <el-card class="section-card" shadow="never">
      <template #header>基本信息</template>
      <el-descriptions :column="3" border>
        <el-descriptions-item label="名称">{{ queue?.name ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="虚拟主机">{{ queue?.vhost ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="类型">{{ queue?.type ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="状态">
          <el-tag :type="queue?.state === 'running' ? 'success' : 'danger'" size="small">
            {{ queue?.state ?? '—' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="所在节点">{{ queue?.node ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="持久化">{{ formatBoolean(queue?.durable) }}</el-descriptions-item>
        <el-descriptions-item label="自动删除">{{ formatBoolean(queue?.auto_delete) }}</el-descriptions-item>
        <el-descriptions-item label="排他">{{ formatBoolean(queue?.exclusive) }}</el-descriptions-item>
        <el-descriptions-item label="消费者数">{{ formatNumber(queue?.consumers, 0) }}</el-descriptions-item>
        <el-descriptions-item label="消息总数">{{ formatNumber(queue?.messages, 0) }}</el-descriptions-item>
        <el-descriptions-item label="就绪消息">{{ formatNumber(queue?.messages_ready, 0) }}</el-descriptions-item>
        <el-descriptions-item label="未确认消息">
          {{ formatNumber(queue?.messages_unacknowledged, 0) }}
        </el-descriptions-item>
        <el-descriptions-item label="入队速率">{{ formatRate(queue?.messages_details) }}</el-descriptions-item>
        <el-descriptions-item label="就绪速率">{{ formatRate(queue?.messages_ready_details) }}</el-descriptions-item>
        <el-descriptions-item label="未确认速率">
          {{ formatRate(queue?.messages_unacknowledged_details) }}
        </el-descriptions-item>
        <el-descriptions-item label="占用内存">{{ formatBytes(queue?.memory) }}</el-descriptions-item>
        <el-descriptions-item label="空闲起始">{{ queue?.idle_since ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="策略">{{ queue?.policy ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="排他消费者">{{ queue?.exclusive_consumer_tag ?? '—' }}</el-descriptions-item>
        <el-descriptions-item label="reductions">{{ formatNumber(queue?.reductions, 0) }}</el-descriptions-item>
        <el-descriptions-item label="参数（arguments）">
          <span class="mono">{{ formatJson(queue?.arguments ?? {}) }}</span>
        </el-descriptions-item>
      </el-descriptions>
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>发布测试消息</template>
      <el-form label-width="110px">
        <el-row :gutter="16">
          <el-col :xs="24" :md="8">
            <el-form-item label="目标交换机">
              <el-select
                v-model="publishForm.exchange"
                filterable
                allow-create
                default-first-option
                placeholder="amq.default"
                style="width: 100%"
              >
                <el-option v-for="name in builtinExchanges" :key="name" :label="name" :value="name" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item label="路由键">
              <el-input v-model="publishForm.routing_key" placeholder="默认使用队列名" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="8">
            <el-form-item label="内容类型">
              <el-input v-model="publishForm.content_type" placeholder="text/plain" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="16">
          <el-col :xs="24" :md="8">
            <el-form-item label="投递模式">
              <el-select v-model="publishForm.delivery_mode" style="width: 100%">
                <el-option :value="1" label="1 - 非持久化" />
                <el-option :value="2" label="2 - 持久化" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="16">
            <el-form-item label="Headers（JSON）">
              <el-input v-model="publishForm.headersText" placeholder="{}" />
            </el-form-item>
          </el-col>
        </el-row>
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

    <el-card class="section-card" shadow="never">
      <template #header>取消息（Get messages）</template>
      <el-form label-width="110px">
        <el-row :gutter="16">
          <el-col :xs="24" :md="6">
            <el-form-item label="数量">
              <el-input-number v-model="getForm.count" :min="1" :max="500" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="6">
            <el-form-item label="截断长度">
              <el-input-number v-model="getForm.truncate" :min="1" :max="1000000" style="width: 100%" />
            </el-form-item>
          </el-col>
          <el-col :xs="24" :md="12">
            <el-form-item label="编码">
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
            <el-button type="primary" :icon="Search" :loading="getting" @click="loadMessages">取消息</el-button>
            <div class="page-subtitle" style="margin-top: 6px">{{ currentAckHint }}</div>
          </div>
        </el-form-item>
      </el-form>

      <el-alert
        v-if="fetched && messages.length > 0"
        type="info"
        :closable="false"
        show-icon
        :title="`本次取出 ${messages.length} 条消息（服务端剩余 message_count 见下表首列）`"
        style="margin-bottom: 12px"
      />

      <el-table v-if="messages.length > 0" :data="messages" border>
        <el-table-column label="序号" type="index" width="70" />
        <el-table-column label="路由键" prop="routing_key" min-width="120" />
        <el-table-column label="交换机" width="130">
          <template #default="{ row }">{{ row.exchange || '(default)' }}</template>
        </el-table-column>
        <el-table-column label="大小" width="90" align="right">
          <template #default="{ row }">{{ formatNumber(row.payload_bytes, 0) }} B</template>
        </el-table-column>
        <el-table-column label="重投递" width="90">
          <template #default="{ row }">{{ formatBoolean(row.redelivered) }}</template>
        </el-table-column>
        <el-table-column label="剩余消息" width="100" align="right">
          <template #default="{ row }">{{ formatNumber(row.message_count, 0) }}</template>
        </el-table-column>
        <el-table-column label="内容预览" min-width="240">
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
              时间戳：{{ formatTimestamp(row.properties?.timestamp) }} ｜ payload_encoding：{{ row.payload_encoding }}
            </div>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-else-if="fetched" description="没有取到消息" :image-size="80" />
    </el-card>

    <el-card class="section-card" shadow="never">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>绑定（Bindings）</span>
          <el-button type="primary" size="small" :icon="Plus" @click="openBinding">添加绑定</el-button>
        </div>
      </template>
      <el-table :data="bindings" stripe>
        <el-table-column label="源（source）" min-width="160">
          <template #default="{ row }">{{ row.source || 'amq.default' }}</template>
        </el-table-column>
        <el-table-column label="目标类型" width="110">
          <template #default="{ row }">{{ row.destination_type === 'queue' ? '队列' : '交换机' }}</template>
        </el-table-column>
        <el-table-column label="目标" prop="destination" min-width="160" />
        <el-table-column label="路由键" prop="routing_key" min-width="140" />
        <el-table-column label="properties_key" prop="properties_key" min-width="140" />
        <el-table-column label="参数" min-width="160">
          <template #default="{ row }">
            <span class="mono">{{ formatJson(row.arguments) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="100" fixed="right">
          <template #default="{ row }">
            <el-button v-if="row.source !== ''" link type="danger" @click="unbindBinding(row)">解绑</el-button>
            <span v-else class="page-subtitle" title="默认交换机的隐式绑定，随队列存在">—</span>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty description="该队列没有任何绑定" :image-size="80" />
        </template>
      </el-table>
    </el-card>

    <!-- 添加绑定：把某个交换机绑定到本队列 -->
    <el-dialog v-model="bindingVisible" title="添加绑定" width="600px">
      <el-form label-width="110px">
        <el-form-item label="来源交换机">
          <el-select v-model="bindingForm.source" filterable placeholder="请选择来源交换机" style="width: 100%">
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
      <div class="page-subtitle">
        默认交换机（amq.default）的绑定是隐式的，不能手工建立；其余交换机均可作为来源。
      </div>
      <template #footer>
        <el-button @click="bindingVisible = false">取消</el-button>
        <el-button type="primary" :loading="bindingSubmitting" @click="submitBinding">绑定</el-button>
      </template>
    </el-dialog>

    <el-card shadow="never">
      <template #header>消费者（Consumers）</template>
      <el-table :data="consumers" stripe>
        <el-table-column label="消费者标签" prop="consumer_tag" min-width="200" />
        <el-table-column label="队列" min-width="140">
          <template #default="{ row }">{{ row.queue.name }}</template>
        </el-table-column>
        <el-table-column label="需要确认" width="110">
          <template #default="{ row }">{{ formatBoolean(row.ack_required) }}</template>
        </el-table-column>
        <el-table-column label="prefetch" width="100" align="right">
          <template #default="{ row }">{{ formatNumber(row.prefetch_count, 0) }}</template>
        </el-table-column>
        <el-table-column label="排他" width="90">
          <template #default="{ row }">{{ formatBoolean(row.exclusive) }}</template>
        </el-table-column>
        <el-table-column label="参数" min-width="160">
          <template #default="{ row }">
            <span class="mono">{{ formatJson(row.arguments) }}</span>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty description="该队列当前没有消费者" :image-size="80" />
        </template>
      </el-table>
    </el-card>
  </div>
</template>
