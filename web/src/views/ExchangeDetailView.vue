<script setup lang="ts">
// 交换机详情：基本信息 + source 绑定 + 发布测试消息
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft, Promotion, Refresh } from '@element-plus/icons-vue'
import { api } from '@/api'
import type { Binding, Exchange } from '@/api/types'
import { formatBoolean, formatJson, formatNumber } from '@/utils/format'
import { showError } from '@/utils/message'

const route = useRoute()
const router = useRouter()

const vhost = computed<string>(() => String(route.params.vhost ?? '/'))
const exchangeName = computed<string>(() => String(route.params.name ?? ''))

const exchange = ref<Exchange | null>(null)
const bindings = ref<Binding[]>([])
const loading = ref(false)

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
    const [exchangeData, bindingList] = await Promise.all([
      api.exchange(vhost.value, exchangeName.value),
      api.exchangeSourceBindings(vhost.value, exchangeName.value),
    ])
    exchange.value = exchangeData
    bindings.value = bindingList
  } catch (error) {
    showError(error, '加载交换机详情失败')
  } finally {
    loading.value = false
  }
}

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

onMounted(() => {
  void load()
})

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
      <el-button :icon="Refresh" @click="load">刷新</el-button>
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
      <template #header>源绑定（bindings/source）</template>
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
        <template #empty>
          <el-empty description="该交换机没有源绑定" :image-size="80" />
        </template>
      </el-table>
    </el-card>
  </div>
</template>
