<script lang="ts">
// 绑定「可选参数（arguments）」的键值对编辑器与转换逻辑，供队列 / 交换机详情复用。
export type ArgValueType = 'string' | 'number' | 'boolean'

/** 一行参数：键 + 值类型 + 值的文本形式 */
export interface ArgRow {
  key: string
  valueType: ArgValueType
  value: string
}

/** 参数转换结果：成功返回 arguments，失败返回提示文案 */
export type BuildArgumentsResult =
  | { ok: true; arguments: Record<string, unknown> }
  | { ok: false; error: string }

/** 把参数行转成 arguments 对象；键为空的行忽略，数字非法时返回失败 */
export function buildArguments(rows: ArgRow[]): BuildArgumentsResult {
  const result: Record<string, unknown> = {}
  for (const row of rows) {
    const key = row.key.trim()
    if (!key) continue
    if (row.valueType === 'number') {
      const num = Number(row.value)
      if (row.value.trim() === '' || Number.isNaN(num)) {
        return { ok: false, error: `参数「${key}」的值必须是数字` }
      }
      result[key] = num
    } else if (row.valueType === 'boolean') {
      result[key] = row.value === 'true'
    } else {
      result[key] = row.value
    }
  }
  return { ok: true, arguments: result }
}
</script>

<script setup lang="ts">
import { Delete, Plus } from '@element-plus/icons-vue'

/** 参数行数组由父组件持有（v-model） */
const rows = defineModel<ArgRow[]>({ required: true })

function addRow(): void {
  rows.value.push({ key: '', valueType: 'string', value: '' })
}

function removeRow(index: number): void {
  rows.value.splice(index, 1)
}
</script>

<template>
  <div style="width: 100%">
    <div v-for="(row, index) in rows" :key="index" class="arg-row">
      <el-input v-model="row.key" placeholder="键" style="flex: 1" />
      <el-select v-model="row.valueType" style="width: 96px">
        <el-option value="string" label="字符串" />
        <el-option value="number" label="数字" />
        <el-option value="boolean" label="布尔" />
      </el-select>
      <el-select v-if="row.valueType === 'boolean'" v-model="row.value" style="width: 110px">
        <el-option value="true" label="true" />
        <el-option value="false" label="false" />
      </el-select>
      <el-input v-else v-model="row.value" placeholder="值" style="flex: 1" />
      <el-button link type="danger" :icon="Delete" @click="removeRow(index)" />
    </div>
    <el-button link type="primary" :icon="Plus" @click="addRow">添加参数</el-button>
  </div>
</template>

<style scoped>
.arg-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
</style>
