<script setup lang="ts">
// 强制改密对话框：首次登录（must_change_password）时必须先修改账号名与口令。
//
// 与登录框一样不可通过遮罩/Esc/关闭按钮关闭 —— 在改密成功之前不放行任何业务页面。
import { ref, watch } from 'vue'
import { ApiError } from '@/api/client'
import { api } from '@/api'
import { useAuthStore } from '@/stores/auth'

const auth = useAuthStore()

// 本组件由父级 v-if 控制挂载，弹窗自身始终保持打开
const visible = ref(true)
const form = ref({ name: '', password: '', confirm: '' })
const submitting = ref(false)
const errorText = ref('')

// 默认用当前账号名填充新账号名（允许修改）
watch(
  () => auth.user,
  (name) => {
    if (name !== null && form.value.name === '') form.value.name = name
  },
  { immediate: true },
)

async function submit(): Promise<void> {
  const name = form.value.name.trim()
  if (!name) {
    errorText.value = '请输入新的账号名'
    return
  }
  if (!form.value.password) {
    errorText.value = '请输入新口令'
    return
  }
  if (form.value.password !== form.value.confirm) {
    errorText.value = '两次输入的口令不一致'
    return
  }
  const currentName = auth.user
  if (currentName === null) {
    errorText.value = '当前登录状态异常，请重新登录'
    return
  }
  errorText.value = ''
  submitting.value = true
  try {
    await api.changeCredentials(currentName, { name, password: form.value.password })
    // 旧凭据已失效，用新账号名/口令刷新本地凭据与认证状态
    await auth.applyCredentials(name, form.value.password)
    ElMessage.success('账号名与口令已更新')
  } catch (error) {
    errorText.value = error instanceof ApiError ? error.reason || error.message : '修改失败，请稍后重试'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <el-dialog
    v-model="visible"
    title="首次登录：请修改账号名与口令"
    width="460px"
    align-center
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    :show-close="false"
  >
    <el-alert
      class="login-error"
      type="warning"
      :closable="false"
      show-icon
      title="总管理员首次登录必须修改账号名与口令"
      description="为安全起见，默认的 guest/guest 必须替换为你自己的账号名与口令后才能使用管理后台。"
    />
    <el-alert
      v-if="errorText"
      class="login-error"
      :title="errorText"
      type="error"
      :closable="false"
      show-icon
    />
    <el-form label-width="88px" @submit.prevent>
      <el-form-item label="新账号名">
        <el-input v-model="form.name" placeholder="请输入新的账号名" autocomplete="username" />
      </el-form-item>
      <el-form-item label="新口令">
        <el-input
          v-model="form.password"
          type="password"
          show-password
          placeholder="请输入新口令"
          autocomplete="new-password"
        />
      </el-form-item>
      <el-form-item label="确认口令">
        <el-input
          v-model="form.confirm"
          type="password"
          show-password
          placeholder="请再次输入新口令"
          autocomplete="new-password"
          @keyup.enter="submit"
        />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button type="primary" :loading="submitting" @click="submit">提交修改</el-button>
    </template>
  </el-dialog>
</template>
