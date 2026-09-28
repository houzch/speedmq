<script setup lang="ts">
// 登录框：默认 guest/guest，密码框回车提交；不可通过遮罩/Esc 关闭
import { ref, watch } from 'vue'
import { ApiError } from '@/api/client'
import { useAuthStore } from '@/stores/auth'

const visible = defineModel<boolean>({ required: true })
const auth = useAuthStore()

const form = ref({ user: 'guest', password: 'guest' })
const submitting = ref(false)
const errorText = ref('')

watch(visible, (open) => {
  if (open) errorText.value = ''
})

async function submit(): Promise<void> {
  if (!form.value.user.trim()) {
    errorText.value = '请输入用户名'
    return
  }
  errorText.value = ''
  submitting.value = true
  try {
    await auth.login(form.value.user, form.value.password)
    visible.value = false
  } catch (error) {
    errorText.value = error instanceof ApiError ? error.reason || error.message : '登录失败，请稍后重试'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <el-dialog
    v-model="visible"
    title="登录 SwiftMQ 管理后台"
    width="420px"
    align-center
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    :show-close="false"
  >
    <el-alert v-if="errorText" class="login-error" :title="errorText" type="error" :closable="false" show-icon />
    <el-form label-width="72px" @submit.prevent>
      <el-form-item label="用户名">
        <el-input v-model="form.user" placeholder="guest" autocomplete="username" />
      </el-form-item>
      <el-form-item label="密码">
        <el-input
          v-model="form.password"
          type="password"
          show-password
          placeholder="guest"
          autocomplete="current-password"
          @keyup.enter="submit"
        />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button type="primary" :loading="submitting" @click="submit">登录</el-button>
    </template>
  </el-dialog>
</template>
