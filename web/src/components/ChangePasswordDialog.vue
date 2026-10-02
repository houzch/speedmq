<script setup lang="ts">
// 修改口令对话框：由顶栏「当前用户」下拉里的「修改密码」打开。
//
// 与首次强制改密（ForcePasswordDialog）的差别：这里只改自己的口令、不改账号名，
// 而且可以取消 —— 它是一次普通的自助操作，不是登录前的强制关卡。
import { ref, watch } from 'vue'
import { ApiError } from '@/api/client'
import { api } from '@/api'
import { useAuthStore } from '@/stores/auth'

const visible = defineModel<boolean>({ required: true })

const auth = useAuthStore()
const form = ref({ password: '', confirm: '' })
const submitting = ref(false)
const errorText = ref('')

// 每次打开都清空上次的输入，避免残留
watch(visible, (open) => {
  if (open) {
    form.value = { password: '', confirm: '' }
    errorText.value = ''
  }
})

async function submit(): Promise<void> {
  const name = auth.user
  if (name === null) {
    errorText.value = '当前登录状态异常，请重新登录'
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
  errorText.value = ''
  submitting.value = true
  try {
    await api.changeCredentials(name, { password: form.value.password })
    // 服务端在改密的瞬间就让旧口令失效了，必须立刻用新口令刷新本地凭据，
    // 否则下一个请求就会 401、被踢回登录框。
    await auth.applyCredentials(name, form.value.password)
    visible.value = false
    ElMessage.success('口令已修改')
  } catch (error) {
    errorText.value = error instanceof ApiError ? error.reason || error.message : '修改失败，请稍后重试'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <el-dialog v-model="visible" title="修改密码" width="460px" align-center>
    <el-alert v-if="errorText" class="login-error" :title="errorText" type="error" :closable="false" show-icon />
    <el-form label-width="88px" @submit.prevent>
      <el-form-item label="账号名">
        <span class="account-name">{{ auth.user }}</span>
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
      <el-button @click="visible = false">取消</el-button>
      <el-button type="primary" :loading="submitting" @click="submit">确定</el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.account-name {
  color: #606266;
}
</style>
