<script setup lang="ts">
// 修改口令对话框：由顶栏「当前用户」下拉里的「修改密码」打开。
//
// 与首次强制改密（ForcePasswordDialog）的差别：这里只改自己的口令、不改账号名，
// 而且可以取消 —— 它是一次普通的自助操作，不是登录前的强制关卡。
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ApiError } from '@/api/client'
import { api } from '@/api'
import { useAuthStore } from '@/stores/auth'

const visible = defineModel<boolean>({ required: true })

const auth = useAuthStore()
const { t } = useI18n()
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
    errorText.value = t('common.sessionInvalid')
    return
  }
  if (!form.value.password) {
    errorText.value = t('password.required')
    return
  }
  if (form.value.password !== form.value.confirm) {
    errorText.value = t('password.mismatch')
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
    ElMessage.success(t('changePassword.success'))
  } catch (error) {
    errorText.value = error instanceof ApiError ? error.reason || error.message : t('common.updateFailed')
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <el-dialog v-model="visible" :title="t('changePassword.title')" width="460px" align-center>
    <el-alert v-if="errorText" class="login-error" :title="errorText" type="error" :closable="false" show-icon />
    <el-form label-width="88px" @submit.prevent>
      <el-form-item :label="t('changePassword.accountName')">
        <span class="account-name">{{ auth.user }}</span>
      </el-form-item>
      <el-form-item :label="t('password.new')">
        <el-input
          v-model="form.password"
          type="password"
          show-password
          :placeholder="t('password.newPlaceholder')"
          autocomplete="new-password"
        />
      </el-form-item>
      <el-form-item :label="t('password.confirm')">
        <el-input
          v-model="form.confirm"
          type="password"
          show-password
          :placeholder="t('password.confirmPlaceholder')"
          autocomplete="new-password"
          @keyup.enter="submit"
        />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="visible = false">{{ t('common.cancel') }}</el-button>
      <el-button type="primary" :loading="submitting" @click="submit">{{ t('common.confirm') }}</el-button>
    </template>
  </el-dialog>
</template>

<style scoped>
.account-name {
  color: #606266;
}
</style>
