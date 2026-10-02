<script setup lang="ts">
// 强制改密对话框：首次登录（must_change_password）时必须先修改账号名与口令。
//
// 与登录框一样不可通过遮罩/Esc/关闭按钮关闭 —— 在改密成功之前不放行任何业务页面。
import { ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { ApiError } from '@/api/client'
import { api } from '@/api'
import { useAuthStore } from '@/stores/auth'

const auth = useAuthStore()
const { t } = useI18n()

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
    errorText.value = t('forcePassword.nameRequired')
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
  const currentName = auth.user
  if (currentName === null) {
    errorText.value = t('common.sessionInvalid')
    return
  }
  errorText.value = ''
  submitting.value = true
  try {
    await api.changeCredentials(currentName, { name, password: form.value.password })
    // 旧凭据已失效，用新账号名/口令刷新本地凭据与认证状态
    await auth.applyCredentials(name, form.value.password)
    ElMessage.success(t('forcePassword.success'))
  } catch (error) {
    errorText.value = error instanceof ApiError ? error.reason || error.message : t('common.updateFailed')
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <el-dialog
    v-model="visible"
    :title="t('forcePassword.title')"
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
      :title="t('forcePassword.alertTitle')"
      :description="t('forcePassword.alertDesc')"
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
      <el-form-item :label="t('forcePassword.newName')">
        <el-input v-model="form.name" :placeholder="t('forcePassword.newNamePlaceholder')" autocomplete="username" />
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
      <el-button type="primary" :loading="submitting" @click="submit">{{ t('forcePassword.submit') }}</el-button>
    </template>
  </el-dialog>
</template>
