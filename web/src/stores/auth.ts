// 认证状态：用户名/密码仅保存在 sessionStorage，不落 localStorage

import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { clearCredentials, getCredentials, setCredentials } from '@/api/client'
import { api } from '@/api'

export const useAuthStore = defineStore('auth', () => {
  /** 已认证的当前用户（来自 /api/whoami） */
  const user = ref<string | null>(null)
  /** 用户标签（administrator / monitoring 等） */
  const tags = ref<string>('')
  /** 是否已通过服务端校验 */
  const authenticated = ref(false)

  const isLoggedIn = computed(() => authenticated.value)

  /** 使用给定凭据登录：先校验 /api/whoami，失败则丢弃凭据 */
  async function login(username: string, password: string): Promise<void> {
    setCredentials({ user: username, password })
    try {
      const whoami = await api.whoami()
      user.value = whoami.name
      tags.value = whoami.tags
      authenticated.value = true
    } catch (error) {
      clearCredentials()
      authenticated.value = false
      user.value = null
      tags.value = ''
      throw error
    }
  }

  /** 使用 sessionStorage 中已保存的凭据做一次校验 */
  async function verify(): Promise<void> {
    const whoami = await api.whoami()
    user.value = whoami.name
    tags.value = whoami.tags
    authenticated.value = true
  }

  /** 仅清理本地状态（401 时由统一处理器调用，凭据已在客户端清空） */
  function clearLocal(): void {
    authenticated.value = false
    user.value = null
    tags.value = ''
  }

  /** 主动退出登录 */
  function logout(): void {
    clearCredentials()
    clearLocal()
  }

  /** 本地是否还留有凭据 */
  function hasStoredCredentials(): boolean {
    return getCredentials() !== null
  }

  return { user, tags, authenticated, isLoggedIn, login, verify, clearLocal, logout, hasStoredCredentials }
})
