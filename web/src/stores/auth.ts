// 认证状态：用户名/密码仅保存在 sessionStorage，不落 localStorage

import { computed, ref } from 'vue'
import { defineStore } from 'pinia'
import { clearCredentials, getCredentials, setCredentials } from '@/api/client'
import { api } from '@/api'
import type { Whoami } from '@/api/types'

export const useAuthStore = defineStore('auth', () => {
  /** 已认证的当前用户（来自 /api/whoami） */
  const user = ref<string | null>(null)
  /** 用户标签（administrator / monitoring 等） */
  const tags = ref<string>('')
  /** 是否总管理员（总账号：不可删除/禁用/降级，只有本人能改自己） */
  const isRoot = ref(false)
  /** 是否必须强制修改账号名与口令（首次登录时为 true，改密前不得进入任何页面） */
  const mustChangePassword = ref(false)
  /** 可访问的管理接口功能组；空数组表示不限制（用标签允许的全部接口） */
  const apiGroups = ref<string[]>([])
  /** 是否已通过服务端校验 */
  const authenticated = ref(false)

  const isLoggedIn = computed(() => authenticated.value)

  /** 用 whoami 结果刷新本地认证状态 */
  function applyWhoami(whoami: Whoami): void {
    user.value = whoami.name
    tags.value = whoami.tags
    isRoot.value = whoami.is_root
    mustChangePassword.value = whoami.must_change_password
    apiGroups.value = whoami.api_groups ?? []
    authenticated.value = true
  }

  /** 当前账号是否能访问某个管理接口功能组：功能组为空表示不限制 */
  function canAccessApi(group: string): boolean {
    return apiGroups.value.length === 0 || apiGroups.value.includes(group)
  }

  /** 使用给定凭据登录：先校验 /api/whoami，失败则丢弃凭据 */
  async function login(username: string, password: string): Promise<void> {
    setCredentials({ user: username, password })
    try {
      applyWhoami(await api.whoami())
    } catch (error) {
      clearCredentials()
      authenticated.value = false
      user.value = null
      tags.value = ''
      isRoot.value = false
      mustChangePassword.value = false
      apiGroups.value = []
      throw error
    }
  }

  /** 使用 sessionStorage 中已保存的凭据做一次校验 */
  async function verify(): Promise<void> {
    applyWhoami(await api.whoami())
  }

  /**
   * 更新凭据并重新校验（改密成功后调用）。
   *
   * 服务端在改密/改名的瞬间就会让旧凭据失效，因此必须先把新凭据写入 sessionStorage，
   * 再用它重新拉取 whoami 刷新账号名、总管理员与强制改密标记。
   */
  async function applyCredentials(username: string, password: string): Promise<void> {
    setCredentials({ user: username, password })
    applyWhoami(await api.whoami())
  }

  /** 仅清理本地状态（401 时由统一处理器调用，凭据已在客户端清空） */
  function clearLocal(): void {
    authenticated.value = false
    user.value = null
    tags.value = ''
    isRoot.value = false
    mustChangePassword.value = false
    apiGroups.value = []
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

  return {
    user,
    tags,
    isRoot,
    mustChangePassword,
    apiGroups,
    authenticated,
    isLoggedIn,
    canAccessApi,
    login,
    verify,
    applyCredentials,
    clearLocal,
    logout,
    hasStoredCredentials,
  }
})
