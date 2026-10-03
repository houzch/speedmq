// vhost 全局状态：顶部选择器与列表页联动

import { ref } from 'vue'
import { defineStore } from 'pinia'
import { api } from '@/api'
import type { Vhost } from '@/api/types'

/** 默认 vhost */
const DEFAULT_VHOST = '/'

export const useVhostStore = defineStore('vhost', () => {
  const vhosts = ref<Vhost[]>([])
  const current = ref<string>(DEFAULT_VHOST)
  const loading = ref(false)
  const loaded = ref(false)

  /** 拉取 /api/vhosts；若当前 vhost 不在列表中则回退到第一个 */
  async function loadVhosts(): Promise<void> {
    loading.value = true
    try {
      const list = await api.vhosts()
      vhosts.value = list
      loaded.value = true
      if (!list.some((item) => item.name === current.value)) {
        current.value = list.length > 0 ? list[0].name : DEFAULT_VHOST
      }
    } catch (error) {
      // 权限收窄的账号可能读不到 /api/vhosts（403）：这里必须捕获，
      // 否则调用方用 `void` 丢弃时会变成未处理的 Promise 拒绝（控制台报错、选择器空白且无提示）。
      console.warn('加载 vhost 列表失败', error)
    } finally {
      loading.value = false
    }
  }

  function setCurrent(name: string): void {
    current.value = name
  }

  return { vhosts, current, loading, loaded, loadVhosts, setCurrent }
})
