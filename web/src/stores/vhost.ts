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
    } finally {
      loading.value = false
    }
  }

  function setCurrent(name: string): void {
    current.value = name
  }

  return { vhosts, current, loading, loaded, loadVhosts, setCurrent }
})
