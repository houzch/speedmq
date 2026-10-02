// 全局刷新设置：自动刷新间隔（持久化到 localStorage）+ 最近一次刷新时间

import { ref } from 'vue'
import { defineStore } from 'pinia'

/** 间隔持久化键 */
const STORAGE_KEY = 'swiftmq.refresh_interval'

/** 合法间隔（秒），0 表示不自动刷新 */
const VALID_INTERVALS = [0, 5, 10, 30]

/** 默认间隔（秒） */
const DEFAULT_INTERVAL = 5

/** 下拉选项：间隔（秒）与中文标签 */
export const REFRESH_OPTIONS: { seconds: number; label: string }[] = [
  { seconds: 5, label: '每 5 秒刷新' },
  { seconds: 10, label: '每 10 秒刷新' },
  { seconds: 30, label: '每 30 秒刷新' },
  { seconds: 0, label: '不自动刷新' },
]

/** 读取持久化的间隔；缺失或非法时回退默认值 */
function readStoredInterval(): number {
  const raw = localStorage.getItem(STORAGE_KEY)
  if (raw === null || raw.trim() === '') return DEFAULT_INTERVAL
  const parsed = Number(raw)
  return VALID_INTERVALS.includes(parsed) ? parsed : DEFAULT_INTERVAL
}

export const useRefreshStore = defineStore('refresh', () => {
  /** 当前自动刷新间隔（秒），0 表示不自动刷新 */
  const intervalSeconds = ref<number>(readStoredInterval())
  /** 最近一次刷新的 epoch 毫秒，尚未刷新过为 null */
  const lastRefreshed = ref<number | null>(null)

  /** 记录一次刷新时间 */
  function markRefreshed(): void {
    lastRefreshed.value = Date.now()
  }

  /** 设置自动刷新间隔（非法值忽略），并持久化以便跨刷新保留 */
  function setIntervalSeconds(v: number): void {
    if (!VALID_INTERVALS.includes(v)) return
    intervalSeconds.value = v
    localStorage.setItem(STORAGE_KEY, String(v))
  }

  return { intervalSeconds, lastRefreshed, markRefreshed, setIntervalSeconds }
})
