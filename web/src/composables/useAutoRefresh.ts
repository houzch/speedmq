// 数据页面自动刷新：挂载时加载一次，并按全局间隔自动重复加载

import { onMounted, onUnmounted, watch } from 'vue'
import { useAuthStore } from '@/stores/auth'
import { useRefreshStore } from '@/stores/refresh'

/** 定时器句柄（浏览器为 number，交给对应平台的 clearInterval 清理） */
type TimerHandle = ReturnType<typeof setInterval>

/**
 * 挂载时立即执行一次 load，并按全局设置的间隔自动重复执行。
 *
 * 加载时机与认证状态绑定，这一点很关键 —— 页面是「先挂载、后登录」的：
 * 直接打开带 hash 的地址（或刷新页面）时，视图会先于登录完成挂载。
 * 若此时就去请求，只会拿到一串 401、并在登录框后面弹一堆错误提示；
 * 更糟的是登录成功后没人再触发加载，页面会一直空着（选了「不自动刷新」就永远空着）。
 * 因此这里统一成：**未认证不加载也不轮询，认证通过后立刻补一次**。
 *
 * - 间隔为 0 时不启动定时器；
 * - 间隔变化时先清旧定时器再按新值重排，避免定时器叠加；
 * - 退出登录时停止轮询（否则会持续打 401）；
 * - 组件卸载时清理定时器，避免离开页面后仍在请求。
 */
export function useAutoRefresh(load: () => void | Promise<void>): void {
  const auth = useAuthStore()
  const refresh = useRefreshStore()
  let timer: TimerHandle | null = null

  /** 清理当前定时器 */
  function stop(): void {
    if (timer !== null) {
      clearInterval(timer)
      timer = null
    }
  }

  /** 先清后起：按当前状态重排定时器（未认证或间隔为 0 则不启动） */
  function restart(): void {
    stop()
    if (!auth.authenticated || refresh.intervalSeconds <= 0) return
    timer = setInterval(() => {
      void load()
    }, refresh.intervalSeconds * 1000)
  }

  onMounted(() => {
    if (auth.authenticated) {
      void load()
    }
    restart()
  })

  // 认证状态变化：登录成功立刻补一次加载（不必等下一个刷新周期），退出登录则停掉轮询。
  watch(
    () => auth.authenticated,
    (ok) => {
      if (ok) {
        void load()
      }
      restart()
    },
  )

  watch(
    () => refresh.intervalSeconds,
    () => {
      restart()
    },
  )

  onUnmounted(() => {
    stop()
  })
}
