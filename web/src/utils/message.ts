// 统一的错误提示（ElMessage 由 unplugin-auto-import 自动引入）

import { ApiError } from '@/api/client'

/** 以 Element Plus 消息提示展示错误；401 已由登录框接管，静默处理 */
export function showError(error: unknown, fallback: string): void {
  if (error instanceof ApiError) {
    if (error.status === 401) return
    const detail = error.reason ? `${error.message}：${error.reason}` : error.message
    ElMessage.error(detail)
    return
  }
  if (error instanceof Error) {
    ElMessage.error(`${fallback}：${error.message}`)
    return
  }
  ElMessage.error(fallback)
}
