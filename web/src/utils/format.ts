// 展示层格式化工具：数字/时间格式跟随当前界面语言，文案经 i18n。

import { i18n } from '@/locales'
import type { RateDetails } from '@/api/types'

/** 当前界面语言 code（用于 toLocaleString 与日期本地化） */
function currentLocale(): string {
  return String(i18n.global.locale.value)
}

/** 取翻译文案（i18n.global.t 在渲染上下文中会跟随语言变化重新求值） */
function t(key: string, params?: Record<string, unknown>): string {
  return params ? i18n.global.t(key, params) : i18n.global.t(key)
}

/** 字节数格式化：1024 → "1.0 KB" */
export function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—'
  if (value === 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const index = Math.min(Math.floor(Math.log(Math.abs(value)) / Math.log(1024)), units.length - 1)
  const scaled = value / 1024 ** index
  return `${scaled.toFixed(index === 0 ? 0 : 1)} ${units[index]}`
}

/** 秒数格式化为「x 天 x 小时 x 分 x 秒」（单位随语言切换） */
export function formatDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds) || seconds < 0) return '—'
  const total = Math.floor(seconds)
  const days = Math.floor(total / 86400)
  const hours = Math.floor((total % 86400) / 3600)
  const minutes = Math.floor((total % 3600) / 60)
  const secs = total % 60
  const parts: string[] = []
  if (days > 0) parts.push(t('format.days', { n: days }))
  if (hours > 0 || days > 0) parts.push(t('format.hours', { n: hours }))
  if (minutes > 0 || hours > 0 || days > 0) parts.push(t('format.minutes', { n: minutes }))
  parts.push(t('format.seconds', { n: secs }))
  return parts.join(' ')
}

/** 从 epoch 毫秒计算已持续时间 */
export function formatElapsedFrom(epochMillis: number | null | undefined): string {
  if (epochMillis === null || epochMillis === undefined || epochMillis <= 0) return '—'
  return formatDuration((Date.now() - epochMillis) / 1000)
}

/** 数字千分位显示，默认最多 2 位小数 */
export function formatNumber(value: number | null | undefined, fractionDigits = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—'
  return value.toLocaleString(currentLocale(), { maximumFractionDigits: fractionDigits })
}

/** 速率显示："0.00/s" */
export function formatRate(details: RateDetails | null | undefined, fractionDigits = 2): string {
  const rate = details?.rate
  if (rate === undefined || rate === null || Number.isNaN(rate)) return '—'
  return `${rate.toFixed(fractionDigits)}/s`
}

/** 时间戳（epoch 毫秒）格式化为本地时间 */
export function formatTimestamp(epochMillis: number | null | undefined): string {
  if (!epochMillis) return '—'
  return new Date(epochMillis).toLocaleString(currentLocale(), { hour12: false })
}

/** 任意值格式化为 JSON 文本 */
export function formatJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

/** 布尔值按当前语言显示为是/否 */
export function formatBoolean(value: boolean | null | undefined): string {
  if (value === null || value === undefined) return '—'
  return value ? t('format.yes') : t('format.no')
}
