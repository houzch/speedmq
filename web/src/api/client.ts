// 统一的 HTTP 客户端：Basic Auth、10 秒超时、错误体解析
// 凭据只保存在 sessionStorage（不落 localStorage），关闭标签页即失效。

import { i18n } from '@/locales'

export interface Credentials {
  user: string
  password: string
}

const SS_USER = 'speedmq.auth.user'
const SS_PASSWORD = 'speedmq.auth.password'

/** 请求超时时间（毫秒） */
const REQUEST_TIMEOUT = 10_000

/** 服务端错误体形如 {"error":"Object Not Found","reason":"..."} */
interface ErrorBody {
  error?: string
  reason?: string
}

/** 管理 API 错误，携带 HTTP 状态码与服务端 reason */
export class ApiError extends Error {
  readonly status: number
  readonly reason: string

  constructor(status: number, error: string, reason: string) {
    super(error)
    this.name = 'ApiError'
    this.status = status
    this.reason = reason
  }
}

export function getCredentials(): Credentials | null {
  const user = window.sessionStorage.getItem(SS_USER)
  const password = window.sessionStorage.getItem(SS_PASSWORD)
  if (user === null || password === null) return null
  return { user, password }
}

export function setCredentials(credentials: Credentials): void {
  window.sessionStorage.setItem(SS_USER, credentials.user)
  window.sessionStorage.setItem(SS_PASSWORD, credentials.password)
}

export function clearCredentials(): void {
  window.sessionStorage.removeItem(SS_USER)
  window.sessionStorage.removeItem(SS_PASSWORD)
}

/** 生成 Basic Auth 头，按 UTF-8 编码以支持非 ASCII 账号密码 */
function basicAuthHeader(credentials: Credentials): string {
  const bytes = new TextEncoder().encode(`${credentials.user}:${credentials.password}`)
  let binary = ''
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte)
  })
  return `Basic ${window.btoa(binary)}`
}

/** 401 回调：由界面层注册，用于清空凭据并弹出登录框 */
let unauthorizedHandler: (() => void) | null = null

export function setUnauthorizedHandler(handler: (() => void) | null): void {
  unauthorizedHandler = handler
}

export type QueryValue = string | number | boolean | undefined | null

export interface RequestOptions {
  query?: Record<string, QueryValue>
  body?: unknown
  /** 覆盖默认的 10 秒超时（毫秒）。少数接口本身很慢，例如仲裁队列扩副本要等新副本追平。 */
  timeout?: number
}

function buildUrl(path: string, query?: Record<string, QueryValue>): string {
  if (!query) return path
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null) continue
    search.append(key, String(value))
  }
  const queryString = search.toString()
  return queryString ? `${path}?${queryString}` : path
}

async function readErrorBody(response: Response): Promise<ErrorBody> {
  try {
    return (await response.json()) as ErrorBody
  } catch {
    return {}
  }
}

/** 发起管理 API 请求；204 与空响应体返回 undefined */
export async function request<T>(method: string, path: string, options: RequestOptions = {}): Promise<T> {
  const url = buildUrl(path, options.query)
  const controller = new AbortController()
  const timeout = options.timeout ?? REQUEST_TIMEOUT
  const timer = window.setTimeout(() => controller.abort(), timeout)

  try {
    const headers = new Headers({ Accept: 'application/json' })
    const credentials = getCredentials()
    if (credentials) headers.set('Authorization', basicAuthHeader(credentials))

    let body: string | undefined
    if (options.body !== undefined) {
      headers.set('Content-Type', 'application/json')
      body = JSON.stringify(options.body)
    }

    const response = await fetch(url, {
      method,
      headers,
      body,
      signal: controller.signal,
      cache: 'no-store',
    })

    // 401：清空凭据并通知界面重新登录
    if (response.status === 401) {
      clearCredentials()
      unauthorizedHandler?.()
      throw new ApiError(401, 'Unauthorized', i18n.global.t('client.invalidCredentials'))
    }

    if (!response.ok) {
      const errorBody = await readErrorBody(response)
      throw new ApiError(response.status, errorBody.error ?? `HTTP ${response.status}`, errorBody.reason ?? '')
    }

    if (response.status === 204) return undefined as T

    const text = await response.text()
    if (!text) return undefined as T
    return JSON.parse(text) as T
  } catch (error) {
    if (error instanceof ApiError) throw error
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new ApiError(0, 'Request Timeout', i18n.global.t('client.timeout', { seconds: Math.round(timeout / 1000) }))
    }
    throw new ApiError(0, 'Network Error', i18n.global.t('client.networkError'))
  } finally {
    window.clearTimeout(timer)
  }
}
