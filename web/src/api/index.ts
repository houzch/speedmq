// SwiftMQ 管理 HTTP API 封装（全部路径以 /api 开头，vhost 与名称均需 percent-encode）
import { request } from './client'
import type {
  Binding,
  Connection,
  Consumer,
  Exchange,
  ExchangeQuery,
  GetMessage,
  GetMessagesRequest,
  NodeInfo,
  Overview,
  PublishRequest,
  PublishResult,
  Queue,
  QueueQuery,
  Vhost,
  Whoami,
} from './types'

/** vhost 与对象名在 URL 中必须 percent-encode（默认 vhost "/" → "%2F"） */
const encodePath = (value: string): string => encodeURIComponent(value)

export const api = {
  // ---- 概览与节点 ----
  overview: (): Promise<Overview> => request<Overview>('GET', '/api/overview'),
  nodes: (): Promise<NodeInfo[]> => request<NodeInfo[]>('GET', '/api/nodes'),
  whoami: (): Promise<Whoami> => request<Whoami>('GET', '/api/whoami'),
  vhosts: (): Promise<Vhost[]> => request<Vhost[]>('GET', '/api/vhosts'),

  // ---- 队列 ----
  queues: (query?: QueueQuery): Promise<Queue[]> => request<Queue[]>('GET', '/api/queues', { query }),
  queue: (vhost: string, name: string): Promise<Queue> =>
    request<Queue>('GET', `/api/queues/${encodePath(vhost)}/${encodePath(name)}`),
  deleteQueue: (vhost: string, name: string, ifUnused = false, ifEmpty = false): Promise<void> =>
    request<void>('DELETE', `/api/queues/${encodePath(vhost)}/${encodePath(name)}`, {
      query: { 'if-unused': ifUnused, 'if-empty': ifEmpty },
    }),
  purgeQueue: (vhost: string, name: string): Promise<void> =>
    request<void>('DELETE', `/api/queues/${encodePath(vhost)}/${encodePath(name)}/contents`),
  getMessages: (vhost: string, name: string, body: GetMessagesRequest): Promise<GetMessage[]> =>
    request<GetMessage[]>('POST', `/api/queues/${encodePath(vhost)}/${encodePath(name)}/get`, { body }),
  queueBindings: (vhost: string, name: string): Promise<Binding[]> =>
    request<Binding[]>('GET', `/api/queues/${encodePath(vhost)}/${encodePath(name)}/bindings`),

  // ---- 交换机 ----
  exchanges: (query?: ExchangeQuery): Promise<Exchange[]> =>
    request<Exchange[]>('GET', '/api/exchanges', { query }),
  exchange: (vhost: string, name: string): Promise<Exchange> =>
    request<Exchange>('GET', `/api/exchanges/${encodePath(vhost)}/${encodePath(name)}`),
  exchangeSourceBindings: (vhost: string, name: string): Promise<Binding[]> =>
    request<Binding[]>('GET', `/api/exchanges/${encodePath(vhost)}/${encodePath(name)}/bindings/source`),
  publish: (vhost: string, exchange: string, body: PublishRequest): Promise<PublishResult> =>
    request<PublishResult>('POST', `/api/exchanges/${encodePath(vhost)}/${encodePath(exchange)}/publish`, { body }),

  // ---- 绑定 ----
  bindings: (vhost?: string): Promise<Binding[]> =>
    vhost === undefined
      ? request<Binding[]>('GET', '/api/bindings')
      : request<Binding[]>('GET', `/api/bindings/${encodePath(vhost)}`),

  // ---- 连接 ----
  connections: (): Promise<Connection[]> => request<Connection[]>('GET', '/api/connections'),
  connection: (name: string): Promise<Connection> =>
    request<Connection>('GET', `/api/connections/${encodePath(name)}`),
  closeConnection: (name: string, reason?: string): Promise<void> =>
    request<void>('DELETE', `/api/connections/${encodePath(name)}`, {
      query: reason ? { reason } : undefined,
    }),

  // ---- 消费者 ----
  consumers: (vhost: string): Promise<Consumer[]> =>
    request<Consumer[]>('GET', `/api/consumers/${encodePath(vhost)}`),
}
