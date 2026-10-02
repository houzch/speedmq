// SwiftMQ 管理 HTTP API 封装（全部路径以 /api 开头，vhost 与名称均需 percent-encode）
import { request } from './client'
import type {
  Binding,
  Cluster,
  ClusterMembers,
  Connection,
  Consumer,
  Exchange,
  ExchangeDeclareRequest,
  ExchangeQuery,
  GetMessage,
  GetMessagesRequest,
  NodeInfo,
  Overview,
  Permission,
  PublishRequest,
  PublishResult,
  QuorumView,
  Queue,
  QueueDeclareRequest,
  QueueQuery,
  RebalanceResult,
  User,
  UserUpsertRequest,
  CredentialsRequest,
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

  // ---- 集群 ----
  cluster: (): Promise<Cluster> => request<Cluster>('GET', '/api/cluster'),
  clusterMembers: (): Promise<ClusterMembers> => request<ClusterMembers>('GET', '/api/cluster/members'),
  addClusterMember: (nodeId: string, addr: string): Promise<ClusterMembers> =>
    request<ClusterMembers>('PUT', `/api/cluster/members/${encodePath(nodeId)}`, { body: { addr } }),
  removeClusterMember: (nodeId: string): Promise<ClusterMembers> =>
    request<ClusterMembers>('DELETE', `/api/cluster/members/${encodePath(nodeId)}`),

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
  /** 声明队列（语义同 queue.declare）：201 新建 / 204 已存在且参数等价 / 400 参数不等价 */
  declareQueue: (vhost: string, name: string, body: QueueDeclareRequest): Promise<void> =>
    request<void>('PUT', `/api/queues/${encodePath(vhost)}/${encodePath(name)}`, { body }),
  getMessages: (vhost: string, name: string, body: GetMessagesRequest): Promise<GetMessage[]> =>
    request<GetMessage[]>('POST', `/api/queues/${encodePath(vhost)}/${encodePath(name)}/get`, { body }),
  queueBindings: (vhost: string, name: string): Promise<Binding[]> =>
    request<Binding[]>('GET', `/api/queues/${encodePath(vhost)}/${encodePath(name)}/bindings`),
  /**
   * 把仲裁队列的副本数扩到 count（只增不减）。
   *
   * 改组要等新副本追平日志，可能耗时较久，因此这里单独放宽超时（默认 10 秒不够）。
   */
  growQueue: (vhost: string, name: string, count: number): Promise<QuorumView> =>
    request<QuorumView>('PUT', `/api/queues/${encodePath(vhost)}/${encodePath(name)}/grow`, {
      body: { count },
      timeout: 120_000,
    }),
  /** 把仲裁队列的 leader 迁到副本集中较空的节点 */
  rebalanceQueue: (vhost: string, name: string): Promise<RebalanceResult> =>
    request<RebalanceResult>('PUT', `/api/queues/${encodePath(vhost)}/${encodePath(name)}/rebalance`),

  // ---- 交换机 ----
  exchanges: (query?: ExchangeQuery): Promise<Exchange[]> =>
    request<Exchange[]>('GET', '/api/exchanges', { query }),
  exchange: (vhost: string, name: string): Promise<Exchange> =>
    request<Exchange>('GET', `/api/exchanges/${encodePath(vhost)}/${encodePath(name)}`),
  /** 声明交换机（语义同 exchange.declare）：201 新建 / 204 已存在且参数等价 / 400 类型非法等 */
  declareExchange: (vhost: string, name: string, body: ExchangeDeclareRequest): Promise<void> =>
    request<void>('PUT', `/api/exchanges/${encodePath(vhost)}/${encodePath(name)}`, { body }),
  /** 删除交换机：if-unused=true 时仅在无绑定/无消费者时删除 */
  deleteExchange: (vhost: string, name: string, ifUnused = false): Promise<void> =>
    request<void>('DELETE', `/api/exchanges/${encodePath(vhost)}/${encodePath(name)}`, {
      query: { 'if-unused': ifUnused },
    }),
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

  // ---- 账号（用户）----
  users: (): Promise<User[]> => request<User[]>('GET', '/api/users'),
  user: (name: string): Promise<User> => request<User>('GET', `/api/users/${encodePath(name)}`),
  saveUser: (name: string, body: UserUpsertRequest): Promise<void> =>
    request<void>('PUT', `/api/users/${encodePath(name)}`, { body }),
  deleteUser: (name: string): Promise<void> =>
    request<void>('DELETE', `/api/users/${encodePath(name)}`),
  changeCredentials: (name: string, body: CredentialsRequest): Promise<void> =>
    request<void>('POST', `/api/users/${encodePath(name)}/credentials`, { body }),

  // ---- 权限 ----
  permissions: (): Promise<Permission[]> => request<Permission[]>('GET', '/api/permissions'),
  vhostPermissions: (vhost: string): Promise<Permission[]> =>
    request<Permission[]>('GET', `/api/vhosts/${encodePath(vhost)}/permissions`),
  userPermission: (vhost: string, user: string): Promise<Permission> =>
    request<Permission>('GET', `/api/permissions/${encodePath(vhost)}/${encodePath(user)}`),
  setPermission: (
    vhost: string,
    user: string,
    body: Pick<Permission, 'configure' | 'write' | 'read'>,
  ): Promise<void> =>
    request<void>('PUT', `/api/permissions/${encodePath(vhost)}/${encodePath(user)}`, { body }),
  deletePermission: (vhost: string, user: string): Promise<void> =>
    request<void>('DELETE', `/api/permissions/${encodePath(vhost)}/${encodePath(user)}`),
}
