// SpeedMQ 管理 HTTP API 封装（全部路径以 /api 开头，vhost 与名称均需 percent-encode）
import { request } from './client'
import type {
  Binding,
  BindingRequest,
  Cluster,
  ClusterMembers,
  Connection,
  Consumer,
  DeprecatedFeature,
  Exchange,
  ExchangeDeclareRequest,
  ExchangeQuery,
  FeatureFlag,
  GetMessage,
  GetMessagesRequest,
  NodeInfo,
  Overview,
  Permission,
  Plugin,
  Policy,
  PolicyRequest,
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
  VHostLimit,
  VHostLimitName,
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
  /** 新建虚拟主机：201 新建 / 204 已存在（幂等） */
  createVHost: (name: string): Promise<void> =>
    request<void>('PUT', `/api/vhosts/${encodePath(name)}`),
  /** 删除虚拟主机（连同其队列/交换机/绑定/权限/策略/限制）；默认 vhost 会被服务端拒绝 */
  deleteVHost: (name: string): Promise<void> =>
    request<void>('DELETE', `/api/vhosts/${encodePath(name)}`),

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
  /** 建立「交换机 → 队列」绑定（成功 201，重复绑定幂等亦 201） */
  bindQueue: (vhost: string, source: string, destination: string, body: BindingRequest): Promise<void> =>
    request<void>(
      'POST',
      `/api/bindings/${encodePath(vhost)}/e/${encodePath(source)}/q/${encodePath(destination)}`,
      { body },
    ),
  /** 建立「交换机 → 交换机」绑定（成功 201，重复绑定幂等亦 201） */
  bindExchange: (vhost: string, source: string, destination: string, body: BindingRequest): Promise<void> =>
    request<void>(
      'POST',
      `/api/bindings/${encodePath(vhost)}/e/${encodePath(source)}/e/${encodePath(destination)}`,
      { body },
    ),
  /** 删除「交换机 → 队列」绑定；props 为绑定对象的 properties_key（原样回传，不要自行拼接） */
  unbindQueue: (vhost: string, source: string, destination: string, props: string): Promise<void> =>
    request<void>(
      'DELETE',
      `/api/bindings/${encodePath(vhost)}/e/${encodePath(source)}/q/${encodePath(destination)}/${encodePath(props)}`,
    ),
  /** 删除「交换机 → 交换机」绑定；props 为绑定对象的 properties_key（原样回传，不要自行拼接） */
  unbindExchange: (vhost: string, source: string, destination: string, props: string): Promise<void> =>
    request<void>(
      'DELETE',
      `/api/bindings/${encodePath(vhost)}/e/${encodePath(source)}/e/${encodePath(destination)}/${encodePath(props)}`,
    ),

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

  // ---- 策略 ----
  policies: (): Promise<Policy[]> => request<Policy[]>('GET', '/api/policies'),
  /** 新建策略返回 201、更新返回 204（两者都不需要区分处理） */
  savePolicy: (vhost: string, name: string, body: PolicyRequest): Promise<void> =>
    request<void>('PUT', `/api/policies/${encodePath(vhost)}/${encodePath(name)}`, { body }),
  deletePolicy: (vhost: string, name: string): Promise<void> =>
    request<void>('DELETE', `/api/policies/${encodePath(vhost)}/${encodePath(name)}`),

  // ---- vhost 级限制 ----
  /** 返回按 vhost 分组的限制列表；vhost 为空表示全部 */
  vhostLimits: (vhost?: string): Promise<VHostLimit[]> =>
    vhost === undefined
      ? request<VHostLimit[]>('GET', '/api/vhost-limits')
      : request<VHostLimit[]>('GET', `/api/vhost-limits/${encodePath(vhost)}`),
  /** 设置一条限制；成功返回 204 */
  setVHostLimit: (vhost: string, name: VHostLimitName, value: number): Promise<void> =>
    request<void>('PUT', `/api/vhost-limits/${encodePath(vhost)}/${encodePath(name)}`, { body: { value } }),
  /** 删除一条限制；不存在也返回 204 */
  deleteVHostLimit: (vhost: string, name: VHostLimitName): Promise<void> =>
    request<void>('DELETE', `/api/vhost-limits/${encodePath(vhost)}/${encodePath(name)}`),

  // ---- 特性开关与弃用特性 ----
  featureFlags: (): Promise<FeatureFlag[]> => request<FeatureFlag[]>('GET', '/api/feature-flags'),
  setFeatureFlag: (name: string, enabled: boolean): Promise<void> =>
    request<void>('PUT', `/api/feature-flags/${encodePath(name)}/${enabled ? 'enable' : 'disable'}`),
  /** 弃用特性是只读清单：RabbitMQ 4.x 上没有 enable/disable 接口 */
  deprecatedFeatures: (): Promise<DeprecatedFeature[]> =>
    request<DeprecatedFeature[]>('GET', '/api/deprecated-features'),

  // ---- 插件 ----
  plugins: (): Promise<Plugin[]> => request<Plugin[]>('GET', '/api/plugins'),
  plugin: (name: string): Promise<Plugin> => request<Plugin>('GET', `/api/plugins/${encodePath(name)}`),
  /** 热启用插件（恢复其对外监听） */
  enablePlugin: (name: string): Promise<void> =>
    request<void>('PUT', `/api/plugins/${encodePath(name)}/enable`),
  /** 热停用插件（关闭其对外监听） */
  disablePlugin: (name: string): Promise<void> =>
    request<void>('PUT', `/api/plugins/${encodePath(name)}/disable`),
}
