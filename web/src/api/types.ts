// SwiftMQ 管理 HTTP API 的数据结构定义
// 字段名严格对齐 RabbitMQ Management API 子集（见任务契约）

import type { QueryValue } from './client'

/** 速率明细（RabbitMQ 风格：{ rate: number }） */
export interface RateDetails {
  rate: number
}

/** 消息统计（累计值 + 速率明细） */
export interface MessageStats {
  publish?: number
  deliver?: number
  ack?: number
  deliver_get?: number
  publish_details?: RateDetails
  deliver_get_details?: RateDetails
  ack_details?: RateDetails
}

/** 对象总数 */
export interface ObjectTotals {
  connections: number
  channels: number
  queues: number
  consumers: number
  exchanges: number
}

/** 队列消息总数 */
export interface QueueTotals {
  messages: number
  messages_ready: number
  messages_unacknowledged: number
}

/** 监听器 */
export interface Listener {
  node: string
  protocol: string
  ip_address: string
  port: number
}

/** GET /api/overview */
export interface Overview {
  management_version: string
  rabbitmq_version: string
  product_name: string
  product_version: string
  cluster_name: string
  node: string
  listeners: Listener[]
  object_totals: ObjectTotals
  queue_totals: QueueTotals
  message_stats: MessageStats
}

/** GET /api/nodes */
export interface NodeInfo {
  name: string
  type: string
  running: boolean
  uptime: number
  mem_used: number
  mem_limit: number
  disk_free: number
  disk_free_limit: number
  proc_used: number
  proc_total: number
  os_pid: number
  fd_used: number
  fd_total: number
  sockets_used: number
  sockets_total: number
  applications: { name: string; version: string; description: string }[]
  enabled_plugins: string[]
  partitions: unknown[]
}

/** GET /api/whoami */
/** GET /api/default-language（免认证接口，登录前用它决定管理 UI 初始语言） */
export interface DefaultLanguage {
  /** 服务端安装时按系统时区推断的默认语言 code，如 "zh-CN" */
  default_language: string
}

export interface Whoami {
  name: string
  tags: string
  auth_backend: string
  /** 是否总管理员（总账号不可删除、禁用或移除 administrator 标签） */
  is_root: boolean
  /** 是否必须强制修改账号名与口令（首次登录时为 true） */
  must_change_password: boolean
  /** 可访问的管理接口功能组；空数组表示不限制（用标签允许的全部接口） */
  api_groups: string[]
}

/** GET /api/users 与 /api/users/{name} 的账号对象 */
export interface User {
  name: string
  /** 空格分隔的标签串，如 "administrator" / "management monitoring" */
  tags: string
  auth_backend: string
  is_root: boolean
  disabled: boolean
  must_change_password: boolean
  /** 可访问的管理接口功能组；空数组表示不限制（用标签允许的全部接口） */
  api_groups: string[]
}

/** 权限记录（GET /api/permissions 等） */
export interface Permission {
  user: string
  vhost: string
  configure: string
  write: string
  read: string
}

/** PUT /api/users/{name} 请求体（新建或更新账号） */
export interface UserUpsertRequest {
  /** 新建时必须提供；更新时留空表示不改口令 */
  password?: string
  /** 空格分隔标签串或标签数组；不传表示保持原状 */
  tags?: string | string[]
  disabled?: boolean
  must_change_password?: boolean
  /** 可访问的管理接口功能组；传空数组表示不限制，不传表示保持原状 */
  api_groups?: string[]
}

/** POST /api/users/{name}/credentials 请求体（改账号名与/或口令） */
export interface CredentialsRequest {
  /** 新账号名；留空或同旧名表示只改口令 */
  name?: string
  password?: string
}

/** 集群元数据规模（GET /api/cluster 的 object_totals） */
export interface ClusterObjectTotals {
  queues: number
  exchanges: number
  bindings: number
  users: number
}

/** 跨节点转发运行态（GET /api/cluster 的 forwarding） */
export interface ClusterForwarding {
  proxy_consumers: number
  remote_consumers: number
  held_deliveries: number
  forwarded_out: number
  forwarded_in: number
  deliveries: number
}

/**
 * GET /api/cluster：本节点的集群/元数据层状态。
 *
 * 单机模式也会返回（enabled=false、mode="local"、role="single"），
 * 界面据此禁用成员增删等集群专属操作 —— 服务端对这些操作返回 501。
 */
export interface Cluster {
  enabled: boolean
  mode: string
  node_id: string
  role: string
  term: number
  leader: string
  has_quorum: boolean
  paused: boolean
  peers: string[]
  learners: string[]
  commit_index: number
  last_applied: number
  applied_records: number
  object_totals: ClusterObjectTotals
  forwarding: ClusterForwarding
}

/** GET /api/cluster/members：成员划分 */
export interface ClusterMembers {
  voters: string[]
  learners: string[]
}

/** 仲裁队列副本集视图（队列对象的 swiftmq_quorum 字段） */
export interface QuorumView {
  vhost: string
  queue: string
  leader: string
  replicas: string[]
  count: number
  voters: string[]
  learners: string[]
}

/** PUT /api/queues/{vhost}/{name}/rebalance 的响应 */
export interface RebalanceResult {
  vhost: string
  queue: string
  moved: boolean
  from: string
  to: string
  reason?: string
}

/** GET /api/vhosts */
export interface Vhost {
  name: string
  tracing: boolean
  type: string
  messages: number
  messages_ready: number
  messages_unacknowledged: number
  messages_details: RateDetails
  cluster_state: Record<string, string>
}

/** 队列对象 */
export interface Queue {
  name: string
  vhost: string
  durable: boolean
  auto_delete: boolean
  exclusive: boolean
  type: string
  node: string
  state: string
  arguments: Record<string, unknown>
  consumers: number
  policy: string | null
  exclusive_consumer_tag: string | null
  messages: number
  messages_ready: number
  messages_unacknowledged: number
  messages_details: RateDetails
  messages_ready_details: RateDetails
  messages_unacknowledged_details: RateDetails
  message_stats?: MessageStats
  memory: number
  idle_since: string
  reductions: number
  /** RabbitMQ 字段：仲裁队列的投票成员（经典队列没有该字段） */
  members?: string[]
  /** RabbitMQ 字段：仲裁队列当前的服务节点 */
  leader?: string
  /** SwiftMQ 扩展：仲裁队列的副本集细节 */
  swiftmq_quorum?: QuorumView
}

/** 交换机对象 */
export interface Exchange {
  name: string
  vhost: string
  type: string
  durable: boolean
  auto_delete: boolean
  internal: boolean
  arguments: Record<string, unknown>
  policy: string | null
  message_stats?: { publish?: number }
  message_stats_details?: RateDetails
}

/** 连接对象 */
export interface Connection {
  name: string
  vhost: string
  user: string
  node: string
  state: string
  protocol: string
  channels: number
  connected_at: number
  timeout: number
  frame_max: number
  recv_oct: number
  send_oct: number
  ssl: boolean
  peer_host: string
  peer_port: number
  auth_mechanism: string
  client_properties: Record<string, unknown>
}

/** 消费者对象 */
export interface Consumer {
  consumer_tag: string
  queue: { name: string; vhost: string }
  ack_required: boolean
  prefetch_count: number
  exclusive: boolean
  arguments: Record<string, unknown>
}

/** 绑定关系 */
export interface Binding {
  source: string
  vhost: string
  destination: string
  destination_type: 'queue' | 'exchange'
  routing_key: string
  arguments: Record<string, unknown>
  properties_key: string
}

/** POST /api/bindings/{vhost}/e/{source}/q|e/{destination} 请求体（建立绑定） */
export interface BindingRequest {
  routing_key: string
  /** 绑定参数（headers 交换机等场景使用）；不传表示无参数 */
  arguments?: Record<string, unknown>
}

/** 消息属性 */
export interface MessageProperties {
  content_type: string
  delivery_mode: number
  headers: Record<string, unknown>
  priority: number
  correlation_id: string
  reply_to: string
  expiration: string
  message_id: string
  timestamp: number
  type: string
  user_id: string
  app_id: string
}

/** GET /api/queues/{vhost}/{name}/get 返回的单条消息 */
export interface GetMessage {
  payload: string
  payload_bytes: number
  payload_encoding: string
  redelivered: boolean
  exchange: string
  routing_key: string
  message_count: number
  properties: MessageProperties
}

/** POST .../get 的取消息模式 */
export type AckMode =
  | 'ack_requeue_true'
  | 'ack_requeue_false'
  | 'reject_requeue_true'
  | 'reject_requeue_false'

/** POST /api/queues/{vhost}/{name}/get 请求体 */
export interface GetMessagesRequest {
  count: number
  ackmode: AckMode
  encoding: 'auto' | 'base64'
  truncate: number
}

/** POST /api/exchanges/{vhost}/{name}/publish 请求体 */
export interface PublishRequest {
  properties: {
    content_type?: string
    delivery_mode?: number
    headers?: Record<string, unknown>
  }
  routing_key: string
  payload: string
  payload_encoding: 'string' | 'base64'
  mandatory: boolean
}

/** 发布结果 */
export interface PublishResult {
  routed: boolean
}

/** 队列列表查询参数（继承索引签名以便直接传给 fetch 客户端） */
export interface QueueQuery extends Record<string, QueryValue> {
  vhost?: string
  name?: string
  use_regex?: boolean
  page?: number
  page_size?: number
}

/** 交换机列表查询参数 */
export interface ExchangeQuery extends Record<string, QueryValue> {
  vhost?: string
  name?: string
}

/**
 * PUT /api/queues/{vhost}/{name} 请求体（声明队列，语义同客户端 queue.declare）。
 *
 * 队列类型通过 arguments 表达：{"x-queue-type":"quorum"} 为仲裁队列；不传为经典队列。
 */
export interface QueueDeclareRequest {
  durable: boolean
  auto_delete: boolean
  arguments?: Record<string, unknown>
}

/** PUT /api/exchanges/{vhost}/{name} 请求体（声明交换机，语义同客户端 exchange.declare） */
export interface ExchangeDeclareRequest {
  /** direct / fanout / topic / headers，不传默认 direct */
  type: string
  durable: boolean
  auto_delete: boolean
  internal: boolean
  arguments?: Record<string, unknown>
}

/** 策略对象（GET /api/policies 等） */
export interface Policy {
  vhost: string
  name: string
  /** 对象名匹配用的正则（非锚定，与 RabbitMQ 一致） */
  pattern: string
  /** 作用对象：queues / classic_queues / quorum_queues / exchanges / all */
  'apply-to': string
  /** 策略内容，键是不带 x- 前缀的形式（message-ttl / max-length / ...） */
  definition: Record<string, unknown>
  priority: number
}

/** PUT /api/policies/{vhost}/{name} 请求体 */
export interface PolicyRequest {
  pattern: string
  'apply-to': string
  definition: Record<string, unknown>
  priority: number
}

/**
 * vhost 级限制（GET /api/vhost-limits 的返回元素）。
 *
 * 形状对齐 RabbitMQ：同一个 vhost 的多条限制收在一个元素里，
 * `value` 是"限制名 → 值"的映射，如 `{"max-queues": 10}`。
 */
export interface VHostLimit {
  vhost: string
  value: Record<string, number>
}

/** 支持的限制名（顺序即界面展示顺序，与后端 broker.LimitNames 一致） */
export type VHostLimitName = 'max-connections' | 'max-queues'

/** 特性开关（GET /api/feature-flags） */
export interface FeatureFlag {
  name: string
  /** enabled / disabled */
  state: string
  stability: string
  desc: string
  doc_url: string
  provided_by: string
}

/** 弃用特性（GET /api/deprecated-features，只读清单） */
export interface DeprecatedFeature {
  name: string
  /** denied / permitted */
  state: string
  /** denied_by_default / permitted_by_default / removed */
  deprecation_phase: string
  desc: string
  doc_url: string
  provided_by: string
}

/** 插件对象（GET /api/plugins） */
export interface Plugin {
  name: string
  version: string
  /** 插件依赖的插件 API 版本 */
  api_version: string
  /** enabled / disabled / failed / down / stopped */
  state: string
  /** 启动失败会阻塞内核启动（仅官方核心插件） */
  required: boolean
  /** 随内核编译进来；false 表示外部进程（sidecar）插件 */
  builtin: boolean
  capabilities: string[]
  dependencies: string[]
  description: string
  /** 插件自报的运行期状态原因（如外部进程插件 down 的原因）；可为空 */
  runtime_note: string
  /** 插件自带管理界面的地址；为空表示没有可跳转的操作界面 */
  console_url: string
}
