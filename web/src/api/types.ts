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
export interface Whoami {
  name: string
  tags: string
  auth_backend: string
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
