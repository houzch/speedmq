package sidecar

import "time"

// 本文件定义"内核语义桥"的线契约：插件进程通过**反向调用**（插件 → 内核）触达内核的
// plugin.Session（队列 / 交换机 / 绑定 / 发布 / 消费 / 权限），而不只是拿原始字节流。
//
// 边界：本包只依赖标准库，因此这里只出现 JSON DTO 与方法名，**不**引用 pkg/plugin ——
// 类型化的友好封装留给插件自己（示例见 swiftmq-test/test/integration/echosidecar）。
//
// 设计要点：
//   - 每条反向调用都带 stream（由内核在建流时分配），内核据此定位到该连接对应的会话；
//   - 入参/出参是自包含 JSON（消息体走 base64，属性里的 time.Time 走 RFC3339），可无损往返；
//   - 错误的语义分类（kind）随应答一起回传，插件侧可还原成 plugin.Error，而不是压成字符串。

// 反向调用（插件 → 内核）与投递回推（内核 → 插件）的方法名。
const (
	// MethodSessionOpen 打开某条流上的会话：params SessionOpenParams。
	MethodSessionOpen = "session.open"
	// MethodSessionClose 释放某条流上的会话：params SessionCloseParams。
	MethodSessionClose = "session.close"
	// MethodSessionDeclareExchange 声明交换机：params ExchangeDeclareParams。
	MethodSessionDeclareExchange = "session.declare_exchange"
	// MethodSessionDeleteExchange 删除交换机：params DeleteExchangeParams。
	MethodSessionDeleteExchange = "session.delete_exchange"
	// MethodSessionBindExchange 建立交换机绑定：params ExchangeBindParams。
	MethodSessionBindExchange = "session.bind_exchange"
	// MethodSessionUnbindExchange 解除交换机绑定：params ExchangeBindParams。
	MethodSessionUnbindExchange = "session.unbind_exchange"
	// MethodSessionDeclareQueue 声明队列：params QueueDeclareParams → QueueInfoResult。
	MethodSessionDeclareQueue = "session.declare_queue"
	// MethodSessionDeleteQueue 删除队列：params DeleteQueueParams → QueueInfoResult。
	MethodSessionDeleteQueue = "session.delete_queue"
	// MethodSessionBindQueue 建立队列绑定：params QueueBindParams。
	MethodSessionBindQueue = "session.bind_queue"
	// MethodSessionUnbindQueue 解除队列绑定：params QueueBindParams。
	MethodSessionUnbindQueue = "session.unbind_queue"
	// MethodSessionPurgeQueue 清空队列：params PurgeQueueParams → PurgeResult。
	MethodSessionPurgeQueue = "session.purge_queue"
	// MethodSessionPublish 发布消息：params PublishParams → PublishResultDTO。
	MethodSessionPublish = "session.publish"
	// MethodSessionGet 主动拉取：params GetParams → GetResult。
	MethodSessionGet = "session.get"
	// MethodSessionConsume 注册消费者：params ConsumeParams → ConsumeResult。
	MethodSessionConsume = "session.consume"
	// MethodSessionCancel 取消消费者：params CancelParams。
	MethodSessionCancel = "session.cancel"
	// MethodSessionSettle 结算一条投递：params SettleParams。
	MethodSessionSettle = "session.settle"

	// MethodSessionDeliver 是**正向**调用（内核 → 插件）：内核把一条投递回推给插件，
	// 插件处理后经 MethodSessionSettle 结算。params DeliverParams。
	MethodSessionDeliver = "session.deliver"
)

// 结算动作（SettleParams.Action 的取值），与 plugin.SettleAction 一一对应。
const (
	// SettleActionAck 正常确认消费。
	SettleActionAck = "ack"
	// SettleActionRequeue 重新入队。
	SettleActionRequeue = "requeue"
	// SettleActionReject 拒绝（可能进死信）。
	SettleActionReject = "reject"
)

// ---------------------------------------------------------------------------
// 反向调用的参数与结果
// ---------------------------------------------------------------------------

// SessionOpenParams 是 MethodSessionOpen 的参数。
type SessionOpenParams struct {
	Stream uint32 `json:"stream"`
	VHost  string `json:"vhost"`
}

// SessionCloseParams 是 MethodSessionClose 的参数。
type SessionCloseParams struct {
	Stream uint32 `json:"stream"`
}

// SessionParams 是只带流号、无其他参数的方法（如 declare_exchange 的简化形式）的通用形式。
type SessionParams struct {
	Stream uint32 `json:"stream"`
}

// ExchangeDeclareParams 是 MethodSessionDeclareExchange 的参数。
type ExchangeDeclareParams struct {
	Stream     uint32         `json:"stream"`
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Passive    bool           `json:"passive,omitempty"`
	Durable    bool           `json:"durable,omitempty"`
	AutoDelete bool           `json:"auto_delete,omitempty"`
	Internal   bool           `json:"internal,omitempty"`
	Arguments  map[string]any `json:"arguments,omitempty"`
}

// DeleteExchangeParams 是 MethodSessionDeleteExchange 的参数。
type DeleteExchangeParams struct {
	Stream   uint32 `json:"stream"`
	Name     string `json:"name"`
	IfUnused bool   `json:"if_unused,omitempty"`
}

// ExchangeBindParams 是 MethodSessionBindExchange / MethodSessionUnbindExchange 的参数。
type ExchangeBindParams struct {
	Stream      uint32         `json:"stream"`
	Destination string         `json:"destination"`
	Source      string         `json:"source"`
	RoutingKey  string         `json:"routing_key,omitempty"`
	Arguments   map[string]any `json:"arguments,omitempty"`
}

// QueueDeclareParams 是 MethodSessionDeclareQueue 的参数。
type QueueDeclareParams struct {
	Stream     uint32         `json:"stream"`
	Name       string         `json:"name,omitempty"`
	Passive    bool           `json:"passive,omitempty"`
	Durable    bool           `json:"durable,omitempty"`
	Exclusive  bool           `json:"exclusive,omitempty"`
	AutoDelete bool           `json:"auto_delete,omitempty"`
	Arguments  map[string]any `json:"arguments,omitempty"`
}

// QueueInfoResult 是声明/删除队列的结果。
type QueueInfoResult struct {
	Name          string `json:"name"`
	MessageCount  uint32 `json:"message_count"`
	ConsumerCount uint32 `json:"consumer_count"`
}

// DeleteQueueParams 是 MethodSessionDeleteQueue 的参数。
type DeleteQueueParams struct {
	Stream   uint32 `json:"stream"`
	Name     string `json:"name"`
	IfUnused bool   `json:"if_unused,omitempty"`
	IfEmpty  bool   `json:"if_empty,omitempty"`
}

// QueueBindParams 是 MethodSessionBindQueue / MethodSessionUnbindQueue 的参数。
type QueueBindParams struct {
	Stream     uint32         `json:"stream"`
	Queue      string         `json:"queue"`
	Exchange   string         `json:"exchange"`
	RoutingKey string         `json:"routing_key,omitempty"`
	Arguments  map[string]any `json:"arguments,omitempty"`
}

// PurgeQueueParams 是 MethodSessionPurgeQueue 的参数。
type PurgeQueueParams struct {
	Stream uint32 `json:"stream"`
	Name   string `json:"name"`
}

// PurgeResult 是清空队列的结果。
type PurgeResult struct {
	Count uint32 `json:"count"`
}

// PublishParams 是 MethodSessionPublish 的参数。
type PublishParams struct {
	Stream     uint32     `json:"stream"`
	Exchange   string     `json:"exchange,omitempty"`
	RoutingKey string     `json:"routing_key,omitempty"`
	Mandatory  bool       `json:"mandatory,omitempty"`
	Message    MessageDTO `json:"message"`
}

// PublishResultDTO 是发布的结果。
//
// Durable 不在这里：内核在应答之前就把持久化等待（PublishResult.Durable）做完，
// 于是"调用返回"即等于"已按 fsync 档位落盘"，与协议插件内的语义一致。
type PublishResultDTO struct {
	Routed   bool `json:"routed"`
	Rejected bool `json:"rejected"`
}

// GetParams 是 MethodSessionGet 的参数。
type GetParams struct {
	Stream uint32 `json:"stream"`
	Queue  string `json:"queue"`
	NoAck  bool   `json:"no_ack,omitempty"`
}

// GetResult 是主动拉取的结果；Found 为 false 表示队列为空。
type GetResult struct {
	Found    bool         `json:"found"`
	Delivery *DeliveryDTO `json:"delivery,omitempty"`
}

// ConsumeParams 是 MethodSessionConsume 的参数（需要结算的投递随后经 MethodSessionDeliver 回推）。
type ConsumeParams struct {
	Stream    uint32 `json:"stream"`
	Queue     string `json:"queue"`
	Tag       string `json:"tag,omitempty"`
	NoAck     bool   `json:"no_ack,omitempty"`
	Exclusive bool   `json:"exclusive,omitempty"`
	Prefetch  uint16 `json:"prefetch,omitempty"`
}

// ConsumeResult 是注册消费者的结果（Tag 为内核最终使用的标签）。
type ConsumeResult struct {
	Tag string `json:"tag"`
}

// CancelParams 是 MethodSessionCancel 的参数。
type CancelParams struct {
	Stream uint32 `json:"stream"`
	Tag    string `json:"tag"`
}

// SettleParams 是 MethodSessionSettle 的参数。
//
// 只有 DeliveryID 没有 stream：投递编号由内核在整条连接上统一分配，全局唯一，
// 因此结算不必再带流号（这也让 session.settle 的契约更小）。
type SettleParams struct {
	DeliveryID uint64 `json:"delivery_id"`
	Action     string `json:"action"`
}

// DeliverParams 是 MethodSessionDeliver（正向）的参数：内核把一条投递回推给插件。
type DeliverParams struct {
	Stream      uint32     `json:"stream"`
	DeliveryID  uint64     `json:"delivery_id"`
	Queue       string     `json:"queue"`
	ConsumerTag string     `json:"consumer_tag,omitempty"`
	Redelivered bool       `json:"redelivered,omitempty"`
	Message     MessageDTO `json:"message"`
}

// ---------------------------------------------------------------------------
// 消息与投递 DTO（自包含，可无损往返）
// ---------------------------------------------------------------------------

// MessageDTO 是 plugin.Message 的线表达。
type MessageDTO struct {
	Exchange    string        `json:"exchange,omitempty"`
	RoutingKey  string        `json:"routing_key,omitempty"`
	Properties  PropertiesDTO `json:"properties"`
	Body        []byte        `json:"body,omitempty"` // JSON 里是 base64
	Redelivered bool          `json:"redelivered,omitempty"`
}

// PropertiesDTO 是 plugin.Properties 的线表达。时间走 RFC3339，头走 JSON 对象。
type PropertiesDTO struct {
	ContentType     string         `json:"content_type,omitempty"`
	ContentEncoding string         `json:"content_encoding,omitempty"`
	Headers         map[string]any `json:"headers,omitempty"`
	DeliveryMode    uint8          `json:"delivery_mode,omitempty"`
	Priority        uint8          `json:"priority,omitempty"`
	CorrelationID   string         `json:"correlation_id,omitempty"`
	ReplyTo         string         `json:"reply_to,omitempty"`
	Expiration      string         `json:"expiration,omitempty"`
	MessageID       string         `json:"message_id,omitempty"`
	Timestamp       time.Time      `json:"timestamp,omitempty"`
	Type            string         `json:"type,omitempty"`
	UserID          string         `json:"user_id,omitempty"`
	AppID           string         `json:"app_id,omitempty"`
}

// DeliveryDTO 是 plugin.Delivery 的线表达。Settle 是内核注入的函数，不进帧；
// 插件结算时用回带的 DeliveryID 调 MethodSessionSettle。
type DeliveryDTO struct {
	Message     MessageDTO `json:"message"`
	Queue       string     `json:"queue"`
	ConsumerTag string     `json:"consumer_tag,omitempty"`
	Redelivered bool       `json:"redelivered,omitempty"`
	DeliveryID  uint64     `json:"delivery_id,omitempty"`
}

// ---------------------------------------------------------------------------
// 错误语义载体
// ---------------------------------------------------------------------------

// ErrorKindCarrier 由"带语义分类的 error"实现。内核侧的反向调用处理器返回这类错误时，
// 客户端会把 Kind 与 Text 一并写进应答，让插件能还原成 plugin.Errorf(kind, ...)，
// 而不是把分类信息丢在 error.Error() 的字符串里。
//
// 之所以由 pkg/sidecar 定义这个接口、而不是直接返回 pkg/plugin.Error：
// 本包只依赖标准库（插件进程也用它），不能引用 pkg/plugin。
type ErrorKindCarrier interface {
	error
	// RPCErrorKind 返回语义分类（数值与 pkg/plugin.ErrorKind 一致）。
	RPCErrorKind() int
}

// RPCError 是跨 RPC 往返的带分类错误，由插件侧 Bridge.Call 还原得到。
type RPCError struct {
	// Kind 是语义分类（对应 pkg/plugin.ErrorKind）。
	Kind int
	// Text 是给运维看的描述（对应 plugin.Error.Text）。
	Text string
}

// Error 实现 error。
func (e *RPCError) Error() string { return e.Text }

// RPCErrorKind 实现 ErrorKindCarrier。
func (e *RPCError) RPCErrorKind() int { return e.Kind }

// ErrorPayload 是应答 Data 里承载错误分类的结构（仅当应答为失败且带分类时出现）。
type ErrorPayload struct {
	// Kind 是语义分类（对应 pkg/plugin.ErrorKind）。
	Kind int `json:"kind"`
	// Text 是描述文本。
	Text string `json:"text"`
}
