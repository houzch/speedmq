package plugin

import (
	"fmt"
	"time"
)

// 本文件定义协议无关的会话操作面。协议插件只负责把各自协议的帧翻译成这里的调用，
// 因此 vhost、权限、路由、队列语义、确认语义在所有协议上必然一致。
//
// 重要约束：pkg/plugin 只依赖标准库。第三方插件是独立 module，
// 无法 import 本项目的 internal/ 包，所以这里的类型不得引用内核内部类型。

// ExchangeType 是交换机类型。
type ExchangeType string

const (
	// ExchangeDirect 按 routing key 精确匹配。
	ExchangeDirect ExchangeType = "direct"
	// ExchangeFanout 广播到所有绑定队列，忽略 routing key。
	ExchangeFanout ExchangeType = "fanout"
	// ExchangeTopic 按 "." 分词的通配匹配，支持 "*"（单段）与 "#"（多段）。
	ExchangeTopic ExchangeType = "topic"
	// ExchangeHeaders 按消息头匹配（M2 仅登记类型，匹配在 M3 实现）。
	ExchangeHeaders ExchangeType = "headers"
)

// Properties 是消息属性的语义视图（与具体协议的字段布局无关）。
type Properties struct {
	ContentType     string
	ContentEncoding string
	// Headers 是消息头（AMQP field-table 的语义等价物）。
	Headers map[string]any
	// DeliveryMode 为 2 表示持久化消息，1 表示瞬时消息。
	DeliveryMode  uint8
	Priority      uint8
	CorrelationID string
	ReplyTo       string
	// Expiration 是毫秒为单位的过期时间字符串（AMQP 0-9-1 用字符串承载）。
	Expiration string
	MessageID  string
	Timestamp  time.Time
	Type       string
	UserID     string
	AppID      string
}

// Persistent 表示该消息是否要求持久化（delivery-mode=2）。
func (p Properties) Persistent() bool { return p.DeliveryMode == 2 }

// Message 是内核持有的一条消息。
type Message struct {
	// Exchange 是发布时指定的交换机（用于死信等场景回溯来源）。
	Exchange string
	// RoutingKey 是发布时使用的路由键。
	RoutingKey string
	// Properties 是消息属性。
	Properties Properties
	// Body 是消息体。
	Body []byte
	// Redelivered 表示该消息曾被投递过（重新投递时应置为 true）。
	Redelivered bool
}

// Delivery 是内核交给协议层的一条投递。
//
// 生命周期约定：协议层收到 Delivery 后必须恰好调用一次 Settle，
// 或在连接/通道断开时由内核自动重新入队。
type Delivery struct {
	// Message 是消息本体。
	Message *Message
	// Queue 是投递来源队列。
	Queue string
	// ConsumerTag 是投递给哪个消费者（basic.get 拉取的投递为空）。
	ConsumerTag string
	// Redelivered 与 Message.Redelivered 一致，单独暴露便于协议层直接写入帧。
	Redelivered bool

	// Settle 由内核注入：requeue=false 表示确认并丢弃，true 表示重新入队。
	// 协议层不要自行构造 Delivery —— 该字段为空时调用会被忽略。
	Settle func(requeue bool)
}

// Subscription 是内核与协议层之间的消费者契约。
type Subscription struct {
	// Tag 是消费者标签。
	Tag string
	// Queue 是队列名。
	Queue string
	// NoAck 为 true 时无需确认，投递即视为已结算。
	NoAck bool
	// Exclusive 为 true 时该消费者独占队列，其他消费者不得再挂到同一队列。
	Exclusive bool
	// Prefetch 是该消费者的 in-flight 上限；0 表示不限制。
	Prefetch uint16
	// Deliver 由内核调用以投递一条消息。返回错误表示投递失败（连接已断），
	// 内核会据此把消息重新入队。
	Deliver func(*Delivery) error
	// Cancel 由内核调用以通知协议层该消费者已被取消（例如队列被删除）。
	Cancel func(reason string)
}

// ExchangeDeclare 是交换机声明请求。
type ExchangeDeclare struct {
	// Name 为空表示默认交换机；默认交换机不可显式声明。
	Name       string
	Type       ExchangeType
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	Arguments  map[string]any
}

// QueueDeclare 是队列声明请求。
type QueueDeclare struct {
	// Name 为空时由服务端生成队列名。
	Name       string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	Arguments  map[string]any
}

// QueueInfo 是队列声明或删除的结果。
type QueueInfo struct {
	// Name 是队列名（服务端生成时与请求中的空名不同）。
	Name string
	// MessageCount 是队列中的就绪消息数。
	MessageCount uint32
	// ConsumerCount 是队列上的消费者数。
	ConsumerCount uint32
}

// ErrorKind 是内核错误的语义分类。具体错误码由协议层映射
// （AMQP 0-9-1：404 / 406 / 403 / 405 / 402 / 540 / 541），
// 这样内核不必知道任何协议的编码方式。
type ErrorKind int

const (
	// KindNotFound 对象不存在。AMQP 0-9-1 → 404 NOT_FOUND（关 Channel）。
	KindNotFound ErrorKind = iota
	// KindPreconditionFailed 声明参数与已存在对象不一致。→ 406 PRECONDITION_FAILED（关 Channel）。
	KindPreconditionFailed
	// KindAccessRefused 权限不足或使用了保留名。→ 403 ACCESS_REFUSED（关 Channel）。
	KindAccessRefused
	// KindResourceLocked 独占资源被其他连接占用。→ 405 RESOURCE_LOCKED（关 Channel）。
	KindResourceLocked
	// KindInvalidPath vhost 不存在。→ 402 INVALID_PATH（关连接）。
	KindInvalidPath
	// KindNotImplemented 能力尚未实现。→ 540 NOT_IMPLEMENTED（关连接）。
	KindNotImplemented
	// KindInternal 内核内部错误。→ 541 INTERNAL_ERROR（关连接）。
	KindInternal
)

// Error 是内核返回的带语义分类的错误。
type Error struct {
	// Kind 是错误分类。
	Kind ErrorKind
	// Text 是给运维看的描述，协议层会把它写入 reply-text。
	Text string
}

func (e *Error) Error() string { return e.Text }

// Errorf 构造一个带分类的错误。
func Errorf(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Text: fmt.Sprintf(format, args...)}
}

// Session 是绑定到某个 vhost 的协议无关操作面。
//
// 返回错误时一律是 *Error；协议层据 Kind 映射错误码与作用域（软错误关 Channel，硬错误关连接）。
type Session interface {
	// ---------- 交换机 ----------

	// DeclareExchange 声明交换机；被动声明不存在时返回 KindNotFound。
	DeclareExchange(req ExchangeDeclare) error
	// DeleteExchange 删除交换机；删除默认交换机返回 KindAccessRefused。
	DeleteExchange(name string, ifUnused bool) error
	// BindExchange 建立交换机到交换机的绑定。
	BindExchange(destination, source, routingKey string, arguments map[string]any) error
	// UnbindExchange 解除交换机到交换机的绑定。
	UnbindExchange(destination, source, routingKey string, arguments map[string]any) error

	// ---------- 队列 ----------

	// DeclareQueue 声明队列；Name 为空时服务端生成名字。
	DeclareQueue(req QueueDeclare) (QueueInfo, error)
	// DeleteQueue 删除队列并返回删除前的统计信息。
	DeleteQueue(name string, ifUnused, ifEmpty bool) (QueueInfo, error)
	// BindQueue 建立队列到交换机的绑定。
	BindQueue(queue, exchange, routingKey string, arguments map[string]any) error
	// UnbindQueue 解除队列到交换机的绑定。
	UnbindQueue(queue, exchange, routingKey string, arguments map[string]any) error
	// PurgeQueue 清空队列中的就绪消息，返回被清除的条数（不含未确认消息）。
	PurgeQueue(name string) (uint32, error)

	// ---------- 发布 ----------

	// Publish 把消息投递到交换机；返回是否至少命中一个队列。
	// 未命中时由协议层按 mandatory 标志决定是否回 Basic.Return。
	Publish(msg *Message, exchange, routingKey string, mandatory bool) (routed bool, err error)

	// ---------- 消费 ----------

	// Consume 注册一个消费者。Tag 为空时由内核生成，返回值为最终使用的标签。
	Consume(sub Subscription) (tag string, err error)
	// Cancel 取消指定消费者；不存在时返回 KindNotFound。
	Cancel(consumerTag string) error
	// Get 从队列主动拉取一条消息；第二个返回值为 false 表示队列为空。
	// noAck 为 true 时该投递无需确认。
	Get(queue string, noAck bool) (delivery *Delivery, ok bool, err error)

	// ---------- 生命周期 ----------

	// Close 释放该会话持有的资源：取消它的全部消费者、删除它的独占队列。
	// 连接结束时协议插件必须调用，且必须可重复调用。
	Close()
}
