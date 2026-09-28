package plugin

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
)

// ListenerSpec 描述协议插件希望内核为其创建的监听。
type ListenerSpec struct {
	// Name 监听名，同一插件内唯一，用于日志与指标区分。
	Name string
	// Addr 形如 ":5672"。
	Addr string
	// TLS 非空时使用 TLS 监听。
	TLS *tls.Config
	// DetectOnly 为 true 时不创建监听，只参与协议嗅探。
	// 用于"复用其他插件监听端口"的场景，例如 AMQP 1.0 与 0-9-1 同端口共存。
	DetectOnly bool
}

// Protocol 是协议插件契约。
//
// 三条硬约束：
//  1. 协议插件不做存储、不做路由：只负责编解码与会话，所有业务动作通过 Core 完成，
//     以此保证 vhost / 权限 / 路由 / 队列语义 / 确认 / DLX / TTL 在所有协议上完全一致；
//  2. 同端口多协议由 Sniff 决定；
//  3. 协议间消息格式转换不由协议插件承担（交给消息处理插件）。
type Protocol interface {
	// Name 协议名，如 "amqp091"。
	Name() string
	// DefaultListeners 返回默认监听，可被配置覆盖。
	DefaultListeners() []ListenerSpec
	// Sniff 判断连接首部是否属于本协议；按注册顺序调用，先匹配者生效。
	Sniff(peek []byte) bool
	// Serve 处理一条已建立（必要时已完成 TLS 握手）的连接，返回即代表连接结束。
	Serve(ctx context.Context, conn net.Conn, core Core) error
}

// Identity 表示认证通过后的调用方身份。
type Identity struct {
	// User 登录用户名。
	User string
	// VHost 该连接最终打开的 vhost（在 Connection.Open 阶段填充）。
	VHost string
}

// AuthFailureKind 是认证失败的语义分类。具体错误码由各协议自行映射，
// 这样内核的认证逻辑不必知道 AMQP 的 403 / 530 等协议细节。
type AuthFailureKind int

const (
	// AuthFailureAccessRefused 凭证不可接受（AMQP 0-9-1 映射为 403 ACCESS_REFUSED）。
	AuthFailureAccessRefused AuthFailureKind = iota
	// AuthFailureMechanismUnsupported 认证机制不支持（AMQP 0-9-1 映射为 530 NOT_ALLOWED）。
	AuthFailureMechanismUnsupported
)

// AuthError 是 Core.Authenticate 失败时返回的错误。
type AuthError struct {
	// Kind 失败分类。
	Kind AuthFailureKind
	// Text 供协议层写入 reply-text 的描述。
	Text string
}

func (e *AuthError) Error() string { return e.Text }

// Notification 是内核下发给连接的事件。
//
// 目前只有资源水位导致的阻塞与解除（对应 Connection.Blocked / Unblocked）。
type Notification struct {
	// Blocked 为 true 表示连接因内存/磁盘水位被阻塞，false 表示解除阻塞。
	Blocked bool
	// Reason 是给运维看的原因描述。
	Reason string
}

// Core 是协议无关的内核操作面：任何协议插件都只能通过这些方法触达内核语义。
//
// 连接级操作用 Core，vhost 作用域内的拓扑与消息操作经 Core.Session 取得 Session。
type Core interface {
	// Logger 返回连接级日志器。
	Logger() *slog.Logger
	// Notifications 返回连接级事件通道。内核在资源水位触发/解除时向连接广播事件，
	// 协议层据此下发 Connection.Blocked / Connection.Unblocked。
	// 通道由内核持有，调用 Close 后不再投递。
	Notifications() <-chan Notification
	// Close 释放连接级资源（通知订阅等）。协议插件在连接结束时必须调用，且必须可重复调用。
	Close()
	// SetConnectionProbe 由协议层注入"连接与通道实时快照"回调，管理面按需拉取。
	// 应在握手完成后尽早调用；可重复调用以替换。
	SetConnectionProbe(probe func() ConnectionInfo)
	// SetDisconnectFunc 由协议层注册"内核要求断开本连接"的回调（管理面强制关闭连接用）。
	// 回调应尽力发出 Connection.Close（reply-code 320 CONNECTION_FORCED）后关闭底层连接。
	SetDisconnectFunc(fn func(reason string))
	// ServerProperties 返回 Connection.Start 下发的 server-properties（含 capabilities）。
	//
	// 注意：capabilities 声明即承诺 —— 客户端会依据它切换代码路径，只能在对应能力真正实现后打开。
	ServerProperties() map[string]any
	// Mechanisms 返回支持的 SASL 机制名，如 ["PLAIN", "AMQPLAIN"]。
	Mechanisms() []string
	// Authenticate 校验 SASL 响应；失败返回 *AuthError（协议侧映射 403 / 530）。
	Authenticate(ctx context.Context, mechanism string, response []byte, remoteAddr net.Addr) (Identity, error)
	// VHostExists 判断 vhost 是否存在；不存在时协议侧返回 402 INVALID_PATH。
	VHostExists(name string) bool
	// DefaultVHost 返回默认 vhost 名。
	DefaultVHost() string
	// Session 返回绑定到指定 vhost 的操作面。
	// vhost 不存在时返回 *Error{Kind: KindInvalidPath}。
	Session(vhost string) (Session, error)
}
