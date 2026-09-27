// Package spec 定义 AMQP 0-9-1 的类/方法标识、错误码与方法级编解码。
//
// M1 只覆盖连接建立与 Channel 开关所需的方法；其余方法在 M2 补齐
// （届时改为由规范文件生成方法表，避免手写数百个方法出错）。
package spec

import "fmt"

// 类标识。
const (
	ClassConnection uint16 = 10
	ClassChannel    uint16 = 20
	ClassExchange   uint16 = 40
	ClassQueue      uint16 = 50
	ClassBasic      uint16 = 60
	ClassConfirm    uint16 = 85
	ClassTx         uint16 = 90
)

// Connection (10) 方法标识。
const (
	MethodConnectionStart     uint16 = 10
	MethodConnectionStartOk   uint16 = 11
	MethodConnectionSecure    uint16 = 20
	MethodConnectionSecureOk  uint16 = 21
	MethodConnectionTune      uint16 = 30
	MethodConnectionTuneOk    uint16 = 31
	MethodConnectionOpen      uint16 = 40
	MethodConnectionOpenOk    uint16 = 41
	MethodConnectionClose     uint16 = 50
	MethodConnectionCloseOk   uint16 = 51
	MethodConnectionBlocked   uint16 = 60
	MethodConnectionUnblocked uint16 = 61
)

// Channel (20) 方法标识。
const (
	MethodChannelOpen    uint16 = 10
	MethodChannelOpenOk  uint16 = 11
	MethodChannelFlow    uint16 = 20
	MethodChannelFlowOk  uint16 = 21
	MethodChannelClose   uint16 = 40
	MethodChannelCloseOk uint16 = 41
)

// Exchange (40) 方法标识。注意 Exchange.Unbind-Ok 的编号是 51（规范如此，不是笔误）。
const (
	MethodExchangeDeclare   uint16 = 10
	MethodExchangeDeclareOk uint16 = 11
	MethodExchangeDelete    uint16 = 20
	MethodExchangeDeleteOk  uint16 = 21
	MethodExchangeBind      uint16 = 30
	MethodExchangeBindOk    uint16 = 31
	MethodExchangeUnbind    uint16 = 40
	MethodExchangeUnbindOk  uint16 = 51
)

// Queue (50) 方法标识。
const (
	MethodQueueDeclare   uint16 = 10
	MethodQueueDeclareOk uint16 = 11
	MethodQueueBind      uint16 = 20
	MethodQueueBindOk    uint16 = 21
	MethodQueuePurge     uint16 = 30
	MethodQueuePurgeOk   uint16 = 31
	MethodQueueDelete    uint16 = 40
	MethodQueueDeleteOk  uint16 = 41
	MethodQueueUnbind    uint16 = 50
	MethodQueueUnbindOk  uint16 = 51
)

// Basic (60) 方法标识。
const (
	MethodBasicQos          uint16 = 10
	MethodBasicQosOk        uint16 = 11
	MethodBasicConsume      uint16 = 20
	MethodBasicConsumeOk    uint16 = 21
	MethodBasicCancel       uint16 = 30
	MethodBasicCancelOk     uint16 = 31
	MethodBasicPublish      uint16 = 40
	MethodBasicReturn       uint16 = 50
	MethodBasicDeliver      uint16 = 60
	MethodBasicGet          uint16 = 70
	MethodBasicGetOk        uint16 = 71
	MethodBasicGetEmpty     uint16 = 72
	MethodBasicAck          uint16 = 80
	MethodBasicReject       uint16 = 90
	MethodBasicRecoverAsync uint16 = 100
	MethodBasicRecover      uint16 = 110
	MethodBasicRecoverOk    uint16 = 111
	MethodBasicNack         uint16 = 120
)

// 错误码。这些值决定客户端"关 Channel 还是关连接"，属兼容性高危区，不可随意改动。
const (
	ReplySuccess       uint16 = 200
	ContentTooLarge    uint16 = 311
	NoRoute            uint16 = 312
	NoConsumers        uint16 = 313
	ConnectionForced   uint16 = 320
	InvalidPath        uint16 = 402
	AccessRefused      uint16 = 403
	NotFound           uint16 = 404
	ResourceLocked     uint16 = 405
	PreconditionFailed uint16 = 406
	FrameError         uint16 = 501
	SyntaxError        uint16 = 502
	CommandInvalid     uint16 = 503
	ChannelError       uint16 = 504
	UnexpectedFrame    uint16 = 505
	ResourceError      uint16 = 506
	NotAllowed         uint16 = 530
	NotImplemented     uint16 = 540
	InternalError      uint16 = 541
)

// Scope 表示错误的作用域：软错误关 Channel，硬错误关 Connection。
//
// 客户端据此决定"重建 Channel"还是"整条连接重连"，判断错误后果严重，必须严格对齐。
type Scope int

const (
	// ScopeChannel 软错误：只关闭出错的 Channel。
	ScopeChannel Scope = iota
	// ScopeConnection 硬错误：关闭整条连接。
	ScopeConnection
)

// ScopeOf 返回错误码对应的作用域。
func ScopeOf(code uint16) Scope {
	switch code {
	case ContentTooLarge, NoRoute, NoConsumers, AccessRefused,
		NotFound, ResourceLocked, PreconditionFailed:
		return ScopeChannel
	default:
		return ScopeConnection
	}
}

// Reply 是 connection.close / channel.close 的载荷。
type Reply struct {
	Code     uint16
	Text     string
	ClassID  uint16
	MethodID uint16
}

// Error 让 Reply 可用于 error 接口。
func (r Reply) Error() string {
	return fmt.Sprintf("%d %s (class=%d method=%d)", r.Code, r.Text, r.ClassID, r.MethodID)
}
