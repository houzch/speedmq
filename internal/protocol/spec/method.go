package spec

import (
	"fmt"

	"github.com/houzch/swiftmq/internal/protocol/codec"
)

// Method 是方法帧的解码结果。方法帧 payload = class-id(2) + method-id(2) + 参数区。
type Method struct {
	// ClassID 类标识。
	ClassID uint16
	// MethodID 方法标识。
	MethodID uint16
	// Args 参数区（按方法定义自行解码）。
	Args []byte
}

// DecodeMethod 解析方法帧的 payload。
func DecodeMethod(payload []byte) (Method, error) {
	d := codec.NewDecoder(payload)
	classID, err := d.Short()
	if err != nil {
		return Method{}, err
	}
	methodID, err := d.Short()
	if err != nil {
		return Method{}, err
	}
	return Method{ClassID: classID, MethodID: methodID, Args: d.Rest()}, nil
}

// Encode 把方法组装成帧 payload（class-id + method-id + 参数区）。
func (m Method) Encode() []byte {
	buf := make([]byte, 4, 4+len(m.Args))
	buf[0] = byte(m.ClassID >> 8)
	buf[1] = byte(m.ClassID)
	buf[2] = byte(m.MethodID >> 8)
	buf[3] = byte(m.MethodID)
	return append(buf, m.Args...)
}

// EncodeMethod 直接按类/方法标识拼装方法帧 payload。
func EncodeMethod(classID, methodID uint16, args []byte) []byte {
	return Method{ClassID: classID, MethodID: methodID, Args: args}.Encode()
}

// methodNames 是方法名表，仅用于日志与错误信息，不参与协议行为。
// 名称覆盖全部类（含 M1 尚未实现的方法），以便"未实现"的报错一目了然。
var methodNames = map[uint16]map[uint16]string{
	ClassConnection: {
		MethodConnectionStart:     "Connection.Start",
		MethodConnectionStartOk:   "Connection.Start-Ok",
		MethodConnectionSecure:    "Connection.Secure",
		MethodConnectionSecureOk:  "Connection.Secure-Ok",
		MethodConnectionTune:      "Connection.Tune",
		MethodConnectionTuneOk:    "Connection.Tune-Ok",
		MethodConnectionOpen:      "Connection.Open",
		MethodConnectionOpenOk:    "Connection.Open-Ok",
		MethodConnectionClose:     "Connection.Close",
		MethodConnectionCloseOk:   "Connection.Close-Ok",
		MethodConnectionBlocked:   "Connection.Blocked",
		MethodConnectionUnblocked: "Connection.Unblocked",
	},
	ClassChannel: {
		MethodChannelOpen:    "Channel.Open",
		MethodChannelOpenOk:  "Channel.Open-Ok",
		MethodChannelFlow:    "Channel.Flow",
		MethodChannelFlowOk:  "Channel.Flow-Ok",
		MethodChannelClose:   "Channel.Close",
		MethodChannelCloseOk: "Channel.Close-Ok",
	},
	ClassExchange: {
		MethodExchangeDeclare:   "Exchange.Declare",
		MethodExchangeDeclareOk: "Exchange.Declare-Ok",
		MethodExchangeDelete:    "Exchange.Delete",
		MethodExchangeDeleteOk:  "Exchange.Delete-Ok",
		MethodExchangeBind:      "Exchange.Bind",
		MethodExchangeBindOk:    "Exchange.Bind-Ok",
		MethodExchangeUnbind:    "Exchange.Unbind",
		MethodExchangeUnbindOk:  "Exchange.Unbind-Ok",
	},
	ClassQueue: {
		MethodQueueDeclare:   "Queue.Declare",
		MethodQueueDeclareOk: "Queue.Declare-Ok",
		MethodQueueBind:      "Queue.Bind",
		MethodQueueBindOk:    "Queue.Bind-Ok",
		MethodQueuePurge:     "Queue.Purge",
		MethodQueuePurgeOk:   "Queue.Purge-Ok",
		MethodQueueDelete:    "Queue.Delete",
		MethodQueueDeleteOk:  "Queue.Delete-Ok",
		MethodQueueUnbind:    "Queue.Unbind",
		MethodQueueUnbindOk:  "Queue.Unbind-Ok",
	},
	ClassBasic: {
		MethodBasicQos:          "Basic.Qos",
		MethodBasicQosOk:        "Basic.Qos-Ok",
		MethodBasicConsume:      "Basic.Consume",
		MethodBasicConsumeOk:    "Basic.Consume-Ok",
		MethodBasicCancel:       "Basic.Cancel",
		MethodBasicCancelOk:     "Basic.Cancel-Ok",
		MethodBasicPublish:      "Basic.Publish",
		MethodBasicReturn:       "Basic.Return",
		MethodBasicDeliver:      "Basic.Deliver",
		MethodBasicGet:          "Basic.Get",
		MethodBasicGetOk:        "Basic.Get-Ok",
		MethodBasicGetEmpty:     "Basic.Get-Empty",
		MethodBasicAck:          "Basic.Ack",
		MethodBasicReject:       "Basic.Reject",
		MethodBasicRecoverAsync: "Basic.Recover-Async",
		MethodBasicRecover:      "Basic.Recover",
		MethodBasicRecoverOk:    "Basic.Recover-Ok",
		MethodBasicNack:         "Basic.Nack",
	},
	ClassConfirm: {
		10: "Confirm.Select", 11: "Confirm.Select-Ok",
	},
	ClassTx: {
		10: "Tx.Select", 11: "Tx.Select-Ok",
		20: "Tx.Commit", 21: "Tx.Commit-Ok",
		30: "Tx.Rollback", 31: "Tx.Rollback-Ok",
	},
}

// Name 返回可读的方法名。
func (m Method) Name() string {
	if byMethod, ok := methodNames[m.ClassID]; ok {
		if n, ok := byMethod[m.MethodID]; ok {
			return n
		}
	}
	return fmt.Sprintf("class %d method %d", m.ClassID, m.MethodID)
}

// ---------------------------------------------------------------------------
// Connection (10)
// ---------------------------------------------------------------------------

// EncodeConnectionStart 构造 Connection.Start 的参数区。
func EncodeConnectionStart(versionMajor, versionMinor uint8, serverProps codec.Table, mechanisms, locales string) ([]byte, error) {
	e := codec.NewEncoder()
	e.Octet(versionMajor)
	e.Octet(versionMinor)
	if err := e.Table(serverProps); err != nil {
		return nil, fmt.Errorf("编码 server-properties 失败: %w", err)
	}
	e.LongStr([]byte(mechanisms))
	e.LongStr([]byte(locales))
	return e.Bytes(), nil
}

// ConnectionStartOk 是 Connection.Start-Ok 的载荷。
type ConnectionStartOk struct {
	// ClientProperties 客户端属性表（必须能正确跳过，否则后续字段全错位）。
	ClientProperties codec.Table
	// Mechanism SASL 机制名。
	Mechanism string
	// Response SASL 响应原文。
	Response []byte
	// Locale 客户端 locale。
	Locale string
}

// DecodeConnectionStartOk 解析 Connection.Start-Ok 的参数区。
func DecodeConnectionStartOk(args []byte) (ConnectionStartOk, error) {
	d := codec.NewDecoder(args)
	props, err := d.Table()
	if err != nil {
		return ConnectionStartOk{}, fmt.Errorf("解析 client-properties 失败: %w", err)
	}
	mechanism, err := d.ShortStr()
	if err != nil {
		return ConnectionStartOk{}, err
	}
	response, err := d.LongStr()
	if err != nil {
		return ConnectionStartOk{}, err
	}
	locale, err := d.ShortStr()
	if err != nil {
		return ConnectionStartOk{}, err
	}
	return ConnectionStartOk{ClientProperties: props, Mechanism: mechanism, Response: response, Locale: locale}, nil
}

// EncodeConnectionTune 构造 Connection.Tune 的参数区。
func EncodeConnectionTune(channelMax uint16, frameMax uint32, heartbeat uint16) []byte {
	e := codec.NewEncoder()
	e.Short(channelMax)
	e.Long(frameMax)
	e.Short(heartbeat)
	return e.Bytes()
}

// ConnectionTuneOk 是 Connection.Tune-Ok 的载荷。
type ConnectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

// DecodeConnectionTuneOk 解析 Connection.Tune-Ok 的参数区。
func DecodeConnectionTuneOk(args []byte) (ConnectionTuneOk, error) {
	d := codec.NewDecoder(args)
	channelMax, err := d.Short()
	if err != nil {
		return ConnectionTuneOk{}, err
	}
	frameMax, err := d.Long()
	if err != nil {
		return ConnectionTuneOk{}, err
	}
	heartbeat, err := d.Short()
	if err != nil {
		return ConnectionTuneOk{}, err
	}
	return ConnectionTuneOk{ChannelMax: channelMax, FrameMax: frameMax, Heartbeat: heartbeat}, nil
}

// DecodeConnectionOpen 解析 Connection.Open 的参数区，只取 virtual-host。
func DecodeConnectionOpen(args []byte) (string, error) {
	d := codec.NewDecoder(args)
	vhost, err := d.ShortStr()
	if err != nil {
		return "", err
	}
	if _, err := d.ShortStr(); err != nil { // reserved-1
		return "", err
	}
	// reserved-2 是 bit 字段，占 1 个 bit；本实现只校验其存在。
	bits := codec.NewBitReader(d)
	if _, err := bits.Bit(); err != nil {
		return "", err
	}
	return vhost, nil
}

// EncodeConnectionOpenOk 构造 Connection.Open-Ok 的参数区（reserved-1 为空 shortstr）。
func EncodeConnectionOpenOk() []byte {
	e := codec.NewEncoder()
	_ = e.ShortStr("")
	return e.Bytes()
}

// EncodeConnectionClose 构造 Connection.Close 的参数区。
func EncodeConnectionClose(r Reply) ([]byte, error) {
	e := codec.NewEncoder()
	e.Short(r.Code)
	if err := e.ShortStr(r.Text); err != nil {
		return nil, err
	}
	e.Short(r.ClassID)
	e.Short(r.MethodID)
	return e.Bytes(), nil
}

// DecodeConnectionClose 解析 Connection.Close 的参数区。
func DecodeConnectionClose(args []byte) (Reply, error) {
	return decodeReply(args)
}

// EncodeConnectionCloseOk 构造 Connection.Close-Ok 的参数区（无字段）。
func EncodeConnectionCloseOk() []byte { return nil }

// ---------------------------------------------------------------------------
// Channel (20)
// ---------------------------------------------------------------------------

// DecodeChannelOpen 解析 Channel.Open 的参数区。
func DecodeChannelOpen(args []byte) error {
	d := codec.NewDecoder(args)
	_, err := d.ShortStr() // reserved-1
	return err
}

// EncodeChannelOpenOk 构造 Channel.Open-Ok 的参数区（reserved-1 为空 longstr）。
func EncodeChannelOpenOk() []byte {
	e := codec.NewEncoder()
	e.LongStr(nil)
	return e.Bytes()
}

// DecodeChannelFlow 解析 Channel.Flow 的参数区。
func DecodeChannelFlow(args []byte) (bool, error) {
	d := codec.NewDecoder(args)
	return codec.NewBitReader(d).Bit()
}

// EncodeChannelFlowOk 构造 Channel.Flow-Ok 的参数区。
func EncodeChannelFlowOk(active bool) []byte {
	e := codec.NewEncoder()
	w := codec.NewBitWriter(e)
	w.Bit(active)
	w.Flush()
	return e.Bytes()
}

// DecodeChannelClose 解析 Channel.Close 的参数区。
func DecodeChannelClose(args []byte) (Reply, error) { return decodeReply(args) }

// EncodeChannelClose 构造 Channel.Close 的参数区。
func EncodeChannelClose(r Reply) ([]byte, error) { return EncodeConnectionClose(r) }

// EncodeChannelCloseOk 构造 Channel.Close-Ok 的参数区（无字段）。
func EncodeChannelCloseOk() []byte { return nil }

func decodeReply(args []byte) (Reply, error) {
	d := codec.NewDecoder(args)
	code, err := d.Short()
	if err != nil {
		return Reply{}, err
	}
	text, err := d.ShortStr()
	if err != nil {
		return Reply{}, err
	}
	classID, err := d.Short()
	if err != nil {
		return Reply{}, err
	}
	methodID, err := d.Short()
	if err != nil {
		return Reply{}, err
	}
	return Reply{Code: code, Text: text, ClassID: classID, MethodID: methodID}, nil
}
