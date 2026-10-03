package spec

import (
	"github.com/houzch/swiftmq/internal/protocol/codec"
)

// 本文件覆盖 M2 所需的 Exchange / Queue / Basic 方法编解码。
//
// 字段顺序与 bit 分组严格照 AMQP 0-9-1 规范；注意各方法的 ticket(short) 保留字段
// 并不是所有方法都有（basic.qos / cancel / ack / nack / reject / recover 没有）。

// ---------------------------------------------------------------------------
// Exchange (40)
// ---------------------------------------------------------------------------

// ExchangeDeclare 是 Exchange.Declare 的参数区。
type ExchangeDeclare struct {
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  codec.Table
}

// DecodeExchangeDeclare 解析 Exchange.Declare 的参数区。
func DecodeExchangeDeclare(args []byte) (ExchangeDeclare, error) {
	d := codec.NewDecoder(args)
	var m ExchangeDeclare
	if _, err := d.Short(); err != nil { // ticket（保留）
		return m, err
	}
	var err error
	if m.Exchange, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.Type, err = d.ShortStr(); err != nil {
		return m, err
	}
	bits := codec.NewBitReader(d)
	if m.Passive, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Durable, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.AutoDelete, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Internal, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.NoWait, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Arguments, err = d.Table(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeExchangeDeclareOk 构造 Exchange.Declare-Ok 的参数区（无字段）。
func EncodeExchangeDeclareOk() []byte { return nil }

// ExchangeDelete 是 Exchange.Delete 的参数区。
type ExchangeDelete struct {
	Exchange string
	IfUnused bool
	NoWait   bool
}

// DecodeExchangeDelete 解析 Exchange.Delete 的参数区。
func DecodeExchangeDelete(args []byte) (ExchangeDelete, error) {
	d := codec.NewDecoder(args)
	var m ExchangeDelete
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Exchange, err = d.ShortStr(); err != nil {
		return m, err
	}
	bits := codec.NewBitReader(d)
	if m.IfUnused, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.NoWait, err = bits.Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeExchangeDeleteOk 构造 Exchange.Delete-Ok 的参数区（无字段）。
func EncodeExchangeDeleteOk() []byte { return nil }

// ExchangeBind 是 Exchange.Bind / Exchange.Unbind 的参数区（两者字段相同）。
type ExchangeBind struct {
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   codec.Table
}

// DecodeExchangeBind 解析 Exchange.Bind 的参数区。
func DecodeExchangeBind(args []byte) (ExchangeBind, error) {
	d := codec.NewDecoder(args)
	var m ExchangeBind
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Destination, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.Source, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.RoutingKey, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.NoWait, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	if m.Arguments, err = d.Table(); err != nil {
		return m, err
	}
	return m, nil
}

// DecodeExchangeUnbind 解析 Exchange.Unbind 的参数区（与 Bind 字段相同）。
func DecodeExchangeUnbind(args []byte) (ExchangeBind, error) { return DecodeExchangeBind(args) }

// EncodeExchangeBindOk 构造 Exchange.Bind-Ok 的参数区（无字段）。
func EncodeExchangeBindOk() []byte { return nil }

// EncodeExchangeUnbindOk 构造 Exchange.Unbind-Ok 的参数区（无字段）。
func EncodeExchangeUnbindOk() []byte { return nil }

// ---------------------------------------------------------------------------
// Queue (50)
// ---------------------------------------------------------------------------

// QueueDeclare 是 Queue.Declare 的参数区。
type QueueDeclare struct {
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  codec.Table
}

// DecodeQueueDeclare 解析 Queue.Declare 的参数区。
func DecodeQueueDeclare(args []byte) (QueueDeclare, error) {
	d := codec.NewDecoder(args)
	var m QueueDeclare
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Queue, err = d.ShortStr(); err != nil {
		return m, err
	}
	bits := codec.NewBitReader(d)
	if m.Passive, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Durable, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Exclusive, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.AutoDelete, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.NoWait, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Arguments, err = d.Table(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeQueueDeclareOk 构造 Queue.Declare-Ok 的参数区。
func EncodeQueueDeclareOk(queue string, messageCount, consumerCount uint32) ([]byte, error) {
	e := codec.NewEncoder()
	if err := e.ShortStr(queue); err != nil {
		return nil, err
	}
	e.Long(messageCount)
	e.Long(consumerCount)
	return e.Bytes(), nil
}

// QueueBind 是 Queue.Bind 的参数区。
type QueueBind struct {
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  codec.Table
}

// DecodeQueueBind 解析 Queue.Bind 的参数区。
func DecodeQueueBind(args []byte) (QueueBind, error) {
	d := codec.NewDecoder(args)
	var m QueueBind
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Queue, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.Exchange, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.RoutingKey, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.NoWait, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	if m.Arguments, err = d.Table(); err != nil {
		return m, err
	}
	return m, nil
}

// DecodeQueueUnbind 解析 Queue.Unbind 的参数区。
// 注意：Queue.Unbind 没有 no-wait 字段，这是规范里容易写错的一处。
func DecodeQueueUnbind(args []byte) (QueueBind, error) {
	d := codec.NewDecoder(args)
	var m QueueBind
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Queue, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.Exchange, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.RoutingKey, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.Arguments, err = d.Table(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeQueueBindOk 构造 Queue.Bind-Ok 的参数区（无字段）。
func EncodeQueueBindOk() []byte { return nil }

// EncodeQueueUnbindOk 构造 Queue.Unbind-Ok 的参数区（无字段）。
func EncodeQueueUnbindOk() []byte { return nil }

// QueuePurge 是 Queue.Purge 的参数区。
type QueuePurge struct {
	Queue  string
	NoWait bool
}

// DecodeQueuePurge 解析 Queue.Purge 的参数区。
func DecodeQueuePurge(args []byte) (QueuePurge, error) {
	d := codec.NewDecoder(args)
	var m QueuePurge
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Queue, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.NoWait, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeQueuePurgeOk 构造 Queue.Purge-Ok 的参数区。
func EncodeQueuePurgeOk(messageCount uint32) []byte {
	e := codec.NewEncoder()
	e.Long(messageCount)
	return e.Bytes()
}

// QueueDelete 是 Queue.Delete 的参数区。
type QueueDelete struct {
	Queue    string
	IfUnused bool
	IfEmpty  bool
	NoWait   bool
}

// DecodeQueueDelete 解析 Queue.Delete 的参数区。
func DecodeQueueDelete(args []byte) (QueueDelete, error) {
	d := codec.NewDecoder(args)
	var m QueueDelete
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Queue, err = d.ShortStr(); err != nil {
		return m, err
	}
	bits := codec.NewBitReader(d)
	if m.IfUnused, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.IfEmpty, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.NoWait, err = bits.Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeQueueDeleteOk 构造 Queue.Delete-Ok 的参数区。
func EncodeQueueDeleteOk(messageCount uint32) []byte {
	e := codec.NewEncoder()
	e.Long(messageCount)
	return e.Bytes()
}

// ---------------------------------------------------------------------------
// Basic (60)
// ---------------------------------------------------------------------------

// BasicQos 是 Basic.Qos 的参数区。
type BasicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

// DecodeBasicQos 解析 Basic.Qos 的参数区。
func DecodeBasicQos(args []byte) (BasicQos, error) {
	d := codec.NewDecoder(args)
	var m BasicQos
	var err error
	if m.PrefetchSize, err = d.Long(); err != nil {
		return m, err
	}
	if m.PrefetchCount, err = d.Short(); err != nil {
		return m, err
	}
	if m.Global, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeBasicQosOk 构造 Basic.Qos-Ok 的参数区（无字段）。
func EncodeBasicQosOk() []byte { return nil }

// BasicConsume 是 Basic.Consume 的参数区。
type BasicConsume struct {
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   codec.Table
}

// DecodeBasicConsume 解析 Basic.Consume 的参数区。
func DecodeBasicConsume(args []byte) (BasicConsume, error) {
	d := codec.NewDecoder(args)
	var m BasicConsume
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Queue, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.ConsumerTag, err = d.ShortStr(); err != nil {
		return m, err
	}
	bits := codec.NewBitReader(d)
	if m.NoLocal, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.NoAck, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Exclusive, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.NoWait, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Arguments, err = d.Table(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeBasicConsumeOk 构造 Basic.Consume-Ok 的参数区。
func EncodeBasicConsumeOk(consumerTag string) ([]byte, error) {
	e := codec.NewEncoder()
	if err := e.ShortStr(consumerTag); err != nil {
		return nil, err
	}
	return e.Bytes(), nil
}

// BasicCancel 是 Basic.Cancel 的参数区。
type BasicCancel struct {
	ConsumerTag string
	NoWait      bool
}

// DecodeBasicCancel 解析 Basic.Cancel 的参数区。
func DecodeBasicCancel(args []byte) (BasicCancel, error) {
	d := codec.NewDecoder(args)
	var m BasicCancel
	var err error
	if m.ConsumerTag, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.NoWait, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeBasicCancelOk 构造 Basic.Cancel-Ok 的参数区。
func EncodeBasicCancelOk(consumerTag string) ([]byte, error) {
	e := codec.NewEncoder()
	if err := e.ShortStr(consumerTag); err != nil {
		return nil, err
	}
	return e.Bytes(), nil
}

// EncodeBasicCancel 构造 Basic.Cancel 的参数区（consumer-tag + no-wait 位）。
//
// 服务端主动取消（consumer_cancel_notify）走的是 Basic.Cancel，参数区**含一个 nowait 位**；
// 若借用只有 shortstr 的 Cancel-Ok 编码器，客户端按规范会在读完 shortstr 后继续读一个 bit，
// 字节耗尽即解码失败并断开连接。
func EncodeBasicCancel(consumerTag string, noWait bool) ([]byte, error) {
	e := codec.NewEncoder()
	if err := e.ShortStr(consumerTag); err != nil {
		return nil, err
	}
	w := codec.NewBitWriter(e)
	w.Bit(noWait)
	w.Flush()
	return e.Bytes(), nil
}

// BasicPublish 是 Basic.Publish 的参数区。
type BasicPublish struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

// DecodeBasicPublish 解析 Basic.Publish 的参数区。
func DecodeBasicPublish(args []byte) (BasicPublish, error) {
	d := codec.NewDecoder(args)
	var m BasicPublish
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Exchange, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.RoutingKey, err = d.ShortStr(); err != nil {
		return m, err
	}
	bits := codec.NewBitReader(d)
	if m.Mandatory, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Immediate, err = bits.Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// BasicGet 是 Basic.Get 的参数区。
type BasicGet struct {
	Queue string
	NoAck bool
}

// DecodeBasicGet 解析 Basic.Get 的参数区。
func DecodeBasicGet(args []byte) (BasicGet, error) {
	d := codec.NewDecoder(args)
	var m BasicGet
	if _, err := d.Short(); err != nil {
		return m, err
	}
	var err error
	if m.Queue, err = d.ShortStr(); err != nil {
		return m, err
	}
	if m.NoAck, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// EncodeBasicGetOk 构造 Basic.Get-Ok 的参数区。
func EncodeBasicGetOk(deliveryTag uint64, redelivered bool, exchange, routingKey string, messageCount uint32) ([]byte, error) {
	e := codec.NewEncoder()
	e.LongLong(deliveryTag)
	w := codec.NewBitWriter(e)
	w.Bit(redelivered)
	w.Flush()
	if err := e.ShortStr(exchange); err != nil {
		return nil, err
	}
	if err := e.ShortStr(routingKey); err != nil {
		return nil, err
	}
	e.Long(messageCount)
	return e.Bytes(), nil
}

// EncodeBasicGetEmpty 构造 Basic.Get-Empty 的参数区（reserved-1 为空 shortstr）。
func EncodeBasicGetEmpty() []byte {
	e := codec.NewEncoder()
	_ = e.ShortStr("")
	return e.Bytes()
}

// EncodeBasicDeliver 构造 Basic.Deliver 的参数区。
func EncodeBasicDeliver(consumerTag string, deliveryTag uint64, redelivered bool, exchange, routingKey string) ([]byte, error) {
	e := codec.NewEncoder()
	if err := e.ShortStr(consumerTag); err != nil {
		return nil, err
	}
	e.LongLong(deliveryTag)
	w := codec.NewBitWriter(e)
	w.Bit(redelivered)
	w.Flush()
	if err := e.ShortStr(exchange); err != nil {
		return nil, err
	}
	if err := e.ShortStr(routingKey); err != nil {
		return nil, err
	}
	return e.Bytes(), nil
}

// EncodeBasicReturn 构造 Basic.Return 的参数区（mandatory 未命中时回给生产者）。
func EncodeBasicReturn(replyCode uint16, replyText, exchange, routingKey string) ([]byte, error) {
	e := codec.NewEncoder()
	e.Short(replyCode)
	if err := e.ShortStr(replyText); err != nil {
		return nil, err
	}
	if err := e.ShortStr(exchange); err != nil {
		return nil, err
	}
	if err := e.ShortStr(routingKey); err != nil {
		return nil, err
	}
	return e.Bytes(), nil
}

// BasicAck 是 Basic.Ack 的参数区。
type BasicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

// DecodeBasicAck 解析 Basic.Ack 的参数区。
func DecodeBasicAck(args []byte) (BasicAck, error) {
	d := codec.NewDecoder(args)
	var m BasicAck
	var err error
	if m.DeliveryTag, err = d.LongLong(); err != nil {
		return m, err
	}
	if m.Multiple, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// BasicNack 是 Basic.Nack 的参数区（RabbitMQ 扩展）。
type BasicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

// DecodeBasicNack 解析 Basic.Nack 的参数区。
func DecodeBasicNack(args []byte) (BasicNack, error) {
	d := codec.NewDecoder(args)
	var m BasicNack
	var err error
	if m.DeliveryTag, err = d.LongLong(); err != nil {
		return m, err
	}
	bits := codec.NewBitReader(d)
	if m.Multiple, err = bits.Bit(); err != nil {
		return m, err
	}
	if m.Requeue, err = bits.Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// BasicReject 是 Basic.Reject 的参数区。
type BasicReject struct {
	DeliveryTag uint64
	Requeue     bool
}

// DecodeBasicReject 解析 Basic.Reject 的参数区。
func DecodeBasicReject(args []byte) (BasicReject, error) {
	d := codec.NewDecoder(args)
	var m BasicReject
	var err error
	if m.DeliveryTag, err = d.LongLong(); err != nil {
		return m, err
	}
	if m.Requeue, err = codec.NewBitReader(d).Bit(); err != nil {
		return m, err
	}
	return m, nil
}

// DecodeBasicRecover 解析 Basic.Recover / Recover-Async 的参数区。
func DecodeBasicRecover(args []byte) (requeue bool, err error) {
	d := codec.NewDecoder(args)
	return codec.NewBitReader(d).Bit()
}

// EncodeBasicRecoverOk 构造 Basic.Recover-Ok 的参数区（无字段）。
func EncodeBasicRecoverOk() []byte { return nil }
