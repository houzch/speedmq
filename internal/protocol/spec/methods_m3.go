package spec

import (
	"github.com/houzch/swiftmq/internal/protocol/codec"
)

// 本文件覆盖 M3 所需的方法：发布确认（class 85）与内核回给生产者的 Basic.Ack / Basic.Nack。

// EncodeBasicAck 构造 Basic.Ack 的参数区（服务端向生产者确认）。
//
// 注意 Basic.Ack 是双向方法：客户端用它确认消费，服务端用它在 confirm 模式下
// 确认发布。两者的字段布局相同，但语义作用在完全不同的对象上。
func EncodeBasicAck(deliveryTag uint64, multiple bool) []byte {
	e := codec.NewEncoder()
	e.LongLong(deliveryTag)
	w := codec.NewBitWriter(e)
	w.Bit(multiple)
	w.Flush()
	return e.Bytes()
}

// EncodeBasicNack 构造 Basic.Nack 的参数区（服务端向生产者否定确认）。
//
// requeue 对服务端发起的 nack 恒为 false：消息根本没被接收，谈不上重新入队。
func EncodeBasicNack(deliveryTag uint64, multiple, requeue bool) []byte {
	e := codec.NewEncoder()
	e.LongLong(deliveryTag)
	w := codec.NewBitWriter(e)
	w.Bit(multiple)
	w.Bit(requeue)
	w.Flush()
	return e.Bytes()
}

// DecodeConfirmSelect 解析 Confirm.Select 的参数区，返回是否 no-wait。
func DecodeConfirmSelect(args []byte) (noWait bool, err error) {
	return codec.NewBitReader(codec.NewDecoder(args)).Bit()
}

// EncodeConfirmSelectOk 构造 Confirm.Select-Ok 的参数区（无字段）。
func EncodeConfirmSelectOk() []byte { return nil }
