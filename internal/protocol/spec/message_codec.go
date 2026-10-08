package spec

import (
	"fmt"
	"math"

	"github.com/houzch/speedmq/internal/protocol/codec"
	"github.com/houzch/speedmq/pkg/plugin"
)

// MessageCodec 实现 plugin.MessageCodec：用 AMQP 内容头的**规范编码**承载属性与消息头。
//
// 为什么不另发明一种格式：跨节点转发必须对客户端保真，而内容头的属性标志位与
// field-table 的类型编码本来就与客户端看到的一模一样 —— 直接复用即可保证
// int32 / double / bytes / timestamp / 嵌套表 都不会退化（见 plugin.MessageCodec 的约束）。
//
// 线格式（全部是 AMQP 基本类型，便于对照排查）：
//
//	shortstr exchange | shortstr routing_key | longstr 内容头载荷 | longstr 消息体
//
// 其中"内容头载荷"就是 Basic.Deliver 之前那一帧的内容，因此它自带了 BodySize 与全部属性。
type MessageCodec struct{}

// NewMessageCodec 返回消息编解码器。
func NewMessageCodec() MessageCodec { return MessageCodec{} }

var _ plugin.MessageCodec = MessageCodec{}

// EncodeMessage 编码一条消息。
func (MessageCodec) EncodeMessage(m *plugin.Message) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("编码消息失败: 消息为空")
	}
	header, err := EncodeContentHeader(ContentHeader{
		BodySize:   uint64(len(m.Body)),
		Properties: m.Properties,
	})
	if err != nil {
		return nil, fmt.Errorf("编码消息内容头失败: %w", err)
	}
	// 线格式用 u32 承载长度：先挡住超限输入，避免静默截断。
	// 用 int64 比较：本仓库需在 32 位平台（int 为 32 位）上可编译。
	if int64(len(header)) > math.MaxUint32 || int64(len(m.Body)) > math.MaxUint32 {
		return nil, fmt.Errorf("编码消息失败: 长度超出单条上限（头 %d 字节、体 %d 字节）", len(header), len(m.Body))
	}

	e := codec.NewEncoder()
	// 交换机名与路由键在 AMQP 里都是 shortstr（≤255 字节），超长即非法输入。
	if err := e.ShortStr(m.Exchange); err != nil {
		return nil, fmt.Errorf("编码消息交换机名失败: %w", err)
	}
	if err := e.ShortStr(m.RoutingKey); err != nil {
		return nil, fmt.Errorf("编码消息路由键失败: %w", err)
	}
	e.LongStr(header)
	e.LongStr(m.Body)
	return e.Bytes(), nil
}

// DecodeMessage 还原一条消息。
func (MessageCodec) DecodeMessage(data []byte) (*plugin.Message, error) {
	d := codec.NewDecoder(data)
	exchange, err := d.ShortStr()
	if err != nil {
		return nil, fmt.Errorf("解析消息交换机名失败: %w", err)
	}
	routingKey, err := d.ShortStr()
	if err != nil {
		return nil, fmt.Errorf("解析消息路由键失败: %w", err)
	}
	header, err := d.LongStr()
	if err != nil {
		return nil, fmt.Errorf("解析消息内容头失败: %w", err)
	}
	body, err := d.LongStr()
	if err != nil {
		return nil, fmt.Errorf("解析消息体失败: %w", err)
	}
	h, err := DecodeContentHeader(header)
	if err != nil {
		return nil, fmt.Errorf("解码消息内容头失败: %w", err)
	}
	// 体长度必须与内容头声明的一致：不一致说明数据被截断或串位，宁可报错也不要投出一条坏消息。
	if uint64(len(body)) != h.BodySize {
		return nil, fmt.Errorf("消息体长度与内容头不一致: 头声明 %d 字节，实际 %d 字节", h.BodySize, len(body))
	}
	return &plugin.Message{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Properties: h.Properties,
		Body:       body,
	}, nil
}
