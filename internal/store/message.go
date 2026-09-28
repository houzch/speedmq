package store

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/houzch/swiftmq/internal/protocol/spec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 队列日志记录中的消息编码：
//
//	<exchange shortstr><routing-key shortstr><props-len u32><props><body-len u32><body>
//
// 属性段复用 AMQP 内容头的编解码（spec.EncodeContentHeader）而不是 JSON：
// 消息头里可能有字节数组、时间戳、嵌套表与 Decimal，JSON 往返会把它们降级成字符串，
// 消费者拿到的属性类型就变了。复用内容头编解码等于复用协议层已测过的类型保真实现。
func encodeMessage(msg *plugin.Message) ([]byte, error) {
	props, err := spec.EncodeContentHeader(spec.ContentHeader{
		ClassID:    spec.ClassBasic,
		BodySize:   uint64(len(msg.Body)),
		Properties: msg.Properties,
	})
	if err != nil {
		return nil, fmt.Errorf("编码消息属性失败: %w", err)
	}

	buf := make([]byte, 0, len(props)+len(msg.Body)+16)
	if buf, err = appendShortStr(buf, msg.Exchange); err != nil {
		return nil, err
	}
	if buf, err = appendShortStr(buf, msg.RoutingKey); err != nil {
		return nil, err
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(props)))
	buf = append(buf, props...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(msg.Body)))
	buf = append(buf, msg.Body...)
	return buf, nil
}

func decodeMessage(payload []byte) (*plugin.Message, error) {
	exchange, rest, err := readShortStr(payload)
	if err != nil {
		return nil, err
	}
	routingKey, rest, err := readShortStr(rest)
	if err != nil {
		return nil, err
	}
	props, rest, err := readBytes(rest)
	if err != nil {
		return nil, err
	}
	body, rest, err := readBytes(rest)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, errors.New("消息记录尾部有未消费的字节")
	}

	header, err := spec.DecodeContentHeader(props)
	if err != nil {
		return nil, fmt.Errorf("解码消息属性失败: %w", err)
	}
	return &plugin.Message{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Properties: header.Properties,
		Body:       body,
	}, nil
}

func appendShortStr(buf []byte, s string) ([]byte, error) {
	if len(s) > 255 {
		return nil, fmt.Errorf("字段长度 %d 超过 shortstr 上限 255", len(s))
	}
	buf = append(buf, byte(len(s)))
	return append(buf, s...), nil
}

func readShortStr(b []byte) (string, []byte, error) {
	if len(b) < 1 {
		return "", nil, errors.New("读取 shortstr 时数据不足")
	}
	n := int(b[0])
	if len(b) < 1+n {
		return "", nil, errors.New("读取 shortstr 时数据不足")
	}
	return string(b[1 : 1+n]), b[1+n:], nil
}

// readBytes 读取一个 u32 长度前缀的字节段。
func readBytes(b []byte) ([]byte, []byte, error) {
	if len(b) < 4 {
		return nil, nil, errors.New("读取长度前缀时数据不足")
	}
	n := int(binary.BigEndian.Uint32(b[0:4]))
	if n < 0 || len(b) < 4+n {
		return nil, nil, errors.New("读取字段内容时数据不足")
	}
	return b[4 : 4+n], b[4+n:], nil
}
