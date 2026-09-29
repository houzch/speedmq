// Package spec_test 从包外验证 AMQP 0-9-1 的内容头与消息编解码。
//
// 重点在**保真**：跨节点转发依赖 plugin.MessageCodec，而它的价值恰恰是
// "不把 int32 变成 float64、不把 bytes 变成字符串"。这些退化用 JSON 中转时
// 一定会发生，因此必须有用例把它们钉死。
package spec_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/internal/protocol/spec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

func TestMessageCodecRoundTripPreservesTypes(t *testing.T) {
	codecImpl := spec.NewMessageCodec()
	ts := time.Unix(1700000000, 0).UTC()

	original := &plugin.Message{
		Exchange:   "amq.topic",
		RoutingKey: "a.b.c",
		Body:       []byte("hello \x00 world"),
		Properties: plugin.Properties{
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			DeliveryMode:    2,
			Priority:        5,
			CorrelationID:   "corr-1",
			ReplyTo:         "rq",
			Expiration:      "60000",
			MessageID:       "m-1",
			Timestamp:       ts,
			Type:            "event",
			UserID:          "guest",
			AppID:           "app",
			Headers: map[string]any{
				// 每一种都挑了"用 JSON 会退化"的类型
				"i8":  int8(-8),
				"u8":  uint8(200),
				"i16": int16(-1600),
				"u16": uint16(60000),
				"i32": int32(-320000),
				"u32": uint32(4000000000),
				"i64": int64(-640000000000),
				// 说明：AMQP 的 longlong 是**有符号**的，uint64 不是一种独立的线格式类型
				// （编码器把 uint64 也写成 'l'，解码回来是 int64）。因此这里用 int64，
				// 不把"uint64 原样往返"当成编解码器应当保证的事。
				"i64b":  int64(9000000000000000000),
				"f32":   float32(1.5),
				"f64":   float64(3.25),
				"bool":  true,
				"str":   "abc",
				"bytes": []byte{0x00, 0x01, 0xff},
				"ts":    ts,
				"dec":   codec.Decimal{Scale: 2, Value: 12345},
				"tbl":   codec.Table{"inner": int32(7), "deep": codec.Table{"x": []byte("y")}},
				"arr":   []any{int32(1), "two", []byte("three")},
			},
		},
	}

	encoded, err := codecImpl.EncodeMessage(original)
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}
	decoded, err := codecImpl.DecodeMessage(encoded)
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}

	if decoded.Exchange != original.Exchange || decoded.RoutingKey != original.RoutingKey {
		t.Fatalf("路由信息未保真: got %q/%q want %q/%q",
			decoded.Exchange, decoded.RoutingKey, original.Exchange, original.RoutingKey)
	}
	if !reflect.DeepEqual(decoded.Body, original.Body) {
		t.Fatalf("消息体未保真: got %v want %v", decoded.Body, original.Body)
	}
	// 时间戳先单独比（AMQP timestamp 是秒精度，且解码出来是本地时区）
	if decoded.Properties.Timestamp.Unix() != ts.Unix() {
		t.Fatalf("时间戳未保真: got %v want %v", decoded.Properties.Timestamp.Unix(), ts.Unix())
	}
	got, want := decoded.Properties, original.Properties
	got.Timestamp, want.Timestamp = time.Time{}, time.Time{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("属性未保真:\n got %#v\nwant %#v", got, want)
	}
	// 逐项断言关键类型，失败时能直接看出是哪个类型退化
	for name, want := range original.Properties.Headers {
		g := decoded.Properties.Headers[name]
		if reflect.TypeOf(g) != reflect.TypeOf(want) {
			t.Fatalf("消息头 %q 的类型退化: got %T want %T", name, g, want)
		}
	}
}

func TestMessageCodecRejectsMalformedData(t *testing.T) {
	c := spec.NewMessageCodec()
	if _, err := c.DecodeMessage([]byte{0xff}); err == nil {
		t.Fatalf("截断的数据应报错")
	}
	if _, err := c.EncodeMessage(nil); err == nil {
		t.Fatalf("空消息应报错")
	}
}
