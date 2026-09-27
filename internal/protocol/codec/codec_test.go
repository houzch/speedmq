package codec

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

// TestFieldTableRoundTrip 覆盖 field-table 的全部常用类型，含嵌套表与数组。
// 类型标记写错一位客户端就会解析崩溃，是兼容性高危区。
func TestFieldTableRoundTrip(t *testing.T) {
	orig := Table{
		"flag":   true,
		"i8":     int8(-3),
		"u8":     uint8(200),
		"i16":    int16(-300),
		"u16":    uint16(65535),
		"i32":    int32(-70000),
		"u32":    uint32(4000000000),
		"i64":    int64(1) << 40,
		"f32":    float32(1.5),
		"f64":    2.25,
		"str":    "hello 世界",
		"bytes":  []byte{1, 2, 3},
		"ts":     time.Unix(1700000000, 0).UTC(),
		"dec":    Decimal{Scale: 2, Value: 1234},
		"nil":    nil,
		"nested": Table{"k": "v"},
		"arr":    []any{int32(1), "two"},
	}

	enc := NewEncoder()
	if err := enc.Table(orig); err != nil {
		t.Fatalf("编码失败: %v", err)
	}

	got, err := NewDecoder(enc.Bytes()).Table()
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}

	// 注意：整数类型按 AMQP 类型标记解码，故 int 会被解成 int32（'I'）。
	want := Table{
		"flag":   true,
		"i8":     int8(-3),
		"u8":     uint8(200),
		"i16":    int16(-300),
		"u16":    uint16(65535),
		"i32":    int32(-70000),
		"u32":    uint32(4000000000),
		"i64":    int64(1) << 40,
		"f32":    float32(1.5),
		"f64":    2.25,
		"str":    "hello 世界",
		"bytes":  []byte{1, 2, 3},
		"ts":     time.Unix(1700000000, 0).UTC(),
		"dec":    Decimal{Scale: 2, Value: 1234},
		"nil":    nil,
		"nested": Table{"k": "v"},
		"arr":    []any{int32(1), "two"},
	}

	if len(got) != len(want) {
		t.Fatalf("字段数量不一致: got %d, want %d", len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("缺少字段 %q", k)
			continue
		}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("字段 %q 不一致: got %#v (%T), want %#v (%T)", k, g, g, w, w)
		}
	}
}

// TestUnknownFieldTypeRejected 验证未知类型标记被拒。
//
// field-table 的字段值没有统一长度前缀，遇到未知类型无法安全跳过，只能报语法错误。
func TestUnknownFieldTypeRejected(t *testing.T) {
	body := NewEncoder()
	if err := body.ShortStr("k"); err != nil {
		t.Fatal(err)
	}
	body.Octet('Z') // 不存在的类型标记

	inner := body.Bytes()
	outer := NewEncoder()
	outer.Long(uint32(len(inner))) // field-table 以 4 字节长度开头
	raw := append(outer.Bytes(), inner...)

	_, err := NewDecoder(raw).Table()
	if !errors.Is(err, ErrSyntax) {
		t.Fatalf("期望 ErrSyntax，实际: %v", err)
	}
}

func TestShortStrTooLong(t *testing.T) {
	e := NewEncoder()
	if err := e.ShortStr(string(bytes.Repeat([]byte{'x'}, 256))); !errors.Is(err, ErrSyntax) {
		t.Fatalf("期望 ErrSyntax，实际: %v", err)
	}
}

// TestBitPacking 验证 bit 字段的打包规则：同一字节内连续打包，第 9 个 bit 另起一字节。
func TestBitPacking(t *testing.T) {
	e := NewEncoder()
	w := NewBitWriter(e)
	// 1,0,1,0,1,0,1,0 → 0x55；第 9 个 bit 应为 1，落在新字节的最低位
	pattern := []bool{true, false, true, false, true, false, true, false, true}
	for _, b := range pattern {
		w.Bit(b)
	}
	w.Flush()

	if len(e.Bytes()) != 2 {
		t.Fatalf("期望 2 字节，实际 %d 字节: % x", len(e.Bytes()), e.Bytes())
	}
	if e.Bytes()[0] != 0x55 || e.Bytes()[1] != 0x01 {
		t.Fatalf("打包结果错误: % x", e.Bytes())
	}

	r := NewBitReader(NewDecoder(e.Bytes()))
	for i, want := range pattern {
		got, err := r.Bit()
		if err != nil {
			t.Fatalf("第 %d 个 bit 读取失败: %v", i, err)
		}
		if got != want {
			t.Fatalf("第 %d 个 bit 不一致: got %v, want %v", i, got, want)
		}
	}
}

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	fw := NewFrameWriter(bufio.NewWriter(&buf))

	payload := []byte{0, 10, 0, 10, 1, 2, 3}
	if err := fw.Write(Frame{Type: FrameMethod, Channel: 0, Payload: payload}); err != nil {
		t.Fatalf("写帧失败: %v", err)
	}
	if err := fw.WriteHeartbeat(); err != nil {
		t.Fatalf("写心跳失败: %v", err)
	}
	if err := fw.Flush(); err != nil {
		t.Fatalf("flush 失败: %v", err)
	}

	fr := NewFrameReader(bufio.NewReader(&buf), FrameMaxDefault)

	f, err := fr.Read()
	if err != nil {
		t.Fatalf("读帧失败: %v", err)
	}
	if f.Type != FrameMethod || f.Channel != 0 || !bytes.Equal(f.Payload, payload) {
		t.Fatalf("帧内容不一致: type=%d channel=%d payload=% x", f.Type, f.Channel, f.Payload)
	}

	hb, err := fr.Read()
	if err != nil {
		t.Fatalf("读心跳失败: %v", err)
	}
	if hb.Type != FrameHeartbeat || len(hb.Payload) != 0 {
		t.Fatalf("心跳帧内容不一致: type=%d payload=% x", hb.Type, hb.Payload)
	}
}

func TestFrameEndByteValidated(t *testing.T) {
	raw := []byte{FrameHeartbeat, 0, 0, 0, 0, 0, 0, 0x00} // 结束字节应为 0xCE
	_, err := NewFrameReader(bufio.NewReader(bytes.NewReader(raw)), FrameMaxDefault).Read()
	if !errors.Is(err, ErrFrameEnd) {
		t.Fatalf("期望 ErrFrameEnd，实际: %v", err)
	}
}

func TestFrameTooLarge(t *testing.T) {
	// size 声明为 100，但 frame-max 限制为 16
	raw := []byte{FrameMethod, 0, 0, 0, 0, 0, 100, 0}
	_, err := NewFrameReader(bufio.NewReader(bytes.NewReader(raw)), 16).Read()
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("期望 ErrFrameTooLarge，实际: %v", err)
	}
}
