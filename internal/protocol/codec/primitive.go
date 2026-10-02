// Package codec 实现 AMQP 0-9-1 的线级编解码：基本类型、field-table 与帧读写。
//
// 它与具体协议插件解耦，以便被内核其他部分复用（例如 AMQPLAIN 认证需要解析 field-table）。
//
// 兼容性要点：
//   - bit 字段按"同一字节内连续打包、跨字段边界另起字节"的规则处理；
//   - field-table 必须支持嵌套表、数组、Decimal、Timestamp、Void、ByteArray。
package codec

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	// ErrShortBuffer 表示数据不足（多半是被截断的帧）。
	ErrShortBuffer = errors.New("codec: 数据不足")
	// ErrSyntax 表示字段不符合 AMQP 0-9-1 语法（对应 502 SYNTAX_ERROR）。
	ErrSyntax = errors.New("codec: 语法错误")
)

// 帧类型。
const (
	FrameMethod    uint8 = 1
	FrameHeader    uint8 = 2
	FrameBody      uint8 = 3
	FrameHeartbeat uint8 = 8
	// FrameEnd 是每个帧的固定结束字节。
	FrameEnd uint8 = 0xCE
)

// 帧长度约束。
const (
	// FrameMaxDefault 是协商默认的帧上限（对齐 RabbitMQ 4.x）。
	FrameMaxDefault uint32 = 131072
	// FrameMaxAuthInitial 是认证完成前的初始帧上限。
	// 对齐 RabbitMQ 4.1+：该值从 4096 提升到 8192，客户端若自定义 frame-max 必须 ≥ 此值。
	FrameMaxAuthInitial uint32 = 8192
	// FrameMinSize 是 AMQP 规范允许的最小 frame-max（协议事实，不等于本实现接受的下限）。
	FrameMinSize uint32 = 4096
	// FrameMaxNegotiatedMin 是协商时**接受**的最小 frame-max（对齐 RabbitMQ 4.x）。
	//
	// 规范允许 4096，但 RabbitMQ 4.1 起把协商下限提到了 8192：客户端请求更小的值时，
	// 服务端在收到 Tune-Ok 后以 530 关闭连接（实测 4.3 的日志：
	// "negotiated frame_max = 4096 is lower than the minimum allowed value (8192)"）。
	// 不设这条下限，客户端在 Python/Go 库上都察觉不到差异，但从 SwiftMQ 迁回 RabbitMQ 就会连不上 ——
	// 这是双跑对照（M8-5）抓到的差异，按"对齐 RabbitMQ 4.x 语义"处理。
	FrameMaxNegotiatedMin uint32 = 8192
	// ChannelMaxDefault 是协商默认的 channel 上限。
	ChannelMaxDefault uint16 = 2047
	// HeartbeatDefault 是协商默认的心跳间隔（秒）。
	HeartbeatDefault uint16 = 60
)

// Table 是 AMQP 的 field-table。
type Table map[string]any

// Decimal 对应 field-table 的 'D' 类型。
type Decimal struct {
	Scale uint8
	Value int32
}

// ---------------------------------------------------------------------------
// 解码
// ---------------------------------------------------------------------------

// Decoder 从一个字节序列上顺序读取 AMQP 基本类型。
type Decoder struct {
	buf []byte
	pos int
}

// NewDecoder 构造解码器。
func NewDecoder(b []byte) *Decoder { return &Decoder{buf: b} }

// Pos 返回当前读取位置。
func (d *Decoder) Pos() int { return d.pos }

// Rest 返回剩余未读字节。
func (d *Decoder) Rest() []byte { return d.buf[d.pos:] }

// Empty 表示已读完。
func (d *Decoder) Empty() bool { return d.pos >= len(d.buf) }

func (d *Decoder) take(n int) ([]byte, error) {
	if n < 0 || d.pos+n > len(d.buf) {
		return nil, ErrShortBuffer
	}
	b := d.buf[d.pos : d.pos+n]
	d.pos += n
	return b, nil
}

// Octet 读取 1 字节无符号整数。
func (d *Decoder) Octet() (uint8, error) {
	b, err := d.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

// Short 读取 2 字节大端无符号整数。
func (d *Decoder) Short() (uint16, error) {
	b, err := d.take(2)
	if err != nil {
		return 0, err
	}
	return uint16(b[0])<<8 | uint16(b[1]), nil
}

// Long 读取 4 字节大端无符号整数。
func (d *Decoder) Long() (uint32, error) {
	b, err := d.take(4)
	if err != nil {
		return 0, err
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), nil
}

// LongLong 读取 8 字节大端无符号整数。
func (d *Decoder) LongLong() (uint64, error) {
	b, err := d.take(8)
	if err != nil {
		return 0, err
	}
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v, nil
}

// ShortStr 读取 shortstr（1 字节长度前缀）。
func (d *Decoder) ShortStr() (string, error) {
	n, err := d.Octet()
	if err != nil {
		return "", err
	}
	b, err := d.take(int(n))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// LongStr 读取 longstr（4 字节长度前缀）。
func (d *Decoder) LongStr() ([]byte, error) {
	n, err := d.Long()
	if err != nil {
		return nil, err
	}
	return d.take(int(n))
}

// Table 读取 field-table。
func (d *Decoder) Table() (Table, error) {
	n, err := d.Long()
	if err != nil {
		return nil, err
	}
	sub, err := d.take(int(n))
	if err != nil {
		return nil, err
	}
	inner := NewDecoder(sub)
	t := Table{}
	for !inner.Empty() {
		name, err := inner.ShortStr()
		if err != nil {
			return nil, err
		}
		v, err := inner.fieldValue()
		if err != nil {
			return nil, err
		}
		t[name] = v
	}
	return t, nil
}

// fieldValue 读取一个带类型标记的字段值。
//
// 注意：field-table 的字段值没有统一的长度前缀，因此遇到未知类型标记时**无法安全跳过**，
// 只能报语法错误 —— 这是格式本身决定的，不是实现偷懒。
func (d *Decoder) fieldValue() (any, error) {
	kind, err := d.Octet()
	if err != nil {
		return nil, err
	}
	switch kind {
	case 't': // boolean
		v, err := d.Octet()
		return v != 0, err
	case 'b': // short-short-int
		v, err := d.Octet()
		return int8(v), err
	case 'B': // short-short-uint
		return d.Octet()
	case 's': // short-int
		v, err := d.Short()
		return int16(v), err
	case 'u': // short-uint
		return d.Short()
	case 'I': // long-int
		v, err := d.Long()
		return int32(v), err
	case 'i': // long-uint
		return d.Long()
	case 'l': // long-long-int
		v, err := d.LongLong()
		return int64(v), err
	case 'f': // float
		v, err := d.Long()
		return math.Float32frombits(v), err
	case 'd': // double
		v, err := d.LongLong()
		return math.Float64frombits(v), err
	case 'D': // decimal
		scale, err := d.Octet()
		if err != nil {
			return nil, err
		}
		v, err := d.Long()
		if err != nil {
			return nil, err
		}
		return Decimal{Scale: scale, Value: int32(v)}, nil
	case 'S': // long string
		b, err := d.LongStr()
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case 'x': // byte array
		return d.LongStr()
	case 'T': // timestamp（秒）
		v, err := d.LongLong()
		if err != nil {
			return nil, err
		}
		return time.Unix(int64(v), 0).UTC(), nil
	case 'F': // nested table
		return d.Table()
	case 'A': // array
		return d.array()
	case 'V': // void
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: field-table 未知类型标记 %q", ErrSyntax, string(kind))
	}
}

func (d *Decoder) array() ([]any, error) {
	n, err := d.Long()
	if err != nil {
		return nil, err
	}
	sub, err := d.take(int(n))
	if err != nil {
		return nil, err
	}
	inner := NewDecoder(sub)
	out := []any{}
	for !inner.Empty() {
		v, err := inner.fieldValue()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 编码
// ---------------------------------------------------------------------------

// Encoder 顺序写出 AMQP 基本类型。
type Encoder struct {
	buf []byte
}

// NewEncoder 构造编码器。
func NewEncoder() *Encoder { return &Encoder{} }

// Bytes 返回已写入的字节（不复制）。
func (e *Encoder) Bytes() []byte { return e.buf }

// Len 返回已写入长度。
func (e *Encoder) Len() int { return len(e.buf) }

// Octet 写入 1 字节。
func (e *Encoder) Octet(v uint8) { e.buf = append(e.buf, v) }

// Short 写入 2 字节大端。
func (e *Encoder) Short(v uint16) {
	e.buf = append(e.buf, byte(v>>8), byte(v))
}

// Long 写入 4 字节大端。
func (e *Encoder) Long(v uint32) {
	e.buf = append(e.buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// LongLong 写入 8 字节大端。
func (e *Encoder) LongLong(v uint64) {
	e.buf = append(e.buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// ShortStr 写入 shortstr；长度超过 255 返回错误。
func (e *Encoder) ShortStr(s string) error {
	if len(s) > 255 {
		return fmt.Errorf("%w: shortstr 超过 255 字节", ErrSyntax)
	}
	e.Octet(uint8(len(s)))
	e.buf = append(e.buf, s...)
	return nil
}

// LongStr 写入 longstr。
func (e *Encoder) LongStr(b []byte) {
	e.Long(uint32(len(b)))
	e.buf = append(e.buf, b...)
}

// Table 写入 field-table。
func (e *Encoder) Table(t Table) error {
	inner := NewEncoder()
	for k, v := range t {
		if err := inner.ShortStr(k); err != nil {
			return err
		}
		if err := inner.fieldValue(v); err != nil {
			return err
		}
	}
	e.LongStr(inner.Bytes())
	return nil
}

func (e *Encoder) fieldValue(v any) error {
	switch x := v.(type) {
	case nil:
		e.Octet('V')
	case bool:
		e.Octet('t')
		if x {
			e.Octet(1)
		} else {
			e.Octet(0)
		}
	case int:
		e.Octet('I')
		e.Long(uint32(int32(x)))
	case int8:
		e.Octet('b')
		e.Octet(uint8(x))
	case uint8:
		e.Octet('B')
		e.Octet(x)
	case int16:
		e.Octet('s')
		e.Short(uint16(x))
	case uint16:
		e.Octet('u')
		e.Short(x)
	case int32:
		e.Octet('I')
		e.Long(uint32(x))
	case uint32:
		e.Octet('i')
		e.Long(x)
	case int64:
		e.Octet('l')
		e.LongLong(uint64(x))
	case uint64:
		e.Octet('l')
		e.LongLong(x)
	case float32:
		e.Octet('f')
		e.Long(math.Float32bits(x))
	case float64:
		e.Octet('d')
		e.LongLong(math.Float64bits(x))
	case Decimal:
		e.Octet('D')
		e.Octet(x.Scale)
		e.Long(uint32(x.Value))
	case string:
		e.Octet('S')
		e.LongStr([]byte(x))
	case []byte:
		e.Octet('x')
		e.LongStr(x)
	case time.Time:
		e.Octet('T')
		e.LongLong(uint64(x.Unix()))
	case Table:
		e.Octet('F')
		return e.Table(x)
	case map[string]any:
		e.Octet('F')
		return e.Table(Table(x))
	case []any:
		e.Octet('A')
		inner := NewEncoder()
		for _, item := range x {
			if err := inner.fieldValue(item); err != nil {
				return err
			}
		}
		e.LongStr(inner.Bytes())
	default:
		return fmt.Errorf("%w: 不支持的 field-table 值类型 %T", ErrSyntax, v)
	}
	return nil
}

// ---------------------------------------------------------------------------
// bit 字段打包
// ---------------------------------------------------------------------------

// BitWriter 按 AMQP 规则打包 bit 字段：同一字节内连续打包，跨字段边界另起字节。
//
// 调用方必须在所有 bit 字段写完后调用 Flush，否则最后一个未满字节不会落盘。
type BitWriter struct {
	e    *Encoder
	cur  uint8
	used uint8
}

// NewBitWriter 构造 bit 写入器。
func NewBitWriter(e *Encoder) *BitWriter { return &BitWriter{e: e} }

// Bit 写入一个 bit。
func (w *BitWriter) Bit(v bool) {
	if w.used == 8 {
		w.flushByte()
	}
	if v {
		w.cur |= 1 << w.used
	}
	w.used++
}

func (w *BitWriter) flushByte() {
	if w.used == 0 {
		return
	}
	w.e.Octet(w.cur)
	w.cur = 0
	w.used = 0
}

// Flush 落盘最后一个未满字节。
func (w *BitWriter) Flush() { w.flushByte() }

// BitReader 按 AMQP 规则读取 bit 字段。
type BitReader struct {
	d    *Decoder
	cur  uint8
	used uint8 // 已从 cur 取出的 bit 数；8 表示当前字节已用完
}

// NewBitReader 构造 bit 读取器。
func NewBitReader(d *Decoder) *BitReader { return &BitReader{d: d} }

// Bit 读取一个 bit（当前字节用尽时自动读入下一个字节）。
func (r *BitReader) Bit() (bool, error) {
	if r.used == 8 {
		r.used = 0
	}
	if r.used == 0 {
		b, err := r.d.Octet()
		if err != nil {
			return false, err
		}
		r.cur = b
	}
	v := r.cur&(1<<r.used) != 0
	r.used++
	return v, nil
}
