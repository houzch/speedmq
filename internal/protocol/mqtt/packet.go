// 本文件是 MQTT 3.1.1 的**编解码层**：报文进出、字段读写、以及"畸形报文一律拒绝"。
//
// 它刻意不知道内核的存在（不 import pkg/plugin）：编解码正确性与会话语义解耦，
// 单测因此可以只喂字节、断言字节（见 test/unit/mqtt 的编解码用例）。
package mqtt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// Version 是本插件版本。
const Version = "0.1.0"

// 报文类型（MQTT 3.1.1 §2.2.1）。
const (
	pktConnect     byte = 1
	pktConnack     byte = 2
	pktPublish     byte = 3
	pktPuback      byte = 4
	pktPubrec      byte = 5
	pktPubrel      byte = 6
	pktPubcomp     byte = 7
	pktSubscribe   byte = 8
	pktSuback      byte = 9
	pktUnsubscribe byte = 10
	pktUnsuback    byte = 11
	pktPingreq     byte = 12
	pktPingresp    byte = 13
	pktDisconnect  byte = 14
)

// CONNACK 返回码（MQTT 3.1.1 §3.2.2.3）。
const (
	connAccepted          byte = 0x00
	connBadProtocol       byte = 0x01 // 协议版本不被接受
	connIDRejected        byte = 0x02 // 客户端标识不合法（如 clean=0 但为空）
	connServerUnavailable byte = 0x03
	connBadCredentials    byte = 0x04 // 用户名/口令不被接受
	connNotAuthorized     byte = 0x05
)

// subackFailure 是 SUBACK 里的"订阅失败"返回码。
const subackFailure byte = 0x80

// maxRemainingLength 是 MQTT 剩余长度字段能表示的上限（4 字节可变长度整数）。
const maxRemainingLength = 268435455

// errMalformed 表示对端报文不符合 MQTT 3.1.1：协议层遇到它一律断开连接。
var errMalformed = errors.New("mqtt: 报文格式非法")

// ---------------------------------------------------------------------------
// 固定头与可变长度整数
// ---------------------------------------------------------------------------

// readFixedHeader 读取固定头，返回报文类型、标志位与剩余长度。
func readFixedHeader(r *bufio.Reader) (typ byte, flags byte, remaining int, err error) {
	head, err := r.ReadByte()
	if err != nil {
		return 0, 0, 0, err
	}
	typ = head >> 4
	flags = head & 0x0f
	if typ == 0 || typ > pktDisconnect {
		return 0, 0, 0, fmt.Errorf("%w: 未知报文类型 %d", errMalformed, typ)
	}
	remaining, err = readRemainingLength(r)
	if err != nil {
		return 0, 0, 0, err
	}
	return typ, flags, remaining, nil
}

// readRemainingLength 读取"可变长度整数"（每字节 7 位，最高位是继续标志）。
func readRemainingLength(r *bufio.Reader) (int, error) {
	value, multiplier := 0, 1
	for i := 0; i < 4; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(b&0x7f) * multiplier
		if b&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	// 第 4 个字节仍带继续标志：按规范属于畸形报文。
	return 0, fmt.Errorf("%w: 剩余长度超过 4 字节", errMalformed)
}

// appendRemainingLength 编码"可变长度整数"。
func appendRemainingLength(buf []byte, n int) []byte {
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		buf = append(buf, d)
		if n == 0 {
			return buf
		}
	}
}

// frame 把报文体包成完整报文（固定头 + 体）。
func frame(typ byte, flags byte, body []byte) []byte {
	out := make([]byte, 0, len(body)+5)
	out = append(out, typ<<4|flags)
	out = appendRemainingLength(out, len(body))
	return append(out, body...)
}

// readBody 按剩余长度把一个报文读全。
//
// limit 是单报文上限（来自配置）：超过即拒绝，避免一个声明了巨大长度的报文把内存打爆。
func readBody(r *bufio.Reader, remaining, limit int) ([]byte, error) {
	if remaining < 0 || remaining > maxRemainingLength {
		return nil, fmt.Errorf("%w: 剩余长度 %d 越界", errMalformed, remaining)
	}
	if limit > 0 && remaining > limit {
		return nil, fmt.Errorf("%w: 报文长度 %d 超过上限 %d", errMalformed, remaining, limit)
	}
	body := make([]byte, remaining)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

// ---------------------------------------------------------------------------
// 字段读写
// ---------------------------------------------------------------------------

// fieldReader 顺序读取报文体里的字段（越界一律报错，不做"读到多少算多少"）。
type fieldReader struct {
	buf []byte
	off int
}

func (r *fieldReader) u8() (byte, error) {
	if r.off+1 > len(r.buf) {
		return 0, fmt.Errorf("%w: 字段不足（需要 1 字节）", errMalformed)
	}
	b := r.buf[r.off]
	r.off++
	return b, nil
}

func (r *fieldReader) u16() (uint16, error) {
	if r.off+2 > len(r.buf) {
		return 0, fmt.Errorf("%w: 字段不足（需要 2 字节）", errMalformed)
	}
	v := binary.BigEndian.Uint16(r.buf[r.off:])
	r.off += 2
	return v, nil
}

// raw 读取"2 字节长度 + 内容"的二进制字段。
func (r *fieldReader) raw() ([]byte, error) {
	n, err := r.u16()
	if err != nil {
		return nil, err
	}
	if r.off+int(n) > len(r.buf) {
		return nil, fmt.Errorf("%w: 字段长度 %d 超出报文", errMalformed, n)
	}
	out := r.buf[r.off : r.off+int(n)]
	r.off += int(n)
	return out, nil
}

// str 读取 UTF-8 字符串字段。
//
// MQTT 3.1.1 §1.5.3 要求字符串是合法 UTF-8 且不含 U+0000；两条都校验，
// 否则一个畸形长度就能把后续解析全带偏。
func (r *fieldReader) str() (string, error) {
	b, err := r.raw()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", fmt.Errorf("%w: 字符串不是合法 UTF-8", errMalformed)
	}
	for _, c := range b {
		if c == 0 {
			return "", fmt.Errorf("%w: 字符串含 U+0000", errMalformed)
		}
	}
	return string(b), nil
}

// remaining 返回尚未读取的字节数与剩余内容。
func (r *fieldReader) remaining() (int, []byte) {
	n := len(r.buf) - r.off
	if n < 0 {
		n = 0
	}
	return n, r.buf[r.off:]
}

// appendUTF8 追加 UTF-8 字符串字段。
func appendUTF8(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

// appendRaw 追加二进制字段。
func appendRaw(buf []byte, b []byte) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(b)))
	return append(buf, b...)
}

// ---------------------------------------------------------------------------
// CONNECT / CONNACK
// ---------------------------------------------------------------------------

// willMessage 是 CONNECT 里的遗嘱消息。
type willMessage struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// connectPacket 是解析后的 CONNECT。
type connectPacket struct {
	ProtocolName  string
	ProtocolLevel byte
	CleanSession  bool
	KeepAlive     uint16
	ClientID      string
	Will          *willMessage
	Username      string
	Password      []byte
}

// parseConnect 解析 CONNECT 报文体。
func parseConnect(body []byte) (*connectPacket, error) {
	r := &fieldReader{buf: body}
	p := &connectPacket{}
	var err error
	if p.ProtocolName, err = r.str(); err != nil {
		return nil, err
	}
	if p.ProtocolLevel, err = r.u8(); err != nil {
		return nil, err
	}
	flags, err := r.u8()
	if err != nil {
		return nil, err
	}
	if flags&0x01 != 0 {
		return nil, fmt.Errorf("%w: CONNECT 的保留位必须为 0", errMalformed)
	}
	hasUser := flags&0x80 != 0
	hasPass := flags&0x40 != 0
	willRetain := flags&0x20 != 0
	willQoS := (flags >> 3) & 0x03
	hasWill := flags&0x04 != 0
	p.CleanSession = flags&0x02 != 0
	if !hasWill && (willRetain || willQoS != 0) {
		return nil, fmt.Errorf("%w: 未设置 Will Flag 却带了遗嘱参数", errMalformed)
	}
	if willQoS > 2 {
		return nil, fmt.Errorf("%w: 遗嘱 QoS=%d 非法", errMalformed, willQoS)
	}
	if p.KeepAlive, err = r.u16(); err != nil {
		return nil, err
	}
	if p.ClientID, err = r.str(); err != nil {
		return nil, err
	}
	if hasWill {
		w := &willMessage{QoS: willQoS, Retain: willRetain}
		if w.Topic, err = r.str(); err != nil {
			return nil, err
		}
		if w.Payload, err = r.raw(); err != nil {
			return nil, err
		}
		p.Will = w
	}
	if hasUser {
		if p.Username, err = r.str(); err != nil {
			return nil, err
		}
	}
	if hasPass {
		if p.Password, err = r.raw(); err != nil {
			return nil, err
		}
	}
	if n, _ := r.remaining(); n != 0 {
		return nil, fmt.Errorf("%w: CONNECT 尾部有 %d 字节多余数据", errMalformed, n)
	}
	return p, nil
}

// encodeConnack 编码 CONNACK。
func encodeConnack(sessionPresent bool, code byte) []byte {
	flags := byte(0)
	if sessionPresent {
		flags = 0x01
	}
	return frame(pktConnack, 0, []byte{flags, code})
}

// ---------------------------------------------------------------------------
// PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP
// ---------------------------------------------------------------------------

// publishPacket 是解析后的 PUBLISH。
type publishPacket struct {
	Dup      bool
	QoS      byte
	Retain   bool
	Topic    string
	PacketID uint16
	Payload  []byte
}

// parsePublish 解析 PUBLISH（flags 参与解析：QoS 决定是否带 Packet ID）。
func parsePublish(flags byte, body []byte) (*publishPacket, error) {
	p := &publishPacket{
		Dup:    flags&0x08 != 0,
		QoS:    (flags >> 1) & 0x03,
		Retain: flags&0x01 != 0,
	}
	if p.QoS == 3 {
		return nil, fmt.Errorf("%w: PUBLISH QoS=3 非法", errMalformed)
	}
	if p.Dup && p.QoS == 0 {
		return nil, fmt.Errorf("%w: QoS=0 的 PUBLISH 不得置 DUP", errMalformed)
	}
	r := &fieldReader{buf: body}
	var err error
	if p.Topic, err = r.str(); err != nil {
		return nil, err
	}
	if p.QoS > 0 {
		if p.PacketID, err = r.u16(); err != nil {
			return nil, err
		}
		if p.PacketID == 0 {
			return nil, fmt.Errorf("%w: PUBLISH 的 Packet ID 不得为 0", errMalformed)
		}
	}
	_, p.Payload = r.remaining()
	return p, nil
}

// encodePublish 编码 PUBLISH。
func encodePublish(dup bool, qos byte, retain bool, topic string, packetID uint16, payload []byte) []byte {
	flags := qos << 1
	if dup {
		flags |= 0x08
	}
	if retain {
		flags |= 0x01
	}
	body := make([]byte, 0, len(topic)+len(payload)+4)
	body = appendUTF8(body, topic)
	if qos > 0 {
		body = binary.BigEndian.AppendUint16(body, packetID)
	}
	body = append(body, payload...)
	return frame(pktPublish, flags, body)
}

// parsePacketID 解析只带 Packet ID 的报文（PUBACK / PUBREC / PUBREL / PUBCOMP / UNSUBACK）。
func parsePacketID(body []byte) (uint16, error) {
	if len(body) != 2 {
		return 0, fmt.Errorf("%w: Packet ID 报文长度应为 2，实际 %d", errMalformed, len(body))
	}
	return binary.BigEndian.Uint16(body), nil
}

// encodeAck 编码只带 Packet ID 的报文。
func encodeAck(typ byte, packetID uint16) []byte {
	flags := byte(0)
	if typ == pktPubrel {
		// PUBREL 的标志位固定为 0b0010（MQTT 3.1.1 §3.6.1）。
		flags = 0x02
	}
	return frame(typ, flags, binary.BigEndian.AppendUint16(nil, packetID))
}

// ---------------------------------------------------------------------------
// SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK
// ---------------------------------------------------------------------------

// topicSubscription 是一次订阅请求里的单个主题过滤器。
type topicSubscription struct {
	Filter string
	QoS    byte
}

// subscribePacket 是解析后的 SUBSCRIBE。
type subscribePacket struct {
	PacketID uint16
	Topics   []topicSubscription
}

// parseSubscribe 解析 SUBSCRIBE（标志位必须为 0b0010）。
func parseSubscribe(flags byte, body []byte) (*subscribePacket, error) {
	if flags != 0x02 {
		return nil, fmt.Errorf("%w: SUBSCRIBE 标志位应为 2，实际 %d", errMalformed, flags)
	}
	r := &fieldReader{buf: body}
	p := &subscribePacket{}
	var err error
	if p.PacketID, err = r.u16(); err != nil {
		return nil, err
	}
	for {
		n, _ := r.remaining()
		if n == 0 {
			break
		}
		var filter string
		if filter, err = r.str(); err != nil {
			return nil, err
		}
		qos, err := r.u8()
		if err != nil {
			return nil, err
		}
		if qos > 2 {
			return nil, fmt.Errorf("%w: 订阅 QoS=%d 非法", errMalformed, qos)
		}
		p.Topics = append(p.Topics, topicSubscription{Filter: filter, QoS: qos})
	}
	if len(p.Topics) == 0 {
		return nil, fmt.Errorf("%w: SUBSCRIBE 未携带任何主题过滤器", errMalformed)
	}
	return p, nil
}

// encodeSuback 编码 SUBACK。
func encodeSuback(packetID uint16, codes []byte) []byte {
	body := binary.BigEndian.AppendUint16(nil, packetID)
	body = append(body, codes...)
	return frame(pktSuback, 0, body)
}

// unsubscribePacket 是解析后的 UNSUBSCRIBE。
type unsubscribePacket struct {
	PacketID uint16
	Filters  []string
}

// parseUnsubscribe 解析 UNSUBSCRIBE（标志位必须为 0b0010）。
func parseUnsubscribe(flags byte, body []byte) (*unsubscribePacket, error) {
	if flags != 0x02 {
		return nil, fmt.Errorf("%w: UNSUBSCRIBE 标志位应为 2，实际 %d", errMalformed, flags)
	}
	r := &fieldReader{buf: body}
	p := &unsubscribePacket{}
	var err error
	if p.PacketID, err = r.u16(); err != nil {
		return nil, err
	}
	for {
		n, _ := r.remaining()
		if n == 0 {
			break
		}
		filter, err := r.str()
		if err != nil {
			return nil, err
		}
		p.Filters = append(p.Filters, filter)
	}
	if len(p.Filters) == 0 {
		return nil, fmt.Errorf("%w: UNSUBSCRIBE 未携带任何主题过滤器", errMalformed)
	}
	return p, nil
}

// ---------------------------------------------------------------------------
// 无载荷报文
// ---------------------------------------------------------------------------

// encodePingresp 编码 PINGRESP。
func encodePingresp() []byte { return frame(pktPingresp, 0, nil) }

// expectEmptyBody 校验"无载荷报文"确实不带数据。
func expectEmptyBody(typ byte, body []byte) error {
	if len(body) != 0 {
		return fmt.Errorf("%w: 报文类型 %d 不应携带载荷（%d 字节）", errMalformed, typ, len(body))
	}
	return nil
}
