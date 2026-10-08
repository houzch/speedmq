package spec

import (
	"fmt"
	"time"

	"github.com/houzch/speedmq/internal/protocol/codec"
	"github.com/houzch/speedmq/pkg/plugin"
)

// 内容头（Content Header）承载 Basic 类的 14 个内容属性。
//
// 兼容性高危区：这 14 个属性按固定顺序用 16 位标志位标记"是否出现"，
// 顺序错一位或标志位算错一位，客户端就会解析崩溃或读到错位的属性值。
const (
	flagContentType     uint16 = 1 << 15
	flagContentEncoding uint16 = 1 << 14
	flagHeaders         uint16 = 1 << 13
	flagDeliveryMode    uint16 = 1 << 12
	flagPriority        uint16 = 1 << 11
	flagCorrelationID   uint16 = 1 << 10
	flagReplyTo         uint16 = 1 << 9
	flagExpiration      uint16 = 1 << 8
	flagMessageID       uint16 = 1 << 7
	flagTimestamp       uint16 = 1 << 6
	flagType            uint16 = 1 << 5
	flagUserID          uint16 = 1 << 4
	flagAppID           uint16 = 1 << 3
	flagClusterID       uint16 = 1 << 2
	// flagContinuation 表示"还有后续标志位字"，本实现与主流客户端都不使用。
	flagContinuation uint16 = 1 << 0
)

// ContentHeader 是内容头帧的载荷。
type ContentHeader struct {
	// ClassID 必须为 ClassBasic（60）。
	ClassID uint16
	// BodySize 是消息体的总长度（跨多个体帧）。
	BodySize uint64
	// Properties 是内容属性。
	Properties plugin.Properties
}

// EncodeContentHeader 编码内容头帧载荷。
func EncodeContentHeader(h ContentHeader) ([]byte, error) {
	classID := h.ClassID
	if classID == 0 {
		classID = ClassBasic
	}

	e := codec.NewEncoder()
	e.Short(classID)
	e.Short(0) // weight：已废弃，固定为 0
	e.LongLong(h.BodySize)

	p := h.Properties
	var flags uint16
	if p.ContentType != "" {
		flags |= flagContentType
	}
	if p.ContentEncoding != "" {
		flags |= flagContentEncoding
	}
	if len(p.Headers) > 0 {
		flags |= flagHeaders
	}
	if p.DeliveryMode != 0 {
		flags |= flagDeliveryMode
	}
	if p.Priority != 0 {
		flags |= flagPriority
	}
	if p.CorrelationID != "" {
		flags |= flagCorrelationID
	}
	if p.ReplyTo != "" {
		flags |= flagReplyTo
	}
	if p.Expiration != "" {
		flags |= flagExpiration
	}
	if p.MessageID != "" {
		flags |= flagMessageID
	}
	if !p.Timestamp.IsZero() {
		flags |= flagTimestamp
	}
	if p.Type != "" {
		flags |= flagType
	}
	if p.UserID != "" {
		flags |= flagUserID
	}
	if p.AppID != "" {
		flags |= flagAppID
	}

	e.Short(flags)

	// 值的顺序必须与标志位顺序严格一致
	if flags&flagContentType != 0 {
		if err := e.ShortStr(p.ContentType); err != nil {
			return nil, err
		}
	}
	if flags&flagContentEncoding != 0 {
		if err := e.ShortStr(p.ContentEncoding); err != nil {
			return nil, err
		}
	}
	if flags&flagHeaders != 0 {
		if err := e.Table(codec.Table(p.Headers)); err != nil {
			return nil, err
		}
	}
	if flags&flagDeliveryMode != 0 {
		e.Octet(p.DeliveryMode)
	}
	if flags&flagPriority != 0 {
		e.Octet(p.Priority)
	}
	if flags&flagCorrelationID != 0 {
		if err := e.ShortStr(p.CorrelationID); err != nil {
			return nil, err
		}
	}
	if flags&flagReplyTo != 0 {
		if err := e.ShortStr(p.ReplyTo); err != nil {
			return nil, err
		}
	}
	if flags&flagExpiration != 0 {
		if err := e.ShortStr(p.Expiration); err != nil {
			return nil, err
		}
	}
	if flags&flagMessageID != 0 {
		if err := e.ShortStr(p.MessageID); err != nil {
			return nil, err
		}
	}
	if flags&flagTimestamp != 0 {
		e.LongLong(uint64(p.Timestamp.Unix()))
	}
	if flags&flagType != 0 {
		if err := e.ShortStr(p.Type); err != nil {
			return nil, err
		}
	}
	if flags&flagUserID != 0 {
		if err := e.ShortStr(p.UserID); err != nil {
			return nil, err
		}
	}
	if flags&flagAppID != 0 {
		if err := e.ShortStr(p.AppID); err != nil {
			return nil, err
		}
	}
	return e.Bytes(), nil
}

// DecodeContentHeader 解析内容头帧载荷。
func DecodeContentHeader(payload []byte) (ContentHeader, error) {
	d := codec.NewDecoder(payload)

	classID, err := d.Short()
	if err != nil {
		return ContentHeader{}, err
	}
	if classID != ClassBasic {
		return ContentHeader{}, fmt.Errorf("%w: 内容头 class-id 期望 %d，实际 %d",
			codec.ErrSyntax, ClassBasic, classID)
	}
	if _, err := d.Short(); err != nil { // weight
		return ContentHeader{}, err
	}
	bodySize, err := d.LongLong()
	if err != nil {
		return ContentHeader{}, err
	}
	flags, err := d.Short()
	if err != nil {
		return ContentHeader{}, err
	}
	if flags&flagContinuation != 0 {
		return ContentHeader{}, fmt.Errorf("%w: 不支持的内容属性标志位续接", codec.ErrSyntax)
	}

	var p plugin.Properties
	if flags&flagContentType != 0 {
		if p.ContentType, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagContentEncoding != 0 {
		if p.ContentEncoding, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagHeaders != 0 {
		t, err := d.Table()
		if err != nil {
			return ContentHeader{}, err
		}
		// 保持 nil 与空表可区分：客户端显式发来的空表仍视为空表
		p.Headers = map[string]any(t)
	}
	if flags&flagDeliveryMode != 0 {
		if p.DeliveryMode, err = d.Octet(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagPriority != 0 {
		if p.Priority, err = d.Octet(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagCorrelationID != 0 {
		if p.CorrelationID, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagReplyTo != 0 {
		if p.ReplyTo, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagExpiration != 0 {
		if p.Expiration, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagMessageID != 0 {
		if p.MessageID, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagTimestamp != 0 {
		ts, err := d.LongLong()
		if err != nil {
			return ContentHeader{}, err
		}
		if ts != 0 {
			p.Timestamp = time.Unix(int64(ts), 0).UTC()
		}
	}
	if flags&flagType != 0 {
		if p.Type, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagUserID != 0 {
		if p.UserID, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagAppID != 0 {
		if p.AppID, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}
	if flags&flagClusterID != 0 {
		// 保留字段，读取后丢弃，但必须消费掉字节以免后续解析错位
		if _, err = d.ShortStr(); err != nil {
			return ContentHeader{}, err
		}
	}

	return ContentHeader{ClassID: classID, BodySize: bodySize, Properties: p}, nil
}
