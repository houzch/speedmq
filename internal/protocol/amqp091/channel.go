package amqp091

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/internal/protocol/spec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// channel 是一条 AMQP Channel 的状态。
//
// 分工：内核负责"消息该给谁"，这里负责"怎么把消息变成帧"以及本通道的
// delivery-tag 空间、未确认集合与 prefetch 额度。
type channel struct {
	id  uint16
	con *connection
	log *slog.Logger

	mu sync.Mutex
	// consumers 记录本通道注册的消费者：消费者标签 → 消费者信息
	consumers map[string]*consumerEntry
	// unacked 是本通道未确认的投递：delivery-tag → Delivery
	unacked map[uint64]*plugin.Delivery
	// nextTag 是 delivery-tag 分配器（每个通道独立，单调递增）
	nextTag uint64
	// prefetch 来自 basic.qos，作为新建消费者的默认额度
	prefetch uint16
	// confirm 为 true 时进入发布确认模式；publishSeq 是发布序号计数器
	// （与 delivery-tag 是两套独立的编号空间）。
	confirm    bool
	publishSeq uint64
	// pending 是正在组装的内容（basic.publish 之后等 header 与 body 帧）
	pending *pendingContent
	// closing 表示已因软错误关闭，等待客户端的 Channel.Close-Ok
	closing bool
}

// consumerEntry 的定义见 channel_methods.go：它带有"就绪闸门"，用于保证
// Consume-Ok 一定先于第一条 Basic.Deliver 发出。

// pendingContent 是尚未组装完成的一条发布内容。
type pendingContent struct {
	pub      spec.BasicPublish
	header   spec.ContentHeader
	body     []byte
	received uint64
}

func newChannel(id uint16, con *connection) *channel {
	return &channel{
		id:        id,
		con:       con,
		log:       con.log.With("channel", id),
		consumers: map[string]*consumerEntry{},
		unacked:   map[uint64]*plugin.Delivery{},
	}
}

// address 便于日志输出。
func (ch *channel) address() string {
	return fmt.Sprintf("channel %d", ch.id)
}

// ---------------------------------------------------------------------------
// 内容收发
// ---------------------------------------------------------------------------

// startPublish 记录一次发布的开始，等待内容头与体帧。
func (ch *channel) startPublish(pub spec.BasicPublish) error {
	if pub.Immediate {
		// 对齐 RabbitMQ：immediate 已不再支持，返回 540 而非静默忽略
		return plugin.Errorf(plugin.KindNotImplemented, "NOT_IMPLEMENTED - immediate=true")
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.pending != nil {
		// 上一条内容还没收完就来了新的 publish：协议违规
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 上一条内容尚未完成，收到新的 Basic.Publish")
	}
	ch.pending = &pendingContent{pub: pub}
	return nil
}

// setContentHeader 接收内容头帧。
func (ch *channel) setContentHeader(header spec.ContentHeader) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.pending == nil {
		return
	}
	ch.pending.header = header
	ch.pending.body = make([]byte, 0, minInt(int(header.BodySize), 4096))
}

// appendBody 追加内容体；返回 true 表示内容已收齐。
func (ch *channel) appendBody(chunk []byte) (complete bool, err error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.pending == nil {
		return false, plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 收到未预期的内容体帧")
	}
	ch.pending.body = append(ch.pending.body, chunk...)
	ch.pending.received += uint64(len(chunk))
	if ch.pending.received > ch.pending.header.BodySize {
		return false, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 内容体超过声明的长度")
	}
	return ch.pending.received == ch.pending.header.BodySize, nil
}

// finishPublish 把组装好的内容交给内核。
func (ch *channel) finishPublish() error {
	ch.mu.Lock()
	p := ch.pending
	ch.pending = nil
	ch.mu.Unlock()
	if p == nil {
		return nil
	}

	msg := &plugin.Message{
		Exchange:   p.pub.Exchange,
		RoutingKey: p.pub.RoutingKey,
		Properties: p.header.Properties,
		Body:       p.body,
	}
	sess := ch.con.sessionOrNil()
	if sess == nil {
		return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - vhost 会话未打开")
	}

	// confirm 模式下先取号：无论结果如何，每次发布都占用一个连续序号，
	// 否则客户端会看到序号跳跃而对不上自己的未确认记录。
	seq, confirmed := ch.nextPublishSeq()

	res, err := sess.Publish(msg, p.pub.Exchange, p.pub.RoutingKey, p.pub.Mandatory)
	if err != nil {
		return err
	}
	if !res.Routed && p.pub.Mandatory {
		// mandatory 未命中任何队列：按规范把消息退回给生产者。
		// 顺序很关键 —— Basic.Return 必须先于 confirm 发出，客户端才能把它与这条发布关联起来。
		if err := ch.sendReturn(uint16(spec.NoRoute), "NO_ROUTE", msg); err != nil {
			return err
		}
	}
	if !confirmed {
		return nil
	}
	if res.Rejected {
		// 被队列因长度限制拒绝：否定确认，让生产者知道这条没进队列
		return ch.sendConfirmNack(seq, false)
	}
	return ch.sendConfirmAck(seq, false)
}

// enableConfirm 打开本通道的发布确认模式。
func (ch *channel) enableConfirm() {
	ch.mu.Lock()
	ch.confirm = true
	ch.publishSeq = 0
	ch.mu.Unlock()
}

// nextPublishSeq 在 confirm 模式下分配发布序号；未开启时第二个返回值为 false。
func (ch *channel) nextPublishSeq() (uint64, bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if !ch.confirm {
		return 0, false
	}
	ch.publishSeq++
	return ch.publishSeq, true
}

// sendConfirmAck 向生产者确认一条消息已被接收。
//
// 当前语义是"已入队（内存）"，与 RabbitMQ 对瞬时消息的处理一致。
// 持久化在 M4 落地后，持久消息的确认必须改为落盘之后 —— 那时这里要接存储层。
func (ch *channel) sendConfirmAck(seq uint64, multiple bool) error {
	return ch.con.sendMethod(ch.id, spec.ClassBasic, spec.MethodBasicAck,
		spec.EncodeBasicAck(seq, multiple))
}

// sendConfirmNack 向生产者否定确认。
func (ch *channel) sendConfirmNack(seq uint64, multiple bool) error {
	return ch.con.sendMethod(ch.id, spec.ClassBasic, spec.MethodBasicNack,
		spec.EncodeBasicNack(seq, multiple, false))
}

// sendReturn 回送 Basic.Return + 内容。
func (ch *channel) sendReturn(code uint16, text string, msg *plugin.Message) error {
	args, err := spec.EncodeBasicReturn(code, text, msg.Exchange, msg.RoutingKey)
	if err != nil {
		return err
	}
	frames, err := ch.buildContentFrames(spec.EncodeMethod(spec.ClassBasic, spec.MethodBasicReturn, args), msg.Properties, msg.Body)
	if err != nil {
		return err
	}
	return ch.con.writeFrames(frames...)
}

// sendDelivery 发送 Basic.Deliver + 内容。
func (ch *channel) sendDelivery(tag uint64, d *plugin.Delivery) error {
	args, err := spec.EncodeBasicDeliver(d.ConsumerTag, tag, d.Redelivered, d.Message.Exchange, d.Message.RoutingKey)
	if err != nil {
		return err
	}
	frames, err := ch.buildContentFrames(spec.EncodeMethod(spec.ClassBasic, spec.MethodBasicDeliver, args),
		d.Message.Properties, d.Message.Body)
	if err != nil {
		return err
	}
	return ch.con.writeFrames(frames...)
}

// sendGetOk 发送 Basic.Get-Ok + 内容。
func (ch *channel) sendGetOk(tag uint64, d *plugin.Delivery, remaining uint32) error {
	args, err := spec.EncodeBasicGetOk(tag, d.Redelivered, d.Message.Exchange, d.Message.RoutingKey, remaining)
	if err != nil {
		return err
	}
	frames, err := ch.buildContentFrames(spec.EncodeMethod(spec.ClassBasic, spec.MethodBasicGetOk, args),
		d.Message.Properties, d.Message.Body)
	if err != nil {
		return err
	}
	return ch.con.writeFrames(frames...)
}

// buildContentFrames 把"方法 + 内容头 + 内容体"组装成一组帧。
//
// 必须一次性写出：这三类帧之间不允许插入其他帧，否则客户端会把它当成另一条消息的内容。
func (ch *channel) buildContentFrames(methodPayload []byte, props plugin.Properties, body []byte) ([]codec.Frame, error) {
	headerPayload, err := spec.EncodeContentHeader(spec.ContentHeader{
		ClassID:    spec.ClassBasic,
		BodySize:   uint64(len(body)),
		Properties: props,
	})
	if err != nil {
		return nil, err
	}

	frames := make([]codec.Frame, 0, 3)
	frames = append(frames,
		codec.Frame{Type: codec.FrameMethod, Channel: ch.id, Payload: methodPayload},
		codec.Frame{Type: codec.FrameHeader, Channel: ch.id, Payload: headerPayload},
	)

	// 单个体帧的载荷上限 = frame-max - 8（帧头 7 字节 + 结束字节 1 字节）
	maxBody := int(ch.con.frameMax) - 8
	if maxBody <= 0 {
		maxBody = 4096
	}
	for off := 0; off < len(body); off += maxBody {
		end := off + maxBody
		if end > len(body) {
			end = len(body)
		}
		frames = append(frames, codec.Frame{Type: codec.FrameBody, Channel: ch.id, Payload: body[off:end]})
	}
	return frames, nil
}

// ---------------------------------------------------------------------------
// 消费者
// ---------------------------------------------------------------------------

// assignTag 分配本通道的下一个 delivery-tag。
func (ch *channel) assignTag() uint64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.nextTag++
	return ch.nextTag
}

// deliver 把一条投递写成帧发给客户端。
//
// 该回调可能由内核的其他协程触发（发布者的连接协程、ack 路径），
// 因此这里必须自行持锁并保证 3 类帧原子写出。
func (ch *channel) deliver(entry *consumerEntry, d *plugin.Delivery) error {
	// delivery-tag 无论是否 auto-ack 都分配：RabbitMQ 的 tag 空间与 ack 模式无关，
	// 且客户端可能以 tag 为 0 判定异常。
	tag := ch.assignTag()
	if !entry.noAck {
		ch.mu.Lock()
		ch.unacked[tag] = d
		ch.mu.Unlock()
	}
	if err := ch.sendDelivery(tag, d); err != nil {
		// 写失败（连接已断）：把投递给内核结算，触发重新入队
		if !entry.noAck {
			ch.discardUnacked(tag)
		}
		return err
	}
	return nil
}

// serverCancel 由内核调用：通知客户端该消费者已被取消（队列被删除等）。
//
// 这是 consumer_cancel_notify 能力要求的服务端主动 basic.cancel。
func (ch *channel) serverCancel(entry *consumerEntry, reason string) {
	ch.mu.Lock()
	delete(ch.consumers, entry.tag)
	ch.mu.Unlock()

	args, err := spec.EncodeBasicCancelOk(entry.tag)
	if err != nil {
		return
	}
	// 服务端发起的取消用 Basic.Cancel（而非 Cancel-Ok），no-wait 语义由客户端处理
	payload := spec.EncodeMethod(spec.ClassBasic, spec.MethodBasicCancel, args)
	if err := ch.con.writeFrames(codec.Frame{Type: codec.FrameMethod, Channel: ch.id, Payload: payload}); err != nil {
		ch.log.Debug("下发 basic.cancel 失败", "consumer_tag", entry.tag, "err", err)
		return
	}
	ch.log.Debug("已通知客户端消费者被取消", "consumer_tag", entry.tag, "reason", reason)
}

// cancelAllConsumers 取消本通道全部消费者（通道或连接关闭时）。
func (ch *channel) cancelAllConsumers(sess plugin.Session) {
	ch.mu.Lock()
	tags := make([]string, 0, len(ch.consumers))
	for tag := range ch.consumers {
		tags = append(tags, tag)
	}
	ch.consumers = map[string]*consumerEntry{}
	ch.mu.Unlock()

	for _, tag := range tags {
		if err := sess.Cancel(tag); err != nil {
			ch.log.Debug("取消消费者失败", "consumer_tag", tag, "err", err)
		}
	}
}

// requeueAll 把本通道未确认的投递全部重新入队（连接/通道异常关闭时）。
func (ch *channel) requeueAll() {
	ch.mu.Lock()
	items := make([]*plugin.Delivery, 0, len(ch.unacked))
	for _, d := range ch.unacked {
		items = append(items, d)
	}
	ch.unacked = map[uint64]*plugin.Delivery{}
	ch.mu.Unlock()

	for _, d := range items {
		// 未确认消息必须回到队列，否则连接断开就会丢消息
		d.Settle(plugin.SettleRequeue)
	}
}

// discardUnacked 把某个 tag 从本通道的未确认集合移除并重新入队。
func (ch *channel) discardUnacked(tag uint64) {
	ch.mu.Lock()
	d, ok := ch.unacked[tag]
	delete(ch.unacked, tag)
	ch.mu.Unlock()
	if ok {
		d.Settle(plugin.SettleRequeue)
	}
}

// ---------------------------------------------------------------------------
// 确认
// ---------------------------------------------------------------------------

// ack 处理 basic.ack（正常消费完成）。
func (ch *channel) ack(tag uint64, multiple bool) error {
	targets, err := ch.takeUnacked(tag, multiple)
	if err != nil {
		return err
	}
	for _, d := range targets {
		d.Settle(plugin.SettleAck)
	}
	return nil
}

// nack 处理 basic.nack / basic.reject。
func (ch *channel) nack(tag uint64, multiple, requeue bool) error {
	targets, err := ch.takeUnacked(tag, multiple)
	if err != nil {
		return err
	}
	action := plugin.SettleReject
	if requeue {
		action = plugin.SettleRequeue
	}
	for _, d := range targets {
		d.Settle(action)
	}
	return nil
}

// takeUnacked 取出要结算的投递。
//
// multiple=true 时展开为"所有 tag ≤ 指定值的未确认投递" —— 这个展开必须在本通道内做，
// 因为 delivery-tag 空间是通道级的，且可能横跨多个队列。
func (ch *channel) takeUnacked(tag uint64, multiple bool) ([]*plugin.Delivery, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !multiple {
		d, ok := ch.unacked[tag]
		if !ok {
			// 对齐 AMQP：未知 delivery-tag 视为 PRECONDITION_FAILED
			return nil, plugin.Errorf(plugin.KindPreconditionFailed,
				"PRECONDITION_FAILED - unknown delivery tag %d", tag)
		}
		delete(ch.unacked, tag)
		return []*plugin.Delivery{d}, nil
	}

	var out []*plugin.Delivery
	for t, d := range ch.unacked {
		if t <= tag {
			out = append(out, d)
			delete(ch.unacked, t)
		}
	}
	return out, nil
}

// recover 处理 basic.recover：把本通道未确认的投递全部重新入队。
//
// Requueue=false 的原始语义（"重新投递给同一个消费者"）在规范里含糊，
// RabbitMQ 对两种取值都做重新入队，这里保持一致。
func (ch *channel) recover(requeue bool) {
	ch.mu.Lock()
	items := make([]*plugin.Delivery, 0, len(ch.unacked))
	for _, d := range ch.unacked {
		items = append(items, d)
	}
	ch.unacked = map[uint64]*plugin.Delivery{}
	ch.mu.Unlock()

	for _, d := range items {
		d.Settle(plugin.SettleRequeue)
	}
}

// ---------------------------------------------------------------------------
// 错误映射
// ---------------------------------------------------------------------------

// replyFor 把内核错误映射为 AMQP 错误码与文本。
func replyFor(err error) (uint16, string) {
	var ke *plugin.Error
	if errors.As(err, &ke) {
		switch ke.Kind {
		case plugin.KindNotFound:
			return spec.NotFound, ke.Text
		case plugin.KindPreconditionFailed:
			return spec.PreconditionFailed, ke.Text
		case plugin.KindAccessRefused:
			return spec.AccessRefused, ke.Text
		case plugin.KindResourceLocked:
			return spec.ResourceLocked, ke.Text
		case plugin.KindInvalidPath:
			return spec.InvalidPath, ke.Text
		case plugin.KindNotImplemented:
			return spec.NotImplemented, ke.Text
		default:
			return spec.InternalError, ke.Text
		}
	}
	return spec.InternalError, err.Error()
}

// fail 按错误作用域处理内核返回的错误：软错误关 Channel，硬错误关连接。
func (ch *channel) fail(err error, classID, methodID uint16) error {
	code, text := replyFor(err)
	if spec.ScopeOf(code) == spec.ScopeConnection {
		return ch.con.failConnection(code, text, classID, methodID)
	}
	return ch.con.closeChannelByID(ch.id, code, text, classID, methodID)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
