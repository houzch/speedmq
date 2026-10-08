package amqp091

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/houzch/speedmq/internal/protocol/codec"
	"github.com/houzch/speedmq/internal/protocol/spec"
	"github.com/houzch/speedmq/pkg/plugin"
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
	// tx 为 true 时通道处于事务模式（Tx.Select）；txOps 是事务缓冲。
	// 事务完全在协议层实现 —— 内核 Session 不需要知道事务的存在：
	// 缓冲里的 publish 在 commit 时才真正投递，ack 在 commit 时才真正结算。
	tx    bool
	txOps []txOp
	// pending 是正在组装的内容（basic.publish 之后等 header 与 body 帧）
	pending *pendingContent
	// directReply 非 nil 表示本通道消费着 direct reply-to 伪队列（每通道至多一条）。
	directReply *directReply
	// closing 表示已因软错误关闭，等待客户端的 Channel.Close-Ok
	closing bool

	// ---- 发布确认流水线（M0）----
	// confirm 模式下，连接读循环**不再同步等** res.Durable()：只把"待确认项"投进 confirmCh，
	// 由 confirmLoop 协程按发布序号顺序等落盘后再回 basic.ack/nack。这样同一连接可同时存在多条
	// 在途提案 —— 否则服务端永远只有 1 条在途，组提交无对象可合并（单发布者吞吐被卡死）。
	// 这三个字段在 enableConfirm 时建立，此后只读。
	confirmCh   chan confirmItem
	confirmStop chan struct{}
	stopOnce    sync.Once
}

// confirmItem 是一条等待确认的发布（confirm 模式下由读循环登记、确认器消费）。
type confirmItem struct {
	seq uint64
	// durable 等"按 fsync 档位落盘 / 已多数派"完成；可能为 nil（瞬时消息 / 非持久档位）。
	durable func() error
	// rejected 表示被队列因长度限制拒绝（应回 nack）。
	rejected bool
}

// confirmQueueDepth 是每通道"待确认发布"的队列深度：既是内存上界，也是背压点
// （队列满时读循环阻塞，起到限流作用）。
const confirmQueueDepth = 4096

// txOpKind 区分事务缓冲里的一项操作。
type txOpKind uint8

const (
	txOpPublish txOpKind = iota
	txOpSettle
)

// txOp 是事务缓冲里的一项操作（publish 或 ack/nack/reject）。
//
// 两者放在**同一个有序切片**里，是为了让 commit 严格按客户端的调用顺序应用 ——
// 一个事务里"先 ack 再 publish"与"先 publish 再 ack"的结果可能不同
// （例如队列长度限制 / 溢出策略），保持顺序才不会让客户端遇到意外的语义。
type txOp struct {
	kind txOpKind
	// publish 用：消息本体与 mandatory 标志。
	msg       *plugin.Message
	mandatory bool
	// settle 用：投递标签、是否批量、结算动作。
	tag      uint64
	multiple bool
	action   plugin.SettleAction
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

// maxMessageSize 是单条消息体的上限，对齐 RabbitMQ 默认的 max_message_size（128 MiB）。
//
// 必须在收到内容头时就校验：BodySize 是客户端声明的 uint64，直接拿它做 make 的容量
// 会因 int 溢出（≥2^63）得到负值而 panic（运行期无 recover，等于远程可崩进程）；
// 即便不溢出，不设上限也等于把本机内存交给客户端支配。
const maxMessageSize uint64 = 128 << 20

// setContentHeader 接收内容头帧；声明的消息体超过上限时返回错误（由调用方关 channel）。
func (ch *channel) setContentHeader(header spec.ContentHeader) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.pending == nil {
		return nil
	}
	if header.BodySize > maxMessageSize {
		// 对齐 RabbitMQ：超限是软错误（406），只关 channel 不断连接。
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - message size %d is larger than configured max size %d",
			header.BodySize, maxMessageSize)
	}
	ch.pending.header = header
	ch.pending.body = make([]byte, 0, minInt(int(header.BodySize), 4096))
	return nil
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

	// direct reply-to：把请求里的伪队列名换成真正可路由的应答队列名。
	// 必须在这里做（而不是等投递时），因为改写结果要随消息属性一起到达应答方。
	if err := ch.rewriteReplyTo(msg); err != nil {
		return err
	}

	// 事务模式：先缓冲，等 Tx.Commit 再真正投递给内核。
	// 缓冲期间这条消息对任何队列都不可见 —— 这正是"提交前不可见"的事务语义。
	if ch.inTx() {
		ch.bufferPublish(msg, p.pub.Mandatory)
		return nil
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
	// M0：不再在连接读循环里同步等落盘 —— 只登记，由 confirmLoop 按序号顺序确认。
	// 语义不变：basic.ack 仍然只在 res.Durable()（落盘/多数派）成功之后才发出；
	// 变的只是"谁来等、何时回"，从而让同一连接可以有多条在途（组提交才有对象可合并）。
	return ch.enqueueConfirm(confirmItem{seq: seq, durable: res.Durable, rejected: res.Rejected})
}

// enableConfirm 打开本通道的发布确认模式。
//
// 与事务互斥：同一通道上先 tx.select 再 confirm.select 会回 406（反向亦然，见 enableTx）。
func (ch *channel) enableConfirm() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.tx {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - cannot switch from tx to confirm mode")
	}
	ch.confirm = true
	ch.publishSeq = 0
	// M0：建立每通道的异步确认器（重复 confirm.select 只启一个）。
	if ch.confirmCh == nil {
		ch.confirmCh = make(chan confirmItem, confirmQueueDepth)
		ch.confirmStop = make(chan struct{})
		go ch.confirmLoop()
	}
	return nil
}

// enqueueConfirm 登记一条待确认发布（读循环调用）。
//
// 队列满时阻塞 —— 这是**背压**：待确认项本身就是"已加入队列但客户端还没收到 ack"的消息，
// 无上限会让内存被客户端支配。通道关闭时立刻返回（消息已投给内核，客户端也会收到 Channel.Close）。
func (ch *channel) enqueueConfirm(it confirmItem) error {
	ch.mu.Lock()
	q, stop := ch.confirmCh, ch.confirmStop
	ch.mu.Unlock()
	if q == nil || stop == nil {
		// 未开启确认器（理论不可达）：退化为同步确认，宁可慢也不把消息"发布了却没确认"。
		return ch.confirmNow(it)
	}
	select {
	case q <- it:
		return nil
	case <-stop:
		return nil
	}
}

// confirmLoop 是确认器：按发布序号**顺序**等待每条待确认项落盘，再回 ack/nack。
//
// 顺序等待不会削弱流水线：各条的在途工作（Raft 提案 / 组提交 fsync）在 Publish 时就已经启动，
// 这里只是按序"收取结果"，因此通常不需要真正等待。
func (ch *channel) confirmLoop() {
	for {
		select {
		case <-ch.confirmStop:
			return
		case it := <-ch.confirmCh:
			if err := ch.confirmNow(it); err != nil {
				// 写确认帧失败（多半是连接已断）：继续处理后续项意义不大，退出。
				return
			}
		}
	}
}

// confirmNow 等落盘并按语义回 ack/nack。
func (ch *channel) confirmNow(it confirmItem) error {
	// 持久消息：必须等"落盘/多数派"完成再确认，否则客户端会把 confirm 当成"不会丢"的依据。
	// 队列因长度限制拒绝发布时也经此返回错误（转发路径由 Owner 侧回填），同样按否定确认处理。
	if it.durable != nil {
		if err := it.durable(); err != nil {
			ch.log.Error("发布未能完成，已对发布者否定确认", "err", err)
			return ch.sendConfirmNack(it.seq, false)
		}
	}
	if it.rejected {
		// 被队列因长度限制拒绝：否定确认，让生产者知道这条没进队列
		return ch.sendConfirmNack(it.seq, false)
	}
	return ch.sendConfirmAck(it.seq, false)
}

// stopConfirms 停止确认器（幂等）。不等待其退出：它最多再完成一次落盘等待即有界退出，
// 与通知/心跳协程的收尾方式一致，不阻塞连接关闭。
func (ch *channel) stopConfirms() {
	ch.mu.Lock()
	stop := ch.confirmStop
	ch.mu.Unlock()
	if stop == nil {
		return
	}
	ch.stopOnce.Do(func() { close(stop) })
}

// ---------------------------------------------------------------------------
// 事务（Tx.Select / Tx.Commit / Tx.Rollback）
// ---------------------------------------------------------------------------

// inTx 返回本通道是否处于事务模式。
func (ch *channel) inTx() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.tx
}

// enableTx 打开事务模式（Tx.Select）。与 Confirm 互斥。
//
// 重复 tx.select 是幂等的（RabbitMQ 亦然）：既不清空缓冲，也不报错。
func (ch *channel) enableTx() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.confirm {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - cannot switch from confirm to tx mode")
	}
	ch.tx = true
	return nil
}

// bufferPublish 在事务模式下缓冲一条发布。
func (ch *channel) bufferPublish(msg *plugin.Message, mandatory bool) {
	ch.mu.Lock()
	ch.txOps = append(ch.txOps, txOp{kind: txOpPublish, msg: msg, mandatory: mandatory})
	ch.mu.Unlock()
}

// bufferSettle 在事务模式下缓冲一次结算（ack / nack / reject）。
//
// 缓冲期间**不动** unacked 集合：事务未提交前这些消息仍是未确认的，
// 因此 rollback 之后客户端可以重新 ack；这也让"通道关闭 → 未确认消息重新入队"
// 这条既有保证依然成立。
func (ch *channel) bufferSettle(action plugin.SettleAction, tag uint64, multiple bool) {
	ch.mu.Lock()
	ch.txOps = append(ch.txOps, txOp{kind: txOpSettle, tag: tag, multiple: multiple, action: action})
	ch.mu.Unlock()
}

// commitTx 提交事务：按客户端调用顺序把缓冲里的操作应用到内核。
//
// 任一步失败即中止并返回错误（协议层据此回 channel 级错误）。
// 已经应用过的部分**不回滚**：RabbitMQ 的事务也不是跨队列的原子提交，
// 它给出的保证是"提交前对外不可见"，而不是"要么全做要么全不做"。
func (ch *channel) commitTx(sess plugin.Session) error {
	ch.mu.Lock()
	ops := ch.txOps
	ch.txOps = nil
	ch.mu.Unlock()

	for _, op := range ops {
		switch op.kind {
		case txOpPublish:
			res, err := sess.Publish(op.msg, op.msg.Exchange, op.msg.RoutingKey, op.mandatory)
			if err != nil {
				return err
			}
			if !res.Routed && op.mandatory {
				if err := ch.sendReturn(uint16(spec.NoRoute), "NO_ROUTE", op.msg); err != nil {
					return err
				}
			}
			// 提交必须等落盘：客户端把 Commit-Ok 当作"这批消息已经生效"的依据，
			// 持久消息只在内存里就回 Ok 会违背这条约定。
			if res.Durable != nil {
				if err := res.Durable(); err != nil {
					return plugin.Errorf(plugin.KindInternal,
						"INTERNAL_ERROR - 事务提交时持久化失败: %v", err)
				}
			}
		case txOpSettle:
			if err := ch.settle(op.action, op.tag, op.multiple); err != nil {
				return err
			}
		}
	}
	return nil
}

// rollbackTx 回滚事务：丢弃缓冲。未提交的 ack 随之作废（消息仍处于未确认状态）。
func (ch *channel) rollbackTx() {
	ch.mu.Lock()
	ch.txOps = nil
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
// 调用时机已由 finishPublish 保证：持久消息会先等到落盘（见 PublishResult.Durable），
// 因此"收到 confirm = 已按当前 fsync 档位持久化"这一语义成立；
// 瞬时消息或 os 档位下则是"已入队（内存 / 已 write）"，与 RabbitMQ 经典队列一致。
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

	args, err := spec.EncodeBasicCancel(entry.tag, false)
	if err != nil {
		return
	}
	// 服务端发起的取消用 Basic.Cancel（而非 Cancel-Ok），no-wait 位由编码器写出
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

// ack 处理 basic.ack（正常消费完成）。事务模式下只缓冲，commit 时才真正生效。
func (ch *channel) ack(tag uint64, multiple bool) error {
	if ch.inTx() {
		ch.bufferSettle(plugin.SettleAck, tag, multiple)
		return nil
	}
	return ch.settle(plugin.SettleAck, tag, multiple)
}

// nack 处理 basic.nack / basic.reject。事务模式下只缓冲，commit 时才真正生效。
func (ch *channel) nack(tag uint64, multiple, requeue bool) error {
	action := plugin.SettleReject
	if requeue {
		action = plugin.SettleRequeue
	}
	if ch.inTx() {
		ch.bufferSettle(action, tag, multiple)
		return nil
	}
	return ch.settle(action, tag, multiple)
}

// settle 应用一次结算：取出未确认投递，并把动作交给内核。
func (ch *channel) settle(action plugin.SettleAction, tag uint64, multiple bool) error {
	targets, err := ch.takeUnacked(tag, multiple)
	if err != nil {
		return err
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

	// multiple=true：tag==0 表示"该通道上全部未确认投递"（RabbitMQ 语义）；
	// 非零 tag 表示"所有 ≤ tag 的未确认投递"。此前 tag=0 时 `t <= 0` 恒不成立，会静默不结算。
	var out []*plugin.Delivery
	for t, d := range ch.unacked {
		if tag == 0 || t <= tag {
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

// kindIs 判断内核错误是否为指定语义分类。
//
// 用于"个别方法需要吞掉某一类软错误"的路径（例：queue.delete 的幂等语义，见 channel_methods.go）：
// 只有确知 Kind 时才放行，其余错误照旧走 fail 的作用域判定，避免把真错误也当成成功。
func kindIs(err error, kind plugin.ErrorKind) bool {
	var ke *plugin.Error
	return errors.As(err, &ke) && ke.Kind == kind
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
