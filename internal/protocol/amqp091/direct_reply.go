package amqp091

import (
	"github.com/houzch/speedmq/internal/protocol/spec"
	"github.com/houzch/speedmq/pkg/plugin"
)

// 本文件实现 AMQP 0-9-1 的 direct reply-to 伪队列 `amq.rabbitmq.reply-to`。
//
// 归属说明：这是 AMQP 0-9-1 专有的"快速 RPC 应答"约定（不是协议无关能力），
// 因此实现留在协议层；内核只看到"一条每 channel 独占的普通队列"，不需要知道伪队列的名字。
//
// 下列语义全部由**实测 RabbitMQ 4.3**得到（见 README「M8-14」条目），不是照抄文档：
//
//  1. 伪队列是**每 channel**的：同一连接的两个 channel 各自有一份应答队列，
//     在一个 channel 上消费、在另一个 channel 上发布请求不会被改写、也就回不来；
//  2. 消费必须 no-ack=true，否则 406 `reply consumer cannot acknowledge`；
//     同一个 channel 上第二次消费伪队列 → 406 `reply consumer already set`；
//     `exclusive` / `arguments` / `nowait` 均被接受（不影响语义）；
//  3. 真正让应答回来的机制是**属性改写**：发起方发布请求时把 reply_to 写成
//     `amq.rabbitmq.reply-to`，服务端把它改写成该 channel 的应答队列名；
//     应答方再按这个值作 routing key 发布到默认交换机即可回到发起方；
//  4. 发布"带该 reply_to 的消息"但本 channel 没有伪队列消费者 → 406
//     `PRECONDITION_FAILED - fast reply consumer does not exist`（软错误，只关 channel）；
//  5. 直接把 `amq.rabbitmq.reply-to` 当 routing key 发布**没有**特殊语义：
//     默认交换机下它只是一条没有对应队列的未路由消息。
const directReplyQueue = "amq.rabbitmq.reply-to"

// directReply 记录本通道的应答队列：内核里真实承载应答的那条队列。
//
// 名字由内核生成（`amq.gen-*`），与 RabbitMQ 的 `amq.rabbitmq.reply-to.<...>` 不同，
// 但客户端只会把收到的 reply_to 原样用作 routing key，因此对外行为一致。
type directReply struct {
	// tag 是伪队列消费者的标签（basic.cancel 用它识别）。
	tag string
	// queue 是应答队列名（发布请求时写回 reply_to 属性）。
	queue string
}

// handleReplyConsume 处理对伪队列的 basic.consume。
//
// 与 RabbitMQ 的差别只有一处：应答队列用内核生成的 `amq.gen-*` 名字，
// 且声明为独占（随会话回收）。独占只影响"谁能消费/声明它"，
// 不影响别的连接发布应答 —— 发布路径不检查独占归属，这正是应答方能在另一条连接上的原因。
func (ch *channel) handleReplyConsume(sess plugin.Session, req spec.BasicConsume, m spec.Method) error {
	if !req.NoAck {
		// 伪队列没有"未确认消息"的概念：应答一旦到达就该交付给客户端，无法 ack
		return ch.fail(plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - reply consumer cannot acknowledge"), m.ClassID, m.MethodID)
	}
	if ch.replyQueueName() != "" {
		return ch.fail(plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - reply consumer already set"), m.ClassID, m.MethodID)
	}

	tag := req.ConsumerTag
	if tag == "" {
		tag = serverConsumerTag()
	}

	info, err := sess.DeclareQueue(plugin.QueueDeclare{Exclusive: true, AutoDelete: true})
	if err != nil {
		return ch.fail(err, m.ClassID, m.MethodID)
	}

	entry := ch.registerConsumer(tag, info.Name, true, false)
	sub := plugin.Subscription{
		Tag:   tag,
		Queue: info.Name,
		NoAck: true,
		Deliver: func(d *plugin.Delivery) error {
			return entry.deliver(d)
		},
		Cancel: func(reason string) {
			ch.serverCancel(entry, reason)
		},
	}
	if _, err := sess.Consume(sub); err != nil {
		ch.unregisterConsumer(tag)
		// 尽力回收：注册失败时那条独占队列已没有消费者，删不掉也只随会话回收
		_, _ = sess.DeleteQueue(info.Name, false, false)
		return ch.fail(err, m.ClassID, m.MethodID)
	}

	ch.mu.Lock()
	ch.directReply = &directReply{tag: tag, queue: info.Name}
	ch.mu.Unlock()

	if req.NoWait {
		entry.openGate()
		return nil
	}
	args, err := spec.EncodeBasicConsumeOk(tag)
	if err != nil {
		return ch.fail(internalErr(err), m.ClassID, m.MethodID)
	}
	if err := ch.con.sendMethod(ch.id, spec.ClassBasic, spec.MethodBasicConsumeOk, args); err != nil {
		return err
	}
	entry.openGate()
	return nil
}

// replyQueueName 返回本通道的应答队列名（未启用伪队列时为空）。
func (ch *channel) replyQueueName() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.directReply == nil {
		return ""
	}
	return ch.directReply.queue
}

// forgetDirectReply 在伪队列消费者被取消时清掉本通道的应答队列记录。
//
// 队列本身由内核的 auto-delete 回收（最后一个消费者取消即删），这里不必也不该重复删。
func (ch *channel) forgetDirectReply(tag string) {
	ch.mu.Lock()
	if ch.directReply != nil && ch.directReply.tag == tag {
		ch.directReply = nil
	}
	ch.mu.Unlock()
}

// rewriteReplyTo 实现 direct reply-to 的属性改写。
//
// 请求里的 `reply_to = amq.rabbitmq.reply-to` 是"回我这里"的约定写法，
// 服务端必须把它换成真正可路由的应答队列名，应答方才能照原样发布回来。
// 本通道没有伪队列消费者时按 RabbitMQ 的实测行为报 406（channel 级软错误）。
func (ch *channel) rewriteReplyTo(msg *plugin.Message) error {
	if msg.Properties.ReplyTo != directReplyQueue {
		return nil
	}
	name := ch.replyQueueName()
	if name == "" {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - fast reply consumer does not exist")
	}
	msg.Properties.ReplyTo = name
	return nil
}
