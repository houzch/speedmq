package amqp091

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/internal/protocol/spec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// handle 分发本通道收到的方法。
func (ch *channel) handle(m spec.Method) error {
	switch m.ClassID {
	case spec.ClassChannel:
		return ch.handleChannel(m)
	case spec.ClassExchange:
		return ch.handleExchange(m)
	case spec.ClassQueue:
		return ch.handleQueue(m)
	case spec.ClassBasic:
		return ch.handleBasic(m)
	case spec.ClassConfirm:
		return ch.handleConfirm(m)
	case spec.ClassTx:
		return ch.handleTx(m)
	default:
		return ch.con.failConnection(spec.NotImplemented,
			fmt.Sprintf("NOT_IMPLEMENTED - 尚未支持 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// handleConfirm 处理发布确认（class 85）。
func (ch *channel) handleConfirm(m spec.Method) error {
	switch m.MethodID {
	case spec.MethodConfirmSelect:
		noWait, err := spec.DecodeConfirmSelect(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Confirm.Select", err), m.ClassID, m.MethodID)
		}
		if err := ch.enableConfirm(); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if noWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassConfirm, spec.MethodConfirmSelectOk,
			spec.EncodeConfirmSelectOk())
	default:
		return ch.con.failConnection(spec.NotImplemented,
			fmt.Sprintf("NOT_IMPLEMENTED - 尚未支持 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// handleTx 处理事务（class 90）。
//
// 事务的全部语义都在协议层：publish 与 ack 先进缓冲，Tx.Commit 时按调用顺序应用，
// Tx.Rollback 时整体丢弃。内核 Session 对此一无所知 —— 这也是它能零改动接入的原因。
func (ch *channel) handleTx(m spec.Method) error {
	switch m.MethodID {
	case spec.MethodTxSelect:
		noWait, err := spec.DecodeTxSelect(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Tx.Select", err), m.ClassID, m.MethodID)
		}
		if err := ch.enableTx(); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if noWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassTx, spec.MethodTxSelectOk, spec.EncodeTxSelectOk())

	case spec.MethodTxCommit:
		if !ch.inTx() {
			return ch.fail(plugin.Errorf(plugin.KindPreconditionFailed,
				"PRECONDITION_FAILED - channel is not transactional"), m.ClassID, m.MethodID)
		}
		sess := ch.con.sessionOrNil()
		if sess == nil {
			return ch.fail(plugin.Errorf(plugin.KindInternal,
				"INTERNAL_ERROR - vhost 会话未打开"), m.ClassID, m.MethodID)
		}
		if err := ch.commitTx(sess); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		return ch.con.sendMethod(ch.id, spec.ClassTx, spec.MethodTxCommitOk, spec.EncodeTxCommitOk())

	case spec.MethodTxRollback:
		if !ch.inTx() {
			return ch.fail(plugin.Errorf(plugin.KindPreconditionFailed,
				"PRECONDITION_FAILED - channel is not transactional"), m.ClassID, m.MethodID)
		}
		ch.rollbackTx()
		return ch.con.sendMethod(ch.id, spec.ClassTx, spec.MethodTxRollbackOk, spec.EncodeTxRollbackOk())

	default:
		return ch.con.failConnection(spec.NotImplemented,
			fmt.Sprintf("NOT_IMPLEMENTED - 尚未支持 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// release 释放本通道占用的资源：取消消费者、把未确认投递重新入队。
// 通道关闭与连接关闭都必须调用，否则未确认消息会滞留在 unacked 集合里。
func (ch *channel) release() {
	ch.stopConfirms() // M0：停掉发布确认器（消息已投给内核，此处只是收尾协程）
	if sess := ch.con.sessionOrNil(); sess != nil {
		ch.cancelAllConsumers(sess)
	}
	ch.drainPending()
	ch.requeueAll()
}

// ---------------------------------------------------------------------------
// Channel (20)
// ---------------------------------------------------------------------------

func (ch *channel) handleChannel(m spec.Method) error {
	switch m.MethodID {
	case spec.MethodChannelClose:
		reply, err := spec.DecodeChannelClose(m.Args)
		if err != nil {
			return ch.con.failConnection(spec.SyntaxError, "Channel.Close 解析失败: "+err.Error(),
				m.ClassID, m.MethodID)
		}
		ch.log.Debug("channel 已由客户端关闭", "code", reply.Code, "text", reply.Text)
		ch.con.dropChannel(ch.id)
		return ch.con.sendMethod(ch.id, spec.ClassChannel, spec.MethodChannelCloseOk,
			spec.EncodeChannelCloseOk())

	case spec.MethodChannelCloseOk:
		// 我方发起的 Channel.Close 已获确认
		return nil

	case spec.MethodChannelFlow:
		active, err := spec.DecodeChannelFlow(m.Args)
		if err != nil {
			return ch.fail(plugin.Errorf(plugin.KindPreconditionFailed,
				"PRECONDITION_FAILED - Channel.Flow 解析失败: %v", err), m.ClassID, m.MethodID)
		}
		// 不做服务端限流，但必须回 Flow-Ok，否则客户端会一直等待
		return ch.con.sendMethod(ch.id, spec.ClassChannel, spec.MethodChannelFlowOk,
			spec.EncodeChannelFlowOk(active))

	case spec.MethodChannelFlowOk:
		return nil

	default:
		return ch.fail(plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 未预期的通道方法 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// ---------------------------------------------------------------------------
// Exchange (40)
// ---------------------------------------------------------------------------

func (ch *channel) handleExchange(m spec.Method) error {
	sess := ch.con.sessionOrNil()
	if sess == nil {
		return ch.con.failConnection(spec.CommandInvalid, "COMMAND_INVALID - vhost 未打开",
			m.ClassID, m.MethodID)
	}

	switch m.MethodID {
	case spec.MethodExchangeDeclare:
		req, err := spec.DecodeExchangeDeclare(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Exchange.Declare", err), m.ClassID, m.MethodID)
		}
		err = sess.DeclareExchange(plugin.ExchangeDeclare{
			Name:       req.Exchange,
			Type:       plugin.ExchangeType(req.Type),
			Passive:    req.Passive,
			Durable:    req.Durable,
			AutoDelete: req.AutoDelete,
			Internal:   req.Internal,
			Arguments:  map[string]any(req.Arguments),
		})
		if err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if req.NoWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassExchange, spec.MethodExchangeDeclareOk,
			spec.EncodeExchangeDeclareOk())

	case spec.MethodExchangeDelete:
		req, err := spec.DecodeExchangeDelete(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Exchange.Delete", err), m.ClassID, m.MethodID)
		}
		if err := sess.DeleteExchange(req.Exchange, req.IfUnused); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if req.NoWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassExchange, spec.MethodExchangeDeleteOk,
			spec.EncodeExchangeDeleteOk())

	case spec.MethodExchangeBind:
		req, err := spec.DecodeExchangeBind(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Exchange.Bind", err), m.ClassID, m.MethodID)
		}
		if err := sess.BindExchange(req.Destination, req.Source, req.RoutingKey, map[string]any(req.Arguments)); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if req.NoWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassExchange, spec.MethodExchangeBindOk,
			spec.EncodeExchangeBindOk())

	case spec.MethodExchangeUnbind:
		req, err := spec.DecodeExchangeUnbind(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Exchange.Unbind", err), m.ClassID, m.MethodID)
		}
		if err := sess.UnbindExchange(req.Destination, req.Source, req.RoutingKey, map[string]any(req.Arguments)); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if req.NoWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassExchange, spec.MethodExchangeUnbindOk,
			spec.EncodeExchangeUnbindOk())

	default:
		return ch.con.failConnection(spec.NotImplemented,
			fmt.Sprintf("NOT_IMPLEMENTED - 尚未支持 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// ---------------------------------------------------------------------------
// Queue (50)
// ---------------------------------------------------------------------------

func (ch *channel) handleQueue(m spec.Method) error {
	sess := ch.con.sessionOrNil()
	if sess == nil {
		return ch.con.failConnection(spec.CommandInvalid, "COMMAND_INVALID - vhost 未打开",
			m.ClassID, m.MethodID)
	}

	switch m.MethodID {
	case spec.MethodQueueDeclare:
		req, err := spec.DecodeQueueDeclare(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Queue.Declare", err), m.ClassID, m.MethodID)
		}
		// 伪队列名的声明被 RabbitMQ 一律当作"被动探测"：不建真队列、一定回
		// message_count=0 / consumer_count=1（实测，与是否已有伪队列消费者无关）。
		if req.Queue == directReplyQueue {
			if req.NoWait {
				return nil
			}
			args, err := spec.EncodeQueueDeclareOk(directReplyQueue, 0, 1)
			if err != nil {
				return ch.fail(internalErr(err), m.ClassID, m.MethodID)
			}
			return ch.con.sendMethod(ch.id, spec.ClassQueue, spec.MethodQueueDeclareOk, args)
		}
		info, err := sess.DeclareQueue(plugin.QueueDeclare{
			Name:       req.Queue,
			Passive:    req.Passive,
			Durable:    req.Durable,
			Exclusive:  req.Exclusive,
			AutoDelete: req.AutoDelete,
			Arguments:  map[string]any(req.Arguments),
		})
		if err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if req.NoWait {
			return nil
		}
		args, err := spec.EncodeQueueDeclareOk(info.Name, info.MessageCount, info.ConsumerCount)
		if err != nil {
			return ch.fail(internalErr(err), m.ClassID, m.MethodID)
		}
		return ch.con.sendMethod(ch.id, spec.ClassQueue, spec.MethodQueueDeclareOk, args)

	case spec.MethodQueueBind:
		req, err := spec.DecodeQueueBind(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Queue.Bind", err), m.ClassID, m.MethodID)
		}
		if err := sess.BindQueue(req.Queue, req.Exchange, req.RoutingKey, map[string]any(req.Arguments)); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if req.NoWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassQueue, spec.MethodQueueBindOk, spec.EncodeQueueBindOk())

	case spec.MethodQueueUnbind:
		req, err := spec.DecodeQueueUnbind(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Queue.Unbind", err), m.ClassID, m.MethodID)
		}
		if err := sess.UnbindQueue(req.Queue, req.Exchange, req.RoutingKey, map[string]any(req.Arguments)); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		// Queue.Unbind 没有 no-wait 字段，必须回 Unbind-Ok
		return ch.con.sendMethod(ch.id, spec.ClassQueue, spec.MethodQueueUnbindOk, spec.EncodeQueueUnbindOk())

	case spec.MethodQueuePurge:
		req, err := spec.DecodeQueuePurge(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Queue.Purge", err), m.ClassID, m.MethodID)
		}
		n, err := sess.PurgeQueue(req.Queue)
		if err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		if req.NoWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassQueue, spec.MethodQueuePurgeOk, spec.EncodeQueuePurgeOk(n))

	case spec.MethodQueueDelete:
		req, err := spec.DecodeQueueDelete(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Queue.Delete", err), m.ClassID, m.MethodID)
		}
		// 伪队列名没有真队列可删：RabbitMQ 一律回 Delete-Ok(0)（实测）。
		// 这里也不动本通道的伪队列消费者 —— 实测两者互不影响。
		if req.Queue == directReplyQueue {
			if req.NoWait {
				return nil
			}
			return ch.con.sendMethod(ch.id, spec.ClassQueue, spec.MethodQueueDeleteOk,
				spec.EncodeQueueDeleteOk(0))
		}
		info, err := sess.DeleteQueue(req.Queue, req.IfUnused, req.IfEmpty)
		if err != nil {
			// queue.delete 的语义是**幂等**的：删不存在的队列回 Delete-Ok(0)，而不是 404
			// （RabbitMQ 4.3 实测）。404 会把客户端 Channel 关掉，破坏"清理残留队列"这种
			// 常见写法。只吞"队列不存在"这一种：权限不足（403）、独占占用（405）等
			// 仍照旧按作用域处理。管理面 DELETE /api/queues 对不存在对象回 404 是另一条契约，
			// 由 management 层单独保持（见 api.go 的 deleteQueue）。
			if !kindIs(err, plugin.KindNotFound) {
				return ch.fail(err, m.ClassID, m.MethodID)
			}
			info = plugin.QueueInfo{}
		}
		if req.NoWait {
			return nil
		}
		return ch.con.sendMethod(ch.id, spec.ClassQueue, spec.MethodQueueDeleteOk,
			spec.EncodeQueueDeleteOk(info.MessageCount))

	default:
		return ch.con.failConnection(spec.NotImplemented,
			fmt.Sprintf("NOT_IMPLEMENTED - 尚未支持 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// ---------------------------------------------------------------------------
// Basic (60)
// ---------------------------------------------------------------------------

func (ch *channel) handleBasic(m spec.Method) error {
	sess := ch.con.sessionOrNil()
	if sess == nil {
		return ch.con.failConnection(spec.CommandInvalid, "COMMAND_INVALID - vhost 未打开",
			m.ClassID, m.MethodID)
	}

	switch m.MethodID {
	case spec.MethodBasicQos:
		req, err := spec.DecodeBasicQos(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Qos", err), m.ClassID, m.MethodID)
		}
		if req.Global {
			// 对齐 RabbitMQ 4.3：全局 QoS 已被移除，必须明确报错而不是静默降级
			return ch.con.failConnection(spec.NotImplemented,
				"NOT_IMPLEMENTED - global QoS 已移除，请使用 per-consumer prefetch",
				m.ClassID, m.MethodID)
		}
		if req.PrefetchSize > 0 {
			return ch.con.failConnection(spec.NotImplemented,
				"NOT_IMPLEMENTED - prefetch-size 不受支持，请使用 prefetch-count",
				m.ClassID, m.MethodID)
		}
		ch.mu.Lock()
		ch.prefetch = req.PrefetchCount
		ch.mu.Unlock()
		return ch.con.sendMethod(ch.id, spec.ClassBasic, spec.MethodBasicQosOk, spec.EncodeBasicQosOk())

	case spec.MethodBasicConsume:
		return ch.handleConsume(sess, m)

	case spec.MethodBasicCancel:
		req, err := spec.DecodeBasicCancel(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Cancel", err), m.ClassID, m.MethodID)
		}
		ch.mu.Lock()
		_, known := ch.consumers[req.ConsumerTag]
		ch.mu.Unlock()
		if !known {
			// 客户端自己的标签空间里没有该消费者：按 404 处理
			return ch.fail(plugin.Errorf(plugin.KindNotFound,
				"NOT_FOUND - no consumer with tag '%s'", req.ConsumerTag), m.ClassID, m.MethodID)
		}
		if err := sess.Cancel(req.ConsumerTag); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		// 伪队列消费者被取消后，本通道不能再改写 reply_to（对齐 RabbitMQ）
		ch.forgetDirectReply(req.ConsumerTag)
		ch.mu.Lock()
		delete(ch.consumers, req.ConsumerTag)
		ch.mu.Unlock()
		if req.NoWait {
			return nil
		}
		args, err := spec.EncodeBasicCancelOk(req.ConsumerTag)
		if err != nil {
			return ch.fail(internalErr(err), m.ClassID, m.MethodID)
		}
		return ch.con.sendMethod(ch.id, spec.ClassBasic, spec.MethodBasicCancelOk, args)

	case spec.MethodBasicPublish:
		req, err := spec.DecodeBasicPublish(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Publish", err), m.ClassID, m.MethodID)
		}
		if err := ch.startPublish(req); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		return nil

	case spec.MethodBasicGet:
		req, err := spec.DecodeBasicGet(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Get", err), m.ClassID, m.MethodID)
		}
		return ch.handleGet(sess, req, m)

	case spec.MethodBasicAck:
		req, err := spec.DecodeBasicAck(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Ack", err), m.ClassID, m.MethodID)
		}
		if err := ch.ack(req.DeliveryTag, req.Multiple); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		return nil

	case spec.MethodBasicNack:
		req, err := spec.DecodeBasicNack(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Nack", err), m.ClassID, m.MethodID)
		}
		if err := ch.nack(req.DeliveryTag, req.Multiple, req.Requeue); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		return nil

	case spec.MethodBasicReject:
		req, err := spec.DecodeBasicReject(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Reject", err), m.ClassID, m.MethodID)
		}
		if err := ch.nack(req.DeliveryTag, false, req.Requeue); err != nil {
			return ch.fail(err, m.ClassID, m.MethodID)
		}
		return nil

	case spec.MethodBasicRecover:
		requeue, err := spec.DecodeBasicRecover(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Recover", err), m.ClassID, m.MethodID)
		}
		ch.recover(requeue)
		return ch.con.sendMethod(ch.id, spec.ClassBasic, spec.MethodBasicRecoverOk,
			spec.EncodeBasicRecoverOk())

	case spec.MethodBasicRecoverAsync:
		requeue, err := spec.DecodeBasicRecover(m.Args)
		if err != nil {
			return ch.fail(syntaxErr("Basic.Recover-Async", err), m.ClassID, m.MethodID)
		}
		ch.recover(requeue)
		return nil

	default:
		return ch.con.failConnection(spec.NotImplemented,
			fmt.Sprintf("NOT_IMPLEMENTED - 尚未支持 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// handleConsume 处理 Basic.Consume。
//
// 两条顺序约束，都关系到客户端能不能正确解析投递：
//  1. 先在本地登记消费者（此时投递会被**暂存**），再向内核注册 —— 避免注册成功后
//     投递在客户端还没准备好时到达；
//  2. **先回 Consume-Ok、再放行暂存投递** —— 客户端要先用 Ok 里的 consumer-tag 建立
//     本地消费者表，之后收到的 Basic.Deliver 才认得出来。反过来的话，投递会先于
//     Consume-Ok 到达，客户端把它判成 "unsolicited delivery" 并断开连接。
//
// 第 2 条是 M8-8 的 Java 用例抓到的真实缺陷：amqp-client 在 consumer-tag 为空时
// **由服务端生成** tag（自己不知道 tag 是什么，只能等 Consume-Ok），因此必然踩中；
// 而 pika / amqp091-go 会自行生成 tag，侥幸避开了这个竞态。原先的实现是
// openGate() 写在 sendMethod(Consume-Ok) 之前，两个协程抢着往同一条连接写帧。
func (ch *channel) handleConsume(sess plugin.Session, m spec.Method) error {
	req, err := spec.DecodeBasicConsume(m.Args)
	if err != nil {
		return ch.fail(syntaxErr("Basic.Consume", err), m.ClassID, m.MethodID)
	}

	// direct reply-to 伪队列不是真队列：它由本通道的一条独占队列承载，走单独的处理路径。
	if req.Queue == directReplyQueue {
		return ch.handleReplyConsume(sess, req, m)
	}

	priority, err := consumerPriority(req.Arguments, req.Queue, ch.con.identity.VHost)
	if err != nil {
		return ch.fail(err, m.ClassID, m.MethodID)
	}

	tag := req.ConsumerTag
	if tag == "" {
		tag = serverConsumerTag()
	}

	entry := ch.registerConsumer(tag, req.Queue, req.NoAck, req.Exclusive)
	sub := plugin.Subscription{
		Tag:       tag,
		Queue:     req.Queue,
		NoAck:     req.NoAck,
		Exclusive: req.Exclusive,
		Prefetch:  ch.prefetchCount(),
		Priority:  priority,
		Deliver: func(d *plugin.Delivery) error {
			return entry.deliver(d)
		},
		Cancel: func(reason string) {
			ch.serverCancel(entry, reason)
		},
	}
	if _, err := sess.Consume(sub); err != nil {
		ch.unregisterConsumer(tag)
		return ch.fail(err, m.ClassID, m.MethodID)
	}

	// nowait 时没有 Consume-Ok 可等，客户端必须自己知道 tag（规范要求它显式给出），
	// 因此直接放行；否则必须在 Consume-Ok **之后**才放行暂存的投递。
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

// handleGet 处理 Basic.Get。
func (ch *channel) handleGet(sess plugin.Session, req spec.BasicGet, m spec.Method) error {
	d, ok, err := sess.Get(req.Queue, req.NoAck)
	if err != nil {
		return ch.fail(err, m.ClassID, m.MethodID)
	}
	if !ok {
		// 空队列必须回 Get-Empty，而不是错误
		return ch.con.sendMethod(ch.id, spec.ClassBasic, spec.MethodBasicGetEmpty, spec.EncodeBasicGetEmpty())
	}

	// delivery-tag 必须非零：部分客户端（如 amqp091-go）用 tag==0 表示"拉取到空队列"。
	// RabbitMQ 无论 autoAck 与否都分配非零 tag，这里保持一致。
	tag := ch.assignTag()
	if !req.NoAck {
		ch.mu.Lock()
		ch.unacked[tag] = d
		ch.mu.Unlock()
	}
	if err := ch.sendGetOk(tag, d, 0); err != nil {
		if !req.NoAck {
			ch.discardUnacked(tag)
		}
		return err
	}
	return nil
}

// prefetchCount 返回当前通道的 prefetch 额度。
func (ch *channel) prefetchCount() uint16 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.prefetch
}

// maybeFinishContent 在内容收齐后交给内核发布。
func (ch *channel) maybeFinishContent() error {
	ch.mu.Lock()
	done := ch.pending != nil && ch.pending.received >= ch.pending.header.BodySize
	ch.mu.Unlock()
	if !done {
		return nil
	}
	if err := ch.finishPublish(); err != nil {
		return ch.fail(err, spec.ClassBasic, spec.MethodBasicPublish)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 消费者条目：带"就绪闸门"与暂存队列
// ---------------------------------------------------------------------------

// consumerEntry 是本通道上的一条消费者记录。
//
// 闸门用于保证 Consume-Ok 一定先于第一条 Basic.Deliver 发出：
// 闸门关闭期间收到的投递先暂存，放行后按序补发。
type consumerEntry struct {
	tag     string
	queue   string
	noAck   bool
	channel *channel

	mu      sync.Mutex
	ready   bool
	pending []*plugin.Delivery
}

// registerConsumer 登记消费者（闸门初始为关闭）。
func (ch *channel) registerConsumer(tag, queue string, noAck, exclusive bool) *consumerEntry {
	entry := &consumerEntry{tag: tag, queue: queue, noAck: noAck, channel: ch}
	ch.mu.Lock()
	ch.consumers[tag] = entry
	ch.mu.Unlock()
	return entry
}

// unregisterConsumer 撤销登记（注册失败时回滚）。
func (ch *channel) unregisterConsumer(tag string) {
	ch.mu.Lock()
	delete(ch.consumers, tag)
	ch.mu.Unlock()
}

// openGate 放行暂存投递。
func (e *consumerEntry) openGate() {
	e.mu.Lock()
	e.ready = true
	buffered := e.pending
	e.pending = nil
	e.mu.Unlock()

	for _, d := range buffered {
		if err := e.deliver(d); err != nil {
			e.channel.log.Debug("补发暂存投递失败", "consumer_tag", e.tag, "err", err)
			return
		}
	}
}

// deliver 投递一条消息：闸门未开时暂存。
func (e *consumerEntry) deliver(d *plugin.Delivery) error {
	e.mu.Lock()
	if !e.ready {
		e.pending = append(e.pending, d)
		e.mu.Unlock()
		return nil
	}
	e.mu.Unlock()
	return e.channel.deliver(e, d)
}

// drainPending 把暂存的投递全部退回内核（连接/通道关闭时，避免消息滞留）。
func (ch *channel) drainPending() {
	ch.mu.Lock()
	entries := make([]*consumerEntry, 0, len(ch.consumers))
	for _, e := range ch.consumers {
		entries = append(entries, e)
	}
	ch.mu.Unlock()

	for _, e := range entries {
		e.mu.Lock()
		buffered := e.pending
		e.pending = nil
		e.mu.Unlock()
		for _, d := range buffered {
			d.Settle(plugin.SettleRequeue)
		}
	}
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func syntaxErr(what string, err error) error {
	return plugin.Errorf(plugin.KindPreconditionFailed,
		"PRECONDITION_FAILED - %s 解析失败: %v", what, err)
}

func internalErr(err error) error {
	return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - %v", err)
}

// serverConsumerTag 生成服务端消费者标签（客户端未指定时使用）。
func serverConsumerTag() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("amq.ctag-%d", time.Now().UnixNano())
	}
	return "amq.ctag-" + hex.EncodeToString(b)
}

// consumerPriority 从 basic.consume 的 arguments 里读消费者优先级 x-priority。
//
// 对齐 RabbitMQ（实测 4.3）：缺省为 0；显式给出但不是整数时是 channel 级 406，
// 且报文里带上队列名与 vhost（照抄它的措辞，便于两侧对照排查）。
// 只校验类型不校验范围 —— 实测 300 / -1 都被接受，因此这里用 int 原样承载。
func consumerPriority(args codec.Table, queue, vhost string) (int, error) {
	v, ok := args["x-priority"]
	if !ok {
		return 0, nil
	}
	p, ok := asPriority(v)
	if !ok {
		return 0, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - invalid arg 'x-priority' for queue '%s' in vhost '%s': "+
				"\"expected integer, got %s\"", queue, vhost, fieldTypeName(v))
	}
	return p, nil
}

// asPriority 把 field-table 的整数值转成优先级；AMQP 的整型有符号/无符号都算整数。
func asPriority(v any) (int, bool) {
	switch n := v.(type) {
	case int8:
		return int(n), true
	case int16:
		return int(n), true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case uint8:
		return int(n), true
	case uint16:
		return int(n), true
	case uint32:
		return int(n), true
	default:
		return 0, false
	}
}

// fieldTypeName 给出 field-table 值在错误报文里的类型名（与 RabbitMQ 的措辞对齐）。
func fieldTypeName(v any) string {
	switch v.(type) {
	case bool:
		return "bool"
	case string:
		return "longstr"
	case []byte:
		return "byte_array"
	case codec.Decimal:
		return "decimal"
	case float32:
		return "float"
	case float64:
		return "double"
	case codec.Table:
		return "table"
	case []any:
		return "array"
	case time.Time:
		return "timestamp"
	case nil:
		return "void"
	default:
		return "unknown"
	}
}
