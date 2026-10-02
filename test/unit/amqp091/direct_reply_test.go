package amqp091_test

import (
	"strings"
	"testing"
)

// 本文件覆盖 M8-14 的 direct reply-to（`amq.rabbitmq.reply-to` 伪队列）。
//
// 断言全部落在"客户端观察到什么"上，规则由**实测 RabbitMQ 4.3**得到（见 README 的 M8-14 条目）：
//   - 伪队列是**每 channel** 的；
//   - 必须 no-ack 消费（no-ack=false → 406 reply consumer cannot acknowledge）；
//   - 同一个 channel 上重复消费 → 406 reply consumer already set；
//   - 请求的 reply_to 被改写成该 channel 的应答队列名，应答方照原样发布即可回到发起方；
//   - 本 channel 没有伪队列消费者却发布带该 reply_to 的消息 → 406 fast reply consumer does not exist。

const (
	errReplyNoAck    = "reply consumer cannot acknowledge"
	errReplyAlready  = "reply consumer already set"
	errFastReplyNone = "fast reply consumer does not exist"
)

func TestDirectReplyToConsumeRequiresNoAck(t *testing.T) {
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)

	// no-ack=false：伪队列没有"未确认消息"的概念，必须被拒
	c.sendConsume(1, replyQueue, false, nil, nil)
	code, text := c.expectChannelClose(1)
	if code != 406 {
		t.Fatalf("no-ack=false 消费伪队列应回 406，实际 %d（%s）", code, text)
	}
	if !strings.Contains(text, errReplyNoAck) {
		t.Fatalf("错误文案应含 %q，实际 %q", errReplyNoAck, text)
	}

	// 软错误：连接与后续 channel 都还能用
	c.openChannel(2)
	c.queueDeclare(2, "py.drt.alive")
}

func TestDirectReplyToConsumeTwiceOnSameChannelRejected(t *testing.T) {
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)

	c.consume(1, replyQueue, true, nil, nil)
	c.sendConsume(1, replyQueue, true, nil, nil) // 同一个 channel 第二次消费

	code, text := c.expectChannelClose(1)
	if code != 406 {
		t.Fatalf("同一 channel 重复消费伪队列应回 406，实际 %d（%s）", code, text)
	}
	if !strings.Contains(text, errReplyAlready) {
		t.Fatalf("错误文案应含 %q，实际 %q", errReplyAlready, text)
	}
}

func TestDirectReplyToPublishWithoutConsumerRejected(t *testing.T) {
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)
	c.queueDeclare(1, "py.drt.noconsumer")

	// 本 channel 没有伪队列消费者：带 reply_to=amq.rabbitmq.reply-to 的发布必须被拒
	c.publish(1, "", "py.drt.noconsumer", publishProps{replyTo: replyQueue, correlationID: "c1"}, []byte("ping"))

	code, text := c.expectChannelClose(1)
	if code != 406 {
		t.Fatalf("未消费伪队列就发布应回 406，实际 %d（%s）", code, text)
	}
	if !strings.Contains(text, errFastReplyNone) {
		t.Fatalf("错误文案应含 %q，实际 %q", errFastReplyNone, text)
	}
}

func TestDirectReplyToIsPerChannel(t *testing.T) {
	// 实测：伪队列是每 channel 的 —— 在 channel 1 上消费，channel 2 上发布请求同样会被拒。
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)
	c.openChannel(2)
	c.queueDeclare(1, "py.drt.perchannel")
	c.queueDeclare(2, "py.drt.other")

	c.consume(1, replyQueue, true, nil, nil)
	c.publish(2, "", "py.drt.perchannel", publishProps{replyTo: replyQueue, correlationID: "c1"}, []byte("ping"))

	code, text := c.expectChannelClose(2)
	if code != 406 {
		t.Fatalf("跨 channel 发布（本 channel 未消费伪队列）应回 406，实际 %d（%s）", code, text)
	}
	if !strings.Contains(text, errFastReplyNone) {
		t.Fatalf("错误文案应含 %q，实际 %q", errFastReplyNone, text)
	}
	// 软错误只影响 channel 2：channel 1 上的伪队列消费者与后续操作都照常
	if tag := c.consume(1, "py.drt.other", true, nil, nil); tag == "" {
		t.Fatal("channel 1 不应受 channel 2 的软错误影响")
	}
}

func TestDirectReplyToRoundTripAcrossConnections(t *testing.T) {
	// 真实 RPC 往返：发起方消费伪队列 → 发布请求（reply_to=amq.rabbitmq.reply-to）→
	// 服务端（**另一条连接**）消费请求并按 reply_to 回发 → 发起方收到应答。
	//
	// 服务端必须能从请求里读出可路由的应答队列名：这正是属性改写的意义所在。
	b := newTestBroker(t)

	server := dial(t, b)
	server.openChannel(1)
	server.queueDeclare(1, "py.drt.rpc")
	server.consume(1, "py.drt.rpc", true, nil, nil)

	requester := dial(t, b)
	requester.openChannel(1)
	requester.consume(1, replyQueue, true, nil, nil)

	requester.publish(1, "", "py.drt.rpc",
		publishProps{replyTo: replyQueue, correlationID: "corr-1"}, []byte("ping"))

	req := server.readDelivery(1)
	if string(req.body) != "ping" {
		t.Fatalf("服务端收到的请求体应是 ping，实际 %q", req.body)
	}
	if req.correlation != "corr-1" {
		t.Fatalf("请求的 correlation-id 应原样到达服务端，实际 %q", req.correlation)
	}
	if req.replyTo == replyQueue {
		t.Fatal("请求里的 reply_to 仍是伪队列名：服务端无法应答（属性未被改写）")
	}
	if req.replyTo == "" {
		t.Fatal("请求里缺少 reply_to：服务端无从应答")
	}

	// 服务端按请求里的 reply_to 发布应答（默认交换机 + routing key = reply_to）
	server.publish(1, "", req.replyTo, publishProps{correlationID: req.correlation}, []byte("pong"))

	resp := requester.readDelivery(1)
	if string(resp.body) != "pong" {
		t.Fatalf("发起方应收到 pong，实际 %q", resp.body)
	}
	if resp.correlation != "corr-1" {
		t.Fatalf("应答的 correlation-id 应为 corr-1，实际 %q", resp.correlation)
	}
}

func TestDirectReplyToCancelStopsRewriting(t *testing.T) {
	// 取消伪队列消费者后，本 channel 不再改写 reply_to（对齐 RabbitMQ：
	// 应答队列随消费者一起消失），此后再发布带该 reply_to 的消息就是 406。
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)
	c.queueDeclare(1, "py.drt.cancel")

	tag := c.consume(1, replyQueue, true, nil, nil)
	c.cancel(1, tag)
	c.publish(1, "", "py.drt.cancel", publishProps{replyTo: replyQueue}, []byte("ping"))

	code, text := c.expectChannelClose(1)
	if code != 406 || !strings.Contains(text, errFastReplyNone) {
		t.Fatalf("取消伪队列消费者后发布应回 406（%s），实际 %d（%s）", errFastReplyNone, code, text)
	}
}

func TestDirectReplyToQueueDeclareAndDelete(t *testing.T) {
	// 实测：对伪队列名的 declare 一律当"被动探测"（Ok + message_count=0 / consumer_count=1），
	// delete 回 Delete-Ok(0)；两者都不真的创建/删除队列。
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)

	count, consumers := c.queueDeclare(1, replyQueue)
	if count != 0 || consumers != 1 {
		t.Fatalf("伪队列的 declare 应答应为 (0, 1)，实际 (%d, %d)", count, consumers)
	}
	if got := c.queueDelete(1, replyQueue); got != 0 {
		t.Fatalf("伪队列的 delete 应答应为 0 条，实际 %d", got)
	}

	// declare/delete 都不影响本 channel 的伪队列消费者：应答照常回到它
	c.consume(1, replyQueue, true, nil, nil)
	other := dial(t, b)
	other.openChannel(1)
	other.queueDeclare(1, "py.drt.after")
	other.consume(1, "py.drt.after", true, nil, nil)
	c.publish(1, "", "py.drt.after", publishProps{replyTo: replyQueue}, []byte("req"))
	req := other.readDelivery(1)
	other.publish(1, "", req.replyTo, publishProps{}, []byte("ok"))
	if got := c.readDelivery(1); string(got.body) != "ok" {
		t.Fatalf("应答应回到发起方，实际 %q", got.body)
	}
}
