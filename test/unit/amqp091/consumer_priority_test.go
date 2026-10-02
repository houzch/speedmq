package amqp091_test

import (
	"strings"
	"testing"
)

// 本文件覆盖 M8-14 的消费者优先级（basic.consume 的 `x-priority`）。
//
// 规则由**实测 RabbitMQ 4.3**得到：
//   - 多个消费者同时**还有投递额度**时，优先级高的先拿消息；同优先级之间轮询；
//   - 高优先级消费者没有额度（prefetch 用尽）时，低优先级消费者照常收到消息，不会被饿死；
//   - `x-priority` 不是整数时是 channel 级 406（报文里带队列名与 vhost）。

func TestConsumerPriorityRejectsNonInteger(t *testing.T) {
	b := newTestBroker(t)
	c := dial(t, b)
	c.openChannel(1)
	c.queueDeclare(1, "py.prio.bad")

	// longstr 类型的 x-priority：类型不合法，必须 406（而不是被当成 0 静默接受）
	c.sendConsume(1, "py.prio.bad", true, nil, appendTable(nil, entryLongStr("x-priority", "abc")))

	code, text := c.expectChannelClose(1)
	if code != 406 {
		t.Fatalf("x-priority 非法类型应回 406，实际 %d（%s）", code, text)
	}
	if !strings.Contains(text, "invalid arg 'x-priority'") {
		t.Fatalf("错误文案应指出 x-priority 非法，实际 %q", text)
	}
}

func TestConsumerPriorityPrefersHigherPriorityConsumer(t *testing.T) {
	b := newTestBroker(t)

	high := dial(t, b)
	high.openChannel(1)
	high.queueDeclare(1, "py.prio.q")
	prioHigh := 10
	high.consume(1, "py.prio.q", true, &prioHigh, nil)

	low := dial(t, b)
	low.openChannel(1)
	prioLow := 1
	low.consume(1, "py.prio.q", true, &prioLow, nil)

	pub := dial(t, b)
	pub.openChannel(1)
	pub.queueDeclare(1, "py.prio.q")

	const total = 30
	for i := 0; i < total; i++ {
		pub.publish(1, "", "py.prio.q", publishProps{}, []byte("m"))
	}
	// 屏障：同一条连接上的方法按顺序处理，读到 Declare-Ok 说明 30 条发布都已投递完
	pub.queueDeclare(1, "py.prio.q")

	if got := len(high.drainDeliveries(1)); got != total {
		t.Fatalf("高优先级消费者应拿到全部 %d 条，实际 %d 条", total, got)
	}
	low.expectNoDelivery(1)
}

func TestConsumerPriorityEqualPriorityRoundRobins(t *testing.T) {
	// 同优先级之间是轮询（实测 RabbitMQ：两个默认优先级消费者各拿一半）
	b := newTestBroker(t)

	a := dial(t, b)
	a.openChannel(1)
	a.queueDeclare(1, "py.prio.eq")
	prio := 5
	a.consume(1, "py.prio.eq", true, &prio, nil)

	c := dial(t, b)
	c.openChannel(1)
	c.consume(1, "py.prio.eq", true, &prio, nil)

	pub := dial(t, b)
	pub.openChannel(1)
	pub.queueDeclare(1, "py.prio.eq")
	const total = 20
	for i := 0; i < total; i++ {
		pub.publish(1, "", "py.prio.eq", publishProps{}, []byte("m"))
	}
	pub.queueDeclare(1, "py.prio.eq")

	gotA, gotC := len(a.drainDeliveries(1)), len(c.drainDeliveries(1))
	if gotA+gotC != total {
		t.Fatalf("同优先级的两个消费者应共收到 %d 条，实际 %d + %d", total, gotA, gotC)
	}
	if gotA == 0 || gotC == 0 {
		t.Fatalf("同优先级必须轮询，实际分配 %d / %d", gotA, gotC)
	}
}
