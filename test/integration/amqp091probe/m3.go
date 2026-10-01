package main

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// m3Cases 覆盖 M3 的协议层行为：发布确认、TTL、死信、长度限制。
//
// 这些必须用真实客户端验证 —— confirm 的时序、Basic.Return 与 confirm 的先后、
// x-death 的结构，都是"客户端能看到什么"的问题，内核单测覆盖不到。
func m3Cases() []testCase {
	return []testCase{
		{"M3 发布确认：confirm.select + 逐条 ack + 序号从 1 连续", testConfirmBasic},
		{"M3 TTL 到期进入死信队列（x-death reason=expired）", testTTLToDeadLetter},
		{"M3 nack(requeue=false) 进入死信队列（x-death reason=rejected）", testRejectToDeadLetter},
		{"M3 长度限制 reject-publish：第二条被 basic.nack", testRejectPublishNack},
		{"M3 mandatory 未命中：Basic.Return 必须先于 confirm 到达", testReturnBeforeConfirm},
	}
}

// testConfirmBasic 验证发布确认的序号与 ack 行为。
func testConfirmBasic() error {
	return withChannel(func(ch *amqp.Channel) error {
		qname, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("开启 confirm 失败: %w", err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8))

		const n = 3
		for i := 0; i < n; i++ {
			if err := ch.Publish("", qname, false, false,
				amqp.Publishing{Body: []byte(fmt.Sprintf("c-%d", i))}); err != nil {
				return fmt.Errorf("发布第 %d 条失败: %w", i, err)
			}
		}
		for i := 1; i <= n; i++ {
			select {
			case c := <-confirms:
				if !c.Ack {
					return fmt.Errorf("第 %d 条收到否定确认", i)
				}
				// 序号必须从 1 开始且连续：客户端靠它把自己的未确认集合对上号
				if c.DeliveryTag != uint64(i) {
					return fmt.Errorf("确认序号不连续: got %d want %d", c.DeliveryTag, i)
				}
			case <-time.After(5 * time.Second):
				return fmt.Errorf("等待第 %d 条确认超时", i)
			}
		}
		// 消息应确实进了队列
		if info, err := ch.QueueDeclarePassive(qname, false, false, true, false, nil); err == nil {
			if info.Messages != n {
				return fmt.Errorf("确认已收到但队列里只有 %d 条，期望 %d", info.Messages, n)
			}
		}
		return nil
	})
}

// setupDLX 建立死信拓扑，返回死信队列名。
func setupDLX(ch *amqp.Channel, prefix string) (string, error) {
	dlx := prefix + ".dlx"
	dlq := prefix + ".dlq"
	if err := ch.ExchangeDeclare(dlx, "direct", false, false, false, false, nil); err != nil {
		return "", fmt.Errorf("声明死信交换机失败: %w", err)
	}
	// durable=true：声明"非持久且非独占"的队列已被禁止（对齐 RabbitMQ 4.x，541 硬错误），
	// 而 dlq 要跨 withChannel 的连接存活、不能用 exclusive，因此用持久队列。
	if _, err := ch.QueueDeclare(dlq, true, false, false, false, nil); err != nil {
		return "", fmt.Errorf("声明死信队列失败: %w", err)
	}
	// 清掉上一次运行可能残留的消息，保证断言稳定
	if _, err := ch.QueuePurge(dlq, false); err != nil {
		return "", fmt.Errorf("清空死信队列失败: %w", err)
	}
	if err := ch.QueueBind(dlq, dlq, dlx, false, nil); err != nil {
		return "", fmt.Errorf("绑定死信队列失败: %w", err)
	}
	return dlq, nil
}

// waitMessage 在超时内反复 basic.get，直到取到一条消息。
func waitMessage(ch *amqp.Channel, queue string, timeout time.Duration) (amqp.Delivery, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d, ok, err := ch.Get(queue, true)
		if err != nil {
			return amqp.Delivery{}, fmt.Errorf("get 失败: %w", err)
		}
		if ok {
			return d, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return amqp.Delivery{}, fmt.Errorf("等待消息超时")
}

// checkDeath 校验 x-death 头。
func checkDeath(d amqp.Delivery, wantReason, wantQueue string) error {
	raw, ok := d.Headers["x-death"]
	if !ok {
		return fmt.Errorf("缺少 x-death 头: %#v", d.Headers)
	}
	arr, ok := raw.([]interface{})
	if !ok || len(arr) == 0 {
		return fmt.Errorf("x-death 不是非空数组: %#v", raw)
	}
	first, ok := arr[0].(amqp.Table)
	if !ok {
		return fmt.Errorf("x-death 首项不是表: %#v", arr[0])
	}
	if first["reason"] != wantReason {
		return fmt.Errorf("x-death reason = %v, want %q", first["reason"], wantReason)
	}
	if first["queue"] != wantQueue {
		return fmt.Errorf("x-death queue = %v, want %q", first["queue"], wantQueue)
	}
	return nil
}

func testTTLToDeadLetter() error {
	return withChannel(func(ch *amqp.Channel) error {
		dlq, err := setupDLX(ch, "m3.ttl")
		if err != nil {
			return err
		}
		src := "m3.ttl.src"
		if _, err := ch.QueueDeclare(src, true, false, false, false, amqp.Table{
			"x-message-ttl":             int32(100),
			"x-dead-letter-exchange":    "m3.ttl.dlx",
			"x-dead-letter-routing-key": dlq,
		}); err != nil {
			return fmt.Errorf("声明带 TTL 的队列失败: %w", err)
		}
		if err := ch.Publish("", src, false, false, amqp.Publishing{Body: []byte("expire")}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}

		d, err := waitMessage(ch, dlq, 5*time.Second)
		if err != nil {
			return fmt.Errorf("TTL 到期后消息未进入死信队列: %w", err)
		}
		if string(d.Body) != "expire" {
			return fmt.Errorf("死信内容错误: %q", d.Body)
		}
		return checkDeath(d, "expired", src)
	})
}

func testRejectToDeadLetter() error {
	return withChannel(func(ch *amqp.Channel) error {
		dlq, err := setupDLX(ch, "m3.rej")
		if err != nil {
			return err
		}
		src := "m3.rej.src"
		if _, err := ch.QueueDeclare(src, true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    "m3.rej.dlx",
			"x-dead-letter-routing-key": dlq,
		}); err != nil {
			return fmt.Errorf("声明队列失败: %w", err)
		}
		if err := ch.Publish("", src, false, false, amqp.Publishing{Body: []byte("reject-me")}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}

		d, ok, err := ch.Get(src, false)
		if err != nil || !ok {
			return fmt.Errorf("取源队列消息失败: ok=%v err=%v", ok, err)
		}
		// requeue=false：拒绝并进死信
		if err := d.Nack(false, false); err != nil {
			return fmt.Errorf("nack 失败: %w", err)
		}

		d2, err := waitMessage(ch, dlq, 5*time.Second)
		if err != nil {
			return fmt.Errorf("被拒绝的消息未进入死信队列: %w", err)
		}
		return checkDeath(d2, "rejected", src)
	})
}

func testRejectPublishNack() error {
	return withChannel(func(ch *amqp.Channel) error {
		q := "m3.lim.q"
		if _, err := ch.QueueDeclare(q, true, false, false, false, amqp.Table{
			"x-max-length": int32(1),
			"x-overflow":   "reject-publish",
		}); err != nil {
			return fmt.Errorf("声明队列失败: %w", err)
		}
		if _, err := ch.QueuePurge(q, false); err != nil {
			return fmt.Errorf("清空队列失败: %w", err)
		}
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("开启 confirm 失败: %w", err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4))

		if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte("a")}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}
		if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte("b")}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}

		// 第一条应被确认，第二条应被否定确认
		for i, wantAck := range []bool{true, false} {
			select {
			case c := <-confirms:
				if c.Ack != wantAck {
					return fmt.Errorf("第 %d 条的确认结果 = %v, want Ack=%v", i+1, c.Ack, wantAck)
				}
			case <-time.After(5 * time.Second):
				return fmt.Errorf("等待第 %d 条确认超时", i+1)
			}
		}
		return nil
	})
}

// testReturnBeforeConfirm 验证 mandatory 未命中时的时序：
// 服务端必须先把 Basic.Return 发出来，再发 confirm，客户端才能把两者对上。
func testReturnBeforeConfirm() error {
	return withChannel(func(ch *amqp.Channel) error {
		returns := ch.NotifyReturn(make(chan amqp.Return, 1))
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("开启 confirm 失败: %w", err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		// 路由键指向不存在的队列 + mandatory：消息必须被退回
		body := []byte("nowhere")
		if err := ch.Publish("", "m3.no.such.queue", true, false,
			amqp.Publishing{Body: body}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}

		sawReturn := false
		sawConfirm := false
		for i := 0; i < 2; i++ {
			select {
			case r := <-returns:
				if string(r.Body) != string(body) {
					return fmt.Errorf("退回的消息内容错误: %q", r.Body)
				}
				if r.ReplyCode != 312 {
					return fmt.Errorf("退回码 = %d, want 312 NO_ROUTE", r.ReplyCode)
				}
				if sawConfirm {
					return fmt.Errorf("Basic.Return 在 confirm 之后才到，客户端无法关联两者")
				}
				sawReturn = true
			case <-confirms:
				sawConfirm = true
			case <-time.After(5 * time.Second):
				return fmt.Errorf("等待 Basic.Return 与 confirm 超时（Return=%v Confirm=%v）",
					sawReturn, sawConfirm)
			}
			if sawReturn && sawConfirm {
				break
			}
		}
		if !sawReturn {
			return fmt.Errorf("未收到 Basic.Return")
		}
		if !sawConfirm {
			return fmt.Errorf("未收到发布确认")
		}
		return nil
	})
}
