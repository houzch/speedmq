package main

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// m4Cases 覆盖 M4 的持久化行为。
//
// 崩溃恢复本身需要重启 broker，无法在探针里完成（见内核单测与 M4 里程碑说明）；
// 这里验证的是"持久化不会破坏对外可见的协议行为"，以及服务端如实声明了能力。
func m4Cases() []testCase {
	return []testCase{
		{"M4 durable 队列 + 持久消息：confirm 逐条 ack 且消息可正常消费", testDurablePersistentPublish},
		{"M4 服务端如实声明 connection.blocked 能力", testConnectionBlockedCapability},
	}
}

// testDurablePersistentPublish 验证持久消息走通"发布确认 → 落盘 → 消费"全链路。
//
// 若协议层在等落盘时出错，最典型的表现就是 confirm 迟迟不来，因此这里对超时很敏感。
func testDurablePersistentPublish() error {
	return withChannel(func(ch *amqp.Channel) error {
		const q = "m4.probe.durable"
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			return fmt.Errorf("声明 durable 队列失败: %w", err)
		}
		// 清掉历史数据，保证断言稳定
		if _, err := ch.QueuePurge(q, false); err != nil {
			return fmt.Errorf("清空队列失败: %w", err)
		}
		if err := ch.Confirm(false); err != nil {
			return fmt.Errorf("开启 confirm 失败: %w", err)
		}
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4))

		const n = 3
		for i := 0; i < n; i++ {
			if err := ch.Publish("", q, false, false, amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(fmt.Sprintf("p-%d", i)),
			}); err != nil {
				return fmt.Errorf("发布第 %d 条失败: %w", i, err)
			}
		}

		for i := 1; i <= n; i++ {
			select {
			case c := <-confirms:
				if !c.Ack {
					return fmt.Errorf("第 %d 条收到否定确认", i)
				}
				if c.DeliveryTag != uint64(i) {
					return fmt.Errorf("确认序号不连续: got %d want %d", c.DeliveryTag, i)
				}
			case <-time.After(5 * time.Second):
				return fmt.Errorf("等待第 %d 条确认超时（持久消息的 confirm 依赖落盘完成）", i)
			}
		}

		for i := 0; i < n; i++ {
			d, ok, err := ch.Get(q, true)
			if err != nil || !ok {
				return fmt.Errorf("取第 %d 条失败: ok=%v err=%v", i, ok, err)
			}
			want := fmt.Sprintf("p-%d", i)
			if string(d.Body) != want {
				return fmt.Errorf("第 %d 条内容错误: got %q want %q", i, d.Body, want)
			}
			if d.DeliveryMode != amqp.Persistent {
				return fmt.Errorf("delivery-mode 未保真: got %d want %d", d.DeliveryMode, amqp.Persistent)
			}
		}

		// 删除队列应同时回收它的磁盘数据
		if _, err := ch.QueueDelete(q, false, false, false); err != nil {
			return fmt.Errorf("删除 durable 队列失败: %w", err)
		}
		return nil
	})
}

// testConnectionBlockedCapability 校验 capabilities 如实声明。
//
// 声明了就必须实现（否则客户端会走入错误分支），因此这里也顺便确认它没有被漏掉。
func testConnectionBlockedCapability() error {
	conn, err := dial(url(""))
	if err != nil {
		return fmt.Errorf("拨号失败: %w", err)
	}
	defer conn.Close()

	caps, ok := conn.Properties["capabilities"].(amqp.Table)
	if !ok {
		return fmt.Errorf("server-properties 缺少 capabilities 表: %#v", conn.Properties)
	}
	for _, name := range []string{"connection.blocked", "publisher_confirms", "basic.nack"} {
		if v, _ := caps[name].(bool); !v {
			return fmt.Errorf("capabilities 未声明 %s: %#v", name, caps)
		}
	}
	return nil
}
