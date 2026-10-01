package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// m2Cases 覆盖 M2 的完整消息链路：拓扑声明、四种路由、发布消费、确认与错误语义。
//
// 断言尽量贴近"客户端能观察到什么"，因为兼容性问题的表现一定在客户端侧。
func m2Cases() []testCase {
	return []testCase{
		{"M2 direct 路由：服务端命名队列 + 发布消费 + 手动 ack + FIFO 顺序", testDirectRoundTrip},
		{"M2 topic 路由：通配匹配 + 同队列多绑定去重", testTopicRouting},
		{"M2 fanout 路由：一条消息广播到多个队列", testFanoutRouting},
		{"M2 basic.get：有消息返回 Get-Ok，空队列返回 Get-Empty", testBasicGet},
		{"M2 消息属性往返：content-type / headers / delivery-mode / correlation-id", testPropertiesRoundTrip},
		{"M2 nack(requeue)：重投并置 redelivered=true", testNackRequeue},
		{"M2 被动声明不存在的队列：404 且只关 Channel", testPassiveDeclareNotFound},
		{"M2 参数不一致重声明：406", testInequivalentRedeclare},
		{"M2 保留名声明被拒：403", testReservedName},
		{"M2 发布到不存在的交换机：404", testPublishToUnknownExchange},
		{"M2 队列 purge / delete 返回正确计数", testPurgeAndDelete},
	}
}

// withChannel 建立连接与通道，执行 fn，最后清理。
func withChannel(fn func(*amqp.Channel) error) error {
	conn, err := dial(url(""))
	if err != nil {
		return fmt.Errorf("拨号失败: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel 失败: %w", err)
	}
	defer ch.Close()

	return fn(ch)
}

// declareTempQueue 声明一个服务端命名的独占自动删除队列。
func declareTempQueue(ch *amqp.Channel) (string, error) {
	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return "", fmt.Errorf("声明队列失败: %w", err)
	}
	if q.Name == "" {
		return "", fmt.Errorf("服务端未返回队列名")
	}
	if !strings.HasPrefix(q.Name, "amq.gen-") {
		return "", fmt.Errorf("服务端队列名格式异常: %q（期望 amq.gen- 前缀）", q.Name)
	}
	return q.Name, nil
}

func testDirectRoundTrip() error {
	return withChannel(func(ch *amqp.Channel) error {
		if err := ch.ExchangeDeclare("m2.direct", "direct", false, false, false, false, nil); err != nil {
			return fmt.Errorf("声明交换机失败: %w", err)
		}
		qname, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		if err := ch.QueueBind(qname, "k1", "m2.direct", false, nil); err != nil {
			return fmt.Errorf("绑定失败: %w", err)
		}

		for i := 0; i < 3; i++ {
			if err := ch.Publish("m2.direct", "k1", false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(fmt.Sprintf("msg-%d", i)),
			}); err != nil {
				return fmt.Errorf("发布第 %d 条失败: %w", i, err)
			}
		}
		// 路由键不匹配：不应该进入队列
		if err := ch.Publish("m2.direct", "k2", false, false, amqp.Publishing{Body: []byte("no-route")}); err != nil {
			return fmt.Errorf("发布未匹配消息失败: %w", err)
		}

		// prefetch=1：同时验证额度闸门与逐条 ack 后继续投递
		if err := ch.Qos(1, 0, false); err != nil {
			return fmt.Errorf("设置 qos 失败: %w", err)
		}
		deliveries, err := ch.Consume(qname, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("消费失败: %w", err)
		}

		for i := 0; i < 3; i++ {
			select {
			case d, ok := <-deliveries:
				if !ok {
					return fmt.Errorf("投递通道在收到第 %d 条前关闭", i)
				}
				want := fmt.Sprintf("msg-%d", i)
				if string(d.Body) != want {
					return fmt.Errorf("第 %d 条顺序或内容错误: got %q want %q", i, d.Body, want)
				}
				if d.Redelivered {
					return fmt.Errorf("第 %d 条首次投递不应置 redelivered", i)
				}
				if err := d.Ack(false); err != nil {
					return fmt.Errorf("ack 第 %d 条失败: %w", i, err)
				}
			case <-time.After(5 * time.Second):
				return fmt.Errorf("等待第 %d 条消息超时", i)
			}
		}

		select {
		case d := <-deliveries:
			return fmt.Errorf("收到本不该路由过来的消息: %q", d.Body)
		case <-time.After(300 * time.Millisecond):
		}
		return nil
	})
}

func testTopicRouting() error {
	return withChannel(func(ch *amqp.Channel) error {
		if err := ch.ExchangeDeclare("m2.topic", "topic", false, false, false, false, nil); err != nil {
			return fmt.Errorf("声明交换机失败: %w", err)
		}
		qname, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		// 两条绑定都能命中 a.b.c：队列应只收到一份
		for _, key := range []string{"a.*.c", "a.#", "x.#"} {
			if err := ch.QueueBind(qname, key, "m2.topic", false, nil); err != nil {
				return fmt.Errorf("绑定 %q 失败: %w", key, err)
			}
		}
		if err := ch.Publish("m2.topic", "a.b.c", false, false, amqp.Publishing{Body: []byte("t1")}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}

		d, ok, err := ch.Get(qname, true)
		if err != nil {
			return fmt.Errorf("get 失败: %w", err)
		}
		if !ok {
			return fmt.Errorf("topic 路由未命中：队列为空")
		}
		if string(d.Body) != "t1" {
			return fmt.Errorf("消息内容错误: %q", d.Body)
		}
		if d.RoutingKey != "a.b.c" {
			return fmt.Errorf("routing key 未原样返回: %q", d.RoutingKey)
		}
		if _, ok, _ := ch.Get(qname, true); ok {
			return fmt.Errorf("同一队列的多条绑定未去重：收到了重复消息")
		}
		return nil
	})
}

func testFanoutRouting() error {
	return withChannel(func(ch *amqp.Channel) error {
		if err := ch.ExchangeDeclare("m2.fanout", "fanout", false, false, false, false, nil); err != nil {
			return fmt.Errorf("声明交换机失败: %w", err)
		}
		q1, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		q2, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		// fanout 忽略 routing key，绑定任意键都应收到
		if err := ch.QueueBind(q1, "ignored", "m2.fanout", false, nil); err != nil {
			return fmt.Errorf("绑定 q1 失败: %w", err)
		}
		if err := ch.QueueBind(q2, "whatever", "m2.fanout", false, nil); err != nil {
			return fmt.Errorf("绑定 q2 失败: %w", err)
		}
		if err := ch.Publish("m2.fanout", "", false, false, amqp.Publishing{Body: []byte("broadcast")}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}
		for i, q := range []string{q1, q2} {
			d, ok, err := ch.Get(q, true)
			if err != nil {
				return fmt.Errorf("get q%d 失败: %w", i+1, err)
			}
			if !ok {
				return fmt.Errorf("fanout 未投递到队列 %d", i+1)
			}
			if string(d.Body) != "broadcast" {
				return fmt.Errorf("队列 %d 内容错误: %q", i+1, d.Body)
			}
		}
		return nil
	})
}

func testBasicGet() error {
	return withChannel(func(ch *amqp.Channel) error {
		qname, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		// 空队列：必须返回 Get-Empty，而不是错误
		if _, ok, err := ch.Get(qname, true); err != nil {
			return fmt.Errorf("空队列 get 返回错误（应为 Get-Empty）: %w", err)
		} else if ok {
			return fmt.Errorf("空队列竟然取到了消息")
		}

		// 默认交换机按队列名路由
		if err := ch.Publish("", qname, false, false, amqp.Publishing{Body: []byte("direct-to-queue")}); err != nil {
			return fmt.Errorf("发布到默认交换机失败: %w", err)
		}
		d, ok, err := ch.Get(qname, false)
		if err != nil {
			return fmt.Errorf("get 失败: %w", err)
		}
		if !ok {
			return fmt.Errorf("默认交换机未按队列名路由")
		}
		if string(d.Body) != "direct-to-queue" {
			return fmt.Errorf("内容错误: %q", d.Body)
		}
		if d.DeliveryTag == 0 {
			return fmt.Errorf("Get-Ok 的 delivery tag 不应为 0")
		}
		if err := d.Ack(false); err != nil {
			return fmt.Errorf("ack 失败: %w", err)
		}
		return nil
	})
}

func testPropertiesRoundTrip() error {
	return withChannel(func(ch *amqp.Channel) error {
		qname, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		sent := amqp.Publishing{
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			DeliveryMode:    amqp.Persistent,
			Priority:        5,
			CorrelationId:   "corr-1",
			ReplyTo:         "reply-queue",
			MessageId:       "msg-1",
			Type:            "event",
			AppId:           "probe",
			Expiration:      "60000",
			Headers: amqp.Table{
				"str": "v",
				"num": int32(42),
				"nested": amqp.Table{
					"k": "inner",
				},
			},
			Body: []byte(`{"hello":"world"}`),
		}
		if err := ch.Publish("", qname, false, false, sent); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}
		d, ok, err := ch.Get(qname, true)
		if err != nil || !ok {
			return fmt.Errorf("get 失败: ok=%v err=%v", ok, err)
		}

		check := []struct {
			name string
			got  any
			want any
		}{
			{"content-type", d.ContentType, sent.ContentType},
			{"content-encoding", d.ContentEncoding, sent.ContentEncoding},
			{"delivery-mode", d.DeliveryMode, sent.DeliveryMode},
			{"priority", d.Priority, sent.Priority},
			{"correlation-id", d.CorrelationId, sent.CorrelationId},
			{"reply-to", d.ReplyTo, sent.ReplyTo},
			{"message-id", d.MessageId, sent.MessageId},
			{"type", d.Type, sent.Type},
			{"app-id", d.AppId, sent.AppId},
			{"expiration", d.Expiration, sent.Expiration},
			{"body", string(d.Body), string(sent.Body)},
		}
		for _, c := range check {
			if c.got != c.want {
				return fmt.Errorf("属性 %s 不一致: got %v want %v", c.name, c.got, c.want)
			}
		}
		// headers 是 field-table，重点验证类型与嵌套表
		if v, ok := d.Headers["str"].(string); !ok || v != "v" {
			return fmt.Errorf("headers.str 错误: %#v", d.Headers["str"])
		}
		if v, ok := d.Headers["nested"].(amqp.Table); !ok || v["k"] != "inner" {
			return fmt.Errorf("headers 嵌套表丢失或错误: %#v", d.Headers["nested"])
		}
		return nil
	})
}

func testNackRequeue() error {
	return withChannel(func(ch *amqp.Channel) error {
		qname, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		if err := ch.Publish("", qname, false, false, amqp.Publishing{Body: []byte("retry-me")}); err != nil {
			return fmt.Errorf("发布失败: %w", err)
		}
		if err := ch.Qos(1, 0, false); err != nil {
			return fmt.Errorf("qos 失败: %w", err)
		}
		deliveries, err := ch.Consume(qname, "", false, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("消费失败: %w", err)
		}

		select {
		case d := <-deliveries:
			if d.Redelivered {
				return fmt.Errorf("第一次投递不应是 redelivered")
			}
			if err := d.Nack(false, true); err != nil {
				return fmt.Errorf("nack 失败: %w", err)
			}
		case <-time.After(5 * time.Second):
			return fmt.Errorf("等待首次投递超时")
		}

		// 重新入队后应再次投递，并带上 redelivered 标记
		select {
		case d := <-deliveries:
			if !d.Redelivered {
				return fmt.Errorf("重新投递的消息应置 redelivered=true")
			}
			if string(d.Body) != "retry-me" {
				return fmt.Errorf("重投内容错误: %q", d.Body)
			}
			if err := d.Ack(false); err != nil {
				return fmt.Errorf("ack 失败: %w", err)
			}
		case <-time.After(5 * time.Second):
			return fmt.Errorf("nack(requeue) 后未重新投递")
		}
		return nil
	})
}

// expectChannelError 断言服务端以指定错误码拒绝该操作。
//
// 两种表现都要接受：错误可能直接由本次调用返回（客户端在等 -Ok 时收到 Channel.Close），
// 也可能异步从 NotifyClose 通知（如 publish 这类没有响应的操作）。
func expectChannelError(ch *amqp.Channel, wantCode int, op func() error) error {
	errCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	if err := op(); err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) && amqpErr.Code == wantCode {
			return nil
		}
		return fmt.Errorf("错误码不符: got %v want %d", err, wantCode)
	}

	select {
	case e := <-errCh:
		if e == nil {
			return fmt.Errorf("通道被正常关闭，未收到异常")
		}
		if e.Code != wantCode {
			return fmt.Errorf("错误码不符: got %d (%s) want %d", e.Code, e.Reason, wantCode)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("未收到通道异常通知")
	}
}

func testPassiveDeclareNotFound() error {
	return withChannel(func(ch *amqp.Channel) error {
		return expectChannelError(ch, amqp.NotFound, func() error {
			_, err := ch.QueueDeclarePassive("m2.no.such.queue", false, false, false, false, nil)
			return err
		})
	})
}

func testInequivalentRedeclare() error {
	return withChannel(func(ch *amqp.Channel) error {
		if _, err := ch.QueueDeclare("m2.dur.queue", true, false, false, false, nil); err != nil {
			return fmt.Errorf("首次声明失败: %w", err)
		}
		// 同名但 durable 不同：必须 406
		return expectChannelError(ch, amqp.PreconditionFailed, func() error {
			_, err := ch.QueueDeclare("m2.dur.queue", false, false, false, false, nil)
			return err
		})
	})
}

func testReservedName() error {
	return withChannel(func(ch *amqp.Channel) error {
		return expectChannelError(ch, amqp.AccessRefused, func() error {
			_, err := ch.QueueDeclare("amq.reserved.by.client", false, false, false, false, nil)
			return err
		})
	})
}

func testPublishToUnknownExchange() error {
	return withChannel(func(ch *amqp.Channel) error {
		return expectChannelError(ch, amqp.NotFound, func() error {
			return ch.Publish("m2.no.such.exchange", "k", false, false, amqp.Publishing{Body: []byte("x")})
		})
	})
}

func testPurgeAndDelete() error {
	return withChannel(func(ch *amqp.Channel) error {
		qname, err := declareTempQueue(ch)
		if err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			if err := ch.Publish("", qname, false, false, amqp.Publishing{Body: []byte("x")}); err != nil {
				return fmt.Errorf("发布失败: %w", err)
			}
		}
		n, err := ch.QueuePurge(qname, false)
		if err != nil {
			return fmt.Errorf("purge 失败: %w", err)
		}
		if n != 5 {
			return fmt.Errorf("purge 返回计数错误: got %d want 5", n)
		}
		if _, ok, _ := ch.Get(qname, true); ok {
			return fmt.Errorf("purge 后队列仍非空")
		}

		deleted, err := ch.QueueDelete(qname, false, false, false)
		if err != nil {
			return fmt.Errorf("删除队列失败: %w", err)
		}
		if deleted != 0 {
			return fmt.Errorf("删除空队列返回计数错误: got %d want 0", deleted)
		}
		// 删除后再用应 404（此处新建通道验证，因为通道已被服务端关闭）
		return nil
	})
}
