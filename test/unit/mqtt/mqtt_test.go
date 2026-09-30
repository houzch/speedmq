package mqtt_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/protocol/mqtt"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 MQTT 3.1.1 插件的端到端语义。每个用例都跑在**真实内核**上：
// 订阅落到内核队列、发布走内核的 amq.topic 交换机，因此这里验证的不只是报文，
// 还有"MQTT 与 AMQP 共用同一套路由/队列/确认语义"这一插件化主张。

// waitFor 轮询等待条件成立（与仓库其它测试保持同一口径：不靠固定睡眠假装同步）。
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", desc)
}

// queueNameOf 返回某客户端某 QoS 的订阅队列名（与插件实现约定一致）。
func queueNameOf(clientID string, qos byte) string {
	return "mqtt-subscription-q" + string(rune('0'+qos)) + "-" + clientID
}

// TestConnectPingDisconnect 覆盖最小闭环：握手、心跳、优雅断开。
func TestConnectPingDisconnect(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	c := dial(t, b, p)

	c.mustConnect("m7-basic", true)
	c.ping()
	c.ping()
	c.disconnect()
	c.expectClosed()
}

// TestPublishSubscribeQoS0：QoS0 订阅者能收到发布，主题与载荷完整（含通配匹配）。
func TestPublishSubscribeQoS0(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	sub := dial(t, b, p)
	pub := dial(t, b, p)

	sub.mustConnect("m7-sub0", true)
	if codes := sub.subscribe(1, "sensors/+/temp"); len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("SUBACK 应授予 QoS1，实际 %v", codes)
	}
	pub.mustConnect("m7-pub0", true)
	pub.publishQoS0("sensors/room1/temp", "21.5", false)

	got := sub.recvPublish()
	if got.Topic != "sensors/room1/temp" || got.Payload != "21.5" {
		t.Fatalf("投递内容不符: topic=%q payload=%q", got.Topic, got.Payload)
	}
	if got.QoS != 1 {
		t.Fatalf("QoS1 订阅应收到 QoS1 投递（消息以 QoS0 发布，投递按订阅 QoS 升格），实际 %d", got.QoS)
	}
	sub.write(puback(got.PacketID))

	// 不匹配的主题不得投递。
	pub.publishQoS0("sensors/room1/humidity", "60", false)
	sub.expectNoPacket()
}

// TestPublishSubscribeQoS1AndAck：QoS1 发布/投递/确认闭环，确认后队列清空。
func TestPublishSubscribeQoS1AndAck(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	sub := dial(t, b, p)
	pub := dial(t, b, p)

	sub.mustConnect("m7-sub1", true)
	sub.subscribe(1, "orders/#")
	pub.mustConnect("m7-pub1", true)

	pub.publishQoS1(7, "orders/new", "order-1", false)
	got := sub.recvPublishQoS1()
	if got.Topic != "orders/new" || got.Payload != "order-1" {
		t.Fatalf("投递内容不符: %+v", got)
	}

	// 确认之后队列里不应留下未确认消息。
	waitFor(t, "内核队列的未确认数归零", func() bool {
		s, ok := b.QueueSnapshot("/", queueNameOf("m7-sub1", 1))
		return ok && s.Unacked == 0 && s.Ready == 0
	})
}

// TestQoS2DowngradedToQoS1：订阅 QoS2 按规范降级授予 1；入站 QoS2 完成四步握手。
func TestQoS2DowngradedToQoS1(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	sub := dial(t, b, p)
	pub := dial(t, b, p)

	sub.mustConnect("m7-sub2", true)
	// 手工发一个 QoS2 订阅请求。
	body := []byte{0x00, 0x01}
	body = putString(body, "qos2/#")
	body = append(body, 2)
	sub.write(frame(pktSubscribe, 0x02, body))
	_, subackBody := sub.expect(pktSuback)
	_, codes := parseSuback(subackBody)
	if len(codes) != 1 || codes[0] != 1 {
		t.Fatalf("QoS2 订阅应降级授予 QoS1，实际 %v", codes)
	}

	pub.mustConnect("m7-pub2", true)
	pub.publishQoS2(11, "qos2/x", "exactly-once-ish")
	got := sub.recvPublishQoS1()
	if got.Topic != "qos2/x" || got.Payload != "exactly-once-ish" {
		t.Fatalf("投递内容不符: %+v", got)
	}
	sub.write(puback(got.PacketID))
}

// TestRetainedMessage：保留消息在订阅建立时立即下发，且带 retain 标志。
func TestRetainedMessage(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	pub := dial(t, b, p)
	pub.mustConnect("m7-ret-pub", true)
	pub.publishQoS1(1, "news/1", "hello", true)

	sub := dial(t, b, p)
	sub.mustConnect("m7-ret-sub", true)
	sub.subscribe(1, "news/#")
	got := sub.recvPublish()
	if got.Topic != "news/1" || got.Payload != "hello" || !got.Retain {
		t.Fatalf("保留消息投递不符: %+v", got)
	}
	sub.write(puback(got.PacketID))

	// 空载荷 + retain=1 表示清除保留消息：后来的订阅者不应再收到。
	pub.publishQoS1(2, "news/1", "", true)
	late := dial(t, b, p)
	late.mustConnect("m7-ret-late", true)
	late.subscribe(1, "news/#")
	late.expectNoPacket()
}

// TestWillMessageOnAbruptDisconnect：异常断开（未发 DISCONNECT）时下发遗嘱。
func TestWillMessageOnAbruptDisconnect(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	watcher := dial(t, b, p)
	watcher.mustConnect("m7-will-watch", true)
	watcher.subscribe(1, "clients/+/status")

	dying := dial(t, b, p)
	_, code := dying.connect("m7-will-client", true, 30, "guest", "guest",
		&willSpec{topic: "clients/m7-will-client/status", payload: "offline", qos: 1})
	if code != 0 {
		t.Fatalf("带遗嘱的 CONNECT 应被接受，实际返回码 %d", code)
	}
	// 直接关闭 TCP（不发 DISCONNECT）→ 属于异常断开。
	dying.close()

	got := watcher.recvPublish()
	if got.Topic != "clients/m7-will-client/status" || got.Payload != "offline" {
		t.Fatalf("遗嘱消息不符: %+v", got)
	}
	watcher.write(puback(got.PacketID))
}

// TestCleanDisconnectSuppressesWill：正常 DISCONNECT 之后不得下发遗嘱。
func TestCleanDisconnectSuppressesWill(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	watcher := dial(t, b, p)
	watcher.mustConnect("m7-nowill-watch", true)
	watcher.subscribe(1, "clients/#")

	leaving := dial(t, b, p)
	_, code := leaving.connect("m7-nowill-client", true, 30, "guest", "guest",
		&willSpec{topic: "clients/m7-nowill-client/status", payload: "offline", qos: 1})
	if code != 0 {
		t.Fatalf("CONNECT 应被接受，实际返回码 %d", code)
	}
	leaving.disconnect()
	leaving.expectClosed()

	watcher.expectNoPacket()
}

// TestPersistentSessionRedeliversUnacked：Clean Session=0 的会话在重连后继续投递未确认消息。
//
// 这是"MQTT 会话映射到内核 durable 队列"最直接的证据：消息真的留在内核队列里，
// 客户端断开→重连（同 Client ID）后能拿回来。
func TestPersistentSessionRedeliversUnacked(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	const clientID = "m7-persist"

	sub := dial(t, b, p)
	if present, code := sub.connect(clientID, false, 30, "guest", "guest", nil); code != 0 || present {
		t.Fatalf("首次连接：期望 code=0 present=false，实际 code=%d present=%v", code, present)
	}
	sub.subscribe(1, "tasks/#")

	pub := dial(t, b, p)
	pub.mustConnect("m7-persist-pub", true)
	pub.publishQoS1(1, "tasks/a", "t1", false)
	pub.publishQoS1(2, "tasks/b", "t2", false)

	// 收到但不确认，然后异常断开：消息应回到内核队列。
	first := sub.recvPublishOnly()
	second := sub.recvPublishOnly()
	if first.Payload == second.Payload {
		t.Fatalf("期望两条不同的消息，实际都收到 %q", first.Payload)
	}
	queue := queueNameOf(clientID, 1)
	sub.close()
	deadline := time.Now().Add(testTimeout)
	observed := "（未观测到）"
	for time.Now().Before(deadline) {
		s, ok := b.QueueSnapshot("/", queue)
		observed = fmt.Sprintf("ok=%v ready=%d unacked=%d consumers=%d", ok, s.Ready, s.Unacked, s.ConsumerCount)
		if ok && s.Ready == 2 && s.Unacked == 0 {
			observed = ""
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if observed != "" {
		t.Fatalf("断开后未确认消息未回到队列 %s: %s", queue, observed)
	}

	// 重连：Session Present 应为 true，且能重新收到那两条消息。
	sub2 := dial(t, b, p)
	present, code := sub2.connect(clientID, false, 30, "guest", "guest", nil)
	if code != 0 || !present {
		t.Fatalf("重连：期望 code=0 present=true，实际 code=%d present=%v", code, present)
	}
	sub2.subscribe(1, "tasks/#")
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := sub2.recvPublishQoS1()
		got[m.Payload] = true
	}
	if !got["t1"] || !got["t2"] {
		t.Fatalf("重连后应重新收到 t1/t2，实际 %v", got)
	}
	waitFor(t, "确认后队列清空", func() bool {
		s, ok := b.QueueSnapshot("/", queue)
		return ok && s.Ready == 0 && s.Unacked == 0
	})
}

// TestCleanSessionDropsQueue：Clean Session=1 的订阅队列应在断开后随会话一起消失。
func TestCleanSessionDropsQueue(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	c := dial(t, b, p)
	c.mustConnect("m7-clean", true)
	c.subscribe(1, "temp/#")
	queue := queueNameOf("m7-clean", 1)
	if _, ok := b.QueueSnapshot("/", queue); !ok {
		t.Fatalf("订阅后队列应存在")
	}
	c.disconnect()
	c.expectClosed()
	waitFor(t, "Clean Session 的订阅队列被回收", func() bool {
		_, ok := b.QueueSnapshot("/", queue)
		return !ok
	})
}

// TestUnsubscribeStopsDelivery：退订之后不再收到消息。
func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	sub := dial(t, b, p)
	sub.mustConnect("m7-unsub", true)
	sub.subscribe(1, "events/#")
	sub.unsubscribe(2, "events/#")

	pub := dial(t, b, p)
	pub.mustConnect("m7-unsub-pub", true)
	pub.publishQoS0("events/a", "x", false)
	sub.expectNoPacket()
}

// TestAuthFailureAndProtocolVersion：认证失败与协议版本不符都必须被 CONNACK 拒绝。
func TestAuthFailureAndProtocolVersion(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()

	bad := dial(t, b, p)
	if _, code := bad.connect("m7-badpass", true, 30, "guest", "wrong", nil); code != connBadCredentials {
		t.Fatalf("错误口令应返回 0x04，实际 %d", code)
	}
	bad.expectClosed()

	// 协议级别 3（MQTT 3.1）不被本插件支持。
	old := dial(t, b, p)
	body := putString(nil, "MQIsdp")
	body = append(body, 3)
	body = append(body, 0x02) // clean session
	body = append(body, 0x00, 0x1e)
	body = putString(body, "m7-old")
	old.write(frame(pktConnect, 0, body))
	_, connack := old.expect(pktConnack)
	if connack[1] != connBadProtocol {
		t.Fatalf("协议版本不符应返回 0x01，实际 %d", connack[1])
	}
	old.expectClosed()
}

// TestMalformedPacketsCloseConnection：畸形报文一律断开（不静默忽略）。
func TestMalformedPacketsCloseConnection(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()

	// SUBSCRIBE 标志位不是 0b0010。
	badFlags := dial(t, b, p)
	badFlags.mustConnect("m7-badsub", true)
	badFlags.write(frame(pktSubscribe, 0x00, []byte{0x00, 0x01, 0x00, 0x01, 'a', 0x01}))
	badFlags.expectClosed()

	// PUBLISH 到通配主题（发布不允许带通配符）。
	badTopic := dial(t, b, p)
	badTopic.mustConnect("m7-badpub", true)
	badTopic.write(encodePublish(0, false, false, "a/+/b", 0, []byte("x")))
	badTopic.expectClosed()

	// 非法的通配位置（# 必须在最后一层）只让该条订阅失败，不应断开连接。
	badFilter := dial(t, b, p)
	badFilter.mustConnect("m7-badfilter", true)
	if codes := badFilter.subscribe(1, "a/#/b", "ok/#"); len(codes) != 2 ||
		codes[0] != subackFailure || codes[1] != 1 {
		t.Fatalf("非法过滤器应回失败码且不影响同报文其他订阅，实际 %v", codes)
	}
	badFilter.ping()
}

// TestInteropWithOtherProtocol：别的协议（这里直接用一个内核会话）发布到 amq.topic，
// MQTT 订阅者也能收到 —— 证明两个协议共用同一套路由与队列。
func TestInteropWithOtherProtocol(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	sub := dial(t, b, p)
	sub.mustConnect("m7-interop", true)
	sub.subscribe(1, "building/+/temp")

	sess := otherProtocolSession(t, b)
	res, err := sess.Publish(&plugin.Message{
		Body: []byte("23.0"),
		// 约定：发布方自行填充 Message.RoutingKey（AMQP 插件也是这么做的），
		// 它同时是 MQTT 主题的来源。
		RoutingKey: "building.floor3.temp",
		Properties: plugin.Properties{DeliveryMode: 1},
	}, "amq.topic", "building.floor3.temp", false)
	if err != nil || !res.Routed {
		t.Fatalf("通过内核会话发布失败: routed=%v err=%v", res.Routed, err)
	}

	got := sub.recvPublish()
	// 没有 x-mqtt-topic 头时按路由键反推主题（"." → "/"），这正是跨协议互通的约定。
	if got.Topic != "building/floor3/temp" || got.Payload != "23.0" {
		t.Fatalf("跨协议投递不符: %+v", got)
	}
	sub.write(puback(got.PacketID))
}

// TestDeliveryWithoutRoutingKeyIsRejected：消息若没有路由键（无法确定 MQTT 主题），
// 不得投出一条非法 PUBLISH，也不得陷入"重投—失败"活锁，而是按拒绝结算。
func TestDeliveryWithoutRoutingKeyIsRejected(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	sub := dial(t, b, p)
	sub.mustConnect("m7-notopic", true)
	sub.subscribe(1, "notopic/#")

	sess := otherProtocolSession(t, b)
	// 故意不填 Message.RoutingKey。
	if _, err := sess.Publish(&plugin.Message{
		Body:       []byte("x"),
		Properties: plugin.Properties{DeliveryMode: 1},
	}, "amq.topic", "notopic/a", false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	sub.expectNoPacket()
	waitFor(t, "无法确定主题的消息被结算掉（不残留、不重投）", func() bool {
		s, ok := b.QueueSnapshot("/", queueNameOf("m7-notopic", 1))
		return ok && s.Ready == 0 && s.Unacked == 0
	})
}

// TestConcurrentSubscribersStress 是多连接并发下的"竞态探测器"：
// 8 个订阅者 + 1 个发布者 + 20 条消息，投递协程、读循环与确认路径彼此交错。
//
// 单连接的顺序用例碰不到这些时序（例如"投递正在写 socket 时客户端发来 PUBACK"、
// "订阅大量并发建立/拆除"），因此这里刻意把并发量放大。
func TestConcurrentSubscribersStress(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	const (
		subscribers = 8
		messages    = 20
	)
	clients := make([]*mqttClient, 0, subscribers)
	for i := 0; i < subscribers; i++ {
		c := dial(t, b, p)
		c.mustConnect(fmt.Sprintf("m7-stress-%d", i), true)
		c.subscribe(1, "stress/#")
		clients = append(clients, c)
	}

	pub := dial(t, b, p)
	pub.mustConnect("m7-stress-pub", true)
	for i := 0; i < messages; i++ {
		pub.publishQoS1(uint16(i+1), "stress/a", fmt.Sprintf("m%02d", i), false)
	}

	// 逐个订阅者收完自己的 20 条：期间其它连接仍在投递，确认与投递并发进行。
	for i, c := range clients {
		seen := map[string]bool{}
		for got := 0; got < messages; got++ {
			m := c.recvPublishOnly()
			if m.Topic != "stress/a" {
				t.Fatalf("订阅者 %d 收到意外主题 %q", i, m.Topic)
			}
			seen[m.Payload] = true
			c.write(puback(m.PacketID))
		}
		if len(seen) != messages {
			t.Fatalf("订阅者 %d 去重后只收到 %d/%d 条", i, len(seen), messages)
		}
	}
	// 全部确认后，每个订阅队列都应为空。
	for i := range clients {
		queue := queueNameOf(fmt.Sprintf("m7-stress-%d", i), 1)
		waitFor(t, "订阅队列清空", func() bool {
			s, ok := b.QueueSnapshot("/", queue)
			return ok && s.Ready == 0 && s.Unacked == 0
		})
	}
}

// TestConnectionVisibleToManagement：MQTT 连接必须出现在内核的连接视图里
// （协议无关的管理面：内核不认识 MQTT，但能展示它的协议名与订阅数）。
func TestConnectionVisibleToManagement(t *testing.T) {
	b := testBroker(t)
	p := mqtt.New()
	c := dial(t, b, p)
	c.mustConnect("m7-visible", true)
	c.subscribe(1, "visible/#")

	waitFor(t, "内核能看到 MQTT 连接与其订阅", func() bool {
		for _, conn := range b.Connections() {
			if conn.Protocol != "MQTT 3.1.1" {
				continue
			}
			if conn.Channels > 0 {
				return true
			}
		}
		return false
	})
}
