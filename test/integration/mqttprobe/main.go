// Command mqttprobe 用**手写的 MQTT 3.1.1 客户端**对一个真实运行的 swiftmqd 做端到端验证。
//
// 为什么自己写客户端而不是引第三方库（如 paho）：
//   - 本仓库的核心承诺之一是"零第三方依赖、可离线构建"，集成探针也保持同样的口径；
//   - 手写字节能验证**线上格式**本身（长度前缀、标志位、Packet ID），
//     而这正是"插件写错了但自测也写错了"最容易漏掉的地方。
//
// 用法：
//
//	go run . -addr 127.0.0.1:1883 [-user guest] [-pass guest]
//
// 覆盖：连接/鉴权、订阅（含通配）、QoS0/QoS1 发布与确认、保留消息、遗嘱、
// 退订、Keep Alive 心跳、错误口令被拒、协议版本不符被拒、畸形报文断开。
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

var (
	addr = flag.String("addr", "127.0.0.1:1883", "MQTT broker 地址")
	user = flag.String("user", "guest", "用户名")
	pass = flag.String("pass", "guest", "口令")
	// 下面两个模式用于"MQTT 消息就是内核普通队列消息"的验证（见 README 的 M7 说明）：
	// 它们各自只做一件事然后退出，由外部脚本用管理 API 检查队列。
	persistentSub = flag.String("persistent-sub", "",
		"以 Clean Session=0 订阅 probe/keep/# 后立刻断开（留下 durable 订阅队列）")
	publishTopic = flag.String("publish", "",
		"发布一条 QoS1 消息到该主题（消息体由 -payload 给出）")
	publishPayload = flag.String("payload", "", "配合 -publish 的消息体")
)

// 报文类型（MQTT 3.1.1 §2.2.1）。
const (
	pktConnect     byte = 1
	pktConnack     byte = 2
	pktPublish     byte = 3
	pktPuback      byte = 4
	pktPubrec      byte = 5
	pktPubrel      byte = 6
	pktPubcomp     byte = 7
	pktSubscribe   byte = 8
	pktSuback      byte = 9
	pktUnsubscribe byte = 10
	pktUnsuback    byte = 11
	pktPingreq     byte = 12
	pktPingresp    byte = 13
	pktDisconnect  byte = 14
)

const ioTimeout = 5 * time.Second

func main() {
	flag.Parse()

	if *persistentSub != "" {
		if err := runPersistentSub(*persistentSub); err != nil {
			fmt.Printf("FAIL  持久会话订阅: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("OK    已建立持久订阅（Clean Session=0），队列应由内核保留\n")
		return
	}
	if *publishTopic != "" {
		if err := runPublish(*publishTopic, *publishPayload); err != nil {
			fmt.Printf("FAIL  发布: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("OK    已发布 QoS1 消息到 %s\n", *publishTopic)
		return
	}

	cases := []struct {
		name string
		run  func() error
	}{
		{"连接 + CONNACK（含鉴权）", testConnect},
		{"错误口令被拒（CONNACK 0x04）", testBadPassword},
		{"协议版本不符被拒（CONNACK 0x01）", testBadProtocol},
		{"订阅 + SUBACK（QoS1）", testSubscribe},
		{"QoS0 发布 → 订阅者收到（含 + 通配）", testPublishQoS0},
		{"QoS1 发布/投递/确认闭环", testPublishQoS1},
		{"QoS2 订阅降级为 QoS1 并完成四步握手", testQoS2},
		{"保留消息：订阅即收到，清空后不再收到", testRetained},
		{"遗嘱消息：异常断开时下发", testWill},
		{"退订后不再投递", testUnsubscribe},
		{"PINGREQ → PINGRESP", testPing},
		{"畸形报文（SUBSCRIBE 标志位非法）断开连接", testMalformed},
	}

	failed := 0
	for _, c := range cases {
		if err := c.run(); err != nil {
			fmt.Printf("FAIL  %s\n      %v\n", c.name, err)
			failed++
			continue
		}
		fmt.Printf("PASS  %s\n", c.name)
	}
	total := len(cases)
	if failed > 0 {
		fmt.Printf("\n失败 %d/%d\n", failed, total)
		os.Exit(1)
	}
	fmt.Printf("\n全部通过（%d/%d）\n", total, total)
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

func testConnect() error {
	c, err := dialAndConnect("probe-connect", true, nil)
	if err != nil {
		return err
	}
	defer c.close()
	return c.disconnect()
}

func testBadPassword() error {
	c, err := dial()
	if err != nil {
		return err
	}
	defer c.close()
	if err := c.write(encodeConnect("probe-badpass", true, 30, *user, "definitely-wrong", nil)); err != nil {
		return err
	}
	_, body, err := c.read(pktConnack)
	if err != nil {
		return err
	}
	if len(body) != 2 || body[1] != 0x04 {
		return fmt.Errorf("期望 CONNACK 返回码 0x04（口令错误），实际 % x", body)
	}
	return nil
}

func testBadProtocol() error {
	c, err := dial()
	if err != nil {
		return err
	}
	defer c.close()
	body := putString(nil, "MQIsdp")
	body = append(body, 3)    // 协议级别 3（MQTT 3.1）
	body = append(body, 0x02) // clean session
	body = append(body, 0x00, 0x1e)
	body = putString(body, "probe-old")
	if err := c.write(frame(pktConnect, 0, body)); err != nil {
		return err
	}
	_, resp, err := c.read(pktConnack)
	if err != nil {
		return err
	}
	if len(resp) != 2 || resp[1] != 0x01 {
		return fmt.Errorf("期望 CONNACK 返回码 0x01（协议版本不符），实际 % x", resp)
	}
	return nil
}

func testSubscribe() error {
	c, err := dialAndConnect("probe-sub", true, nil)
	if err != nil {
		return err
	}
	defer c.close()
	codes, err := c.subscribe(1, "probe/basic/#")
	if err != nil {
		return err
	}
	if len(codes) != 1 || codes[0] != 1 {
		return fmt.Errorf("期望授予 QoS1，实际 %v", codes)
	}
	return c.disconnect()
}

func testPublishQoS0() error {
	sub, err := dialAndConnect("probe-q0-sub", true, nil)
	if err != nil {
		return err
	}
	defer sub.close()
	if _, err := sub.subscribe(1, "probe/+/temp"); err != nil {
		return err
	}
	pub, err := dialAndConnect("probe-q0-pub", true, nil)
	if err != nil {
		return err
	}
	defer pub.close()
	if err := pub.write(encodePublish(0, false, false, "probe/room1/temp", 0, []byte("21.5"))); err != nil {
		return err
	}
	m, err := sub.recvPublish()
	if err != nil {
		return err
	}
	if m.topic != "probe/room1/temp" || string(m.payload) != "21.5" {
		return fmt.Errorf("投递内容不符: topic=%q payload=%q", m.topic, m.payload)
	}
	if m.qos != 1 {
		return fmt.Errorf("QoS1 订阅应收到 QoS1 投递，实际 QoS%d", m.qos)
	}
	if err := sub.write(puback(m.packetID)); err != nil {
		return err
	}
	// 不匹配的主题不该被投递。
	if err := pub.write(encodePublish(0, false, false, "probe/room1/humidity", 0, []byte("60"))); err != nil {
		return err
	}
	if err := sub.expectSilence(300 * time.Millisecond); err != nil {
		return err
	}
	return nil
}

func testPublishQoS1() error {
	sub, err := dialAndConnect("probe-q1-sub", true, nil)
	if err != nil {
		return err
	}
	defer sub.close()
	if _, err := sub.subscribe(1, "probe/q1/#"); err != nil {
		return err
	}
	pub, err := dialAndConnect("probe-q1-pub", true, nil)
	if err != nil {
		return err
	}
	defer pub.close()
	if err := pub.publishQoS1(7, "probe/q1/a", "order-1", false); err != nil {
		return err
	}
	m, err := sub.recvPublish()
	if err != nil {
		return err
	}
	if m.qos != 1 || m.topic != "probe/q1/a" || string(m.payload) != "order-1" {
		return fmt.Errorf("投递不符: %+v", m)
	}
	return sub.write(puback(m.packetID))
}

func testQoS2() error {
	sub, err := dialAndConnect("probe-q2-sub", true, nil)
	if err != nil {
		return err
	}
	defer sub.close()
	// 手工发 QoS2 订阅：期望被降级授予 QoS1。
	body := []byte{0x00, 0x01}
	body = putString(body, "probe/q2/#")
	body = append(body, 2)
	if err := sub.write(frame(pktSubscribe, 0x02, body)); err != nil {
		return err
	}
	_, resp, err := sub.read(pktSuback)
	if err != nil {
		return err
	}
	if len(resp) < 3 || resp[2] != 1 {
		return fmt.Errorf("QoS2 订阅应降级授予 QoS1，实际 % x", resp)
	}
	pub, err := dialAndConnect("probe-q2-pub", true, nil)
	if err != nil {
		return err
	}
	defer pub.close()
	// 入站 QoS2 走完四步握手。
	if err := pub.write(encodePublish(2, false, false, "probe/q2/x", 11, []byte("exactly-once-ish"))); err != nil {
		return err
	}
	if _, _, err := pub.read(pktPubrec); err != nil {
		return err
	}
	if err := pub.write(frame(pktPubrel, 0x02, binary.BigEndian.AppendUint16(nil, 11))); err != nil {
		return err
	}
	if _, _, err := pub.read(pktPubcomp); err != nil {
		return err
	}
	m, err := sub.recvPublish()
	if err != nil {
		return err
	}
	if string(m.payload) != "exactly-once-ish" {
		return fmt.Errorf("投递内容不符: %q", m.payload)
	}
	return sub.write(puback(m.packetID))
}

func testRetained() error {
	pub, err := dialAndConnect("probe-ret-pub", true, nil)
	if err != nil {
		return fmt.Errorf("发布者连接失败: %w", err)
	}
	defer pub.close()
	if err := pub.publishQoS1(1, "probe/retained/1", "hello", true); err != nil {
		return fmt.Errorf("发布保留消息失败: %w", err)
	}
	sub, err := dialAndConnect("probe-ret-sub", true, nil)
	if err != nil {
		return fmt.Errorf("订阅者连接失败: %w", err)
	}
	defer sub.close()
	if _, err := sub.subscribe(1, "probe/retained/#"); err != nil {
		return fmt.Errorf("订阅失败: %w", err)
	}
	m, err := sub.recvPublish()
	if err != nil {
		return fmt.Errorf("等待保留消息失败: %w", err)
	}
	if !m.retain || m.topic != "probe/retained/1" || string(m.payload) != "hello" {
		return fmt.Errorf("保留消息投递不符: %+v", m)
	}
	if err := sub.write(puback(m.packetID)); err != nil {
		return fmt.Errorf("确认保留消息失败: %w", err)
	}
	// 空载荷 + retain=1 → 清除；后来的订阅者不应再收到。
	if err := pub.publishQoS1(2, "probe/retained/1", "", true); err != nil {
		return fmt.Errorf("清除保留消息失败: %w", err)
	}
	late, err := dialAndConnect("probe-ret-late", true, nil)
	if err != nil {
		return fmt.Errorf("后来的订阅者连接失败: %w", err)
	}
	defer late.close()
	if _, err := late.subscribe(1, "probe/retained/#"); err != nil {
		return fmt.Errorf("后来的订阅者订阅失败: %w", err)
	}
	if err := late.expectSilence(300 * time.Millisecond); err != nil {
		return fmt.Errorf("清除后仍收到保留消息: %w", err)
	}
	return nil
}

func testWill() error {
	watch, err := dialAndConnect("probe-will-watch", true, nil)
	if err != nil {
		return err
	}
	defer watch.close()
	if _, err := watch.subscribe(1, "probe/clients/+/status"); err != nil {
		return err
	}
	dying, err := dialAndConnect("probe-will-client", true, &will{topic: "probe/clients/probe-will-client/status", payload: "offline"})
	if err != nil {
		return err
	}
	// 直接关闭 TCP（不发 DISCONNECT）→ 属于异常断开。
	dying.close()
	m, err := watch.recvPublish()
	if err != nil {
		return err
	}
	if m.topic != "probe/clients/probe-will-client/status" || string(m.payload) != "offline" {
		return fmt.Errorf("遗嘱消息不符: %+v", m)
	}
	return watch.write(puback(m.packetID))
}

func testUnsubscribe() error {
	sub, err := dialAndConnect("probe-unsub", true, nil)
	if err != nil {
		return err
	}
	defer sub.close()
	if _, err := sub.subscribe(1, "probe/unsub/#"); err != nil {
		return err
	}
	if err := sub.write(frame(pktUnsubscribe, 0x02, append(binary.BigEndian.AppendUint16(nil, 2), putString(nil, "probe/unsub/#")...))); err != nil {
		return err
	}
	if _, _, err := sub.read(pktUnsuback); err != nil {
		return err
	}
	pub, err := dialAndConnect("probe-unsub-pub", true, nil)
	if err != nil {
		return err
	}
	defer pub.close()
	if err := pub.write(encodePublish(0, false, false, "probe/unsub/a", 0, []byte("x"))); err != nil {
		return err
	}
	return sub.expectSilence(300 * time.Millisecond)
}

func testPing() error {
	c, err := dialAndConnect("probe-ping", true, nil)
	if err != nil {
		return err
	}
	defer c.close()
	if err := c.write([]byte{pktPingreq << 4, 0}); err != nil {
		return err
	}
	if _, _, err := c.read(pktPingresp); err != nil {
		return err
	}
	return c.disconnect()
}

func testMalformed() error {
	c, err := dialAndConnect("probe-malformed", true, nil)
	if err != nil {
		return err
	}
	defer c.close()
	// SUBSCRIBE 的标志位必须是 0b0010。
	if err := c.write(frame(pktSubscribe, 0x00, []byte{0x00, 0x01, 0x00, 0x01, 'a', 0x01})); err != nil {
		return err
	}
	if err := c.expectClosed(); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// "MQTT 消息 = 内核普通队列消息"的验证入口
// ---------------------------------------------------------------------------

// runPersistentSub 以 Clean Session=0 订阅 probe/keep/# 后立刻断开。
//
// 断开是**异常断开**（不发 DISCONNECT）：这样能看到内核把未确认消息放回队列，
// 由外部脚本用管理 API 读出来 —— 证明 MQTT 的消息确实躺在同一个内核队列里。
func runPersistentSub(clientID string) error {
	c, err := dial()
	if err != nil {
		return err
	}
	defer c.close()
	if err := c.write(encodeConnect(clientID, false, 30, *user, *pass, nil)); err != nil {
		return err
	}
	_, body, err := c.read(pktConnack)
	if err != nil {
		return err
	}
	if len(body) != 2 || body[1] != 0 {
		return fmt.Errorf("CONNECT 被拒绝: % x", body)
	}
	codes, err := c.subscribe(1, "probe/keep/#")
	if err != nil {
		return err
	}
	if len(codes) != 1 || codes[0] != 1 {
		return fmt.Errorf("期望授予 QoS1，实际 %v", codes)
	}
	// 主动断开 TCP：clean=0 的订阅队列（durable）会留在内核里。
	c.close()
	return nil
}

// runPublish 发布一条 QoS1 消息。
func runPublish(topic, payload string) error {
	if topic == "" {
		return fmt.Errorf("必须给出 -publish 主题")
	}
	c, err := dialAndConnect("probe-one-shot", true, nil)
	if err != nil {
		return err
	}
	defer c.close()
	if err := c.publishQoS1(1, topic, payload, false); err != nil {
		return err
	}
	return c.disconnect()
}

// ---------------------------------------------------------------------------
// 极简 MQTT 客户端
// ---------------------------------------------------------------------------

type will struct {
	topic   string
	payload string
}

type client struct {
	conn net.Conn
	br   *bufio.Reader
}

func dial() (*client, error) {
	conn, err := net.DialTimeout("tcp", *addr, ioTimeout)
	if err != nil {
		return nil, fmt.Errorf("连接 %s 失败: %w", *addr, err)
	}
	return &client{conn: conn, br: bufio.NewReader(conn)}, nil
}

// dialAndConnect 建连并完成 CONNECT/CONNACK，返回可用连接。
func dialAndConnect(clientID string, clean bool, w *will) (*client, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	if err := c.write(encodeConnect(clientID, clean, 30, *user, *pass, w)); err != nil {
		c.close()
		return nil, err
	}
	_, body, err := c.read(pktConnack)
	if err != nil {
		c.close()
		return nil, err
	}
	if len(body) != 2 {
		c.close()
		return nil, fmt.Errorf("CONNACK 长度应为 2: % x", body)
	}
	if body[1] != 0 {
		c.close()
		return nil, fmt.Errorf("CONNECT 被拒绝，返回码 %d", body[1])
	}
	return c, nil
}

func (c *client) close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *client) write(b []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(ioTimeout)); err != nil {
		return err
	}
	_, err := c.conn.Write(b)
	return err
}

// read 读取一个报文并断言其类型。
func (c *client) read(want byte) (flags byte, body []byte, err error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
		return 0, nil, err
	}
	head, err := c.br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := readVarint(c.br)
	if err != nil {
		return 0, nil, err
	}
	body = make([]byte, n)
	if _, err := io.ReadFull(c.br, body); err != nil {
		return 0, nil, err
	}
	if head>>4 != want {
		return 0, nil, fmt.Errorf("期望报文类型 %d，实际 %d（body=% x）", want, head>>4, body)
	}
	return head & 0x0f, body, nil
}

// expectSilence 断言在窗口内没有报文到达。
func (c *client) expectSilence(d time.Duration) error {
	if err := c.conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		return err
	}
	b, err := c.br.ReadByte()
	if err == nil {
		return fmt.Errorf("期望没有报文，却读到首字节 0x%02x", b)
	}
	_ = c.conn.SetReadDeadline(time.Time{})
	return nil
}

// expectClosed 断言服务端关闭了连接。
func (c *client) expectClosed() error {
	if err := c.conn.SetReadDeadline(time.Now().Add(ioTimeout)); err != nil {
		return err
	}
	if _, err := c.br.ReadByte(); err == nil {
		return fmt.Errorf("期望连接被服务端关闭，却读到了数据")
	}
	return nil
}

func (c *client) subscribe(packetID uint16, filters ...string) ([]byte, error) {
	body := binary.BigEndian.AppendUint16(nil, packetID)
	for _, f := range filters {
		body = putString(body, f)
		body = append(body, 1) // 请求 QoS 1
	}
	if err := c.write(frame(pktSubscribe, 0x02, body)); err != nil {
		return nil, err
	}
	_, resp, err := c.read(pktSuback)
	if err != nil {
		return nil, err
	}
	if len(resp) < 2 {
		return nil, fmt.Errorf("SUBACK 过短: % x", resp)
	}
	return resp[2:], nil
}

// publishQoS1 发布一条 QoS1 消息并等待 PUBACK。
func (c *client) publishQoS1(packetID uint16, topic, payload string, retain bool) error {
	if err := c.write(encodePublish(1, false, retain, topic, packetID, []byte(payload))); err != nil {
		return err
	}
	_, body, err := c.read(pktPuback)
	if err != nil {
		return err
	}
	if id := binary.BigEndian.Uint16(body); id != packetID {
		return fmt.Errorf("PUBACK 的 Packet ID 应为 %d，实际 %d", packetID, id)
	}
	return nil
}

func (c *client) disconnect() error {
	return c.write([]byte{pktDisconnect << 4, 0})
}

type incoming struct {
	topic    string
	payload  []byte
	qos      byte
	packetID uint16
	retain   bool
}

func (c *client) recvPublish() (incoming, error) {
	flags, body, err := c.read(pktPublish)
	if err != nil {
		return incoming{}, err
	}
	if len(body) < 2 {
		return incoming{}, fmt.Errorf("PUBLISH 过短: % x", body)
	}
	n := int(binary.BigEndian.Uint16(body))
	if len(body) < 2+n {
		return incoming{}, fmt.Errorf("PUBLISH 主题长度越界: % x", body)
	}
	out := incoming{
		topic:  string(body[2 : 2+n]),
		qos:    (flags >> 1) & 0x03,
		retain: flags&0x01 != 0,
	}
	rest := body[2+n:]
	if out.qos > 0 {
		if len(rest) < 2 {
			return incoming{}, fmt.Errorf("PUBLISH 缺少 Packet ID: % x", body)
		}
		out.packetID = binary.BigEndian.Uint16(rest)
		rest = rest[2:]
	}
	out.payload = rest
	return out, nil
}

// ---------------------------------------------------------------------------
// 报文编码（与内核实现独立，刻意各写一份）
// ---------------------------------------------------------------------------

func frame(typ, flags byte, body []byte) []byte {
	out := []byte{typ<<4 | flags}
	n := len(body)
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		out = append(out, d)
		if n == 0 {
			return append(out, body...)
		}
	}
}

func putString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

func encodeConnect(clientID string, clean bool, keepAlive uint16, user, pass string, w *will) []byte {
	body := putString(nil, "MQTT")
	body = append(body, 4) // 协议级别（3.1.1）
	flags := byte(0)
	if clean {
		flags |= 0x02
	}
	if w != nil {
		flags |= 0x04
	}
	if user != "" {
		flags |= 0x80
	}
	if pass != "" {
		flags |= 0x40
	}
	body = append(body, flags)
	body = binary.BigEndian.AppendUint16(body, keepAlive)
	body = putString(body, clientID)
	if w != nil {
		body = putString(body, w.topic)
		body = binary.BigEndian.AppendUint16(body, uint16(len(w.payload)))
		body = append(body, w.payload...)
	}
	if user != "" {
		body = putString(body, user)
	}
	if pass != "" {
		body = binary.BigEndian.AppendUint16(body, uint16(len(pass)))
		body = append(body, pass...)
	}
	return frame(pktConnect, 0, body)
}

func encodePublish(qos byte, dup, retain bool, topic string, packetID uint16, payload []byte) []byte {
	flags := qos << 1
	if dup {
		flags |= 0x08
	}
	if retain {
		flags |= 0x01
	}
	body := putString(nil, topic)
	if qos > 0 {
		body = binary.BigEndian.AppendUint16(body, packetID)
	}
	body = append(body, payload...)
	return frame(pktPublish, flags, body)
}

func puback(packetID uint16) []byte {
	return frame(pktPuback, 0, binary.BigEndian.AppendUint16(nil, packetID))
}

func readVarint(r *bufio.Reader) (int, error) {
	value, multiplier := 0, 1
	for i := 0; i < 4; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(b&0x7f) * multiplier
		if b&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("剩余长度超过 4 字节")
}
