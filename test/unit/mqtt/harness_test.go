package mqtt_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/protocol/mqtt"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件是 MQTT 测试的脚手架：
//   - testBroker 起一个**真实内核**（单机），MQTT 主题落在它的 amq.topic 上；
//   - mqttClient 是**手写字节**的极简 MQTT 客户端 —— 刻意不复用插件的编解码器，
//     否则"编码器写错、解码器同样写错"会两边一起绿，测试就失去意义。
//
// 连接用真实 TCP（而不是 net.Pipe）：net.Pipe 是无缓冲的，服务端一边写、
// 客户端一边写就会互相阻塞。真实 socket 由内核缓冲，更接近实际部署。

const testTimeout = 5 * time.Second

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// testBroker 起一个单机内核。
func testBroker(t *testing.T) *broker.Broker {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载默认配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	b, err := broker.New(discardLogger(), cfg)
	if err != nil {
		t.Fatalf("启动内核失败: %v", err)
	}
	t.Cleanup(b.Close)
	return b
}

// dial 在进程内接一条 MQTT 连接：一端交给插件 Serve，另一端给测试客户端。
//
// plugin 实例由调用方持有 —— 保留消息存在插件内存里，必须跨连接共享才谈得上语义。
func dial(t *testing.T, b *broker.Broker, p *mqtt.Plugin) *mqttClient {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听测试端口失败: %v", err)
	}
	// accepted 用于"等真正 accept 完成后再关监听"。
	//
	// 顺序很关键：连接在 accept 之前只是躺在**监听队列**里，此时关掉监听，
	// 操作系统会把这条尚未 accept 的连接直接 RST 掉（Windows 上尤其明显）——
	// 表现为客户端随后的写入报 "connection forcibly closed by the remote host"，
	// 而服务端其实什么都没做。这类偶发失败极难排查，因此在这里一次性杜绝。
	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		close(accepted)
		if err != nil {
			return
		}
		defer conn.Close()
		core := b.NewSession(conn.RemoteAddr(), conn.LocalAddr())
		_ = p.Serve(context.Background(), conn, core)
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		t.Fatalf("连接测试端口失败: %v", err)
	}
	<-accepted
	_ = ln.Close()
	c := &mqttClient{t: t, conn: conn, br: bufio.NewReader(conn)}
	t.Cleanup(c.close)
	return c
}

// ---------------------------------------------------------------------------
// 手写 MQTT 客户端
// ---------------------------------------------------------------------------

// 返回码常量在测试侧独立定义（插件里的同名常量未导出，测试不该依赖实现细节）。
const (
	connBadProtocol    = 0x01
	connBadCredentials = 0x04
	subackFailure      = 0x80
)

type mqttClient struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

// otherProtocolSession 取一个"非 MQTT 协议"的内核会话：用于验证跨协议互通
// （MQTT 订阅者能收到别的协议发布到 amq.topic 的消息）。
func otherProtocolSession(t *testing.T, b *broker.Broker) plugin.Session {
	t.Helper()
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40001}
	core := b.NewSession(remote, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672})
	if _, err := core.Authenticate(context.Background(), "PLAIN", []byte("\x00guest\x00guest"), remote); err != nil {
		t.Fatalf("内核会话认证失败: %v", err)
	}
	sess, err := core.Session("/")
	if err != nil {
		t.Fatalf("打开默认 vhost 失败: %v", err)
	}
	t.Cleanup(func() {
		sess.Close()
		core.Close()
	})
	return sess
}

func (c *mqttClient) close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// willSpec 描述 CONNECT 里的遗嘱消息。
type willSpec struct {
	topic   string
	payload string
	qos     byte
	retain  bool
}

func (c *mqttClient) write(b []byte) {
	c.t.Helper()
	_ = c.conn.SetWriteDeadline(time.Now().Add(testTimeout))
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatalf("写入 MQTT 报文失败: %v", err)
	}
}

// readPacket 读取一个报文；超时即失败。
func (c *mqttClient) readPacket() (typ, flags byte, body []byte) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	head, err := c.br.ReadByte()
	if err != nil {
		c.t.Fatalf("读取 MQTT 报文头失败: %v", err)
	}
	remaining, err := readVarint(c.br)
	if err != nil {
		c.t.Fatalf("读取剩余长度失败: %v", err)
	}
	body = make([]byte, remaining)
	if _, err := io.ReadFull(c.br, body); err != nil {
		c.t.Fatalf("读取 MQTT 报文体失败: %v", err)
	}
	return head >> 4, head & 0x0f, body
}

// expect 读取一个报文并断言其类型。
func (c *mqttClient) expect(typ byte) (flags byte, body []byte) {
	c.t.Helper()
	got, flags, body := c.readPacket()
	if got != typ {
		c.t.Fatalf("期望报文类型 %d，实际 %d（body=% x）", typ, got, body)
	}
	return flags, body
}

// expectClosed 断言连接被**服务端关闭**（畸形报文/认证失败/DISCONNECT 后的收尾）。
//
// 判据是"读到 EOF 而不是超时"：若服务端只是不回数据、连接仍然活着，
// 超时同样会让 Read 返回错误 —— 那样这条断言就会变成"永远通过"，失去意义。
func (c *mqttClient) expectClosed() {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	_, err := c.br.ReadByte()
	switch {
	case err == nil:
		c.t.Fatalf("期望连接被关闭，但仍读到了数据")
	case errors.Is(err, os.ErrDeadlineExceeded):
		c.t.Fatalf("期望服务端主动关闭连接，但连接仍然存活（读超时）")
	}
}

// expectNoPacket 断言在短窗口内没有任何报文到达（"不该投递"的负向用例）。
func (c *mqttClient) expectNoPacket() {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if b, err := c.br.ReadByte(); err == nil {
		c.t.Fatalf("期望没有报文，实际读到首字节 0x%02x", b)
	}
	_ = c.conn.SetReadDeadline(time.Time{})
}

// connect 发送 CONNECT 并返回 CONNACK。
func (c *mqttClient) connect(clientID string, clean bool, keepAlive uint16, user, pass string, will *willSpec) (sessionPresent bool, code byte) {
	c.t.Helper()
	c.write(encodeConnect(clientID, clean, keepAlive, user, pass, will))
	_, body := c.expect(pktConnack)
	if len(body) != 2 {
		c.t.Fatalf("CONNACK 长度应为 2，实际 %d", len(body))
	}
	return body[0]&0x01 != 0, body[1]
}

// mustConnect 断言握手成功。
func (c *mqttClient) mustConnect(clientID string, clean bool) {
	c.t.Helper()
	present, code := c.connect(clientID, clean, 30, "guest", "guest", nil)
	if code != 0 {
		c.t.Fatalf("CONNECT 期望被接受，实际返回码 %d", code)
	}
	if present {
		c.t.Fatalf("全新客户端的 Session Present 应为 false")
	}
}

// subscribe 发送 SUBSCRIBE 并返回 SUBACK 的返回码。
func (c *mqttClient) subscribe(packetID uint16, filters ...string) []byte {
	c.t.Helper()
	c.write(encodeSubscribe(packetID, filters...))
	_, body := c.expect(pktSuback)
	_, codes := parseSuback(body)
	return codes
}

// publishQoS0 发布一条 QoS0 消息。
func (c *mqttClient) publishQoS0(topic, payload string, retain bool) {
	c.t.Helper()
	c.write(encodePublish(0, false, retain, topic, 0, []byte(payload)))
}

// publishQoS1 发布一条 QoS1 消息并等待 PUBACK。
func (c *mqttClient) publishQoS1(packetID uint16, topic, payload string, retain bool) {
	c.t.Helper()
	c.write(encodePublish(1, false, retain, topic, packetID, []byte(payload)))
	_, body := c.expect(pktPuback)
	if id := binary.BigEndian.Uint16(body); id != packetID {
		c.t.Fatalf("PUBACK 的 Packet ID 应为 %d，实际 %d", packetID, id)
	}
}

// publishQoS2 走完 QoS2 的四步握手。
func (c *mqttClient) publishQoS2(packetID uint16, topic, payload string) {
	c.t.Helper()
	c.write(encodePublish(2, false, false, topic, packetID, []byte(payload)))
	_, body := c.expect(pktPubrec)
	if id := binary.BigEndian.Uint16(body); id != packetID {
		c.t.Fatalf("PUBREC 的 Packet ID 应为 %d，实际 %d", packetID, id)
	}
	c.write(encodePubrel(packetID))
	_, body = c.expect(pktPubcomp)
	if id := binary.BigEndian.Uint16(body); id != packetID {
		c.t.Fatalf("PUBCOMP 的 Packet ID 应为 %d，实际 %d", packetID, id)
	}
}

// incomingPublish 是服务端下发的一条 PUBLISH。
type incomingPublish struct {
	Topic    string
	Payload  string
	QoS      byte
	PacketID uint16
	Dup      bool
	Retain   bool
}

// recvPublish 收到一条 PUBLISH（不带确认）。
func (c *mqttClient) recvPublish() incomingPublish {
	c.t.Helper()
	flags, body := c.expect(pktPublish)
	return parsePublish(flags, body, c.t)
}

// recvPublishQoS1 收到一条 QoS1 PUBLISH 并回 PUBACK。
func (c *mqttClient) recvPublishQoS1() incomingPublish {
	c.t.Helper()
	p := c.recvPublishOnly()
	c.write(puback(p.PacketID))
	return p
}

// recvPublishOnly 收到一条 QoS1 PUBLISH 但**不确认**：用于验证未确认消息的重投。
func (c *mqttClient) recvPublishOnly() incomingPublish {
	c.t.Helper()
	p := c.recvPublish()
	if p.QoS != 1 {
		c.t.Fatalf("期望 QoS1 投递，实际 QoS%d", p.QoS)
	}
	return p
}

func (c *mqttClient) unsubscribe(packetID uint16, filters ...string) {
	c.t.Helper()
	c.write(encodeUnsubscribe(packetID, filters...))
	_, body := c.expect(pktUnsuback)
	if id := binary.BigEndian.Uint16(body); id != packetID {
		c.t.Fatalf("UNSUBACK 的 Packet ID 应为 %d，实际 %d", packetID, id)
	}
}

func (c *mqttClient) ping() {
	c.t.Helper()
	c.write([]byte{pktPingreq << 4, 0})
	c.expect(pktPingresp)
}

func (c *mqttClient) disconnect() {
	c.t.Helper()
	c.write([]byte{pktDisconnect << 4, 0})
}

// ---------------------------------------------------------------------------
// 报文类型与手写编解码（独立于插件实现）
// ---------------------------------------------------------------------------

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
			break
		}
	}
	return append(out, body...)
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

func putString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

// encodeConnect 手写编码 CONNECT。
func encodeConnect(clientID string, clean bool, keepAlive uint16, user, pass string, will *willSpec) []byte {
	body := putString(nil, "MQTT")
	body = append(body, 4) // 协议级别（3.1.1）
	flags := byte(0)
	if clean {
		flags |= 0x02
	}
	if will != nil {
		flags |= 0x04 | will.qos<<3
		if will.retain {
			flags |= 0x20
		}
	}
	hasUser := user != ""
	hasPass := pass != ""
	if hasUser {
		flags |= 0x80
	}
	if hasPass {
		flags |= 0x40
	}
	body = append(body, flags)
	body = binary.BigEndian.AppendUint16(body, keepAlive)
	body = putString(body, clientID)
	if will != nil {
		body = putString(body, will.topic)
		body = binary.BigEndian.AppendUint16(body, uint16(len(will.payload)))
		body = append(body, will.payload...)
	}
	if hasUser {
		body = putString(body, user)
	}
	if hasPass {
		body = binary.BigEndian.AppendUint16(body, uint16(len(pass)))
		body = append(body, pass...)
	}
	return frame(pktConnect, 0, body)
}

func encodeSubscribe(packetID uint16, filters ...string) []byte {
	body := binary.BigEndian.AppendUint16(nil, packetID)
	for _, f := range filters {
		body = putString(body, f)
		body = append(body, 1) // 请求 QoS 1
	}
	return frame(pktSubscribe, 0x02, body)
}

func encodeUnsubscribe(packetID uint16, filters ...string) []byte {
	body := binary.BigEndian.AppendUint16(nil, packetID)
	for _, f := range filters {
		body = putString(body, f)
	}
	return frame(pktUnsubscribe, 0x02, body)
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

func encodePubrel(packetID uint16) []byte {
	return frame(pktPubrel, 0x02, binary.BigEndian.AppendUint16(nil, packetID))
}

func parseSuback(body []byte) (uint16, []byte) {
	return binary.BigEndian.Uint16(body), body[2:]
}

func parsePublish(flags byte, body []byte, t *testing.T) incomingPublish {
	t.Helper()
	n := int(binary.BigEndian.Uint16(body))
	if len(body) < 2+n {
		t.Fatalf("PUBLISH 主题长度越界: % x", body)
	}
	out := incomingPublish{
		Topic:  string(body[2 : 2+n]),
		QoS:    (flags >> 1) & 0x03,
		Dup:    flags&0x08 != 0,
		Retain: flags&0x01 != 0,
	}
	rest := body[2+n:]
	if out.QoS > 0 {
		out.PacketID = binary.BigEndian.Uint16(rest)
		rest = rest[2:]
	}
	out.Payload = string(rest)
	return out
}
