// Package amqp091_test 从包外驱动 AMQP 0-9-1 协议插件：一端交给插件 Serve，另一端是
// **手写字节**的极简客户端。
//
// 为什么手写而不是复用 internal/protocol/spec 的编解码器：编解码器写错时"编码与解码
// 一起错"会让用例照样全绿。协议线格式的权威验证是真实客户端（test/integration/amqp091probe、
// swiftmq-test 的 pika 用例），这里手写字节与之呼应（做法同 test/unit/mqtt 的脚手架）。
package amqp091_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/protocol/amqp091"
	"github.com/houzch/swiftmq/pkg/plugin"
)

const testTimeout = 5 * time.Second

// 帧类型与方法号是**协议规范值**，在测试侧独立写死（不引用实现的常量）。
const (
	frameMethod    byte = 1
	frameHeader    byte = 2
	frameBody      byte = 3
	frameHeartbeat byte = 8

	classConnection uint16 = 10
	classChannel    uint16 = 20
	classQueue      uint16 = 50
	classBasic      uint16 = 60

	methodConnectionStartOk uint16 = 11
	methodConnectionTuneOk  uint16 = 31
	methodConnectionOpen    uint16 = 40
	methodConnectionOpenOk  uint16 = 41

	methodChannelOpen    uint16 = 10
	methodChannelOpenOk  uint16 = 11
	methodChannelClose   uint16 = 40
	methodChannelCloseOk uint16 = 41
	methodQueueDeclare   uint16 = 10
	methodQueueDeclareOk uint16 = 11
	methodQueueDelete    uint16 = 40
	methodQueueDeleteOk  uint16 = 41
	methodBasicConsume   uint16 = 20
	methodBasicConsumeOk uint16 = 21
	methodBasicPublish   uint16 = 40
	methodBasicDeliver   uint16 = 60
	methodBasicCancel    uint16 = 30
	methodBasicCancelOk  uint16 = 31
)

// replyQueue 是 direct reply-to 的伪队列名（协议级契约，写死字面量）。
const replyQueue = "amq.rabbitmq.reply-to"

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeHost 满足 plugin.Host：AMQP 插件的日志器来自 Init（MQTT 插件不用日志器，因此那边没有这一步）。
type fakeHost struct{ registered plugin.Protocol }

func (h *fakeHost) PluginName() string   { return "amqp091" }
func (h *fakeHost) Logger() *slog.Logger { return discardLogger() }
func (h *fakeHost) Config(any) error     { return nil }

func (h *fakeHost) RegisterProtocol(p plugin.Protocol) error {
	h.registered = p
	return nil
}

// newPlugin 构造并初始化一个 AMQP 插件实例。
//
// 每次都新建：插件本身不持有跨连接状态（保留消息之类的状态在 MQTT 插件里，AMQP 没有）。
func newPlugin(t *testing.T) *amqp091.Plugin {
	t.Helper()
	p := amqp091.New()
	if err := p.Init(&fakeHost{}); err != nil {
		t.Fatalf("初始化 AMQP 协议插件失败: %v", err)
	}
	return p
}

// newTestBroker 起一个真实的单机内核（协议插件必须挂在真内核上才谈得上语义）。
func newTestBroker(t *testing.T) *broker.Broker {
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

// dial 接一条真实的 AMQP 0-9-1 连接并完成握手。
//
// 用真实 TCP 而不是 net.Pipe：net.Pipe 无缓冲，一侧在写、另一侧也在写时会互相阻塞。
func dial(t *testing.T, b *broker.Broker) *client {
	t.Helper()
	p := newPlugin(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听测试端口失败: %v", err)
	}
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

	c := &client{t: t, conn: conn, br: bufio.NewReader(conn)}
	t.Cleanup(func() { _ = conn.Close() })
	c.handshake()
	return c
}

// ---------------------------------------------------------------------------
// 手写 AMQP 客户端
// ---------------------------------------------------------------------------

type client struct {
	t    *testing.T
	conn net.Conn
	br   *bufio.Reader
}

func (c *client) handshake() {
	c.t.Helper()
	c.sendBytes([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1})

	classID, methodID, _ := c.expectMethod(0)
	if classID != classConnection || methodID != 10 {
		c.t.Fatalf("期望 Connection.Start，实际 %d/%d", classID, methodID)
	}
	// Start-Ok：空的客户端属性表 + PLAIN + guest/guest + en_US
	args := appendTable(nil)
	args = appendShortStr(args, "PLAIN")
	args = appendLongStr(args, []byte("\x00guest\x00guest"))
	args = appendShortStr(args, "en_US")
	c.sendMethod(0, classConnection, methodConnectionStartOk, args)

	classID, methodID, _ = c.expectMethod(0)
	if classID != classConnection || methodID != 30 {
		c.t.Fatalf("期望 Connection.Tune，实际 %d/%d", classID, methodID)
	}
	// Tune-Ok 全填 0：服务端按自己的默认值生效（channel-max / frame-max / 心跳）
	tuneOk := appendU16(nil, 0)
	tuneOk = appendU32(tuneOk, 0)
	tuneOk = appendU16(tuneOk, 0)
	c.sendMethod(0, classConnection, methodConnectionTuneOk, tuneOk)

	openArgs := appendShortStr(nil, "/")    // vhost
	openArgs = appendShortStr(openArgs, "") // capabilities
	openArgs = append(openArgs, 0)          // insist
	c.sendMethod(0, classConnection, methodConnectionOpen, openArgs)

	classID, methodID, _ = c.expectMethod(0)
	if classID != classConnection || methodID != methodConnectionOpenOk {
		c.t.Fatalf("期望 Connection.Open-Ok，实际 %d/%d", classID, methodID)
	}
}

func (c *client) openChannel(ch uint16) {
	c.t.Helper()
	c.sendMethod(ch, classChannel, methodChannelOpen, appendShortStr(nil, ""))
	classID, methodID, _ := c.expectMethod(ch)
	if classID != classChannel || methodID != methodChannelOpenOk {
		c.t.Fatalf("期望 Channel.Open-Ok，实际 %d/%d", classID, methodID)
	}
}

// queueDeclare 声明一个队列并返回 (消息数, 消费者数)。
//
// 固定声明为 durable：瞬时（non-durable）非独占队列在 RabbitMQ 4.x 语义下是被禁止的（541），
// 而那些语义由 test/unit/broker 的用例覆盖，这里只想要一条普通队列。
func (c *client) queueDeclare(ch uint16, name string) (uint32, uint32) {
	c.t.Helper()
	args := appendU16(nil, 0) // reserved-1
	args = appendShortStr(args, name)
	args = append(args, 0x02) // 标志位：durable=true，其余为 false
	args = appendTable(args)
	c.sendMethod(ch, classQueue, methodQueueDeclare, args)

	classID, methodID, payload := c.expectMethod(ch)
	if classID != classQueue || methodID != methodQueueDeclareOk {
		c.t.Fatalf("期望 Queue.Declare-Ok，实际 %d/%d", classID, methodID)
	}
	r := newArgReader(c.t, payload)
	r.shortstr() // 队列名
	return r.u32(), r.u32()
}

// queueDelete 删除队列并返回被删除的消息数。
func (c *client) queueDelete(ch uint16, name string) uint32 {
	c.t.Helper()
	args := appendU16(nil, 0)
	args = appendShortStr(args, name)
	args = append(args, 0) // if-unused / if-empty / nowait
	c.sendMethod(ch, classQueue, methodQueueDelete, args)

	classID, methodID, payload := c.expectMethod(ch)
	if classID != classQueue || methodID != methodQueueDeleteOk {
		c.t.Fatalf("期望 Queue.Delete-Ok，实际 %d/%d", classID, methodID)
	}
	return newArgReader(c.t, payload).u32()
}

// consume 注册消费者并返回服务端确认的 consumer-tag。
//
// priority 非 nil 时带上 `x-priority`（用 short-short-int 承载，pika 也这么发）；
// extra 用于构造非法取值（如 longstr）。
func (c *client) consume(ch uint16, queue string, noAck bool, priority *int, extra []byte) string {
	c.t.Helper()
	c.sendConsume(ch, queue, noAck, priority, extra)

	classID, methodID, payload := c.expectMethod(ch)
	if classID != classBasic || methodID != methodBasicConsumeOk {
		c.t.Fatalf("期望 Basic.Consume-Ok，实际 %d/%d", classID, methodID)
	}
	return newArgReader(c.t, payload).shortstr()
}

// sendConsume 只发出 Basic.Consume（用于"服务端必须拒绝"的负向用例）。
func (c *client) sendConsume(ch uint16, queue string, noAck bool, priority *int, extra []byte) {
	c.t.Helper()
	args := appendU16(nil, 0)
	args = appendShortStr(args, queue)
	args = appendShortStr(args, "") // consumer-tag：留空由服务端生成
	bits := byte(0)
	if noAck {
		bits |= 0x02
	}
	args = append(args, bits)
	if extra != nil {
		args = append(args, extra...)
	} else if priority != nil {
		args = appendTable(args, entryInt8("x-priority", int8(*priority)))
	} else {
		args = appendTable(args)
	}
	c.sendMethod(ch, classBasic, methodBasicConsume, args)
}

// publish 发布一条消息，并紧接着发出内容头与内容体帧。
func (c *client) publish(ch uint16, exchange, routingKey string, props publishProps, body []byte) {
	c.t.Helper()
	args := appendU16(nil, 0) // reserved-1
	args = appendShortStr(args, exchange)
	args = appendShortStr(args, routingKey)
	args = append(args, 0) // mandatory / immediate
	c.sendMethod(ch, classBasic, methodBasicPublish, args)

	var flags uint16
	var fields []byte
	if props.replyTo != "" {
		flags |= 0x0200
	}
	if props.correlationID != "" {
		flags |= 0x0400
	}
	// 属性按位从高到低排列：correlation-id(bit10) 在 reply-to(bit9) 之前
	if props.correlationID != "" {
		fields = appendShortStr(fields, props.correlationID)
	}
	if props.replyTo != "" {
		fields = appendShortStr(fields, props.replyTo)
	}

	header := appendU16(nil, classBasic)
	header = appendU16(header, 0) // weight
	header = appendU64(header, uint64(len(body)))
	header = appendU16(header, flags)
	header = append(header, fields...)
	c.sendFrame(frameHeader, ch, header)
	if len(body) > 0 {
		c.sendFrame(frameBody, ch, body)
	}
}

// publishProps 是发布时用到的内容属性（只覆盖用例需要的字段）。
type publishProps struct {
	correlationID string
	replyTo       string
}

// cancel 取消一个消费者。
func (c *client) cancel(ch uint16, tag string) {
	c.t.Helper()
	args := appendShortStr(nil, tag)
	args = append(args, 0) // nowait=false
	c.sendMethod(ch, classBasic, methodBasicCancel, args)
	classID, methodID, _ := c.expectMethod(ch)
	if classID != classBasic || methodID != methodBasicCancelOk {
		c.t.Fatalf("期望 Basic.Cancel-Ok，实际 %d/%d", classID, methodID)
	}
}

// delivery 是一条 Basic.Deliver 及其内容。
type delivery struct {
	tag         uint64
	consumerTag string
	routingKey  string
	redelivered bool
	correlation string
	replyTo     string
	body        []byte
}

// readDelivery 读取一条投递（Basic.Deliver + 内容帧）。
func (c *client) readDelivery(ch uint16) delivery {
	c.t.Helper()
	classID, methodID, payload := c.expectMethod(ch)
	if classID != classBasic || methodID != methodBasicDeliver {
		c.t.Fatalf("期望 Basic.Deliver，实际 %d/%d", classID, methodID)
	}
	r := newArgReader(c.t, payload)
	d := delivery{consumerTag: r.shortstr(), tag: r.u64()}
	d.redelivered = r.octet()&0x01 != 0
	r.shortstr() // exchange
	d.routingKey = r.shortstr()

	header, body := c.readContent(ch)
	d.correlation = header.correlationID
	d.replyTo = header.replyTo
	d.body = body
	return d
}

// readContent 读取内容头帧与全部内容体帧。
func (c *client) readContent(ch uint16) (contentHeader, []byte) {
	c.t.Helper()
	typ, gotCh, payload := c.readFrame()
	if typ != frameHeader || gotCh != ch {
		c.t.Fatalf("期望内容头帧，实际 type=%d channel=%d", typ, gotCh)
	}
	h := parseContentHeader(c.t, payload)
	body := make([]byte, 0, h.bodySize)
	for uint64(len(body)) < h.bodySize {
		typ, gotCh, payload = c.readFrame()
		if typ != frameBody || gotCh != ch {
			c.t.Fatalf("期望内容体帧，实际 type=%d channel=%d", typ, gotCh)
		}
		body = append(body, payload...)
	}
	return h, body
}

// expectChannelClose 等待服务端关闭指定 channel，并回 Close-Ok；返回错误码与文案。
func (c *client) expectChannelClose(ch uint16) (uint16, string) {
	c.t.Helper()
	for {
		classID, methodID, payload := c.expectMethod(ch)
		if classID == classChannel && methodID == methodChannelClose {
			r := newArgReader(c.t, payload)
			code := r.u16()
			text := r.shortstr()
			c.sendMethod(ch, classChannel, methodChannelCloseOk, nil)
			return code, text
		}
	}
}

// expectNoDelivery 断言在短窗口内没有投递到达（负向用例）。
func (c *client) expectNoDelivery(ch uint16) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	defer func() { _ = c.conn.SetReadDeadline(time.Now().Add(testTimeout)) }()
	head := make([]byte, 7)
	if _, err := io.ReadFull(c.br, head); err == nil {
		c.t.Fatalf("期望没有投递，实际收到帧 type=%d", head[0])
	}
}

// drainDeliveries 读走直到**短暂空闲**为止的全部投递（用于统计分配比例）。
func (c *client) drainDeliveries(ch uint16) []delivery {
	c.t.Helper()
	var out []delivery
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		classID, methodID, payload := c.expectMethodOrTimeout(ch)
		if classID == 0 {
			break
		}
		if classID != classBasic || methodID != methodBasicDeliver {
			c.t.Fatalf("期望 Basic.Deliver，实际 %d/%d", classID, methodID)
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
		r := newArgReader(c.t, payload)
		d := delivery{consumerTag: r.shortstr(), tag: r.u64()}
		d.redelivered = r.octet()&0x01 != 0
		r.shortstr()
		d.routingKey = r.shortstr()
		h, body := c.readContent(ch)
		d.correlation, d.replyTo, d.body = h.correlationID, h.replyTo, body
		out = append(out, d)
	}
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	return out
}

// expectMethodOrTimeout 读一条方法帧；超时返回 classID=0。
func (c *client) expectMethodOrTimeout(ch uint16) (classID, methodID uint16, args []byte) {
	c.t.Helper()
	for {
		typ, gotCh, payload := c.readFrameOrTimeout()
		if typ == 0 {
			return 0, 0, nil
		}
		if typ == frameHeartbeat {
			continue
		}
		if typ != frameMethod {
			c.t.Fatalf("期望方法帧，实际类型 %d", typ)
		}
		if gotCh != ch {
			c.t.Fatalf("期望 channel %d 上的帧，实际 %d", ch, gotCh)
		}
		if len(payload) < 4 {
			c.t.Fatalf("方法帧载荷过短: % x", payload)
		}
		return binary.BigEndian.Uint16(payload[0:2]), binary.BigEndian.Uint16(payload[2:4]), payload[4:]
	}
}

// ---------------------------------------------------------------------------
// 帧收发
// ---------------------------------------------------------------------------

func (c *client) sendBytes(b []byte) {
	c.t.Helper()
	_ = c.conn.SetWriteDeadline(time.Now().Add(testTimeout))
	if _, err := c.conn.Write(b); err != nil {
		c.t.Fatalf("写入失败: %v", err)
	}
}

func (c *client) sendMethod(ch uint16, classID, methodID uint16, args []byte) {
	c.t.Helper()
	payload := appendU16(nil, classID)
	payload = appendU16(payload, methodID)
	payload = append(payload, args...)
	c.sendFrame(frameMethod, ch, payload)
}

func (c *client) sendFrame(typ byte, ch uint16, payload []byte) {
	c.t.Helper()
	head := make([]byte, 7)
	head[0] = typ
	binary.BigEndian.PutUint16(head[1:3], ch)
	binary.BigEndian.PutUint32(head[3:7], uint32(len(payload)))
	frame := append(head, payload...)
	c.sendBytes(append(frame, 0xCE))
}

func (c *client) readFrame() (typ byte, ch uint16, payload []byte) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(testTimeout))
	typ, ch, payload, ok := c.readFrame0()
	if !ok {
		c.t.Fatalf("读取帧失败（超时或连接关闭）")
	}
	return typ, ch, payload
}

// readFrameOrTimeout 读一条帧；超时/连接关闭时返回 typ=0。
func (c *client) readFrameOrTimeout() (typ byte, ch uint16, payload []byte) {
	typ, ch, payload, ok := c.readFrame0()
	if !ok {
		return 0, 0, nil
	}
	return typ, ch, payload
}

// readFrame0 读一条帧；出错时返回 typ=0（超时与连接关闭在这里等价：都没有数据可读）。
func (c *client) readFrame0() (byte, uint16, []byte, bool) {
	head := make([]byte, 7)
	if _, err := io.ReadFull(c.br, head); err != nil {
		return 0, 0, nil, false
	}
	size := binary.BigEndian.Uint32(head[3:7])
	payload := make([]byte, size)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return 0, 0, nil, false
	}
	end, err := c.br.ReadByte()
	if err != nil || end != 0xCE {
		return 0, 0, nil, false
	}
	return head[0], binary.BigEndian.Uint16(head[1:3]), payload, true
}

// expectMethod 读取下一条方法帧（跳过心跳与 Connection 级通知）。
func (c *client) expectMethod(ch uint16) (classID, methodID uint16, args []byte) {
	c.t.Helper()
	for {
		typ, gotCh, payload := c.readFrame()
		if typ == frameHeartbeat {
			continue
		}
		if typ != frameMethod {
			c.t.Fatalf("期望方法帧，实际类型 %d", typ)
		}
		if len(payload) < 4 {
			c.t.Fatalf("方法帧载荷过短: % x", payload)
		}
		gotClass := binary.BigEndian.Uint16(payload[0:2])
		gotMethod := binary.BigEndian.Uint16(payload[2:4])
		if gotCh == 0 && gotClass == classConnection && (gotMethod == 60 || gotMethod == 61) {
			// Connection.Blocked / Unblocked 是服务端随时可能下发的通知，客户端必须容忍
			continue
		}
		if gotCh == 0 && gotClass == classConnection && gotMethod == 50 {
			// 硬错误：把码与文案直接打出来，否则只能看到"缺少期望的帧"
			r := newArgReader(c.t, payload[4:])
			code := r.u16()
			text := r.shortstr()
			c.t.Fatalf("连接被服务端关闭：%d %s", code, text)
		}
		if gotCh != ch {
			c.t.Fatalf("期望 channel %d 上的帧，实际 channel %d（方法 %d/%d）", ch, gotCh, gotClass, gotMethod)
		}
		return gotClass, gotMethod, payload[4:]
	}
}

// ---------------------------------------------------------------------------
// 内容头属性
// ---------------------------------------------------------------------------

// contentHeader 是用例关心的内容头字段（其余字段解析后丢弃）。
type contentHeader struct {
	bodySize      uint64
	deliveryMode  uint8
	correlationID string
	replyTo       string
}

// parseContentHeader 解析内容头帧载荷（属性区按 14 个可选属性逐位读取）。
func parseContentHeader(t *testing.T, b []byte) contentHeader {
	t.Helper()
	r := newArgReader(t, b)
	if got := r.u16(); got != classBasic {
		t.Fatalf("内容头的 class-id 应为 %d，实际 %d", classBasic, got)
	}
	r.u16() // weight
	h := contentHeader{bodySize: r.u64()}
	flags := r.u16()
	// 位序：bit15 = content-type … bit2 = cluster-id
	if flags&0x8000 != 0 {
		r.shortstr() // content-type
	}
	if flags&0x4000 != 0 {
		r.shortstr() // content-encoding
	}
	if flags&0x2000 != 0 {
		r.skipTable() // headers
	}
	if flags&0x1000 != 0 {
		h.deliveryMode = r.octet()
	}
	if flags&0x0800 != 0 {
		r.octet() // priority
	}
	if flags&0x0400 != 0 {
		h.correlationID = r.shortstr()
	}
	if flags&0x0200 != 0 {
		h.replyTo = r.shortstr()
	}
	if flags&0x0100 != 0 {
		r.shortstr() // expiration
	}
	if flags&0x0080 != 0 {
		r.shortstr() // message-id
	}
	if flags&0x0040 != 0 {
		r.u64() // timestamp
	}
	if flags&0x0020 != 0 {
		r.shortstr() // type
	}
	if flags&0x0010 != 0 {
		r.shortstr() // user-id
	}
	if flags&0x0008 != 0 {
		r.shortstr() // app-id
	}
	if flags&0x0004 != 0 {
		r.shortstr() // cluster-id
	}
	if r.rem() != 0 {
		t.Fatalf("属性区有 %d 字节未被解析（属性位图与编码不一致）", r.rem())
	}
	return h
}

// ---------------------------------------------------------------------------
// 编码 / 解码小工具
// ---------------------------------------------------------------------------

func appendU16(b []byte, v uint16) []byte { return binary.BigEndian.AppendUint16(b, v) }
func appendU32(b []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(b, v) }
func appendU64(b []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(b, v) }

func appendShortStr(b []byte, s string) []byte {
	b = append(b, byte(len(s)))
	return append(b, s...)
}

func appendLongStr(b []byte, s []byte) []byte {
	b = appendU32(b, uint32(len(s)))
	return append(b, s...)
}

// appendTable 编码一个 field-table（entries 为已编码的键值项）。
func appendTable(b []byte, entries ...[]byte) []byte {
	var body []byte
	for _, e := range entries {
		body = append(body, e...)
	}
	return appendLongStr(b, body)
}

// entryInt8 构造一个 short-short-int 类型的表项（pika 发小整数用的就是它）。
func entryInt8(key string, v int8) []byte {
	e := appendShortStr(nil, key)
	e = append(e, 'b', byte(v))
	return e
}

// entryLongStr 构造一个 longstr 类型的表项（用于非法取值的负向用例）。
func entryLongStr(key, v string) []byte {
	e := appendShortStr(nil, key)
	e = append(e, 'S')
	return appendLongStr(e, []byte(v))
}

// argReader 按 AMQP 基本类型顺序读取参数区。
type argReader struct {
	t *testing.T
	b []byte
	i int
}

func newArgReader(t *testing.T, b []byte) *argReader {
	t.Helper()
	return &argReader{t: t, b: b}
}

func (r *argReader) rem() int { return len(r.b) - r.i }

func (r *argReader) take(n int) []byte {
	r.t.Helper()
	if r.rem() < n {
		r.t.Fatalf("参数区越界：需要 %d 字节，只剩 %d", n, r.rem())
	}
	out := r.b[r.i : r.i+n]
	r.i += n
	return out
}

func (r *argReader) octet() byte { return r.take(1)[0] }

func (r *argReader) u16() uint16 { return binary.BigEndian.Uint16(r.take(2)) }

func (r *argReader) u32() uint32 { return binary.BigEndian.Uint32(r.take(4)) }

func (r *argReader) u64() uint64 { return binary.BigEndian.Uint64(r.take(8)) }

func (r *argReader) shortstr() string { return string(r.take(int(r.octet()))) }

func (r *argReader) longstr() []byte { return r.take(int(r.u32())) }

// skipTable 跳过一个 field-table（用例不解析表内容，只需要跨过它）。
func (r *argReader) skipTable() {
	r.t.Helper()
	r.take(int(r.u32()))
}
