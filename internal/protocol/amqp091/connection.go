package amqp091

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/internal/protocol/spec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// errClientClosed 表示客户端主动关闭连接，属于正常结束。
var errClientClosed = errors.New("客户端主动关闭连接")

const (
	readBufferSize  = 4096
	writeBufferSize = 4096
	// closeWaitTimeout 是发出 Connection.Close 后等待对端 Close-Ok 的最长时间。
	closeWaitTimeout = 2 * time.Second
	// handshakeTimeout 是握手阶段（读到 Connection.Open 之前）的读超时。
	handshakeTimeout = 10 * time.Second
)

// connection 是一条 AMQP 0-9-1 连接的状态机。
//
// 顺序性保证：帧由单一读循环顺序处理，因此同一 Channel 上的消息天然严格有序
// 写侧由 mu 串行化，避免主循环与心跳协程交错写出半个帧。
type connection struct {
	log  *slog.Logger
	conn net.Conn
	core plugin.Core

	r  *bufio.Reader
	w  *bufio.Writer
	fr *codec.FrameReader
	fw *codec.FrameWriter

	// 协商结果
	channelMax uint16
	frameMax   uint32
	heartbeat  time.Duration

	identity plugin.Identity

	// 握手结果：只写一次、之后只读（管理面的连接快照会读它们）。
	clientProps   map[string]any
	authMechanism string

	mu        sync.Mutex
	lastRead  time.Time
	lastWrite time.Time

	// channels 记录已打开的 channel。
	//
	// 读循环是唯一的写者，但**管理面会在另一个协程读取通道表**（连接/通道列表），
	// 因此这里必须有一把独立的锁：它与 mu（写帧串行化）分开，避免"读通道表"与
	// "写帧"互相阻塞，也避免在持锁状态下写帧造成自锁。
	chMu     sync.RWMutex
	channels map[uint16]*channel
	// session 是打开 vhost 后取得的协议无关操作面。
	// 它在握手阶段（任何消费者出现之前）写入，之后只读，因此无需加锁。
	session plugin.Session

	closed    chan struct{}
	closeOnce sync.Once
}

func newConnection(log *slog.Logger, conn net.Conn, core plugin.Core) *connection {
	return &connection{
		log:      log,
		conn:     conn,
		core:     core,
		r:        bufio.NewReaderSize(conn, readBufferSize),
		w:        bufio.NewWriterSize(conn, writeBufferSize),
		channels: map[uint16]*channel{},
		closed:   make(chan struct{}),
		lastRead: time.Now(),
	}
}

// sessionOrNil 返回当前会话（未打开 vhost 时为 nil）。
func (c *connection) sessionOrNil() plugin.Session { return c.session }

// run 执行完整连接生命周期：协议头 → Start → Tune → Open → 主循环。
func (c *connection) run(ctx context.Context) error {
	defer c.shutdown()
	defer c.teardown()

	// 认证完成前使用初始 frame-max（对齐 RabbitMQ 4.1+：8192）
	c.frameMax = codec.FrameMaxAuthInitial
	c.fr = codec.NewFrameReader(c.r, c.frameMax)
	c.fw = codec.NewFrameWriter(c.w)

	// 握手阶段设读超时：否则客户端发完协议头就停住不发，会永久占住连接与 goroutine
	// （slowloris）。握手完成即清除，之后由心跳与主循环兜底。
	if err := c.conn.SetReadDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return err
	}

	if err := c.handshakeProtocolHeader(); err != nil {
		return err
	}
	if err := c.handshakeStart(ctx); err != nil {
		return err
	}
	if err := c.handshakeTune(); err != nil {
		return err
	}
	if err := c.handshakeOpen(); err != nil {
		return err
	}

	// 握手完成：清除握手读超时（0 值表示不再自动超时）。
	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}

	c.log.Info("连接已建立",
		"user", c.identity.User,
		"vhost", c.identity.VHost,
		"channel_max", c.channelMax,
		"frame_max", c.frameMax,
		"heartbeat", c.heartbeat.String())

	// 把"连接/通道快照"与"强制断开"两个回调交给内核：管理面据此列出连接、
	// 以及在运维点"强制关闭"时优雅断开（reply-code 320）。
	c.core.SetConnectionProbe(c.snapshot)
	c.core.SetDisconnectFunc(c.forceClose)

	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if c.heartbeat > 0 {
		go c.heartbeatLoop(hbCtx)
	}
	// 资源水位通知：内存/磁盘水位变化时向客户端下发 Connection.Blocked / Unblocked
	go c.notifyLoop(hbCtx)

	err := c.loop(hbCtx)
	if errors.Is(err, errClientClosed) {
		c.log.Info("连接已由客户端正常关闭", "user", c.identity.User)
		return nil
	}
	return err
}

func (c *connection) shutdown() {
	c.closeOnce.Do(func() { close(c.closed) })
	_ = c.conn.Close()
}

// ---------------------------------------------------------------------------
// 握手
// ---------------------------------------------------------------------------

// handshakeProtocolHeader 校验协议头；版本不受支持时按规范回写本服务端支持的协议头。
func (c *connection) handshakeProtocolHeader() error {
	hdr := make([]byte, len(protocolHeader))
	if _, err := io.ReadFull(c.r, hdr); err != nil {
		return fmt.Errorf("读取协议头失败: %w", err)
	}
	if bytes.Equal(hdr, protocolHeader) {
		return nil
	}
	c.log.Warn("客户端协议头不受支持，已回写支持的版本", "header", fmt.Sprintf("% x", hdr))
	if _, err := c.conn.Write(protocolHeader); err != nil {
		return fmt.Errorf("回写协议头失败: %w", err)
	}
	return fmt.Errorf("不支持的协议头: % x", hdr)
}

// handshakeStart 发送 Connection.Start 并完成 SASL 认证。
func (c *connection) handshakeStart(ctx context.Context) error {
	args, err := spec.EncodeConnectionStart(0, 9, c.core.ServerProperties(),
		strings.Join(c.core.Mechanisms(), " "), "en_US")
	if err != nil {
		return fmt.Errorf("构造 Connection.Start 失败: %w", err)
	}
	if err := c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionStart, args); err != nil {
		return err
	}

	m, err := c.readMethod()
	if err != nil {
		return fmt.Errorf("等待 Connection.Start-Ok 失败: %w", err)
	}
	if m.ClassID != spec.ClassConnection || m.MethodID != spec.MethodConnectionStartOk {
		return c.failConnection(spec.CommandInvalid,
			fmt.Sprintf("握手阶段期望 Connection.Start-Ok，收到 %s", m.Name()), m.ClassID, m.MethodID)
	}
	ok, err := spec.DecodeConnectionStartOk(m.Args)
	if err != nil {
		return c.failConnection(spec.SyntaxError, "Connection.Start-Ok 解析失败: "+err.Error(),
			m.ClassID, m.MethodID)
	}

	ident, err := c.core.Authenticate(ctx, ok.Mechanism, ok.Response, c.conn.RemoteAddr())
	if err != nil {
		code := spec.AccessRefused
		text := err.Error()
		var authErr *plugin.AuthError
		if errors.As(err, &authErr) {
			text = authErr.Text
			if authErr.Kind == plugin.AuthFailureMechanismUnsupported {
				code = spec.NotAllowed
			}
		}
		c.log.Warn("认证失败", "mechanism", ok.Mechanism, "remote", c.conn.RemoteAddr().String())
		// authentication_failure_close：用 Connection.Close 明确告知失败原因，而不是直接断开
		return c.failConnection(code, text, spec.ClassConnection, spec.MethodConnectionStartOk)
	}
	c.identity = ident
	// 记录客户端属性与机制名：管理面的连接列表要展示它们（对齐 RabbitMQ 的 client_properties）
	c.clientProps = map[string]any(ok.ClientProperties)
	c.authMechanism = ok.Mechanism
	return nil
}

// handshakeTune 协商 channel-max / frame-max / heartbeat。
func (c *connection) handshakeTune() error {
	args := spec.EncodeConnectionTune(codec.ChannelMaxDefault, codec.FrameMaxDefault, codec.HeartbeatDefault)
	if err := c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionTune, args); err != nil {
		return err
	}

	m, err := c.readMethod()
	if err != nil {
		return fmt.Errorf("等待 Connection.Tune-Ok 失败: %w", err)
	}
	if m.ClassID != spec.ClassConnection || m.MethodID != spec.MethodConnectionTuneOk {
		return c.failConnection(spec.CommandInvalid,
			fmt.Sprintf("握手阶段期望 Connection.Tune-Ok，收到 %s", m.Name()), m.ClassID, m.MethodID)
	}
	ok, err := spec.DecodeConnectionTuneOk(m.Args)
	if err != nil {
		return c.failConnection(spec.SyntaxError, "Connection.Tune-Ok 解析失败: "+err.Error(),
			m.ClassID, m.MethodID)
	}

	c.channelMax = negotiateLimit16(ok.ChannelMax, codec.ChannelMaxDefault)
	c.frameMax = negotiateLimit32(ok.FrameMax, codec.FrameMaxDefault)
	if c.frameMax < codec.FrameMaxNegotiatedMin {
		// 对齐 RabbitMQ 4.x：低于下限的 frame-max 在 Tune 阶段就被拒绝（见 codec.FrameMaxNegotiatedMin）。
		// 放在这里而不是"默默按最小值放大"，是因为客户端的收发缓冲是按它自己请求的值分配的：
		// 服务端单方面放大，客户端仍按小值发帧 —— 表面能用，实际把不一致藏了起来。
		return c.failConnection(spec.NotAllowed, fmt.Sprintf(
			"NOT_ALLOWED - negotiated frame_max = %d is lower than the minimum allowed value (%d)",
			c.frameMax, codec.FrameMaxNegotiatedMin), m.ClassID, m.MethodID)
	}
	c.heartbeat = negotiateHeartbeat(ok.Heartbeat, codec.HeartbeatDefault)
	c.fr.SetMax(c.frameMax)
	return nil
}

// handshakeOpen 打开 vhost。
func (c *connection) handshakeOpen() error {
	m, err := c.readMethod()
	if err != nil {
		return fmt.Errorf("等待 Connection.Open 失败: %w", err)
	}
	if m.ClassID != spec.ClassConnection || m.MethodID != spec.MethodConnectionOpen {
		return c.failConnection(spec.CommandInvalid,
			fmt.Sprintf("握手阶段期望 Connection.Open，收到 %s", m.Name()), m.ClassID, m.MethodID)
	}
	vhost, err := spec.DecodeConnectionOpen(m.Args)
	if err != nil {
		return c.failConnection(spec.SyntaxError, "Connection.Open 解析失败: "+err.Error(),
			m.ClassID, m.MethodID)
	}
	if !c.core.VHostExists(vhost) {
		// 对齐 RabbitMQ：vhost 不存在是硬错误 530 NOT_ALLOWED（reply-text 同样以 NOT_ALLOWED 开头）。
		return c.failConnection(spec.NotAllowed,
			fmt.Sprintf("NOT_ALLOWED - vhost %s not found", vhost), m.ClassID, m.MethodID)
	}
	sess, err := c.core.Session(vhost)
	if err != nil {
		code, text := replyFor(err)
		return c.failConnection(code, text, m.ClassID, m.MethodID)
	}
	c.session = sess
	c.identity.VHost = vhost
	return c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionOpenOk, spec.EncodeConnectionOpenOk())
}

// ---------------------------------------------------------------------------
// 主循环
// ---------------------------------------------------------------------------

func (c *connection) loop(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := c.fr.Read()
		if err != nil {
			select {
			case <-c.closed:
				return nil
			default:
			}
			// 帧超过协商的 frame-max、或帧结构非法：按 RabbitMQ 回 501 FRAME_ERROR，
			// 让客户端拿到明确的关闭原因，而不是只看到连接被重置。
			if errors.Is(err, codec.ErrFrameTooLarge) || errors.Is(err, codec.ErrFrameEnd) {
				return c.failConnection(spec.FrameError, err.Error(), 0, 0)
			}
			return err
		}
		c.touchRead()

		switch f.Type {
		case codec.FrameHeartbeat:
			// 心跳仅用于保活与超时判定，无业务语义
			continue
		case codec.FrameMethod:
			if err := c.handleMethod(f); err != nil {
				return err
			}
		case codec.FrameHeader:
			if err := c.handleContentHeader(f); err != nil {
				return err
			}
		case codec.FrameBody:
			if err := c.handleContentBody(f); err != nil {
				return err
			}
		default:
			return c.failConnection(spec.FrameError,
				fmt.Sprintf("未知帧类型 %d", f.Type), 0, 0)
		}
	}
}

func (c *connection) handleMethod(f codec.Frame) error {
	m, err := spec.DecodeMethod(f.Payload)
	if err != nil {
		return c.failConnection(spec.SyntaxError, "方法帧解析失败: "+err.Error(), 0, 0)
	}

	// Connection 级方法必须走 channel 0
	if m.ClassID == spec.ClassConnection {
		if f.Channel != 0 {
			return c.failConnection(spec.CommandInvalid,
				fmt.Sprintf("%s 必须使用 channel 0", m.Name()), m.ClassID, m.MethodID)
		}
		return c.handleConnectionMethod(m)
	}
	if f.Channel == 0 {
		return c.failConnection(spec.CommandInvalid,
			fmt.Sprintf("%s 不能使用 channel 0", m.Name()), m.ClassID, m.MethodID)
	}

	if ch, ok := c.getChannel(f.Channel); ok {
		return ch.handle(m)
	}

	switch {
	case m.ClassID == spec.ClassChannel && m.MethodID == spec.MethodChannelOpen:
		return c.openChannel(f.Channel, m)
	case m.ClassID == spec.ClassChannel && m.MethodID == spec.MethodChannelCloseOk:
		// 我方发起的 Channel.Close 得到确认；该 channel 在发出关闭时已从集合移除
		return nil
	case m.ClassID == spec.ClassChannel && m.MethodID == spec.MethodChannelClose:
		// 对已关闭的 channel 再发 Channel.Close：宽容地回 Close-Ok。
		// 客户端清理阶段常会这么做，报错只会让它拿到一个无意义的异常。
		return c.sendMethod(f.Channel, spec.ClassChannel, spec.MethodChannelCloseOk,
			spec.EncodeChannelCloseOk())
	default:
		// 在未打开的 channel 上发方法 → 软错误，只关该 channel
		return c.closeChannelByID(f.Channel, spec.ChannelError,
			fmt.Sprintf("channel %d 未打开", f.Channel), m.ClassID, m.MethodID)
	}
}

// ---------------------------------------------------------------------------
// 通道表访问（并发安全：读循环写、管理面读）
// ---------------------------------------------------------------------------

func (c *connection) getChannel(id uint16) (*channel, bool) {
	c.chMu.RLock()
	defer c.chMu.RUnlock()
	ch, ok := c.channels[id]
	return ch, ok
}

func (c *connection) channelCount() int {
	c.chMu.RLock()
	defer c.chMu.RUnlock()
	return len(c.channels)
}

func (c *connection) addChannel(id uint16, ch *channel) {
	c.chMu.Lock()
	c.channels[id] = ch
	c.chMu.Unlock()
}

// removeChannel 取出并移除通道。
func (c *connection) removeChannel(id uint16) (*channel, bool) {
	c.chMu.Lock()
	defer c.chMu.Unlock()
	ch, ok := c.channels[id]
	if ok {
		delete(c.channels, id)
	}
	return ch, ok
}

// drainChannels 取出全部通道并清空（连接结束时调用）。
func (c *connection) drainChannels() []*channel {
	c.chMu.Lock()
	defer c.chMu.Unlock()
	out := make([]*channel, 0, len(c.channels))
	for id, ch := range c.channels {
		out = append(out, ch)
		delete(c.channels, id)
	}
	return out
}

// snapshot 返回本连接的实时快照（管理面按需拉取）。
func (c *connection) snapshot() plugin.ConnectionInfo {
	info := plugin.ConnectionInfo{
		Protocol:         "AMQP 0-9-1",
		ClientProperties: c.clientProps,
		AuthMechanism:    c.authMechanism,
		FrameMax:         c.frameMax,
		ChannelMax:       c.channelMax,
	}
	if c.heartbeat > 0 {
		info.HeartbeatSeconds = uint16(c.heartbeat / time.Second)
	}

	c.chMu.RLock()
	type chInfo struct {
		num uint16
		ch  *channel
	}
	list := make([]chInfo, 0, len(c.channels))
	for id, ch := range c.channels {
		list = append(list, chInfo{num: id, ch: ch})
	}
	c.chMu.RUnlock()

	// 通道号排序：管理 UI 与 CLI 的输出顺序必须稳定，否则每次刷新都在跳
	sort.Slice(list, func(i, j int) bool { return list[i].num < list[j].num })
	for _, item := range list {
		ch := item.ch
		ch.mu.Lock()
		info.Channels = append(info.Channels, plugin.ChannelInfo{
			Number:        item.num,
			ConsumerCount: len(ch.consumers),
			PrefetchCount: ch.prefetch,
			Confirm:       ch.confirm,
			Unacked:       len(ch.unacked),
		})
		ch.mu.Unlock()
	}
	return info
}

// forceClose 响应管理面的强制关闭：先发 Connection.Close（320 CONNECTION_FORCED），
// 再关闭底层连接唤醒读循环。
//
// 这里**不等待**对端回 Close-Ok：该回调由管理面的 HTTP 协程调用，
// 而读循环也在同一连接上读，两个协程同时读会破坏帧解析。发完就断开是安全且足够的 ——
// 客户端已经能从 320 错误码分辨"被运维关闭"与"网络抖动"。
func (c *connection) forceClose(reason string) {
	args, err := spec.EncodeConnectionClose(spec.Reply{
		Code: spec.ConnectionForced,
		Text: reason,
	})
	if err != nil {
		c.log.Warn("构造 Connection.Close 失败，直接断开连接", "err", err)
	} else if err := c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionClose, args); err != nil {
		c.log.Debug("发送 Connection.Close 失败（对端可能已断开）", "err", err)
	}
	c.shutdown()
}

func (c *connection) handleConnectionMethod(m spec.Method) error {
	switch m.MethodID {
	case spec.MethodConnectionClose:
		reply, err := spec.DecodeConnectionClose(m.Args)
		if err != nil {
			return c.failConnection(spec.SyntaxError, "Connection.Close 解析失败: "+err.Error(),
				m.ClassID, m.MethodID)
		}
		c.log.Info("客户端请求关闭连接", "code", reply.Code, "text", reply.Text)
		if err := c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionCloseOk,
			spec.EncodeConnectionCloseOk()); err != nil {
			return err
		}
		return errClientClosed

	case spec.MethodConnectionCloseOk:
		// 我方发起关闭后对端回的确认
		return errClientClosed

	case spec.MethodConnectionBlocked, spec.MethodConnectionUnblocked:
		return c.failConnection(spec.CommandInvalid,
			fmt.Sprintf("%s 只能由服务端发送", m.Name()), m.ClassID, m.MethodID)

	default:
		return c.failConnection(spec.NotImplemented,
			fmt.Sprintf("M1 尚未实现 %s", m.Name()), m.ClassID, m.MethodID)
	}
}

// openChannel 处理 Channel.Open：创建通道状态并回复 Open-Ok。
func (c *connection) openChannel(id uint16, m spec.Method) error {
	if err := spec.DecodeChannelOpen(m.Args); err != nil {
		return c.closeChannelByID(id, spec.SyntaxError, "Channel.Open 解析失败: "+err.Error(),
			m.ClassID, m.MethodID)
	}
	if c.channelCount() >= int(c.channelMax) {
		return c.closeChannelByID(id, spec.ChannelError,
			fmt.Sprintf("channel 数量已达到 channel-max=%d", c.channelMax), m.ClassID, m.MethodID)
	}
	c.addChannel(id, newChannel(id, c))
	c.log.Debug("channel 已打开", "channel", id)
	return c.sendMethod(id, spec.ClassChannel, spec.MethodChannelOpenOk, spec.EncodeChannelOpenOk())
}

// closeChannelByID 以软错误关闭一个 channel：释放它的消费者、把未确认消息重新入队，
// 并通知客户端。连接与其余 channel 不受影响。
func (c *connection) closeChannelByID(id uint16, code uint16, text string, classID, methodID uint16) error {
	if ch, ok := c.removeChannel(id); ok {
		ch.release()
	}
	args, err := spec.EncodeChannelClose(spec.Reply{Code: code, Text: text, ClassID: classID, MethodID: methodID})
	if err != nil {
		return err
	}
	c.log.Warn("关闭 channel", "channel", id, "code", code, "text", text)
	return c.sendMethod(id, spec.ClassChannel, spec.MethodChannelClose, args)
}

// teardown 释放连接级资源。必须在读循环退出后调用，且在 run 的 defer 中保证执行。
func (c *connection) teardown() {
	for _, ch := range c.drainChannels() {
		ch.release()
	}
	if c.session != nil {
		// 取消本连接的全部消费者、删除它的独占队列
		c.session.Close()
	}
	// 注销连接级通知订阅，避免连接结束后仍被内核引用
	c.core.Close()
}

// handleContentHeader 处理内容头帧：把属性挂到该 channel 正在组装的内容上。
func (c *connection) handleContentHeader(f codec.Frame) error {
	ch, ok := c.getChannel(f.Channel)
	if !ok {
		return c.closeChannelByID(f.Channel, spec.ChannelError,
			fmt.Sprintf("channel %d 未打开", f.Channel), spec.ClassBasic, spec.MethodBasicPublish)
	}
	header, err := spec.DecodeContentHeader(f.Payload)
	if err != nil {
		return ch.fail(plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 内容头解析失败: %v", err), spec.ClassBasic, 0)
	}
	if err := ch.setContentHeader(header); err != nil {
		return ch.fail(err, spec.ClassBasic, spec.MethodBasicPublish)
	}
	return ch.maybeFinishContent()
}

// handleContentBody 处理内容体帧。
func (c *connection) handleContentBody(f codec.Frame) error {
	ch, ok := c.getChannel(f.Channel)
	if !ok {
		return c.closeChannelByID(f.Channel, spec.ChannelError,
			fmt.Sprintf("channel %d 未打开", f.Channel), spec.ClassBasic, 0)
	}
	complete, err := ch.appendBody(f.Payload)
	if err != nil {
		return ch.fail(err, spec.ClassBasic, 0)
	}
	if complete {
		return ch.maybeFinishContent()
	}
	return nil
}

// ---------------------------------------------------------------------------
// 读 / 写 / 错误
// ---------------------------------------------------------------------------

// readMethod 读取下一个方法帧（跳过心跳）。
func (c *connection) readMethod() (spec.Method, error) {
	for {
		f, err := c.fr.Read()
		if err != nil {
			return spec.Method{}, err
		}
		c.touchRead()
		switch f.Type {
		case codec.FrameHeartbeat:
			continue
		case codec.FrameMethod:
			return spec.DecodeMethod(f.Payload)
		default:
			return spec.Method{}, fmt.Errorf("握手段收到非方法帧: type=%d", f.Type)
		}
	}
}

func (c *connection) sendMethod(ch uint16, classID, methodID uint16, args []byte) error {
	return c.write(codec.Frame{
		Type:    codec.FrameMethod,
		Channel: ch,
		Payload: spec.EncodeMethod(classID, methodID, args),
	})
}

// write 写出一条帧并立即 Flush。
func (c *connection) write(f codec.Frame) error { return c.writeFrames(f) }

// writeFrames 原子地写出一组帧并立即 Flush。
//
// 一组帧之间不允许插入其他帧：例如 Basic.Deliver + 内容头 + 内容体必须连续到达，
// 否则客户端会把它们当成两条消息的内容。因此整组必须在同一把锁内写完。
func (c *connection) writeFrames(frames ...codec.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range frames {
		if err := c.fw.Write(f); err != nil {
			return err
		}
	}
	if err := c.fw.Flush(); err != nil {
		return err
	}
	c.lastWrite = time.Now()
	return nil
}

// failConnection 发送 Connection.Close（硬错误）并返回该错误以结束连接。
func (c *connection) failConnection(code uint16, text string, classID, methodID uint16) error {
	reply := spec.Reply{Code: code, Text: text, ClassID: classID, MethodID: methodID}
	args, err := spec.EncodeConnectionClose(reply)
	if err != nil {
		return err
	}
	c.log.Warn("关闭连接", "code", code, "text", text, "class", classID, "method", methodID)
	if err := c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionClose, args); err != nil {
		return err
	}
	// 给对端一个回复 Close-Ok 的机会；超时或直接断开都不影响后续清理
	_ = c.conn.SetReadDeadline(time.Now().Add(closeWaitTimeout))
	_, _ = c.readMethod()
	_ = c.conn.SetReadDeadline(time.Time{})
	return reply
}

// dropChannel 从已打开集合中移除通道并释放其资源（客户端主动关闭时）。
func (c *connection) dropChannel(id uint16) {
	if ch, ok := c.removeChannel(id); ok {
		ch.release()
	}
}

// ---------------------------------------------------------------------------
// 资源水位通知
// ---------------------------------------------------------------------------

// notifyLoop 把内核的资源水位事件转成 Connection.Blocked / Unblocked 帧。
//
// 这两个方法只有服务端会发，且必须走 channel 0 —— 客户端据此暂停发布或告警。
func (c *connection) notifyLoop(ctx context.Context) {
	notify := c.core.Notifications()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case n, ok := <-notify:
			if !ok {
				return
			}
			if err := c.sendFlowNotification(n); err != nil {
				c.log.Debug("下发资源水位通知失败", "blocked", n.Blocked, "err", err)
				return
			}
		}
	}
}

func (c *connection) sendFlowNotification(n plugin.Notification) error {
	if !n.Blocked {
		return c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionUnblocked,
			spec.EncodeConnectionUnblocked())
	}
	// reason 是 shortstr：超长时截断，否则整个通知会因编码失败而发不出去
	reason := n.Reason
	if len(reason) > 255 {
		reason = reason[:255]
	}
	args, err := spec.EncodeConnectionBlocked(reason)
	if err != nil {
		return err
	}
	c.log.Warn("已向客户端下发 Connection.Blocked", "reason", reason)
	return c.sendMethod(0, spec.ClassConnection, spec.MethodConnectionBlocked, args)
}

// ---------------------------------------------------------------------------
// 心跳
// ---------------------------------------------------------------------------

// heartbeatLoop 周期性发送心跳，并在 2 倍心跳间隔无入站流量时判定超时。
//
// 超时判定用 2 倍间隔是刻意的宽松（对齐 RabbitMQ）：网络抖动不应误杀正常连接。
func (c *connection) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(c.heartbeat)
	defer ticker.Stop()

	var timeout time.Duration
	if c.heartbeat > 0 {
		timeout = 2 * c.heartbeat
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case <-ticker.C:
			if time.Since(c.lastReadTime()) > timeout {
				c.log.Warn("心跳超时，强制关闭连接",
					"heartbeat", c.heartbeat.String(), "timeout", timeout.String())
				c.shutdown() // 关闭连接以唤醒阻塞中的读循环
				return
			}
			// 仅在本周期内没有出站流量时发送心跳，避免与业务流量重复占带宽
			if time.Since(c.lastWriteTime()) < c.heartbeat {
				continue
			}
			if err := c.write(codec.Frame{Type: codec.FrameHeartbeat}); err != nil {
				return
			}
		}
	}
}

func (c *connection) touchRead() {
	c.mu.Lock()
	c.lastRead = time.Now()
	c.mu.Unlock()
}

func (c *connection) lastReadTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRead
}

func (c *connection) lastWriteTime() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastWrite
}

// ---------------------------------------------------------------------------
// 协商
// ---------------------------------------------------------------------------

// negotiateLimit16 按 AMQP 规则取协商值：客户端返回 0 表示"沿用服务端值"，
// 大于服务端提议值时以服务端为准（客户端不得放大上限）。
func negotiateLimit16(client, server uint16) uint16 {
	if client == 0 || client > server {
		return server
	}
	return client
}

func negotiateLimit32(client, server uint32) uint32 {
	if client == 0 || client > server {
		return server
	}
	return client
}

// negotiateHeartbeat 返回协商后的心跳间隔；0 表示关闭心跳。
//
// 客户端返回 0 时必须尊重（有客户端靠它关闭心跳），不能强行开启。
func negotiateHeartbeat(client, server uint16) time.Duration {
	if client == 0 || server == 0 {
		return 0
	}
	if client > server {
		client = server
	}
	return time.Duration(client) * time.Second
}
