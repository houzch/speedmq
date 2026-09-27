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

	mu        sync.Mutex
	lastRead  time.Time
	lastWrite time.Time

	// channels 记录已打开的 channel。
	channels map[uint16]struct{}

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
		channels: map[uint16]struct{}{},
		closed:   make(chan struct{}),
		lastRead: time.Now(),
	}
}

// run 执行完整连接生命周期：协议头 → Start → Tune → Open → 主循环。
func (c *connection) run(ctx context.Context) error {
	defer c.shutdown()

	// 认证完成前使用初始 frame-max（对齐 RabbitMQ 4.1+：8192）
	c.frameMax = codec.FrameMaxAuthInitial
	c.fr = codec.NewFrameReader(c.r, c.frameMax)
	c.fw = codec.NewFrameWriter(c.w)

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

	c.log.Info("连接已建立",
		"user", c.identity.User,
		"vhost", c.identity.VHost,
		"channel_max", c.channelMax,
		"frame_max", c.frameMax,
		"heartbeat", c.heartbeat.String())

	hbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if c.heartbeat > 0 {
		go c.heartbeatLoop(hbCtx)
	}

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
		// 对齐 RabbitMQ：vhost 不存在是硬错误 402 INVALID_PATH，直接关闭连接
		return c.failConnection(spec.InvalidPath,
			fmt.Sprintf("NOT_ALLOWED - vhost %s not found", vhost), m.ClassID, m.MethodID)
	}
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
		case codec.FrameHeader, codec.FrameBody:
			// M1 尚未实现 basic.publish，收到内容帧属于预期外帧，返回 505 UNEXPECTED_FRAME
			return c.failConnection(spec.UnexpectedFrame,
				"M1 尚未实现内容帧（当前仅支持连接建立与 Channel 开关）", 0, 0)
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

	switch m.ClassID {
	case spec.ClassConnection:
		if f.Channel != 0 {
			// Connection 级方法必须走 channel 0；否则关闭整条连接
			return c.failConnection(spec.CommandInvalid,
				fmt.Sprintf("%s 必须使用 channel 0", m.Name()), m.ClassID, m.MethodID)
		}
		return c.handleConnectionMethod(m)

	case spec.ClassChannel:
		if f.Channel == 0 {
			return c.failConnection(spec.CommandInvalid,
				fmt.Sprintf("%s 不能使用 channel 0", m.Name()), m.ClassID, m.MethodID)
		}
		if m.MethodID != spec.MethodChannelOpen {
			if _, open := c.channels[f.Channel]; !open {
				// 在未打开的 channel 上发方法 → 软错误，只关该 channel
				return c.closeChannel(f.Channel, spec.ChannelError,
					fmt.Sprintf("channel %d 未打开", f.Channel), m.ClassID, m.MethodID)
			}
		}
		return c.handleChannelMethod(f.Channel, m)

	default:
		// M2 起补齐 Exchange / Queue / Basic / Confirm / Tx
		return c.failConnection(spec.NotImplemented,
			fmt.Sprintf("M1 尚未实现 class %d method %d（%s）", m.ClassID, m.MethodID, m.Name()),
			m.ClassID, m.MethodID)
	}
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

func (c *connection) handleChannelMethod(ch uint16, m spec.Method) error {
	switch m.MethodID {
	case spec.MethodChannelOpen:
		if err := spec.DecodeChannelOpen(m.Args); err != nil {
			return c.closeChannel(ch, spec.SyntaxError, "Channel.Open 解析失败: "+err.Error(),
				m.ClassID, m.MethodID)
		}
		if _, dup := c.channels[ch]; dup {
			return c.closeChannel(ch, spec.ChannelError,
				fmt.Sprintf("channel %d 已打开", ch), m.ClassID, m.MethodID)
		}
		if uint16(len(c.channels)) >= c.channelMax {
			return c.closeChannel(ch, spec.ChannelError,
				fmt.Sprintf("channel 数量已达到 channel-max=%d", c.channelMax), m.ClassID, m.MethodID)
		}
		c.channels[ch] = struct{}{}
		return c.sendMethod(ch, spec.ClassChannel, spec.MethodChannelOpenOk, spec.EncodeChannelOpenOk())

	case spec.MethodChannelClose:
		reply, err := spec.DecodeChannelClose(m.Args)
		if err != nil {
			return c.failConnection(spec.SyntaxError, "Channel.Close 解析失败: "+err.Error(),
				m.ClassID, m.MethodID)
		}
		delete(c.channels, ch)
		c.log.Debug("channel 已由客户端关闭", "channel", ch, "code", reply.Code, "text", reply.Text)
		return c.sendMethod(ch, spec.ClassChannel, spec.MethodChannelCloseOk, spec.EncodeChannelCloseOk())

	case spec.MethodChannelCloseOk:
		// 我方发起 Channel.Close 后的确认，无需再回应
		return nil

	case spec.MethodChannelFlow:
		active, err := spec.DecodeChannelFlow(m.Args)
		if err != nil {
			return c.closeChannel(ch, spec.SyntaxError, "Channel.Flow 解析失败: "+err.Error(),
				m.ClassID, m.MethodID)
		}
		// M1 不做服务端限流，但必须回 Flow-Ok，否则客户端会一直等待
		return c.sendMethod(ch, spec.ClassChannel, spec.MethodChannelFlowOk, spec.EncodeChannelFlowOk(active))

	default:
		return c.closeChannel(ch, spec.NotImplemented,
			fmt.Sprintf("M1 尚未实现 %s", m.Name()), m.ClassID, m.MethodID)
	}
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

// write 串行化写出并立即 Flush。
//
// 握手与错误路径都必须立即落到连接上（否则客户端会卡住等响应），
// 因此这里不做批量攒帧；M2 起再对高频路径做写合并优化。
func (c *connection) write(f codec.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.fw.Write(f); err != nil {
		return err
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

// closeChannel 发送 Channel.Close（软错误）。连接与其余 channel 不受影响。
func (c *connection) closeChannel(ch uint16, code uint16, text string, classID, methodID uint16) error {
	reply := spec.Reply{Code: code, Text: text, ClassID: classID, MethodID: methodID}
	args, err := spec.EncodeChannelClose(reply)
	if err != nil {
		return err
	}
	delete(c.channels, ch)
	c.log.Warn("关闭 channel", "channel", ch, "code", code, "text", text)
	return c.sendMethod(ch, spec.ClassChannel, spec.MethodChannelClose, args)
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
