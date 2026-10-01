package sidecar

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// 内核侧（也可以是任何想驱动 sidecar 的进程）通过 Client 与插件进程通信。

// 默认超时。心跳是"发现插件进程死掉"的主要手段：它的周期必须远小于运维的容忍窗口，
// 又要远大于一次正常方法调用的耗时，避免把慢当成死。
const (
	DefaultHandshakeTimeout = 5 * time.Second
	DefaultHeartbeat        = 2 * time.Second
	DefaultHeartbeatTimeout = 8 * time.Second
	// DefaultOpenTimeout 是"请求插件为一条客户端连接建流"的超时。
	DefaultOpenTimeout = 5 * time.Second
)

// ErrClosed 表示连接已关闭（插件进程崩溃、心跳超时或内核主动关闭）。
var ErrClosed = errors.New("sidecar: 连接已关闭")

// ErrHeartbeatTimeout 表示心跳超时：对端进程大概率已经不在了。
var ErrHeartbeatTimeout = errors.New("sidecar: 心跳超时")

// ClientOptions 是 Client 的可选参数。
type ClientOptions struct {
	// HandshakeTimeout 是握手超时，零值用 DefaultHandshakeTimeout。
	HandshakeTimeout time.Duration
	// Heartbeat 是心跳间隔，零值用 DefaultHeartbeat；负值表示关闭心跳。
	Heartbeat time.Duration
	// HeartbeatTimeout 是心跳应答超时，零值用 DefaultHeartbeatTimeout。
	HeartbeatTimeout time.Duration
	// Dial 可注入自定义拨号（测试用）；为 nil 时按地址方案选择 tcp/unix。
	Dial func(ctx context.Context, address string) (net.Conn, error)
	// Logger 记录连接级事件（可空）。
	Logger Logger
}

// Logger 是 sidecar 的最小日志接口（避免本包依赖内核的日志类型）。
type Logger interface {
	// Info 记录一条信息日志。
	Info(msg string, args ...any)
	// Warn 记录一条告警日志。
	Warn(msg string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Info(string, ...any) {}
func (nopLogger) Warn(string, ...any) {}

// Client 是一条到插件进程的连接。
type Client struct {
	conn net.Conn
	fw   *frameWriter
	br   *bufio.Reader
	opts ClientOptions
	log  Logger

	mu         sync.Mutex
	streams    map[uint32]*Stream
	opens      map[uint32]chan OpenAck
	pending    map[uint64]chan Reply
	closed     bool
	closeErr   error
	lastPong   time.Time
	nextStream uint32
	nextCall   uint64

	done chan struct{}

	ack HelloAck
}

// Dial 建立连接并完成握手。握手失败（协议版本不符、被拒绝）时返回错误并关闭连接。
func Dial(ctx context.Context, address string, hello Hello, opts ClientOptions) (*Client, error) {
	if opts.HandshakeTimeout == 0 {
		opts.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if opts.Heartbeat == 0 {
		opts.Heartbeat = DefaultHeartbeat
	}
	if opts.HeartbeatTimeout == 0 {
		opts.HeartbeatTimeout = DefaultHeartbeatTimeout
	}
	if opts.Logger == nil {
		opts.Logger = nopLogger{}
	}
	hello.ProtocolVersion = ProtocolVersion

	conn, err := dial(ctx, address, opts.Dial)
	if err != nil {
		return nil, fmt.Errorf("连接插件进程失败（%s）: %w", address, err)
	}
	c := &Client{
		conn:     conn,
		fw:       &frameWriter{w: conn},
		br:       bufio.NewReaderSize(conn, 4096),
		opts:     opts,
		log:      opts.Logger,
		streams:  map[uint32]*Stream{},
		opens:    map[uint32]chan OpenAck{},
		pending:  map[uint64]chan Reply{},
		lastPong: time.Now(),
		done:     make(chan struct{}),
	}
	// 先把握手做完再开读循环：握手的应答就走在同一条连接上，
	// 此时还没有并发读，逻辑最简单也最不容易出错。
	if err := c.handshake(hello); err != nil {
		_ = conn.Close()
		return nil, err
	}
	go c.readLoop()
	if opts.Heartbeat > 0 {
		go c.heartbeatLoop(opts.Heartbeat)
	}
	return c, nil
}

// dial 支持 tcp://host:port、unix:///path 与裸 host:port（默认 tcp）。
//
// 不传文件描述符（Windows 上没有 SCM_RIGHTS），因此数据面走代理转发 —— 见包注释。
func dial(ctx context.Context, address string, custom func(context.Context, string) (net.Conn, error)) (net.Conn, error) {
	if custom != nil {
		return custom(ctx, address)
	}
	network, addr := "tcp", address
	switch {
	case strings.HasPrefix(address, "unix://"):
		network, addr = "unix", strings.TrimPrefix(address, "unix://")
	case strings.HasPrefix(address, "tcp://"):
		addr = strings.TrimPrefix(address, "tcp://")
	}
	d := net.Dialer{}
	return d.DialContext(ctx, network, addr)
}

func (c *Client) handshake(hello Hello) error {
	payload, err := json.Marshal(hello)
	if err != nil {
		return err
	}
	if err := c.fw.write(kindHello, payload); err != nil {
		return fmt.Errorf("发送握手请求失败: %w", err)
	}
	if dl, ok := c.conn.(interface{ SetReadDeadline(time.Time) error }); ok {
		_ = dl.SetReadDeadline(time.Now().Add(c.opts.HandshakeTimeout))
		defer func() { _ = dl.SetReadDeadline(time.Time{}) }()
	}
	k, resp, err := readFrame(c.br)
	if err != nil {
		return fmt.Errorf("等待握手应答失败: %w", err)
	}
	if k != kindHelloAck {
		return fmt.Errorf("%w: 握手时期望 %d，收到 %d", ErrProtocol, kindHelloAck, k)
	}
	var ack HelloAck
	if err := json.Unmarshal(resp, &ack); err != nil {
		return fmt.Errorf("握手应答无法解析: %w", err)
	}
	if ack.Deny != "" {
		return fmt.Errorf("插件 %s 拒绝握手: %s", hello.Plugin, ack.Deny)
	}
	if ack.Name != hello.Plugin {
		return fmt.Errorf("插件名不一致：配置声明 %q，插件自报 %q", hello.Plugin, ack.Name)
	}
	if ack.APIVersion != hello.APIVersion {
		return fmt.Errorf("插件 API 版本不匹配：内核支持 %q，插件实现 %q",
			hello.APIVersion, ack.APIVersion)
	}
	c.ack = ack
	c.log.Info("sidecar 握手完成", "plugin", ack.Name, "version", ack.Version,
		"protocols", ack.Protocols, "methods", ack.Methods)
	return nil
}

// Ack 返回握手时插件自报的元数据。
func (c *Client) Ack() HelloAck {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ack
}

// Done 在连接断开（或主动关闭）时关闭。
func (c *Client) Done() <-chan struct{} { return c.done }

// Err 返回断开原因；仍在连接时返回 nil。
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// Open 请求插件为一条客户端连接建流。
func (c *Client) Open(ctx context.Context, meta Open) (*Stream, error) {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		if err == nil {
			err = ErrClosed
		}
		return nil, err
	}
	c.nextStream++
	meta.Stream = c.nextStream
	wait := make(chan OpenAck, 1)
	c.opens[meta.Stream] = wait
	c.mu.Unlock()

	payload, err := json.Marshal(meta)
	if err != nil {
		c.dropOpen(meta.Stream)
		return nil, err
	}
	if err := c.fw.write(kindOpen, payload); err != nil {
		c.dropOpen(meta.Stream)
		return nil, err
	}
	select {
	case ack := <-wait:
		if !ack.OK {
			reason := ack.Error
			if reason == "" {
				reason = "插件拒绝打开流"
			}
			return nil, errors.New(reason)
		}
	case <-ctx.Done():
		c.dropOpen(meta.Stream)
		return nil, ctx.Err()
	case <-c.done:
		c.dropOpen(meta.Stream)
		return nil, c.closedErr()
	}

	s := newStream(c.fw, meta.Stream, c.forgetStream)
	c.mu.Lock()
	c.streams[meta.Stream] = s
	c.mu.Unlock()
	return s, nil
}

// Call 调用插件的一个方法（控制面，载荷是 JSON）。
func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	c.mu.Lock()
	if c.closed {
		err := c.closeErr
		c.mu.Unlock()
		if err == nil {
			err = ErrClosed
		}
		return err
	}
	c.nextCall++
	id := c.nextCall
	wait := make(chan Reply, 1)
	c.pending[id] = wait
	c.mu.Unlock()

	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			c.dropPending(id)
			return err
		}
		raw = b
	}
	payload, err := json.Marshal(Call{ID: id, Method: method, Params: raw})
	if err != nil {
		c.dropPending(id)
		return err
	}
	if err := c.fw.write(kindCall, payload); err != nil {
		c.dropPending(id)
		return err
	}
	select {
	case reply := <-wait:
		if !reply.OK {
			if reply.Error == "" {
				return fmt.Errorf("插件方法 %s 调用失败", method)
			}
			return errors.New(reply.Error)
		}
		if out != nil && len(reply.Data) > 0 {
			return json.Unmarshal(reply.Data, out)
		}
		return nil
	case <-ctx.Done():
		c.dropPending(id)
		return ctx.Err()
	case <-c.done:
		c.dropPending(id)
		return c.closedErr()
	}
}

// Close 主动关闭连接（幂等）。
func (c *Client) Close() error {
	c.shutdown(ErrClosed)
	return nil
}

func (c *Client) closedErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return ErrClosed
}

func (c *Client) dropOpen(id uint32) {
	c.mu.Lock()
	delete(c.opens, id)
	c.mu.Unlock()
}

func (c *Client) dropPending(id uint64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) forgetStream(s *Stream) {
	c.mu.Lock()
	delete(c.streams, s.id)
	c.mu.Unlock()
}

// heartbeatLoop 定期发 Ping；超过 HeartbeatTimeout 没收到 Pong 就判定插件进程已死。
func (c *Client) heartbeatLoop(interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-tick.C:
		}
		if err := c.fw.write(kindPing, nil); err != nil {
			c.shutdown(err)
			return
		}
		c.mu.Lock()
		last := c.lastPong
		c.mu.Unlock()
		if time.Since(last) > c.opts.HeartbeatTimeout {
			c.shutdown(ErrHeartbeatTimeout)
			return
		}
	}
}

func (c *Client) readLoop() {
	for {
		k, payload, err := readFrame(c.br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("插件进程关闭了连接")
			}
			c.shutdown(err)
			return
		}
		switch k {
		case kindPong:
			c.mu.Lock()
			c.lastPong = time.Now()
			c.mu.Unlock()
		case kindPing:
			// 插件也可以主动探活：立刻回 Pong（内核不会因收到 Ping 而改变状态）。
			if err := c.fw.write(kindPong, nil); err != nil {
				c.shutdown(err)
				return
			}
		case kindReply:
			var reply Reply
			if err := json.Unmarshal(payload, &reply); err != nil {
				c.shutdown(fmt.Errorf("%w: 应答无法解析: %v", ErrProtocol, err))
				return
			}
			c.mu.Lock()
			ch := c.pending[reply.ID]
			delete(c.pending, reply.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- reply
			}
		case kindOpenAck:
			var ack OpenAck
			if err := json.Unmarshal(payload, &ack); err != nil {
				c.shutdown(fmt.Errorf("%w: 开流应答无法解析: %v", ErrProtocol, err))
				return
			}
			c.mu.Lock()
			ch := c.opens[ack.Stream]
			delete(c.opens, ack.Stream)
			c.mu.Unlock()
			if ch != nil {
				ch <- ack
			}
		case kindData:
			id, data, err := splitData(payload)
			if err != nil {
				c.shutdown(err)
				return
			}
			c.mu.Lock()
			s := c.streams[id]
			c.mu.Unlock()
			if s != nil {
				s.push(data)
			}
		case kindClose:
			var msg Close
			if err := json.Unmarshal(payload, &msg); err != nil {
				c.shutdown(fmt.Errorf("%w: 关闭帧无法解析: %v", ErrProtocol, err))
				return
			}
			c.mu.Lock()
			s := c.streams[msg.Stream]
			c.mu.Unlock()
			if s != nil {
				reason := msg.Reason
				if reason == "" {
					reason = "插件关闭了流"
				}
				s.fail(errors.New(reason))
			}
		default:
			c.shutdown(fmt.Errorf("%w: 未知帧类型 %d", ErrProtocol, k))
			return
		}
	}
}

// shutdown 收尾：唤醒所有等待者并关闭连接（幂等）。
func (c *Client) shutdown(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.closeErr == nil {
		c.closeErr = err
	}
	streams := make([]*Stream, 0, len(c.streams))
	for _, s := range c.streams {
		streams = append(streams, s)
	}
	c.streams = map[uint32]*Stream{}
	opens := c.opens
	c.opens = map[uint32]chan OpenAck{}
	pending := c.pending
	c.pending = map[uint64]chan Reply{}
	c.mu.Unlock()

	_ = c.conn.Close()
	close(c.done)
	reason := ErrClosed.Error()
	if err != nil {
		reason = err.Error()
	}
	for _, s := range streams {
		s.fail(err)
	}
	for _, ch := range opens {
		select {
		case ch <- OpenAck{OK: false, Error: reason}:
		default:
		}
	}
	for _, ch := range pending {
		select {
		case ch <- Reply{OK: false, Error: reason}:
		default:
		}
	}
	c.log.Warn("sidecar 连接已断开", "err", reason)
}
