package sidecar

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// Server 是插件进程侧的实现：监听一个本机地址，等待内核连接，并把请求交给 Handler。
//
// 插件作者只需要实现 Handler 的三个方法，协议细节（握手、心跳、多路复用、分块）都在这里。
type Server struct {
	ln      net.Listener
	handler Handler
	opts    ServerOptions
	log     Logger

	mu      sync.Mutex
	conns   []*serverConn
	closed  bool
	serveWG sync.WaitGroup
}

// Handler 是插件进程实现的业务面。
//
// 三个方法都可能被并发调用（不同连接、不同流），实现方要自己保证并发安全。
type Handler interface {
	// Hello 处理握手：返回的 HelloAck 会被发给内核。
	// 返回 error 时握手被拒绝（内核侧会得到一条明确的错误并隔离该插件）。
	Hello(ctx context.Context, hello Hello) (HelloAck, error)
	// Call 处理方法调用（控制面）。
	Call(ctx context.Context, method string, params json.RawMessage) (any, error)
	// Open 处理一条新打开的流：通常是**阻塞处理到流结束**再返回。
	// 返回后该流即被视为结束（内核侧对应的客户端连接会被关闭）。
	Open(ctx context.Context, stream *Stream, meta Open) error
}

// ServerOptions 是 Server 的可选参数。
type ServerOptions struct {
	// Address 是监听地址：tcp://host:port、unix:///path；裸地址按 tcp 处理。
	// 空的 host（如 tcp://:0）表示监听回环随机端口，Address() 可读回实际地址。
	Address string
	// Logger 可空。
	Logger Logger
	// IdleTimeout 是"内核连接静默"的上限：超过它没有任何帧就关闭该连接。
	// 它让插件进程不会因为"内核被 kill -9 而 socket 未及时关闭"而留下僵尸连接。
	IdleTimeout time.Duration
}

// NewServer 创建并开始监听。
func NewServer(h Handler, opts ServerOptions) (*Server, error) {
	if h == nil {
		return nil, errors.New("sidecar: Handler 不能为空")
	}
	if opts.Logger == nil {
		opts.Logger = nopLogger{}
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = 3 * DefaultHeartbeatTimeout
	}
	ln, err := listen(opts.Address)
	if err != nil {
		return nil, err
	}
	return &Server{ln: ln, handler: h, opts: opts, log: opts.Logger}, nil
}

func listen(address string) (net.Listener, error) {
	network, addr := "tcp", address
	switch {
	case strings.HasPrefix(address, "unix://"):
		network, addr = "unix", strings.TrimPrefix(address, "unix://")
	case strings.HasPrefix(address, "tcp://"):
		addr = strings.TrimPrefix(address, "tcp://")
	case address == "":
		addr = "127.0.0.1:0"
	}
	return net.Listen(network, addr)
}

// Address 返回实际监听地址（配置里写 0 端口时用得上）。
func (s *Server) Address() string { return s.ln.Addr().String() }

// Serve 接受内核连接直到 ctx 结束或 Close 被调用。
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		sc := &serverConn{
			srv:     s,
			conn:    conn,
			fw:      &frameWriter{w: conn},
			br:      bufio.NewReaderSize(conn, 4096),
			streams: map[uint32]*Stream{},
			pending: map[uint64]chan Reply{},
			done:    make(chan struct{}),
		}
		s.mu.Lock()
		s.conns = append(s.conns, sc)
		s.mu.Unlock()
		s.serveWG.Add(1)
		go func() {
			defer s.serveWG.Done()
			sc.serve(ctx)
		}()
	}
}

// Close 停止监听并断开所有内核连接（幂等）。
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	conns := append([]*serverConn(nil), s.conns...)
	s.mu.Unlock()

	err := s.ln.Close()
	for _, c := range conns {
		c.close(ErrClosed)
	}
	return err
}

// serverConn 是一条来自内核的连接。
type serverConn struct {
	srv  *Server
	conn net.Conn
	fw   *frameWriter
	br   *bufio.Reader

	mu      sync.Mutex
	streams map[uint32]*Stream
	acked   bool
	closed  bool
	closeWG sync.WaitGroup

	// 反向调用（插件 → 内核）的编号与等待表。与"正向调用"（内核 → 插件）分属两个 ID 空间，
	// 两边都从 1 开始自增，因此应答必须靠 Reply.Reverse 判断该唤醒哪张表（见 proto.go）。
	nextCall uint64
	pending  map[uint64]chan Reply
	// done 在连接关闭时关闭，用来唤醒仍在等待应答的反向调用者。
	done chan struct{}
}

func (c *serverConn) serve(ctx context.Context) {
	defer func() {
		c.close(ErrClosed)
		c.closeWG.Wait()
	}()
	// 把 Bridge 挂进 ctx：Handler 的 Call/Open 通过 BridgeFromContext 取到它，
	// 从而能在处理请求时回调内核（这正是"薄封装"的入口）。用 ctx 传而非改 Handler 签名，
	// 是为了不破坏既有的插件实现。
	ctx = context.WithValue(ctx, bridgeCtxKey{}, &Bridge{c: c})
	for {
		// 静默超时：内核进程被强杀时 socket 可能不及时关闭，靠它兜底回收。
		_ = c.conn.SetReadDeadline(time.Now().Add(c.srv.opts.IdleTimeout))
		k, payload, err := readFrame(c.br)
		if err != nil {
			return
		}
		switch k {
		case kindHello:
			if err := c.handleHello(ctx, payload); err != nil {
				c.srv.log.Warn("sidecar 握手被拒绝", "err", err)
				return
			}
		case kindPing:
			if err := c.fw.write(kindPong, nil); err != nil {
				return
			}
		case kindCall:
			c.handleCall(ctx, payload)
		case kindOpen:
			c.handleOpen(ctx, payload)
		case kindReply:
			// 这是插件发起的反向调用（插件 → 内核）的应答：只路由 Reverse 应答，
			// 与内核发来的正向应答（插件侧不会收到）严格区分开。
			var reply Reply
			if err := json.Unmarshal(payload, &reply); err != nil {
				return
			}
			if !reply.Reverse {
				c.srv.log.Warn("收到意外的正向应答，已丢弃", "id", reply.ID)
				continue
			}
			c.mu.Lock()
			ch := c.pending[reply.ID]
			delete(c.pending, reply.ID)
			c.mu.Unlock()
			if ch != nil {
				ch <- reply
			}
		case kindData:
			id, data, err := splitData(payload)
			if err != nil {
				return
			}
			if s := c.stream(id); s != nil {
				s.push(data)
			}
		case kindClose:
			var msg Close
			if err := json.Unmarshal(payload, &msg); err != nil {
				return
			}
			if s := c.stream(msg.Stream); s != nil {
				s.fail(errors.New("内核关闭了流"))
			}
		default:
			return
		}
	}
}

func (c *serverConn) stream(id uint32) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[id]
}

func (c *serverConn) handleHello(ctx context.Context, payload []byte) error {
	var hello Hello
	if err := json.Unmarshal(payload, &hello); err != nil {
		return fmt.Errorf("握手请求无法解析: %w", err)
	}
	ack, err := c.srv.handler.Hello(ctx, hello)
	if err != nil {
		// 拒绝也要**明确回一帧**：否则内核侧只能看到"连接被关闭"，定位不到原因。
		ack = HelloAck{Name: hello.Plugin, APIVersion: hello.APIVersion, Deny: err.Error()}
	}
	resp, merr := json.Marshal(ack)
	if merr != nil {
		return merr
	}
	if werr := c.fw.write(kindHelloAck, resp); werr != nil {
		return werr
	}
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.acked = true
	c.mu.Unlock()
	c.srv.log.Info("内核已接入 sidecar 插件", "protocol_version", hello.ProtocolVersion,
		"kernel", hello.KernelVersion)
	return nil
}

func (c *serverConn) handleCall(ctx context.Context, payload []byte) {
	var call Call
	if err := json.Unmarshal(payload, &call); err != nil {
		return
	}
	// 每次调用单独起协程：慢方法既不该阻塞读循环，也不该拖住其他调用。
	c.closeWG.Add(1)
	go func() {
		defer c.closeWG.Done()
		data, err := c.srv.handler.Call(ctx, call.Method, call.Params)
		reply := Reply{ID: call.ID, OK: err == nil}
		if err != nil {
			reply.Error = err.Error()
		} else if data != nil {
			raw, merr := json.Marshal(data)
			if merr != nil {
				reply.OK, reply.Error = false, merr.Error()
			} else {
				reply.Data = raw
			}
		}
		raw, merr := json.Marshal(reply)
		if merr != nil {
			return
		}
		_ = c.fw.write(kindReply, raw)
	}()
}

func (c *serverConn) handleOpen(ctx context.Context, payload []byte) {
	var meta Open
	if err := json.Unmarshal(payload, &meta); err != nil {
		return
	}
	s := newStream(c.fw, meta.Stream, c.forgetStream)
	c.mu.Lock()
	c.streams[meta.Stream] = s
	c.mu.Unlock()

	// 先回 OpenAck 再交给 Handler：Handler 通常要阻塞到流结束，
	// 若等它返回再应答，内核侧会一直以为"流没打开"。
	ack, err := json.Marshal(OpenAck{Stream: meta.Stream, OK: true})
	if err != nil {
		return
	}
	if werr := c.fw.write(kindOpenAck, ack); werr != nil {
		return
	}
	c.closeWG.Add(1)
	go func() {
		defer c.closeWG.Done()
		defer func() {
			// 插件返回即流结束：通知内核并回收本地状态。
			msg, _ := json.Marshal(Close{Stream: meta.Stream, Reason: "handler returned"})
			_ = c.fw.write(kindClose, msg)
			s.fail(ErrClosed)
		}()
		if err := c.srv.handler.Open(ctx, s, meta); err != nil {
			c.srv.log.Warn("处理流失败", "stream", meta.Stream, "remote", meta.Remote, "err", err)
		}
	}()
}

func (c *serverConn) forgetStream(s *Stream) {
	c.mu.Lock()
	delete(c.streams, s.id)
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 反向调用（插件 → 内核）
// ---------------------------------------------------------------------------

// bridgeCtxKey 是 Bridge 在 ctx 里的键（未导出，外部只能经 BridgeFromContext 取）。
type bridgeCtxKey struct{}

// Bridge 是插件进程发起反向调用（插件 → 内核）的句柄。
//
// 它绑定在"一条来自内核的连接"上：Handler 的 Call/Open 拿到的 ctx 里就挂着当前连接对应的
// Bridge（见 BridgeFromContext）。插件用它把请求打到内核的 plugin.Session 上。
type Bridge struct {
	c *serverConn
}

// BridgeFromContext 取出当前请求所属连接的 Bridge。第二个返回值为 false 表示 ctx 里没有
// （例如不是由内核连接触发的调用）。
func BridgeFromContext(ctx context.Context) (*Bridge, bool) {
	b, ok := ctx.Value(bridgeCtxKey{}).(*Bridge)
	return b, ok
}

// Call 发起一次反向调用：method 为 MethodSession* 之一，params 为该方法的参数（会被 JSON 序列化），
// out 非 nil 时接收应答的 Data。
//
// 返回错误分两类：传输/解码类错误（连接已断、应答无法解析）返回普通 error；
// 内核明确拒绝时返回 *RPCError（含 Kind 与 Text），插件可据此还原错误分类。
func (b *Bridge) Call(ctx context.Context, method string, params any, out any) error {
	if b == nil || b.c == nil {
		return errors.New("sidecar: 当前上下文没有内核连接桥")
	}
	return b.c.reverseCall(ctx, method, params, out)
}

// reverseCall 是反向调用的实现：分配编号 → 登记等待 → 发送 → 等待应答。
func (c *serverConn) reverseCall(ctx context.Context, method string, params any, out any) error {
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		raw = b
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.nextCall++
	id := c.nextCall
	wait := make(chan Reply, 1)
	c.pending[id] = wait
	c.mu.Unlock()

	payload, err := json.Marshal(Call{ID: id, Method: method, Reverse: true, Params: raw})
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
			return replyError(reply)
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
		return ErrClosed
	}
}

// replyError 把失败应答还原成 error：带分类时返回 *RPCError，否则返回纯文本错误。
func replyError(reply Reply) error {
	if reply.Error == "" {
		return errors.New("sidecar: 反向调用失败")
	}
	if len(reply.Data) > 0 {
		var payload ErrorPayload
		if err := json.Unmarshal(reply.Data, &payload); err == nil && payload.Text != "" {
			return &RPCError{Kind: payload.Kind, Text: payload.Text}
		}
	}
	return errors.New(reply.Error)
}

func (c *serverConn) dropPending(id uint64) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *serverConn) close(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	streams := make([]*Stream, 0, len(c.streams))
	for _, s := range c.streams {
		streams = append(streams, s)
	}
	c.streams = map[uint32]*Stream{}
	pending := c.pending
	c.pending = map[uint64]chan Reply{}
	c.mu.Unlock()

	_ = c.conn.Close()
	// 唤醒仍在等待反向调用应答的协程，避免它们挂到超时。
	close(c.done)
	reason := err.Error()
	for _, ch := range pending {
		select {
		case ch <- Reply{OK: false, Error: reason}:
		default:
		}
	}
	for _, s := range streams {
		s.fail(err)
	}
}
