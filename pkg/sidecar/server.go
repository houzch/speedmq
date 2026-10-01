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
}

func (c *serverConn) serve(ctx context.Context) {
	defer func() {
		c.close(ErrClosed)
		c.closeWG.Wait()
	}()
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
	c.mu.Unlock()

	_ = c.conn.Close()
	for _, s := range streams {
		s.fail(err)
	}
}
