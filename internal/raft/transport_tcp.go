package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// 线格式（一根连接、按方法名复用；生产用它，测试用 MemNetwork）：
//
//	请求： <fromLen u16 BE><from><methodLen u16 BE><method><payloadLen u32 BE><payload>
//	响应： <status u8><payloadLen u32 BE><payload>
//
// 比"只有 method+payload"多带一个 from：Transport 契约里的 from 参数在进程内网络里
// 天生可得，走 TCP 时只能由发送方显式写上；否则该参数在真实集群下永远是空串，
// 上层想按来源做日志/审计就没有依据。status 单独一个字节（0 成功 / 1 错误），
// 使"对端处理失败"与"传输失败"能在调用方区分开。
const (
	maxMethodLen = 1 << 12
	maxPayload   = 64 << 20
	// tcpRPCTimeout 是未设置 ctx 截止时间时的兜底超时，避免连接永久挂起。
	tcpRPCTimeout = 10 * time.Second
)

const (
	statusOK  = 0
	statusErr = 1
)

// tcpTransport 是生产环境的集群通道。
//
// 每次 Call 新建一条连接（不做连接池）：Raft 的 RPC 频率低（心跳级），
// 复用连接带来的"陈旧连接、半开连接"处理成本远高于收益，短连接更简单也更稳。
type tcpTransport struct {
	selfID string
	ln     net.Listener
	log    Logger

	mu       sync.RWMutex
	addrs    map[string]string
	handlers map[string]handler
	conns    map[net.Conn]struct{}

	// lifeMu 只用于"接连接"与"开始关停"之间的互斥，保证 wg.Add 不会与 wg.Wait 竞争。
	lifeMu  sync.Mutex
	closing bool
	wg      sync.WaitGroup

	done     chan struct{}
	closeOne sync.Once
}

// NewTCPTransport 创建 TCP 传输：监听 listen，并通过 peers（id → 地址）定位其他节点。
//
// listen 允许 "127.0.0.1:0"：此时由内核分配端口，测试里不会与其它实例撞端口。
func NewTCPTransport(listen, selfID string, peers map[string]string, log Logger) (Transport, error) {
	if selfID == "" {
		return nil, fmt.Errorf("raft: TCP 传输的 selfID 不能为空")
	}
	if log == nil {
		log = nopLogger{}
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("监听集群端口 %s 失败: %w", listen, err)
	}
	addrs := make(map[string]string, len(peers))
	for id, addr := range peers {
		if addr != "" {
			addrs[id] = addr
		}
	}
	t := &tcpTransport{
		selfID:   selfID,
		ln:       ln,
		log:      log,
		addrs:    addrs,
		handlers: make(map[string]handler),
		conns:    make(map[net.Conn]struct{}),
		done:     make(chan struct{}),
	}
	t.wg.Add(1)
	go t.acceptLoop()
	log.Info("集群 RPC 监听已启动", "id", selfID, "addr", ln.Addr().String())
	return t, nil
}

func (t *tcpTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			t.lifeMu.Lock()
			closing := t.closing
			t.lifeMu.Unlock()
			if closing {
				return
			}
			// 非关停导致的 Accept 错误（如瞬时 fd 耗尽）：退避后重试，避免错误风暴打满 CPU。
			select {
			case <-t.done:
				return
			case <-time.After(50 * time.Millisecond):
				continue
			}
		}

		t.lifeMu.Lock()
		if t.closing {
			t.lifeMu.Unlock()
			_ = conn.Close()
			continue
		}
		t.wg.Add(1)
		t.lifeMu.Unlock()

		t.track(conn)
		go func() {
			defer t.wg.Done()
			defer t.untrack(conn)
			t.serveConn(conn)
		}()
	}
}

func (t *tcpTransport) track(conn net.Conn) {
	t.mu.Lock()
	t.conns[conn] = struct{}{}
	t.mu.Unlock()
}

func (t *tcpTransport) untrack(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

// serveConn 处理一条连接上的多个请求（一根连接按方法名复用）。
func (t *tcpTransport) serveConn(conn net.Conn) {
	defer conn.Close()
	for {
		from, method, payload, err := readRequest(conn)
		if err != nil {
			return
		}
		h := t.handlerFor(method)
		if h == nil {
			if err := writeResponse(conn, statusErr, []byte(fmt.Sprintf("方法 %s 未注册", method))); err != nil {
				return
			}
			continue
		}
		resp, herr := h(context.Background(), from, payload)
		if herr != nil {
			if err := writeResponse(conn, statusErr, []byte(herr.Error())); err != nil {
				return
			}
			continue
		}
		if err := writeResponse(conn, statusOK, resp); err != nil {
			return
		}
	}
}

// Call 向 to 发起一次 RPC。
func (t *tcpTransport) Call(ctx context.Context, to, method string, payload []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.RLock()
	if t.closing {
		t.mu.RUnlock()
		return nil, fmt.Errorf("raft: 传输已关闭")
	}
	addr := t.addrs[to]
	t.mu.RUnlock()
	if addr == "" {
		return nil, fmt.Errorf("raft: 未知节点 %s", to)
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("连接节点 %s(%s) 失败: %w", to, addr, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(tcpRPCTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	// 兜底超时是 best-effort：即便设置失败，调用方仍可用 ctx 取消。
	_ = conn.SetDeadline(deadline)

	if err := writeRequest(conn, t.selfID, method, payload); err != nil {
		return nil, err
	}
	status, resp, err := readResponse(conn)
	if err != nil {
		return nil, err
	}
	if status != statusOK {
		return nil, fmt.Errorf("raft: 节点 %s 返回错误: %s", to, string(resp))
	}
	return resp, nil
}

// Serve 注册方法处理器；重复注册返回 ErrDuplicateMethod。
func (t *tcpTransport) Serve(method string, h handler) error {
	if len(method) == 0 || len(method) > maxMethodLen {
		return fmt.Errorf("raft: 非法方法名长度 %d", len(method))
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing {
		return fmt.Errorf("raft: 传输已关闭")
	}
	if _, dup := t.handlers[method]; dup {
		return ErrDuplicateMethod
	}
	t.handlers[method] = h
	return nil
}

// Close 停止监听、断开全部连接并等待接收协程退出（幂等）。
func (t *tcpTransport) Close() error {
	var err error
	t.closeOne.Do(func() {
		t.lifeMu.Lock()
		t.closing = true
		t.lifeMu.Unlock()
		close(t.done)

		err = t.ln.Close()

		t.mu.Lock()
		conns := make([]net.Conn, 0, len(t.conns))
		for c := range t.conns {
			conns = append(conns, c)
		}
		t.mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		t.wg.Wait()
	})
	if err != nil {
		return fmt.Errorf("关闭集群监听失败: %w", err)
	}
	return nil
}

func (t *tcpTransport) handlerFor(method string) handler {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closing {
		return nil
	}
	return t.handlers[method]
}

func writeRequest(w io.Writer, from, method string, payload []byte) error {
	if len(from) > maxMethodLen {
		return fmt.Errorf("raft: 来源标识过长")
	}
	buf := make([]byte, 0, 2+len(from)+2+len(method)+4+len(payload))
	buf = appendU16(buf, uint16(len(from)))
	buf = append(buf, from...)
	buf = appendU16(buf, uint16(len(method)))
	buf = append(buf, method...)
	buf = appendU32(buf, uint32(len(payload)))
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}

func readRequest(r io.Reader) (string, string, []byte, error) {
	from, err := readSized(r, maxMethodLen)
	if err != nil {
		return "", "", nil, err
	}
	method, err := readSized(r, maxMethodLen)
	if err != nil {
		return "", "", nil, err
	}
	payload, err := readSized(r, maxPayload)
	if err != nil {
		return "", "", nil, err
	}
	return string(from), string(method), payload, nil
}

func writeResponse(w io.Writer, status byte, payload []byte) error {
	buf := make([]byte, 0, 1+4+len(payload))
	buf = append(buf, status)
	buf = appendU32(buf, uint32(len(payload)))
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}

func readResponse(r io.Reader) (byte, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[1:5])
	if n > maxPayload {
		return 0, nil, fmt.Errorf("raft: 响应载荷长度 %d 超出上限", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return head[0], payload, nil
}

// readSized 读取"u16 长度 + 内容"字段，并限制长度上限（防御畸形帧）。
func readSized(r io.Reader, limit int) ([]byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(head[:]))
	if n > limit {
		return nil, fmt.Errorf("raft: 字段长度 %d 超出上限 %d", n, limit)
	}
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func appendU16(buf []byte, v uint16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

func appendU32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
