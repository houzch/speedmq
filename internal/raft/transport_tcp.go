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

	// tcpPoolSize 是"每个对端"最多保留的可复用连接数：既复用连接、又保证连接数有界。
	tcpPoolSize = 4
	// tcpIdleTimeout 是复用连接的闲置上限：超过即弃用重拨，避免复用一条早已被对端
	// 或中间设备回收的"陈旧/半开"连接。
	tcpIdleTimeout = 30 * time.Second
)

const (
	statusOK  = 0
	statusErr = 1
)

// tcpTransport 是生产环境的集群通道。
//
// 出站连接按对端复用：每个对端维护一个有界连接池（tcpPoolSize），Call 时取一条
// 空闲连接，用完归还。这样既避免"每 RPC 一次 dial + 服务端一个接收协程"的连接/线程
// churn（高 RPC 频率下曾把受限环境的 OS 线程耗尽），又用容量上限保证连接数有界。
// 一条连接同一时刻只被一个 Call 独占，因此一问一答的线上格式不会交错。
type tcpTransport struct {
	selfID string
	ln     net.Listener
	log    Logger

	mu       sync.RWMutex
	addrs    map[string]string
	handlers map[string]handler
	conns    map[net.Conn]struct{}
	// pools 是"对端地址 → 可复用连接池"（缓冲通道，容量 tcpPoolSize）。由 mu 保护。
	pools map[string]chan *pooledConn
	// poolClosed 表示连接池已随 Close 关停：此后取用一律新建、归还一律关闭。
	// 单独用一个 mu 保护的标志，避免依赖 closing 的既有锁口径（其写入在 lifeMu 下）。
	poolClosed bool

	// lifeMu 只用于"接连接"与"开始关停"之间的互斥，保证 wg.Add 不会与 wg.Wait 竞争。
	lifeMu  sync.Mutex
	closing bool
	wg      sync.WaitGroup

	done     chan struct{}
	closeOne sync.Once
}

// pooledConn 是一条可复用的出站连接，lastUsed 用于判定闲置是否超时。
type pooledConn struct {
	net.Conn
	lastUsed time.Time
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
		pools:    make(map[string]chan *pooledConn),
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

// RegisterPeer 运行期注册/更新一个成员的地址（成员变更用）。
//
// 空地址会被忽略：地址信息来自 ConfChange，而进程内网络与"仅知道 ID"的场景
// 都不需要地址，不该因此把已有映射清掉。
func (t *tcpTransport) RegisterPeer(id, addr string) {
	if id == "" || addr == "" {
		return
	}
	t.mu.Lock()
	if t.addrs == nil {
		t.addrs = make(map[string]string)
	}
	old, existed := t.addrs[id]
	t.addrs[id] = addr
	t.mu.Unlock()
	if !existed || old != addr {
		t.log.Info("已注册集群成员地址", "id", id, "addr", addr)
	}
}

// UnregisterPeer 注销一个成员的地址（成员被移除时调用）。
//
// 只删地址、不影响既有连接：成员被移除不代表它的进程立刻消失，
// 保留"能连但不再被调用"的状态比强行断开更安全。
func (t *tcpTransport) UnregisterPeer(id string) {
	t.mu.Lock()
	_, existed := t.addrs[id]
	delete(t.addrs, id)
	t.mu.Unlock()
	if existed {
		t.log.Info("已注销集群成员地址", "id", id)
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

	pc, err := t.acquireConn(ctx, to, addr)
	if err != nil {
		return nil, err
	}

	deadline := time.Now().Add(tcpRPCTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	// 兜底超时是 best-effort：即便设置失败，调用方仍可用 ctx 取消。
	_ = pc.SetDeadline(deadline)

	if err := writeRequest(pc, t.selfID, method, payload); err != nil {
		// 连接可能已损坏，不再复用。
		_ = pc.Close()
		return nil, err
	}
	status, resp, err := readResponse(pc)
	if err != nil {
		_ = pc.Close()
		return nil, err
	}
	if status != statusOK {
		// 对端明确报错：连接本身是好的，归还复用。
		t.putPooledConn(addr, pc)
		return nil, fmt.Errorf("raft: 节点 %s 返回错误: %s", to, string(resp))
	}
	t.putPooledConn(addr, pc)
	return resp, nil
}

// acquireConn 取一条到 addr 的可用连接：优先复用池中空闲连接（闲置超时的弃用重拨），
// 池空时新建一条。
func (t *tcpTransport) acquireConn(ctx context.Context, to, addr string) (*pooledConn, error) {
	if pc := t.takePooledConn(addr); pc != nil {
		return pc, nil
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("连接节点 %s(%s) 失败: %w", to, addr, err)
	}
	return &pooledConn{Conn: conn, lastUsed: time.Now()}, nil
}

// takePooledConn 取一条可复用连接；没有（或传输已关闭）时返回 nil。
func (t *tcpTransport) takePooledConn(addr string) *pooledConn {
	t.mu.Lock()
	if t.poolClosed {
		t.mu.Unlock()
		return nil
	}
	pool := t.pools[addr]
	var stale []*pooledConn
	var out *pooledConn
Loop:
	for {
		select {
		case pc := <-pool:
			if time.Since(pc.lastUsed) > tcpIdleTimeout {
				// 闲置过久：可能已被对端回收，弃用后重拨。
				stale = append(stale, pc)
				continue
			}
			out = pc
			break Loop
		default:
			break Loop
		}
	}
	t.mu.Unlock()
	// 连接关闭是系统调用，放在锁外做（避免持锁做 IO）。
	for _, pc := range stale {
		_ = pc.Close()
	}
	return out
}

// putPooledConn 归还一条连接。传输已关闭或池已满时直接关闭它，保证连接数有界；
// 归还必须在 mu 内完成，避免与 Close 的清空竞态（否则会漏关一条连接）。
func (t *tcpTransport) putPooledConn(addr string, pc *pooledConn) {
	pc.lastUsed = time.Now()
	t.mu.Lock()
	if t.poolClosed {
		t.mu.Unlock()
		_ = pc.Close()
		return
	}
	pool := t.pools[addr]
	if pool == nil {
		pool = make(chan *pooledConn, tcpPoolSize)
		t.pools[addr] = pool
	}
	pooled := true
	select {
	case pool <- pc:
	default:
		pooled = false // 池已满：丢弃多余连接，保持连接数有界
	}
	t.mu.Unlock()
	if !pooled {
		_ = pc.Close()
	}
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
		// 关停连接池：置位后归还的连接会直接被关闭；已归还的连接在此清空并关闭。
		t.poolClosed = true
		pools := t.pools
		t.pools = make(map[string]chan *pooledConn)
		t.mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		for _, pool := range pools {
			for {
				select {
				case pc := <-pool:
					_ = pc.Close()
					continue
				default:
				}
				break
			}
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
	payload, err := readPayload(r)
	if err != nil {
		return "", "", nil, err
	}
	return string(from), string(method), payload, nil
}

// readPayload 读取"u32 长度 + 内容"的请求载荷。
//
// 长度字段是 u32（见 writeRequest），不能用 readSized 的 u16 口径读 ——
// 那样只会读到长度字段的高 16 位，得到空载荷并把整条流读错位。
func readPayload(r io.Reader) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n > maxPayload {
		return nil, fmt.Errorf("raft: 请求载荷长度 %d 超出上限 %d", n, maxPayload)
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
