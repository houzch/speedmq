// 本文件覆盖"把 plugin.Session 桥成 RPC"：反向调用（插件 → 内核）的线协议与内核侧分发器。
//
// 断言落在**可观察行为**上：插件经反向调用拿到的结果、内核把投递回推给插件、结算后队列的
// 消息数变化、流关闭后未结算投递被重新入队。用**真内核**（internal/broker）驱动 session.*
// 映射，因此 session.open 的失败分类、声明/发布/消费/结算的整体链路都按内核真实语义断言；
// 只有"反向调用基本通路"与"未配置处理器"两条用例用裸客户端，因为那两条只关心通路本身。
package sidecar_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	sidecarhost "github.com/houzch/swiftmq/internal/plugin/sidecar"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

// ---------------------------------------------------------------------------
// 测试用的插件侧实现（业务动作由各用例以回调注入）
// ---------------------------------------------------------------------------

// bridgePlugin 是一个可编程的插件侧 Handler：Open/Call 的行为由用例注入。
type bridgePlugin struct {
	name string

	mu        sync.Mutex
	openFn    func(ctx context.Context, stream *sidecar.Stream, br *sidecar.Bridge)
	deliverFn func(ctx context.Context, p sidecar.DeliverParams) error
}

func (h *bridgePlugin) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	return sidecar.HelloAck{
		Name:         h.name,
		Version:      "bridge-test",
		APIVersion:   hello.APIVersion,
		Capabilities: []string{"net.listen"},
		Protocols:    []string{"bridge"},
	}, nil
}

func (h *bridgePlugin) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeliver:
		var p sidecar.DeliverParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		h.mu.Lock()
		fn := h.deliverFn
		h.mu.Unlock()
		if fn == nil {
			return nil, nil
		}
		return nil, fn(ctx, p)
	case "echo":
		var p struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		return map[string]int{"n": p.N}, nil
	default:
		return nil, fmt.Errorf("未知方法 %q", method)
	}
}

func (h *bridgePlugin) Open(ctx context.Context, stream *sidecar.Stream, _ sidecar.Open) error {
	br, _ := sidecar.BridgeFromContext(ctx)
	h.mu.Lock()
	fn := h.openFn
	h.mu.Unlock()
	if fn != nil {
		fn(ctx, stream, br)
	}
	// 阻塞到流结束（内核关闭连接 / 发送 Close 帧），以便桥状态在用例断言期间一直有效。
	buf := make([]byte, 256)
	for {
		if _, err := stream.Read(buf); err != nil {
			return nil
		}
	}
}

// echoResult 承载一次反向调用的结果（在测试 goroutine 里断言）。
type echoResult struct {
	out struct {
		N int `json:"n"`
	}
	err error
}

// ---------------------------------------------------------------------------
// 内核侧装配
// ---------------------------------------------------------------------------

// capturingHost 是 sdk.Host 的最小替身，顺便捕获插件注册的协议（用例要用它的 Serve）。
type capturingHost struct {
	log   *slog.Logger
	proto sdk.Protocol
}

func (h *capturingHost) PluginName() string                    { return "echo-sidecar" }
func (h *capturingHost) Logger() *slog.Logger                  { return h.log }
func (h *capturingHost) Config(any) error                      { return nil }
func (h *capturingHost) RegisterProtocol(p sdk.Protocol) error { h.proto = p; return nil }

// startBridgeHost 启动内核侧 sidecar 宿主，并返回它注册的协议与到插件的连接。
func startBridgeHost(t *testing.T, addr string) (sdk.Protocol, clienter) {
	t.Helper()
	plugs, err := sidecarhost.FromConfig(sidecarConfig(t, addr), testLogger(), "test-kernel")
	if err != nil {
		t.Fatalf("解析 sidecar 配置失败: %v", err)
	}
	if len(plugs) != 1 {
		t.Fatalf("应解析出 1 个 sidecar 插件，实际 %d", len(plugs))
	}
	h := &capturingHost{log: testLogger()}
	p := plugs[0]
	if err := p.Init(h); err != nil {
		t.Fatalf("宿主插件 Init 失败: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("宿主插件 Start 失败: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	if h.proto == nil {
		t.Fatalf("宿主插件未注册协议")
	}
	cl, ok := p.(clienter)
	if !ok {
		t.Fatalf("宿主插件应提供 Client()")
	}
	return h.proto, cl
}

// newBridgeCore 构造一个**真内核**并返回两条已认证的会话面。
func newBridgeCore(t *testing.T) (sdk.Core, sdk.Session) {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载默认配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	b, err := broker.New(testLogger(), cfg)
	if err != nil {
		t.Fatalf("构造内核失败: %v", err)
	}
	t.Cleanup(b.Close)

	// 两条独立连接：一条交给插件使用（会话归桥所有），一条留给用例做旁路检查。
	pluginCore := authCore(t, b)
	inspectCore := authCore(t, b)
	sess, err := inspectCore.Session("/")
	if err != nil {
		t.Fatalf("打开检查会话失败: %v", err)
	}
	return pluginCore, sess
}

func authCore(t *testing.T, b *broker.Broker) sdk.Core {
	t.Helper()
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := b.NewSession(remote, local)
	resp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	return core
}

// serveProto 用 net.Pipe 冒充一条客户端连接，把协议代理跑起来；返回一个"关闭该连接"的函数。
func serveProto(t *testing.T, proto sdk.Protocol, core sdk.Core) func() {
	t.Helper()
	kc, sc := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = proto.Serve(ctx, kc, core)
	}()
	var once sync.Once
	closeConn := func() { once.Do(func() { _ = sc.Close() }) }
	t.Cleanup(func() {
		cancel()
		closeConn()
		<-done
	})
	return closeConn
}

// waitUntil 轮询等待条件成立（替代 time.Sleep 做同步）。
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", what)
}

func queueCount(t *testing.T, sess sdk.Session, name string) uint32 {
	t.Helper()
	info, err := sess.DeclareQueue(sdk.QueueDeclare{Name: name, Passive: true})
	if err != nil {
		t.Fatalf("被动声明队列 %s 失败: %v", name, err)
	}
	return info.MessageCount
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

// TestBridgeReverseCallBasicPath 覆盖反向调用基本通路：插件发起 → 内核钩子执行 → 插件拿到结果。
func TestBridgeReverseCallBasicPath(t *testing.T) {
	got := make(chan echoResult, 1)
	h := &bridgePlugin{name: "echo-sidecar"}
	h.openFn = func(ctx context.Context, _ *sidecar.Stream, br *sidecar.Bridge) {
		var res echoResult
		err := br.Call(ctx, "test.echo", map[string]int{"n": 21}, &res.out)
		res.err = err
		got <- res
	}
	_, addr := newServer(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := sidecar.Dial(ctx, addr, sidecar.Hello{Plugin: "echo-sidecar", APIVersion: "v1"},
		sidecar.ClientOptions{
			Heartbeat: -1,
			OnCall: func(_ context.Context, method string, params json.RawMessage) (any, error) {
				if method != "test.echo" {
					return nil, fmt.Errorf("未知方法 %q", method)
				}
				var p struct {
					N int `json:"n"`
				}
				if err := json.Unmarshal(params, &p); err != nil {
					return nil, err
				}
				return map[string]int{"n": p.N * 2}, nil
			},
		})
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	stream, err := c.Open(ctx, sidecar.Open{})
	if err != nil {
		t.Fatalf("开流失败: %v", err)
	}
	defer func() { _ = stream.Close() }()

	select {
	case res := <-got:
		if res.err != nil {
			t.Fatalf("反向调用失败: %v", res.err)
		}
		if res.out.N != 42 {
			t.Fatalf("反向调用结果不符：期望 42，实际 %d", res.out.N)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("等待反向调用结果超时")
	}
}

// TestBridgeReverseWithoutHandler 覆盖"内核没开反向调用"的路径：必须明确报错，不能静默丢弃。
func TestBridgeReverseWithoutHandler(t *testing.T) {
	got := make(chan error, 1)
	h := &bridgePlugin{name: "echo-sidecar"}
	h.openFn = func(ctx context.Context, _ *sidecar.Stream, br *sidecar.Bridge) {
		got <- br.Call(ctx, sidecar.MethodSessionOpen,
			sidecar.SessionOpenParams{Stream: 1, VHost: "/"}, nil)
	}
	_, addr := newServer(t, h)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 故意不配置 OnCall。
	c, err := sidecar.Dial(ctx, addr, sidecar.Hello{Plugin: "echo-sidecar", APIVersion: "v1"},
		sidecar.ClientOptions{Heartbeat: -1})
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	stream, err := c.Open(ctx, sidecar.Open{})
	if err != nil {
		t.Fatalf("开流失败: %v", err)
	}
	defer func() { _ = stream.Close() }()

	select {
	case err := <-got:
		if err == nil || !strings.Contains(err.Error(), "未提供反向调用处理器") {
			t.Fatalf("未配置处理器时应回明确错误，实际: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("等待反向调用错误超时")
	}
}

// TestBridgeSessionOpen 覆盖 session.open 的成功与失败：vhost 不存在时错误分类应为 KindInvalidPath。
func TestBridgeSessionOpen(t *testing.T) {
	pluginCore, _ := newBridgeCore(t)

	badCh := make(chan error, 1)
	okCh := make(chan error, 1)
	h := &bridgePlugin{name: "echo-sidecar"}
	h.openFn = func(ctx context.Context, stream *sidecar.Stream, br *sidecar.Bridge) {
		sid := stream.ID()
		// 先试一个不存在的 vhost（失败），再试 "/"（成功）——同一个流上两者互不影响。
		badCh <- br.Call(ctx, sidecar.MethodSessionOpen,
			sidecar.SessionOpenParams{Stream: sid, VHost: "不存在的vhost"}, nil)
		okCh <- br.Call(ctx, sidecar.MethodSessionOpen,
			sidecar.SessionOpenParams{Stream: sid, VHost: "/"}, nil)
	}
	_, addr := newServer(t, h)
	proto, _ := startBridgeHost(t, addr)
	serveProto(t, proto, pluginCore)

	select {
	case err := <-badCh:
		var rpc *sidecar.RPCError
		if !errors.As(err, &rpc) {
			t.Fatalf("vhost 不存在应返回带分类的错误，实际: %v", err)
		}
		if rpc.Kind != int(sdk.KindInvalidPath) {
			t.Fatalf("vhost 不存在的错误分类应为 KindInvalidPath(%d)，实际 %d（%s）",
				int(sdk.KindInvalidPath), rpc.Kind, rpc.Text)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("等待 session.open 失败路径超时")
	}
	select {
	case err := <-okCh:
		if err != nil {
			t.Fatalf("打开已存在 vhost 的会话应成功，实际: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("等待 session.open 超时")
	}
}

// TestBridgeDeclarePublishConsumeSettle 覆盖完整链路：
// 声明队列 → 发布 → 消费 → 内核回推投递 → 插件 session.settle(ack) → 消息不再重投。
func TestBridgeDeclarePublishConsumeSettle(t *testing.T) {
	pluginCore, inspect := newBridgeCore(t)
	const qname = "bridge.ack"

	ready := make(chan struct{})
	delivered := make(chan sidecar.DeliverParams, 1)
	h := &bridgePlugin{name: "echo-sidecar"}
	h.openFn = func(ctx context.Context, stream *sidecar.Stream, br *sidecar.Bridge) {
		sid := stream.ID()
		if err := br.Call(ctx, sidecar.MethodSessionOpen,
			sidecar.SessionOpenParams{Stream: sid, VHost: "/"}, nil); err != nil {
			return
		}
		if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue,
			sidecar.QueueDeclareParams{Stream: sid, Name: qname, Durable: true}, nil); err != nil {
			return
		}
		if err := br.Call(ctx, sidecar.MethodSessionPublish,
			sidecar.PublishParams{Stream: sid, RoutingKey: qname,
				Message: sidecar.MessageDTO{Body: []byte("payload")}}, nil); err != nil {
			return
		}
		if err := br.Call(ctx, sidecar.MethodSessionConsume,
			sidecar.ConsumeParams{Stream: sid, Queue: qname}, nil); err != nil {
			return
		}
		close(ready)
	}
	h.deliverFn = func(ctx context.Context, p sidecar.DeliverParams) error {
		delivered <- p
		br, _ := sidecar.BridgeFromContext(ctx)
		return br.Call(ctx, sidecar.MethodSessionSettle,
			sidecar.SettleParams{DeliveryID: p.DeliveryID, Action: sidecar.SettleActionAck}, nil)
	}
	_, addr := newServer(t, h)
	proto, _ := startBridgeHost(t, addr)
	serveProto(t, proto, pluginCore)

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("等待插件完成声明/发布/消费超时")
	}
	select {
	case p := <-delivered:
		if string(p.Message.Body) != "payload" {
			t.Fatalf("回推的消息体不符: %q", p.Message.Body)
		}
		if p.Queue != qname || p.ConsumerTag == "" {
			t.Fatalf("回推的投递字段不符: %+v", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("等待内核回推投递超时")
	}

	// 已 ack：消息不再留在队列里（重新投递也不会发生）。
	waitUntil(t, "ack 后队列清空", func() bool { return queueCount(t, inspect, qname) == 0 })
}

// TestBridgeStreamCloseRequeuesUnsettled 覆盖：流关闭时未结算的投递被重新入队。
func TestBridgeStreamCloseRequeuesUnsettled(t *testing.T) {
	pluginCore, inspect := newBridgeCore(t)
	const qname = "bridge.requeue"

	ready := make(chan struct{})
	delivered := make(chan sidecar.DeliverParams, 1)
	h := &bridgePlugin{name: "echo-sidecar"}
	h.openFn = func(ctx context.Context, stream *sidecar.Stream, br *sidecar.Bridge) {
		sid := stream.ID()
		if err := br.Call(ctx, sidecar.MethodSessionOpen,
			sidecar.SessionOpenParams{Stream: sid, VHost: "/"}, nil); err != nil {
			return
		}
		if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue,
			sidecar.QueueDeclareParams{Stream: sid, Name: qname, Durable: true}, nil); err != nil {
			return
		}
		if err := br.Call(ctx, sidecar.MethodSessionConsume,
			sidecar.ConsumeParams{Stream: sid, Queue: qname}, nil); err != nil {
			return
		}
		// 消费注册好后再发布，确保消息一定经消费者投递（而非留在就绪队列）。
		if err := br.Call(ctx, sidecar.MethodSessionPublish,
			sidecar.PublishParams{Stream: sid, RoutingKey: qname,
				Message: sidecar.MessageDTO{Body: []byte("held")}}, nil); err != nil {
			return
		}
		close(ready)
	}
	// 收到投递后**故意不结算**：模拟插件还没来得及处理就断流。
	h.deliverFn = func(_ context.Context, p sidecar.DeliverParams) error {
		delivered <- p
		return nil
	}
	_, addr := newServer(t, h)
	proto, _ := startBridgeHost(t, addr)
	closeConn := serveProto(t, proto, pluginCore)

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatalf("等待插件完成声明/消费/发布超时")
	}
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatalf("等待内核回推投递超时")
	}

	// 关闭这条客户端连接 → 内核释放该流的会话 → 未结算投递应重新入队。
	closeConn()
	waitUntil(t, "断流后未结算投递重新入队", func() bool { return queueCount(t, inspect, qname) == 1 })
}

// TestBridgeForwardAndReverseConcurrent 是 ID 撞车的回归测试：
// 内核发正向调用、插件发反向调用，两者编号都从 1 开始，并发进行也绝不能串台。
func TestBridgeForwardAndReverseConcurrent(t *testing.T) {
	pluginCore, _ := newBridgeCore(t)

	const n = 40
	reverseErr := make(chan error, n)
	reverseDone := make(chan struct{})
	h := &bridgePlugin{name: "echo-sidecar"}
	h.openFn = func(ctx context.Context, stream *sidecar.Stream, br *sidecar.Bridge) {
		defer close(reverseDone)
		sid := stream.ID()
		if err := br.Call(ctx, sidecar.MethodSessionOpen,
			sidecar.SessionOpenParams{Stream: sid, VHost: "/"}, nil); err != nil {
			reverseErr <- err
			return
		}
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				name := fmt.Sprintf("bridge.concurrent.%d", i)
				var info sidecar.QueueInfoResult
				if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue,
					sidecar.QueueDeclareParams{Stream: sid, Name: name, Durable: true}, &info); err != nil {
					reverseErr <- fmt.Errorf("反向调用 #%d 失败: %w", i, err)
					return
				}
				if info.Name != name {
					reverseErr <- fmt.Errorf("反向调用 #%d 结果串台：期望 %q，实际 %q", i, name, info.Name)
				}
			}(i)
		}
		wg.Wait()
	}
	_, addr := newServer(t, h)
	proto, cl := startBridgeHost(t, addr)
	serveProto(t, proto, pluginCore)

	// 等连接就绪（宿主重连是异步的），再并发打正向调用。
	waitUntil(t, "宿主连接就绪", func() bool { return cl.Client() != nil })
	client := cl.Client()

	var wg sync.WaitGroup
	fwdErr := make(chan error, n)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var out struct {
				N int `json:"n"`
			}
			if err := client.Call(ctx, "echo", map[string]int{"n": i}, &out); err != nil {
				fwdErr <- fmt.Errorf("正向调用 #%d 失败: %w", i, err)
				return
			}
			if out.N != i {
				fwdErr <- fmt.Errorf("正向调用 #%d 结果串台：实际 %d", i, out.N)
			}
		}(i)
	}
	wg.Wait()
	// 等插件侧的反向调用全部收尾，再关闭收集通道（否则会 send on closed channel）。
	select {
	case <-reverseDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("等待插件侧反向调用收尾超时")
	}

	close(fwdErr)
	close(reverseErr)
	for err := range fwdErr {
		t.Fatalf("正向调用出错: %v", err)
	}
	for err := range reverseErr {
		t.Fatalf("反向调用出错: %v", err)
	}
}
