// Package sidecar_test 从包外驱动 B 形态插件通信：内核侧用 pkg/sidecar.Client，
// 插件侧用 pkg/sidecar.Server（参考实现在 test/integration/echosidecar）。
//
// 断言只落在**可观察行为**上（握手是否被拒、字节有没有原样回显、连接断开后状态是否变化），
// 不触碰任何内部字段 —— 这也顺便证明"线协议可以被第三方独立实现"。
//
// 文件位置与包形式的约定见 AGENTS.md §10.1。
package sidecar_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/houzch/swiftmq/pkg/sidecar"
)

// 线协议的裸字节常量：与 pkg/sidecar/frame.go 的 kind 一一对应。
// 测试里**独立**写一份，是为了让"帧格式"这件事不只由被测代码自己印证。
const (
	rawKindHello    = 1
	rawKindHelloAck = 2
	rawKindPing     = 3
	rawKindPong     = 4
)

// testHandler 是一个最小可用的插件侧业务实现（等价于 echosidecar 的核心部分）。
type testHandler struct {
	name     string
	apiVer   string
	protoVer string
	deny     string

	mu     sync.Mutex
	prefix string
}

func (h *testHandler) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	if h.deny != "" {
		return sidecar.HelloAck{}, errors.New(h.deny)
	}
	protoVer := h.protoVer
	if protoVer == "" {
		protoVer = sidecar.ProtocolVersion
	}
	if hello.ProtocolVersion != protoVer {
		return sidecar.HelloAck{}, fmt.Errorf("线协议版本不匹配：收到 %q，期望 %q", hello.ProtocolVersion, protoVer)
	}
	apiVer := h.apiVer
	if apiVer == "" {
		apiVer = "v1"
	}
	if hello.APIVersion != apiVer {
		return sidecar.HelloAck{}, fmt.Errorf("API 版本不匹配：收到 %q，期望 %q", hello.APIVersion, apiVer)
	}
	return sidecar.HelloAck{
		Name:         h.name,
		Version:      "test-0.1.0",
		APIVersion:   apiVer,
		Capabilities: []string{"net.listen"},
		Protocols:    []string{"echo"},
		Methods:      []string{"stats"},
	}, nil
}

func (h *testHandler) Call(_ context.Context, method string, _ json.RawMessage) (any, error) {
	if method != "stats" {
		return nil, fmt.Errorf("未知方法 %q", method)
	}
	h.mu.Lock()
	prefix := h.prefix
	h.mu.Unlock()
	return map[string]any{"prefix": prefix, "n": 7}, nil
}

func (h *testHandler) Open(ctx context.Context, stream *sidecar.Stream, _ sidecar.Open) error {
	sc := bufio.NewScanner(stream)
	for sc.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := sc.Text()
		if line == "quit" {
			return nil
		}
		h.mu.Lock()
		prefix := h.prefix
		h.mu.Unlock()
		if _, err := io.WriteString(stream, prefix+" "+line+"\n"); err != nil {
			return err
		}
	}
	return sc.Err()
}

// startServer 起一个监听在回环随机端口上的插件侧 Server，并返回其地址。
func startServer(t *testing.T, h sidecar.Handler) string {
	t.Helper()
	_, addr := newServer(t, h)
	return addr
}

// testClient 建立一条握手完成的连接。
func testClient(t *testing.T, addr, plugin string, opts sidecar.ClientOptions) *sidecar.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := sidecar.Dial(ctx, addr, sidecar.Hello{Plugin: plugin, APIVersion: "v1"}, opts)
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestHandshakeCallAndStream 覆盖 happy path：握手 → 控制面方法 → 数据面双向流。
func TestHandshakeCallAndStream(t *testing.T) {
	h := &testHandler{name: "echo-sidecar", prefix: "ECHO"}
	addr := startServer(t, h)
	c := testClient(t, addr, "echo-sidecar", sidecar.ClientOptions{Heartbeat: -1})

	ack := c.Ack()
	if ack.Name != "echo-sidecar" || ack.APIVersion != "v1" {
		t.Fatalf("握手元数据不符: %+v", ack)
	}
	if len(ack.Protocols) != 1 || ack.Protocols[0] != "echo" {
		t.Fatalf("插件声明的协议不符: %+v", ack.Protocols)
	}

	// 控制面：方法调用
	var out struct {
		Prefix string `json:"prefix"`
		N      int    `json:"n"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Call(ctx, "stats", nil, &out); err != nil {
		t.Fatalf("调用 stats 失败: %v", err)
	}
	if out.Prefix != "ECHO" || out.N != 7 {
		t.Fatalf("stats 返回不符: %+v", out)
	}

	// 控制面：未知方法必须明确报错
	if err := c.Call(ctx, "no-such-method", nil, nil); err == nil {
		t.Fatalf("调用未知方法应报错")
	} else if !strings.Contains(err.Error(), "no-such-method") {
		t.Fatalf("未知方法的错误应带上方法名，实际: %v", err)
	}

	// 数据面：开流并回显
	stream, err := c.Open(ctx, sidecar.Open{Remote: "kb", Local: "ks"})
	if err != nil {
		t.Fatalf("开流失败: %v", err)
	}
	defer func() { _ = stream.Close() }()

	if _, err := io.WriteString(stream, "hello sidecar\n"); err != nil {
		t.Fatalf("写入流失败: %v", err)
	}
	line, err := bufio.NewReader(stream).ReadString('\n')
	if err != nil {
		t.Fatalf("读取回显失败: %v", err)
	}
	if line != "ECHO hello sidecar\n" {
		t.Fatalf("回显不符: %q", line)
	}
}

// TestHandshakeRejections 覆盖所有"明确拒绝"的路径：拒绝服务 / 线协议版本 / API 版本 / 插件名。
func TestHandshakeRejections(t *testing.T) {
	cases := []struct {
		name    string
		handler *testHandler
		plugin  string
		wantSub string
	}{
		{
			name:    "插件拒绝服务",
			handler: &testHandler{name: "echo-sidecar", deny: "维护中，暂不服务"},
			plugin:  "echo-sidecar",
			wantSub: "维护中",
		},
		{
			name:    "线协议版本不匹配",
			handler: &testHandler{name: "echo-sidecar", protoVer: "v9"},
			plugin:  "echo-sidecar",
			wantSub: "线协议版本不匹配",
		},
		{
			name:    "API 版本不匹配",
			handler: &testHandler{name: "echo-sidecar", apiVer: "v2"},
			plugin:  "echo-sidecar",
			wantSub: "API 版本",
		},
		{
			name:    "插件名不一致",
			handler: &testHandler{name: "另一个插件"},
			plugin:  "echo-sidecar",
			wantSub: "插件名不一致",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr := startServer(t, tc.handler)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := sidecar.Dial(ctx, addr, sidecar.Hello{Plugin: tc.plugin, APIVersion: "v1"},
				sidecar.ClientOptions{Heartbeat: -1})
			if err == nil {
				t.Fatalf("期望握手被拒绝，实际成功")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("拒绝原因应包含 %q，实际: %v", tc.wantSub, err)
			}
		})
	}
}

// TestConnectionLossIsObservable 覆盖"插件进程没了"的内核侧信号：Done 关闭且 Err 非空。
func TestConnectionLossIsObservable(t *testing.T) {
	h := &testHandler{name: "echo-sidecar"}
	addr := startServer(t, h)
	c := testClient(t, addr, "echo-sidecar", sidecar.ClientOptions{Heartbeat: -1})

	// 主动断开（等价于插件进程崩溃时内核观察到的事件）。
	_ = c.Close()
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("连接关闭后 Done 未关闭")
	}
	if c.Err() == nil {
		t.Fatalf("连接关闭后 Err 应非空")
	}
	// 断开后所有操作都必须立即失败，而不是挂起。
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Call(ctx, "stats", nil, nil); err == nil {
		t.Fatalf("断开后调用应失败")
	}
	if _, err := c.Open(ctx, sidecar.Open{}); err == nil {
		t.Fatalf("断开后开流应失败")
	}
}

// TestHeartbeatTimeout 覆盖"对端卡死但 socket 未断"的场景：靠心跳判定进程已死。
//
// 这里用一个**裸 socket 对端**：它完成握手后就不再回应任何帧（连 Ping 也不回），
// 于是只能靠心跳超时发现异常。同时这也是"帧格式可被独立实现"的一次验证。
func TestHeartbeatTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		br := bufio.NewReader(conn)
		k, _, err := readRawFrame(br)
		if err != nil || k != rawKindHello {
			return
		}
		ack, _ := json.Marshal(sidecar.HelloAck{Name: "echo-sidecar", Version: "raw", APIVersion: "v1"})
		_ = writeRawFrame(conn, rawKindHelloAck, ack)
		// 之后故意不再回任何帧（包括 Ping）：让内核侧只能靠心跳超时判死。
		// 持续读取（不回复）是为了让内核侧的写不会阻塞，并在连接关闭时自然退出。
		for {
			if _, _, err := readRawFrame(br); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := sidecar.Dial(ctx, ln.Addr().String(), sidecar.Hello{Plugin: "echo-sidecar", APIVersion: "v1"},
		sidecar.ClientOptions{Heartbeat: 30 * time.Millisecond, HeartbeatTimeout: 120 * time.Millisecond})
	if err != nil {
		t.Fatalf("握手失败: %v", err)
	}
	defer func() { _ = c.Close() }()

	select {
	case <-c.Done():
	case <-time.After(3 * time.Second):
		t.Fatalf("心跳超时后 Done 未关闭")
	}
	if !errors.Is(c.Err(), sidecar.ErrHeartbeatTimeout) {
		t.Fatalf("断开原因应为心跳超时，实际: %v", c.Err())
	}
}

// ---------------------------------------------------------------------------
// 裸帧读写（测试自带一份，用于验证线协议可被外部实现）
// ---------------------------------------------------------------------------

func writeRawFrame(w io.Writer, k byte, payload []byte) error {
	buf := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(buf, uint32(1+len(payload)))
	buf[4] = k
	copy(buf[5:], payload)
	_, err := w.Write(buf)
	return err
}

func readRawFrame(r io.Reader) (byte, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[:4])
	if n < 1 {
		return 0, nil, errors.New("长度字段非法")
	}
	payload := make([]byte, n-1)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return head[4], payload, nil
}
