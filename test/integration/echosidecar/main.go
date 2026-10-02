// Command echosidecar 是一个**参考用的外部进程插件（B 形态）**，用来验证内核的 sidecar 宿主：
//
//   - 它只依赖标准库与 github.com/houzch/swiftmq/pkg/sidecar（对外稳定契约）；
//   - 业务极简：把客户端发来的每一行原样加前缀回显（ECHO），并提供两个控制面方法
//     （stats 查看计数、set_greeting 改回显前缀）——足以覆盖握手、方法调用与流代理三条链路。
//
// 单独成 module 并 `replace` 指回仓库根：这样它演示的是"**外部**插件如何接入"，
// 而不是内核自己的一部分（依赖方向见 AGENTS.md §2）。
//
// 用法（通常由内核按配置 spawn，也可手动起）：
//
//	echosidecar -addr tcp://127.0.0.1:19001 -name echo-sidecar
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/houzch/swiftmq/pkg/sidecar"
)

const (
	// defaultName 必须与内核配置里声明的插件名一致，否则握手会被内核拒绝。
	defaultName = "echo-sidecar"
	// version 是插件自报的版本。
	version = "0.1.0"
	// protocol 是本插件提供的协议名。
	protocol = "echo"
	// apiVersion 是插件实现所依据的插件 API 版本。
	apiVersion = "v1"
)

func main() {
	addr := flag.String("addr", "tcp://127.0.0.1:19001", "监听地址：tcp://host:port 或 unix:///path")
	name := flag.String("name", defaultName, "插件名（须与内核配置一致）")
	// 下面两个开关只为验证内核的**拒绝路径**：故意报错版本，观察内核是否明确隔离该插件。
	reportAPIVersion := flag.String("api-version", apiVersion, "自报的插件 API 版本（改掉可复现版本不匹配）")
	reportProtocolVersion := flag.String("protocol-version", sidecar.ProtocolVersion, "自报的线协议版本")
	deny := flag.String("deny", "", "非空则在握手时拒绝服务（复现插件拒绝路径）")
	sessionDemo := flag.Bool("session-demo", false, "每条流打开后演示内核语义桥（声明队列→发布→消费→结算）")
	vhost := flag.String("vhost", "/", "session-demo 使用的 vhost")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	h := &handler{
		name:        *name,
		log:         logger,
		greeting:    "ECHO",
		apiVer:      *reportAPIVersion,
		protoVer:    *reportProtocolVersion,
		denyText:    *deny,
		sessionDemo: *sessionDemo,
		vhost:       *vhost,
	}

	srv, err := sidecar.NewServer(h, sidecar.ServerOptions{
		Address: *addr,
		Logger:  stdLogger{logger},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "echosidecar 监听失败: %v\n", err)
		os.Exit(1)
	}
	// 打印实际监听地址：配置里写 0 端口时，宿主需要从这行读回真实端口（本示例用固定端口）。
	logger.Printf("echosidecar 已启动 name=%s version=%s addr=%s", *name, version, srv.Address())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "echosidecar 服务退出: %v\n", err)
		os.Exit(1)
	}
}

// handler 实现 sidecar.Handler：握手 + 控制面方法 + 数据面回显。
type handler struct {
	name     string
	log      *log.Logger
	apiVer   string
	protoVer string
	denyText string

	// sessionDemo 为 true 时，每条流打开后在它上面演示一次内核语义桥。
	sessionDemo bool
	vhost       string

	mu       sync.Mutex
	greeting string

	// 计数用 atomic.Int64 而不是 int64 + atomic.AddInt64：
	// 在 32 位平台（本项目当前即 windows/386）上，手工对 64 位字做原子操作要求 8 字节对齐，
	// 结构体中间字段很容易不满足，会直接 panic（"unaligned 64-bit atomic operation"）。
	// atomic.Int64 由编译器保证对齐，跨平台安全。
	streams  atomic.Int64
	bytesIn  atomic.Int64
	bytesOut atomic.Int64
}

// Hello 处理握手。任一校验失败都返回 error，内核会据此**明确拒绝并隔离**该插件。
func (h *handler) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	if h.denyText != "" {
		return sidecar.HelloAck{}, fmt.Errorf("%s", h.denyText)
	}
	if hello.ProtocolVersion != h.protoVer {
		return sidecar.HelloAck{}, fmt.Errorf("不支持的线协议版本 %q（本插件为 %q）",
			hello.ProtocolVersion, h.protoVer)
	}
	if hello.APIVersion != h.apiVer {
		// 只有当内核版本与插件实现版本一致时才继续；不一致就直接拒绝。
		return sidecar.HelloAck{}, fmt.Errorf("不支持的插件 API 版本 %q（本插件实现 %q）",
			hello.APIVersion, h.apiVer)
	}
	if hello.Plugin != h.name {
		return sidecar.HelloAck{}, fmt.Errorf("插件名不匹配：内核声明 %q，本插件是 %q", hello.Plugin, h.name)
	}
	return sidecar.HelloAck{
		Name:         h.name,
		Version:      version,
		APIVersion:   h.apiVer,
		Capabilities: []string{"net.listen"},
		Protocols:    []string{protocol},
		Methods:      []string{"stats", "set_greeting"},
	}, nil
}

// Call 处理控制面方法调用。
func (h *handler) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeliver:
		// 内核把一条投递回推过来（正向调用）；这里结算它，演示"消费 → 结算"闭环。
		return h.handleDeliver(ctx, params)
	case "stats":
		h.mu.Lock()
		greeting := h.greeting
		h.mu.Unlock()
		return map[string]any{
			"name":      h.name,
			"greeting":  greeting,
			"streams":   h.streams.Load(),
			"bytes_in":  h.bytesIn.Load(),
			"bytes_out": h.bytesOut.Load(),
		}, nil
	case "set_greeting":
		var p struct {
			Text string `json:"text"`
		}
		if len(params) > 0 {
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, fmt.Errorf("set_greeting 参数解析失败: %w", err)
			}
		}
		if strings.TrimSpace(p.Text) == "" {
			return nil, fmt.Errorf("set_greeting 需要非空的 text")
		}
		h.mu.Lock()
		h.greeting = p.Text
		h.mu.Unlock()
		h.log.Printf("greeting 已更新为 %q", p.Text)
		return map[string]any{"greeting": p.Text}, nil
	default:
		// 未知方法必须明确报错：静默成功会让调用方以为生效了。
		return nil, fmt.Errorf("未知方法 %q", method)
	}
}

// Open 处理一条新流：逐行读取并回显，直到对端关闭或客户端发送 quit。
//
// 若以 -session-demo 启动，还会在这条流上先跑一遍内核语义桥演示（见 runSessionDemo）。
func (h *handler) Open(ctx context.Context, stream *sidecar.Stream, meta sidecar.Open) error {
	h.streams.Add(1)
	h.log.Printf("流已打开 stream=%d remote=%s local=%s", stream.ID(), meta.Remote, meta.Local)
	if h.sessionDemo {
		h.runSessionDemo(ctx, stream)
	}

	sc := bufio.NewScanner(stream)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
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
		h.bytesIn.Add(int64(len(line)))
		h.mu.Lock()
		greeting := h.greeting
		h.mu.Unlock()
		reply := greeting + " " + line + "\n"
		if _, err := io.WriteString(stream, reply); err != nil {
			return err
		}
		h.bytesOut.Add(int64(len(reply)))
	}
	return sc.Err()
}

// stdLogger 把 sidecar 的最小日志接口接到标准库日志。
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Info(msg string, args ...any) {
	s.l.Printf("%s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
func (s stdLogger) Warn(msg string, args ...any) {
	s.l.Printf("WARN %s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}

// 让编译器确认 stdLogger 满足 sidecar.Logger（顺便当作接口文档）。
var _ sidecar.Logger = stdLogger{}

// 让编译器确认 handler 满足 sidecar.Handler。
var _ sidecar.Handler = (*handler)(nil)
