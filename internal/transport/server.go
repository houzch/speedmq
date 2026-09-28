// Package transport 是接入层：负责监听、TLS、协议嗅探与连接分发。
//
// 它不认识任何具体协议：协议识别交给已注册的 plugin.Protocol，
// 因此新增协议不需要改这里一行代码。
package transport

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"

	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

const (
	// sniffSize 是协议嗅探读取的最大前缀长度：8 字节足以区分 AMQP 0-9-1 / AMQP 1.0 / MQTT / STOMP。
	sniffSize = 8
	// sniffTimeout 是等待客户端送来协议前缀的最长时间，避免空闲连接长期占用资源。
	sniffTimeout = 10 * time.Second
	// sniffBufSize 是嗅探用缓冲大小。
	sniffBufSize = 512
)

// Binding 描述"某个协议在某个地址上监听"。
type Binding struct {
	// Protocol 协议插件。
	Protocol sdk.Protocol
	// Spec 监听参数。
	Spec sdk.ListenerSpec
}

// SessionFactory 为每条新连接创建内核操作面（由 broker 提供）。
//
// 同时传入远端与本地地址：连接名（管理面展示与强制关闭的定位键）需要对端与本地两端信息，
// 与 RabbitMQ 的 `"<peer> -> <local>"` 命名一致。
type SessionFactory func(remote, local net.Addr) sdk.Core

// ListenerSnapshot 是一个已启动监听的可观测快照（管理面 /api/overview 用）。
type ListenerSnapshot struct {
	// Protocol 是协议插件名，如 "amqp091"。
	Protocol string
	// Listener 是监听名，如 "amqp"。
	Listener string
	// Addr 是实际绑定的地址，如 "[::]:5672"。
	Addr string
}

// boundListener 是一个已启动的监听。
type boundListener struct {
	spec sdk.ListenerSpec
	ln   net.Listener
}

// ProtocolSource 返回当前参与嗅探的协议（按优先级排序）。
//
// 用"每次调用的函数"而不是启动时固定的切片：插件可以在运行期被热启用/停用，
// 嗅探候选集必须跟着变，否则停用中的协议仍会从别的端口把连接接走。
type ProtocolSource func() []sdk.Protocol

// Server 是接入层。
type Server struct {
	log       *slog.Logger
	protocols ProtocolSource
	factory   SessionFactory

	mu sync.Mutex
	// listeners 按**协议插件名**归属，这样"热停用一个插件"就是关掉它名下的监听集合
	// （对齐设计 10.10：不重启内核即可启停一个协议插件的 listener）。
	listeners map[string][]boundListener
	wg        sync.WaitGroup
}

// New 构造接入层；protocols 的顺序即嗅探优先级。
func New(log *slog.Logger, protocols ProtocolSource, factory SessionFactory) *Server {
	return &Server{
		log:       log,
		protocols: protocols,
		factory:   factory,
		listeners: map[string][]boundListener{},
	}
}

// Start 依据监听清单创建监听并开始接受连接。
func (s *Server) Start(ctx context.Context, bindings []Binding) error {
	for _, b := range bindings {
		if b.Spec.DetectOnly {
			// 只参与嗅探、不建监听（例如后续 AMQP 1.0 复用 0-9-1 的端口）
			continue
		}
		if err := s.startOne(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) startOne(ctx context.Context, b Binding) error {
	ln, err := net.Listen("tcp", b.Spec.Addr)
	if err != nil {
		return fmt.Errorf("监听 %s（协议 %s）失败: %w", b.Spec.Addr, b.Protocol.Name(), err)
	}
	if b.Spec.TLS != nil {
		ln = tls.NewListener(ln, b.Spec.TLS)
	}
	name := b.Protocol.Name() + "/" + b.Spec.Name
	s.mu.Lock()
	s.listeners[b.Protocol.Name()] = append(s.listeners[b.Protocol.Name()],
		boundListener{spec: b.Spec, ln: ln})
	s.mu.Unlock()

	s.log.Info("监听已启动", "protocol", b.Protocol.Name(), "listener", b.Spec.Name, "addr", ln.Addr().String())
	s.wg.Add(1)
	go s.acceptLoop(ctx, ln, name)
	return nil
}

// StopPlugin 关闭某个协议插件名下的全部监听（热停用）。
//
// 只关监听、不动其他插件与已有连接：已建立的连接由客户端在连接断开后自行重连，
// 这也是"能力级热停用"的语义边界（对齐设计 10.6）。
func (s *Server) StopPlugin(name string) error {
	s.mu.Lock()
	bound := s.listeners[name]
	delete(s.listeners, name)
	s.mu.Unlock()

	for _, b := range bound {
		if err := b.ln.Close(); err != nil {
			s.log.Warn("关闭监听失败", "protocol", name, "listener", b.spec.Name, "err", err)
		}
	}
	if len(bound) > 0 {
		s.log.Info("插件监听已停止", "protocol", name, "listeners", len(bound))
	}
	return nil
}

// Listeners 返回当前所有监听快照（按协议名、监听名排序，保证输出稳定）。
func (s *Server) Listeners() []ListenerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ListenerSnapshot, 0, len(s.listeners))
	for proto, list := range s.listeners {
		for _, b := range list {
			out = append(out, ListenerSnapshot{
				Protocol: proto,
				Listener: b.spec.Name,
				Addr:     b.ln.Addr().String(),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Protocol != out[j].Protocol {
			return out[i].Protocol < out[j].Protocol
		}
		return out[i].Listener < out[j].Listener
	})
	return out
}

// Shutdown 停止接受新连接，并等待已有连接结束（受 ctx 超时约束）。
func (s *Server) Shutdown(ctx context.Context) {
	s.mu.Lock()
	var all []net.Listener
	for _, list := range s.listeners {
		for _, b := range list {
			all = append(all, b.ln)
		}
	}
	s.listeners = map[string][]boundListener{}
	s.mu.Unlock()

	for _, ln := range all {
		_ = ln.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.log.Warn("等待连接结束超时，仍有连接未退出")
	}
}

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener, name string) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// 临时错误（如 fd 瞬时限耗尽）不应终止监听
			s.log.Warn("accept 失败，继续监听", "listener", name, "err", err)
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(ctx, name, conn)
		}()
	}
}

// handle 完成协议嗅探并把连接交给对应协议插件。
func (s *Server) handle(ctx context.Context, name string, conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	if tc, ok := conn.(*net.TCPConn); ok {
		// 消息中间件以延迟优先，禁用 Nagle 合并
		_ = tc.SetNoDelay(true)
	}

	br := bufio.NewReaderSize(conn, sniffBufSize)
	_ = conn.SetReadDeadline(time.Now().Add(sniffTimeout))
	peek, err := br.Peek(sniffSize)
	_ = conn.SetReadDeadline(time.Time{})
	if len(peek) == 0 {
		if err != nil && !errors.Is(err, io.EOF) {
			s.log.Debug("读取协议前缀失败", "listener", name, "err", err)
		}
		return
	}

	proto := s.sniff(peek)
	if proto == nil {
		s.log.Warn("无法识别的协议前缀，已断开",
			"listener", name, "remote", conn.RemoteAddr().String(),
			"peek", fmt.Sprintf("% x", peek))
		return
	}

	// 把嗅探阶段已缓冲的字节与连接拼回一个 net.Conn，协议插件仍按标准方式工作
	pc := &peekedConn{Conn: conn, r: br}
	if err := proto.Serve(ctx, pc, s.factory(conn.RemoteAddr(), conn.LocalAddr())); err != nil {
		s.log.Debug("连接结束",
			"protocol", proto.Name(), "remote", conn.RemoteAddr().String(), "err", err)
	}
}

// sniff 按注册顺序匹配协议，先匹配者生效。
func (s *Server) sniff(peek []byte) sdk.Protocol {
	if s.protocols == nil {
		return nil
	}
	for _, p := range s.protocols() {
		if p.Sniff(peek) {
			return p
		}
	}
	return nil
}

// peekedConn 把"已被嗅探读取过的缓冲"与连接重新拼成 net.Conn。
//
// 嵌入 net.Conn 使 Write / Close / 地址 / 超时等方法自动透传，
// 只把 Read 改为从嗅探缓冲继续读，从而不会丢掉嗅探时消费掉的字节。
type peekedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *peekedConn) Read(p []byte) (int, error) { return c.r.Read(p) }
