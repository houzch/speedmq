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
type SessionFactory func(remote net.Addr) sdk.Core

// Server 是接入层。
type Server struct {
	log       *slog.Logger
	protocols []sdk.Protocol
	factory   SessionFactory

	mu        sync.Mutex
	listeners []net.Listener
	wg        sync.WaitGroup
}

// New 构造接入层；protocols 的顺序即嗅探优先级。
func New(log *slog.Logger, protocols []sdk.Protocol, factory SessionFactory) *Server {
	return &Server{log: log, protocols: protocols, factory: factory}
}

// Start 依据监听清单创建监听并开始接受连接。
func (s *Server) Start(ctx context.Context, bindings []Binding) error {
	for _, b := range bindings {
		if b.Spec.DetectOnly {
			// 只参与嗅探、不建监听（例如后续 AMQP 1.0 复用 0-9-1 的端口）
			continue
		}
		ln, err := net.Listen("tcp", b.Spec.Addr)
		if err != nil {
			return fmt.Errorf("监听 %s（协议 %s）失败: %w", b.Spec.Addr, b.Protocol.Name(), err)
		}
		if b.Spec.TLS != nil {
			ln = tls.NewListener(ln, b.Spec.TLS)
		}
		name := b.Protocol.Name() + "/" + b.Spec.Name
		s.mu.Lock()
		s.listeners = append(s.listeners, ln)
		s.mu.Unlock()

		s.log.Info("监听已启动", "protocol", b.Protocol.Name(), "listener", b.Spec.Name, "addr", ln.Addr().String())
		s.wg.Add(1)
		go s.acceptLoop(ctx, ln, name)
	}
	return nil
}

// Shutdown 停止接受新连接，并等待已有连接结束（受 ctx 超时约束）。
func (s *Server) Shutdown(ctx context.Context) {
	s.mu.Lock()
	for _, ln := range s.listeners {
		_ = ln.Close()
	}
	s.listeners = nil
	s.mu.Unlock()

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
	if err := proto.Serve(ctx, pc, s.factory(conn.RemoteAddr())); err != nil {
		s.log.Debug("连接结束",
			"protocol", proto.Name(), "remote", conn.RemoteAddr().String(), "err", err)
	}
}

// sniff 按注册顺序匹配协议，先匹配者生效。
func (s *Server) sniff(peek []byte) sdk.Protocol {
	for _, p := range s.protocols {
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
