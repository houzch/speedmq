// Command swiftmqd 是 SwiftMQ 的 broker 进程入口。
//
// M1 职责：加载内置插件（AMQP 0-9-1 是第一个协议插件）、按插件声明的监听启动接入层、
// 处理优雅退出。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	pluginkit "github.com/houzch/swiftmq/internal/plugin"
	"github.com/houzch/swiftmq/internal/protocol/amqp091"
	"github.com/houzch/swiftmq/internal/transport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "swiftmqd 启动失败: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath = flag.String("config", "", "配置文件路径（JSON，可选）")
		logLevel   = flag.String("log-level", "info", "日志级别：debug/info/warn/error")
	)
	flag.Parse()

	log := newLogger(*logLevel)

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("SwiftMQ 启动中", "version", broker.Version, "data_dir", cfg.DataDir, "vhost", cfg.DefaultVHost)

	kernel := broker.New(log, cfg)
	// 停止 TTL 扫描与死信派发协程
	defer kernel.Close()

	// 内置插件清单：AMQP 0-9-1 从第一天就以"协议插件"的形式接入，
	// 避免后续新增协议时再回头拆内核。
	registry := pluginkit.NewRegistry(log)
	manager := pluginkit.NewManager(registry, cfg, log)
	if err := manager.Load(ctx, amqp091.New()); err != nil {
		return err
	}

	server := transport.New(log, registry.Protocols(), kernel.NewSession)
	if err := server.Start(ctx, bindings(registry, cfg)); err != nil {
		return err
	}

	<-ctx.Done()
	log.Info("收到停止信号，开始优雅退出")

	server.Shutdown(context.Background())
	manager.Stop(context.Background())

	if failed := manager.Failed(); len(failed) > 0 {
		for name, err := range failed {
			log.Warn("存在被隔离的插件", "plugin", name, "err", err)
		}
	}
	log.Info("已退出")
	return nil
}

// bindings 计算最终监听清单：协议插件的默认监听 + 配置按插件名的覆盖。
func bindings(registry *pluginkit.Registry, cfg *config.Config) []transport.Binding {
	var out []transport.Binding
	for _, p := range registry.Protocols() {
		specs := p.DefaultListeners()
		if overrides, ok := cfg.Listeners[p.Name()]; ok {
			for i := range specs {
				if i < len(overrides) && overrides[i].Addr != "" {
					specs[i].Addr = overrides[i].Addr
				}
			}
		}
		for _, spec := range specs {
			out = append(out, transport.Binding{Protocol: p, Spec: spec})
		}
	}
	return out
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
}
