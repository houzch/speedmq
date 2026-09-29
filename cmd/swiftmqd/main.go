// Command swiftmqd 是 SwiftMQ 的 broker 进程入口。
//
// 职责：加载配置 → 装配内核与插件运行时 → 起监听（按插件）→ 起管理面 → 优雅退出。
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
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/management"
	pluginkit "github.com/houzch/swiftmq/internal/plugin"
	"github.com/houzch/swiftmq/internal/protocol/amqp091"
	"github.com/houzch/swiftmq/internal/protocol/mqtt"
	"github.com/houzch/swiftmq/internal/protocol/spec"
	"github.com/houzch/swiftmq/internal/transport"
	"github.com/houzch/swiftmq/web"
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
		logLevel   = flag.String("log-level", "", "日志级别：debug/info/warn/error（默认 info，可被 SWIFTMQ_LOG_LEVEL 覆盖）")
		logFormat  = flag.String("log-format", "", "日志格式：text/json（默认 text，可被 SWIFTMQ_LOG_FORMAT 覆盖）")
	)
	flag.Parse()

	log := newLogger(pick(*logLevel, "SWIFTMQ_LOG_LEVEL", "info"), pick(*logFormat, "SWIFTMQ_LOG_FORMAT", "text"))

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startedAt := time.Now()
	nodeName := nodeName()
	if cfg.Cluster.Enabled && cfg.Cluster.NodeID != "" {
		// 集群模式下以集群身份作为节点名：否则同一个节点会同时出现在
		// `/api/cluster`（cluster.node_id）与 `/api/nodes`（本机名派生）里两个名字下，运维对不上号。
		nodeName = cfg.Cluster.NodeID
	}
	log.Info("SwiftMQ 启动中",
		"version", broker.Version,
		"node", nodeName,
		"data_dir", cfg.DataDir,
		"vhost", cfg.DefaultVHost,
		"fsync", cfg.Storage.Fsync)

	kernel, err := broker.New(log, cfg)
	if err != nil {
		return err
	}
	// 跨节点转发需要把整条消息搬到队列数据所在的节点：属性值的类型体系由协议决定
	// （AMQP 是 field-table），因此编解码器由协议侧提供，在这里显式装配。
	// 单机部署下它不会被用到；集群模式下缺少它会让转发明确报错而不是静默丢消息。
	kernel.SetMessageCodec(spec.NewMessageCodec())
	// 停止 TTL 扫描、死信派发、水位检查，并收尾刷盘
	defer kernel.Close()

	// 内置插件清单：AMQP 0-9-1 从第一天就以"协议插件"的形式接入，
	// 避免后续新增协议时再回头拆内核。
	registry := pluginkit.NewRegistry(log)
	manager := pluginkit.NewManager(registry, cfg, log)
	// 接入层的嗅探候选集来自"当前已启用的插件"，因此先建服务再 Load 也必须正确：
	// 这里传的是函数而不是快照，热启用/停用会立刻反映到嗅探上。
	server := transport.New(log, manager.EnabledProtocols, kernel.NewSession)
	// 监听器控制器：让"插件热启用/停用"落到真实的端口起停上（见设计 10.10）。
	manager.SetListenerController(pluginkit.NewListenerController(ctx, server, registry, cfg))
	// M7 的插件化验证：MQTT 3.1.1 是内核内置的第二个协议插件。
	// 新增协议对内核的改动**只有这里的一行**（协议自身全部落在 internal/protocol/mqtt）。
	if err := manager.Load(ctx, amqp091.New(), mqtt.New()); err != nil {
		return err
	}

	// 管理面（Management HTTP API + 内嵌 UI + Prometheus 指标）。
	// 它属于兼容性契约的一部分，因此是内建的，不做成插件（见设计 10.1）。
	var mgmt *management.Server
	if cfg.Management.Enabled {
		mgmt, err = management.New(log, management.Deps{
			Addr:      cfg.Management.Addr,
			Broker:    kernel,
			Plugins:   manager,
			Listeners: server.Listeners,
			Version:   broker.Version,
			NodeName:  nodeName,
			StartedAt: startedAt,
			UI:        web.Dist,
		})
		if err != nil {
			return err
		}
		if err := mgmt.Start(); err != nil {
			return err
		}
	} else {
		log.Warn("管理面已在配置中停用（management.enabled=false）")
	}

	<-ctx.Done()
	log.Info("收到停止信号，开始优雅退出")

	if mgmt != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		mgmt.Shutdown(shutdownCtx)
		cancel()
	}
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

// nodeName 生成节点名，形如 swiftmq@<hostname>（对齐 RabbitMQ 的 name@host）。
func nodeName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "localhost"
	}
	return "swiftmq@" + host
}

// pick 返回优先级最高的取值：命令行 > 环境变量 > 默认值。
func pick(flagValue, envKey, fallback string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return fallback
}

func newLogger(level, format string) *slog.Logger {
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
	opts := &slog.HandlerOptions{Level: lv}
	// JSON 输出便于日志系统采集（设计 9.2）；text 更适合人肉看本地调试
	if strings.EqualFold(format, "json") {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}
