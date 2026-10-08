// Package amqp091 以"协议插件"的形式实现 AMQP 0-9-1。
//
// 它是 v1 内核内置的第一个协议插件：内核本身不认识 AMQP，只认识 plugin.Protocol。
// 后续新增协议（AMQP 1.0 / MQTT / STOMP）走同一套扩展点，内核零改动。
//
// M1 覆盖范围：协议头校验、Connection 的 Start / Tune / Open / Close、心跳、
// Channel 的 Open / Flow / Close，以及软硬错误的作用域框架。
package amqp091

import (
	"context"
	"log/slog"
	"net"

	"github.com/houzch/speedmq/pkg/plugin"
)

// Version 是本插件版本。
const Version = "0.1.0"

// protocolHeader 是 AMQP 0-9-1 的协议头：'A”M”Q”P' 0x00 0x00 0x09 0x01。
var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0x00, 0x00, 0x09, 0x01}

// Plugin 同时实现 plugin.Plugin 与 plugin.Protocol。
type Plugin struct {
	log *slog.Logger
}

var (
	_ plugin.Plugin   = (*Plugin)(nil)
	_ plugin.Protocol = (*Plugin)(nil)
)

// New 构造 AMQP 0-9-1 协议插件。
func New() *Plugin { return &Plugin{} }

// Name 实现 plugin.Protocol。
func (p *Plugin) Name() string { return "amqp091" }

// Version 实现 plugin.Plugin。
func (p *Plugin) Version() string { return Version }

// APIVersion 实现 plugin.Plugin。
func (p *Plugin) APIVersion() string { return plugin.APIVersion }

// Requires 实现 plugin.Plugin。
func (p *Plugin) Requires() []string { return nil }

// Capabilities 实现 plugin.Plugin：需要创建监听端口。
func (p *Plugin) Capabilities() []plugin.Capability {
	return []plugin.Capability{plugin.CapNetListen}
}

// Description 实现 plugin.Describer：给管理面一句话说明。
func (p *Plugin) Description() string {
	return "AMQP 0-9-1 协议插件（RabbitMQ 客户端兼容基线）"
}

// Init 实现 plugin.Plugin：只注册扩展点，不做耗时动作。
func (p *Plugin) Init(h plugin.Host) error {
	p.log = h.Logger()
	return h.RegisterProtocol(p)
}

// Start 实现 plugin.Plugin。监听由接入层依据 DefaultListeners 创建，故此处无动作。
func (p *Plugin) Start(context.Context) error { return nil }

// Stop 实现 plugin.Plugin：幂等，监听的生命周期由接入层负责。
func (p *Plugin) Stop(context.Context) error { return nil }

// DefaultListeners 实现 plugin.Protocol。
func (p *Plugin) DefaultListeners() []plugin.ListenerSpec {
	return []plugin.ListenerSpec{{Name: "amqp", Addr: ":5672"}}
}

// Sniff 实现 plugin.Protocol。
//
// 精确匹配 0-9-1 的版本字节；信息不足 8 字节时先接管，由 Serve 做版本校验并回写支持的协议头。
// 注意 AMQP 1.0 的协议头同样是 "AMQP" 开头（0x00 0x01 0x00 0x00），
// 因此精确匹配版本号能让 0-9-1 与 1.0 共存而**不依赖插件注册顺序**。
func (p *Plugin) Sniff(peek []byte) bool {
	if len(peek) < 4 || string(peek[:4]) != "AMQP" {
		return false
	}
	if len(peek) < 8 {
		return true
	}
	return peek[4] == 0x00 && peek[5] == 0x00 && peek[6] == 0x09 && peek[7] == 0x01
}

// Serve 实现 plugin.Protocol。
func (p *Plugin) Serve(ctx context.Context, conn net.Conn, core plugin.Core) error {
	c := newConnection(p.log, conn, core)
	return c.run(ctx)
}
