// Package mqtt 是 MQTT 3.1.1 协议插件（内核内置的第二个协议插件）。
//
// 边界（设计 §10.5）：本包只 import pkg/plugin 与标准库 —— 既不认识内核的队列/存储实现，
// 也不允许被内核反向依赖。因此"新增一种协议"对本项目而言就是"新增一个包 + 在
// 组装处注册一行"，内核代码零改动。
//
// 覆盖范围（有意为之的取舍见 README 与设计文档的 M7 交付说明）：
//   - 报文：CONNECT / CONNACK / PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP /
//     SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK / PINGREQ / PINGRESP / DISCONNECT；
//   - QoS：0 与 1；订阅请求 QoS=2 时按规范降级授予 1（入站 QoS2 的四步握手仍完整）；
//   - 会话：Clean Session 映射到队列的 durable/autoDelete，持久会话在重连后继续投递；
//   - 保留消息：支持（但存在插件内存里，重启即丢）；
//   - 遗嘱消息：支持，在非正常断开时发布；
//   - Keep Alive：支持（按 1.5 倍判定连接失效）。
package mqtt

import (
	"context"
	"log/slog"
	"net"

	"github.com/houzch/swiftmq/pkg/plugin"
)

// Plugin 同时实现 plugin.Plugin 与 plugin.Protocol。
type Plugin struct {
	log      *slog.Logger
	opts     options
	retained *retainedStore
}

var (
	_ plugin.Plugin   = (*Plugin)(nil)
	_ plugin.Protocol = (*Plugin)(nil)
)

// New 构造 MQTT 协议插件。
func New() *Plugin { return &Plugin{retained: newRetainedStore(), opts: options{}.withDefaults()} }

// Name 实现 plugin.Plugin 与 plugin.Protocol。
func (p *Plugin) Name() string { return "mqtt" }

// Version 实现 plugin.Plugin。
func (p *Plugin) Version() string { return Version }

// APIVersion 实现 plugin.Plugin。
func (p *Plugin) APIVersion() string { return plugin.APIVersion }

// Requires 实现 plugin.Plugin：MQTT 不依赖其他插件。
func (p *Plugin) Requires() []string { return nil }

// Capabilities 实现 plugin.Plugin：只需要创建监听端口。
//
// 注意这里**没有**申请 store.read / store.write —— 协议插件不做存储，
// 消息一律经 plugin.Session 交给内核，能力面因此可以保持最小。
func (p *Plugin) Capabilities() []plugin.Capability {
	return []plugin.Capability{plugin.CapNetListen}
}

// Description 实现 plugin.Describer。
func (p *Plugin) Description() string {
	return "MQTT 3.1.1 协议插件（QoS 0/1、保留消息、遗嘱、Keep Alive）"
}

// Init 实现 plugin.Plugin：读取配置并注册协议扩展点，不做耗时动作。
func (p *Plugin) Init(h plugin.Host) error {
	p.log = h.Logger()
	p.opts = options{}.withDefaults()
	if err := h.Config(&p.opts); err != nil {
		// 配置段非法不阻塞启动：采用默认值并告警，避免一个笔误让内核起不来。
		p.log.Warn("MQTT 配置解析失败，采用默认值", "err", err)
	}
	p.opts = p.opts.withDefaults()
	p.log.Info("MQTT 插件已初始化",
		"exchange", p.opts.Exchange, "max_packet_size", p.opts.MaxPacketSize, "prefetch", p.opts.Prefetch)
	return h.RegisterProtocol(p)
}

// Start 实现 plugin.Plugin：监听由接入层依据 DefaultListeners 创建，故此处无动作。
func (p *Plugin) Start(context.Context) error { return nil }

// Stop 实现 plugin.Plugin：幂等，监听的生命周期由接入层负责。
func (p *Plugin) Stop(context.Context) error { return nil }

// DefaultListeners 实现 plugin.Protocol：MQTT 的默认端口 1883（对齐惯例）。
func (p *Plugin) DefaultListeners() []plugin.ListenerSpec {
	return []plugin.ListenerSpec{{Name: "mqtt", Addr: ":1883"}}
}

// Sniff 实现 plugin.Protocol。
//
// MQTT 3.1.1 的第一个报文必须是 CONNECT，其固定头首字节恒为 0x10
// （类型 1 在 4 个高位、标志位必须为 0）。因此单字节即可判定，
// 且与 AMQP 0-9-1（"AMQP"）、AMQP 1.0（"AMQP"）不会互相误判 —— 与注册顺序无关。
func (p *Plugin) Sniff(peek []byte) bool {
	return len(peek) >= 1 && peek[0] == 0x10
}

// Serve 实现 plugin.Protocol。
func (p *Plugin) Serve(ctx context.Context, conn net.Conn, core plugin.Core) error {
	log := p.log
	if log == nil {
		log = slog.Default()
	}
	return newConn(log, conn, core, p.opts, p.retained).run(ctx)
}
