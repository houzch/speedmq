package plugin

import (
	"context"
	"fmt"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/transport"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// listenerController 把插件运行时的"启用/停用"决策落到接入层的真实端口上。
//
// 放在 internal/plugin 而不是 main：它是"插件治理"的一部分，需要被测试直接覆盖
// （停用后端口必须真的关掉，而不是只改一个状态位）。
type listenerController struct {
	ctx      context.Context
	server   *transport.Server
	registry *Registry
	cfg      *config.Config
}

// NewListenerController 构造监听控制器。
//
// ctx 决定了后续"热启用"时新起监听的生命周期（应与进程生命周期一致）。
func NewListenerController(ctx context.Context, server *transport.Server, reg *Registry, cfg *config.Config) ListenerController {
	return &listenerController{ctx: ctx, server: server, registry: reg, cfg: cfg}
}

// StartListeners 为某插件启动它声明的监听（含配置里的地址覆盖）。
func (l *listenerController) StartListeners(pluginName string) error {
	var bindings []transport.Binding
	for _, p := range l.registry.ProtocolsOf(pluginName) {
		specs, err := resolveListeners(p, l.cfg.Listeners[p.Name()])
		if err != nil {
			return err
		}
		for _, spec := range specs {
			bindings = append(bindings, transport.Binding{Protocol: p, Spec: spec})
		}
	}
	return l.server.Start(l.ctx, bindings)
}

// resolveListeners 把配置里的监听清单落到插件的默认监听上。
//
// 语义：配置**整体定义**该插件的监听集合。第 i 项沿用默认监听第 i 项的名字与其它属性，
// 超出默认数量的是**新增**监听（名为 `<默认名>-<序号>`，如 `amqp-2`）。
//
// 早先的实现只覆盖 `i < len(defaults)` 的前若干项，多出来的配置被静默丢弃 ——
// 于是"一个插件同时开明文与 TLS 两个端口"根本配不出来（TLS 端口压根没监听），
// 这种静默失败比报错更糟，因此改为显式定义集合。
func resolveListeners(p sdk.Protocol, overrides []config.Listener) ([]sdk.ListenerSpec, error) {
	defaults := p.DefaultListeners()
	if len(overrides) == 0 {
		return defaults, nil
	}
	if len(defaults) == 0 {
		return nil, fmt.Errorf("插件 %s 未声明默认监听，无法应用配置里的监听清单", p.Name())
	}
	specs := make([]sdk.ListenerSpec, 0, len(overrides))
	for i, ov := range overrides {
		base := defaults[0]
		if i < len(defaults) {
			base = defaults[i]
		} else {
			base.Name = fmt.Sprintf("%s-%d", defaults[0].Name, i+1)
			base.Addr = ""
		}
		if ov.Addr != "" {
			base.Addr = ov.Addr
		}
		// TLS 在启动时构造：证书读不出来就拒绝启动，而不是等客户端连上来才失败。
		if ov.TLS.Enabled() {
			tlsCfg, err := ov.TLS.Config()
			if err != nil {
				return nil, fmt.Errorf("插件 %s 的监听 %s 的 TLS 配置无效: %w", p.Name(), base.Name, err)
			}
			base.TLS = tlsCfg
		} else {
			// 显式清空：默认监听若自带 TLS，配置里不写 tls 就是不要 TLS。
			base.TLS = nil
		}
		if base.Addr == "" {
			return nil, fmt.Errorf("插件 %s 的监听 %s 未提供地址", p.Name(), base.Name)
		}
		specs = append(specs, base)
	}
	return specs, nil
}

// StopListeners 关闭某插件名下的全部监听。
//
// 接入层是按**协议名**归属监听的（它不认识插件），而这里拿到的是**插件名**：
// 内置协议插件的插件名与协议名恰好相同，但外部进程插件（sidecar）可以不同
// （配置里 `plugins.<插件名>` 与 `protocols[].name` 是两回事）。
// 因此必须先经注册中心把插件映射到它注册的协议，再逐个关闭 ——
// 否则热停用只会把状态位置成 disabled，对外端口仍然开着。
func (l *listenerController) StopListeners(pluginName string) error {
	var firstErr error
	for _, p := range l.registry.ProtocolsOf(pluginName) {
		if err := l.server.StopPlugin(p.Name()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
