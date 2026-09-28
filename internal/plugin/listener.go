package plugin

import (
	"context"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/transport"
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
		specs := p.DefaultListeners()
		if overrides, ok := l.cfg.Listeners[p.Name()]; ok {
			for i := range specs {
				if i < len(overrides) && overrides[i].Addr != "" {
					specs[i].Addr = overrides[i].Addr
				}
			}
		}
		for _, spec := range specs {
			bindings = append(bindings, transport.Binding{Protocol: p, Spec: spec})
		}
	}
	return l.server.Start(l.ctx, bindings)
}

// StopListeners 关闭某插件名下的全部监听。
func (l *listenerController) StopListeners(pluginName string) error {
	return l.server.StopPlugin(pluginName)
}
