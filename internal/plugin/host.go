package plugin

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/swiftmq/swiftmq/internal/config"

	sdk "github.com/swiftmq/swiftmq/pkg/plugin"
)

// host 是 sdk.Host 的内核侧实现，绑定到单个插件。
//
// 它是插件访问内核的唯一边界：插件拿到的一切能力都必须经过这里，
// 因此"未声明的能力被拒绝"这一约束在这里落地（设计文档 10.7）。
type host struct {
	reg  *Registry
	cfg  *config.Config
	name string
	log  *slog.Logger
	caps map[sdk.Capability]bool
}

func newHost(reg *Registry, cfg *config.Config, log *slog.Logger, p sdk.Plugin) *host {
	caps := make(map[sdk.Capability]bool, len(p.Capabilities()))
	for _, c := range p.Capabilities() {
		caps[c] = true
	}
	return &host{
		reg:  reg,
		cfg:  cfg,
		name: p.Name(),
		log:  log.With("plugin", p.Name()),
		caps: caps,
	}
}

// PluginName 返回当前宿主对应的插件名。
func (h *host) PluginName() string { return h.name }

// Logger 返回带插件名前缀的日志器。
func (h *host) Logger() *slog.Logger { return h.log }

// Config 把该插件的配置段按 JSON 语义解码到 out。
func (h *host) Config(out any) error {
	raw := h.cfg.PluginConfig(h.name)
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("插件 %s 配置解析失败: %w", h.name, err)
	}
	return nil
}

// RegisterProtocol 注册协议扩展点；未声明 net.listen 能力时拒绝。
func (h *host) RegisterProtocol(p sdk.Protocol) error {
	if p == nil {
		return fmt.Errorf("插件 %s 注册了空协议", h.name)
	}
	if !h.caps[sdk.CapNetListen] {
		return fmt.Errorf("%w: 插件 %s 调用 RegisterProtocol 但未声明 %s",
			ErrCapabilityDenied, h.name, sdk.CapNetListen)
	}
	if err := h.reg.registerProtocol(h.name, p); err != nil {
		return err
	}
	h.log.Info("协议扩展点已注册", "protocol", p.Name())
	return nil
}
