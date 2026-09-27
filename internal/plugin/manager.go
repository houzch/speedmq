package plugin

import (
	"context"
	"log/slog"

	"github.com/swiftmq/swiftmq/internal/config"

	sdk "github.com/swiftmq/swiftmq/pkg/plugin"
)

// Manager 负责插件生命周期：Add → Resolve → Init → Start → Stop。
type Manager struct {
	reg     *Registry
	cfg     *config.Config
	log     *slog.Logger
	started []sdk.Plugin
	failed  map[string]error
}

// NewManager 构造生命周期管理器。
func NewManager(reg *Registry, cfg *config.Config, log *slog.Logger) *Manager {
	return &Manager{reg: reg, cfg: cfg, log: log, failed: map[string]error{}}
}

// Load 登记并启动插件。
//
// 失败隔离（设计文档 10.6）：单个插件 Init / Start 失败只标记该插件为 failed，
// 内核与其余插件继续运行；只有"重名 / API 版本不匹配 / 依赖缺失或成环"这类
// 内核级契约错误才让 Load 直接返回错误。
func (m *Manager) Load(ctx context.Context, plugins ...sdk.Plugin) error {
	for _, p := range plugins {
		if err := m.reg.Add(p); err != nil {
			return err
		}
	}
	order, err := m.reg.Resolve()
	if err != nil {
		return err
	}
	// 能力审计：把每个插件申请的权限显式打出来，避免静默授权。
	for _, line := range m.reg.Audit() {
		m.log.Info(line)
	}

	for _, p := range order {
		h := newHost(m.reg, m.cfg, m.log, p)
		if err := p.Init(h); err != nil {
			m.failed[p.Name()] = err
			m.log.Error("插件初始化失败，已隔离", "plugin", p.Name(), "err", err)
			continue
		}
		if err := p.Start(ctx); err != nil {
			m.failed[p.Name()] = err
			m.log.Error("插件启动失败，已隔离", "plugin", p.Name(), "err", err)
			continue
		}
		m.started = append(m.started, p)
		m.log.Info("插件已启动", "plugin", p.Name(), "version", p.Version())
	}
	return nil
}

// Stop 按启动的反序停止已启动的插件。
func (m *Manager) Stop(ctx context.Context) {
	for i := len(m.started) - 1; i >= 0; i-- {
		p := m.started[i]
		if err := p.Stop(ctx); err != nil {
			m.log.Warn("插件停止失败", "plugin", p.Name(), "err", err)
		}
	}
	m.started = nil
}

// Failed 返回被隔离的插件及其失败原因。
func (m *Manager) Failed() map[string]error {
	out := make(map[string]error, len(m.failed))
	for k, v := range m.failed {
		out[k] = v
	}
	return out
}
