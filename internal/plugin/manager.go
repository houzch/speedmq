package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/houzch/swiftmq/internal/config"

	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// ListenerController 由接入层实现：按插件热启动/停止其监听端口。
//
// 把"端口生命周期"放在接入层、把"启停决策"放在插件运行时，是为了让"热停用一个插件"
// 只关它名下的监听与会话，不影响其他插件与内核（对齐设计 10.6 / 10.10）。
type ListenerController interface {
	// StartListeners 为指定插件启动它声明的监听；已启动时应幂等忽略。
	StartListeners(pluginName string) error
	// StopListeners 关闭指定插件名下的全部监听。
	StopListeners(pluginName string) error
}

// Manager 负责插件生命周期：Add → Resolve → Init → Start → （运行期启停）→ Stop。
type Manager struct {
	reg *Registry
	cfg *config.Config
	log *slog.Logger

	listeners ListenerController
	ctx       context.Context

	// mu 保护下面的所有可变状态：启停可以通过管理 HTTP API 并发触发，
	// 没有锁就会出现"两个请求同时改状态位"的竞态。
	mu sync.Mutex

	// hosts 保存每个插件的 Host 句柄：运行期 Enable 需要重新 Init 被停用的插件。
	hosts map[string]*host
	// plugins / order 是按依赖顺序排列的插件清单。
	plugins map[string]sdk.Plugin
	order   []string
	// initialized / started 记录插件已经走过的生命周期阶段。
	//
	// 必须记录而不能只看状态位：Disable 之后再 Enable 会再次调用 start()，
	// 若重复 Init，插件会重复注册协议/路由（内核会以"重复注册"直接报错，
	// 表现为"停用过的插件再也起不来"）。
	initialized map[string]bool
	started     map[string]bool
	// state 是每个插件的运行状态（管理面据此展示 enabled / disabled / failed）。
	state map[string]sdk.State
	// failed 记录被隔离的插件及其失败原因。
	failed map[string]error
}

// NewManager 构造生命周期管理器。
func NewManager(reg *Registry, cfg *config.Config, log *slog.Logger) *Manager {
	return &Manager{
		reg:         reg,
		cfg:         cfg,
		log:         log,
		hosts:       map[string]*host{},
		plugins:     map[string]sdk.Plugin{},
		initialized: map[string]bool{},
		started:     map[string]bool{},
		state:       map[string]sdk.State{},
		failed:      map[string]error{},
	}
}

// SetListenerController 注入接入层句柄；必须在 Load 之前调用。
func (m *Manager) SetListenerController(lc ListenerController) { m.listeners = lc }

// Load 登记并启动插件。
//
// 失败隔离（对齐设计 10.10：错误影响面仅限出问题的插件）：
//   - "重名"这类无法二选一的问题仍由 Add 直接报错（内核级，必须人工修配置）；
//   - API 版本不匹配、依赖缺失、依赖成环，以及单个插件 Init/Start 失败，
//     都只把**该插件（及其依赖者）**标记为 failed 并记录原因，内核与其余插件继续运行。
//
// 例外：配置里声明 required: true 的插件失败会阻塞启动（仅限官方核心插件使用）。
func (m *Manager) Load(ctx context.Context, plugins ...sdk.Plugin) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ctx = ctx
	for _, p := range plugins {
		if err := m.reg.Add(p); err != nil {
			return err
		}
		m.plugins[p.Name()] = p
		m.hosts[p.Name()] = newHost(m.reg, m.cfg, m.log, p)
	}
	order, broken := m.reg.Resolve()
	// 能力审计：把每个插件申请的权限显式打出来，避免静默授权。
	for _, line := range m.reg.Audit() {
		m.log.Info(line)
	}

	// 先处理被契约问题隔离的插件：不启动，但状态与原因必须在管理面可见（留痕）。
	for _, name := range sortedNames(broken) {
		err := broken[name]
		m.state[name] = sdk.StateFailed
		m.failed[name] = err
		m.log.Error("插件已被隔离，内核与其余插件不受影响", "plugin", name, "err", err)
		if _, required, _ := m.cfg.PluginFlags(name); required {
			return fmt.Errorf("必需的插件 %s 被隔离: %w", name, err)
		}
	}

	for _, p := range order {
		m.order = append(m.order, p.Name())
		enabled, _, _ := m.cfg.PluginFlags(p.Name())
		if !enabled {
			// 配置里显式停用：不 Init（于是不注册扩展点）、不建监听
			m.state[p.Name()] = sdk.StateDisabled
			m.log.Info("插件已按配置停用", "plugin", p.Name())
			continue
		}
		if err := m.startLocked(p); err != nil {
			m.state[p.Name()] = sdk.StateFailed
			m.failed[p.Name()] = err
			_, required, _ := m.cfg.PluginFlags(p.Name())
			if required {
				return fmt.Errorf("必需的插件 %s 启动失败: %w", p.Name(), err)
			}
			continue
		}
		m.log.Info("插件已启动", "plugin", p.Name(), "version", p.Version())
	}
	// 被隔离的插件排在最后：运维看到的先后顺序仍是"能用的在前"。
	m.order = append(m.order, sortedNames(broken)...)
	return nil
}

// sortedNames 返回 map 的键并按字典序排序，保证输出稳定（便于复现与比对）。
func sortedNames(m map[string]error) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// isContractError 判断一个失败原因是否属于"插件与内核之间的契约问题"。
//
// 这类问题在 Load 阶段就被发现并隔离，且**不能**通过热启用绕过；
// 而 Init/Start 的偶发失败（如端口被占用）留给运维用热启用重试是合理的。
func isContractError(err error) bool {
	return errors.Is(err, ErrAPIVersionMismatch) ||
		errors.Is(err, ErrMissingDependency) ||
		errors.Is(err, ErrDependencyCycle)
}

// startLocked 完成一次"Init → Start → 起监听"，并把状态置为 enabled。调用方需持有 m.mu。
//
// Init / Start 各自只做一次：停用后再启用只是恢复监听，不是重新加载插件。
// 重复 Init 会让插件重复注册协议/路由，内核会直接以"重复注册"报错。
func (m *Manager) startLocked(p sdk.Plugin) error {
	name := p.Name()
	if !m.initialized[name] {
		if err := p.Init(m.hosts[name]); err != nil {
			m.log.Error("插件初始化失败，已隔离", "plugin", name, "err", err)
			return err
		}
		m.initialized[name] = true
	}
	if !m.started[name] {
		if err := p.Start(m.ctx); err != nil {
			m.log.Error("插件启动失败，已隔离", "plugin", name, "err", err)
			return err
		}
		m.started[name] = true
	}
	if m.listeners != nil {
		if err := m.listeners.StartListeners(name); err != nil {
			return err
		}
	}
	m.state[name] = sdk.StateEnabled
	return nil
}

// Enable 热启用一个插件（主要动作是恢复它的监听）。
func (m *Manager) Enable(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.plugins[name]
	if !ok {
		return fmt.Errorf("未找到插件 %s", name)
	}
	if m.state[name] == sdk.StateEnabled {
		return nil
	}
	// 契约问题（API 版本不符 / 依赖缺失 / 依赖成环）不能靠"再启一次"解决：
	// 这类插件在 Load 阶段就被隔离了，热启用只会让它带着坏契约跑起来。
	if err := m.failed[name]; err != nil && isContractError(err) {
		return fmt.Errorf("插件 %s 处于隔离状态（契约问题），无法热启用: %w", name, err)
	}
	// 之前被停用（或启动失败）的插件可能没有 Init 过：这里补一次完整启动。
	if err := m.startLocked(p); err != nil {
		m.state[name] = sdk.StateFailed
		m.failed[name] = err
		return err
	}
	delete(m.failed, name)
	m.log.Info("插件已热启用", "plugin", name)
	return nil
}

// Disable 热停用一个插件：关闭它名下的监听并标记为 disabled。
//
// 不做 p.Stop：设计 10.6 的"运行期启停"是**能力级**的（起停 listener / 挂载卸载路由），
// 而不是整个进程热替换；保留 Init 过的扩展点，才能让 Enable 立刻恢复服务。
func (m *Manager) Disable(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.plugins[name]; !ok {
		return fmt.Errorf("未找到插件 %s", name)
	}
	if m.state[name] == sdk.StateDisabled {
		return nil
	}
	if m.listeners != nil {
		if err := m.listeners.StopListeners(name); err != nil {
			return err
		}
	}
	m.state[name] = sdk.StateDisabled
	m.log.Info("插件已热停用", "plugin", name)
	return nil
}

// Stop 按启动的反序停止已启动的插件（内核退出时调用）。
func (m *Manager) Stop(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := len(m.order) - 1; i >= 0; i-- {
		name := m.order[i]
		if m.state[name] != sdk.StateEnabled {
			continue
		}
		if err := m.plugins[name].Stop(ctx); err != nil {
			m.log.Warn("插件停止失败", "plugin", name, "err", err)
		}
		m.state[name] = sdk.StateStopped
	}
}

// Failed 返回被隔离的插件及其失败原因。
func (m *Manager) Failed() map[string]error {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]error, len(m.failed))
	for k, v := range m.failed {
		out[k] = v
	}
	return out
}

// Plugins 返回全部插件的元数据与状态快照（管理面 `GET /api/plugins` 用）。
//
// 按依赖顺序输出：运维看到的下单顺序与内核实际加载顺序一致，排查依赖问题更直观。
func (m *Manager) Plugins() []sdk.Info {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sdk.Info, 0, len(m.order))
	for _, name := range m.order {
		out = append(out, m.infoLocked(name))
	}
	return out
}

// EnabledProtocols 返回当前**已启用**插件注册的协议，供接入层嗅探使用。
//
// 停用中的插件必须从嗅探候选里消失：否则它的协议仍会把别的端口上的连接接走，
// "停用"就变成了只关了端口、没停能力。
func (m *Manager) EnabledProtocols() []sdk.Protocol {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []sdk.Protocol
	for _, name := range m.order {
		if m.state[name] != sdk.StateEnabled {
			continue
		}
		out = append(out, m.reg.ProtocolsOf(name)...)
	}
	return out
}

// Plugin 返回单个插件的快照。
func (m *Manager) Plugin(name string) (sdk.Info, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.plugins[name]; !ok {
		return sdk.Info{}, false
	}
	return m.infoLocked(name), true
}

func (m *Manager) infoLocked(name string) sdk.Info {
	p := m.plugins[name]
	_, required, builtin := m.cfg.PluginFlags(name)
	state := m.state[name]
	if state == "" {
		state = sdk.StateDisabled
	}
	note := ""
	// 运行期状态由插件自报（拉模式，见 sdk.StateReporter）。
	// 只在"内核认为它正在服务"时才采纳：运维显式停用/停止的插件不该被自报状态覆盖。
	if state == sdk.StateEnabled {
		if r, ok := p.(sdk.StateReporter); ok {
			if runtimeState, reason := r.ReportState(); runtimeState != "" {
				state = runtimeState
				note = reason
			}
		}
	} else if state == sdk.StateFailed {
		// 失败/被隔离的原因也要"留痕"：否则运维只能看到 failed，不知道改哪里。
		if err := m.failed[name]; err != nil {
			note = err.Error()
		}
	}
	caps := make([]string, 0, len(p.Capabilities()))
	for _, c := range p.Capabilities() {
		caps = append(caps, string(c))
	}
	sort.Strings(caps)
	deps := append([]string(nil), p.Requires()...)
	sort.Strings(deps)

	desc := ""
	if d, ok := p.(sdk.Describer); ok {
		desc = d.Description()
	}
	return sdk.Info{
		Name:         name,
		Version:      p.Version(),
		APIVersion:   p.APIVersion(),
		State:        state,
		Required:     required,
		Builtin:      builtin,
		Capabilities: caps,
		Dependencies: deps,
		Description:  desc,
		RuntimeNote:  note,
	}
}
