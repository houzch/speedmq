// Package plugin 是内核侧的插件运行时：注册中心、生命周期管理、Host 句柄与能力审计。
//
// 与 pkg/plugin（对外稳定 API）的分工：本包只在内核内部使用，插件永远看不到它。
package plugin

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	sdk "github.com/swiftmq/swiftmq/pkg/plugin"
)

// 内核级契约错误。这类错误会让内核直接拒绝启动，而不是隔离单个插件。
var (
	// ErrAPIVersionMismatch 插件声明的 API 版本与内核不符。
	ErrAPIVersionMismatch = errors.New("插件 API 版本不匹配")
	// ErrDuplicatePlugin 插件重名。
	ErrDuplicatePlugin = errors.New("插件重名")
	// ErrMissingDependency 依赖的插件不存在。
	ErrMissingDependency = errors.New("插件依赖缺失")
	// ErrDependencyCycle 依赖成环。
	ErrDependencyCycle = errors.New("插件依赖成环")
	// ErrDuplicateProtocol 同一协议名被注册两次。
	ErrDuplicateProtocol = errors.New("协议名重复注册")
	// ErrCapabilityDenied 插件调用了未声明的能力。
	ErrCapabilityDenied = errors.New("插件未声明所需能力")
)

type protocolEntry struct {
	owner string
	proto sdk.Protocol
}

// Registry 是插件注册中心。
type Registry struct {
	mu        sync.RWMutex
	log       *slog.Logger
	plugins   map[string]sdk.Plugin
	protocols []protocolEntry
}

// NewRegistry 构造注册中心。
func NewRegistry(log *slog.Logger) *Registry {
	return &Registry{log: log, plugins: map[string]sdk.Plugin{}}
}

// Add 登记一个插件（只登记，不启动）。
func (r *Registry) Add(p sdk.Plugin) error {
	name := p.Name()
	if name == "" {
		return errors.New("插件名不能为空")
	}
	if v := p.APIVersion(); v != "" && v != sdk.APIVersion {
		return fmt.Errorf("%w: 插件 %s 声明 %s，内核支持 %s",
			ErrAPIVersionMismatch, name, v, sdk.APIVersion)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.plugins[name]; dup {
		return fmt.Errorf("%w: %s", ErrDuplicatePlugin, name)
	}
	r.plugins[name] = p
	return nil
}

// Resolve 返回按依赖顺序排列的插件列表（被依赖者在前），并做环检测。
//
// 顺序确定性：依赖名与被遍历的插件名均排序，保证相同输入产生相同顺序，便于复现问题。
func (r *Registry) Resolve() ([]sdk.Plugin, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	const (
		unvisited = 0
		visiting  = 1
		done      = 2
	)
	state := make(map[string]int, len(r.plugins))
	order := make([]sdk.Plugin, 0, len(r.plugins))

	var visit func(name string, path []string) error
	visit = func(name string, path []string) error {
		switch state[name] {
		case done:
			return nil
		case visiting:
			return fmt.Errorf("%w: %s", ErrDependencyCycle,
				strings.Join(append(path, name), " -> "))
		}
		p, ok := r.plugins[name]
		if !ok {
			return fmt.Errorf("%w: %s（被 %s 依赖）",
				ErrMissingDependency, name, strings.Join(path, " -> "))
		}
		state[name] = visiting
		deps := append([]string(nil), p.Requires()...)
		sort.Strings(deps)
		for _, dep := range deps {
			if err := visit(dep, append(path, name)); err != nil {
				return err
			}
		}
		state[name] = done
		order = append(order, p)
		return nil
	}

	names := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := visit(n, nil); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// Audit 返回能力审计行，供启动时打印（设计文档 10.7：未声明的能力一律拒绝）。
func (r *Registry) Audit() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		names = append(names, n)
	}
	sort.Strings(names)

	lines := make([]string, 0, len(names))
	for _, n := range names {
		p := r.plugins[n]
		caps := make([]string, 0, len(p.Capabilities()))
		for _, c := range p.Capabilities() {
			caps = append(caps, string(c))
		}
		sort.Strings(caps)
		lines = append(lines, fmt.Sprintf("插件 %s v%s（API %s）能力: [%s]",
			n, p.Version(), p.APIVersion(), strings.Join(caps, ", ")))
	}
	return lines
}

// registerProtocol 登记协议扩展点（由 host 在能力校验通过后调用）。
func (r *Registry) registerProtocol(owner string, p sdk.Protocol) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.protocols {
		if e.proto.Name() == p.Name() {
			return fmt.Errorf("%w: %s", ErrDuplicateProtocol, p.Name())
		}
	}
	r.protocols = append(r.protocols, protocolEntry{owner: owner, proto: p})
	return nil
}

// Protocols 按注册顺序返回协议插件。顺序即嗅探优先级，须保持稳定。
func (r *Registry) Protocols() []sdk.Protocol {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]sdk.Protocol, 0, len(r.protocols))
	for _, e := range r.protocols {
		out = append(out, e.proto)
	}
	return out
}

// ProtocolOwner 返回某个协议的归属插件名。
func (r *Registry) ProtocolOwner(name string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.protocols {
		if e.proto.Name() == name {
			return e.owner
		}
	}
	return ""
}
