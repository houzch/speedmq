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

	sdk "github.com/houzch/swiftmq/pkg/plugin"
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
//
// 这里只拦"无法二选一"的问题（空名、重名）；API 版本不匹配等**可隔离**的问题
// 留到 Resolve 阶段处理，这样才能做到"错误影响面仅限该插件"（对齐设计 10.10）。
func (r *Registry) Add(p sdk.Plugin) error {
	name := p.Name()
	if name == "" {
		return errors.New("插件名不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.plugins[name]; dup {
		return fmt.Errorf("%w: %s", ErrDuplicatePlugin, name)
	}
	r.plugins[name] = p
	return nil
}

// Resolve 返回"可加载的插件顺序"（被依赖者在前）以及**被隔离的插件及其原因**。
//
// 与"直接返回 error"的关键差别：API 版本不匹配、依赖缺失、依赖成环都**不再**让内核
// 拒绝启动，而是把受影响的那部分插件摘出去（对齐设计 10.10：错误信息明确且影响面仅限该插件）。
// 隔离会沿依赖链传播：A 被隔离后，所有（直接或间接）依赖 A 的插件也随之隔离 ——
// 否则它们会在运行期踩到"依赖不在"的空指针，那才是真正的影响外溢。
//
// 顺序确定性：依赖名与待遍历的插件名均排序，保证相同输入产生相同顺序。
func (r *Registry) Resolve() (order []sdk.Plugin, broken map[string]error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	broken = map[string]error{}
	names := make([]string, 0, len(r.plugins))
	for n := range r.plugins {
		names = append(names, n)
	}
	sort.Strings(names)

	// 1) API 版本不匹配：只隔离该插件。
	for _, n := range names {
		if v := r.plugins[n].APIVersion(); v != "" && v != sdk.APIVersion {
			broken[n] = fmt.Errorf("%w: 插件 %s 声明 %s，内核支持 %s",
				ErrAPIVersionMismatch, n, v, sdk.APIVersion)
		}
	}

	for {
		// 2) 依赖缺失 / 依赖已被隔离 → 隔离该插件，沿依赖链收敛到不动点。
		for changed := true; changed; {
			changed = false
			for _, n := range names {
				if _, bad := broken[n]; bad {
					continue
				}
				for _, dep := range r.plugins[n].Requires() {
					if _, ok := r.plugins[dep]; !ok {
						broken[n] = fmt.Errorf("%w: %s（被 %s 依赖）", ErrMissingDependency, dep, n)
						changed = true
						break
					}
					if depErr, bad := broken[dep]; bad {
						broken[n] = fmt.Errorf("依赖的插件 %s 已被隔离: %w", dep, depErr)
						changed = true
						break
					}
				}
			}
		}

		healthy := make([]string, 0, len(names))
		for _, n := range names {
			if _, bad := broken[n]; !bad {
				healthy = append(healthy, n)
			}
		}

		// 3) 健康子集上做拓扑排序；发现环就把环上的插件整体隔离，再回到步骤 2。
		ordered, cycle := topoOrder(r.plugins, healthy)
		if cycle == nil {
			return ordered, broken
		}
		path := append(append([]string(nil), cycle...), cycle[0])
		msg := strings.Join(path, " -> ")
		for _, n := range cycle {
			broken[n] = fmt.Errorf("%w: %s", ErrDependencyCycle, msg)
		}
	}
}

// topoOrder 对给定（已经健康的）插件名做拓扑排序，返回被依赖者在前的顺序。
//
// 发现环时返回环上的节点（按环序），调用方据此隔离。
func topoOrder(plugins map[string]sdk.Plugin, names []string) ([]sdk.Plugin, []string) {
	const (
		unvisited = 0
		visiting  = 1
		done      = 2
	)
	state := make(map[string]int, len(names))
	order := make([]sdk.Plugin, 0, len(names))
	var stack []string

	var visit func(n string) []string
	visit = func(n string) []string {
		switch state[n] {
		case done:
			return nil
		case visiting:
			for i, s := range stack {
				if s == n {
					return append([]string(nil), stack[i:]...)
				}
			}
			return []string{n}
		}
		state[n] = visiting
		stack = append(stack, n)
		deps := append([]string(nil), plugins[n].Requires()...)
		sort.Strings(deps)
		for _, dep := range deps {
			if _, ok := plugins[dep]; !ok {
				// 缺失依赖的插件已在收敛阶段被隔离，健康子集里不会出现；这里只是防御。
				continue
			}
			if cycle := visit(dep); cycle != nil {
				return cycle
			}
		}
		stack = stack[:len(stack)-1]
		state[n] = done
		order = append(order, plugins[n])
		return nil
	}

	for _, n := range names {
		if cycle := visit(n); cycle != nil {
			return nil, cycle
		}
	}
	return order, nil
}

// Audit 返回能力审计行，供启动时打印，便于确认每个插件实际被授予了哪些能力。
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

// ProtocolsOf 返回某个插件注册的全部协议（按注册顺序）。
//
// 用于"热停用某个插件的监听"：接入层按插件名找到它名下的监听集合。
func (r *Registry) ProtocolsOf(owner string) []sdk.Protocol {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []sdk.Protocol
	for _, e := range r.protocols {
		if e.owner == owner {
			out = append(out, e.proto)
		}
	}
	return out
}
