// Package plugin_test 的第二个文件：覆盖设计 §10.10 的插件 DoD。
//
// 重点不在"代码有没有返回错误"，而在**影响面**：坏插件必须被隔离，
// 好插件与内核必须照常工作，且原因要在管理面留痕（否则运维无从下手）。
package plugin_test

import (
	"context"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/houzch/swiftmq/internal/config"
	pluginkit "github.com/houzch/swiftmq/internal/plugin"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// stubPlugin 是可控的插件替身：可以指定 API 版本、依赖、能力与 Init 行为。
type stubPlugin struct {
	name     string
	api      string
	deps     []string
	caps     []sdk.Capability
	protocol string // 非空表示 Init 时注册同名协议
}

func (p *stubPlugin) Name() string    { return p.name }
func (p *stubPlugin) Version() string { return "stub-0.0.1" }
func (p *stubPlugin) APIVersion() string {
	if p.api == "" {
		return sdk.APIVersion
	}
	return p.api
}
func (p *stubPlugin) Requires() []string             { return p.deps }
func (p *stubPlugin) Capabilities() []sdk.Capability { return p.caps }
func (p *stubPlugin) Start(context.Context) error    { return nil }
func (p *stubPlugin) Stop(context.Context) error     { return nil }
func (p *stubPlugin) Init(h sdk.Host) error {
	if p.protocol != "" {
		return h.RegisterProtocol(&stubProtocol{name: p.protocol})
	}
	return nil
}

// stubProtocol 是最小的协议扩展点（只用于让内核建立/拆除一个监听）。
type stubProtocol struct{ name string }

func (p *stubProtocol) Name() string { return p.name }
func (p *stubProtocol) DefaultListeners() []sdk.ListenerSpec {
	return []sdk.ListenerSpec{{Name: p.name, Addr: "127.0.0.1:0"}}
}
func (p *stubProtocol) Sniff([]byte) bool                               { return false }
func (p *stubProtocol) Serve(context.Context, net.Conn, sdk.Core) error { return nil }

// loadEnv 构造一套治理环境并 Load 给定插件。
func loadEnv(t *testing.T, cfg *config.Config, plugins ...sdk.Plugin) *pluginkit.Manager {
	t.Helper()
	manager, _ := newGovernanceEnv(t, cfg)
	if err := manager.Load(context.Background(), plugins...); err != nil {
		t.Fatalf("Load 不应因单个插件的问题而失败: %v", err)
	}
	return manager
}

// assertIsolated 断言某插件被隔离（failed + 原因含 wantSub），且 good 插件仍在服务。
func assertIsolated(t *testing.T, m *pluginkit.Manager, bad, good, wantSub string) {
	t.Helper()
	info, ok := m.Plugin(bad)
	if !ok {
		t.Fatalf("被隔离的插件 %s 也应出现在管理面（否则运维看不到它）", bad)
	}
	if info.State != sdk.StateFailed {
		t.Fatalf("插件 %s 状态应为 failed，实际 %q", bad, info.State)
	}
	if !strings.Contains(info.RuntimeNote, wantSub) {
		t.Fatalf("插件 %s 的失败原因应含 %q（留痕），实际 %q", bad, wantSub, info.RuntimeNote)
	}
	if gi, ok := m.Plugin(good); !ok || gi.State != sdk.StateEnabled {
		t.Fatalf("好插件 %s 应不受影响（enabled），实际 %+v", good, gi)
	}
}

// TestAPIVersionMismatchIsolatedOnly 覆盖：API 版本不匹配只隔离该插件。
func TestAPIVersionMismatchIsolatedOnly(t *testing.T) {
	m := loadEnv(t, testConfig(t, "{}"),
		&stubPlugin{name: "good", caps: []sdk.Capability{sdk.CapNetListen}, protocol: "good-proto"},
		&stubPlugin{name: "old-plugin", api: "v0"},
	)
	assertIsolated(t, m, "old-plugin", "good", "API 版本不匹配")

	// 契约问题不能靠热启用绕过。
	if err := m.Enable("old-plugin"); err == nil {
		t.Fatalf("API 版本不符的插件不应能被热启用")
	}
}

// TestMissingDependencyIsolatedOnly 覆盖：依赖缺失只隔离该插件。
func TestMissingDependencyIsolatedOnly(t *testing.T) {
	m := loadEnv(t, testConfig(t, "{}"),
		&stubPlugin{name: "good", caps: []sdk.Capability{sdk.CapNetListen}, protocol: "good-proto"},
		&stubPlugin{name: "needy", deps: []string{"ghost"}},
	)
	assertIsolated(t, m, "needy", "good", "依赖缺失")
}

// TestDependencyCycleIsolatedOnly 覆盖：依赖成环把环上插件一起隔离，其余不受影响。
func TestDependencyCycleIsolatedOnly(t *testing.T) {
	m := loadEnv(t, testConfig(t, "{}"),
		&stubPlugin{name: "good", caps: []sdk.Capability{sdk.CapNetListen}, protocol: "good-proto"},
		&stubPlugin{name: "cyc-a", deps: []string{"cyc-b"}},
		&stubPlugin{name: "cyc-b", deps: []string{"cyc-a"}},
	)
	assertIsolated(t, m, "cyc-a", "good", "依赖成环")
	assertIsolated(t, m, "cyc-b", "good", "依赖成环")
}

// TestIsolationPropagatesAlongDependencyChain 覆盖：隔离沿依赖链传播，
// 避免"依赖者以为自己能用、运行期才崩"的隐蔽故障。
func TestIsolationPropagatesAlongDependencyChain(t *testing.T) {
	m := loadEnv(t, testConfig(t, "{}"),
		&stubPlugin{name: "good", caps: []sdk.Capability{sdk.CapNetListen}, protocol: "good-proto"},
		&stubPlugin{name: "parent", api: "v9"},
		&stubPlugin{name: "child", deps: []string{"parent"}},
	)
	assertIsolated(t, m, "child", "good", "parent")
	// 依赖链上的隔离原因要能一眼看出根因在 parent。
	info, _ := m.Plugin("child")
	if !strings.Contains(info.RuntimeNote, "隔离") {
		t.Fatalf("依赖被隔离的插件，其原因应指明上游被隔离，实际 %q", info.RuntimeNote)
	}
}

// TestCapabilityDeniedIsRejectedAndTraced 覆盖 DoD：能力声明与实际调用不符时，
// 调用被拒绝、插件被隔离、原因留痕，且内核与其余插件照常。
func TestCapabilityDeniedIsRejectedAndTraced(t *testing.T) {
	// naked 没有声明 net.listen，却在 Init 里注册协议：必须被拒。
	m := loadEnv(t, testConfig(t, "{}"),
		&stubPlugin{name: "good", caps: []sdk.Capability{sdk.CapNetListen}, protocol: "good-proto"},
		&stubPlugin{name: "naked", protocol: "naked-proto"},
	)
	assertIsolated(t, m, "naked", "good", "未声明所需能力")

	// 被拒绝的协议绝不能进入嗅探候选（否则"拒绝"只是嘴上说说）。
	for _, p := range m.EnabledProtocols() {
		if p.Name() == "naked-proto" {
			t.Fatalf("被拒绝的插件不应注册出可用协议")
		}
	}
}

// TestKernelDoesNotDependOnProtocolPlugins 覆盖 DoD 第一条的一半：
// **内核层不得直接依赖任何协议插件包** —— 这正是"关掉/移除任意协议插件后，
// 内核仍能编译通过、启动正常"的静态证据。
//
// 为什么只查"协议插件包"而不是整个 internal/protocol/**：
// `internal/protocol/codec` 是**无业务语义**的线格式工具（基础类型 / field-table），
// 认证（AMQPLAIN）等内核组件合法地复用它；真正要挡住的是"内核知道某种具体协议"。
func TestKernelDoesNotDependOnProtocolPlugins(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("本机没有 go 可执行文件，跳过依赖方向检查")
	}
	// 内核层：这些包一旦依赖协议插件，"关插件内核仍可编译"就不再成立。
	kernelPkgs := []string{
		"./internal/broker",
		"./internal/store",
		"./internal/meta",
		"./internal/raft",
		"./internal/transport",
		"./internal/management",
		"./internal/plugin/...",
	}
	forbidden := []string{
		"swiftmq/internal/protocol/amqp091",
		"swiftmq/internal/protocol/mqtt",
		"swiftmq/internal/protocol/spec",
	}

	args := append([]string{"list", "-f", `{{.ImportPath}}|{{join .Imports " "}}`}, kernelPkgs...)
	cmd := exec.Command("go", args...)
	cmd.Dir = filepath.Join("..", "..", "..")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list 失败: %v", err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pkgPath, imports, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}
		for _, imp := range strings.Fields(imports) {
			for _, bad := range forbidden {
				if imp == bad {
					t.Fatalf("内核层包 %s 不得依赖协议插件包 %s（关掉该插件内核就编译不过了）", pkgPath, bad)
				}
			}
		}
	}
}
