// Package plugin_test 从包外驱动插件运行时：只使用 internal/plugin 的导出 API，
// 断言落在"端口是否真的能连上"这类可观察行为上。
//
// 文件位置与包形式的约定见 AGENTS.md §10.1。
package plugin_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	pluginkit "github.com/houzch/swiftmq/internal/plugin"
	"github.com/houzch/swiftmq/internal/protocol/amqp091"
	"github.com/houzch/swiftmq/internal/transport"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 M5 的插件治理 DoD：
// 不重启内核即可启停一个协议插件的 listener（设计 10.10 / 10.6）。
//
// 断言必须落在"端口是否真的能连上"，而不是"状态字段变成了 disabled"——
// 后者很容易做到，前者才是运维真正关心的。

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stubCore 是 plugin.Core 的最小替身：本文件的用例只关心监听与插件状态，不涉及内核语义。
type stubCore struct{}

func (stubCore) Logger() *slog.Logger                         { return testLogger() }
func (stubCore) Notifications() <-chan sdk.Notification       { return make(chan sdk.Notification) }
func (stubCore) Close()                                       {}
func (stubCore) SetConnectionProbe(func() sdk.ConnectionInfo) {}
func (stubCore) SetDisconnectFunc(func(reason string))        {}
func (stubCore) ServerProperties() map[string]any             { return map[string]any{} }
func (stubCore) Mechanisms() []string                         { return []string{"PLAIN"} }
func (stubCore) Authenticate(context.Context, string, []byte, net.Addr) (sdk.Identity, error) {
	return sdk.Identity{}, nil
}
func (stubCore) VHostExists(string) bool { return true }
func (stubCore) DefaultVHost() string    { return "/" }
func (stubCore) Session(string) (sdk.Session, error) {
	return nil, errors.New("stub 不提供会话")
}

// testConfig 通过真实配置文件注入治理开关，覆盖"配置文件 → 治理开关"这条链路。
//
// 数据目录指向测试专属临时目录：绝不写进仓库工作区。
func testConfig(t *testing.T, pluginsJSON string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "swiftmqd.json")
	body := `{"data_dir": "` + filepath.ToSlash(dir) + `", "listeners": {"amqp091": [{"addr": "127.0.0.1:0"}]}, "plugins": ` + pluginsJSON + `}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写测试配置失败: %v", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("加载测试配置失败: %v", err)
	}
	return cfg
}

// newGovernanceEnv 装配 registry + manager + transport。
// 监听地址用 127.0.0.1:0，由内核分配空闲端口，避免与开发机上的 5672 冲突。
func newGovernanceEnv(t *testing.T, cfg *config.Config) (*pluginkit.Manager, *transport.Server) {
	t.Helper()
	log := testLogger()
	reg := pluginkit.NewRegistry(log)
	manager := pluginkit.NewManager(reg, cfg, log)
	server := transport.New(log, manager.EnabledProtocols,
		func(remote, local net.Addr) sdk.Core { return stubCore{} })
	manager.SetListenerController(pluginkit.NewListenerController(context.Background(), server, reg, cfg))

	t.Cleanup(func() { server.Shutdown(context.Background()) })
	return manager, server
}

// TestPluginHotDisableEnable 覆盖"停用 → 端口关闭 → 启用 → 端口恢复"的完整闭环。
func TestPluginHotDisableEnable(t *testing.T) {
	manager, server := newGovernanceEnv(t, testConfig(t, "{}"))
	if err := manager.Load(context.Background(), amqp091.New()); err != nil {
		t.Fatalf("加载插件失败: %v", err)
	}

	addr := listenerAddr(t, server)
	if !canConnect(addr) {
		t.Fatalf("插件启用后应能连上 %s", addr)
	}
	if got := len(manager.EnabledProtocols()); got != 1 {
		t.Fatalf("启用状态下参与嗅探的协议数应为 1，实际 %d", got)
	}

	// 停用：端口必须立即关闭，嗅探候选也必须摘掉
	if err := manager.Disable("amqp091"); err != nil {
		t.Fatalf("停用插件失败: %v", err)
	}
	if canConnect(addr) {
		t.Fatalf("插件停用后 %s 仍可连接（端口没有真的关闭）", addr)
	}
	if got := len(manager.EnabledProtocols()); got != 0 {
		t.Fatalf("停用后参与嗅探的协议数应为 0，实际 %d", got)
	}
	info, ok := manager.Plugin("amqp091")
	if !ok || info.State != sdk.StateDisabled {
		t.Fatalf("停用后状态应为 disabled，实际 %+v", info)
	}

	// 重新启用：端口恢复
	if err := manager.Enable("amqp091"); err != nil {
		t.Fatalf("启用插件失败: %v", err)
	}
	info, _ = manager.Plugin("amqp091")
	if info.State != sdk.StateEnabled {
		t.Fatalf("启用后状态应为 enabled，实际 %+v", info)
	}
	if !canConnect(listenerAddr(t, server)) {
		t.Fatalf("插件重新启用后端口未恢复")
	}

	// 幂等：重复停用/启用不应报错
	if err := manager.Disable("amqp091"); err != nil {
		t.Fatalf("重复停用应幂等: %v", err)
	}
	if err := manager.Disable("amqp091"); err != nil {
		t.Fatalf("重复停用应幂等: %v", err)
	}
	if err := manager.Enable("amqp091"); err != nil {
		t.Fatalf("重复启用应幂等: %v", err)
	}

	// 不存在的插件：明确报错
	if err := manager.Disable("no-such-plugin"); err == nil {
		t.Fatalf("停用不存在的插件应报错")
	}
}

// TestPluginDisabledByConfig 覆盖"配置里显式停用"：启动时不建监听、扩展点也不注册。
func TestPluginDisabledByConfig(t *testing.T) {
	manager, server := newGovernanceEnv(t, testConfig(t, `{"amqp091": {"enabled": false}}`))
	if err := manager.Load(context.Background(), amqp091.New()); err != nil {
		t.Fatalf("加载插件失败: %v", err)
	}
	if got := len(server.Listeners()); got != 0 {
		t.Fatalf("配置停用的插件不应创建监听，实际 %d 个", got)
	}
	info, _ := manager.Plugin("amqp091")
	if info.State != sdk.StateDisabled {
		t.Fatalf("配置停用后状态应为 disabled，实际 %+v", info)
	}

	// 运行期仍然可以被启用（这是运维的救命通道：配置写错不必改文件重启）
	if err := manager.Enable("amqp091"); err != nil {
		t.Fatalf("启用配置停用的插件失败: %v", err)
	}
	if got := len(server.Listeners()); got != 1 {
		t.Fatalf("启用后应恰好有 1 个监听，实际 %d", got)
	}
}

// TestRequiredPluginFailureBlocksStartup 覆盖 required 语义：
// 声明 required 的插件启动失败必须阻塞内核启动（其余情况只做隔离）。
func TestRequiredPluginFailureBlocksStartup(t *testing.T) {
	manager, _ := newGovernanceEnv(t, testConfig(t, `{"broken": {"required": true}}`))
	if err := manager.Load(context.Background(), &brokenPlugin{}); err == nil {
		t.Fatalf("required 插件启动失败应阻塞启动")
	}

	// 非 required：只隔离，不返回错误
	manager2, _ := newGovernanceEnv(t, testConfig(t, "{}"))
	if err := manager2.Load(context.Background(), &brokenPlugin{}); err != nil {
		t.Fatalf("非 required 插件失败不应阻塞启动: %v", err)
	}
	if _, ok := manager2.Failed()["broken"]; !ok {
		t.Fatalf("失败的插件应被记录在 Failed() 中")
	}
	info, _ := manager2.Plugin("broken")
	if info.State != sdk.StateFailed {
		t.Fatalf("失败插件状态应为 failed，实际 %+v", info)
	}
}

// brokenPlugin 是一个 Init 必定失败的插件（用于覆盖失败隔离与 required 语义）。
type brokenPlugin struct{}

func (p *brokenPlugin) Name() string                   { return "broken" }
func (p *brokenPlugin) Version() string                { return "0.0.1" }
func (p *brokenPlugin) APIVersion() string             { return sdk.APIVersion }
func (p *brokenPlugin) Requires() []string             { return nil }
func (p *brokenPlugin) Capabilities() []sdk.Capability { return nil }
func (p *brokenPlugin) Init(sdk.Host) error            { return errors.New("故意失败") }
func (p *brokenPlugin) Start(context.Context) error    { return nil }
func (p *brokenPlugin) Stop(context.Context) error     { return nil }

// listenerAddr 返回接入层当前唯一监听的地址。
func listenerAddr(t *testing.T, server *transport.Server) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ls := server.Listeners()
		if len(ls) > 0 {
			return ls[0].Addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("接入层没有已启动的监听")
	return ""
}

// canConnect 尝试建立一次 TCP 连接（带短超时，避免测试卡住）。
func canConnect(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}
