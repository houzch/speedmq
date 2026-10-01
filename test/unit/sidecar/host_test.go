// 本文件覆盖内核侧的 sidecar 宿主插件（internal/plugin/sidecar）：
// 它把外部进程包装成一个普通 plugin.Plugin，因此"崩溃后显示 down、恢复后回到 enabled"
// 必须能从 Host 的公开接口观察到（对齐设计 §10.10 的 B 形态 DoD）。
package sidecar_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	sidecarhost "github.com/houzch/swiftmq/internal/plugin/sidecar"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newServer 起一个插件侧 Server 并返回它（供用例主动"杀掉"以复现崩溃）。
func newServer(t *testing.T, h sidecar.Handler) (*sidecar.Server, string) {
	t.Helper()
	srv, err := sidecar.NewServer(h, sidecar.ServerOptions{Address: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("启动测试 Server 失败: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = srv.Close()
		<-done
	})
	return srv, srv.Address()
}

// fakeHost 是 sdk.Host 的最小替身。
type fakeHost struct{ log *slog.Logger }

func (h fakeHost) PluginName() string                  { return "echo-sidecar" }
func (h fakeHost) Logger() *slog.Logger                { return h.log }
func (h fakeHost) Config(any) error                    { return nil }
func (h fakeHost) RegisterProtocol(sdk.Protocol) error { return nil }

// sidecarConfig 通过真实配置文件注入 sidecar 段，覆盖"配置 → 宿主插件"这条链路。
func sidecarConfig(t *testing.T, address string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "swiftmqd.json")
	plugins := fmt.Sprintf(`{
      "echo-sidecar": {
        "builtin": false,
        "enabled": true,
        "sidecar": {
          "address": %q,
          "restart": "always",
          "heartbeat_seconds": 1,
          "protocols": [
            {"name": "echo", "prefix": "ECHO",
             "listeners": [{"name": "echo", "addr": "127.0.0.1:0"}]}
          ]
        }
      }
    }`, address)
	body := `{"data_dir": "` + filepath.ToSlash(dir) + `", "plugins": ` + plugins + `}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写测试配置失败: %v", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("加载测试配置失败: %v", err)
	}
	return cfg
}

// clienter 暴露宿主插件当前的连接（测试用；生产代码里只有协议代理会用到）。
type clienter interface{ Client() *sidecar.Client }

// startHost 构造并启动一个 sidecar 宿主插件，返回插件与其状态报告器。
func startHost(t *testing.T, cfg *config.Config) (sdk.Plugin, sdk.StateReporter, clienter) {
	t.Helper()
	plugs, err := sidecarhost.FromConfig(cfg, testLogger(), "test-kernel")
	if err != nil {
		t.Fatalf("解析 sidecar 配置失败: %v", err)
	}
	if len(plugs) != 1 {
		t.Fatalf("应解析出 1 个 sidecar 插件，实际 %d", len(plugs))
	}
	p := plugs[0]
	if err := p.Init(fakeHost{log: testLogger()}); err != nil {
		t.Fatalf("宿主插件 Init 失败: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("宿主插件 Start 失败: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })

	reporter, ok := p.(sdk.StateReporter)
	if !ok {
		t.Fatalf("宿主插件应实现 sdk.StateReporter")
	}
	cl, ok := p.(clienter)
	if !ok {
		t.Fatalf("宿主插件应提供 Client()")
	}
	return p, reporter, cl
}

// waitState 轮询等待插件自报状态变为 want。
func waitState(t *testing.T, r sdk.StateReporter, want sdk.State) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last sdk.State
	var note string
	for time.Now().Before(deadline) {
		last, note = r.ReportState()
		if last == want {
			return note
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待状态 %q 超时，当前为 %q（%s）", want, last, note)
	return ""
}

// TestHostEnabledOnStart 覆盖：能连上插件进程时，宿主对外报告 enabled。
func TestHostEnabledOnStart(t *testing.T) {
	h := &testHandler{name: "echo-sidecar", prefix: "ECHO"}
	_, addr := newServer(t, h)
	_, reporter, _ := startHost(t, sidecarConfig(t, addr))
	waitState(t, reporter, sdk.StateEnabled)
}

// TestHostMarksDownWhenProcessGone 覆盖 DoD：插件进程消失后内核无感，但状态明确变为 down。
func TestHostMarksDownWhenProcessGone(t *testing.T) {
	h := &testHandler{name: "echo-sidecar"}
	srv, addr := newServer(t, h)
	_, reporter, _ := startHost(t, sidecarConfig(t, addr))
	waitState(t, reporter, sdk.StateEnabled)

	// 杀掉插件进程（关闭它的监听与所有连接）。
	if err := srv.Close(); err != nil {
		t.Fatalf("关闭测试 Server 失败: %v", err)
	}
	note := waitState(t, reporter, sdk.StateDown)
	if note == "" {
		t.Fatalf("down 状态应带上一句原因（便于运维定位）")
	}

	// 再观察一段时间：插件没回来之前必须**一直**是 down，不能假装还行。
	time.Sleep(1200 * time.Millisecond)
	if st, _ := reporter.ReportState(); st != sdk.StateDown {
		t.Fatalf("插件仍不可用，状态不应变为 %q", st)
	}
}

// TestHostRecoversAfterReconnect 覆盖 DoD 的"可重启重连"：
// 连接断开后宿主按退避策略自动重连，插件回来时状态回到 enabled。
func TestHostRecoversAfterReconnect(t *testing.T) {
	h := &testHandler{name: "echo-sidecar", prefix: "ECHO"}
	_, addr := newServer(t, h)
	_, reporter, cl := startHost(t, sidecarConfig(t, addr))
	waitState(t, reporter, sdk.StateEnabled)

	// 只断开当前这条连接（插件进程仍在监听），模拟"链路抖动"。
	if err := cl.Client().Close(); err != nil {
		t.Fatalf("关闭连接失败: %v", err)
	}
	waitState(t, reporter, sdk.StateDown)
	// 自动重连：不需要外部干预就应恢复到 enabled。
	waitState(t, reporter, sdk.StateEnabled)
}

// TestHostRejectsMissingAddress 覆盖配置校验：声明了 sidecar 却没写 address 必须明确报错。
func TestHostRejectsMissingAddress(t *testing.T) {
	cfg := sidecarConfig(t, "")
	if _, err := sidecarhost.FromConfig(cfg, testLogger(), "test-kernel"); err == nil {
		t.Fatalf("缺少 address 的 sidecar 配置应被拒绝")
	}
}
