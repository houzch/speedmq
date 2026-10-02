// 本文件覆盖 M8-7「vhost 动态增删」的内核语义：
// 运行期创建/删除 vhost、删除的级联与磁盘回收、默认 vhost 的保护，
// 以及"运行期新建的 vhost 必须能扛住重启"（元数据为准，配置只做首次引导）。
package broker_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// newVHostEnv 返回内核、它的配置与数据目录。
//
// 单独造配置（而不是复用 newTestBroker）是因为持久化断言要知道数据目录，
// 重启用例也要能用同一份配置再建一个内核。
func newVHostEnv(t *testing.T) (*broker.Broker, *config.Config, string) {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载默认配置失败: %v", err)
	}
	dir := t.TempDir()
	cfg.DataDir = dir
	b := mustBroker(t, cfg)
	t.Cleanup(b.Close)
	return b, cfg, dir
}

// vhostDataDir 返回某 vhost 的消息存储目录。
func vhostDataDir(dataDir, vhost string) string {
	return filepath.Join(dataDir, "msg_stores", "vhosts", store.SafeDirName(vhost))
}

// TestVHostLifecycle 覆盖运行期增删的基本契约：创建、幂等重复创建、删除、删除后不可见。
func TestVHostLifecycle(t *testing.T) {
	b, _, _ := newVHostEnv(t)

	if !b.VHostExists("/") {
		t.Fatalf("默认 vhost 必须存在")
	}
	if b.VHostExists("v1") {
		t.Fatalf("v1 不应在创建前存在")
	}

	if err := b.CreateVHost("v1"); err != nil {
		t.Fatalf("创建 vhost 失败: %v", err)
	}
	if !b.VHostExists("v1") {
		t.Fatalf("创建后 v1 应存在")
	}
	if !containsStr(b.VHostNames(), "v1") {
		t.Fatalf("v1 应出现在 vhost 列表里: %v", b.VHostNames())
	}

	// 幂等：重复创建不报错，也不改变结果
	if err := b.CreateVHost("v1"); err != nil {
		t.Fatalf("重复创建 vhost 应成功: %v", err)
	}

	hit, err := b.DeleteVHost("v1")
	if err != nil {
		t.Fatalf("删除 vhost 失败: %v", err)
	}
	if !hit {
		t.Fatalf("删除已存在的 vhost 应返回命中")
	}
	if b.VHostExists("v1") {
		t.Fatalf("删除后 v1 不应存在")
	}
	if containsStr(b.VHostNames(), "v1") {
		t.Fatalf("删除后 v1 不应出现在列表里: %v", b.VHostNames())
	}

	// 再删一次：不存在就是"未命中"，不是静默成功
	hit, err = b.DeleteVHost("v1")
	if err != nil || hit {
		t.Fatalf("删除不存在的 vhost 应返回 (false, nil)，实际 (%v, %v)", hit, err)
	}
}

// TestVHostEmptyNameRejected 空名字直接拒绝：否则会造出一个无法访问的 vhost。
func TestVHostEmptyNameRejected(t *testing.T) {
	b, _, _ := newVHostEnv(t)
	if err := b.CreateVHost("   "); err == nil {
		t.Fatalf("空白 vhost 名应被拒绝")
	}
}

// TestVHostDeleteDefaultRefused 覆盖安全约束：默认 vhost 不可删除。
//
// 它是内核保证存在的连接落点（New 里无条件建立），删掉会让所有使用默认 vhost
// 的客户端立刻失联，因此这里明确拒绝而不是"删了再说"。
func TestVHostDeleteDefaultRefused(t *testing.T) {
	b, _, _ := newVHostEnv(t)
	hit, err := b.DeleteVHost("/")
	if err == nil {
		t.Fatalf("删除默认 vhost 应报错")
	}
	if hit {
		t.Fatalf("删除默认 vhost 不应返回命中")
	}
	if !strings.Contains(err.Error(), "PRECONDITION_FAILED") {
		t.Fatalf("错误应带 PRECONDITION_FAILED，实际 %v", err)
	}
	if !b.VHostExists("/") {
		t.Fatalf("默认 vhost 必须仍然存在")
	}
}

// TestVHostDeleteCascadesAndReclaimsStorage 覆盖删除的级联语义：
// vhost 内的队列 / 交换机 / 绑定 / 权限 / 策略一并清掉（元数据里不留悬空记录），
// 磁盘上的消息存储目录也要回收 —— 否则"删了 vhost 磁盘却不降"会成为长期隐患。
func TestVHostDeleteCascadesAndReclaimsStorage(t *testing.T) {
	b, _, dataDir := newVHostEnv(t)

	if err := b.CreateVHost("v1"); err != nil {
		t.Fatalf("创建 vhost 失败: %v", err)
	}
	// 新 vhost 上没有 guest 的权限记录，先授权（顺带覆盖"权限随 vhost 一起删"）。
	if err := b.SetPermission("guest", "v1", ".*", ".*", ".*"); err != nil {
		t.Fatalf("授权失败: %v", err)
	}

	sess, err := b.SessionFor("guest", "v1")
	if err != nil {
		t.Fatalf("打开 v1 上的会话失败: %v", err)
	}
	defer sess.Close()
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "q1", Durable: true}); err != nil {
		t.Fatalf("在 v1 上声明 durable 队列失败: %v", err)
	}
	if _, err := sess.Publish(&plugin.Message{
		Properties: plugin.Properties{DeliveryMode: 2},
		Body:       []byte("persisted"),
	}, "", "q1", false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}

	dir := vhostDataDir(dataDir, "v1")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("durable 队列应已建好存储目录 %s: %v", dir, err)
	}

	if _, err := b.DeleteVHost("v1"); err != nil {
		t.Fatalf("删除 vhost 失败: %v", err)
	}

	if b.VHostExists("v1") {
		t.Fatalf("删除后 v1 不应存在")
	}
	// 级联的元数据清理：不能留下指向已删 vhost 的悬空记录
	for _, q := range b.QueueSnapshots("v1") {
		t.Fatalf("v1 的队列应被删除，仍有 %s", q.Name)
	}
	for _, ex := range b.ExchangeSnapshots("v1") {
		t.Fatalf("v1 的交换机应被删除，仍有 %s", ex.Name)
	}
	for _, p := range b.VHostPermissions("v1") {
		t.Fatalf("v1 的权限应被删除，仍有 %s", p.User)
	}
	for _, p := range b.VHostPolicies("v1") {
		t.Fatalf("v1 的策略应被删除，仍有 %s", p.Name)
	}

	// 磁盘回收
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("vhost 存储目录应被删除，实际 err=%v", err)
	}
}

// TestVHostDeleteDisconnectsSessions 覆盖删除时的连接收敛：
// 打开着该 vhost 的连接必须被服务端主动断开，否则会话还持有已摘除的 vhost，
// 能在被删的拓扑上继续操作（数据会写进一个"已经不存在"的 vhost）。
func TestVHostDeleteDisconnectsSessions(t *testing.T) {
	b, _, _ := newVHostEnv(t)

	if err := b.CreateVHost("v1"); err != nil {
		t.Fatalf("创建 vhost 失败: %v", err)
	}
	if err := b.SetPermission("guest", "v1", ".*", ".*", ".*"); err != nil {
		t.Fatalf("授权失败: %v", err)
	}

	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40001}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := b.NewSession(remote, local)
	resp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	if _, err := core.Session("v1"); err != nil {
		t.Fatalf("打开 v1 会话失败: %v", err)
	}

	var disconnected atomic.Bool
	var reason atomic.Value
	core.SetDisconnectFunc(func(r string) {
		disconnected.Store(true)
		reason.Store(r)
	})

	if _, err := b.DeleteVHost("v1"); err != nil {
		t.Fatalf("删除 vhost 失败: %v", err)
	}
	if !disconnected.Load() {
		t.Fatalf("删除 vhost 后应断开使用它的连接")
	}
	if r, _ := reason.Load().(string); !strings.Contains(r, "v1") {
		t.Fatalf("断开原因应指明被删的 vhost，实际 %q", r)
	}
}

// TestVHostSurvivesRestart 覆盖"配置只做首次引导、此后以元数据为准"：
// 运行期新建的 vhost 必须能随重启重建，即使配置里没有它。
func TestVHostSurvivesRestart(t *testing.T) {
	b, cfg, _ := newVHostEnv(t)
	if err := b.CreateVHost("vpersist"); err != nil {
		t.Fatalf("创建 vhost 失败: %v", err)
	}
	b.Close()

	// 同一份配置（vhosts 里只有 "/"）重开：vpersist 只能来自元数据。
	b2 := mustBroker(t, cfg)
	defer b2.Close()
	if !b2.VHostExists("vpersist") {
		t.Fatalf("运行期创建的 vhost 应从元数据恢复，实际 vhost 列表: %v", b2.VHostNames())
	}
	if !b2.VHostExists("/") {
		t.Fatalf("默认 vhost 必须始终存在")
	}
}

// TestNewVHostUsableByAdministrator 覆盖新建 vhost 的可用性：
// administrator 标签的用户对新 vhost 具备完全权限，无需先写权限记录
// （实测 RabbitMQ 如此：给新 vhost 发布消息返回 200）；无权限的普通用户仍被拒绝。
func TestNewVHostUsableByAdministrator(t *testing.T) {
	b, _, _ := newVHostEnv(t)
	if err := b.CreateVHost("vadmin"); err != nil {
		t.Fatalf("创建 vhost 失败: %v", err)
	}

	sess, err := b.SessionFor("guest", "vadmin")
	if err != nil {
		t.Fatalf("administrator 应能直接使用新建的 vhost: %v", err)
	}
	defer sess.Close()
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "q1", Durable: true}); err != nil {
		t.Fatalf("在新建 vhost 上声明队列失败: %v", err)
	}

	// 非管理员、且在该 vhost 上没有权限记录 → 拒绝
	if err := b.UpsertUser("ops", "ops-pass", []string{"management"}); err != nil {
		t.Fatalf("创建管理用户失败: %v", err)
	}
	if _, err := b.SessionFor("ops", "vadmin"); err == nil {
		t.Fatalf("无权限的普通用户不应能访问该 vhost")
	}
}

func containsStr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
