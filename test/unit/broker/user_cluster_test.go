package broker_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
)

// 本文件覆盖 M8-4：账号与权限的集群复制与重启保留。
//
// 断言口径与 M6 拓扑一致 —— 看的是"在别的节点上能不能真的用这个账号登录、
// 权限是不是生效"，而不是"内存里的 map 有没有那个键"。

// loopbackAddr 是一个本机来源地址：内置账号的 remote_access 限制对本机放行。
func loopbackAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40000}
}

// hasPermission 判断某节点上是否存在匹配的权限记录。
func hasPermission(b *broker.Broker, user, vhost, configure, write, read string) bool {
	for _, p := range b.UserPermissions(user) {
		if p.VHost == vhost && p.Configure == configure && p.Write == write && p.Read == read {
			return true
		}
	}
	return false
}

// TestUserAndPermissionReplicateAcrossCluster 在**非 leader** 节点上增删账号与权限，
// 断言最终在所有节点上一致。
func TestUserAndPermissionReplicateAcrossCluster(t *testing.T) {
	nodes := startTestCluster(t, 3)
	leader := waitClusterLeader(t, nodes)

	followers := make([]*clusterNode, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.id != leader.id {
			followers = append(followers, node)
		}
	}
	first, second := followers[0], followers[1]

	// 建账号：请求落在 follower 上，要经 leader 提交再复制回全体。
	if err := first.b.UpsertUser("alice", "secret", []string{"management"}); err != nil {
		t.Fatalf("在 follower %s 上创建用户失败: %v", first.id, err)
	}
	waitFor(t, 5*time.Second, "账号复制到全部节点", func() bool {
		for _, node := range nodes {
			if _, ok := node.b.User("alice"); !ok {
				return false
			}
		}
		return true
	})
	// 口令也必须是复制过来的那一份：走真实认证路径，而不是读字段。
	for _, node := range nodes {
		if _, err := node.b.VerifyUser("alice", "secret", loopbackAddr()); err != nil {
			t.Fatalf("节点 %s 上 alice 用复制的口令认证失败: %v", node.id, err)
		}
	}

	// 权限：在**另一个** follower 上设置，仍要全体一致。
	if err := second.b.SetPermission("alice", "/", ".*", "^amq\\.", ".*"); err != nil {
		t.Fatalf("在 follower %s 上设置权限失败: %v", second.id, err)
	}
	waitFor(t, 5*time.Second, "权限复制到全部节点", func() bool {
		for _, node := range nodes {
			if !hasPermission(node.b, "alice", "/", ".*", "^amq\\.", ".*") {
				return false
			}
		}
		return true
	})
	// 删除权限：元数据里的权限是独立记录，删完不能留悬空记录。
	if ok, err := first.b.DeletePermission("alice", "/"); err != nil || !ok {
		t.Fatalf("删除权限失败: ok=%v err=%v", ok, err)
	}
	waitFor(t, 5*time.Second, "权限删除复制到全部节点", func() bool {
		for _, node := range nodes {
			if len(node.b.UserPermissions("alice")) != 0 {
				return false
			}
		}
		return true
	})

	// 删除用户：全体节点都应看不到，且登录必须失败。
	if ok, err := second.b.DeleteUser("alice"); err != nil || !ok {
		t.Fatalf("删除用户失败: ok=%v err=%v", ok, err)
	}
	waitFor(t, 5*time.Second, "用户删除复制到全部节点", func() bool {
		for _, node := range nodes {
			if _, ok := node.b.User("alice"); ok {
				return false
			}
		}
		return true
	})
	for _, node := range nodes {
		if _, err := node.b.VerifyUser("alice", "secret", loopbackAddr()); err == nil {
			t.Fatalf("节点 %s 上已删除的账号仍能登录", node.id)
		}
	}
}

// TestUserChangesSurviveRestart 覆盖单机模式的持久化：账号与权限改动经元数据落盘，
// 重启后保留；并且**运行期删掉的配置账号不会因为配置文件里还有而复活**。
func TestUserChangesSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	open := func() *broker.Broker {
		t.Helper()
		cfg, err := config.Load("")
		if err != nil {
			t.Fatalf("加载默认配置失败: %v", err)
		}
		cfg.DataDir = dir
		return mustBroker(t, cfg)
	}

	b := open()
	// 首次引导由后台协程把配置里的初始账号写进元数据：先等它落定，
	// 否则"重启后以元数据为准"这件事无从验证（元数据还是空的）。
	waitFor(t, 5*time.Second, "初始账号已播种进元数据", func() bool {
		return b.ClusterStatus().Users > 0
	})
	if _, ok := b.User("guest"); !ok {
		t.Fatal("初始账号 guest 应可用")
	}

	if err := b.UpsertUser("bob", "pw", []string{"monitoring"}); err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	if err := b.SetPermission("bob", "/", ".*", ".*", ".*"); err != nil {
		t.Fatalf("设置权限失败: %v", err)
	}
	// 删除一个**来自配置文件**的账号：它只存在于配置里，元数据才是权威。
	if ok, err := b.DeleteUser("guest"); err != nil || !ok {
		t.Fatalf("删除 guest 失败: ok=%v err=%v", ok, err)
	}
	b.Close()

	// 重启：配置里仍有 guest，但元数据里没有 —— 不能被配置"复活"。
	b2 := open()
	defer b2.Close()

	if _, ok := b2.User("bob"); !ok {
		t.Fatal("重启后新建的账号 bob 丢失")
	}
	if _, err := b2.VerifyUser("bob", "pw", loopbackAddr()); err != nil {
		t.Fatalf("重启后 bob 认证失败: %v", err)
	}
	if !hasPermission(b2, "bob", "/", ".*", ".*", ".*") {
		t.Fatal("重启后 bob 的权限丢失")
	}
	if _, ok := b2.User("guest"); ok {
		t.Fatal("重启后已被删除的配置账号 guest 复活了")
	}
}

// TestClusterUserChangesSurviveRestart 覆盖**集群模式**下的账号持久化：走的是 Raft 日志重放，
// 比单机模式多一个陷阱 —— 重放是**异步**的，刚启动的一瞬间"元数据里没有账号"
// 不能被当成"首次引导"，否则配置里的账号会被写回日志尾部，把运行期的删除抹掉。
func TestClusterUserChangesSurviveRestart(t *testing.T) {
	port := freePort(t)
	dir := t.TempDir()
	peers := map[string]string{"n1": fmt.Sprintf("127.0.0.1:%d", port)}

	b1, err := broker.New(discardLogger(), clusterConfig(t, dir, "n1", port, peers))
	if err != nil {
		t.Fatalf("启动单节点集群失败: %v", err)
	}
	waitFor(t, 10*time.Second, "单节点集群成为 leader", func() bool {
		st := b1.ClusterStatus()
		return st.Role == "leader" && st.HasQuorum
	})
	// 先等首次引导把配置里的 account 写进元数据，否则下面的删除会因为账号还没播种而不命中。
	waitFor(t, 10*time.Second, "初始账号已播种进元数据", func() bool {
		return b1.ClusterStatus().Users > 0
	})

	if err := b1.UpsertUser("carol", "pw", nil); err != nil {
		t.Fatalf("创建用户失败: %v", err)
	}
	if ok, err := b1.DeleteUser("guest"); err != nil || !ok {
		t.Fatalf("删除 guest 失败: ok=%v err=%v", ok, err)
	}
	b1.Close()

	b2, err := broker.New(discardLogger(), clusterConfig(t, dir, "n1", port, peers))
	if err != nil {
		t.Fatalf("重启单节点集群失败: %v", err)
	}
	t.Cleanup(b2.Close)
	// 重放是顺序的：等"新账号回来了"且"已删除的账号没回来"同时成立（此刻被删账号仍可能来自配置）。
	waitFor(t, 10*time.Second, "重启后账号从日志恢复", func() bool {
		_, hasCarol := b2.User("carol")
		_, hasGuest := b2.User("guest")
		return hasCarol && !hasGuest
	})
	if _, err := b2.VerifyUser("carol", "pw", loopbackAddr()); err != nil {
		t.Fatalf("重启后 carol 认证失败: %v", err)
	}
	// 不能只看某一瞬间："引导播种"是后台协程，判据被误判时会在几百毫秒内把账号写回来。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := b2.User("guest"); ok {
			t.Fatal("重启后已被删除的配置账号 guest 复活了（把异步重放的空窗误判成了首次引导）")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
