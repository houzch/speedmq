package broker_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖 M6 的集群地基：
//   - 节点身份与成员表（真实 TCP 集群端口）；
//   - durable 拓扑经 Raft 复制到全体节点；
//   - 在 follower 上写入时转发给 leader；
//   - 远端队列（消息不在本节点）的操作明确报 NOT_IMPLEMENTED，而不是静默丢消息；
//   - pause_minority：失去多数派时暂停服务。
//
// 用真实 TCP 端口而不是内存网络：节点间 RPC 正是本里程碑要接通的东西，
// 只有真的监听起来，才算证明"集群能跑"，也才能顺带验证传输层的编解码。

// freePort 预留一个空闲的本地端口：绑定 127.0.0.1:0 读到内核分配的端口后立即关闭。
//
// 关闭到再次绑定之间存在被抢占的窗口，但本机测试里窗口极小；
// 这是"不引入测试专用构造入口"所付出的代价（见 AGENTS.md §1 零依赖、§2 依赖方向）。
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("分配空闲端口失败: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("释放临时监听失败: %v", err)
	}
	return port
}

// clusterNode 是测试集群里的一个内核实例。
type clusterNode struct {
	id   string
	dir  string
	port int
	b    *broker.Broker
}

// startTestCluster 起一个 n 节点的集群（每个节点独立数据目录与集群端口）。
func startTestCluster(t *testing.T, n int) []*clusterNode {
	t.Helper()
	ids := make([]string, n)
	ports := make([]int, n)
	dirs := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
		ports[i] = freePort(t)
		dirs[i] = t.TempDir()
	}
	peers := make(map[string]string, n)
	for i := range ids {
		peers[ids[i]] = fmt.Sprintf("127.0.0.1:%d", ports[i])
	}

	nodes := make([]*clusterNode, 0, n)
	for i := range ids {
		cfg := clusterConfig(t, dirs[i], ids[i], ports[i], peers)
		b, err := broker.New(discardLogger(), cfg)
		if err != nil {
			t.Fatalf("启动节点 %s 失败: %v", ids[i], err)
		}
		nodes = append(nodes, &clusterNode{id: ids[i], dir: dirs[i], port: ports[i], b: b})
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.b.Close()
		}
	})
	return nodes
}

// clusterConfig 装配一个开启集群的配置。
func clusterConfig(t *testing.T, dir, id string, port int, peers map[string]string) *config.Config {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	cfg.DataDir = dir
	// 每个节点各自持有一份成员表副本：配置对象在节点间共享会被并发读写。
	own := make(map[string]string, len(peers))
	for k, v := range peers {
		own[k] = v
	}
	cfg.Cluster = config.Cluster{
		Enabled:         true,
		NodeID:          id,
		Listen:          fmt.Sprintf("127.0.0.1:%d", port),
		Peers:           own,
		PartitionPolicy: config.PartitionPauseMinority,
	}
	return cfg
}

// waitClusterLeader 等集群收敛到唯一 leader 并返回它。
func waitClusterLeader(t *testing.T, nodes []*clusterNode) *clusterNode {
	t.Helper()
	var leader *clusterNode
	waitFor(t, 10*time.Second, "集群收敛到唯一 leader", func() bool {
		var found *clusterNode
		for _, node := range nodes {
			if node.b.ClusterStatus().Role == "leader" {
				if found != nil {
					return false
				}
				found = node
			}
		}
		if found == nil {
			return false
		}
		leader = found
		return true
	})
	return leader
}

// TestClusterReplicatesTopology：在一个 follower 上声明 durable 拓扑，
// 断言它经 leader 提交后出现在全部节点上；并验证远端队列的消息操作明确报错。
func TestClusterReplicatesTopology(t *testing.T) {
	nodes := startTestCluster(t, 3)
	leader := waitClusterLeader(t, nodes)

	// 集群身份与成员表：每个节点都应看到 3 个成员与同一个 leader。
	for _, node := range nodes {
		st := node.b.ClusterStatus()
		if !node.b.ClusterEnabled() {
			t.Fatalf("节点 %s 应处于集群模式", node.id)
		}
		if st.Mode != "raft" {
			t.Fatalf("节点 %s 的元数据模式 = %s, want raft", node.id, st.Mode)
		}
		if len(st.Peers) != 3 {
			t.Fatalf("节点 %s 的成员数 = %d, want 3", node.id, len(st.Peers))
		}
	}
	// follower 是从心跳里"学到" leader 的，存在短暂延迟：等它收敛，而不是要求瞬间一致。
	waitFor(t, 5*time.Second, "全部节点记录同一个 leader", func() bool {
		for _, node := range nodes {
			if node.id != leader.id && node.b.ClusterStatus().Leader != leader.id {
				return false
			}
		}
		return true
	})

	// 客户端连到一个 follower：写入必须被转发给 leader 并复制到全体。
	var follower *clusterNode
	for _, node := range nodes {
		if node.id != leader.id {
			follower = node
			break
		}
	}
	sess, err := testSessionOf(t, follower.b, "guest", "guest")
	if err != nil {
		t.Fatalf("在 follower %s 上打开会话失败: %v", follower.id, err)
	}

	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "m6.repl.q", Durable: true})
	if err := sess.DeclareExchange(plugin.ExchangeDeclare{
		Name: "m6.repl.ex", Type: plugin.ExchangeDirect, Durable: true,
	}); err != nil {
		t.Fatalf("声明 durable 交换机失败: %v", err)
	}
	if err := sess.BindQueue("m6.repl.q", "m6.repl.ex", "rk", nil); err != nil {
		t.Fatalf("绑定 durable 队列失败: %v", err)
	}

	waitFor(t, 5*time.Second, "拓扑复制到全部节点", func() bool {
		for _, node := range nodes {
			if _, ok := node.b.QueueSnapshot("/", "m6.repl.q"); !ok {
				return false
			}
			if _, ok := node.b.ExchangeSnapshot("/", "m6.repl.ex"); !ok {
				return false
			}
			found := false
			for _, b := range node.b.QueueBindings("/", "m6.repl.q") {
				if b.Source == "m6.repl.ex" && b.RoutingKey == "rk" {
					found = true
				}
			}
			if !found {
				return false
			}
		}
		return true
	})

	// Owner 节点（声明它的 follower）上，消息操作是真实可用的。
	res, err := sess.Publish(&plugin.Message{Body: []byte("hello")}, "m6.repl.ex", "rk", false)
	if err != nil || !res.Routed {
		t.Fatalf("在 Owner 节点发布失败: routed=%v err=%v", res.Routed, err)
	}
	if _, ok, err := sess.Get("m6.repl.q", true); err != nil || !ok {
		t.Fatalf("在 Owner 节点拉取失败: ok=%v err=%v", ok, err)
	}

	// 其他节点只持有"远端占位队列"：发布/消费必须明确报 NOT_IMPLEMENTED，
	// 而不是路由到不存在的本地数据上（那等于静默丢消息）。
	// 取一个非 Owner 节点（不假设它就是 leader：任期内角色可能变化）。
	var other *clusterNode
	for _, node := range nodes {
		if node.id != follower.id {
			other = node
			break
		}
	}
	otherSess, err := testSessionOf(t, other.b, "guest", "guest")
	if err != nil {
		t.Fatalf("在节点 %s 上打开会话失败: %v", other.id, err)
	}
	if _, err := otherSess.Publish(&plugin.Message{Body: []byte("x")}, "", "m6.repl.q", false); err == nil {
		t.Fatalf("向远端队列发布应报错（跨节点转发本期未实现）")
	}
	if _, err := otherSess.Consume(plugin.Subscription{Queue: "m6.repl.q", NoAck: true}); err == nil {
		t.Fatalf("消费远端队列应报错（跨节点转发本期未实现）")
	}
	if err := otherSess.BindQueue("m6.repl.q", "m6.repl.ex", "rk2", nil); err == nil {
		t.Fatalf("把远端队列绑到交换机应被拒绝（避免消息黑洞）")
	}
}

// TestClusterPauseMinority：3 节点里失去 2 个后，剩下的节点按 pause_minority 暂停服务。
func TestClusterPauseMinority(t *testing.T) {
	nodes := startTestCluster(t, 3)
	waitClusterLeader(t, nodes)

	survivor := nodes[0]
	var downed []*clusterNode
	for _, node := range nodes {
		if node != survivor {
			downed = append(downed, node)
		}
	}
	for _, node := range downed {
		node.b.Close()
	}

	waitFor(t, 10*time.Second, "幸存节点失去多数派", func() bool {
		return !survivor.b.ClusterStatus().HasQuorum
	})
	waitFor(t, 5*time.Second, "幸存节点按 pause_minority 暂停", survivor.b.ClusterPaused)

	// 暂停期间一切面向客户端的操作都应被拒绝，而不是在可能已经分叉的状态上继续服务。
	sess, err := testSessionOf(t, survivor.b, "guest", "guest")
	if err != nil {
		// 会话可能因连接被主动断开而失败，这也是可接受的表现。
		return
	}
	if _, err := sess.Publish(&plugin.Message{Body: []byte("x")}, "", "any", false); err == nil {
		t.Fatalf("节点暂停期间发布应被拒绝")
	}
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: "m6.paused.q", Durable: true}); err == nil {
		t.Fatalf("节点暂停期间声明队列应被拒绝")
	}
}

// TestSingleNodeClusterPersistsTopology：单节点集群（成员表只含自己）应立刻成为 leader，
// 且 durable 拓扑经 Raft 日志持久化 —— 重启后队列仍在。
func TestSingleNodeClusterPersistsTopology(t *testing.T) {
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
	sess, err := testSessionOf(t, b1, "guest", "guest")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	mustDeclareQueue(t, sess, plugin.QueueDeclare{Name: "m6.single.q", Durable: true})
	b1.Close()

	// 用同一数据目录与端口重启：日志回放后拓扑必须原样回来。
	b2, err := broker.New(discardLogger(), clusterConfig(t, dir, "n1", port, peers))
	if err != nil {
		t.Fatalf("重启单节点集群失败: %v", err)
	}
	t.Cleanup(b2.Close)
	waitFor(t, 10*time.Second, "重启后队列从日志恢复", func() bool {
		_, ok := b2.QueueSnapshot("/", "m6.single.q")
		return ok
	})
	waitFor(t, 10*time.Second, "重启后重新成为 leader", func() bool {
		return b2.ClusterStatus().Role == "leader"
	})
}
