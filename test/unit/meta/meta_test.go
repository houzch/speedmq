// Package meta_test 覆盖元数据层的对外行为：单机落盘与恢复、Raft 复制与转发、
// 分区下的写入语义、以及状态字段。
//
// 断言一律落在**对外可观察的行为**上（State/Status/回调次数），不触碰未导出实现（AGENTS.md §10.1）。
package meta_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/internal/raft"
)

// 统一的等待预算：选举 + 复制在内存网络里通常几十毫秒内完成，5s 只是防呆上限。
const waitTimeout = 5 * time.Second

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// recordedApply 记录一次下发到内核的增量变更。
type recordedApply struct {
	op      meta.Op
	payload []byte
}

// recordingApplier 是内核侧的回调替身：只记录"内核收到了什么"，不实现任何拓扑逻辑。
//
// 用它而不是真内核，是为了把断言放在契约上：一次 open 恰好一次 RestoreMeta、每次写入恰好一次 ApplyMeta。
type recordingApplier struct {
	mu       sync.Mutex
	restores []meta.State
	applies  []recordedApply
}

func (a *recordingApplier) ApplyMeta(op meta.Op, payload []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applies = append(a.applies, recordedApply{op: op, payload: payload})
	return nil
}

func (a *recordingApplier) RestoreMeta(state meta.State) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.restores = append(a.restores, state)
	return nil
}

func (a *recordingApplier) restoreCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.restores)
}

func (a *recordingApplier) lastRestore(t *testing.T) meta.State {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.restores) == 0 {
		t.Fatal("内核从未收到 RestoreMeta")
	}
	return a.restores[len(a.restores)-1]
}

func (a *recordingApplier) ops() []meta.Op {
	a.mu.Lock()
	defer a.mu.Unlock()
	ops := make([]meta.Op, 0, len(a.applies))
	for _, item := range a.applies {
		ops = append(ops, item.op)
	}
	return ops
}

func (a *recordingApplier) payloadOf(t *testing.T, op meta.Op) []byte {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, item := range a.applies {
		if item.op == op {
			return item.payload
		}
	}
	t.Fatalf("内核从未收到 %s 的增量变更", op)
	return nil
}

// waitUntil 以固定间隔轮询直到条件满足或超时。
//
// 用 ticker 而不是 time.Sleep 落地等待：等的是"状态真的变了"，而不是"睡够时间就当它变了"（AGENTS.md §6）。
func waitUntil(timeout time.Duration, cond func() bool) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return true
		}
		select {
		case <-timer.C:
			return cond()
		case <-tick.C:
		}
	}
}

// stateJSON 把状态编码成字符串，用于"逐字节一致"这类强断言（encoding/json 对 map 键排序，结果是确定的）。
func stateJSON(t *testing.T, st meta.State) string {
	t.Helper()
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("编码状态失败: %v", err)
	}
	return string(data)
}

// 下面是固定的测试记录：字段值刻意各不相同，便于断言"落盘/复制的是同一个对象"。
func vhostRec() meta.VHost {
	return meta.VHost{Name: "/", CreatedAt: time.Unix(1700000000, 0).UTC()}
}

func exchangeRec() meta.Exchange {
	return meta.Exchange{VHost: "/", Name: "ex1", Type: "topic", Durable: true, Arguments: map[string]any{"alternate-exchange": "ae"}}
}

func queueRec() meta.Queue {
	return meta.Queue{
		VHost:     "/",
		Name:      "q1",
		Durable:   true,
		Arguments: map[string]any{"x-max-length": 100},
		Owner:     "n1",
		CreatedAt: time.Unix(1700000001, 0).UTC(),
	}
}

func bindingRec() meta.Binding {
	return meta.Binding{VHost: "/", Source: "ex1", Destination: "q1", DestinationType: "queue", RoutingKey: "a.b"}
}

func userRec() meta.User {
	return meta.User{Name: "alice", Password: "p1", Tags: []string{"administrator"}, RemoteAccess: true}
}

func permissionRec() meta.Permission {
	return meta.Permission{User: "alice", VHost: "/", Configure: ".*", Write: "^q1$", Read: ".*"}
}

// namedOp 是一条待写入的变更。
type namedOp struct {
	op      meta.Op
	payload any
}

// fullBatch 是覆盖六类记录的完整批次（12 个 Op 里的 6 个 put）。
func fullBatch() []namedOp {
	return []namedOp{
		{meta.OpPutVHost, vhostRec()},
		{meta.OpPutExchange, exchangeRec()},
		{meta.OpPutQueue, queueRec()},
		{meta.OpPutBinding, bindingRec()},
		{meta.OpPutUser, userRec()},
		{meta.OpPutPermission, permissionRec()},
	}
}

// writeBatch 顺序写入一批变更。
func writeBatch(t *testing.T, ctx context.Context, st *meta.Store, batch []namedOp) {
	t.Helper()
	for _, item := range batch {
		if err := st.Write(ctx, item.op, item.payload); err != nil {
			t.Fatalf("写入 %s 失败: %v", item.op, err)
		}
	}
}

// assertState 断言状态里恰好是 fullBatch() 写入的那套拓扑。
func assertState(t *testing.T, st meta.State) {
	t.Helper()
	if len(st.VHosts) != 1 {
		t.Fatalf("vhost 数量 = %d, want 1", len(st.VHosts))
	}
	if vh, ok := st.VHosts["/"]; !ok || vh.Name != "/" {
		t.Fatalf("vhost / 缺失或不正确: %+v", st.VHosts)
	}
	ex, ok := st.Exchanges[meta.Key("/", "ex1")]
	if !ok {
		t.Fatalf("交换机 /ex1 缺失: %+v", st.Exchanges)
	}
	if ex.Type != "topic" || !ex.Durable {
		t.Fatalf("交换机 /ex1 属性不正确: %+v", ex)
	}
	q, ok := st.Queues[meta.Key("/", "q1")]
	if !ok {
		t.Fatalf("队列 /q1 缺失: %+v", st.Queues)
	}
	if !q.Durable || q.Owner != "n1" || !q.CreatedAt.Equal(time.Unix(1700000001, 0).UTC()) {
		t.Fatalf("队列 /q1 属性不正确: %+v", q)
	}
	if len(st.Bindings) != 1 {
		t.Fatalf("绑定数量 = %d, want 1: %+v", len(st.Bindings), st.Bindings)
	}
	b := st.Bindings[0]
	if b.Source != "ex1" || b.Destination != "q1" || b.RoutingKey != "a.b" || b.DestinationType != "queue" {
		t.Fatalf("绑定内容不正确: %+v", b)
	}
	u, ok := st.Users["alice"]
	if !ok {
		t.Fatalf("用户 alice 缺失: %+v", st.Users)
	}
	if u.Password != "p1" || !u.RemoteAccess || len(u.Tags) != 1 || u.Tags[0] != "administrator" {
		t.Fatalf("用户 alice 属性不正确: %+v", u)
	}
	p, ok := st.Permissions[meta.PermissionKey("/", "alice")]
	if !ok {
		t.Fatalf("权限 (/ alice) 缺失: %+v", st.Permissions)
	}
	if p.Configure != ".*" || p.Write != "^q1$" || p.Read != ".*" {
		t.Fatalf("权限内容不正确: %+v", p)
	}
}

// TestLocalPersistenceAndCallbacks 覆盖单机模式：写入 → 落盘 → 重开恢复，
// 并断言"一次 open 一次 RestoreMeta、每次写入一次 ApplyMeta"。
func TestLocalPersistenceAndCallbacks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	applier := &recordingApplier{}
	st, err := meta.Open(ctx, meta.Options{Mode: meta.ModeLocal, NodeID: "n1", Dir: dir, Applier: applier, Logger: testLogger()})
	if err != nil {
		t.Fatalf("打开单机元数据存储失败: %v", err)
	}
	if got := applier.restoreCount(); got != 1 {
		t.Fatalf("open 应恰好调用一次 RestoreMeta，实际 %d 次", got)
	}
	assertEmpty(t, applier.lastRestore(t))

	batch := fullBatch()
	writeBatch(t, ctx, st, batch)

	gotOps := applier.ops()
	if len(gotOps) != len(batch) {
		t.Fatalf("ApplyMeta 调用次数 = %d, want %d", len(gotOps), len(batch))
	}
	for i, item := range batch {
		if gotOps[i] != item.op {
			t.Fatalf("第 %d 次 ApplyMeta 的 op = %s, want %s（顺序必须与写入一致）", i, gotOps[i], item.op)
		}
	}
	// 下发给内核的 payload 必须是该 Op 对应记录的 JSON —— 内核直接按记录字段解析它。
	var appliedQueue meta.Queue
	if err := json.Unmarshal(applier.payloadOf(t, meta.OpPutQueue), &appliedQueue); err != nil {
		t.Fatalf("ApplyMeta 的 payload 不是合法记录: %v", err)
	}
	if appliedQueue.Name != "q1" || appliedQueue.Owner != "n1" {
		t.Fatalf("ApplyMeta 的 payload 内容不正确: %+v", appliedQueue)
	}
	assertState(t, st.State())
	if err := st.Close(); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("重复关闭必须幂等，实际报错: %v", err)
	}

	// 同一 Dir 重开：状态必须来自 state.json。
	applier2 := &recordingApplier{}
	st2, err := meta.Open(ctx, meta.Options{Mode: meta.ModeLocal, NodeID: "n1", Dir: dir, Applier: applier2, Logger: testLogger()})
	if err != nil {
		t.Fatalf("重开单机元数据存储失败: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })

	if got := applier2.restoreCount(); got != 1 {
		t.Fatalf("重开应恰好调用一次 RestoreMeta，实际 %d 次", got)
	}
	if got := len(applier2.ops()); got != 0 {
		t.Fatalf("重开只应整体恢复，不应逐条 ApplyMeta，实际 %d 条", got)
	}
	assertState(t, st2.State())
}

// TestLocalFirstStartEmpty 覆盖单机首次启动：空目录下也要交付一次"空状态"给内核。
func TestLocalFirstStartEmpty(t *testing.T) {
	applier := &recordingApplier{}
	st, err := meta.Open(context.Background(), meta.Options{
		Mode: meta.ModeLocal, NodeID: "n1", Dir: t.TempDir(), Applier: applier, Logger: testLogger(),
	})
	if err != nil {
		t.Fatalf("打开单机元数据存储失败: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if got := applier.restoreCount(); got != 1 {
		t.Fatalf("open 应恰好调用一次 RestoreMeta，实际 %d 次", got)
	}
	assertEmpty(t, applier.lastRestore(t))
	assertEmpty(t, st.State())
}

// assertEmpty 断言状态里六类记录都为空。
func assertEmpty(t *testing.T, st meta.State) {
	t.Helper()
	q, e, b, u := len(st.Queues), len(st.Exchanges), len(st.Bindings), len(st.Users)
	if len(st.VHosts) != 0 || q != 0 || e != 0 || b != 0 || u != 0 || len(st.Permissions) != 0 {
		t.Fatalf("状态应为空，实际 %s", stateJSON(t, st))
	}
}

// TestIdempotentReplay 覆盖幂等性：同一批 Op 重放后状态完全一致，
// 绑定不重复、用户覆盖式写入、删除不存在的键幂等成功。
func TestIdempotentReplay(t *testing.T) {
	ctx := context.Background()
	st, err := meta.Open(ctx, meta.Options{
		Mode: meta.ModeLocal, NodeID: "n1", Dir: t.TempDir(), Applier: &recordingApplier{}, Logger: testLogger(),
	})
	if err != nil {
		t.Fatalf("打开单机元数据存储失败: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	writeBatch(t, ctx, st, fullBatch())
	first := stateJSON(t, st.State())

	writeBatch(t, ctx, st, fullBatch())
	second := stateJSON(t, st.State())
	if first != second {
		t.Fatalf("重放同一批 Op 后状态应完全一致:\n第一次=%s\n第二次=%s", first, second)
	}
	if got := len(st.State().Bindings); got != 1 {
		t.Fatalf("重复 binding.put 不应产生重复条目，实际 %d 条", got)
	}
	if got := len(st.State().Users); got != 1 {
		t.Fatalf("重复 user.put 不应产生新用户，实际 %d 个", got)
	}

	// 删除不存在的键必须幂等成功，且不改变状态。
	if err := st.Write(ctx, meta.OpDeleteQueue, meta.Queue{VHost: "/", Name: "不存在"}); err != nil {
		t.Fatalf("删除不存在的队列应幂等成功: %v", err)
	}
	if err := st.Write(ctx, meta.OpDeleteQueue, meta.Queue{VHost: "/", Name: "不存在"}); err != nil {
		t.Fatalf("重复删除不存在的队列应幂等成功: %v", err)
	}
	if err := st.Write(ctx, meta.OpDeleteUser, meta.User{Name: "不存在"}); err != nil {
		t.Fatalf("删除不存在的用户应幂等成功: %v", err)
	}
	if got := stateJSON(t, st.State()); got != second {
		t.Fatalf("删除不存在的键不应改变状态:\n之前=%s\n之后=%s", second, got)
	}

	// user.put 是覆盖式写入。
	if err := st.Write(ctx, meta.OpPutUser, meta.User{Name: "alice", Password: "p2", Tags: []string{"management"}}); err != nil {
		t.Fatalf("覆盖写入用户失败: %v", err)
	}
	users := st.State().Users
	if len(users) != 1 || users["alice"].Password != "p2" {
		t.Fatalf("user.put 应覆盖同名用户: %+v", users)
	}

	// 删除绑定两次同样幂等。
	for i := 0; i < 2; i++ {
		if err := st.Write(ctx, meta.OpDeleteBinding, bindingRec()); err != nil {
			t.Fatalf("第 %d 次删除绑定失败: %v", i+1, err)
		}
	}
	if got := len(st.State().Bindings); got != 0 {
		t.Fatalf("删除绑定后应无残留，实际 %d 条", got)
	}
}

// cluster 是三节点内存集群夹具：三个 *Store 共用同一个 MemNetwork，各自有独立 Dir。
type cluster struct {
	net    raft.MemNetwork
	ids    []string
	peers  map[string]string
	dirs   map[string]string
	stores map[string]*meta.Store
}

// newCluster 起 n1/n2/n3 三个节点并等到 leader 选出。
func newCluster(t *testing.T) *cluster {
	t.Helper()
	ids := []string{"n1", "n2", "n3"}
	peers := make(map[string]string, len(ids))
	for _, id := range ids {
		peers[id] = "mem://" + id
	}
	c := &cluster{net: raft.NewMemNetwork(), ids: ids, peers: peers, dirs: map[string]string{}, stores: map[string]*meta.Store{}}
	for _, id := range ids {
		c.dirs[id] = t.TempDir()
		c.open(t, id)
	}
	// 在 TempDir 注册之后注册：t.Cleanup 是后进先出，这样会"先关节点、再删目录"，
	// 否则 Windows 上删不掉还被 Raft 打开的日志文件。
	t.Cleanup(c.closeAll)
	c.waitLeader(t)
	return c
}

// open 用固定 Dir / 固定 MemNetwork 打开（或重新打开）一个节点。
func (c *cluster) open(t *testing.T, id string) {
	t.Helper()
	st, err := meta.Open(context.Background(), meta.Options{
		Mode:      meta.ModeRaft,
		NodeID:    id,
		Dir:       c.dirs[id],
		Listen:    "mem://" + id,
		Peers:     c.peers,
		Transport: c.net.Transport(id),
		Applier:   &recordingApplier{},
		Logger:    testLogger(),
		// 选举参数留零值 = 用 raft 的默认值（300ms/50ms）：测试跑在负载不定的机器上，
		// 过于激进的选举超时会让"取到 leader 之后它就被赶下台"变成常态，掩盖真正要验的行为。
	})
	if err != nil {
		t.Fatalf("打开集群节点 %s 失败: %v", id, err)
	}
	c.stores[id] = st
}

// close 关闭一个节点并把它从"在跑的节点"里摘掉。
func (c *cluster) close(t *testing.T, id string) {
	t.Helper()
	st, ok := c.stores[id]
	if !ok {
		return
	}
	if err := st.Close(); err != nil {
		t.Fatalf("关闭集群节点 %s 失败: %v", id, err)
	}
	delete(c.stores, id)
}

// restartAll 模拟"三个节点进程重启"：关闭全部节点，再用同一批 Dir 全部重新打开。
//
// 必须换一个新的 MemNetwork：raft 的内存传输在 Close 之后是终态（不能再注册方法），
// 不 Close 又会残留上一个节点实例的方法注册表（重复注册报错），
// 所以一个 memTransport 实例承载不了两个节点生命周期。三个节点一起重启，
// 用新网络实例表示"重启后的集群网络"，不影响本用例要验证的东西 ——
// 每条状态都来自各节点自己落盘的日志（提交进度仍需一次新选举来推进：重启后
// commitIndex 从快照位置起算，要靠 leader 的 leaderCommit 才能把旧任期条目重放出来）。
func (c *cluster) restartAll(t *testing.T) {
	t.Helper()
	for _, id := range c.ids {
		c.close(t, id)
	}
	c.net = raft.NewMemNetwork()
	for _, id := range c.ids {
		c.open(t, id)
	}
}

func (c *cluster) closeAll() {
	for id, st := range c.stores {
		_ = st.Close() // 收尾清理：失败也无处上报，Close 本身是幂等的
		delete(c.stores, id)
	}
}

// findLeader 返回当前自认 leader 的节点 ID。
func (c *cluster) findLeader() (string, bool) {
	for _, id := range c.ids {
		st, ok := c.stores[id]
		if !ok {
			continue
		}
		if st.Status().Role == "leader" {
			return id, true
		}
	}
	return "", false
}

// waitLeader 等待 leader 选出，超时即失败。
func (c *cluster) waitLeader(t *testing.T) string {
	t.Helper()
	var leader string
	if !waitUntil(waitTimeout, func() bool {
		leader, _ = c.findLeader()
		return leader != ""
	}) {
		t.Fatalf("等待 leader 超时，当前状态: %s", c.statusSummary())
	}
	return leader
}

// other 返回一个非 id 的节点。
func (c *cluster) other(id string) string {
	for _, candidate := range c.ids {
		if candidate != id {
			return candidate
		}
	}
	return ""
}

// commitOn 在 id 上写入一条队列声明，返回错误。
func (c *cluster) commitOn(id string, q meta.Queue) error {
	return c.stores[id].Write(context.Background(), meta.OpPutQueue, q)
}

// commitOnCurrentLeader 在当前 leader 上提交一次写入。
//
// 重试而不是"取一次 leader 就直接写"：选主是异步的，取到 leader 之后它仍可能因为
// 心跳延迟而被更高任期赶下台（机器慢时尤其如此）；上层的既定用法本来就是
// "拿 ErrNotLeader 就换节点重试"，所以这里也按这个用法来，重试的是幂等写入。
func (c *cluster) commitOnCurrentLeader(t *testing.T, q meta.Queue) {
	t.Helper()
	var writeErr error
	if !waitUntil(waitTimeout, func() bool {
		id, ok := c.findLeader()
		if !ok {
			return false
		}
		writeErr = c.commitOn(id, q)
		return writeErr == nil
	}) {
		t.Fatalf("在当前 leader 上提交 %s 失败: %v, %s", q.Name, writeErr, c.statusSummary())
	}
}

// hasQueue 判断 id 的状态里是否已有该队列。
func (c *cluster) hasQueue(id string, name string) bool {
	st, ok := c.stores[id]
	if !ok {
		return false
	}
	_, found := st.State().Queues[meta.Key("/", name)]
	return found
}

// waitQueueOnAll 等待三个节点都出现该队列。
func (c *cluster) waitQueueOnAll(t *testing.T, name string) {
	t.Helper()
	if !waitUntil(waitTimeout, func() bool {
		for _, id := range c.ids {
			if !c.hasQueue(id, name) {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("队列 %s 未复制到全部节点: %s", name, c.statusSummary())
	}
}

// assertSameState 等待三个节点的状态快照完全一致（集群最终一致的核心断言）。
func (c *cluster) assertSameState(t *testing.T) {
	t.Helper()
	var want string
	if !waitUntil(waitTimeout, func() bool {
		want = stateJSON(t, c.stores[c.ids[0]].State())
		for _, id := range c.ids[1:] {
			if stateJSON(t, c.stores[id].State()) != want {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("三节点状态未收敛一致: %s", c.statusSummary())
	}
}

func (c *cluster) statusSummary() string {
	out := ""
	for _, id := range c.ids {
		st, ok := c.stores[id]
		if !ok {
			out += id + "=已关闭 "
			continue
		}
		out += id + "=" + jsonStatus(st.Status()) + " "
	}
	return out
}

func jsonStatus(s meta.Status) string {
	data, err := json.Marshal(s)
	if err != nil {
		return "?"
	}
	return string(data)
}

// TestRaftFollowerForwardReplication 覆盖核心路径：在 follower 上写入，
// 由 follower 转发给 leader 提交，三个节点最终状态一致。
func TestRaftFollowerForwardReplication(t *testing.T) {
	c := newCluster(t)
	leaderID := c.waitLeader(t)
	followerID := c.other(leaderID)

	q := meta.Queue{VHost: "/", Name: "fwd.q", Durable: true, Owner: followerID, CreatedAt: time.Unix(1700000002, 0).UTC()}
	var writeErr error
	if !waitUntil(waitTimeout, func() bool {
		writeErr = c.commitOn(followerID, q)
		return writeErr == nil
	}) {
		t.Fatalf("在 follower %s 上写入失败（leader=%s）: %v", followerID, leaderID, writeErr)
	}
	// 写入返回即表示"已在本节点应用"，集群里所有节点最终都要一致。
	c.waitQueueOnAll(t, "fwd.q")
	c.assertSameState(t)
}

// TestRaftLeaderWriteReplication 覆盖 leader 直写路径：三节点最终一致。
func TestRaftLeaderWriteReplication(t *testing.T) {
	c := newCluster(t)
	leaderID := c.waitLeader(t)

	q := meta.Queue{VHost: "/", Name: "lead.q", Durable: true, Owner: leaderID, CreatedAt: time.Unix(1700000003, 0).UTC()}
	c.commitOnCurrentLeader(t, q)
	c.waitQueueOnAll(t, "lead.q")
	c.assertSameState(t)
}

// TestRaftRestartRecovery 覆盖重启恢复：三节点全部关闭后，用同一 Dir 重开，
// 各节点的状态必须从自己落盘的日志恢复出来（整个过程不发起任何新写入）。
func TestRaftRestartRecovery(t *testing.T) {
	c := newCluster(t)
	leaderID := c.waitLeader(t)

	q := meta.Queue{VHost: "/", Name: "restart.q", Durable: true, Owner: leaderID, CreatedAt: time.Unix(1700000004, 0).UTC()}
	c.commitOnCurrentLeader(t, q)
	c.waitQueueOnAll(t, "restart.q")

	c.restartAll(t)
	for _, id := range c.ids {
		if !waitUntil(waitTimeout, func() bool { return c.hasQueue(id, "restart.q") }) {
			t.Fatalf("节点 %s 重启后未恢复队列 restart.q: %s", id, c.statusSummary())
		}
	}
}

// TestRaftPartitionNoCommit 覆盖分区语义：被隔离成少数派的 leader 不得假装写入成功，
// 必须报告无多数派；恢复连通后集群能继续提交。
func TestRaftPartitionNoCommit(t *testing.T) {
	c := newCluster(t)
	leaderID := c.waitLeader(t)

	minority := []string{leaderID}
	majority := []string{}
	for _, id := range c.ids {
		if id != leaderID {
			majority = append(majority, id)
		}
	}
	c.net.Partition(minority, majority)

	isolated := c.stores[leaderID]
	if !waitUntil(waitTimeout, func() bool { return !isolated.Status().HasQuorum }) {
		t.Fatalf("少数派节点 %s 应报告无多数派，实际 %s", leaderID, jsonStatus(isolated.Status()))
	}

	writeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := isolated.Write(writeCtx, meta.OpPutQueue, meta.Queue{VHost: "/", Name: "split.q"}); err == nil {
		t.Fatal("少数派节点上的写入必须失败，不得假装成功")
	}

	// 恢复连通后：可能要先选出新 leader（旧 leader 的任期已落后），随后必须能重新提交。
	c.net.Heal()
	var writeErr error
	if !waitUntil(10*time.Second, func() bool {
		id, ok := c.findLeader()
		if !ok || !c.stores[id].Status().HasQuorum {
			return false
		}
		writeErr = c.commitOn(id, meta.Queue{VHost: "/", Name: "after-heal.q", Durable: true})
		return writeErr == nil
	}) {
		t.Fatalf("分区恢复后集群未能恢复写入: %v, %s", writeErr, c.statusSummary())
	}
	c.waitQueueOnAll(t, "after-heal.q")
}

// TestStatusFields 覆盖 Status 的字段口径：单机与集群两种模式下的模式名、节点、角色、成员与规模计数。
func TestStatusFields(t *testing.T) {
	ctx := context.Background()
	batch := fullBatch()

	t.Run("local", func(t *testing.T) {
		st, err := meta.Open(ctx, meta.Options{
			Mode: meta.ModeLocal, NodeID: "single-1", Dir: t.TempDir(), Applier: &recordingApplier{}, Logger: testLogger(),
		})
		if err != nil {
			t.Fatalf("打开单机元数据存储失败: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })
		writeBatch(t, ctx, st, batch)

		got := st.Status()
		if got.Mode != "local" {
			t.Fatalf("Mode = %q, want local", got.Mode)
		}
		if got.NodeID != "single-1" {
			t.Fatalf("NodeID = %q, want single-1", got.NodeID)
		}
		if got.Role != "single" || got.Leader != "single-1" {
			t.Fatalf("单机模式 Role/Leader 应为 single/single-1，实际 %q/%q", got.Role, got.Leader)
		}
		if !got.HasQuorum || got.Term != 0 {
			t.Fatalf("单机模式 HasQuorum/Term 应为 true/0，实际 %v/%d", got.HasQuorum, got.Term)
		}
		if len(got.Peers) != 1 || got.Peers[0] != "single-1" {
			t.Fatalf("Peers = %v, want [single-1]", got.Peers)
		}
		if got.Queues != 1 || got.Exchanges != 1 || got.Bindings != 1 || got.Users != 1 {
			t.Fatalf("规模计数不正确: %s", jsonStatus(got))
		}
		if got.AppliedRecords != uint64(len(batch)) {
			t.Fatalf("AppliedRecords = %d, want %d", got.AppliedRecords, len(batch))
		}
	})

	t.Run("raft", func(t *testing.T) {
		c := newCluster(t)
		c.waitLeader(t)
		c.commitOnCurrentLeader(t, queueRec())
		c.waitQueueOnAll(t, "q1")
		// 轮询等角色分布稳定：恰好 1 个 leader、其余都是 follower（选举期间会短暂出现 candidate）。
		if !waitUntil(waitTimeout, func() bool {
			leaders, followers := 0, 0
			for _, id := range c.ids {
				switch c.stores[id].Status().Role {
				case "leader":
					leaders++
				case "follower":
					followers++
				}
			}
			return leaders == 1 && followers == len(c.ids)-1
		}) {
			t.Fatalf("角色分布应恰好 1 leader + %d follower: %s", len(c.ids)-1, c.statusSummary())
		}
		leaderID := c.waitLeader(t)

		got := c.stores[leaderID].Status()
		if got.Mode != "raft" {
			t.Fatalf("Mode = %q, want raft", got.Mode)
		}
		if got.NodeID != leaderID {
			t.Fatalf("NodeID = %q, want %q", got.NodeID, leaderID)
		}
		if got.Role != "leader" {
			t.Fatalf("Role = %q, want leader", got.Role)
		}
		if len(got.Peers) != len(c.ids) {
			t.Fatalf("Peers = %v, want %d 项", got.Peers, len(c.ids))
		}
		if !got.HasQuorum {
			t.Fatalf("leader 应报告有多数派: %s", jsonStatus(got))
		}
		if got.Queues != 1 {
			t.Fatalf("Queues = %d, want 1", got.Queues)
		}
		if got.CommitIndex == 0 || got.LastApplied == 0 {
			t.Fatalf("提交后 CommitIndex/LastApplied 应非零: %s", jsonStatus(got))
		}

		followerID := c.other(leaderID)
		fgot := c.stores[followerID].Status()
		if fgot.Mode != "raft" || fgot.NodeID != followerID || len(fgot.Peers) != len(c.ids) {
			t.Fatalf("follower 的字段不正确: %s", jsonStatus(fgot))
		}
		if fgot.Role != "follower" {
			t.Fatalf("follower %s 的 Role = %q, want follower", followerID, fgot.Role)
		}
		if fgot.Queues != 1 {
			t.Fatalf("follower 的 Queues = %d, want 1", fgot.Queues)
		}
	})
}
