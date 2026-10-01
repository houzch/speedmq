package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/internal/raft"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件是 M6 的集群接线：把 meta.Store（Raft 复制的元数据层）与内核内存拓扑绑在一起。
//
// 职责划分：
//   - **元数据层**（internal/meta）负责"顺序与持久化"：谁先谁后、有没有丢；
//   - **本文件**负责"语义"：哪个操作算集群级元数据、如何把一条记录落到 vhost 上。
//
// 本期把"集群级元数据"限定为**durable 且非 exclusive** 的拓扑：durable 交换机、
// durable 非 exclusive 队列，以及两端都在该范围内的绑定。transient / exclusive / auto-delete
// 的对象属会话本地，不进元数据（与 meta/types.go 的约定 1 一致）—— 这样单机行为与 M1–M5 完全不变。
//
// vhost 集合本身由配置驱动（集群各节点必须配置相同的 vhost 列表），本期不支持运行期增删 vhost。

const (
	// metaSubmitTimeout 是元数据提交（含 follower 转发 leader）的上限。
	metaSubmitTimeout = 5 * time.Second
	// metaApplyTimeout 是等待本地拓扑追上元数据提交的上限。
	//
	// follower 上的应用依赖下一轮心跳（默认 50ms），这里留足余量；
	// 超时说明出现异常（例如日志落后），明确报错比静默返回一个"其实还没建好"的队列更诚实。
	metaApplyTimeout = 3 * time.Second
	// metaApplyPollInterval 是等待本地应用的轮询间隔。
	metaApplyPollInterval = 2 * time.Millisecond
)

// openMeta 打开元数据层并按模式落盘：单机用 ModeLocal（本地 state.json），
// 集群用 ModeRaft（Raft 复制 + follower 转发 leader）。
//
// 集群端口由**本层**创建并注入给元数据层：这个端口不止服务共识，
// 还要承载跨节点消息转发（见 forward.go）。先注册转发处理器、再让 Raft 起来，
// 可以避免"邻居已能连上、方法却还没注册"的启动窗口。
//
// 打开过程会回调 Applier.RestoreMeta 把已有状态交给内核，因此必须在 vhost 建好之后调用。
func (b *Broker) openMeta() error {
	mode := meta.ModeLocal
	if b.clusterOn {
		mode = meta.ModeRaft
	}
	opt := meta.Options{
		Mode:    mode,
		NodeID:  b.nodeID,
		Dir:     filepath.Join(b.cfg.DataDir, "meta"),
		Applier: b,
		Logger:  b.log,
	}
	if b.clusterOn {
		transport, err := raft.NewTCPTransport(b.cfg.Cluster.Listen, b.nodeID, b.cfg.Cluster.Peers, b.log)
		if err != nil {
			return err
		}
		if err := b.serveForwardMethods(transport); err != nil {
			_ = transport.Close()
			return err
		}
		b.cluster = transport
		opt.Listen = b.cfg.Cluster.Listen
		opt.Peers = b.cfg.Cluster.Peers
		opt.Transport = transport
		// 地址簿（opt.Peers）与初始投票成员可能不同：以 learner 身份加入的节点，
		// 地址簿里必须有自己（否则连不上邻居），但它此时还不是投票成员。
		if b.cfg.Cluster.Join {
			opt.Voters = votersWithout(b.cfg.Cluster.Peers, b.nodeID)
			opt.Learners = []string{b.nodeID}
		}
	}
	st, err := meta.Open(context.Background(), opt)
	if err != nil {
		// 元数据层不会关闭注入的传输（借用而非拥有），失败路径要在这里回收监听。
		if b.cluster != nil {
			_ = b.cluster.Close()
			b.cluster = nil
		}
		return err
	}
	b.meta = st
	return nil
}

// votersWithout 返回地址簿里除 exclude 之外的节点 ID（加入模式的初始投票成员）。
func votersWithout(peers map[string]string, exclude string) []string {
	out := make([]string, 0, len(peers))
	for id := range peers {
		if id != exclude {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// ClusterStatus 返回集群/元数据层状态（管理面与 CLI 展示用）。
func (b *Broker) ClusterStatus() meta.Status {
	if b.meta == nil {
		return meta.Status{}
	}
	return b.meta.Status()
}

// ClusterEnabled 表示本节点是否以集群模式运行。
func (b *Broker) ClusterEnabled() bool { return b.clusterOn }

// ClusterPaused 表示节点是否因 pause_minority 暂停了服务。
func (b *Broker) ClusterPaused() bool { return b.clusterPaused.Load() }

// NodeID 返回本节点标识。
func (b *Broker) NodeID() string { return b.nodeID }

// checkServing 在集群暂停期间拒绝面向客户端的操作。
//
// 用 KindInternal（→ 541 INTERNAL_ERROR，关连接）而不是软错误：暂停是节点级故障，
// 让客户端整条连接失败并重连到健康节点，比让它在坏节点上逐个通道试错要好。
func (b *Broker) checkServing() error {
	if b.clusterPaused.Load() {
		return plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 节点已暂停服务（pause_minority：与多数派失联），请重连到集群中的其他节点")
	}
	return nil
}

// checkCluster 重新评估集群健康度，必要时按 pause_minority 暂停/恢复服务。
//
// 只在集群模式下生效，且只在 pause_minority 策略下暂停（ignore 策略只记录状态）。
func (b *Broker) checkCluster() {
	if !b.clusterOn || b.cfg.Cluster.PartitionPolicy != config.PartitionPauseMinority {
		return
	}
	st := b.ClusterStatus()
	paused := !st.HasQuorum
	if paused == b.clusterPaused.Load() {
		return
	}
	b.clusterPaused.Store(paused)
	if paused {
		b.log.Warn("与多数派失联，按 pause_minority 暂停服务",
			"node_id", st.NodeID, "term", st.Term, "leader", st.Leader)
		// 主动断开在途连接：客户端据此重连到健康节点，而不是留在暂停节点上空转。
		b.disconnectAll("CONNECTION_FORCED - node paused: lost quorum (pause_minority)")
		return
	}
	b.log.Info("已恢复与多数派的联系，服务恢复", "node_id", st.NodeID, "leader", st.Leader)
}

// disconnectAll 断开当前全部已达握手的连接（收集回调后逐个调用，避免持锁回调）。
func (b *Broker) disconnectAll(reason string) {
	b.connsMu.RLock()
	fns := make([]func(string), 0, len(b.conns))
	for _, entry := range b.conns {
		if entry.disconnect != nil {
			fns = append(fns, entry.disconnect)
		}
	}
	b.connsMu.RUnlock()
	for _, fn := range fns {
		fn(reason)
	}
}

// ---------------------------------------------------------------------------
// 写路径：提交元数据变更
// ---------------------------------------------------------------------------

// submitMeta 提交一次元数据变更：单机直接落盘，集群下 leader 直提、follower 转发 leader。
//
// 元数据层返回的是普通 error，这里统一收敛成 *plugin.Error —— 协议层依赖 Kind 决定
// 错误码与作用域，"内核返回错误一律是 *Error"这条契约不能破。
func (b *Broker) submitMeta(op meta.Op, payload any) error {
	ctx, cancel := context.WithTimeout(context.Background(), metaSubmitTimeout)
	defer cancel()
	if err := b.meta.Write(ctx, op, payload); err != nil {
		return plugin.Errorf(plugin.KindInternal,
			"INTERNAL_ERROR - 提交元数据变更 %s 失败: %v", op, err)
	}
	return nil
}

// awaitMeta 等待本地拓扑反映某次已提交的变更。
//
// 集群里 follower 的"提交成功"发生在 leader 侧，本地应用要等下一轮复制；
// 不一致会让紧随其后的 stats 查询看到"队列还不存在"，因此这里等到本地可见为止。
func (b *Broker) awaitMeta(present func() bool) error {
	deadline := time.Now().Add(metaApplyTimeout)
	for {
		if present() {
			return nil
		}
		if !time.Now().Before(deadline) {
			return plugin.Errorf(plugin.KindInternal,
				"INTERNAL_ERROR - 元数据变更已提交，但本地拓扑未在 %s 内更新", metaApplyTimeout)
		}
		time.Sleep(metaApplyPollInterval)
	}
}

// ---------------------------------------------------------------------------
// 成员变更（M6d）
// ---------------------------------------------------------------------------

// memberOpTimeout 是成员变更（含新成员追平）的上限。
//
// 比普通写入宽松：新节点要从零拉日志/快照，日志多时耗时不可忽略。
const memberOpTimeout = 90 * time.Second

// ClusterMembers 返回集群成员划分（单机模式为空）。
func (b *Broker) ClusterMembers() raft.Membership {
	if b.meta == nil {
		return raft.Membership{}
	}
	return b.meta.Membership()
}

// awaitMembership 等本地成员表反映某次已提交的成员变更（尽力而为）。
//
// 为什么需要它：成员变更可能在**别的节点**（leader）上完成，本节点要等下一轮复制
// 才看到结果。不等待的话，"加入成功"的应答里会带着一份**旧**成员表，运维会以为没生效。
// 超时只告警不报错：变更本身已经提交，本地视图落后不应让调用方以为操作失败。
func (b *Broker) awaitMembership(op string, cond func(raft.Membership) bool) {
	deadline := time.Now().Add(metaApplyTimeout)
	for {
		if cond(b.ClusterMembers()) {
			return
		}
		if !time.Now().Before(deadline) {
			b.log.Warn("成员变更已提交，但本地成员表未在超时内更新", "op", op, "timeout", metaApplyTimeout)
			return
		}
		time.Sleep(metaApplyPollInterval)
	}
}

func membershipHas(m raft.Membership, id string) bool {
	return hasVoter(m, id) || hasLearner(m, id)
}

// hasVoter 判断 id 是否为投票成员。
//
// 加入操作要等的是**提升完成**（而不是"已作为 learner 出现"）：否则应答里会带着
// 一份"还是 learner"的中间态，运维会以为加入只成功了一半。
func hasVoter(m raft.Membership, id string) bool {
	for _, v := range m.Voters {
		if v == id {
			return true
		}
	}
	return false
}

func hasLearner(m raft.Membership, id string) bool {
	for _, l := range m.Learners {
		if l == id {
			return true
		}
	}
	return false
}

// AddClusterMember 把一个节点加入集群：先作为 learner 加入、追平后提升为投票成员。
//
// 非 leader 节点会自动把请求转发给 leader，因此运维在任意节点上执行都可。
func (b *Broker) AddClusterMember(ctx context.Context, id, addr string) error {
	if !b.clusterOn || b.meta == nil {
		return plugin.Errorf(plugin.KindNotImplemented, "NOT_IMPLEMENTED - 单机模式没有集群成员可变更")
	}
	if id == "" {
		return plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - 成员 ID 不能为空")
	}
	ctx, cancel := context.WithTimeout(ctx, memberOpTimeout)
	defer cancel()
	if err := b.meta.AddMember(ctx, id, addr); err != nil {
		return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 加入集群成员 %s 失败: %v", id, err)
	}
	b.awaitMembership("add_member", func(m raft.Membership) bool { return hasVoter(m, id) })
	return nil
}

// RemoveClusterMember 把一个节点从集群移除。
func (b *Broker) RemoveClusterMember(ctx context.Context, id string) error {
	if !b.clusterOn || b.meta == nil {
		return plugin.Errorf(plugin.KindNotImplemented, "NOT_IMPLEMENTED - 单机模式没有集群成员可变更")
	}
	if id == "" {
		return plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - 成员 ID 不能为空")
	}
	ctx, cancel := context.WithTimeout(ctx, memberOpTimeout)
	defer cancel()
	if err := b.meta.RemoveMember(ctx, id); err != nil {
		return plugin.Errorf(plugin.KindInternal, "INTERNAL_ERROR - 移除集群成员 %s 失败: %v", id, err)
	}
	b.awaitMembership("remove_member", func(m raft.Membership) bool { return !membershipHas(m, id) })
	return nil
}

// ---------------------------------------------------------------------------
// 读侧：Applier 实现（meta.Applier）
// ---------------------------------------------------------------------------

// ApplyMeta 把一条已提交的元数据变更落到本地拓扑。
//
// 必须幂等：Raft 重放与快照恢复会重复应用同一批变更（meta/types.go 约定 2）。
func (b *Broker) ApplyMeta(op meta.Op, payload []byte) error {
	switch op {
	case meta.OpPutExchange:
		rec, err := decodeMeta[meta.Exchange](op, payload)
		if err != nil {
			return err
		}
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		v.applyExchangePut(rec)

	case meta.OpDeleteExchange:
		rec, err := decodeMeta[meta.Exchange](op, payload)
		if err != nil {
			return err
		}
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		v.applyExchangeDelete(rec.Name)

	case meta.OpPutQueue:
		rec, err := decodeMeta[meta.Queue](op, payload)
		if err != nil {
			return err
		}
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		return b.applyQueuePut(v, rec)

	case meta.OpDeleteQueue:
		rec, err := decodeMeta[meta.Queue](op, payload)
		if err != nil {
			return err
		}
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		v.applyQueueDelete(rec.Name)

	case meta.OpPutBinding:
		rec, err := decodeMeta[meta.Binding](op, payload)
		if err != nil {
			return err
		}
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		v.applyBindingPut(rec)

	case meta.OpDeleteBinding:
		rec, err := decodeMeta[meta.Binding](op, payload)
		if err != nil {
			return err
		}
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		v.applyBindingDelete(rec)

	case meta.OpPutVHost, meta.OpDeleteVHost:
		// vhost 集合由配置驱动（见本文件顶部说明）：元数据层里的 vhost 记录只做展示，
		// 不在本地增删 vhost —— 否则一个错误记录就能把正在服务的 vhost 摘掉。

	case meta.OpPutUser:
		rec, err := decodeMeta[meta.User](op, payload)
		if err != nil {
			return err
		}
		return b.auth.ApplyUser(rec.Name, config.User{
			Password:     rec.Password,
			Tags:         append([]string(nil), rec.Tags...),
			RemoteAccess: rec.RemoteAccess,
		})

	case meta.OpDeleteUser:
		rec, err := decodeMeta[meta.User](op, payload)
		if err != nil {
			return err
		}
		b.auth.ApplyDeleteUser(rec.Name)

	case meta.OpPutPermission:
		rec, err := decodeMeta[meta.Permission](op, payload)
		if err != nil {
			return err
		}
		if !b.auth.ApplyPermission(rec.User, rec.VHost, config.Permission{
			Configure: rec.Configure, Write: rec.Write, Read: rec.Read,
		}) {
			// 用户还不存在：跳过而不是报错。返回错误会让 Raft 反复重放这条日志，
			// 而"先给权限、后建用户"在重放与快照恢复里都是合法顺序。
			b.log.Warn("元数据中的权限引用了不存在的用户，已跳过",
				"user", rec.User, "vhost", rec.VHost)
		}

	case meta.OpDeletePermission:
		rec, err := decodeMeta[meta.Permission](op, payload)
		if err != nil {
			return err
		}
		b.auth.ApplyDeletePermission(rec.User, rec.VHost)

	default:
		return fmt.Errorf("未知的元数据操作 %q", op)
	}
	return nil
}

// RestoreMeta 用完整快照重建本地拓扑。
//
// 拓扑只做**增量补齐**、不删除本地已有实体：transient / exclusive 队列不属于元数据，
// 按快照删除会把它们连同会话状态一起抹掉。启动时本地为空，因此"补齐"等价于"重建"。
//
// 账号与权限不同：它们在元数据里是**唯一权威**，因此按快照**整体替换** ——
// 不替换的话，运行期删掉的账号会在重启后从配置里"复活"。
// 唯一的例外是空快照：单机新装 / 新集群首次启动时元数据里本来就没有账号，
// 此时保留配置带来的初始账号，由 bootstrapUsers 把它们写进元数据。
func (b *Broker) RestoreMeta(state meta.State) error {
	if len(state.Users) > 0 {
		b.auth.ReplaceUsers(restoredUsers(state))
	}
	for _, rec := range sortedExchanges(state) {
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		v.applyExchangePut(rec)
	}
	for _, rec := range sortedQueues(state) {
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		if err := b.applyQueuePut(v, rec); err != nil {
			return err
		}
	}
	for _, rec := range sortedBindings(state) {
		v, err := b.vhostForMeta(rec.VHost)
		if err != nil {
			return err
		}
		v.applyBindingPut(rec)
	}
	return nil
}

// applyQueuePut 在 vhost 上重建一个由元数据描述的队列（幂等）。
//
// 三种形态：
//   - 仲裁队列：每个节点都为它启动一份 Raft 组，消息由日志复制（没有独立 store）；
//   - 经典队列且 Owner 是本节点：打开消息存储并恢复消息；
//   - 经典队列且 Owner 是别的节点：只建"占位队列"（拓扑可见、绑定可解析），
//     数据操作经 M6b 的转发层交给 Owner。
func (b *Broker) applyQueuePut(v *vhost, rec meta.Queue) error {
	if _, ok := v.getQueue(rec.Name); ok {
		return nil
	}
	args, err := parseQueueArgs(rec.Arguments)
	if err != nil {
		return fmt.Errorf("元数据中的队列 %s/%s 参数非法: %w", rec.VHost, rec.Name, err)
	}
	q := newQueue(rec.Name, rec.Durable, rec.Exclusive, rec.AutoDelete, "", rec.Arguments, args, v.log)
	q.nodeOwner = rec.Owner
	// 仲裁队列没有固定 Owner（服务节点是 Raft leader，会变），因此不能按 Owner 判远端。
	q.remote = args.queueType != queueTypeQuorum && rec.Owner != "" && rec.Owner != b.nodeID
	v.wireDeadLetter(q, args)

	if args.queueType == queueTypeQuorum {
		if err := b.startQuorumGroup(v, q); err != nil {
			return fmt.Errorf("启动仲裁队列 %s/%s 的 Raft 组失败: %w", rec.VHost, rec.Name, err)
		}
		if !v.addQueueObject(q, args) {
			q.quorum.stop()
			return nil
		}
		return nil
	}

	if !q.remote && v.stores != nil {
		st, recovered, err := v.stores.Open(v.name, rec.Name, rec.Durable)
		if err != nil {
			return fmt.Errorf("打开队列 %s/%s 的存储失败: %w", rec.VHost, rec.Name, err)
		}
		q.store = st
		q.restore(recovered)
	}
	if !v.addQueueObject(q, args) {
		// 并发路径已创建同名队列：只回收自己刚打开的存储，绝不动磁盘数据。
		q.discardStore()
		return nil
	}
	v.log.Debug("队列已按元数据建立", "queue", rec.Name, "type", args.queueType,
		"owner", rec.Owner, "remote", q.remote)
	return nil
}

// applyQueueDelete 按元数据删除本地队列（幂等）。
func (v *vhost) applyQueueDelete(name string) {
	q, ok := v.getQueue(name)
	if !ok {
		return
	}
	v.removeQueue(q)
}

// deleteManagedQueue 提交"删除集群托管队列"的元数据变更（供 x-expires 等内部删除路径使用）。
func (b *Broker) deleteManagedQueue(vhost, queue string) error {
	return b.submitMeta(meta.OpDeleteQueue, meta.Queue{VHost: vhost, Name: queue})
}

// queueOwner 返回本节点声明的队列在元数据里的 Owner 标识。
//
// 单机模式返回空串：这样同一份 state.json 日后切到集群模式时，
// 不会被误判成"消息在别的节点上"的远端队列。
func (b *Broker) queueOwner() string {
	if b.clusterOn {
		return b.nodeID
	}
	return ""
}

// bindingsForExchange 返回元数据中与某交换机相关的全部绑定
// （以它为 source，或以它为 destination 的交换机绑定）。
//
// 删除 durable 交换机时用它做级联清理：本地绑定会随本地交换机一起消失，
// 但元数据里的记录不会 —— 不清理就会不断堆积悬空绑定。
func (b *Broker) bindingsForExchange(vhost, exchange string) []meta.Binding {
	if b.meta == nil {
		return nil
	}
	var out []meta.Binding
	for _, rec := range b.meta.State().Bindings {
		if rec.VHost != vhost {
			continue
		}
		if rec.Source == exchange ||
			(rec.DestinationType == destinationExchange && rec.Destination == exchange) {
			out = append(out, rec)
		}
	}
	return out
}

// vhostForMeta 取本地 vhost；不存在即报错。
//
// 集群各节点的 vhost 配置必须一致：不一致会让元数据引用的对象无处安放，
// 这里明确失败比"跳过这条记录"更安全 —— 后者会让各节点状态悄悄分叉。
func (b *Broker) vhostForMeta(name string) (*vhost, error) {
	v, ok := b.vhosts[name]
	if !ok {
		return nil, fmt.Errorf("元数据引用了本节点不存在的 vhost %q（集群各节点的 vhost 配置必须一致）", name)
	}
	return v, nil
}

// decodeMeta 把一条 Op 的 payload 解析成对应记录类型。
func decodeMeta[T any](op meta.Op, payload []byte) (T, error) {
	var rec T
	if len(payload) == 0 {
		return rec, fmt.Errorf("元数据操作 %s 缺少记录体", op)
	}
	if err := json.Unmarshal(payload, &rec); err != nil {
		return rec, fmt.Errorf("解析 %s 的记录失败: %w", op, err)
	}
	return rec, nil
}

// restoredUsers 把快照里的用户与权限拼回 auth.Store 的记录形态。
//
// 元数据里"用户"与"权限"是两组平铺记录，而 auth.Store 把权限挂在用户记录下；
// 在这里合回一层，是为了不让 auth 再维护一份"权限属于谁"的映射（两处存储必然两处不一致）。
func restoredUsers(state meta.State) map[string]config.User {
	out := make(map[string]config.User, len(state.Users))
	for name, u := range state.Users {
		out[name] = config.User{
			Password:     u.Password,
			Tags:         append([]string(nil), u.Tags...),
			RemoteAccess: u.RemoteAccess,
			Permissions:  map[string]config.Permission{},
		}
	}
	for _, p := range state.Permissions {
		u, ok := out[p.User]
		if !ok {
			continue // 权限引用了不存在的用户：删用户时权限记录会被一并清掉
		}
		u.Permissions[p.VHost] = config.Permission{
			Configure: p.Configure, Write: p.Write, Read: p.Read,
		}
		out[p.User] = u
	}
	return out
}

// userBootstrapRetry 是首次引导播种账号的重试间隔。
//
// 比普通写入宽松：集群刚起来时可能还没选出 leader，重试几次比让启动失败更好 ——
// 一个"等 2 秒没把 guest 写进去"的启动错误，会把新集群永久卡在无人能登录的状态。
const userBootstrapRetry = time.Second

// userSeedMark 是"初始账号已处理过"的标记文件（<data_dir>/meta/users.seeded）。
//
// 判据**不能**是"元数据里有没有账号"：集群模式下启动后的日志重放是**异步**的，
// 刚起来的那一瞬间元数据看起来就是空的 —— 拿配置去播种，会把运行期删掉的账号写回日志尾部，
// 等于让删除失效。标记文件把"本节点是否做过首次引导"变成一个与重放进度无关的确定事实。
//
// 它只影响**引导**：删掉这个文件，下次启动会重新把配置里的账号补齐到元数据（幂等）。
const userSeedMark = "users.seeded"

// bootstrapUsers 把配置里的初始账号写进元数据，随后退出。
//
// 与 cluster.peers 同一约定：**配置文件只负责首次引导**，此后账号以元数据为准 ——
// 否则改密码要靠改配置文件 + 重启，而且各节点配置漂移时会各说各话。
func (b *Broker) bootstrapUsers(ctx context.Context) {
	if b.meta == nil {
		return
	}
	mark := filepath.Join(b.cfg.DataDir, "meta", userSeedMark)
	if _, err := os.Stat(mark); err == nil {
		return // 本节点已引导过：此后账号以元数据为准
	}
	if b.cfg.Cluster.Join {
		// 以 learner 身份加入既有集群：账号由 leader 的日志/快照带过来，
		// 本地配置不参与播种（各节点配置漂移时，播种会覆盖集群里已有的口令）。
		b.log.Info("以 learner 身份加入既有集群，账号由集群元数据接管，不播种本地配置")
		b.writeSeedMark(mark)
		return
	}
	for {
		if err := b.seedConfigUsers(); err != nil {
			b.log.Warn("初始账号写入元数据失败，稍后重试", "err", err, "retry_in", userBootstrapRetry)
		} else {
			b.log.Info("已把配置里的初始账号写入元数据（此后账号以元数据为准）",
				"users", len(b.cfg.Users))
			b.writeSeedMark(mark)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(userBootstrapRetry):
		}
	}
}

// writeSeedMark 写下引导标记。失败只告警：下次启动会重新播种一遍（幂等），
// 因此不值得为此让内核启动失败。
func (b *Broker) writeSeedMark(path string) {
	if err := os.WriteFile(path, []byte("已按配置完成一次初始账号引导\n"), 0o600); err != nil {
		b.log.Warn("写入账号引导标记失败（下次启动会重新引导一次）", "path", path, "err", err)
	}
}

// seedConfigUsers 提交配置里的全部账号（幂等：重复提交同一份记录不改变最终状态）。
func (b *Broker) seedConfigUsers() error {
	for name, u := range b.cfg.Users {
		rec := meta.User{
			Name:         name,
			Password:     u.Password,
			Tags:         append([]string(nil), u.Tags...),
			RemoteAccess: u.RemoteAccess,
		}
		if err := b.submitMeta(meta.OpPutUser, rec); err != nil {
			return err
		}
		// 配置文件里的权限也一并播种：否则首次启动后"配置里写了权限"与"实际生效"不一致。
		for vhost, p := range u.Permissions {
			if err := b.submitMeta(meta.OpPutPermission, meta.Permission{
				User: name, VHost: vhost,
				Configure: p.Configure, Write: p.Write, Read: p.Read,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// vhost 级应用：把一条元数据记录作用到本地拓扑
// ---------------------------------------------------------------------------

// applyExchangePut 建立交换机（幂等）。
func (v *vhost) applyExchangePut(rec meta.Exchange) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.exchanges[rec.Name]; ok {
		return
	}
	v.exchanges[rec.Name] = newExchange(rec.Name, plugin.ExchangeType(rec.Type),
		rec.Durable, rec.AutoDelete, rec.Internal, rec.Arguments)
}

// applyExchangeDelete 删除交换机并清理指向它的绑定（幂等）。
func (v *vhost) applyExchangeDelete(name string) {
	if name == defaultExchange {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.exchanges, name)
	for _, other := range v.exchanges {
		other.removeExchangeBindings(name)
	}
}

// applyBindingPut 建立绑定（幂等；端点缺失时只告警不失败，避免一条记录拖垮整个应用循环）。
func (v *vhost) applyBindingPut(rec meta.Binding) {
	ex, ok := v.getExchange(rec.Source)
	if !ok {
		v.log.Warn("元数据中的绑定引用了不存在的交换机，已跳过",
			"source", rec.Source, "destination", rec.Destination)
		return
	}
	if rec.DestinationType == destinationExchange {
		ex.addExchangeBinding(rec.RoutingKey, rec.Destination, rec.Arguments)
		return
	}
	ex.addBinding(rec.RoutingKey, rec.Destination, rec.Arguments)
}

// applyBindingDelete 解除绑定（幂等）。
func (v *vhost) applyBindingDelete(rec meta.Binding) {
	ex, ok := v.getExchange(rec.Source)
	if !ok {
		return
	}
	if rec.DestinationType == destinationExchange {
		ex.removeExchangeBinding(rec.RoutingKey, rec.Destination)
		return
	}
	ex.removeBinding(rec.RoutingKey, rec.Destination)
}

// addQueueObject 把队列加入 vhost 并登记定时扫描；返回 false 表示同名队列已存在。
func (v *vhost) addQueueObject(q *queue, args queueArgs) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, dup := v.queues[q.name]; dup {
		return false
	}
	v.queues[q.name] = q
	if args.messageTTL > 0 || args.expires > 0 {
		v.sweepSet[q.name] = q
	}
	return true
}

// ---------------------------------------------------------------------------
// 稳定顺序（状态恢复用）
// ---------------------------------------------------------------------------

func sortedExchanges(state meta.State) []meta.Exchange {
	out := make([]meta.Exchange, 0, len(state.Exchanges))
	for _, rec := range state.Exchanges {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].VHost != out[j].VHost {
			return out[i].VHost < out[j].VHost
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func sortedQueues(state meta.State) []meta.Queue {
	out := make([]meta.Queue, 0, len(state.Queues))
	for _, rec := range state.Queues {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].VHost != out[j].VHost {
			return out[i].VHost < out[j].VHost
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func sortedBindings(state meta.State) []meta.Binding {
	out := append([]meta.Binding(nil), state.Bindings...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].VHost != out[j].VHost {
			return out[i].VHost < out[j].VHost
		}
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		if out[i].Destination != out[j].Destination {
			return out[i].Destination < out[j].Destination
		}
		return out[i].RoutingKey < out[j].RoutingKey
	})
	return out
}
