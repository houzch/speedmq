package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
// vhost 集合自 M8-7 起**可在运行期增删**：配置文件里的 vhosts 只负责首次引导，
// 此后 vhost 集合以元数据为准（与账号同一约定，见 bootstrapVHosts）—— 否则一个
// 运行期删掉的 vhost 会在下次重启时从配置里"复活"，而且各节点的集合会悄悄分叉。
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

	case meta.OpPutVHost:
		rec, err := decodeMeta[meta.VHost](op, payload)
		if err != nil {
			return err
		}
		b.addVHost(rec.Name)

	case meta.OpDeleteVHost:
		rec, err := decodeMeta[meta.VHost](op, payload)
		if err != nil {
			return err
		}
		b.applyVHostDelete(rec.Name)

	case meta.OpPutUser:
		rec, err := decodeMeta[meta.User](op, payload)
		if err != nil {
			return err
		}
		return b.auth.ApplyUser(rec.Name, config.User{
			Password:           rec.Password,
			Tags:               append([]string(nil), rec.Tags...),
			RemoteAccess:       rec.RemoteAccess,
			Root:               rec.Root,
			Disabled:           rec.Disabled,
			MustChangePassword: rec.MustChangePassword,
			APIGroups:          append([]string(nil), rec.APIGroups...),
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

	case meta.OpPutPolicy, meta.OpDeletePolicy:
		rec, err := decodeMeta[meta.Policy](op, payload)
		if err != nil {
			return err
		}
		// 策略是"事后施加"的：重建集合后必须回到已有对象上重算，否则运维改了策略
		// 却要等队列重建才生效。重建用的是**已提交**的元数据状态（此时状态机已应用本条目）。
		b.refreshPolicies(b.metaState())
		b.log.Debug("策略已应用", "op", string(op), "vhost", rec.VHost, "policy", rec.Name)

	case meta.OpPutVHostLimit, meta.OpDeleteVHostLimit:
		// vhost 限制与特性开关都是**按需读取**的：判定点在 checkQueueLimit /
		// checkConnectionLimit / featureEnabled 里直接查元数据状态，本节点没有
		// 需要维护的派生缓存，因此应用这一步是空的（幂等性由"集合语义"天然满足）。
		rec, err := decodeMeta[meta.VHostLimit](op, payload)
		if err != nil {
			return err
		}
		b.log.Debug("vhost 限制已应用", "op", string(op), "vhost", rec.VHost, "limit", rec.Name)

	case meta.OpPutFeatureFlag:
		rec, err := decodeMeta[meta.FeatureFlag](op, payload)
		if err != nil {
			return err
		}
		b.log.Info("特性开关已变更", "flag", rec.Name, "enabled", rec.Enabled)

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
	// vhost 集合先对齐：元数据里有记录时它**就是**权威（新增的补建、已删除的摘掉），
	// 这样运行期增删的 vhost 才能随重启/集群复制正确重建。
	b.syncVHostsFromMeta(state.VHosts)
	if len(state.Users) > 0 {
		b.auth.ReplaceUsers(restoredUsers(state))
		// 升级兜底：老部署的元数据里没有 root 标记，这里按配置里的初始账号名补上，
		// 否则"总管理员"保护对既有部署完全不生效。
		//
		// 只在**内存视图**上补，不写元数据：集群下这个视图可能还没追平，拿它回写会覆盖他人的真实记录；
		// 内存兜底则是幂等的 —— 每次启动按同一条规则重算，结果一致。
		b.adoptConfigRootUser()
	}
	// 策略要先建好集合：下面重建队列时就要按它算出生效参数，否则重启后的队列会丢掉策略。
	set := buildPolicySet(state, nil)
	b.policies.Store(&set)
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
	// 拓扑建完后再统一落一遍策略：队列的生效参数在 applyQueuePut 里已经按策略算过，
	// 这一步补上交换机侧的 alternate-exchange 与队列的策略展示信息。
	for _, v := range b.vhostList() {
		b.applyPoliciesForVHost(v.name)
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
	args, polName, polDef, err := b.queuePolicyArgs(rec.VHost, rec.Name, rec.Arguments)
	if err != nil {
		return fmt.Errorf("元数据中的队列 %s/%s 参数非法: %w", rec.VHost, rec.Name, err)
	}
	q := newQueue(rec.Name, rec.Durable, rec.Exclusive, rec.AutoDelete, "", rec.Arguments, args, v.log)
	q.applyPolicy(args, polName, polDef)
	q.nodeOwner = rec.Owner
	// 仲裁队列没有固定 Owner（服务节点是 Raft leader，会变），因此不能按 Owner 判远端。
	q.remote = args.queueType != queueTypeQuorum && rec.Owner != "" && rec.Owner != b.nodeID
	v.wireDeadLetter(q, args)

	if args.queueType == queueTypeQuorum {
		if err := b.startQuorumGroup(v, q, rec); err != nil {
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
	v, ok := b.vhostOf(name)
	if !ok {
		return nil, fmt.Errorf("元数据引用了本节点不存在的 vhost %q（集群各节点的 vhost 配置必须一致）", name)
	}
	return v, nil
}

// ---------------------------------------------------------------------------
// vhost 集合（M8-7）
// ---------------------------------------------------------------------------

// syncVHostsFromMeta 让本地 vhost 集合与元数据对齐。
//
// 空集合时**不**做任何删除：那说明元数据里还没有 vhost 记录（首次启动，或从 M8-7 之前的
// 版本升级上来），此时应当保留配置带来的 vhost，由 bootstrapVHosts 把它们播种进元数据。
// 一旦元数据里有记录，它即权威 —— 运行期删掉的 vhost 不会在重启后从配置里复活。
//
// 默认 vhost 是唯一例外：它永不被摘掉。它是内核保证存在的连接落点（见 New），
// 摘掉它会让所有使用默认 vhost 的客户端直接连不上。
func (b *Broker) syncVHostsFromMeta(vhosts map[string]meta.VHost) {
	if len(vhosts) == 0 {
		return
	}
	for _, rec := range vhosts {
		b.addVHost(rec.Name)
	}
	for _, name := range b.VHostNames() {
		if _, ok := vhosts[name]; ok {
			continue
		}
		if name == b.cfg.DefaultVHost {
			continue
		}
		b.log.Info("vhost 不在元数据中（已被删除），本节点不再提供它", "vhost", name)
		b.applyVHostDelete(name)
	}
}

// applyVHostDelete 在本地移除一个 vhost：摘掉登记、断开使用它的连接、关闭其队列、删磁盘目录。
//
// 幂等：vhost 本就不存在时直接返回（Raft 重放与快照恢复会重复应用同一批 Op）。
func (b *Broker) applyVHostDelete(name string) {
	v := b.dropVHost(name)
	if v == nil {
		return
	}
	// 先断开使用它的连接：否则会话仍持有这个已摘除的 vhost，能在被删的拓扑上继续操作。
	b.disconnectVHostConns(name)
	v.teardown()
	if b.stores != nil {
		if err := b.stores.RemoveVHost(name); err != nil {
			b.log.Warn("删除 vhost 的存储目录失败", "vhost", name, "err", err)
		}
	}
	b.log.Info("vhost 已删除", "vhost", name)
}

// disconnectVHostConns 断开当前打开着指定 vhost 的连接。
//
// 收集回调后在锁外逐个调用：disconnect 会走到协议层的关闭流程，持锁调用有死锁风险。
func (b *Broker) disconnectVHostConns(vhost string) {
	b.connsMu.RLock()
	fns := make([]func(string), 0)
	for _, e := range b.conns {
		if e.vhost == vhost && e.disconnect != nil {
			fns = append(fns, e.disconnect)
		}
	}
	b.connsMu.RUnlock()
	reason := fmt.Sprintf("CONNECTION_FORCED - vhost '%s' was deleted", vhost)
	for _, fn := range fns {
		fn(reason)
	}
}

// CreateVHost 新建一个 vhost（幂等：已存在时重写同一份记录，不改变结果）。
//
// 走元数据层提交：集群下经 Raft 复制到全体节点，单机下落盘，因此运行期新建的 vhost
// 不会随进程退出而消失。
func (b *Broker) CreateVHost(name string) error {
	if strings.TrimSpace(name) == "" {
		return plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - vhost 名不能为空")
	}
	rec := meta.VHost{Name: name, CreatedAt: time.Now().UTC()}
	if err := b.submitMeta(meta.OpPutVHost, rec); err != nil {
		return err
	}
	return b.awaitMeta(func() bool { return b.VHostExists(name) })
}

// DeleteVHost 删除一个 vhost 及其全部内容。返回是否命中。
//
// 级联是**显式**的：元数据层的 delete 不级联（见 meta/fsm.go），因此这里把 vhost 内的
// 队列 / 交换机 / 绑定 / 权限 / 策略逐条提交删除，最后才删 vhost 本身 ——
// 只删 vhost 会在元数据里留下一堆指向已删 vhost 的悬空记录。
//
// 默认 vhost 拒绝删除：它是内核保证存在的落点，删掉会让所有默认 vhost 客户端立刻失联。
func (b *Broker) DeleteVHost(name string) (bool, error) {
	if name == b.cfg.DefaultVHost {
		return false, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 不能删除默认 vhost '%s'", name)
	}
	if !b.VHostExists(name) {
		return false, nil
	}
	// 先**快照**出该 vhost 的全部从属记录，再逐条提交。
	//
	// 不能边遍历元数据状态边提交：ModeRaft 下"提交成功"与"本地应用"发生在不同协程，
	// 遍历过程中状态 map 会被应用协程改写 —— 并发遍历+写 map 是未定义行为（可能直接 panic）。
	st := b.metaState()
	var queues []meta.Queue
	for _, rec := range st.Queues {
		if rec.VHost == name {
			queues = append(queues, rec)
		}
	}
	var exchanges []meta.Exchange
	for _, rec := range st.Exchanges {
		if rec.VHost == name {
			exchanges = append(exchanges, rec)
		}
	}
	var bindings []meta.Binding
	for _, rec := range st.Bindings {
		if rec.VHost == name {
			bindings = append(bindings, rec)
		}
	}
	var permissions []meta.Permission
	for _, rec := range st.Permissions {
		if rec.VHost == name {
			permissions = append(permissions, rec)
		}
	}
	var policies []meta.Policy
	for _, rec := range st.Policies {
		if rec.VHost == name {
			policies = append(policies, rec)
		}
	}
	var limits []meta.VHostLimit
	for _, rec := range st.Limits {
		if rec.VHost == name {
			limits = append(limits, rec)
		}
	}

	for _, rec := range queues {
		if err := b.submitMeta(meta.OpDeleteQueue, meta.Queue{VHost: name, Name: rec.Name}); err != nil {
			return false, err
		}
	}
	for _, rec := range exchanges {
		if err := b.submitMeta(meta.OpDeleteExchange, meta.Exchange{VHost: name, Name: rec.Name}); err != nil {
			return false, err
		}
	}
	for _, rec := range bindings {
		if err := b.submitMeta(meta.OpDeleteBinding, rec); err != nil {
			return false, err
		}
	}
	for _, rec := range permissions {
		if err := b.submitMeta(meta.OpDeletePermission, meta.Permission{User: rec.User, VHost: name}); err != nil {
			return false, err
		}
	}
	for _, rec := range policies {
		if err := b.submitMeta(meta.OpDeletePolicy, meta.Policy{Name: rec.Name, VHost: name}); err != nil {
			return false, err
		}
	}
	for _, rec := range limits {
		if err := b.submitMeta(meta.OpDeleteVHostLimit, meta.VHostLimit{VHost: name, Name: rec.Name}); err != nil {
			return false, err
		}
	}
	if err := b.submitMeta(meta.OpDeleteVHost, meta.VHost{Name: name}); err != nil {
		return false, err
	}
	if err := b.awaitMeta(func() bool { return !b.VHostExists(name) }); err != nil {
		return false, err
	}
	return true, nil
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
			Password:           u.Password,
			Tags:               append([]string(nil), u.Tags...),
			RemoteAccess:       u.RemoteAccess,
			Root:               u.Root,
			Disabled:           u.Disabled,
			MustChangePassword: u.MustChangePassword,
			APIGroups:          append([]string(nil), u.APIGroups...),
			Permissions:        map[string]config.Permission{},
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
	if err := os.WriteFile(path, []byte("已按配置完成一次首次引导\n"), 0o600); err != nil {
		b.log.Warn("写入引导标记失败（下次启动会重新引导一次）", "path", path, "err", err)
	}
}

// seedConfigUsers 提交配置里的全部账号（幂等：重复提交同一份记录不改变最终状态）。
//
// 首次播种顺带定下**总管理员账号**：配置里显式标了 root 的优先，否则取第一个 administrator。
// 只有在这里（以及 RestoreMeta 的升级兜底）会置 root 标记 —— 管理 API 不能把账号提升为 root，
// 否则"总账号"会退化成一个人人可加的标签，保护规则随之失效。
//
// 总账号同时被置上 must_change_password：**新装实例出厂即默认口令**，
// 第一次登录必须先把账号名与口令改掉（管理 UI 据此强制弹窗）。
func (b *Broker) seedConfigUsers() error {
	rootName := b.configRootUserName()
	for name, u := range b.cfg.Users {
		isRoot := name == rootName
		rec := meta.User{
			Name:               name,
			Password:           u.Password,
			Tags:               append([]string(nil), u.Tags...),
			RemoteAccess:       u.RemoteAccess,
			Root:               isRoot,
			MustChangePassword: isRoot,
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
	if rootName == "" {
		b.log.Warn("配置里的初始账号中没有 administrator，将没有总管理员账号；请通过管理面创建并指派")
	}
	return nil
}

// vhostSeedMark 是"配置里的初始 vhost 已处理过"的标记文件（<data_dir>/meta/vhosts.seeded）。
//
// 判据与账号播种同一理由（见 userSeedMark）：集群模式下启动重放是异步的，
// 用"元数据里有没有 vhost"当判据会在刚启动时误判为空，把配置里的 vhost 重新写回日志尾部，
// 使"运行期删掉的 vhost"复活。标记文件把"本节点是否做过首次引导"变成一个确定事实。
const vhostSeedMark = "vhosts.seeded"

// bootstrapVHosts 把配置里的初始 vhost 写进元数据，随后退出。
//
// 与账号、cluster.peers 同一约定：**配置文件只负责首次引导**，此后 vhost 集合以元数据为准。
// 因此"给已有实例新增一个 vhost"要经管理 API / swiftmqctl 做，而不是改配置 —— 改配置不会生效，
// 这一点在 README 的 vhost 说明里写明。
func (b *Broker) bootstrapVHosts(ctx context.Context) {
	if b.meta == nil {
		return
	}
	mark := filepath.Join(b.cfg.DataDir, "meta", vhostSeedMark)
	if _, err := os.Stat(mark); err == nil {
		return // 本节点已引导过：此后 vhost 集合以元数据为准
	}
	if b.cfg.Cluster.Join {
		// 以 learner 身份加入既有集群：vhost 由集群元数据带过来，本地配置不参与播种
		// （各节点配置漂移时，播种会凭空造出集群里没有的 vhost）。
		b.log.Info("以 learner 身份加入既有集群，vhost 由集群元数据接管，不播种本地配置")
		b.writeSeedMark(mark)
		return
	}
	for {
		if err := b.seedConfigVHosts(); err != nil {
			b.log.Warn("初始 vhost 写入元数据失败，稍后重试", "err", err, "retry_in", userBootstrapRetry)
		} else {
			b.log.Info("已把配置里的 vhost 写入元数据（此后 vhost 集合以元数据为准）",
				"vhosts", b.VHostNames())
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

// seedConfigVHosts 把本地已有的 vhost（来自配置首建）补进元数据；已在元数据里的跳过。
//
// 跳过已存在的记录既避免了无谓的日志写入，也让"重跑播种"真正幂等 ——
// 记录里带 CreatedAt，重复覆盖会让它在集群各节点/各次重启间漂移。
func (b *Broker) seedConfigVHosts() error {
	existing := b.metaState().VHosts
	for _, name := range b.VHostNames() {
		if _, ok := existing[name]; ok {
			continue
		}
		if err := b.submitMeta(meta.OpPutVHost, meta.VHost{Name: name, CreatedAt: time.Now().UTC()}); err != nil {
			return err
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
	// 注：策略（alternate-exchange）不在这里设 —— RestoreMeta 末尾会统一落一遍，
	// 那时策略集合已建好；此处再查一次只会多一次重复计算。
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
