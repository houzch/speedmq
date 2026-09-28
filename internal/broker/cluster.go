package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/meta"
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
		opt.Listen = b.cfg.Cluster.Listen
		opt.Peers = b.cfg.Cluster.Peers
	}
	st, err := meta.Open(context.Background(), opt)
	if err != nil {
		return err
	}
	b.meta = st
	return nil
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

// remoteQueueErr 是"队列数据不在本节点"的统一错误。
//
// 本期只有队列 Owner 节点持有消息数据，跨节点转发尚未实现 —— 与其静默丢消息，
// 不如明确报 NOT_IMPLEMENTED（→ 540，关连接），让调用方知道该连到 Owner 节点。
func remoteQueueErr(vhost, queue string) error {
	return plugin.Errorf(plugin.KindNotImplemented,
		"NOT_IMPLEMENTED - queue '%s' in vhost '%s' 的消息数据不在本节点（集群跨节点转发尚未实现，请连接队列 Owner 节点）",
		queue, vhost)
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

	case meta.OpPutUser, meta.OpDeleteUser, meta.OpPutPermission, meta.OpDeletePermission:
		// 账号与权限本期仍由本地 auth.Store 承载（不回写元数据），见 M6 交付说明的"未完成项"。

	default:
		return fmt.Errorf("未知的元数据操作 %q", op)
	}
	return nil
}

// RestoreMeta 用完整快照重建本地拓扑。
//
// 只做**增量补齐**、不删除本地已有实体：transient / exclusive 队列不属于元数据，
// 按快照删除会把它们连同会话状态一起抹掉。启动时本地为空，因此"补齐"等价于"重建"。
func (b *Broker) RestoreMeta(state meta.State) error {
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
// Owner 是本节点时打开消息存储并恢复消息；Owner 是别的节点时只建一个"占位队列"：
// 拓扑上可见（供管理面与绑定解析），但消息操作一律返回 NOT_IMPLEMENTED。
func (b *Broker) applyQueuePut(v *vhost, rec meta.Queue) error {
	if _, ok := v.getQueue(rec.Name); ok {
		return nil
	}
	args, err := parseQueueArgs(rec.Arguments)
	if err != nil {
		return fmt.Errorf("元数据中的队列 %s/%s 参数非法: %w", rec.VHost, rec.Name, err)
	}
	q := newQueue(rec.Name, rec.Durable, rec.Exclusive, rec.AutoDelete, "", rec.Arguments, args, v.log)
	q.remote = rec.Owner != "" && rec.Owner != b.nodeID
	v.wireDeadLetter(q, args)

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
	v.log.Debug("队列已按元数据建立", "queue", rec.Name, "owner", rec.Owner, "remote", q.remote)
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
