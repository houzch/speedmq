package broker

import (
	"sort"

	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件实现 vhost 级限制（对齐 RabbitMQ 的 vhost-limits）。
//
// 与策略（policy.go）的关系：策略决定"某个队列/交换机长什么样"，限制决定
// "这个 vhost 最多能有多少个队列 / 多少条连接" —— 前者是对象属性，后者是容量上限，
// 因此单独一张表、单独一组接口。
//
// 限制记录放在元数据层：单机落 state.json、集群经 Raft 复制、重启后保留。

// 支持的限制名（与 RabbitMQ 同名）。
const (
	LimitMaxConnections = "max-connections"
	LimitMaxQueues      = "max-queues"
)

// LimitNames 返回全部支持的限制名（顺序即管理 UI 的展示顺序）。
func LimitNames() []string {
	return []string{LimitMaxConnections, LimitMaxQueues}
}

// limitDescription 返回限制的中文说明（管理 UI 与错误提示共用）。
func limitDescription(name string) string {
	switch name {
	case LimitMaxConnections:
		return "该 vhost 允许的最大连接数"
	case LimitMaxQueues:
		return "该 vhost 允许的最大队列数"
	default:
		return ""
	}
}

// ---------------------------------------------------------------------------
// 读路径
// ---------------------------------------------------------------------------

// VHostLimitSnapshot 是限制的只读视图。
type VHostLimitSnapshot struct {
	VHost string
	Name  string
	Value int
}

// VHostLimits 返回全部 vhost 的限制（按 vhost、限制名排序）。
func (b *Broker) VHostLimits() []VHostLimitSnapshot {
	st := b.metaState()
	out := make([]VHostLimitSnapshot, 0, len(st.Limits))
	for _, rec := range st.Limits {
		out = append(out, VHostLimitSnapshot{VHost: rec.VHost, Name: rec.Name, Value: rec.Value})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].VHost != out[j].VHost {
			return out[i].VHost < out[j].VHost
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// VHostLimitsOf 返回某个 vhost 的限制。
func (b *Broker) VHostLimitsOf(vhost string) []VHostLimitSnapshot {
	var out []VHostLimitSnapshot
	for _, item := range b.VHostLimits() {
		if item.VHost == vhost {
			out = append(out, item)
		}
	}
	return out
}

// VHostLimit 返回单条限制。
func (b *Broker) VHostLimit(vhost, name string) (VHostLimitSnapshot, bool) {
	rec, ok := b.metaState().Limits[meta.LimitKey(vhost, name)]
	if !ok {
		return VHostLimitSnapshot{}, false
	}
	return VHostLimitSnapshot{VHost: rec.VHost, Name: rec.Name, Value: rec.Value}, true
}

// vhostLimitValue 返回某条限制的值；未设置时 ok 为 false。
func (b *Broker) vhostLimitValue(vhost, name string) (int, bool) {
	rec, ok := b.metaState().Limits[meta.LimitKey(vhost, name)]
	if !ok {
		return 0, false
	}
	return rec.Value, true
}

// ---------------------------------------------------------------------------
// 写路径
// ---------------------------------------------------------------------------

// SetVHostLimit 新建或更新一条 vhost 限制，并等待本地生效。
//
// 值必须为正数：`0` 与负数没有公认口径（RabbitMQ 会原样存下来，但"0 个队列"
// 与"不限"都是可能的解读），与其自创一种，不如在写入口就明确拒绝。
func (b *Broker) SetVHostLimit(vhost, name string, value int) error {
	if limitDescription(name) == "" {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 不支持的限制名 %q（支持：%s / %s）",
			name, LimitMaxConnections, LimitMaxQueues)
	}
	if !b.VHostExists(vhost) {
		return plugin.Errorf(plugin.KindNotFound, "NOT_FOUND - vhost %s 不存在", vhost)
	}
	if value <= 0 {
		return plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 限制 %s 的值必须大于 0，当前为 %d", name, value)
	}
	rec := meta.VHostLimit{VHost: vhost, Name: name, Value: value}
	if err := b.submitMeta(meta.OpPutVHostLimit, rec); err != nil {
		return err
	}
	return b.awaitMeta(func() bool {
		cur, ok := b.metaState().Limits[meta.LimitKey(vhost, name)]
		return ok && cur.Value == value
	})
}

// DeleteVHostLimit 删除一条限制，返回是否命中。
//
// 不支持的限制名同样报错（而不是静默返回 false）：把参数错误伪装成"已删除"
// 会让调用方以为限制真的清掉了。
func (b *Broker) DeleteVHostLimit(vhost, name string) (bool, error) {
	if limitDescription(name) == "" {
		return false, plugin.Errorf(plugin.KindPreconditionFailed,
			"PRECONDITION_FAILED - 不支持的限制名 %q（支持：%s / %s）",
			name, LimitMaxConnections, LimitMaxQueues)
	}
	if _, ok := b.VHostLimit(vhost, name); !ok {
		return false, nil
	}
	rec := meta.VHostLimit{VHost: vhost, Name: name}
	if err := b.submitMeta(meta.OpDeleteVHostLimit, rec); err != nil {
		return false, err
	}
	if err := b.awaitMeta(func() bool {
		_, ok := b.metaState().Limits[meta.LimitKey(vhost, name)]
		return !ok
	}); err != nil {
		return false, err
	}
	return true, nil
}

// ---------------------------------------------------------------------------
// 生效路径：在内核真正拦下来
// ---------------------------------------------------------------------------

// checkQueueLimit 在**新建**队列之前校验 max-queues。
//
// 只拦新建：已存在的队列重声明走等价性校验那条路，不该因为后来加的限制而变成
// "连自己都声明不了"（限制是容量上限，不是既成事实的撤销开关）。
func (b *Broker) checkQueueLimit(v *vhost) error {
	limit, ok := b.vhostLimitValue(v.name, LimitMaxQueues)
	if !ok {
		return nil
	}
	if v.queueCount() >= limit {
		return plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - queue limit reached for vhost '%s' (max-queues=%d)", v.name, limit)
	}
	return nil
}

// checkConnectionLimit 在连接打开某个 vhost 之前校验 max-connections。
func (b *Broker) checkConnectionLimit(vhost string) error {
	limit, ok := b.vhostLimitValue(vhost, LimitMaxConnections)
	if !ok {
		return nil
	}
	if b.connectionCountOf(vhost) >= limit {
		return plugin.Errorf(plugin.KindAccessRefused,
			"ACCESS_REFUSED - connection limit reached for vhost '%s' (max-connections=%d)", vhost, limit)
	}
	return nil
}

// connectionCountOf 返回当前已打开该 vhost 的连接数。
func (b *Broker) connectionCountOf(vhost string) int {
	b.connsMu.RLock()
	defer b.connsMu.RUnlock()
	n := 0
	for _, e := range b.conns {
		if e.vhost == vhost {
			n++
		}
	}
	return n
}
