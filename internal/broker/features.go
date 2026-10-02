package broker

import (
	"sort"

	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件实现两类"能力开关"，字段与路径都对齐 RabbitMQ 的管理 API：
//
//   - 特性开关（feature flags，/api/feature-flags）：可开可关，**关掉之后行为真的会变**；
//   - 弃用特性（deprecated features，/api/deprecated-features）：只读清单，
//     逐条说明本实现对某个旧能力的最终态度。
//
// 一条硬规则：**只有在内核里有真实判定点的能力才允许登记进 featureFlagRegistry**。
// 登记一个"加了也没用"的开关，等于给运维一个假旋钮 —— 他会以为关掉就能停掉某件事，
// 实际什么也没发生。因此这里的条目数刻意很少，每条都能在代码里指出判定位置：
//
//   - quorum_queue               → vhostSession.newQueueWithPolicy / declareManagedQueue（声明拦截）
//   - exchange_exchange_bindings → vhostSession.BindExchange / UnbindExchange（命令拦截）
//     两条同时在 session.ServerProperties 的 capabilities 里同步收敛，保证"不声明也不实现"。

// 特性开关名。
const (
	flagQuorumQueue              = "quorum_queue"
	flagExchangeExchangeBindings = "exchange_exchange_bindings"
)

// featureFlag 是注册表条目。
type featureFlag struct {
	name string
	// def 是默认状态：元数据里没有显式记录时用它。
	// 默认开启，是为了让升级上来的老部署行为完全不变。
	def       bool
	stability string
	desc      string
	docURL    string
}

// featureFlagRegistry 是本实现支持的开关（顺序即管理 UI 的展示顺序）。
var featureFlagRegistry = []featureFlag{
	{
		name: flagQuorumQueue, def: true, stability: "required",
		desc: "支持类型为 quorum 的队列（x-queue-type=quorum）；关闭后新声明这类队列会被拒绝",
	},
	{
		name: flagExchangeExchangeBindings, def: true, stability: "required",
		desc: "支持交换机到交换机的绑定（exchange.bind / exchange.unbind）；关闭后这两条命令返回 NOT_IMPLEMENTED",
	},
}

// FeatureFlagSnapshot 是特性开关的只读视图（字段名对齐 RabbitMQ）。
type FeatureFlagSnapshot struct {
	Name string
	// State 取 enabled / disabled。
	State      string
	Stability  string
	Desc       string
	DocURL     string
	ProvidedBy string
}

// ---------------------------------------------------------------------------
// 读路径
// ---------------------------------------------------------------------------

// FeatureFlags 返回全部特性开关及其当前状态（按名字排序）。
func (b *Broker) FeatureFlags() []FeatureFlagSnapshot {
	out := make([]FeatureFlagSnapshot, 0, len(featureFlagRegistry))
	for _, f := range featureFlagRegistry {
		out = append(out, b.featureFlagSnapshot(f))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// FeatureFlag 返回单个开关。
func (b *Broker) FeatureFlag(name string) (FeatureFlagSnapshot, bool) {
	for _, f := range featureFlagRegistry {
		if f.name == name {
			return b.featureFlagSnapshot(f), true
		}
	}
	return FeatureFlagSnapshot{}, false
}

func (b *Broker) featureFlagSnapshot(f featureFlag) FeatureFlagSnapshot {
	state := "disabled"
	if b.featureEnabled(f.name) {
		state = "enabled"
	}
	return FeatureFlagSnapshot{
		Name: f.name, State: state, Stability: f.stability,
		Desc: f.desc, DocURL: f.docURL, ProvidedBy: "swiftmq",
	}
}

// featureEnabled 返回某个开关当前是否生效。
//
// 判定顺序：元数据里的显式记录 → 注册表默认值。注册表里没有的开关一律视为关闭，
// 于是"代码里加了判定点但忘了登记"会表现为功能不可用，而不是默默放开。
func (b *Broker) featureEnabled(name string) bool {
	if b.meta != nil {
		if rec, ok := b.meta.State().FeatureFlags[name]; ok {
			return rec.Enabled
		}
	}
	for _, f := range featureFlagRegistry {
		if f.name == name {
			return f.def
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// 写路径
// ---------------------------------------------------------------------------

// SetFeatureFlag 开启或关闭一个特性开关，等待本地生效后返回。
func (b *Broker) SetFeatureFlag(name string, enabled bool) error {
	if _, ok := b.FeatureFlag(name); !ok {
		return plugin.Errorf(plugin.KindNotFound, "NOT_FOUND - 未知的特性开关 %s", name)
	}
	rec := meta.FeatureFlag{Name: name, Enabled: enabled}
	if err := b.submitMeta(meta.OpPutFeatureFlag, rec); err != nil {
		return err
	}
	return b.awaitMeta(func() bool { return b.featureEnabled(name) == enabled })
}

// ---------------------------------------------------------------------------
// 弃用特性（只读清单）
// ---------------------------------------------------------------------------

// DeprecatedFeatureSnapshot 是弃用特性的只读视图（字段名对齐 RabbitMQ）。
type DeprecatedFeatureSnapshot struct {
	Name string
	// State 取 denied / permitted。
	State string
	// DeprecationPhase 取 denied_by_default / permitted_by_default / removed。
	DeprecationPhase string
	Desc             string
	DocURL           string
	ProvidedBy       string
}

// deprecatedFeatureRegistry 是本实现对旧能力的最终态度。
//
// 与特性开关不同，这里**没有开关**：RabbitMQ 4.x 的弃用特性是"随版本定型"的
// （其 per-vhost 的 enable/disable 接口在 4.x 上返回 405），因此这一页如实做成只读清单。
// 每条都能在内核里指出对应实现：
//
//   - transient_nonexcl_queues       → newQueueIn 拒绝"非持久且非独占"的队列（4.x 语义）
//   - global_qos                     → channel_methods.go 拒绝 basic.qos 的 global 标志
//   - queue_master_locator           → validatePolicyKey 不接受 queue-master-locator 策略键
//   - management_metrics_collection  → /metrics 与 /api/overview 仍然提供指标
//   - classic_queue_mirroring        → 本实现没有经典队列镜像（4.x 已移除）
//   - ram_node_type                  → 本实现没有 RAM 节点（4.x 已移除）
var deprecatedFeatureRegistry = []DeprecatedFeatureSnapshot{
	{
		Name: "queue_master_locator", State: "denied", DeprecationPhase: "denied_by_default",
		Desc:       "不再支持 queue-master-locator（队列放置策略）：本实现的队列归属由声明节点与仲裁组决定",
		ProvidedBy: "swiftmq",
	},
	{
		Name: "global_qos", State: "denied", DeprecationPhase: "denied_by_default",
		Desc:       "basic.qos 的 global 标志已移除，请改用 per-consumer prefetch",
		DocURL:     "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-global-qos",
		ProvidedBy: "swiftmq",
	},
	{
		Name: "transient_nonexcl_queues", State: "denied", DeprecationPhase: "denied_by_default",
		Desc:       "不允许声明「非持久且非独占」的队列（RabbitMQ 4.x 语义）",
		DocURL:     "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-transient-non-exclusive-queues",
		ProvidedBy: "swiftmq",
	},
	{
		Name: "management_metrics_collection", State: "permitted", DeprecationPhase: "permitted_by_default",
		Desc:       "管理 API（/api/overview、/api/nodes 等）与 /metrics 仍然提供运行指标",
		ProvidedBy: "swiftmq",
	},
	{
		Name: "classic_queue_mirroring", State: "denied", DeprecationPhase: "removed",
		Desc:       "经典队列镜像已移除：需要副本请使用仲裁队列（x-queue-type=quorum）",
		ProvidedBy: "swiftmq",
	},
	{
		Name: "ram_node_type", State: "denied", DeprecationPhase: "removed",
		Desc:       "RAM 节点已移除：本实现只有 disc 节点",
		ProvidedBy: "swiftmq",
	},
}

// DeprecatedFeatures 返回弃用特性清单（保持注册表顺序，便于与 RabbitMQ 对照阅读）。
func (b *Broker) DeprecatedFeatures() []DeprecatedFeatureSnapshot {
	out := make([]DeprecatedFeatureSnapshot, len(deprecatedFeatureRegistry))
	copy(out, deprecatedFeatureRegistry)
	return out
}
