package management

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/houzch/speedmq/internal/broker"
	sdk "github.com/houzch/speedmq/pkg/plugin"
)

// 时间格式：RabbitMQ Management API 用本地时间的 "2006-01-02 15:04:05"。
const apiTimeLayout = "2006-01-02 15:04:05"

// registerRoutes 注册全部管理接口。
//
// 路径与字段都对齐 RabbitMQ Management API 的子集（设计 9.1）：
// 目标不是"实现全部接口"，而是让 rabbitmqadmin 与管理 UI 这两个最具代表性的消费者
// 能直接跑起来 —— 兼容性由真实消费者验证，而不是由接口数量验证。
func (s *Server) registerRoutes() {
	// ---- 概览与节点 ----
	s.handle(http.MethodGet, "/api/overview", apiGroupOverview, s.getOverview)
	s.handle(http.MethodGet, "/api/nodes", apiGroupOverview, s.getNodes)
	// whoami 不参与接口权限收窄（空组）：它是"我是谁"的身份探针，
	// 登录后管理 UI 必须先拿到它才能知道当前账号被允许访问哪些功能组。
	s.handle(http.MethodGet, "/api/whoami", "", s.getWhoami)

	// 默认语言探测：管理 UI 在弹出登录框**之前**就要知道用哪种语言渲染，
	// 因此注册为免认证接口（见 handlePublic）。返回值与调用方无关、不含敏感信息。
	s.handlePublic(http.MethodGet, "/api/default-language", s.getDefaultLanguage)

	// ---- 集群（M6）----
	// /api/cluster 是 SpeedMQ 的扩展端点（RabbitMQ 没有对应接口），
	// /api/cluster/name 则对齐 RabbitMQ，便于既有工具读取集群名。
	s.handle(http.MethodGet, "/api/cluster", apiGroupCluster, s.getCluster)
	s.handle(http.MethodGet, "/api/cluster/name", apiGroupCluster, s.getClusterName)
	// 转发边界按 id 追踪的**全量样本**单独成端点：四组样本可达数十万条（JSON 数 MB 量级），
	// 只用于"数据不丢"的离线对账，不应跟着 /api/cluster 这类高频轮询接口一起返回。
	s.handle(http.MethodGet, "/api/cluster/forward-samples", apiGroupCluster, s.getForwardSamples)
	// 成员变更（M6d）：把节点加入/移出集群的运行期操作。
	// RabbitMQ 用 `rabbitmqctl join_cluster`（在被加入的节点上执行），这里反过来
	// 由集群侧发起（`PUT /api/cluster/members/{node_id}`），因为新节点是以 learner
	// 身份启动、被动等待被纳入；两种方向的语义等价，但集群侧发起更容易与 Raft 的
	// "只有 leader 能改配置"对齐。
	s.handle(http.MethodGet, "/api/cluster/members", apiGroupCluster, s.getClusterMembers)
	s.handle(http.MethodPut, "/api/cluster/members/{name}", apiGroupCluster, s.putClusterMember)
	s.handle(http.MethodDelete, "/api/cluster/members/{name}", apiGroupCluster, s.deleteClusterMember)

	// ---- vhost ----
	s.handle(http.MethodGet, "/api/vhosts", apiGroupVHosts, s.getVHosts)
	s.handle(http.MethodGet, "/api/vhosts/{vhost}", apiGroupVHosts, s.getVHost)
	s.handle(http.MethodPut, "/api/vhosts/{vhost}", apiGroupVHosts, s.putVHost)
	s.handle(http.MethodDelete, "/api/vhosts/{vhost}", apiGroupVHosts, s.deleteVHost)

	// ---- 队列 ----
	s.handle(http.MethodGet, "/api/queues", apiGroupTopology, s.getQueues)
	s.handle(http.MethodGet, "/api/queues/{vhost}", apiGroupTopology, s.getQueues)
	s.handle(http.MethodGet, "/api/queues/{vhost}/{name}", apiGroupTopology, s.getQueue)
	// 声明队列（RabbitMQ 管理 UI 的「Add a new queue」= 这个端点）：
	// 语义与 AMQP 的 queue.declare 一致，因为它复用的就是同一段内核逻辑。
	s.handle(http.MethodPut, "/api/queues/{vhost}/{name}", apiGroupTopology, s.putQueue)
	s.handle(http.MethodDelete, "/api/queues/{vhost}/{name}", apiGroupTopology, s.deleteQueue)
	s.handle(http.MethodDelete, "/api/queues/{vhost}/{name}/contents", apiGroupTopology, s.purgeQueue)
	s.handle(http.MethodPost, "/api/queues/{vhost}/{name}/get", apiGroupTopology, s.getQueueMessages)
	s.handle(http.MethodGet, "/api/queues/{vhost}/{name}/bindings", apiGroupTopology, s.getQueueBindings)
	// 仲裁队列的副本集运行期操作（M8-15，SpeedMQ 扩展端点，RabbitMQ 用 rabbitmq-queues 命令做同样的事）。
	//   PUT .../grow      {"count": N} 把副本数扩到 N（只增不减；单机模式返回 501）
	//   PUT .../rebalance {}           把该队列的 leader 迁到副本集中较空的节点
	s.handle(http.MethodPut, "/api/queues/{vhost}/{name}/grow", apiGroupTopology, s.growQueue)
	s.handle(http.MethodPut, "/api/queues/{vhost}/{name}/rebalance", apiGroupTopology, s.rebalanceQueue)

	// ---- 交换机 ----
	s.handle(http.MethodGet, "/api/exchanges", apiGroupTopology, s.getExchanges)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}", apiGroupTopology, s.getExchanges)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}/{name}", apiGroupTopology, s.getExchange)
	// 声明交换机（RabbitMQ 管理 UI 的「Add a new exchange」= 这个端点）；删除是它的对称操作，
	// 队列页既有删除，交换机页也要有，否则"能建不能删"。
	s.handle(http.MethodPut, "/api/exchanges/{vhost}/{name}", apiGroupTopology, s.putExchange)
	s.handle(http.MethodDelete, "/api/exchanges/{vhost}/{name}", apiGroupTopology, s.deleteExchange)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}/{name}/bindings/source", apiGroupTopology, s.getExchangeSourceBindings)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}/{name}/bindings/destination", apiGroupTopology, s.getExchangeDestinationBindings)
	s.handle(http.MethodPost, "/api/exchanges/{vhost}/{name}/publish", apiGroupTopology, s.publish)

	// ---- 绑定 ----
	s.handle(http.MethodGet, "/api/bindings", apiGroupTopology, s.getBindings)
	s.handle(http.MethodGet, "/api/bindings/{vhost}", apiGroupTopology, s.getBindings)
	// 绑定的增删（RabbitMQ 管理 UI 在队列/交换机详情页用的就是这四个端点）。
	// 目标类型写在路径里：/q/ 是队列、/e/ 是交换机；删除用 properties_key 定位那一条。
	s.handle(http.MethodPost, "/api/bindings/{vhost}/e/{source}/q/{destination}",
		apiGroupTopology, s.postQueueBinding)
	s.handle(http.MethodPost, "/api/bindings/{vhost}/e/{source}/e/{destination}",
		apiGroupTopology, s.postExchangeBinding)
	s.handle(http.MethodDelete, "/api/bindings/{vhost}/e/{source}/q/{destination}/{props}",
		apiGroupTopology, s.deleteQueueBinding)
	s.handle(http.MethodDelete, "/api/bindings/{vhost}/e/{source}/e/{destination}/{props}",
		apiGroupTopology, s.deleteExchangeBinding)

	// ---- 连接与通道 ----
	s.handle(http.MethodGet, "/api/connections", apiGroupConnections, s.getConnections)
	s.handle(http.MethodGet, "/api/connections/{name}", apiGroupConnections, s.getConnection)
	s.handle(http.MethodDelete, "/api/connections/{name}", apiGroupConnections, s.closeConnection)
	s.handle(http.MethodGet, "/api/channels", apiGroupConnections, s.getChannels)
	s.handle(http.MethodGet, "/api/channels/{name}", apiGroupConnections, s.getChannel)

	// ---- 消费者 ----
	s.handle(http.MethodGet, "/api/consumers", apiGroupTopology, s.getConsumers)
	s.handle(http.MethodGet, "/api/consumers/{vhost}", apiGroupTopology, s.getConsumers)

	// ---- 用户与权限 ----
	s.handle(http.MethodGet, "/api/users", apiGroupAccounts, s.getUsers)
	s.handle(http.MethodGet, "/api/users/{name}", apiGroupAccounts, s.getUser)
	// 账号与权限管理要 administrator/management 标签；功能组权限只在此之上做收窄。
	s.handle(http.MethodPut, "/api/users/{name}", apiGroupAccounts, s.putUser)
	s.handle(http.MethodDelete, "/api/users/{name}", apiGroupAccounts, s.deleteUser)
	// 改账号名与/或口令：本人可自助（首次强制改密走它），因此**不参与功能组收窄** ——
	// 否则管理员一旦没给账号勾"账号与权限"，这个账号连自己的初始口令都改不了。
	s.handle(http.MethodPost, "/api/users/{name}/credentials", "", s.putUserCredentials)
	s.handle(http.MethodGet, "/api/permissions", apiGroupAccounts, s.getPermissions)
	s.handle(http.MethodGet, "/api/vhosts/{vhost}/permissions", apiGroupAccounts, s.getVHostPermissions)
	s.handle(http.MethodGet, "/api/permissions/{vhost}/{user}", apiGroupAccounts, s.getPermission)
	s.handle(http.MethodPut, "/api/permissions/{vhost}/{user}", apiGroupAccounts, s.putPermission)
	s.handle(http.MethodDelete, "/api/permissions/{vhost}/{user}", apiGroupAccounts, s.deletePermission)

	// ---- 策略 ----
	s.handle(http.MethodGet, "/api/policies", apiGroupPolicies, s.getPolicies)
	s.handle(http.MethodGet, "/api/policies/{vhost}", apiGroupPolicies, s.getVHostPolicies)
	s.handle(http.MethodGet, "/api/policies/{vhost}/{name}", apiGroupPolicies, s.getPolicy)
	s.handle(http.MethodPut, "/api/policies/{vhost}/{name}", apiGroupPolicies, s.putPolicy)
	s.handle(http.MethodDelete, "/api/policies/{vhost}/{name}", apiGroupPolicies, s.deletePolicy)

	// ---- 插件治理 ----
	s.handle(http.MethodGet, "/api/plugins", apiGroupPlugins, s.getPlugins)
	s.handle(http.MethodGet, "/api/plugins/{name}", apiGroupPlugins, s.getPlugin)
	s.handle(http.MethodPut, "/api/plugins/{name}/enable", apiGroupPlugins, s.enablePlugin)
	s.handle(http.MethodPut, "/api/plugins/{name}/disable", apiGroupPlugins, s.disablePlugin)

	// ---- vhost 级限制 ----
	// 路径与语义对齐 RabbitMQ 的 /api/vhost-limits：写入 204、删除 204（不存在也 204）、
	// 未知 vhost 404、未知限制名 400。
	s.handle(http.MethodGet, "/api/vhost-limits", apiGroupLimits, s.getVHostLimits)
	s.handle(http.MethodGet, "/api/vhost-limits/{vhost}", apiGroupLimits, s.getVHostLimits)
	s.handle(http.MethodPut, "/api/vhost-limits/{vhost}/{name}", apiGroupLimits, s.putVHostLimit)
	s.handle(http.MethodDelete, "/api/vhost-limits/{vhost}/{name}", apiGroupLimits, s.deleteVHostLimit)

	// ---- 特性开关与弃用特性 ----
	// 这两个都是**全局**对象（不像 vhost 限制那样有 vhost 维度），因此写操作要求 administrator。
	s.handle(http.MethodGet, "/api/feature-flags", apiGroupFeatureFlags, s.getFeatureFlags)
	s.handle(http.MethodPut, "/api/feature-flags/{name}/enable", apiGroupFeatureFlags, s.enableFeatureFlag)
	s.handle(http.MethodPut, "/api/feature-flags/{name}/disable", apiGroupFeatureFlags, s.disableFeatureFlag)
	s.handle(http.MethodGet, "/api/deprecated-features", apiGroupFeatureFlags, s.getDeprecatedFeatures)

	// ---- 指标 ----
	s.handle(http.MethodGet, "/metrics", apiGroupOverview, s.getMetrics)
	s.handle(http.MethodGet, "/api/metrics", apiGroupOverview, s.getMetrics)
}

// ---------------------------------------------------------------------------
// 概览与节点
// ---------------------------------------------------------------------------

type rateDetails struct {
	Rate float64 `json:"rate"`
}

type objectTotals struct {
	Connections int `json:"connections"`
	Channels    int `json:"channels"`
	Queues      int `json:"queues"`
	Consumers   int `json:"consumers"`
	Exchanges   int `json:"exchanges"`
}

type queueTotals struct {
	Messages               int `json:"messages"`
	MessagesReady          int `json:"messages_ready"`
	MessagesUnacknowledged int `json:"messages_unacknowledged"`
}

type messageStats struct {
	Publish           uint64      `json:"publish"`
	Deliver           uint64      `json:"deliver"`
	Ack               uint64      `json:"ack"`
	DeliverGet        uint64      `json:"deliver_get"`
	PublishDetails    rateDetails `json:"publish_details"`
	DeliverGetDetails rateDetails `json:"deliver_get_details"`
	AckDetails        rateDetails `json:"ack_details"`
}

type listenerObject struct {
	Node      string `json:"node"`
	Protocol  string `json:"protocol"`
	IPAddress string `json:"ip_address"`
	Port      int    `json:"port"`
}

func (s *Server) getOverview(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	totals := s.deps.Broker.ObjectTotals()
	qt := s.deps.Broker.QueueTotals()
	mt := s.deps.Broker.MessageTotals()

	writeJSON(w, http.StatusOK, map[string]any{
		"management_version": s.deps.Version,
		"rabbitmq_version":   s.deps.Version,
		"product_name":       "SpeedMQ",
		"product_version":    s.deps.Version,
		"cluster_name":       s.deps.NodeName,
		"node":               s.deps.NodeName,
		"listeners":          s.listenerObjects(),
		"object_totals": objectTotals{
			Connections: totals.Connections, Channels: totals.Channels,
			Queues: totals.Queues, Consumers: totals.Consumers, Exchanges: totals.Exchanges,
		},
		"queue_totals": queueTotals{
			Messages: qt.Messages, MessagesReady: qt.Ready, MessagesUnacknowledged: qt.Unacknowledged,
		},
		"message_stats": messageStats{
			Publish: mt.Publish, Deliver: mt.Deliver, Ack: mt.Ack,
			DeliverGet:        mt.Deliver,
			PublishDetails:    rateDetails{},
			DeliverGetDetails: rateDetails{},
			AckDetails:        rateDetails{},
		},
		// 非标准扩展字段：让运维/监控能在同一处看到资源水位与落盘档位
		// （rabbitmqadmin 会忽略不认识的字段，因此不会破坏兼容性）。
		"speedmq_blocked": s.deps.Broker.BlockedState(),
		"speedmq_fsync":   s.deps.Broker.StorageFsync(),
	})
}

func (s *Server) listenerObjects() []listenerObject {
	// 用非 nil 空切片：列表端点必须返回 []，返回 null 会让前端/脚本的遍历直接报错。
	out := make([]listenerObject, 0)
	if s.deps.Listeners == nil {
		return out
	}
	for _, ln := range s.deps.Listeners() {
		host, portStr, err := splitHostPort(ln.Addr)
		if err != nil {
			continue
		}
		port, _ := strconv.Atoi(portStr)
		protocol := ln.Listener
		if protocol == "" {
			protocol = ln.Protocol
		}
		out = append(out, listenerObject{
			Node: s.deps.NodeName, Protocol: protocol, IPAddress: host, Port: port,
		})
	}
	return out
}

func (s *Server) getNodes(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	enabled := s.enabledPlugins()
	apps := make([]map[string]any, 0, len(enabled))
	for _, p := range s.deps.Plugins.Plugins() {
		if p.State != sdk.StateEnabled {
			continue
		}
		apps = append(apps, map[string]any{
			"name": p.Name, "version": p.Version, "description": p.Description,
		})
	}

	node := map[string]any{
		"name":            s.deps.NodeName,
		"type":            "disc",
		"running":         true,
		"uptime":          time.Since(s.deps.StartedAt).Milliseconds(),
		"mem_used":        s.deps.Broker.ProcessMemory(),
		"mem_limit":       0,
		"disk_free":       0,
		"disk_free_limit": 0,
		"proc_used":       0,
		"proc_total":      0,
		"os_pid":          os.Getpid(),
		"fd_used":         0,
		"fd_total":        0,
		"sockets_used":    s.deps.Broker.ObjectTotals().Connections,
		"sockets_total":   0,
		"applications":    apps,
		"enabled_plugins": enabled,
		"partitions":      []any{},
	}
	if total, ok := s.deps.Broker.MemoryTotal(); ok {
		if wm, _ := s.deps.Broker.StorageLimits(); wm > 0 {
			node["mem_limit"] = uint64(wm * float64(total))
		} else {
			node["mem_limit"] = total
		}
	}
	if free, err := s.deps.Broker.DiskFree(); err == nil {
		node["disk_free"] = free
	}
	if _, limit := s.deps.Broker.StorageLimits(); limit > 0 {
		node["disk_free_limit"] = limit
	}
	// 集群信息作为扩展字段挂在节点对象上：rabbitmqadmin 等工具会忽略不认识的字段，
	// 但运维/监控能在 /api/nodes 里一并拿到"本节点在集群中的角色"。
	node["speedmq_cluster"] = s.clusterObject()
	writeJSON(w, http.StatusOK, []map[string]any{node})
}

// getCluster 返回本节点的集群/元数据层状态。
//
// 走读权限：集群状态包含成员与共识进度，属监控范畴（与 /api/nodes 同级）。
func (s *Server) getCluster(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.clusterObject())
}

// getClusterName 对齐 RabbitMQ 的 GET /api/cluster/name（返回 {"name": ...}）。
//
// SpeedMQ 本期没有独立的"集群名"概念：集群由静态成员表定义，
// 因此用本节点名作为集群名 —— 与 RabbitMQ 的默认行为一致（集群名 = 首个节点名）。
func (s *Server) getClusterName(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": s.deps.NodeName})
}

// getForwardSamples 返回转发边界按 id 追踪的**全量样本**（GET /api/cluster/forward-samples）。
//
// 为什么单独成端点：四组样本与 id 一一对应、可达数十万条，序列化成 JSON 有 MB 量级；
// 它们只服务于"数据不丢"的离线对账，挂在 /api/cluster 上会让每次高频轮询都付出这份开销
// （实测单次响应约 5 MB）。转发侧的**汇总计数**仍保留在 /api/cluster 的 forwarding 里。
func (s *Server) getForwardSamples(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	fwd := s.deps.Broker.ForwardStatus()
	writeJSON(w, http.StatusOK, map[string]any{
		// 转发边界按 id 追踪样本（B5-follow-2）：代理侧成功/失败、Owner 侧接受/失败的 id。
		// 四组集合均为尾窗（成功 65536 / 失败 262144），`*_truncated` 为 true 表示发生过覆盖、
		// 样本已不代表全量 —— 显式暴露截断，避免被当成完整样本做对账（P2/R1）。
		"sent_ok_ids":        emptySliceIfNil(fwd.SentOKIDs),
		"sent_bad_ids":       emptySliceIfNil(fwd.SentBadIDs),
		"in_ok_ids":          emptySliceIfNil(fwd.InOKIDs),
		"in_bad_ids":         emptySliceIfNil(fwd.InBadIDs),
		"sent_ok_truncated":  fwd.SentOKTruncated,
		"sent_bad_truncated": fwd.SentBadTruncated,
		"in_ok_truncated":    fwd.InOKTruncated,
		"in_bad_truncated":   fwd.InBadTruncated,
	})
}

// clusterObject 组装集群状态视图（/api/cluster 与 /api/nodes 共用）。
func (s *Server) clusterObject() map[string]any {
	st := s.deps.Broker.ClusterStatus()
	peers := st.Peers
	if peers == nil {
		peers = []string{}
	}
	learners := st.Learners
	if learners == nil {
		learners = []string{}
	}
	fwd := s.deps.Broker.ForwardStatus()
	qs := s.deps.Broker.QuorumWriteStats()
	return map[string]any{
		"enabled":         s.deps.Broker.ClusterEnabled(),
		"mode":            st.Mode,
		"node_id":         st.NodeID,
		"role":            st.Role,
		"term":            st.Term,
		"leader":          st.Leader,
		"has_quorum":      st.HasQuorum,
		"paused":          s.deps.Broker.ClusterPaused(),
		"peers":           peers,
		"learners":        learners,
		"commit_index":    st.CommitIndex,
		"last_applied":    st.LastApplied,
		"applied_records": st.AppliedRecords,
		"object_totals": map[string]any{
			"queues": st.Queues, "exchanges": st.Exchanges,
			"bindings": st.Bindings, "users": st.Users,
		},
		// 跨节点转发的运行态：代理消费者数、持有中的投递、转发进出计数。
		"forwarding": map[string]any{
			"proxy_consumers":  fwd.ProxyConsumers,
			"remote_consumers": fwd.RemoteConsumers,
			"held_deliveries":  fwd.HeldDeliveries,
			"forwarded_out":    fwd.ForwardedOut,
			"forwarded_in":     fwd.ForwardedIn,
			// forwarded_batches 是转发发布所用 RPC 次数：forwarded_out/forwarded_batches 即平均批大小。
			"forwarded_batches": fwd.ForwardedBatches,
			"deliveries":        fwd.Deliveries,
			// Owner 侧转发发布的分支计数（B5-follow-2 排查）：静默丢弃候选 / 服务节点不符 / 入队失败 / 落盘失败 / 被拒。
			"queue_missing":   fwd.QueueMissing,
			"remote_mismatch": fwd.RemoteMismatch,
			"publish_err":     fwd.PublishErr,
			"durable_err":     fwd.DurableErr,
			"rejected":        fwd.Rejected,
			"no_wait":         fwd.NoWait,
			"phantom_ok":      fwd.PhantomOK,
		},
		// Raft 写路径计数（M4）：propose_entries/fsync_total 即组提交的平均批大小。
		"quorum_write": map[string]any{
			"propose_entries":    qs.ProposeEntries,
			"fsync_total":        qs.FsyncTotal,
			"batch_size":         qs.BatchSize(),
			"ack_batches":        qs.AckBatches,
			"ack_seqs":           qs.AckSeqs,
			"ack_batch_size":     qs.AckBatchSize(),
			"publish_batches":    qs.PublishBatches,
			"publish_items":      qs.PublishItems,
			"publish_batch_size": qs.PublishBatchSize(),
			// 接受/应用/撞号（B5-follow-2 排查）：applied 应等于 accepted，dup_seq 应恒为 0。
			"accepted_publish": qs.AcceptedPublish,
			"applied_publish":  qs.AppliedPublish,
			"dup_seq":          qs.DupSeq,
			// 移除原因追踪（B5-follow-2）：确认删掉"从未投递"条目的次数，正常恒为 0。
			"ack_removed_undelivered": qs.AckRemovedUndelivered,
			// 被错删消息的按 id 样本（上限 128 条），供与客户端 confirmed/consumed id 集合对账。
			"ack_removed_ids":  emptySliceIfNil(qs.AckRemovedIDs),
			"ack_removed_seqs": emptySliceIfNil(qs.AckRemovedSeqs),
		},
	}
}

// ---------------------------------------------------------------------------
// 集群成员变更（M6d）
// ---------------------------------------------------------------------------

// memberRequest 是 PUT /api/cluster/members/{name} 的请求体。
type memberRequest struct {
	// Addr 是该节点可达的集群 RPC 地址（形如 "10.0.0.4:25672"）。
	// 必填：新节点的地址需要随配置变更复制到全体成员，才能被联系上。
	Addr string `json:"addr"`
}

// clusterMembersObject 组装成员划分视图。
func (s *Server) clusterMembersObject() map[string]any {
	m := s.deps.Broker.ClusterMembers()
	voters, learners := m.Voters, m.Learners
	if voters == nil {
		voters = []string{}
	}
	if learners == nil {
		learners = []string{}
	}
	return map[string]any{"voters": voters, "learners": learners}
}

// getClusterMembers 返回成员划分（GET /api/cluster/members）。
func (s *Server) getClusterMembers(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.clusterMembersObject())
}

// putClusterMember 把一个节点加入集群（PUT /api/cluster/members/{node_id}）。
//
// 语义是"加入并提升为投票成员"：内部先以 learner 加入、等它追平（最长几十秒，
// 取决于它要拉多少日志/快照），再提升为投票成员 —— 因此这个请求可能耗时较长。
func (s *Server) putClusterMember(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	id := p["name"]
	var req memberRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	if req.Addr == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "缺少 addr：新成员的集群 RPC 地址必须提供")
		return
	}
	if err := s.deps.Broker.AddClusterMember(r.Context(), id, req.Addr); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面加入集群成员", "actor", au.Name, "node", id, "addr", req.Addr)
	writeJSON(w, http.StatusOK, s.clusterMembersObject())
}

// deleteClusterMember 把一个节点移出集群（DELETE /api/cluster/members/{node_id}）。
func (s *Server) deleteClusterMember(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	id := p["name"]
	if err := s.deps.Broker.RemoveClusterMember(r.Context(), id); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面移除集群成员", "actor", au.Name, "node", id)
	writeJSON(w, http.StatusOK, s.clusterMembersObject())
}

func (s *Server) enabledPlugins() []string {
	// 非 nil 空切片：列表端点必须返回 []。
	out := make([]string, 0)
	for _, p := range s.deps.Plugins.Plugins() {
		if p.State == sdk.StateEnabled {
			out = append(out, p.Name)
		}
	}
	sort.Strings(out)
	return out
}

// getDefaultLanguage 返回管理 UI 的默认语言（安装时按部署地系统时区推断）。
//
// 免认证：管理 UI 在登录前就需要它决定初始语言；用户在界面里选择的语言存于浏览器本地，
// 优先于此默认值。默认值缺失时回退 en，保证前端总能拿到一个可用 code。
func (s *Server) getDefaultLanguage(w http.ResponseWriter, _ *http.Request, _ params, _ authUser) {
	lang := s.deps.DefaultLanguage
	if lang == "" {
		lang = "en"
	}
	writeJSON(w, http.StatusOK, map[string]string{"default_language": lang})
}

func (s *Server) getWhoami(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":         au.Name,
		"tags":         strings.Join(sortedCopy(au.Tags), " "),
		"auth_backend": "internal",
		// 前端据此判断要不要强制弹出"首次改账号名/口令"对话框。
		"is_root":              au.Root,
		"must_change_password": au.MustChangePassword,
		// 当前账号可访问的管理接口功能组；空数组表示不限制。管理 UI 据此隐藏无权访问的菜单。
		"api_groups": emptyIfNil(au.APIGroups),
	})
}

// ---------------------------------------------------------------------------
// vhost
// ---------------------------------------------------------------------------

func (s *Server) getVHosts(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	var out []map[string]any
	for _, v := range s.deps.Broker.VHostSnapshots() {
		if !s.canSeeVHost(au, v.Name) {
			continue
		}
		out = append(out, vhostObject(s.deps.NodeName, v))
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func (s *Server) getVHost(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	v, ok := s.deps.Broker.VHostSnapshot(vhost)
	if !ok || !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	writeJSON(w, http.StatusOK, vhostObject(s.deps.NodeName, v))
}

// putVHost 实现 PUT /api/vhosts/{vhost}：新建返回 201、已存在返回 204（幂等，重复 PUT 不报错）。
//
// 要求 administrator 标签（与 RabbitMQ 一致）：vhost 是全局对象，新建它会改变
// "哪些账号能连到哪里"的整体格局，不是某一个 vhost 内的写操作。
func (s *Server) putVHost(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["vhost"]
	if name == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "vhost 名不能为空")
		return
	}
	existed := s.deps.Broker.VHostExists(name)
	if err := s.deps.Broker.CreateVHost(name); err != nil {
		writeKernelError(w, err)
		return
	}
	status := http.StatusCreated
	if existed {
		status = http.StatusNoContent
	}
	s.log.Info("管理面新建 vhost", "actor", au.Name, "vhost", name)
	w.WriteHeader(status)
}

// deleteVHost 实现 DELETE /api/vhosts/{vhost}：级联删除 vhost 的全部内容，成功 204、不存在 404。
//
// 默认 vhost 会被内核拒绝（400）：它是内核保证存在的连接落点。
func (s *Server) deleteVHost(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["vhost"]
	ok, err := s.deps.Broker.DeleteVHost(name)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	if !ok {
		vhostNotFound(w, name)
		return
	}
	s.log.Info("管理面删除 vhost", "actor", au.Name, "vhost", name)
	w.WriteHeader(http.StatusNoContent)
}

func vhostObject(node string, v broker.VHostSnapshot) map[string]any {
	return map[string]any{
		"name":                            v.Name,
		"tracing":                         false,
		"type":                            "vhost",
		"messages":                        v.Messages,
		"messages_details":                rateDetails{},
		"messages_ready":                  v.MessagesReady,
		"messages_ready_details":          rateDetails{},
		"messages_unacknowledged":         v.MessagesUnacked,
		"messages_unacknowledged_details": rateDetails{},
		"cluster_state":                   map[string]string{node: "running"},
	}
}

// ---------------------------------------------------------------------------
// 队列
// ---------------------------------------------------------------------------

func (s *Server) getQueues(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if vhost == "" {
		vhost = r.URL.Query().Get("vhost")
	}
	if vhost != "" && !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}

	filter, err := nameFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}

	var out []map[string]any
	if vhost != "" {
		for _, q := range s.deps.Broker.QueueSnapshots(vhost) {
			out = append(out, queueObject(s.deps.NodeName, q))
		}
	} else {
		for _, name := range s.deps.Broker.VHostNames() {
			if !s.canSeeVHost(au, name) {
				continue
			}
			for _, q := range s.deps.Broker.QueueSnapshots(name) {
				out = append(out, queueObject(s.deps.NodeName, q))
			}
		}
	}
	writeJSON(w, http.StatusOK, emptyIfNil(filterMap(out, filter)))
}

func (s *Server) getQueue(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	q, ok := s.deps.Broker.QueueSnapshot(vhost, name)
	if !ok {
		queueNotFound(w, vhost, name)
		return
	}
	writeJSON(w, http.StatusOK, queueObject(s.deps.NodeName, q))
}

func queueObject(node string, q broker.QueueSnapshot) map[string]any {
	stats := messageStats{
		Publish: q.Published, Deliver: q.Delivered, Ack: q.Acked,
		DeliverGet:     q.Delivered + q.Gotten,
		PublishDetails: rateDetails{}, DeliverGetDetails: rateDetails{}, AckDetails: rateDetails{},
	}
	var exclusiveConsumer any
	if q.ExclusiveConsumerTag != "" {
		exclusiveConsumer = q.ExclusiveConsumerTag
	}
	// `node` 是队列数据的所在节点（对齐 RabbitMQ 的"队列 master 节点"语义）。
	// 快照给了 Owner 就用它（远端经典队列与仲裁队列都会给），否则用本节点名 ——
	// 这样同一个节点在管理面里只会有一个名字，运维不会对不上号。
	owner := node
	if q.Owner != "" {
		owner = q.Owner
	}
	// 队列类型：classic / quorum（quorum 的消息复制到多数派，见 README 的集群一节）。
	queueType := q.QueueType
	if queueType == "" {
		queueType = "classic"
	}
	// 命中的策略：`policy` 是策略名、`effective_policy_definition` 是策略内容，
	// 两者是运维回答"这个队列的 TTL 是谁设的"的唯一线索（对齐 RabbitMQ 的字段名）。
	policyName := any(nil)
	if q.Policy != "" {
		policyName = q.Policy
	}
	obj := map[string]any{
		"name":                            q.Name,
		"vhost":                           q.VHost,
		"durable":                         q.Durable,
		"auto_delete":                     q.AutoDelete,
		"exclusive":                       q.Exclusive,
		"type":                            queueType,
		"node":                            owner,
		"state":                           "running",
		"arguments":                       emptyMapIfNil(q.Arguments),
		"consumers":                       q.ConsumerCount,
		"consumer_utilisation":            nil,
		"policy":                          policyName,
		"exclusive_consumer_tag":          exclusiveConsumer,
		"single_active_consumer_tag":      nil,
		"messages":                        q.Ready + q.Unacked,
		"messages_details":                rateDetails{},
		"messages_ready":                  q.Ready,
		"messages_ready_details":          rateDetails{},
		"messages_unacknowledged":         q.Unacked,
		"messages_unacknowledged_details": rateDetails{},
		"message_stats":                   stats,
		"memory":                          q.MemoryBytes,
		"idle_since":                      q.IdleSince.Format(apiTimeLayout),
		"reductions":                      0,
		"effective_policy_definition":     emptyMapIfNil(q.EffectivePolicyDefinition),
	}
	if q.Quorum != nil {
		// 仲裁队列的副本集：`members` / `leader` 沿用 RabbitMQ 的字段名，
		// `speedmq_quorum` 是扩展细节（元数据里记录的副本集、投票/非投票成员划分）。
		obj["members"] = q.Quorum.Voters
		obj["leader"] = q.Quorum.Leader
		obj["speedmq_quorum"] = quorumObject(*q.Quorum)
	}
	return obj
}

// quorumObject 把仲裁队列的副本集视图翻译成 JSON（grow / rebalance 的响应也复用它）。
func quorumObject(info broker.QuorumQueueInfo) map[string]any {
	voters, learners := info.Voters, info.Learners
	if voters == nil {
		voters = []string{}
	}
	if learners == nil {
		learners = []string{}
	}
	replicas := info.Replicas
	if replicas == nil {
		replicas = []string{}
	}
	return map[string]any{
		"vhost":    info.VHost,
		"queue":    info.Name,
		"leader":   info.Leader,
		"replicas": replicas,
		"count":    len(replicas),
		"voters":   voters,
		"learners": learners,
	}
}

// queueDeclareRequest 是 PUT /api/queues/{vhost}/{name} 的请求体（字段名对齐 RabbitMQ）。
type queueDeclareRequest struct {
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Arguments  map[string]any `json:"arguments"`
	// Node 是 RabbitMQ 的字段（把队列放到指定节点）。SpeedMQ 的队列放置由内核决定，
	// 这里**显式忽略而不是报错** —— 否则 rabbitmqadmin 与既有脚本多传一个字段就会失败。
	Node string `json:"node"`
}

// putQueue 声明一个队列。
//
// RabbitMQ 管理 UI 的「Add a new queue」就是这个端点：UI 只是它的表单壳。
// 语义与 AMQP `queue.declare` **完全一致** —— 因为它复用的就是同一段内核逻辑：
// 经 SessionFor(调用方) 拿会话，`configure` 权限、保留名（403）、参数等价性检查（400）
// 全部照走，因此管理面建出来的队列与客户端建出来的没有任何区别。
//
// 状态码对齐 RabbitMQ：新建 201、已存在且参数等价 204、参数不等价 400。
func (s *Server) putQueue(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var req queueDeclareRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	// 先探测是否已存在，用来决定 201（新建）还是 204（已存在）——这正是 RabbitMQ 的区别。
	_, existed := s.deps.Broker.QueueSnapshot(vhost, name)

	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()
	if _, err := sess.DeclareQueue(sdk.QueueDeclare{
		Name:       name,
		Durable:    req.Durable,
		AutoDelete: req.AutoDelete,
		Arguments:  req.Arguments,
	}); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面声明队列", "actor", au.Name, "vhost", vhost, "queue", name,
		"durable", req.Durable, "auto_delete", req.AutoDelete)
	status := http.StatusCreated
	if existed {
		status = http.StatusNoContent
	}
	w.WriteHeader(status)
}

func (s *Server) deleteQueue(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()

	ifUnused := queryBool(r, "if-unused")
	ifEmpty := queryBool(r, "if-empty")
	if _, err := sess.DeleteQueue(name, ifUnused, ifEmpty); err != nil {
		writeKernelError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) purgeQueue(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()

	if _, err := sess.PurgeQueue(name); err != nil {
		writeKernelError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// growRequest 是 PUT /api/queues/{vhost}/{name}/grow 的请求体。
type growRequest struct {
	// Count 是目标副本数（必须大于 0，且不超过集群成员数）。
	Count int `json:"count"`
}

// growQueue 实现 PUT /api/queues/{vhost}/{name}/grow：把仲裁队列的副本数扩到 count。
//
// 语义对齐 `rabbitmq-queues grow`：只增不减、副本集落进元数据（重启后仍生效）。
// 非仲裁队列 / 超过集群规模 / 缩容 → 400 PRECONDITION_FAILED，队列不存在 → 404，单机模式 → 501。
func (s *Server) growQueue(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	// 副本集是集群级操作（对齐 rabbitmq-queues 需 administrator），
	// 不接受 management 标签代劳，也不放行"只读该 vhost"的账号。
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var req growRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	if req.Count <= 0 {
		writeError(w, http.StatusBadRequest, "Bad Request", "缺少 count：目标副本数必须大于 0")
		return
	}
	info, err := s.deps.Broker.GrowQuorumQueue(r.Context(), vhost, name, req.Count)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面扩大仲裁队列副本", "actor", au.Name, "vhost", vhost, "queue", name,
		"replicas", info.Replicas)
	writeJSON(w, http.StatusOK, quorumObject(info))
}

// rebalanceQueue 实现 PUT /api/queues/{vhost}/{name}/rebalance：把该队列的 leader
// 从承载 leader 最多的节点迁到副本集中较空的投票成员。
func (s *Server) rebalanceQueue(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	// 与 growQueue 同理：再平衡是集群级操作，要求 administrator。
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	res, err := s.deps.Broker.RebalanceQuorumQueue(r.Context(), vhost, name)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面再平衡仲裁队列", "actor", au.Name, "vhost", vhost, "queue", name,
		"moved", res.Moved, "from", res.From, "to", res.To)
	writeJSON(w, http.StatusOK, map[string]any{
		"vhost": res.VHost, "queue": res.Name,
		"moved": res.Moved, "from": res.From, "to": res.To, "reason": res.Reason,
	})
}

// getRequest 是 POST /api/queues/{vhost}/{name}/get 的请求体。
type getRequest struct {
	Count    int    `json:"count"`
	AckMode  string `json:"ackmode"`
	Encoding string `json:"encoding"`
	Truncate int    `json:"truncate"`
}

type getMessage struct {
	Payload         string            `json:"payload"`
	PayloadBytes    int               `json:"payload_bytes"`
	PayloadEncoding string            `json:"payload_encoding"`
	Redelivered     bool              `json:"redelivered"`
	Exchange        string            `json:"exchange"`
	RoutingKey      string            `json:"routing_key"`
	MessageCount    int               `json:"message_count"`
	Properties      messageProperties `json:"properties"`
}

type messageProperties struct {
	ContentType     string         `json:"content_type"`
	ContentEncoding string         `json:"content_encoding"`
	Headers         map[string]any `json:"headers"`
	DeliveryMode    int            `json:"delivery_mode"`
	Priority        int            `json:"priority"`
	CorrelationID   string         `json:"correlation_id"`
	ReplyTo         string         `json:"reply_to"`
	Expiration      string         `json:"expiration"`
	MessageID       string         `json:"message_id"`
	Timestamp       int64          `json:"timestamp"`
	Type            string         `json:"type"`
	UserID          string         `json:"user_id"`
	AppID           string         `json:"app_id"`
}

// getQueueMessages 实现"从队列取消息"（管理 UI 与 rabbitmqadmin 的 get 都走这里）。
//
// ackmode 到内核结算动作的映射（对齐 RabbitMQ）：
//
//	ack_requeue_true   → 重新入队（ack 模式下的"取回但不消费"）
//	ack_requeue_false  → 确认并移除
//	reject_requeue_true  → 拒绝但重新入队
//	reject_requeue_false → 拒绝且不重入队（配置了 DLX 则进死信）
func (s *Server) getQueueMessages(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	req := getRequest{Count: 1, AckMode: "ack_requeue_true", Encoding: "auto", Truncate: 50000}
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	if req.Count <= 0 {
		req.Count = 1
	}
	if req.Count > 1000 {
		// 上限保护：一次取太多会把整个队列拉进内存并序列化成 JSON
		req.Count = 1000
	}

	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()

	action, err := settleActionFor(req.AckMode)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}

	// 先取够 count 条，最后统一结算。原因有二：
	//  1. requeue 会把消息放回队首，逐条"取一条就 requeue 一条"会让第二次取到同一条；
	//  2. 中途出错时也必须结算已取出的消息，否则它们会永远留在未确认集合里（泄露）。
	var deliveries []*sdk.Delivery
	settled := false
	defer func() {
		if settled {
			return
		}
		for _, d := range deliveries {
			d.Settle(sdk.SettleRequeue)
		}
	}()

	for i := 0; i < req.Count; i++ {
		d, ok, err := sess.Get(name, false)
		if err != nil {
			writeKernelError(w, err)
			return
		}
		if !ok {
			break
		}
		deliveries = append(deliveries, d)
	}

	// message_count 是"取完之后队列里还剩多少"：此时本次取出的消息都还在未确认集合里，
	// 因此就绪数正好等于剩余量。
	remaining, _ := s.deps.Broker.QueueSnapshot(vhost, name)

	out := make([]getMessage, 0, len(deliveries))
	for _, d := range deliveries {
		msg := toGetMessage(d.Message, req)
		msg.MessageCount = remaining.Ready
		out = append(out, msg)
	}

	settled = true
	for _, d := range deliveries {
		d.Settle(action)
	}
	writeJSON(w, http.StatusOK, out)
}

func settleActionFor(ackMode string) (sdk.SettleAction, error) {
	switch ackMode {
	case "", "ack_requeue_true", "reject_requeue_true":
		return sdk.SettleRequeue, nil
	case "ack_requeue_false":
		return sdk.SettleAck, nil
	case "reject_requeue_false":
		return sdk.SettleReject, nil
	default:
		return 0, fmt.Errorf("未知的 ackmode %q（可选 ack_requeue_true / ack_requeue_false / reject_requeue_true / reject_requeue_false）", ackMode)
	}
}

func toGetMessage(msg *sdk.Message, req getRequest) getMessage {
	body := msg.Body
	total := len(body)
	if req.Truncate > 0 && len(body) > req.Truncate {
		body = body[:req.Truncate]
	}
	payload := string(body)
	encoding := "string"
	if req.Encoding == "base64" || !utf8.Valid(body) {
		payload = base64.StdEncoding.EncodeToString(body)
		encoding = "base64"
	}
	p := msg.Properties
	var ts int64
	if !p.Timestamp.IsZero() {
		ts = p.Timestamp.UnixMilli()
	}
	return getMessage{
		Payload:         payload,
		PayloadBytes:    total,
		PayloadEncoding: encoding,
		Redelivered:     msg.Redelivered,
		Exchange:        msg.Exchange,
		RoutingKey:      msg.RoutingKey,
		Properties: messageProperties{
			ContentType:     p.ContentType,
			ContentEncoding: p.ContentEncoding,
			Headers:         emptyMapIfNil(p.Headers),
			DeliveryMode:    int(p.DeliveryMode),
			Priority:        int(p.Priority),
			CorrelationID:   p.CorrelationID,
			ReplyTo:         p.ReplyTo,
			Expiration:      p.Expiration,
			MessageID:       p.MessageID,
			Timestamp:       ts,
			Type:            p.Type,
			UserID:          p.UserID,
			AppID:           p.AppID,
		},
	}
}

func (s *Server) getQueueBindings(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	if _, ok := s.deps.Broker.QueueSnapshot(vhost, name); !ok {
		queueNotFound(w, vhost, name)
		return
	}
	writeJSON(w, http.StatusOK, emptyIfNil(bindingObjects(s.deps.Broker.QueueBindings(vhost, name))))
}

// ---------------------------------------------------------------------------
// 交换机
// ---------------------------------------------------------------------------

func (s *Server) getExchanges(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if vhost == "" {
		vhost = r.URL.Query().Get("vhost")
	}
	if vhost != "" && !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	filter, err := nameFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}

	var out []map[string]any
	collect := func(vh string) {
		for _, e := range s.deps.Broker.ExchangeSnapshots(vh) {
			out = append(out, exchangeObject(e))
		}
	}
	if vhost != "" {
		collect(vhost)
	} else {
		for _, name := range s.deps.Broker.VHostNames() {
			if s.canSeeVHost(au, name) {
				collect(name)
			}
		}
	}
	writeJSON(w, http.StatusOK, emptyIfNil(filterMap(out, filter)))
}

func (s *Server) getExchange(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	ex, ok := s.deps.Broker.ExchangeSnapshot(vhost, name)
	if !ok {
		exchangeNotFound(w, vhost, name)
		return
	}
	writeJSON(w, http.StatusOK, exchangeObject(ex))
}

func exchangeObject(e broker.ExchangeSnapshot) map[string]any {
	policyName := any(nil)
	if e.Policy != "" {
		policyName = e.Policy
	}
	return map[string]any{
		"name":                        e.Name,
		"vhost":                       e.VHost,
		"type":                        string(e.Type),
		"durable":                     e.Durable,
		"auto_delete":                 e.AutoDelete,
		"internal":                    e.Internal,
		"arguments":                   emptyMapIfNil(e.Arguments),
		"policy":                      policyName,
		"effective_policy_definition": emptyMapIfNil(e.EffectivePolicyDefinition),
		// message_stats 未按交换机维度计数（内核在队列维度计数），
		// 因此这里不提供该字段，而不是给出一个恒为 0 的假值。
	}
}

// exchangeDeclareRequest 是 PUT /api/exchanges/{vhost}/{name} 的请求体（字段名对齐 RabbitMQ）。
type exchangeDeclareRequest struct {
	// Type 为交换机类型（direct / fanout / topic / headers）。缺省按 direct ——
	// 对齐 RabbitMQ 实测：不带 type 的 PUT 会建成 direct（201）。
	Type       string         `json:"type"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Internal   bool           `json:"internal"`
	Arguments  map[string]any `json:"arguments"`
}

// putExchange 声明一个交换机。
//
// RabbitMQ 管理 UI 的「Add a new exchange」就是这个端点。与 putQueue 同一套做法：
// 经 SessionFor(调用方) 复用内核的声明逻辑 —— `configure` 权限、保留名、类型合法性、
// 参数等价性检查全部照走，因此管理面建的交换机与客户端建的完全一致。
//
// 状态码：新建 201、已存在且参数等价 204、参数或类型非法 400。
func (s *Server) putExchange(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var req exchangeDeclareRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	if req.Type == "" {
		req.Type = "direct"
	}
	_, existed := s.deps.Broker.ExchangeSnapshot(vhost, name)

	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()
	if err := sess.DeclareExchange(sdk.ExchangeDeclare{
		Name:       normalizeExchange(name),
		Type:       sdk.ExchangeType(req.Type),
		Durable:    req.Durable,
		AutoDelete: req.AutoDelete,
		Internal:   req.Internal,
		Arguments:  req.Arguments,
	}); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面声明交换机", "actor", au.Name, "vhost", vhost, "exchange", name, "type", req.Type)
	status := http.StatusCreated
	if existed {
		status = http.StatusNoContent
	}
	w.WriteHeader(status)
}

// deleteExchange 删除交换机（声明端点的对称操作；队列页既有删除，交换机页也要有）。
//
// if-unused=true 时，交换机上仍有绑定的会被拒绝（对齐 RabbitMQ 的同名查询参数）。
func (s *Server) deleteExchange(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()
	if err := sess.DeleteExchange(normalizeExchange(name), queryBool(r, "if-unused")); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面删除交换机", "actor", au.Name, "vhost", vhost, "exchange", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getExchangeSourceBindings(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.exchangeBindings(w, p, au, "source")
}

func (s *Server) getExchangeDestinationBindings(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.exchangeBindings(w, p, au, "destination")
}

func (s *Server) exchangeBindings(w http.ResponseWriter, p params, au authUser, direction string) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	if _, ok := s.deps.Broker.ExchangeSnapshot(vhost, name); !ok {
		exchangeNotFound(w, vhost, name)
		return
	}
	var out []map[string]any
	if direction == "source" {
		out = bindingObjects(s.deps.Broker.ExchangeSourceBindings(vhost, name))
	} else {
		for _, b := range s.deps.Broker.BindingSnapshots(vhost) {
			if b.DestinationType == "exchange" && b.Destination == normalizeExchange(name) {
				out = append(out, bindingObject(b))
			}
		}
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

// publishRequest 是 POST /api/exchanges/{vhost}/{name}/publish 的请求体。
type publishRequest struct {
	Properties      map[string]any `json:"properties"`
	RoutingKey      string         `json:"routing_key"`
	Payload         string         `json:"payload"`
	PayloadEncoding string         `json:"payload_encoding"`
	Mandatory       bool           `json:"mandatory"`
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var req publishRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}

	var body []byte
	if req.PayloadEncoding == "base64" {
		decoded, err := base64.StdEncoding.DecodeString(req.Payload)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Bad Request", "payload 不是合法 base64: "+err.Error())
			return
		}
		body = decoded
	} else {
		body = []byte(req.Payload)
	}

	msg := &sdk.Message{
		Exchange:   normalizeExchange(name),
		RoutingKey: req.RoutingKey,
		Properties: propertiesFromMap(req.Properties),
		Body:       body,
	}

	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()

	res, err := sess.Publish(msg, normalizeExchange(name), req.RoutingKey, req.Mandatory)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"routed": res.Routed})
}

// propertiesFromMap 把管理 API 的属性表翻译成内核属性。
//
// 宽容取值：客户端的属性表常带多余字段或类型不一致的数值，这里只挑认识的键，
// 不做严格校验 —— 管理面发测试消息是运维动作，不该因为一个多余字段就失败。
func propertiesFromMap(m map[string]any) sdk.Properties {
	var p sdk.Properties
	if m == nil {
		return p
	}
	p.ContentType = stringOf(m["content_type"])
	p.ContentEncoding = stringOf(m["content_encoding"])
	if headers, ok := m["headers"].(map[string]any); ok {
		p.Headers = headers
	}
	p.DeliveryMode = uint8(intOf(m["delivery_mode"]))
	p.Priority = uint8(intOf(m["priority"]))
	p.CorrelationID = stringOf(m["correlation_id"])
	p.ReplyTo = stringOf(m["reply_to"])
	p.Expiration = stringOf(m["expiration"])
	p.MessageID = stringOf(m["message_id"])
	p.Type = stringOf(m["type"])
	p.UserID = stringOf(m["user_id"])
	p.AppID = stringOf(m["app_id"])
	if ts := intOf(m["timestamp"]); ts > 0 {
		p.Timestamp = time.Unix(int64(ts), 0).UTC()
	}
	return p
}

// ---------------------------------------------------------------------------
// 绑定
// ---------------------------------------------------------------------------

func (s *Server) getBindings(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if vhost == "" {
		vhost = r.URL.Query().Get("vhost")
	}
	if vhost != "" && !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var out []map[string]any
	if vhost != "" {
		out = bindingObjects(s.deps.Broker.BindingSnapshots(vhost))
	} else {
		for _, name := range s.deps.Broker.VHostNames() {
			if s.canSeeVHost(au, name) {
				out = append(out, bindingObjects(s.deps.Broker.BindingSnapshots(name))...)
			}
		}
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func bindingObjects(list []broker.BindingSnapshot) []map[string]any {
	out := make([]map[string]any, 0, len(list))
	for _, b := range list {
		out = append(out, bindingObject(b))
	}
	return out
}

func bindingObject(b broker.BindingSnapshot) map[string]any {
	return map[string]any{
		"source":           b.Source,
		"vhost":            b.VHost,
		"destination":      b.Destination,
		"destination_type": b.DestinationType,
		"routing_key":      b.RoutingKey,
		"arguments":        emptyMapIfNil(b.Arguments),
		"properties_key":   b.PropertiesKey,
	}
}

// bindingRequest 是建立绑定时的请求体（字段名对齐 RabbitMQ）。
type bindingRequest struct {
	RoutingKey string         `json:"routing_key"`
	Arguments  map[string]any `json:"arguments"`
}

func (s *Server) postQueueBinding(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	s.createBinding(w, r, p, au, "queue")
}

func (s *Server) postExchangeBinding(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	s.createBinding(w, r, p, au, "exchange")
}

func (s *Server) deleteQueueBinding(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.removeBinding(w, p, au, "queue")
}

func (s *Server) deleteExchangeBinding(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.removeBinding(w, p, au, "exchange")
}

// createBinding 建立一条绑定（队列到交换机，或交换机到交换机）。
//
// 与声明端点同一套做法：经 SessionFor(调用方) 复用内核的绑定逻辑 —— `configure` 权限、
// 源与目标必须存在、arguments 的语义全部照走，因此管理面建的绑定与客户端建的完全一致。
//
// 状态码对齐 RabbitMQ 实测：成功一律 201（重复绑定是幂等的，同样回 201）。
func (s *Server) createBinding(w http.ResponseWriter, r *http.Request, p params, au authUser, destType string) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, source, dest := p["vhost"], p["source"], p["destination"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var req bindingRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()

	var bindErr error
	if destType == "queue" {
		bindErr = sess.BindQueue(dest, normalizeExchange(source), req.RoutingKey, req.Arguments)
	} else {
		bindErr = sess.BindExchange(normalizeExchange(dest), normalizeExchange(source), req.RoutingKey, req.Arguments)
	}
	if bindErr != nil {
		writeKernelError(w, bindErr)
		return
	}
	s.log.Info("管理面建立绑定", "actor", au.Name, "vhost", vhost,
		"source", source, "destination", dest, "destination_type", destType, "routing_key", req.RoutingKey)
	w.WriteHeader(http.StatusCreated)
}

// removeBinding 按 properties_key 删除一条绑定。
//
// 管理 API 的 URL 里带的是 properties_key，而内核的解绑接口按"路由键 + 参数"定位，
// 因此先按 properties_key 找到那条绑定、取出它真正的 (routing_key, arguments) 再解绑。
// 这样做的好处是：即使 properties_key 的**取值算法**与 RabbitMQ 不同（各家实现各自的哈希），
// 只要列表里读出来的值原样回传就能删掉，rabbitmqadmin 这类工具因此照常可用。
//
// 找不到 → 404（对齐 RabbitMQ：重复删除回 404）。
func (s *Server) removeBinding(w http.ResponseWriter, p params, au authUser, destType string) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	source := normalizeExchange(p["source"])
	dest := p["destination"]
	if destType == "exchange" {
		dest = normalizeExchange(dest)
	}
	props := p["props"]

	var target *broker.BindingSnapshot
	for _, b := range s.deps.Broker.BindingSnapshots(vhost) {
		if b.Source == source && b.Destination == dest &&
			b.DestinationType == destType && b.PropertiesKey == props {
			found := b
			target = &found
			break
		}
	}
	if target == nil {
		writeError(w, http.StatusNotFound, "Object Not Found",
			fmt.Sprintf("未找到绑定 %s -> %s（%s, properties_key=%s）", source, dest, destType, props))
		return
	}

	sess, err := s.deps.Broker.SessionFor(au.Name, vhost)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	defer sess.Close()

	var unbindErr error
	if destType == "queue" {
		unbindErr = sess.UnbindQueue(dest, source, target.RoutingKey, target.Arguments)
	} else {
		unbindErr = sess.UnbindExchange(dest, source, target.RoutingKey, target.Arguments)
	}
	if unbindErr != nil {
		writeKernelError(w, unbindErr)
		return
	}
	s.log.Info("管理面解除绑定", "actor", au.Name, "vhost", vhost,
		"source", source, "destination", dest, "destination_type", destType)
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// 连接与通道
// ---------------------------------------------------------------------------

func (s *Server) getConnections(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	var out []map[string]any
	for _, c := range s.deps.Broker.Connections() {
		if c.VHost != "" && !s.canSeeVHost(au, c.VHost) {
			continue
		}
		out = append(out, connectionObject(s.deps.NodeName, c))
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func (s *Server) getConnection(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	c, ok := s.deps.Broker.Connection(p["name"])
	if !ok || (c.VHost != "" && !s.canSeeVHost(au, c.VHost)) {
		connectionNotFound(w, p["name"])
		return
	}
	writeJSON(w, http.StatusOK, connectionObject(s.deps.NodeName, c))
}

func connectionObject(node string, c broker.ConnectionSnapshot) map[string]any {
	proto := c.Protocol
	if proto == "" {
		proto = "unknown"
	}
	return map[string]any{
		"name":              c.Name,
		"vhost":             c.VHost,
		"user":              c.User,
		"node":              node,
		"state":             c.State,
		"protocol":          proto,
		"channels":          c.Channels,
		"connected_at":      c.ConnectedAt.UnixMilli(),
		"timeout":           c.HeartbeatSeconds,
		"frame_max":         c.FrameMax,
		"recv_oct":          0,
		"send_oct":          0,
		"ssl":               false,
		"peer_host":         c.PeerHost,
		"peer_port":         c.PeerPort,
		"auth_mechanism":    c.AuthMechanism,
		"client_properties": emptyMapIfNil(c.ClientProperties),
	}
}

func (s *Server) closeConnection(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["name"]
	c, ok := s.deps.Broker.Connection(name)
	if !ok || (c.VHost != "" && !s.canSeeVHost(au, c.VHost)) {
		connectionNotFound(w, name)
		return
	}
	reason := r.URL.Query().Get("reason")
	if !s.deps.Broker.CloseConnection(name, reason) {
		writeError(w, http.StatusNotFound, "Object Not Found",
			fmt.Sprintf("连接 %s 已不可关闭（可能正在握手或已断开）", name))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getChannels(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	var out []map[string]any
	for _, ch := range s.deps.Broker.Channels() {
		if ch.VHost != "" && !s.canSeeVHost(au, ch.VHost) {
			continue
		}
		out = append(out, channelObject(ch))
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func (s *Server) getChannel(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["name"]
	for _, ch := range s.deps.Broker.Channels() {
		if channelName(ch) == name && (ch.VHost == "" || s.canSeeVHost(au, ch.VHost)) {
			writeJSON(w, http.StatusOK, channelObject(ch))
			return
		}
	}
	writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("未找到通道 %s", name))
}

// channelName 生成 RabbitMQ 风格的通道名："<连接名> (<通道号>)"。
func channelName(ch broker.ChannelSnapshot) string {
	return fmt.Sprintf("%s (%d)", ch.ConnectionName, ch.Number)
}

func channelObject(ch broker.ChannelSnapshot) map[string]any {
	return map[string]any{
		"name":                    channelName(ch),
		"number":                  ch.Number,
		"user":                    ch.User,
		"vhost":                   ch.VHost,
		"node":                    "",
		"state":                   ch.State,
		"consumer_count":          ch.ConsumerCount,
		"prefetch_count":          ch.PrefetchCount,
		"confirm":                 ch.Confirm,
		"unacked":                 ch.Unacked,
		"messages_unacknowledged": ch.Unacked,
		"connection_details":      map[string]any{"name": ch.ConnectionName},
	}
}

// ---------------------------------------------------------------------------
// 消费者
// ---------------------------------------------------------------------------

func (s *Server) getConsumers(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if vhost == "" {
		vhost = r.URL.Query().Get("vhost")
	}
	if vhost != "" && !s.canSeeVHost(au, vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var out []map[string]any
	collect := func(vh string) {
		for _, c := range s.deps.Broker.ConsumerSnapshots(vh) {
			out = append(out, consumerObject(c))
		}
	}
	if vhost != "" {
		collect(vhost)
	} else {
		for _, name := range s.deps.Broker.VHostNames() {
			if s.canSeeVHost(au, name) {
				collect(name)
			}
		}
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func consumerObject(c broker.ConsumerSnapshot) map[string]any {
	return map[string]any{
		"consumer_tag":   c.Tag,
		"queue":          map[string]any{"name": c.Queue, "vhost": c.VHost},
		"ack_required":   c.AckRequired,
		"prefetch_count": c.Prefetch,
		"exclusive":      c.Exclusive,
		"arguments":      emptyMapIfNil(c.Arguments),
	}
}

// ---------------------------------------------------------------------------
// 用户与权限
// ---------------------------------------------------------------------------

func (s *Server) getUsers(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	var out []map[string]any
	for _, u := range s.deps.Broker.UserSnapshots() {
		out = append(out, userObject(u))
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func (s *Server) getUser(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	u, ok := s.deps.Broker.User(p["name"])
	if !ok {
		userNotFound(w, p["name"])
		return
	}
	writeJSON(w, http.StatusOK, userObject(u))
}

func userObject(u broker.UserSnapshot) map[string]any {
	return map[string]any{
		"name":         u.Name,
		"tags":         strings.Join(sortedCopy(u.Tags), " "),
		"auth_backend": "internal",
		// 管理 UI 的账号页据此渲染"总账号"标记与"已禁用/待改密"状态。
		"is_root":              u.Root,
		"disabled":             u.Disabled,
		"must_change_password": u.MustChangePassword,
		// 允许访问的管理接口功能组；空数组表示不限制（用标签允许的全部接口）。
		"api_groups": emptyIfNil(u.APIGroups),
	}
}

// userRequest 是 PUT /api/users/{name} 的请求体。
//
// 可选字段用可空形式，区分"没传"与"传了零值"：更新账号时没传 password 表示**不改口令**，
// 没传 disabled 表示**保持原状** —— 否则前端只改一下标签就会顺手把账号解禁。
type userRequest struct {
	Password           string `json:"password"`
	Tags               any    `json:"tags"`
	Disabled           *bool  `json:"disabled"`
	MustChangePassword *bool  `json:"must_change_password"`
	// APIGroups 是可访问的管理接口功能组；传空数组表示"不限制"。不传则保持原状。
	APIGroups *[]string `json:"api_groups"`
}

func (s *Server) putUser(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["name"]
	var req userRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	current, existed := s.deps.Broker.User(name)
	// 新建必须给口令：没有口令的账号建出来也登不上，与其留个"哑账号"不如直接报错。
	if !existed && req.Password == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "新建账号必须提供 password")
		return
	}
	tags := append([]string(nil), current.Tags...)
	if req.Tags != nil {
		tags = parseTags(req.Tags)
	}
	disabled := current.Disabled
	if req.Disabled != nil {
		disabled = *req.Disabled
	}
	mustChange := current.MustChangePassword
	if req.MustChangePassword != nil {
		mustChange = *req.MustChangePassword
	}
	apiGroups := append([]string(nil), current.APIGroups...)
	if req.APIGroups != nil {
		apiGroups = normalizeAPIGroups(*req.APIGroups)
	}

	if err := s.checkUserMutation(au, name, current, existed, tags, disabled, false); err != nil {
		writeKernelError(w, err)
		return
	}
	if _, err := s.deps.Broker.PutUser(broker.UserWrite{
		Name: name, Password: req.Password, Tags: tags, APIGroups: apiGroups,
		Disabled: disabled, MustChangePassword: mustChange,
	}); err != nil {
		writeKernelError(w, err)
		return
	}
	status := http.StatusCreated
	if existed {
		status = http.StatusNoContent
	}
	s.log.Info("管理面更新账号", "actor", au.Name, "user", name, "tags", tags,
		"disabled", disabled, "api_groups", apiGroups)
	w.WriteHeader(status)
}

// normalizeAPIGroups 去重并丢掉未知的功能组名。
//
// 丢掉未知值而不是报错：前端与服务端的功能组集合可能因版本不同而错位，
// 静默忽略未知项比"整个账号存不进去"更不容易把人挡在门外；真正的越权由
// serveHTTP 的 allowsGroup 按**已知**组名判定，未知组名不会带来额外权限。
func normalizeAPIGroups(groups []string) []string {
	out := make([]string, 0, len(groups))
	for _, g := range groups {
		if !knownAPIGroup(g) || hasTag(out, g) {
			continue
		}
		out = append(out, g)
	}
	return out
}

// knownAPIGroup 判断是否为已知的管理接口功能组名。
func knownAPIGroup(group string) bool {
	switch group {
	case apiGroupOverview, apiGroupTopology, apiGroupConnections, apiGroupAccounts,
		apiGroupPolicies, apiGroupVHosts, apiGroupCluster, apiGroupPlugins,
		apiGroupLimits, apiGroupFeatureFlags:
		return true
	default:
		return false
	}
}

// credentialsRequest 是 POST /api/users/{name}/credentials 的请求体。
type credentialsRequest struct {
	// Name 为新账号名；留空或与原名相同表示只改口令。
	Name string `json:"name"`
	// Password 为新口令；留空表示只改名。
	Password string `json:"password"`
}

// putUserCredentials 修改账号名与/或口令。
//
// 授权：本人可自助（这正是"首次登录强制改密"走的路径，因此不需要 administrator 标签），
// administrator 可改他人；总管理员账号只能由本人修改。
//
// 为什么账号名与口令放在**同一个请求**里：首次改密要同时改掉两者，
// 拆成两个请求会出现"改完账号名后旧凭据失效、第二次请求直接 401"的中间态，
// 用户会在半路被踢出去。
func (s *Server) putUserCredentials(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	name := p["name"]
	isSelf := name == au.Name
	if !isSelf && !hasTag(au.Tags, adminTag) {
		writeError(w, http.StatusForbidden, "Access refused",
			fmt.Sprintf("ACCESS_REFUSED - 用户 %s 无权修改账号 %s 的凭据", au.Name, name))
		return
	}
	current, ok := s.deps.Broker.User(name)
	if !ok {
		userNotFound(w, name)
		return
	}
	if current.Root && !isSelf {
		writeError(w, http.StatusForbidden, "Access refused",
			fmt.Sprintf("ACCESS_REFUSED - 总管理员账号 %s 只能由本人修改", name))
		return
	}
	var req credentialsRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	newName := strings.TrimSpace(req.Name)
	if newName == "" {
		newName = name
	}
	if newName == name && req.Password == "" {
		writeError(w, http.StatusBadRequest, "Bad Request", "请至少提供新的账号名或新口令")
		return
	}
	if err := s.deps.Broker.RenameUser(name, newName, req.Password); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面修改账号凭据", "actor", au.Name, "old", name, "new", newName)
	w.WriteHeader(http.StatusNoContent)
}

// checkUserMutation 校验账号的**保护规则**：总账号不可删除/禁用/降级，且不能把自己锁在门外。
//
// 三条规则都返回 403 ACCESS_REFUSED（"不允许"而不是"参数错"），便于前端区分提示语：
//  1. 总账号（root）：不可删除、不可禁用、不可移除 administrator 标签，且只能由本人修改；
//  2. 不能删除**当前登录账号**（要删先换号登录，或由另一个管理员操作）；
//  3. 任何操作之后都必须至少留下一个**启用中的 administrator**（防止把管理权改没了）。
//
// delete 为 true 表示这是删除操作（此时 tags / disabled 无意义）。
func (s *Server) checkUserMutation(
	au authUser, name string, current broker.UserSnapshot, existed bool,
	tags []string, disabled, delete bool,
) error {
	if !existed {
		return nil
	}
	if current.Root {
		switch {
		case delete:
			return sdk.Errorf(sdk.KindAccessRefused, "ACCESS_REFUSED - 总管理员账号 '%s' 不可删除", name)
		case disabled:
			return sdk.Errorf(sdk.KindAccessRefused, "ACCESS_REFUSED - 总管理员账号 '%s' 不可禁用", name)
		case !hasTag(tags, adminTag):
			return sdk.Errorf(sdk.KindAccessRefused,
				"ACCESS_REFUSED - 总管理员账号 '%s' 不可移除 administrator 标签", name)
		case au.Name != name:
			return sdk.Errorf(sdk.KindAccessRefused,
				"ACCESS_REFUSED - 总管理员账号 '%s' 只能由本人修改", name)
		}
	}
	if delete && au.Name == name {
		return sdk.Errorf(sdk.KindAccessRefused, "ACCESS_REFUSED - 不能删除当前登录账号 '%s'", name)
	}
	// 只在"该账号原本是启用中的管理员、且本次会改变这一点"时才做全局计数，
	// 避免每次改标签都遍历一遍用户表。
	if hasTag(current.Tags, adminTag) && !current.Disabled &&
		(delete || disabled || !hasTag(tags, adminTag)) && s.enabledAdminsExcept(name) == 0 {
		return sdk.Errorf(sdk.KindAccessRefused,
			"ACCESS_REFUSED - 至少要保留一个启用中的 administrator 账号")
	}
	return nil
}

// enabledAdminsExcept 统计除 exclude 之外**启用中的** administrator 数量。
func (s *Server) enabledAdminsExcept(exclude string) int {
	n := 0
	for _, u := range s.deps.Broker.UserSnapshots() {
		if u.Name == exclude || u.Disabled {
			continue
		}
		if hasTag(u.Tags, adminTag) {
			n++
		}
	}
	return n
}

// adminTag 是管理员标签（与内核 permission.go 的 adminTag 同义，管理面单独持有一份）。
const adminTag = "administrator"

// hasTag 判断标签集合里是否含某个标签。
func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

// parseTags 兼容两种 tags 表示：RabbitMQ 3.x 的 "a,b c" 字符串与 4.x 的数组。
func parseTags(v any) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return splitTags(t)
	case []any:
		var out []string
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func splitTags(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
	var out []string
	for _, f := range fields {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

func (s *Server) deleteUser(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["name"]
	current, existed := s.deps.Broker.User(name)
	if !existed {
		userNotFound(w, name)
		return
	}
	if err := s.checkUserMutation(au, name, current, true, nil, false, true); err != nil {
		writeKernelError(w, err)
		return
	}
	ok, err := s.deps.Broker.DeleteUser(name)
	if err != nil {
		writeKernelError(w, err)
		return
	}
	if !ok {
		userNotFound(w, name)
		return
	}
	s.log.Info("管理面删除账号", "actor", au.Name, "user", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getPermissions(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptyIfNil(permissionObjects(s.deps.Broker.PermissionSnapshots())))
}

func (s *Server) getVHostPermissions(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if !s.deps.Broker.VHostExists(vhost) {
		vhostNotFound(w, vhost)
		return
	}
	writeJSON(w, http.StatusOK, emptyIfNil(permissionObjects(s.deps.Broker.VHostPermissions(vhost))))
}

func (s *Server) getPermission(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	for _, perm := range s.deps.Broker.UserPermissions(p["user"]) {
		if perm.VHost == p["vhost"] {
			writeJSON(w, http.StatusOK, permissionObject(perm))
			return
		}
	}
	writeError(w, http.StatusNotFound, "Object Not Found",
		fmt.Sprintf("用户 %s 在 vhost %s 上没有权限记录", p["user"], p["vhost"]))
}

type permissionRequest struct {
	Configure string `json:"configure"`
	Write     string `json:"write"`
	Read      string `json:"read"`
}

func (s *Server) putPermission(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	user, vhost := p["user"], p["vhost"]
	if _, ok := s.deps.Broker.User(user); !ok {
		userNotFound(w, user)
		return
	}
	if !s.deps.Broker.VHostExists(vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var req permissionRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	if err := s.deps.Broker.SetPermission(user, vhost, req.Configure, req.Write, req.Read); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面设置权限", "actor", au.Name, "user", user, "vhost", vhost)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deletePermission(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	ok, err := s.deps.Broker.DeletePermission(p["user"], p["vhost"])
	if err != nil {
		writeKernelError(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "Object Not Found",
			fmt.Sprintf("用户 %s 在 vhost %s 上没有权限记录", p["user"], p["vhost"]))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func permissionObjects(perms []broker.PermissionSnapshot) []map[string]any {
	out := make([]map[string]any, 0, len(perms))
	for _, p := range perms {
		out = append(out, permissionObject(p))
	}
	return out
}

func permissionObject(p broker.PermissionSnapshot) map[string]any {
	return map[string]any{
		"user":      p.User,
		"vhost":     p.VHost,
		"configure": p.Configure,
		"write":     p.Write,
		"read":      p.Read,
	}
}

// ---------------------------------------------------------------------------
// 策略（policy）
//
// 字段名对齐 RabbitMQ 的管理 API：`pattern` / `apply-to` / `definition` / `priority`。
// 策略的作用是"按名称匹配批量给队列/交换机设参数"，因此它的效果体现在队列/交换机对象的
// `policy` 与 `effective_policy_definition` 两个字段上（见 queueObject / exchangeObject）。
// ---------------------------------------------------------------------------

func (s *Server) getPolicies(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policyObjects(s.deps.Broker.Policies()))
}

func (s *Server) getVHostPolicies(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if !s.deps.Broker.VHostExists(vhost) {
		vhostNotFound(w, vhost)
		return
	}
	writeJSON(w, http.StatusOK, policyObjects(s.deps.Broker.VHostPolicies(vhost)))
}

func (s *Server) getPolicy(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	pol, ok := s.deps.Broker.Policy(p["vhost"], p["name"])
	if !ok {
		policyNotFound(w, p["vhost"], p["name"])
		return
	}
	writeJSON(w, http.StatusOK, policyObject(pol))
}

// policyRequest 是 PUT /api/policies/{vhost}/{name} 的请求体。
//
// `apply-to` 用连字符（RabbitMQ 的字段名），同时接受下划线写法 ——
// 手写脚本里两种都常见，没必要为此让调用方踩坑。
type policyRequest struct {
	Pattern    string         `json:"pattern"`
	Definition map[string]any `json:"definition"`
	ApplyTo    string         `json:"apply-to"`
	ApplyToAlt string         `json:"apply_to"`
	Priority   *int           `json:"priority"`
}

func (s *Server) putPolicy(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost, name := p["vhost"], p["name"]
	if !s.deps.Broker.VHostExists(vhost) {
		vhostNotFound(w, vhost)
		return
	}
	var req policyRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	applyTo := req.ApplyTo
	if applyTo == "" {
		applyTo = req.ApplyToAlt
	}
	priority := 0
	if req.Priority != nil {
		priority = *req.Priority
	}
	rec := broker.PolicySnapshot{
		VHost: vhost, Name: name, Pattern: req.Pattern,
		ApplyTo: applyTo, Definition: req.Definition, Priority: priority,
	}
	_, existed := s.deps.Broker.Policy(vhost, name)
	if err := s.deps.Broker.SetPolicy(rec); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面设置策略", "actor", au.Name, "vhost", vhost, "policy", name, "apply_to", applyTo)
	// 对齐 RabbitMQ：新建回 201、更新回 204（实测 RabbitMQ 4.3 创建策略返回 201）。
	status := http.StatusCreated
	if existed {
		status = http.StatusNoContent
	}
	w.WriteHeader(status)
}

func (s *Server) deletePolicy(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	ok, err := s.deps.Broker.DeletePolicy(p["vhost"], p["name"])
	if err != nil {
		writeKernelError(w, err)
		return
	}
	if !ok {
		policyNotFound(w, p["vhost"], p["name"])
		return
	}
	s.log.Info("管理面删除策略", "actor", au.Name, "vhost", p["vhost"], "policy", p["name"])
	w.WriteHeader(http.StatusNoContent)
}

func policyObjects(policies []broker.PolicySnapshot) []map[string]any {
	out := make([]map[string]any, 0, len(policies))
	for _, pol := range policies {
		out = append(out, policyObject(pol))
	}
	return out
}

func policyObject(pol broker.PolicySnapshot) map[string]any {
	return map[string]any{
		"vhost":      pol.VHost,
		"name":       pol.Name,
		"pattern":    pol.Pattern,
		"apply-to":   pol.ApplyTo,
		"definition": emptyMapIfNil(pol.Definition),
		"priority":   pol.Priority,
	}
}

func policyNotFound(w http.ResponseWriter, vhost, name string) {
	writeError(w, http.StatusNotFound, "Object Not Found",
		fmt.Sprintf("vhost %s 上没有名为 %s 的策略", vhost, name))
}

// ---------------------------------------------------------------------------
// vhost 级限制
// ---------------------------------------------------------------------------

// getVHostLimits 实现 GET /api/vhost-limits 与 GET /api/vhost-limits/{vhost}。
//
// 返回形状对齐 RabbitMQ：按 vhost 分组，值放在嵌套的 value 对象里 ——
// `[{"vhost":"/","value":{"max-queues":10}}]`。
func (s *Server) getVHostLimits(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	vhost := p["vhost"]
	if vhost != "" && !s.deps.Broker.VHostExists(vhost) {
		vhostNotFound(w, vhost)
		return
	}

	grouped := map[string]map[string]int{}
	order := make([]string, 0)
	for _, item := range s.deps.Broker.VHostLimits() {
		if vhost != "" && item.VHost != vhost {
			continue
		}
		if !s.canSeeVHost(au, item.VHost) {
			continue
		}
		values, ok := grouped[item.VHost]
		if !ok {
			values = map[string]int{}
			grouped[item.VHost] = values
			order = append(order, item.VHost)
		}
		values[item.Name] = item.Value
	}
	out := make([]map[string]any, 0, len(order))
	for _, name := range order {
		out = append(out, map[string]any{"vhost": name, "value": grouped[name]})
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

// putVHostLimit 实现 PUT /api/vhost-limits/{vhost}/{name}，请求体 {"value": N}。
//
// 写成功返回 204（RabbitMQ 实测就是 204，不是 201）。
func (s *Server) putVHostLimit(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	var req vhostLimitRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	if req.Value == nil {
		writeError(w, http.StatusBadRequest, "Bad Request", "请求体缺少 value 字段")
		return
	}
	if !s.deps.Broker.VHostExists(p["vhost"]) {
		vhostNotFound(w, p["vhost"])
		return
	}
	if err := s.deps.Broker.SetVHostLimit(p["vhost"], p["name"], *req.Value); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面设置 vhost 限制", "actor", au.Name, "vhost", p["vhost"], "limit", p["name"], "value", *req.Value)
	w.WriteHeader(http.StatusNoContent)
}

// deleteVHostLimit 实现 DELETE /api/vhost-limits/{vhost}/{name}。
//
// 对齐 RabbitMQ：删除不存在（或本来就没设）的限制同样返回 204。
func (s *Server) deleteVHostLimit(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	if !s.deps.Broker.VHostExists(p["vhost"]) {
		vhostNotFound(w, p["vhost"])
		return
	}
	if _, err := s.deps.Broker.DeleteVHostLimit(p["vhost"], p["name"]); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面删除 vhost 限制", "actor", au.Name, "vhost", p["vhost"], "limit", p["name"])
	w.WriteHeader(http.StatusNoContent)
}

// vhostLimitRequest 是 PUT /api/vhost-limits/{vhost}/{name} 的请求体。
//
// Value 用指针：只有"显式传了 value"才算合法请求，缺字段要报 400 而不是当成 0。
type vhostLimitRequest struct {
	Value *int `json:"value"`
}

// ---------------------------------------------------------------------------
// 特性开关与弃用特性
// ---------------------------------------------------------------------------

func (s *Server) getFeatureFlags(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	flags := s.deps.Broker.FeatureFlags()
	out := make([]map[string]any, 0, len(flags))
	for _, f := range flags {
		out = append(out, featureFlagObject(f))
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func (s *Server) enableFeatureFlag(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.setFeatureFlag(w, p, au, true)
}

func (s *Server) disableFeatureFlag(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.setFeatureFlag(w, p, au, false)
}

// setFeatureFlag 是 enable / disable 两个端点的共同实现。
func (s *Server) setFeatureFlag(w http.ResponseWriter, p params, au authUser, enabled bool) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["name"]
	if _, ok := s.deps.Broker.FeatureFlag(name); !ok {
		writeError(w, http.StatusNotFound, "Object Not Found",
			fmt.Sprintf("未知的特性开关 %s", name))
		return
	}
	if err := s.deps.Broker.SetFeatureFlag(name, enabled); err != nil {
		writeKernelError(w, err)
		return
	}
	s.log.Info("管理面变更特性开关", "actor", au.Name, "flag", name, "enabled", enabled)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getDeprecatedFeatures(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	items := s.deps.Broker.DeprecatedFeatures()
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{
			"name":              item.Name,
			"state":             item.State,
			"deprecation_phase": item.DeprecationPhase,
			"desc":              item.Desc,
			"doc_url":           item.DocURL,
			"provided_by":       item.ProvidedBy,
		})
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func featureFlagObject(f broker.FeatureFlagSnapshot) map[string]any {
	return map[string]any{
		"name":        f.Name,
		"state":       f.State,
		"stability":   f.Stability,
		"desc":        f.Desc,
		"doc_url":     f.DocURL,
		"provided_by": f.ProvidedBy,
	}
}

// ---------------------------------------------------------------------------
// 插件
// ---------------------------------------------------------------------------

func (s *Server) getPlugins(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	infos := s.deps.Plugins.Plugins()
	out := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		out = append(out, pluginObject(info, s.pluginConsoleURL(info.Name)))
	}
	writeJSON(w, http.StatusOK, emptyIfNil(out))
}

func (s *Server) getPlugin(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	info, ok := s.deps.Plugins.Plugin(p["name"])
	if !ok {
		writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("未找到插件 %s", p["name"]))
		return
	}
	writeJSON(w, http.StatusOK, pluginObject(info, s.pluginConsoleURL(info.Name)))
}

// pluginConsoleURL 返回插件自带管理界面的地址（未声明时为空串）。
//
// 插件运行时是**可选**实现该能力，因此这里用类型断言而不是把它并进 PluginController。
func (s *Server) pluginConsoleURL(name string) string {
	if r, ok := s.deps.Plugins.(PluginConsoleResolver); ok {
		return r.ConsoleURL(name)
	}
	return ""
}

func pluginObject(info sdk.Info, consoleURL string) map[string]any {
	return map[string]any{
		"name":         info.Name,
		"version":      info.Version,
		"api_version":  info.APIVersion,
		"state":        string(info.State),
		"required":     info.Required,
		"builtin":      info.Builtin,
		"capabilities": emptyIfNil(info.Capabilities),
		"dependencies": emptyIfNil(info.Dependencies),
		"description":  info.Description,
		// runtime_note 是插件自报运行期状态的原因（如外部进程插件 down 的原因）：
		// 没有它，运维只能看到状态变成了 down，却不知道为什么。
		"runtime_note": info.RuntimeNote,
		// console_url 是插件自带管理界面的地址（部署方在配置里声明，非插件 API 的一部分）：
		// 有它，管理后台的插件页就能直接跳到该插件的操作界面。
		"console_url": consoleURL,
	}
}

func (s *Server) enablePlugin(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.setPluginState(w, p, au, true)
}

func (s *Server) disablePlugin(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.setPluginState(w, p, au, false)
}

func (s *Server) setPluginState(w http.ResponseWriter, p params, au authUser, enable bool) {
	if err := au.requireAdministrator(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["name"]
	if _, ok := s.deps.Plugins.Plugin(name); !ok {
		writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("未找到插件 %s", name))
		return
	}
	var err error
	action := "启用"
	if enable {
		err = s.deps.Plugins.Enable(name)
	} else {
		action = "停用"
		err = s.deps.Plugins.Disable(name)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", fmt.Sprintf("%s插件 %s 失败: %v", action, name, err))
		return
	}
	s.log.Warn("管理面变更插件状态", "actor", au.Name, "plugin", name, "action", action)
	info, _ := s.deps.Plugins.Plugin(name)
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "state": string(info.State)})
}

// ---------------------------------------------------------------------------
// 通用工具
// ---------------------------------------------------------------------------

func vhostNotFound(w http.ResponseWriter, vhost string) {
	writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("vhost %s 不存在或无权访问", vhost))
}

func queueNotFound(w http.ResponseWriter, vhost, name string) {
	writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("未找到队列 %s（vhost %s）", name, vhost))
}

func exchangeNotFound(w http.ResponseWriter, vhost, name string) {
	writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("未找到交换机 %s（vhost %s）", name, vhost))
}

func connectionNotFound(w http.ResponseWriter, name string) {
	writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("未找到连接 %s", name))
}

func userNotFound(w http.ResponseWriter, name string) {
	writeError(w, http.StatusNotFound, "Object Not Found", fmt.Sprintf("未找到用户 %s", name))
}

// normalizeExchange 把管理 API 的 "amq.default" 归一为内核里的默认交换机名（空串）。
func normalizeExchange(name string) string {
	if name == "amq.default" {
		return ""
	}
	return name
}

// queryBool 解析布尔查询参数（支持 "true"/"1"，缺省为 false）。
func queryBool(r *http.Request, key string) bool {
	v := r.URL.Query().Get(key)
	return v == "true" || v == "1"
}

// nameFilter 解析 name / use_regex 查询参数。
//
// 语义对齐 RabbitMQ：use_regex 缺省为 true（按正则匹配），显式传 false 时按子串匹配。
func nameFilter(r *http.Request) (func(string) bool, error) {
	name := r.URL.Query().Get("name")
	if name == "" {
		return nil, nil
	}
	useRegex := true
	if v := r.URL.Query().Get("use_regex"); v != "" {
		useRegex = v == "true" || v == "1"
	}
	if !useRegex {
		return func(s string) bool { return strings.Contains(s, name) }, nil
	}
	re, err := regexp.Compile(name)
	if err != nil {
		return nil, fmt.Errorf("name 不是合法正则: %v", err)
	}
	return re.MatchString, nil
}

func filterMap(list []map[string]any, filter func(string) bool) []map[string]any {
	if filter == nil {
		return list
	}
	out := make([]map[string]any, 0, len(list))
	for _, item := range list {
		name, _ := item["name"].(string)
		if filter(name) {
			out = append(out, item)
		}
	}
	return out
}

// emptyIfNil 保证 JSON 里是 [] 而不是 null。
//
// 这一点对前端与 rabbitmqadmin 都很关键：null 会让 "for x in data" 直接报错，
// 而空数组是安全的。
func emptyIfNil[T any](list []T) []T {
	if list == nil {
		return []T{}
	}
	return list
}

func emptyMapIfNil(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// emptySliceIfNil 让 JSON 输出 `[]` 而不是 `null`，便于门禁用例与脚本稳定判空。
func emptySliceIfNil(v []uint64) []uint64 {
	if v == nil {
		return []uint64{}
	}
	return v
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

// intOf 宽容地把 JSON 数值转成 int（JSON 解出来是 float64）。
func intOf(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	default:
		return 0
	}
}

// splitHostPort 拆分监听地址（" [::]:5672" → "::" + "5672"）。
func splitHostPort(addr string) (string, string, error) {
	idx := strings.LastIndex(addr, ":")
	if idx < 0 {
		return addr, "", fmt.Errorf("地址缺少端口: %s", addr)
	}
	host := strings.Trim(addr[:idx], "[]")
	return host, addr[idx+1:], nil
}
