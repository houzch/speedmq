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

	"github.com/houzch/swiftmq/internal/broker"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
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
	s.handle(http.MethodGet, "/api/overview", s.getOverview)
	s.handle(http.MethodGet, "/api/nodes", s.getNodes)
	s.handle(http.MethodGet, "/api/whoami", s.getWhoami)

	// ---- vhost ----
	s.handle(http.MethodGet, "/api/vhosts", s.getVHosts)
	s.handle(http.MethodGet, "/api/vhosts/{vhost}", s.getVHost)

	// ---- 队列 ----
	s.handle(http.MethodGet, "/api/queues", s.getQueues)
	s.handle(http.MethodGet, "/api/queues/{vhost}", s.getQueues)
	s.handle(http.MethodGet, "/api/queues/{vhost}/{name}", s.getQueue)
	s.handle(http.MethodDelete, "/api/queues/{vhost}/{name}", s.deleteQueue)
	s.handle(http.MethodDelete, "/api/queues/{vhost}/{name}/contents", s.purgeQueue)
	s.handle(http.MethodPost, "/api/queues/{vhost}/{name}/get", s.getQueueMessages)
	s.handle(http.MethodGet, "/api/queues/{vhost}/{name}/bindings", s.getQueueBindings)

	// ---- 交换机 ----
	s.handle(http.MethodGet, "/api/exchanges", s.getExchanges)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}", s.getExchanges)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}/{name}", s.getExchange)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}/{name}/bindings/source", s.getExchangeSourceBindings)
	s.handle(http.MethodGet, "/api/exchanges/{vhost}/{name}/bindings/destination", s.getExchangeDestinationBindings)
	s.handle(http.MethodPost, "/api/exchanges/{vhost}/{name}/publish", s.publish)

	// ---- 绑定 ----
	s.handle(http.MethodGet, "/api/bindings", s.getBindings)
	s.handle(http.MethodGet, "/api/bindings/{vhost}", s.getBindings)

	// ---- 连接与通道 ----
	s.handle(http.MethodGet, "/api/connections", s.getConnections)
	s.handle(http.MethodGet, "/api/connections/{name}", s.getConnection)
	s.handle(http.MethodDelete, "/api/connections/{name}", s.closeConnection)
	s.handle(http.MethodGet, "/api/channels", s.getChannels)
	s.handle(http.MethodGet, "/api/channels/{name}", s.getChannel)

	// ---- 消费者 ----
	s.handle(http.MethodGet, "/api/consumers", s.getConsumers)
	s.handle(http.MethodGet, "/api/consumers/{vhost}", s.getConsumers)

	// ---- 用户与权限 ----
	s.handle(http.MethodGet, "/api/users", s.getUsers)
	s.handle(http.MethodGet, "/api/users/{name}", s.getUser)
	s.handle(http.MethodPut, "/api/users/{name}", s.putUser)
	s.handle(http.MethodDelete, "/api/users/{name}", s.deleteUser)
	s.handle(http.MethodGet, "/api/permissions", s.getPermissions)
	s.handle(http.MethodGet, "/api/vhosts/{vhost}/permissions", s.getVHostPermissions)
	s.handle(http.MethodGet, "/api/permissions/{vhost}/{user}", s.getPermission)
	s.handle(http.MethodPut, "/api/permissions/{vhost}/{user}", s.putPermission)
	s.handle(http.MethodDelete, "/api/permissions/{vhost}/{user}", s.deletePermission)

	// ---- 策略（M5 未实现，返回空数组而不是 404，保持工具链可用）----
	s.handle(http.MethodGet, "/api/policies", s.getPolicies)
	s.handle(http.MethodGet, "/api/policies/{vhost}", s.getPolicies)

	// ---- 插件治理 ----
	s.handle(http.MethodGet, "/api/plugins", s.getPlugins)
	s.handle(http.MethodGet, "/api/plugins/{name}", s.getPlugin)
	s.handle(http.MethodPut, "/api/plugins/{name}/enable", s.enablePlugin)
	s.handle(http.MethodPut, "/api/plugins/{name}/disable", s.disablePlugin)

	// ---- 指标 ----
	s.handle(http.MethodGet, "/metrics", s.getMetrics)
	s.handle(http.MethodGet, "/api/metrics", s.getMetrics)
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
		"product_name":       "SwiftMQ",
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
		"swiftmq_blocked": s.deps.Broker.BlockedState(),
		"swiftmq_fsync":   s.deps.Broker.StorageFsync(),
	})
}

func (s *Server) listenerObjects() []listenerObject {
	var out []listenerObject
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
	writeJSON(w, http.StatusOK, []map[string]any{node})
}

func (s *Server) enabledPlugins() []string {
	var out []string
	for _, p := range s.deps.Plugins.Plugins() {
		if p.State == sdk.StateEnabled {
			out = append(out, p.Name)
		}
	}
	sort.Strings(out)
	return out
}

func (s *Server) getWhoami(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	writeJSON(w, http.StatusOK, map[string]any{
		"name":         au.Name,
		"tags":         strings.Join(sortedCopy(au.Tags), " "),
		"auth_backend": "internal",
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
	return map[string]any{
		"name":                            q.Name,
		"vhost":                           q.VHost,
		"durable":                         q.Durable,
		"auto_delete":                     q.AutoDelete,
		"exclusive":                       q.Exclusive,
		"type":                            "classic",
		"node":                            node,
		"state":                           "running",
		"arguments":                       emptyMapIfNil(q.Arguments),
		"consumers":                       q.ConsumerCount,
		"consumer_utilisation":            nil,
		"policy":                          nil,
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
		"effective_policy_definition":     map[string]any{},
	}
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
	return map[string]any{
		"name":        e.Name,
		"vhost":       e.VHost,
		"type":        string(e.Type),
		"durable":     e.Durable,
		"auto_delete": e.AutoDelete,
		"internal":    e.Internal,
		"arguments":   emptyMapIfNil(e.Arguments),
		"policy":      nil,
		// message_stats 未按交换机维度计数（内核在队列维度计数），
		// 因此这里不提供该字段，而不是给出一个恒为 0 的假值。
	}
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
	}
}

// userRequest 是 PUT /api/users/{name} 的请求体。
type userRequest struct {
	Password string `json:"password"`
	Tags     any    `json:"tags"`
}

func (s *Server) putUser(w http.ResponseWriter, r *http.Request, p params, au authUser) {
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	name := p["name"]
	var req userRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	tags := parseTags(req.Tags)
	_, existed := s.deps.Broker.User(name)
	if err := s.deps.Broker.UpsertUser(name, req.Password, tags); err != nil {
		writeKernelError(w, err)
		return
	}
	status := http.StatusCreated
	if existed {
		status = http.StatusNoContent
	}
	s.log.Info("管理面更新用户", "actor", au.Name, "user", name, "tags", tags)
	w.WriteHeader(status)
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
	if err := au.requireWrite(); err != nil {
		writeKernelError(w, err)
		return
	}
	ok, err := s.deps.Broker.DeleteUser(p["name"])
	if err != nil {
		writeKernelError(w, err)
		return
	}
	if !ok {
		userNotFound(w, p["name"])
		return
	}
	s.log.Info("管理面删除用户", "actor", au.Name, "user", p["name"])
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
	if err := au.requireWrite(); err != nil {
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
	if err := au.requireWrite(); err != nil {
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
// 策略（未实现）
// ---------------------------------------------------------------------------

// getPolicies 返回空数组：策略功能尚未实现（设计里它属于 M5 之后的补齐项）。
//
// 这里刻意返回 200 + []，而不是 404：管理 UI 与部分监控脚本会无条件拉取策略列表，
// 404 会被当成"接口不可用"，进而让整个页面报错。
func (s *Server) getPolicies(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, []any{})
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
		out = append(out, pluginObject(info))
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
	writeJSON(w, http.StatusOK, pluginObject(info))
}

func pluginObject(info sdk.Info) map[string]any {
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
	}
}

func (s *Server) enablePlugin(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.setPluginState(w, p, au, true)
}

func (s *Server) disablePlugin(w http.ResponseWriter, _ *http.Request, p params, au authUser) {
	s.setPluginState(w, p, au, false)
}

func (s *Server) setPluginState(w http.ResponseWriter, p params, au authUser, enable bool) {
	if err := au.requireWrite(); err != nil {
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
