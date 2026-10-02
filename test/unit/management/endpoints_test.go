package management_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"

	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// TestDefaultLanguageEndpoint 覆盖免认证的默认语言接口。
//
// 管理 UI 在**弹出登录框之前**就要用它决定初始语言，因此必须在不带有效凭证时可用；
// 同时确认引入了公开接口后，"未认证访问未知路径仍 401"的既有语义没有被放松。
func TestDefaultLanguageEndpoint(t *testing.T) {
	env := newTestEnv(t)

	// 刻意带上**无效**凭证：能拿到 200 即证明该路由确实绕开了鉴权。
	resp, raw := env.request(t, http.MethodGet, "/api/default-language", nil, "-", "-")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("免认证访问默认语言接口应 200，实际 %d（响应 %s）", resp.StatusCode, raw)
	}
	var body struct {
		DefaultLanguage string `json:"default_language"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if body.DefaultLanguage != "zh-CN" {
		t.Fatalf("应返回配置的默认语言 zh-CN，实际 %q", body.DefaultLanguage)
	}

	unknown, _ := env.request(t, http.MethodGet, "/api/not-exist", nil, "-", "-")
	if unknown.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无有效凭证访问未知路径仍应 401，实际 %d", unknown.StatusCode)
	}
}

// 本文件补齐 api_test.go 尚未覆盖的管理接口：
//   - 路径形式的列表（/api/queues/{vhost}、/api/exchanges/{vhost}）—— 与查询参数形式是两条独立注册路由；
//   - 绑定视图：队列维度的绑定、交换机维度的**反向**绑定、不带 vhost 的跨 vhost 汇总；
//   - 连接 / 通道 / 用户 / 插件 / 权限的**单条读取**及其 404 语义；
//   - 消费者视图（/api/consumers 与 /api/consumers/{vhost}）；
//   - /api/metrics 与 /metrics 的别名关系；
//   - 单机模式下 grow / rebalance 的拒绝语义，以及 grow 缺 count 的 400。
//
// 只读接口也要测：管理 API 的价值在于被外部工具（rabbitmqadmin / 脚本 / UI）消费，
// 少注册一条路由或漏一个字段，外部工具就会静默失效。

// nameObject 是只关心 name 字段的对象（队列 / 交换机 / 用户等列表通用）。
type nameObject struct {
	Name string `json:"name"`
}

func containsName(list []nameObject, want string) bool {
	for _, item := range list {
		if item.Name == want {
			return true
		}
	}
	return false
}

// declareExchange 用内核会话声明交换机（管理面不提供声明接口，与 declareQueue 同一理由）。
func declareExchange(t *testing.T, env *testEnv, name string, typ sdk.ExchangeType) {
	t.Helper()
	sess, err := env.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()
	if err := sess.DeclareExchange(sdk.ExchangeDeclare{Name: name, Type: typ, Durable: true}); err != nil {
		t.Fatalf("声明交换机失败: %v", err)
	}
}

// openConsumerSession 建一条**带消费者**的会话并保持打开：消费者挂在内核会话上，
// 会话一关它就消失（因此这里不 defer 关闭，只交给 t.Cleanup）。
func openConsumerSession(t *testing.T, env *testEnv, queue, tag string, prefetch uint16) {
	t.Helper()
	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 52001}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := env.broker.NewSession(remote, local)
	t.Cleanup(core.Close)

	authResp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", authResp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	sess, err := core.Session("/")
	if err != nil {
		t.Fatalf("打开 vhost 失败: %v", err)
	}
	if _, err := sess.Consume(sdk.Subscription{
		Tag: tag, Queue: queue, Prefetch: prefetch,
		Deliver: func(*sdk.Delivery) error { return nil },
	}); err != nil {
		t.Fatalf("注册消费者失败: %v", err)
	}
}

// TestQueueAndExchangeListByVHostPath 覆盖路径形式的列表路由。
//
// `/api/queues` 与 `/api/queues/{vhost}` 是两条**独立注册**的路由：查询参数形式
// （`/api/queues?vhost=%2F`）过了，不代表路径形式也过了。
func TestQueueAndExchangeListByVHostPath(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "api.path.q", nil)
	declareExchange(t, env, "api.path.ex", sdk.ExchangeDirect)

	var queues []nameObject
	if resp := env.getJSON(t, "/api/queues/%2F", &queues); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/queues/%%2F 应 200，实际 %d", resp.StatusCode)
	}
	if !containsName(queues, "api.path.q") {
		t.Fatalf("路径形式的队列列表缺少 api.path.q: %+v", queues)
	}

	var exchanges []nameObject
	if resp := env.getJSON(t, "/api/exchanges/%2F", &exchanges); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/exchanges/%%2F 应 200，实际 %d", resp.StatusCode)
	}
	if !containsName(exchanges, "api.path.ex") {
		t.Fatalf("路径形式的交换机列表缺少 api.path.ex: %+v", exchanges)
	}

	// 未知 vhost：管理员看得到所有 vhost，列表按仓库约定回 200 + `[]`（既不是 404，也不是 null）
	for _, path := range []string{"/api/queues/no.such.vhost", "/api/exchanges/no.such.vhost"} {
		resp, raw := env.request(t, http.MethodGet, path, nil, "", "")
		if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(raw)) != "[]" {
			t.Fatalf("GET %s 应回 200 + []，实际 %d %s", path, resp.StatusCode, raw)
		}
	}

	// 不带 vhost：跨全部 vhost 汇总（默认 vhost 的对象必须在内）
	var allQueues []nameObject
	if resp := env.getJSON(t, "/api/queues", &allQueues); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/queues 应 200，实际 %d", resp.StatusCode)
	}
	if !containsName(allQueues, "api.path.q") {
		t.Fatalf("跨 vhost 的队列列表缺少 api.path.q: %+v", allQueues)
	}
	var allExchanges []nameObject
	if resp := env.getJSON(t, "/api/exchanges", &allExchanges); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/exchanges 应 200，实际 %d", resp.StatusCode)
	}
	if !containsName(allExchanges, "api.path.ex") {
		t.Fatalf("跨 vhost 的交换机列表缺少 api.path.ex: %+v", allExchanges)
	}
}

// TestQueueBindingsEndpoint 覆盖队列维度的绑定视图（GET /api/queues/{vhost}/{name}/bindings）。
func TestQueueBindingsEndpoint(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "api.qb.q", nil)
	declareExchange(t, env, "api.qb.ex", sdk.ExchangeDirect)

	sess, err := env.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	if err := sess.BindQueue("api.qb.q", "api.qb.ex", "qk", nil); err != nil {
		t.Fatalf("绑定队列失败: %v", err)
	}
	sess.Close()

	var binds []struct {
		Source          string `json:"source"`
		Destination     string `json:"destination"`
		DestinationType string `json:"destination_type"`
		RoutingKey      string `json:"routing_key"`
	}
	if resp := env.getJSON(t, "/api/queues/%2F/api.qb.q/bindings", &binds); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 队列绑定应 200，实际 %d", resp.StatusCode)
	}
	found := false
	for _, b := range binds {
		if b.Source == "api.qb.ex" && b.Destination == "api.qb.q" &&
			b.DestinationType == "queue" && b.RoutingKey == "qk" {
			found = true
		}
	}
	if !found {
		t.Fatalf("队列绑定列表缺少显式绑定（api.qb.ex -qk-> api.qb.q）: %+v", binds)
	}

	// 队列不存在 → 404（明确的对象不存在，而不是空列表）
	if resp := env.getJSON(t, "/api/queues/%2F/no.such.q/bindings", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的队列查绑定应 404，实际 %d", resp.StatusCode)
	}
}

// TestExchangeDestinationBindings 覆盖交换机维度的**反向**绑定视图：
// 只看"以本交换机为 destination"的绑定（交换机到交换机的绑定），与 source 视图互为镜像。
func TestExchangeDestinationBindings(t *testing.T) {
	env := newTestEnv(t)
	declareExchange(t, env, "api.src.ex", sdk.ExchangeFanout)
	declareExchange(t, env, "api.dst.ex", sdk.ExchangeFanout)

	sess, err := env.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	if err := sess.BindExchange("api.dst.ex", "api.src.ex", "ek", nil); err != nil {
		t.Fatalf("绑定交换机失败: %v", err)
	}
	sess.Close()

	var dst []struct {
		Source          string `json:"source"`
		Destination     string `json:"destination"`
		DestinationType string `json:"destination_type"`
	}
	if resp := env.getJSON(t, "/api/exchanges/%2F/api.dst.ex/bindings/destination", &dst); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 交换机反向绑定应 200，实际 %d", resp.StatusCode)
	}
	found := false
	for _, b := range dst {
		if b.Source == "api.src.ex" && b.Destination == "api.dst.ex" && b.DestinationType == "exchange" {
			found = true
		}
	}
	if !found {
		t.Fatalf("交换机反向绑定缺少 api.src.ex -> api.dst.ex: %+v", dst)
	}

	// 交换机不存在 → 404
	if resp := env.getJSON(t, "/api/exchanges/%2F/no.such.ex/bindings/destination", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的交换机查反向绑定应 404，实际 %d", resp.StatusCode)
	}
}

// TestBindingsAcrossVHosts 覆盖不带 vhost 的绑定汇总（GET /api/bindings）。
func TestBindingsAcrossVHosts(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "api.allbind.q", nil)
	declareExchange(t, env, "api.allbind.ex", sdk.ExchangeDirect)

	sess, err := env.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	if err := sess.BindQueue("api.allbind.q", "api.allbind.ex", "ak", nil); err != nil {
		t.Fatalf("绑定队列失败: %v", err)
	}
	sess.Close()

	type binding struct {
		Source      string `json:"source"`
		Destination string `json:"destination"`
	}
	assertContains := func(path string, list []binding) {
		t.Helper()
		for _, b := range list {
			if b.Source == "api.allbind.ex" && b.Destination == "api.allbind.q" {
				return
			}
		}
		t.Fatalf("GET %s 缺少 api.allbind.ex -> api.allbind.q: %+v", path, list)
	}

	// 不带 vhost：跨全部 vhost 汇总
	var all []binding
	if resp := env.getJSON(t, "/api/bindings", &all); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/bindings 应 200，实际 %d", resp.StatusCode)
	}
	assertContains("/api/bindings", all)

	// 带 vhost 的路径形式：仍是这台 vhost 的绑定
	var one []binding
	if resp := env.getJSON(t, "/api/bindings/%2F", &one); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/bindings/%%2F 应 200，实际 %d", resp.StatusCode)
	}
	assertContains("/api/bindings/%2F", one)
}

// TestConnectionAndChannelDetail 覆盖连接与通道的**单条读取**（列表已有用例覆盖）。
func TestConnectionAndChannelDetail(t *testing.T) {
	env := newTestEnv(t)

	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 51001}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := env.broker.NewSession(remote, local)
	t.Cleanup(core.Close)

	authResp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", authResp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	if _, err := core.Session("/"); err != nil {
		t.Fatalf("打开 vhost 失败: %v", err)
	}
	core.SetConnectionProbe(func() sdk.ConnectionInfo {
		return sdk.ConnectionInfo{
			Protocol: "AMQP 0-9-1", AuthMechanism: "PLAIN", FrameMax: 131072,
			Channels: []sdk.ChannelInfo{{Number: 1, ConsumerCount: 2, PrefetchCount: 20}},
		}
	})

	connName := "127.0.0.1:51001 -> 127.0.0.1:5672"
	var conn struct {
		Name     string `json:"name"`
		User     string `json:"user"`
		VHost    string `json:"vhost"`
		Protocol string `json:"protocol"`
		Channels int    `json:"channels"`
	}
	if resp := env.getJSON(t, "/api/connections/"+url.PathEscape(connName), &conn); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 单条连接应 200，实际 %d", resp.StatusCode)
	}
	if conn.Name != connName || conn.User != "guest" || conn.VHost != "/" ||
		conn.Protocol != "AMQP 0-9-1" || conn.Channels != 1 {
		t.Fatalf("单条连接字段错误: %+v", conn)
	}

	chName := fmt.Sprintf("%s (%d)", connName, 1)
	var ch struct {
		Name            string `json:"name"`
		Number          int    `json:"number"`
		ConsumerCount   int    `json:"consumer_count"`
		PrefetchCount   int    `json:"prefetch_count"`
		Unacked         int    `json:"unacked"`
		ConnectionShell struct {
			Name string `json:"name"`
		} `json:"connection_details"`
	}
	if resp := env.getJSON(t, "/api/channels/"+url.PathEscape(chName), &ch); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 单条通道应 200，实际 %d", resp.StatusCode)
	}
	if ch.Name != chName || ch.Number != 1 || ch.ConsumerCount != 2 || ch.PrefetchCount != 20 ||
		ch.ConnectionShell.Name != connName {
		t.Fatalf("单条通道字段错误: %+v", ch)
	}

	// 不存在的对象 → 404
	if resp := env.getJSON(t, "/api/connections/no.such.conn", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的连接应 404，实际 %d", resp.StatusCode)
	}
	if resp := env.getJSON(t, "/api/channels/no.such.channel", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的通道应 404，实际 %d", resp.StatusCode)
	}
}

// TestConsumersEndpoint 覆盖消费者视图：全量列表、vhost 维度、以及未知 vhost 的空列表契约。
func TestConsumersEndpoint(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "api.cons.q", nil)
	openConsumerSession(t, env, "api.cons.q", "api-ctag", 7)

	type consumer struct {
		ConsumerTag string `json:"consumer_tag"`
		Queue       struct {
			Name  string `json:"name"`
			VHost string `json:"vhost"`
		} `json:"queue"`
		AckRequired   bool `json:"ack_required"`
		PrefetchCount int  `json:"prefetch_count"`
		Exclusive     bool `json:"exclusive"`
	}

	var all []consumer
	if resp := env.getJSON(t, "/api/consumers", &all); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/consumers 应 200，实际 %d", resp.StatusCode)
	}
	var got *consumer
	for i := range all {
		if all[i].ConsumerTag == "api-ctag" {
			got = &all[i]
		}
	}
	if got == nil {
		t.Fatalf("消费者列表缺少 api-ctag: %+v", all)
	}
	if got.Queue.Name != "api.cons.q" || got.Queue.VHost != "/" || !got.AckRequired ||
		got.PrefetchCount != 7 || got.Exclusive {
		t.Fatalf("消费者字段错误: %+v", *got)
	}

	// vhost 维度（路径形式）
	var byVHost []consumer
	if resp := env.getJSON(t, "/api/consumers/%2F", &byVHost); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/consumers/%%2F 应 200，实际 %d", resp.StatusCode)
	}
	seen := false
	for _, c := range byVHost {
		if c.ConsumerTag == "api-ctag" {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("vhost 维度的消费者列表缺少 api-ctag: %+v", byVHost)
	}

	// 未知 vhost：回 200 + []（不是 404、也不是 null）
	resp, raw := env.request(t, http.MethodGet, "/api/consumers/no.such.vhost", nil, "", "")
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(raw)) != "[]" {
		t.Fatalf("未知 vhost 的消费者列表应回 200 + []，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestUserDetail 覆盖单条用户读取（含 tags 的拼接形式与 auth_backend 字段）。
func TestUserDetail(t *testing.T) {
	env := newTestEnv(t)

	resp, raw := env.request(t, http.MethodPut, "/api/users/u1",
		map[string]any{"password": "pw", "tags": "monitoring administrator"}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建用户应 201，实际 %d %s", resp.StatusCode, raw)
	}

	var u struct {
		Name        string `json:"name"`
		Tags        string `json:"tags"`
		AuthBackend string `json:"auth_backend"`
	}
	if resp := env.getJSON(t, "/api/users/u1", &u); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 单条用户应 200，实际 %d", resp.StatusCode)
	}
	if u.Name != "u1" || u.AuthBackend != "internal" {
		t.Fatalf("单条用户字段错误: %+v", u)
	}
	// tags 对齐 RabbitMQ 3.x 的"空格分隔"表示，且顺序稳定（已排序）
	if !strings.Contains(u.Tags, "administrator") || !strings.Contains(u.Tags, "monitoring") {
		t.Fatalf("用户 tags 应同时包含两种标签，实际 %q", u.Tags)
	}

	// 内置用户也存在
	if resp := env.getJSON(t, "/api/users/guest", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 内置用户 guest 应 200，实际 %d", resp.StatusCode)
	}
	// 不存在 → 404
	if resp := env.getJSON(t, "/api/users/no.such.user", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的用户应 404，实际 %d", resp.StatusCode)
	}
}

// TestPermissionsReadEndpoints 覆盖权限的单条读取与按 vhost 的列表读取。
func TestPermissionsReadEndpoints(t *testing.T) {
	env := newTestEnv(t)

	resp, raw := env.request(t, http.MethodPut, "/api/users/ops2",
		map[string]any{"password": "pw", "tags": "management"}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建用户应 201，实际 %d %s", resp.StatusCode, raw)
	}
	resp, raw = env.request(t, http.MethodPut, "/api/permissions/%2F/ops2",
		map[string]any{"configure": "^c", "write": "^w", "read": "^r"}, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("设置权限应 204，实际 %d %s", resp.StatusCode, raw)
	}

	var perm struct {
		User      string `json:"user"`
		VHost     string `json:"vhost"`
		Configure string `json:"configure"`
		Write     string `json:"write"`
		Read      string `json:"read"`
	}
	if resp := env.getJSON(t, "/api/permissions/%2F/ops2", &perm); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 单条权限应 200，实际 %d", resp.StatusCode)
	}
	if perm.User != "ops2" || perm.VHost != "/" || perm.Configure != "^c" || perm.Write != "^w" || perm.Read != "^r" {
		t.Fatalf("单条权限字段错误: %+v", perm)
	}

	var list []struct {
		User string `json:"user"`
	}
	if resp := env.getJSON(t, "/api/vhosts/%2F/permissions", &list); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/vhosts/%%2F/permissions 应 200，实际 %d", resp.StatusCode)
	}
	found := false
	for _, p := range list {
		if p.User == "ops2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("vhost 维度的权限列表缺少 ops2: %+v", list)
	}

	// 用户存在但没有任何权限记录 → 404（区分"用户不存在"与"权限记录不存在"）
	resp, raw = env.request(t, http.MethodPut, "/api/users/ops3",
		map[string]any{"password": "pw", "tags": "management"}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建用户应 201，实际 %d %s", resp.StatusCode, raw)
	}
	if resp := env.getJSON(t, "/api/permissions/%2F/ops3", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("没有权限记录的用户应 404，实际 %d", resp.StatusCode)
	}
	// 不存在的 vhost 的权限列表 → 404（这条显式校验 vhost 存在性）
	if resp := env.getJSON(t, "/api/vhosts/no.such.vhost/permissions", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的 vhost 的权限列表应 404，实际 %d", resp.StatusCode)
	}
}

// TestPluginDetail 覆盖单条插件读取。
func TestPluginDetail(t *testing.T) {
	env := newTestEnv(t)

	var info struct {
		Name         string   `json:"name"`
		Version      string   `json:"version"`
		State        string   `json:"state"`
		Builtin      bool     `json:"builtin"`
		Description  string   `json:"description"`
		Capabilities []string `json:"capabilities"`
	}
	if resp := env.getJSON(t, "/api/plugins/amqp091", &info); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 单条插件应 200，实际 %d", resp.StatusCode)
	}
	if info.Name != "amqp091" || info.Version != "0.1.0" || info.State != "enabled" ||
		!info.Builtin || info.Description != "测试插件" || len(info.Capabilities) != 1 {
		t.Fatalf("单条插件字段错误: %+v", info)
	}

	if resp := env.getJSON(t, "/api/plugins/no.such.plugin", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的插件应 404，实际 %d", resp.StatusCode)
	}
}

// TestMetricsAlias 覆盖 /api/metrics 与 /metrics 指向同一份 Prometheus 文本。
func TestMetricsAlias(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "api.metrics.q", nil)

	respA, rawA := env.request(t, http.MethodGet, "/metrics", nil, "", "")
	respB, rawB := env.request(t, http.MethodGet, "/api/metrics", nil, "", "")
	if respA.StatusCode != http.StatusOK || respB.StatusCode != http.StatusOK {
		t.Fatalf("指标端点应均 200，实际 /metrics=%d /api/metrics=%d", respA.StatusCode, respB.StatusCode)
	}
	if ctA, ctB := respA.Header.Get("Content-Type"), respB.Header.Get("Content-Type"); ctA != ctB {
		t.Fatalf("两个指标端点的 Content-Type 应一致：%q vs %q", ctA, ctB)
	}

	// 比较 HELP/TYPE 元数据行（逐行相同即同一份暴露；不比较计数器数值，避免抓取间隔造成抖动）
	headsA, headsB := prometheusHeads(string(rawA)), prometheusHeads(string(rawB))
	if len(headsA) == 0 {
		t.Fatalf("指标输出里没有 HELP/TYPE 行:\n%s", rawA)
	}
	if strings.Join(headsA, "\n") != strings.Join(headsB, "\n") {
		t.Fatalf("/api/metrics 与 /metrics 的指标元数据不一致:\n--- /metrics ---\n%s\n--- /api/metrics ---\n%s",
			strings.Join(headsA, "\n"), strings.Join(headsB, "\n"))
	}
}

// prometheusHeads 取 Prometheus 文本里的 `# HELP` / `# TYPE` 行。
func prometheusHeads(body string) []string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# ") {
			out = append(out, line)
		}
	}
	return out
}

// TestQuorumQueueEndpointsOnSingleNode 覆盖 grow / rebalance 在单机模式下的拒绝语义，
// 以及 grow 缺 count 的参数校验（参数错误优先于能力检查）。
func TestQuorumQueueEndpointsOnSingleNode(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "api.quorum.q", nil)

	// 缺 count → 400（请求体非法，与是否支持集群无关）
	resp, raw := env.request(t, http.MethodPut, "/api/queues/%2F/api.quorum.q/grow",
		map[string]any{}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("grow 缺 count 应 400，实际 %d %s", resp.StatusCode, raw)
	}

	// 单机模式：没有副本可扩 / 可迁 → 501 NOT_IMPLEMENTED
	resp, raw = env.request(t, http.MethodPut, "/api/queues/%2F/api.quorum.q/grow",
		map[string]any{"count": 2}, "", "")
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("单机模式 grow 应 501，实际 %d %s", resp.StatusCode, raw)
	}
	resp, raw = env.request(t, http.MethodPut, "/api/queues/%2F/api.quorum.q/rebalance", nil, "", "")
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("单机模式 rebalance 应 501，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestClusterMembersEndpointOnSingleNode 覆盖集群成员端点的单机语义：
// GET 必须回 200 + `{voters, learners}` 两个数组（集群页会无条件遍历），
// PUT 缺 addr 先被参数校验挡下（400），而真正的成员变更在单机模式一律 501。
func TestClusterMembersEndpointOnSingleNode(t *testing.T) {
	env := newTestEnv(t)

	var members struct {
		Voters   []string `json:"voters"`
		Learners []string `json:"learners"`
	}
	if resp := env.getJSON(t, "/api/cluster/members", &members); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/cluster/members 应 200，实际 %d", resp.StatusCode)
	}
	// 单机模式没有集群成员：两个数组都必须在（空数组而非 null），前端直接遍历
	if members.Voters == nil || members.Learners == nil {
		t.Fatalf("成员划分应回两个数组（可为空），实际 voters=%v learners=%v",
			members.Voters, members.Learners)
	}

	// 缺 addr → 400（参数校验先于集群能力检查）
	resp, raw := env.request(t, http.MethodPut, "/api/cluster/members/node2", map[string]any{}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("加入成员缺 addr 应 400，实际 %d %s", resp.StatusCode, raw)
	}

	// 单机模式：加入 / 移除成员均 501 NOT_IMPLEMENTED
	resp, raw = env.request(t, http.MethodPut, "/api/cluster/members/node2",
		map[string]any{"addr": "10.0.0.4:25672"}, "", "")
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("单机模式加入成员应 501，实际 %d %s", resp.StatusCode, raw)
	}
	resp, raw = env.request(t, http.MethodDelete, "/api/cluster/members/node2", nil, "", "")
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("单机模式移除成员应 501，实际 %d %s", resp.StatusCode, raw)
	}
}
