// Package management_test 从包外驱动管理 HTTP API：只使用 internal/management 的导出 API，
// 断言落在"客户端能看到什么"上（状态码、响应体、指标文本、端口监听）。
//
// 文件位置与包形式的约定见 AGENTS.md §10.1。
package management_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/management"
	"github.com/houzch/swiftmq/internal/transport"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖管理 HTTP API 的对外契约：认证、vhost 路径编码（%2F）、
// 队列/交换机/连接的读写、用户与权限、插件治理、Prometheus 指标。
//
// 这里的断言刻意贴近"客户端能看到什么"——管理 API 的价值就在于被外部工具消费。
//
// 服务经公开装配路径启动：management.New + Start + Addr，从而顺带覆盖真实监听与关停路径。

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakePlugins 是可编程的插件控制器替身（真身是 internal/plugin.Manager）。
// 它结构化满足 management.PluginController，无需导出任何内部符号。
type fakePlugins struct {
	infos map[string]sdk.Info
}

func newFakePlugins() *fakePlugins {
	return &fakePlugins{infos: map[string]sdk.Info{
		"amqp091": {
			Name: "amqp091", Version: "0.1.0", APIVersion: "v1",
			State: sdk.StateEnabled, Builtin: true,
			Capabilities: []string{"net.listen"}, Description: "测试插件",
		},
	}}
}

func (f *fakePlugins) Plugins() []sdk.Info {
	out := make([]sdk.Info, 0, len(f.infos))
	for _, v := range f.infos {
		out = append(out, v)
	}
	return out
}

func (f *fakePlugins) Plugin(name string) (sdk.Info, bool) {
	info, ok := f.infos[name]
	return info, ok
}

func (f *fakePlugins) Enable(name string) error {
	info, ok := f.infos[name]
	if !ok {
		return io.EOF
	}
	info.State = sdk.StateEnabled
	f.infos[name] = info
	return nil
}

func (f *fakePlugins) Disable(name string) error {
	info, ok := f.infos[name]
	if !ok {
		return io.EOF
	}
	info.State = sdk.StateDisabled
	f.infos[name] = info
	return nil
}

type testEnv struct {
	baseURL string
	broker  *broker.Broker
	plugins *fakePlugins
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	b := broker.New(discardLogger(), cfg)
	t.Cleanup(b.Close)

	plugins := newFakePlugins()
	// 监听地址用 127.0.0.1:0 让内核分配空闲端口，避免与开发机上的 15672 冲突。
	srv, err := management.New(discardLogger(), management.Deps{
		Addr:    "127.0.0.1:0",
		Broker:  b,
		Plugins: plugins,
		Listeners: func() []transport.ListenerSnapshot {
			return []transport.ListenerSnapshot{{Protocol: "amqp091", Listener: "amqp", Addr: "[::]:5672"}}
		},
		Version:   "0.5.0-test",
		NodeName:  "swiftmq@test",
		StartedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("构造管理面失败: %v", err)
	}
	if err := srv.Start(); err != nil {
		t.Fatalf("启动管理面失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return &testEnv{baseURL: "http://" + srv.Addr(), broker: b, plugins: plugins}
}

// request 发起一次管理 API 请求；user/pass 为空时带默认管理员凭证。
func (e *testEnv) request(t *testing.T, method, path string, body any, user, pass string) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("序列化请求体失败: %v", err)
		}
		reader = strings.NewReader(string(raw))
	}
	req, err := http.NewRequest(method, e.baseURL+path, reader)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if user == "" && pass == "" {
		user, pass = "guest", "guest"
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("请求 %s %s 失败: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取响应失败: %v", err)
	}
	return resp, raw
}

// getJSON 发起 GET 并解析 JSON。
func (e *testEnv) getJSON(t *testing.T, path string, out any) *http.Response {
	t.Helper()
	resp, raw := e.request(t, http.MethodGet, path, nil, "", "")
	if resp.StatusCode == http.StatusOK && out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("解析 %s 响应失败: %v（原始内容 %s）", path, err, raw)
		}
	}
	return resp
}

// declareQueue 直接用内核会话建队列（管理面本身不提供声明接口）。
func (e *testEnv) declareQueue(t *testing.T, name string, args map[string]any) {
	t.Helper()
	sess, err := e.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()
	if _, err := sess.DeclareQueue(sdk.QueueDeclare{Name: name, Arguments: args}); err != nil {
		t.Fatalf("声明队列失败: %v", err)
	}
}

func (e *testEnv) publish(t *testing.T, queue, body string) {
	t.Helper()
	sess, err := e.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()
	// 与客户端一致：默认交换机的路由键就是队列名，消息上也要带上它
	// （Basic.Deliver 会把 routing-key 原样回给消费者）。
	msg := &sdk.Message{RoutingKey: queue, Body: []byte(body)}
	if _, err := sess.Publish(msg, "", queue, false); err != nil {
		t.Fatalf("发布失败: %v", err)
	}
}

// TestAuthRequired 覆盖认证：缺凭证/错凭证必须 401（前端依赖 401 重新弹登录框）。
func TestAuthRequired(t *testing.T) {
	env := newTestEnv(t)

	resp, _ := env.request(t, http.MethodGet, "/api/overview", nil, "-", "-")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无凭证访问应 401，实际 %d", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); !strings.Contains(got, "Basic") {
		t.Fatalf("401 应带 WWW-Authenticate: Basic，实际 %q", got)
	}

	resp, raw := env.request(t, http.MethodGet, "/api/overview", nil, "guest", "wrong")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错误口令应 401，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil || errBody["error"] == "" {
		t.Fatalf("401 响应体应为 {error,reason}，实际 %s", raw)
	}

	resp, _ = env.request(t, http.MethodGet, "/api/overview", nil, "guest", "guest")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("正确凭证应 200，实际 %d", resp.StatusCode)
	}
}

// TestOverviewAndWhoami 覆盖概览字段与 whoami。
func TestOverviewAndWhoami(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "m5.overview", nil)
	env.publish(t, "m5.overview", "x")

	var overview struct {
		ProductName  string `json:"product_name"`
		Node         string `json:"node"`
		ObjectTotals struct {
			Queues    int `json:"queues"`
			Exchanges int `json:"exchanges"`
		} `json:"object_totals"`
		QueueTotals struct {
			Messages int `json:"messages"`
			Ready    int `json:"messages_ready"`
		} `json:"queue_totals"`
		Listeners []struct {
			Protocol string `json:"protocol"`
			Port     int    `json:"port"`
		} `json:"listeners"`
	}
	resp := env.getJSON(t, "/api/overview", &overview)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/overview 失败: %d", resp.StatusCode)
	}
	if overview.ProductName != "SwiftMQ" || overview.Node != "swiftmq@test" {
		t.Fatalf("概览基本字段错误: %+v", overview)
	}
	if overview.ObjectTotals.Queues != 1 || overview.ObjectTotals.Exchanges == 0 {
		t.Fatalf("对象总数错误: %+v", overview.ObjectTotals)
	}
	if overview.QueueTotals.Messages != 1 || overview.QueueTotals.Ready != 1 {
		t.Fatalf("队列消息总数错误: %+v", overview.QueueTotals)
	}
	if len(overview.Listeners) != 1 || overview.Listeners[0].Protocol != "amqp" || overview.Listeners[0].Port != 5672 {
		t.Fatalf("listeners 字段错误: %+v", overview.Listeners)
	}

	var whoami struct {
		Name string `json:"name"`
		Tags string `json:"tags"`
	}
	if resp := env.getJSON(t, "/api/whoami", &whoami); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/whoami 失败: %d", resp.StatusCode)
	}
	if whoami.Name != "guest" || !strings.Contains(whoami.Tags, "administrator") {
		t.Fatalf("whoami 字段错误: %+v", whoami)
	}
}

// TestQueuePathEncoding 覆盖 %2F 路径解码：默认 vhost 是 "/"，
// 若路由按已解码的 URL.Path 匹配，段数会错位，所有队列接口都会 404。
func TestQueuePathEncoding(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "m5.encode", nil)
	env.publish(t, "m5.encode", "hello")

	var q struct {
		Name      string `json:"name"`
		VHost     string `json:"vhost"`
		Messages  int    `json:"messages"`
		Ready     int    `json:"messages_ready"`
		Consumers int    `json:"consumers"`
		Type      string `json:"type"`
	}
	resp := env.getJSON(t, "/api/queues/%2F/m5.encode", &q)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("按 %%2F 访问队列应 200，实际 %d", resp.StatusCode)
	}
	if q.Name != "m5.encode" || q.VHost != "/" || q.Messages != 1 || q.Ready != 1 || q.Type != "classic" {
		t.Fatalf("队列字段错误: %+v", q)
	}

	// 列表：必须是普通数组（前端与 rabbitmqadmin 都会直接遍历）
	resp, raw := env.request(t, http.MethodGet, "/api/queues?vhost=%2F&use_regex=false&name=m5", nil, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/queues 失败: %d", resp.StatusCode)
	}
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "[") {
		t.Fatalf("队列列表必须是 JSON 数组，实际 %s", raw)
	}
	var list []map[string]any
	if err := json.Unmarshal(raw, &list); err != nil || len(list) != 1 {
		t.Fatalf("队列列表解析失败或条数不符: err=%v list=%v", err, list)
	}

	// 过滤器：名字不匹配时应为空数组而不是 null
	resp, raw = env.request(t, http.MethodGet, "/api/queues?name=zzz-no-match", nil, "", "")
	if resp.StatusCode != http.StatusOK || strings.TrimSpace(string(raw)) != "[]" {
		t.Fatalf("无匹配时应返回 []，实际 %d %s", resp.StatusCode, raw)
	}

	// 不存在的队列：404 + {error,reason}
	resp, raw = env.request(t, http.MethodGet, "/api/queues/%2F/no-such-queue", nil, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的队列应 404，实际 %d", resp.StatusCode)
	}
	var errBody map[string]string
	if err := json.Unmarshal(raw, &errBody); err != nil || errBody["reason"] == "" {
		t.Fatalf("404 响应体应为 {error,reason}，实际 %s", raw)
	}

	// 方法不允许：PUT 到队列详情
	resp, _ = env.request(t, http.MethodPut, "/api/queues/%2F/m5.encode", map[string]any{}, "", "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("不支持的方法应 405，实际 %d", resp.StatusCode)
	}
}

// TestGetMessagesAndPurge 覆盖"取消息"的 ackmode 语义与清空/删除。
func TestGetMessagesAndPurge(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "m5.get", nil)
	for _, body := range []string{"a", "b", "c"} {
		env.publish(t, "m5.get", body)
	}

	// ack_requeue_true：取回来但不消费，队列长度不变
	var msgs []struct {
		Payload         string `json:"payload"`
		PayloadEncoding string `json:"payload_encoding"`
		PayloadBytes    int    `json:"payload_bytes"`
		Redelivered     bool   `json:"redelivered"`
		RoutingKey      string `json:"routing_key"`
		MessageCount    int    `json:"message_count"`
		Properties      struct {
			DeliveryMode int `json:"delivery_mode"`
		} `json:"properties"`
	}
	resp, raw := env.request(t, http.MethodPost, "/api/queues/%2F/m5.get/get",
		map[string]any{"count": 2, "ackmode": "ack_requeue_true", "encoding": "auto"}, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("取消息失败: %d %s", resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &msgs); err != nil {
		t.Fatalf("解析取消息响应失败: %v（%s）", err, raw)
	}
	if len(msgs) != 2 || msgs[0].Payload != "a" || msgs[1].Payload != "b" {
		t.Fatalf("取消息内容或顺序错误: %+v", msgs)
	}
	if msgs[0].PayloadEncoding != "string" || msgs[0].PayloadBytes != 1 || msgs[0].RoutingKey != "m5.get" {
		t.Fatalf("消息字段错误: %+v", msgs[0])
	}

	// 队列长度不变（requeue）
	var q struct {
		Messages int `json:"messages"`
	}
	env.getJSON(t, "/api/queues/%2F/m5.get", &q)
	if q.Messages != 3 {
		t.Fatalf("ack_requeue_true 后队列应仍有 3 条，实际 %d", q.Messages)
	}

	// ack_requeue_false：真取走
	resp, raw = env.request(t, http.MethodPost, "/api/queues/%2F/m5.get/get",
		map[string]any{"count": 1, "ackmode": "ack_requeue_false"}, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("取消息失败: %d %s", resp.StatusCode, raw)
	}
	env.getJSON(t, "/api/queues/%2F/m5.get", &q)
	if q.Messages != 2 {
		t.Fatalf("ack_requeue_false 后应剩 2 条，实际 %d", q.Messages)
	}

	// 非法 ackmode → 400
	resp, _ = env.request(t, http.MethodPost, "/api/queues/%2F/m5.get/get",
		map[string]any{"ackmode": "bogus"}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("非法 ackmode 应 400，实际 %d", resp.StatusCode)
	}

	// purge → 204，队列清空但队列仍在
	resp, _ = env.request(t, http.MethodDelete, "/api/queues/%2F/m5.get/contents", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("purge 应 204，实际 %d", resp.StatusCode)
	}
	env.getJSON(t, "/api/queues/%2F/m5.get", &q)
	if q.Messages != 0 {
		t.Fatalf("purge 后队列应为空，实际 %d", q.Messages)
	}

	// delete → 204，再查 404
	resp, _ = env.request(t, http.MethodDelete, "/api/queues/%2F/m5.get", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除队列应 204，实际 %d", resp.StatusCode)
	}
	resp, _ = env.request(t, http.MethodGet, "/api/queues/%2F/m5.get", nil, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("已删除队列应 404，实际 %d", resp.StatusCode)
	}
}

// TestPublishToDefaultExchange 覆盖 UI 的"发测试消息"路径（amq.default 是 RabbitMQ 约定）。
func TestPublishToDefaultExchange(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "m5.publish", nil)

	var result struct {
		Routed bool `json:"routed"`
	}
	resp, raw := env.request(t, http.MethodPost, "/api/exchanges/%2F/amq.default/publish", map[string]any{
		"properties":       map[string]any{"content_type": "text/plain", "delivery_mode": 2},
		"routing_key":      "m5.publish",
		"payload":          "from-management",
		"payload_encoding": "string",
	}, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("发布失败: %d %s", resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &result); err != nil || !result.Routed {
		t.Fatalf("发布结果应为 routed=true，实际 %s", raw)
	}

	var q struct {
		Messages int `json:"messages"`
	}
	env.getJSON(t, "/api/queues/%2F/m5.publish", &q)
	if q.Messages != 1 {
		t.Fatalf("管理面发布的消息未进入队列，实际 %d 条", q.Messages)
	}

	// 未命中任何队列 → routed=false（不是错误）
	resp, raw = env.request(t, http.MethodPost, "/api/exchanges/%2F/amq.default/publish", map[string]any{
		"routing_key": "no.such.queue", "payload": "x", "payload_encoding": "string",
	}, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("发布到不存在的队列不应报错，实际 %d", resp.StatusCode)
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Routed {
		t.Fatalf("未命中的发布应为 routed=false，实际 %s", raw)
	}

	// 不存在的交换机 → 404
	resp, _ = env.request(t, http.MethodPost, "/api/exchanges/%2F/no.such.exchange/publish", map[string]any{
		"routing_key": "k", "payload": "x", "payload_encoding": "string",
	}, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("发布到不存在的交换机应 404，实际 %d", resp.StatusCode)
	}
}

// TestExchangesAndBindings 覆盖交换机列表、详情与绑定（含默认交换机的隐式绑定）。
func TestExchangesAndBindings(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "m5.bind", nil)

	// 内核侧建一条显式绑定
	sess, err := env.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	if err := sess.DeclareExchange(sdk.ExchangeDeclare{Name: "m5.ex", Type: sdk.ExchangeDirect}); err != nil {
		t.Fatalf("声明交换机失败: %v", err)
	}
	if err := sess.BindQueue("m5.bind", "m5.ex", "rk1", nil); err != nil {
		t.Fatalf("绑定失败: %v", err)
	}
	sess.Close()

	var exs []struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Durable  bool   `json:"durable"`
		Internal bool   `json:"internal"`
	}
	if resp := env.getJSON(t, "/api/exchanges?vhost=%2F", &exs); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/exchanges 失败: %d", resp.StatusCode)
	}
	found := false
	for _, e := range exs {
		if e.Name == "m5.ex" && e.Type == "direct" {
			found = true
		}
	}
	if !found {
		t.Fatalf("交换机列表缺少 m5.ex: %+v", exs)
	}

	var one struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if resp := env.getJSON(t, "/api/exchanges/%2F/m5.ex", &one); resp.StatusCode != http.StatusOK || one.Type != "direct" {
		t.Fatalf("交换机详情错误: %d %+v", resp.StatusCode, one)
	}

	// 默认交换机在 API 里叫 amq.default
	var def struct {
		Name     string `json:"name"`
		Internal bool   `json:"internal"`
	}
	if resp := env.getJSON(t, "/api/exchanges/%2F/amq.default", &def); resp.StatusCode != http.StatusOK || def.Name != "" {
		t.Fatalf("amq.default 详情错误: %d %+v", resp.StatusCode, def)
	}

	var source []struct {
		Source          string `json:"source"`
		Destination     string `json:"destination"`
		DestinationType string `json:"destination_type"`
		RoutingKey      string `json:"routing_key"`
		PropertiesKey   string `json:"properties_key"`
	}
	if resp := env.getJSON(t, "/api/exchanges/%2F/m5.ex/bindings/source", &source); resp.StatusCode != http.StatusOK {
		t.Fatalf("交换机绑定查询失败: %d", resp.StatusCode)
	}
	if len(source) != 1 || source[0].Destination != "m5.bind" || source[0].RoutingKey != "rk1" ||
		source[0].DestinationType != "queue" || source[0].PropertiesKey != "rk1" {
		t.Fatalf("交换机绑定内容错误: %+v", source)
	}

	var all []struct {
		Source string `json:"source"`
	}
	if resp := env.getJSON(t, "/api/bindings/%2F", &all); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/bindings 失败: %d", resp.StatusCode)
	}
	implicit := false
	for _, bd := range all {
		if bd.Source == "" {
			implicit = true
		}
	}
	if !implicit {
		t.Fatalf("绑定列表应包含默认交换机的隐式绑定: %+v", all)
	}
}

// TestConnectionsAndClose 覆盖连接/通道视图与强制关闭。
func TestConnectionsAndClose(t *testing.T) {
	env := newTestEnv(t)

	remote := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 51000}
	local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5672}
	core := env.broker.NewSession(remote, local)
	t.Cleanup(core.Close)

	resp := append([]byte("\x00guest\x00"), []byte("guest")...)
	if _, err := core.Authenticate(context.Background(), "PLAIN", resp, remote); err != nil {
		t.Fatalf("认证失败: %v", err)
	}
	if _, err := core.Session("/"); err != nil {
		t.Fatalf("打开 vhost 失败: %v", err)
	}
	core.SetConnectionProbe(func() sdk.ConnectionInfo {
		return sdk.ConnectionInfo{
			Protocol: "AMQP 0-9-1", AuthMechanism: "PLAIN", FrameMax: 131072,
			Channels: []sdk.ChannelInfo{{Number: 1, ConsumerCount: 1, PrefetchCount: 10}},
		}
	})
	disconnected := make(chan string, 1)
	core.SetDisconnectFunc(func(reason string) { disconnected <- reason })

	var conns []struct {
		Name          string `json:"name"`
		User          string `json:"user"`
		VHost         string `json:"vhost"`
		Protocol      string `json:"protocol"`
		Channels      int    `json:"channels"`
		ConnectedAt   int64  `json:"connected_at"`
		PeerHost      string `json:"peer_host"`
		PeerPort      int    `json:"peer_port"`
		AuthMechanism string `json:"auth_mechanism"`
	}
	if resp := env.getJSON(t, "/api/connections", &conns); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/connections 失败: %d", resp.StatusCode)
	}
	if len(conns) != 1 {
		t.Fatalf("连接数应为 1，实际 %d", len(conns))
	}
	c := conns[0]
	if c.Name != "127.0.0.1:51000 -> 127.0.0.1:5672" || c.User != "guest" || c.VHost != "/" ||
		c.Protocol != "AMQP 0-9-1" || c.Channels != 1 || c.PeerHost != "127.0.0.1" || c.PeerPort != 51000 {
		t.Fatalf("连接字段错误: %+v", c)
	}
	if c.ConnectedAt == 0 || c.AuthMechanism != "PLAIN" {
		t.Fatalf("连接时间/机制字段错误: %+v", c)
	}

	var channels []struct {
		Name          string `json:"name"`
		Number        int    `json:"number"`
		User          string `json:"user"`
		ConsumerCount int    `json:"consumer_count"`
		PrefetchCount int    `json:"prefetch_count"`
		Connection    struct {
			Name string `json:"name"`
		} `json:"connection_details"`
	}
	if resp := env.getJSON(t, "/api/channels", &channels); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/channels 失败: %d", resp.StatusCode)
	}
	if len(channels) != 1 || channels[0].Number != 1 || channels[0].ConsumerCount != 1 ||
		channels[0].PrefetchCount != 10 || channels[0].User != "guest" {
		t.Fatalf("通道字段错误: %+v", channels)
	}

	// 强制关闭：返回 204，并触发内核的下发断开回调
	resp204, raw := env.request(t, http.MethodDelete, "/api/connections/"+url.PathEscape(c.Name), nil, "", "")
	if resp204.StatusCode != http.StatusNoContent {
		t.Fatalf("强制关闭应 204，实际 %d %s", resp204.StatusCode, raw)
	}
	select {
	case reason := <-disconnected:
		if !strings.Contains(reason, "management") {
			t.Fatalf("断开原因应说明来自管理面，实际 %q", reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("强制关闭未触发断开回调")
	}
}

// TestUsersAndPermissions 覆盖用户与权限的动态管理。
func TestUsersAndPermissions(t *testing.T) {
	env := newTestEnv(t)

	// 新建用户 → 201
	resp, raw := env.request(t, http.MethodPut, "/api/users/ops", map[string]any{
		"password": "s3cret", "tags": "management",
	}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建用户应 201，实际 %d %s", resp.StatusCode, raw)
	}

	// 更新已存在用户 → 204
	resp, _ = env.request(t, http.MethodPut, "/api/users/ops", map[string]any{
		"password": "s3cret2", "tags": []any{"administrator"},
	}, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("更新用户应 204，实际 %d", resp.StatusCode)
	}

	var users []struct {
		Name string `json:"name"`
		Tags string `json:"tags"`
	}
	if resp := env.getJSON(t, "/api/users", &users); resp.StatusCode != http.StatusOK || len(users) != 2 {
		t.Fatalf("用户列表错误: %d %+v", resp.StatusCode, users)
	}

	// 设置权限
	resp, raw = env.request(t, http.MethodPut, "/api/permissions/%2F/ops", map[string]any{
		"configure": "^app\\.", "write": "^app\\.", "read": ".*",
	}, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("设置权限应 204，实际 %d %s", resp.StatusCode, raw)
	}

	// 非法正则必须 400（而不是"配了却永远拒绝"）
	resp, _ = env.request(t, http.MethodPut, "/api/permissions/%2F/ops", map[string]any{
		"configure": "([", "write": ".*", "read": ".*",
	}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("非法权限正则应 400，实际 %d", resp.StatusCode)
	}

	var perms []struct {
		User      string `json:"user"`
		VHost     string `json:"vhost"`
		Configure string `json:"configure"`
	}
	if resp := env.getJSON(t, "/api/permissions", &perms); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/permissions 失败: %d", resp.StatusCode)
	}
	foundOps := false
	for _, p := range perms {
		if p.User == "ops" && p.VHost == "/" && p.Configure == "^app\\." {
			foundOps = true
		}
	}
	if !foundOps {
		t.Fatalf("权限列表缺少 ops 的记录: %+v", perms)
	}

	// 删除权限 → 204；再删 → 404
	resp, _ = env.request(t, http.MethodDelete, "/api/permissions/%2F/ops", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除权限应 204，实际 %d", resp.StatusCode)
	}
	resp, _ = env.request(t, http.MethodDelete, "/api/permissions/%2F/ops", nil, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("重复删除权限应 404，实际 %d", resp.StatusCode)
	}

	// 删除用户 → 204
	resp, _ = env.request(t, http.MethodDelete, "/api/users/ops", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除用户应 204，实际 %d", resp.StatusCode)
	}
}

// TestPluginGovernance 覆盖插件列表与热启用/停用。
func TestPluginGovernance(t *testing.T) {
	env := newTestEnv(t)

	var plugins []struct {
		Name         string   `json:"name"`
		State        string   `json:"state"`
		API          string   `json:"api_version"`
		Builtin      bool     `json:"builtin"`
		Capabilities []string `json:"capabilities"`
	}
	if resp := env.getJSON(t, "/api/plugins", &plugins); resp.StatusCode != http.StatusOK || len(plugins) != 1 {
		t.Fatalf("插件列表错误: %d %+v", resp.StatusCode, plugins)
	}
	if plugins[0].Name != "amqp091" || plugins[0].State != "enabled" || !plugins[0].Builtin {
		t.Fatalf("插件字段错误: %+v", plugins[0])
	}

	var result struct {
		Name  string `json:"name"`
		State string `json:"state"`
	}
	resp, raw := env.request(t, http.MethodPut, "/api/plugins/amqp091/disable", nil, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("停用插件应 200，实际 %d %s", resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.State != "disabled" {
		t.Fatalf("停用后状态应为 disabled，实际 %s", raw)
	}

	resp, raw = env.request(t, http.MethodPut, "/api/plugins/amqp091/enable", nil, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("启用插件应 200，实际 %d %s", resp.StatusCode, raw)
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.State != "enabled" {
		t.Fatalf("启用后状态应为 enabled，实际 %s", raw)
	}

	// 不存在的插件 → 404
	resp, _ = env.request(t, http.MethodPut, "/api/plugins/nope/enable", nil, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的插件应 404，实际 %d", resp.StatusCode)
	}
}

// TestMetrics 覆盖 Prometheus 文本暴露格式。
func TestMetrics(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "m5.metrics", nil)
	env.publish(t, "m5.metrics", "x")

	resp, raw := env.request(t, http.MethodGet, "/metrics", nil, "", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics 失败: %d", resp.StatusCode)
	}
	body := string(raw)
	for _, want := range []string{
		"swiftmq_up 1",
		"# TYPE swiftmq_connections gauge",
		"swiftmq_queue_messages_ready{vhost=\"/\",queue=\"m5.metrics\"} 1",
		"swiftmq_queue_messages_published_total{vhost=\"/\",queue=\"m5.metrics\"} 1",
		"swiftmq_plugin_info{name=\"amqp091\"",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("指标缺少 %q；实际输出:\n%s", want, body)
		}
	}
}

// TestStaticUI 覆盖内嵌 UI：无 UI 资源时 / 返回 404 且提示明确，
// /api 仍然可用（管理 API 与 UI 解耦，UI 缺失不该影响接口）。
func TestStaticUI(t *testing.T) {
	env := newTestEnv(t)
	resp, raw := env.request(t, http.MethodGet, "/", nil, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("未嵌入 UI 时访问 / 应 404，实际 %d", resp.StatusCode)
	}
	if !strings.Contains(string(raw), "管理 UI") {
		t.Fatalf("404 提示应说明 UI 未构建，实际 %s", raw)
	}
}

// TestPermissionDenied 覆盖标签不足时的 403（读接口也不放行）。
func TestPermissionDenied(t *testing.T) {
	env := newTestEnv(t)
	sess, err := env.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()
	// 建一个无标签用户
	if err := env.broker.UpsertUser("plain", "pw", nil); err != nil {
		t.Fatalf("建用户失败: %v", err)
	}

	resp, raw := env.request(t, http.MethodGet, "/api/overview", nil, "plain", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("无管理标签的用户应 403，实际 %d %s", resp.StatusCode, raw)
	}
}
