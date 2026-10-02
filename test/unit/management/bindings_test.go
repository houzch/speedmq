package management_test

import (
	"net/http"
	"testing"
)

// 本文件覆盖绑定的增删（管理面）。
//
// 机制（对照 RabbitMQ 实测）：管理 UI 在队列/交换机详情页建绑定，用的就是这四个端点：
//
//	POST   /api/bindings/{vhost}/e/{source}/q/{destination}   队列绑定
//	POST   /api/bindings/{vhost}/e/{source}/e/{destination}   交换机绑定
//	DELETE /api/bindings/{vhost}/e/{source}/q/{destination}/{props}
//	DELETE /api/bindings/{vhost}/e/{source}/e/{destination}/{props}
//
// {props} 是列表里读出来的 `properties_key`：无参数时就是 routing key，空 routing key 时为 `~`
// （RabbitMQ 实测如此；空串会让 URL 多出一个空段、匹配不上），带参数时再追加参数后缀。
// 状态码：建立一律 201（重复绑定幂等）；删除 204；找不到 404。

// declareForBinding 建好一对队列/交换机（都走管理面声明端点）。
func declareForBinding(t *testing.T, env *testEnv, exType, queue, exchange string) {
	t.Helper()
	if resp, raw := env.request(t, http.MethodPut, "/api/queues/%2F/"+queue,
		map[string]any{"durable": true, "auto_delete": false, "arguments": map[string]any{}}, "", ""); resp.StatusCode != http.StatusCreated {
		t.Fatalf("声明队列应 201，实际 %d %s", resp.StatusCode, raw)
	}
	if resp, raw := env.request(t, http.MethodPut, "/api/exchanges/%2F/"+exchange,
		map[string]any{"type": exType, "durable": true}, "", ""); resp.StatusCode != http.StatusCreated {
		t.Fatalf("声明交换机应 201，实际 %d %s", resp.StatusCode, raw)
	}
}

// bindingRow 是绑定列表里用例关心的一行。
type bindingRow struct {
	Source          string         `json:"source"`
	Destination     string         `json:"destination"`
	DestinationType string         `json:"destination_type"`
	RoutingKey      string         `json:"routing_key"`
	Arguments       map[string]any `json:"arguments"`
	PropertiesKey   string         `json:"properties_key"`
}

// findBinding 在队列的绑定列表里找指定 source 的那条。
func findBinding(t *testing.T, env *testEnv, queue, source string) *bindingRow {
	t.Helper()
	var rows []bindingRow
	if resp := env.getJSON(t, "/api/queues/%2F/"+queue+"/bindings", &rows); resp.StatusCode != http.StatusOK {
		t.Fatalf("读队列绑定应 200，实际 %d", resp.StatusCode)
	}
	for i := range rows {
		if rows[i].Source == source {
			return &rows[i]
		}
	}
	return nil
}

// TestQueueBindingCRUDOverManagementAPI 覆盖队列绑定的建立、列表与按 properties_key 删除。
func TestQueueBindingCRUDOverManagementAPI(t *testing.T) {
	env := newTestEnv(t)
	declareForBinding(t, env, "topic", "api.bind.q", "api.bind.ex")

	// 建立绑定 → 201
	resp, raw := env.request(t, http.MethodPost, "/api/bindings/%2F/e/api.bind.ex/q/api.bind.q",
		map[string]any{"routing_key": "rk1", "arguments": map[string]any{}}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("建立绑定应 201，实际 %d %s", resp.StatusCode, raw)
	}
	// 重复建立 → 仍 201（幂等）
	resp, raw = env.request(t, http.MethodPost, "/api/bindings/%2F/e/api.bind.ex/q/api.bind.q",
		map[string]any{"routing_key": "rk1", "arguments": map[string]any{}}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("重复建立绑定应 201（幂等），实际 %d %s", resp.StatusCode, raw)
	}

	got := findBinding(t, env, "api.bind.q", "api.bind.ex")
	if got == nil {
		t.Fatalf("队列绑定列表缺少刚建立的绑定")
	}
	if got.DestinationType != "queue" || got.RoutingKey != "rk1" || got.PropertiesKey != "rk1" {
		t.Fatalf("绑定字段不符: %+v", *got)
	}
	// 绑定真的生效：发布到该交换机 + routing key 应能落到队列
	if resp, raw := env.request(t, http.MethodPost, "/api/exchanges/%2F/api.bind.ex/publish",
		map[string]any{"routing_key": "rk1", "payload": "hi", "payload_encoding": "string"}, "", ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("发布应 200，实际 %d %s", resp.StatusCode, raw)
	}
	var q struct {
		MessagesReady int `json:"messages_ready"`
	}
	env.getJSON(t, "/api/queues/%2F/api.bind.q", &q)
	if q.MessagesReady != 1 {
		t.Fatalf("绑定应让消息路由到队列，实际 ready=%d", q.MessagesReady)
	}

	// 按 properties_key 删除 → 204；再删 → 404
	if resp, _ := env.request(t, http.MethodDelete, "/api/bindings/%2F/e/api.bind.ex/q/api.bind.q/rk1", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除绑定应 204，实际 %d", resp.StatusCode)
	}
	if resp, _ := env.request(t, http.MethodDelete, "/api/bindings/%2F/e/api.bind.ex/q/api.bind.q/rk1", nil, "", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("重复删除绑定应 404，实际 %d", resp.StatusCode)
	}
	if findBinding(t, env, "api.bind.q", "api.bind.ex") != nil {
		t.Fatalf("删除后绑定列表不应再有该绑定")
	}
}

// TestEmptyRoutingKeyBindingUsesTilde 覆盖**空 routing key**（fanout / headers 的常见形态）：
//
//	properties_key 必须是 `~` 而不是空串，否则删除 URL 会多出一个空段、根本匹配不上路由。
//	这条直接锁住 propertiesKey 的对齐口径。
func TestEmptyRoutingKeyBindingUsesTilde(t *testing.T) {
	env := newTestEnv(t)
	declareForBinding(t, env, "fanout", "api.bind.fq", "api.bind.fan")

	if resp, raw := env.request(t, http.MethodPost, "/api/bindings/%2F/e/api.bind.fan/q/api.bind.fq",
		map[string]any{"routing_key": "", "arguments": map[string]any{}}, "", ""); resp.StatusCode != http.StatusCreated {
		t.Fatalf("建立 fanout 绑定应 201，实际 %d %s", resp.StatusCode, raw)
	}
	got := findBinding(t, env, "api.bind.fq", "api.bind.fan")
	if got == nil {
		t.Fatalf("fanout 绑定未建立")
	}
	if got.PropertiesKey != "~" {
		t.Fatalf("空 routing key 的 properties_key 应为 ~（对齐 RabbitMQ），实际 %q", got.PropertiesKey)
	}
	// 用 `~` 删除必须成功
	if resp, raw := env.request(t, http.MethodDelete, "/api/bindings/%2F/e/api.bind.fan/q/api.bind.fq/~", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("用 ~ 删除绑定应 204，实际 %d %s", resp.StatusCode, raw)
	}
	if findBinding(t, env, "api.bind.fq", "api.bind.fan") != nil {
		t.Fatalf("删除后不应还有该绑定")
	}
}

// TestExchangeToExchangeBinding 覆盖交换机到交换机的绑定与解绑。
func TestExchangeToExchangeBinding(t *testing.T) {
	env := newTestEnv(t)
	for _, name := range []string{"api.bind.src", "api.bind.dst"} {
		if resp, raw := env.request(t, http.MethodPut, "/api/exchanges/%2F/"+name,
			map[string]any{"type": "fanout", "durable": true}, "", ""); resp.StatusCode != http.StatusCreated {
			t.Fatalf("声明交换机 %s 应 201，实际 %d %s", name, resp.StatusCode, raw)
		}
	}
	resp, raw := env.request(t, http.MethodPost, "/api/bindings/%2F/e/api.bind.src/e/api.bind.dst",
		map[string]any{"routing_key": "ek", "arguments": map[string]any{}}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("建立交换机绑定应 201，实际 %d %s", resp.StatusCode, raw)
	}

	var rows []bindingRow
	if resp := env.getJSON(t, "/api/exchanges/%2F/api.bind.dst/bindings/destination", &rows); resp.StatusCode != http.StatusOK {
		t.Fatalf("读交换机反向绑定应 200，实际 %d", resp.StatusCode)
	}
	found := false
	for _, b := range rows {
		if b.Source == "api.bind.src" && b.DestinationType == "exchange" && b.PropertiesKey == "ek" {
			found = true
		}
	}
	if !found {
		t.Fatalf("交换机绑定列表缺少 api.bind.src -> api.bind.dst: %+v", rows)
	}

	if resp, raw := env.request(t, http.MethodDelete, "/api/bindings/%2F/e/api.bind.src/e/api.bind.dst/ek", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除交换机绑定应 204，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestBindingNotFoundAndPermissions 覆盖两条拒绝路径：
// 目标对象不存在 → 404；调用方在该 vhost 没有 configure 权限 → 403。
func TestBindingNotFoundAndPermissions(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")
	declareForBinding(t, env, "direct", "api.bind.pq", "api.bind.pex")

	// 目标队列不存在 → 404
	if resp, raw := env.request(t, http.MethodPost, "/api/bindings/%2F/e/api.bind.pex/q/api.bind.nope",
		map[string]any{"routing_key": "k", "arguments": map[string]any{}}, "", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("绑定到不存在的队列应 404，实际 %d %s", resp.StatusCode, raw)
	}
	// 源交换机不存在 → 404
	if resp, raw := env.request(t, http.MethodPost, "/api/bindings/%2F/e/api.bind.noex/q/api.bind.pq",
		map[string]any{"routing_key": "k", "arguments": map[string]any{}}, "", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("不存在的源交换机应 404，实际 %d %s", resp.StatusCode, raw)
	}

	// 无 write 权限 → 403（绑定走的是 write 维度：RabbitMQ 与内核都把 queue.bind 算作
	// 对交换机的"写"操作，见 vhostSession.BindQueue 的 allowWrite）
	if resp, raw := env.request(t, http.MethodPut, "/api/users/bindlimited",
		map[string]any{"password": "pw", "tags": "management"}, "", ""); resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建账号应 201，实际 %d %s", resp.StatusCode, raw)
	}
	if resp, raw := env.request(t, http.MethodPut, "/api/permissions/%2F/bindlimited",
		map[string]any{"configure": ".*", "write": "", "read": ".*"}, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("设置权限应 204，实际 %d %s", resp.StatusCode, raw)
	}
	if resp, raw := env.request(t, http.MethodPost, "/api/bindings/%2F/e/api.bind.pex/q/api.bind.pq",
		map[string]any{"routing_key": "k", "arguments": map[string]any{}}, "bindlimited", "pw"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("无 write 权限建绑定应 403，实际 %d %s", resp.StatusCode, raw)
	}
}
