package management_test

import (
	"net/http"
	"testing"
)

// 本文件覆盖"从管理面声明 / 删除队列与交换机"。
//
// 机制说明（对照 RabbitMQ 实测）：RabbitMQ 管理 UI 的「Add a new queue」「Add a new exchange」
// 不是 UI 自己实现的功能，而是两个**声明端点**：
//
//	PUT    /api/queues/{vhost}/{name}      声明队列    {durable, auto_delete, arguments}
//	PUT    /api/exchanges/{vhost}/{name}   声明交换机  {type, durable, auto_delete, internal, arguments}
//	DELETE /api/exchanges/{vhost}/{name}   删除交换机
//
// 状态码：新建 201、已存在且参数等价 204、参数不等价或类型非法 400。
// SwiftMQ 的实现刻意**复用内核的声明逻辑**（经 SessionFor(调用方) 拿会话），
// 因此权限（configure）、保留名、等价性检查与 AMQP 完全同一套 —— 下面既断言状态码，
// 也断言"管理面建的队列真的能被读到"和"以调用方身份受权限约束"。

// TestDeclareQueueOverManagementAPI 覆盖队列声明的状态码与落库效果。
func TestDeclareQueueOverManagementAPI(t *testing.T) {
	env := newTestEnv(t)

	body := map[string]any{"durable": true, "auto_delete": false, "arguments": map[string]any{}}
	// 新建 → 201
	resp, raw := env.request(t, http.MethodPut, "/api/queues/%2F/api.decl.q", body, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("声明新队列应 201，实际 %d %s", resp.StatusCode, raw)
	}
	// 同参数再声明 → 204（已存在且等价）
	resp, raw = env.request(t, http.MethodPut, "/api/queues/%2F/api.decl.q", body, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("等价重声明应 204，实际 %d %s", resp.StatusCode, raw)
	}

	// 参数真的落到了队列上（读回来自管理面对象）
	var q struct {
		Name       string         `json:"name"`
		Durable    bool           `json:"durable"`
		AutoDelete bool           `json:"auto_delete"`
		Arguments  map[string]any `json:"arguments"`
	}
	if resp := env.getJSON(t, "/api/queues/%2F/api.decl.q", &q); resp.StatusCode != http.StatusOK {
		t.Fatalf("声明的队列应可读，实际 %d", resp.StatusCode)
	}
	if q.Name != "api.decl.q" || !q.Durable || q.AutoDelete {
		t.Fatalf("队列字段不符: %+v", q)
	}

	// 参数不等价 → 400（等价性检查复用 AMQP 那一套）
	resp, raw = env.request(t, http.MethodPut, "/api/queues/%2F/api.decl.q",
		map[string]any{"durable": false, "auto_delete": false, "arguments": map[string]any{}}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("参数不等价的重声明应 400，实际 %d %s", resp.StatusCode, raw)
	}

	// 声明出来的队列与客户端走的是同一份拓扑：删除后再声明又是 201
	if resp, _ := env.request(t, http.MethodDelete, "/api/queues/%2F/api.decl.q", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除队列应 204，实际 %d", resp.StatusCode)
	}
	if resp, raw := env.request(t, http.MethodPut, "/api/queues/%2F/api.decl.q", body, "", ""); resp.StatusCode != http.StatusCreated {
		t.Fatalf("删除后重新声明应 201，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestDeclareQueueArgumentsAndReservedName 覆盖参数透传与保留名。
func TestDeclareQueueArgumentsAndReservedName(t *testing.T) {
	env := newTestEnv(t)

	// 参数（TTL / 死信）应原样落到队列上
	resp, raw := env.request(t, http.MethodPut, "/api/queues/%2F/api.decl.args",
		map[string]any{
			"durable": true, "auto_delete": false,
			"arguments": map[string]any{"x-message-ttl": 60000},
		}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("带参数声明队列应 201，实际 %d %s", resp.StatusCode, raw)
	}
	var q struct {
		Arguments map[string]any `json:"arguments"`
	}
	env.getJSON(t, "/api/queues/%2F/api.decl.args", &q)
	if got, ok := q.Arguments["x-message-ttl"]; !ok || got != float64(60000) {
		t.Fatalf("队列参数未透传: %+v", q.Arguments)
	}

	// 保留名 → 403（复用内核的保留名校验）
	resp, raw = env.request(t, http.MethodPut, "/api/queues/%2F/amq.decl.reserved",
		map[string]any{"durable": true, "auto_delete": false, "arguments": map[string]any{}}, "", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("声明 amq.* 队列应 403，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestDeclareExchangeOverManagementAPI 覆盖交换机声明的状态码、类型校验与删除。
func TestDeclareExchangeOverManagementAPI(t *testing.T) {
	env := newTestEnv(t)

	body := map[string]any{"type": "topic", "durable": true, "auto_delete": false, "internal": false, "arguments": map[string]any{}}
	resp, raw := env.request(t, http.MethodPut, "/api/exchanges/%2F/api.decl.ex", body, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("声明新交换机应 201，实际 %d %s", resp.StatusCode, raw)
	}
	resp, raw = env.request(t, http.MethodPut, "/api/exchanges/%2F/api.decl.ex", body, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("等价重声明应 204，实际 %d %s", resp.StatusCode, raw)
	}

	var ex struct {
		Name    string `json:"name"`
		Type    string `json:"type"`
		Durable bool   `json:"durable"`
	}
	if resp := env.getJSON(t, "/api/exchanges/%2F/api.decl.ex", &ex); resp.StatusCode != http.StatusOK {
		t.Fatalf("声明的交换机应可读，实际 %d", resp.StatusCode)
	}
	if ex.Name != "api.decl.ex" || ex.Type != "topic" || !ex.Durable {
		t.Fatalf("交换机字段不符: %+v", ex)
	}

	// 类型不一致 → 400
	resp, raw = env.request(t, http.MethodPut, "/api/exchanges/%2F/api.decl.ex",
		map[string]any{"type": "fanout", "durable": true}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("类型不一致应 400，实际 %d %s", resp.StatusCode, raw)
	}
	// 未知类型 → 400
	resp, raw = env.request(t, http.MethodPut, "/api/exchanges/%2F/api.decl.bad",
		map[string]any{"type": "no-such-type", "durable": true}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("未知交换机类型应 400，实际 %d %s", resp.StatusCode, raw)
	}
	// 不带 type → 按 direct 建成（对齐 RabbitMQ 实测）
	resp, raw = env.request(t, http.MethodPut, "/api/exchanges/%2F/api.decl.notype",
		map[string]any{"durable": true}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("缺省 type 应建成（201），实际 %d %s", resp.StatusCode, raw)
	}
	var def struct {
		Type string `json:"type"`
	}
	env.getJSON(t, "/api/exchanges/%2F/api.decl.notype", &def)
	if def.Type != "direct" {
		t.Fatalf("缺省 type 应落成 direct，实际 %q", def.Type)
	}

	// 删除 → 204；再删 → 404
	if resp, _ := env.request(t, http.MethodDelete, "/api/exchanges/%2F/api.decl.ex", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除交换机应 204，实际 %d", resp.StatusCode)
	}
	if resp, _ := env.request(t, http.MethodDelete, "/api/exchanges/%2F/api.decl.ex", nil, "", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("重复删除应 404，实际 %d", resp.StatusCode)
	}
	// 声明/删除默认交换机 → 拒绝（保留对象）
	if resp, raw := env.request(t, http.MethodPut, "/api/exchanges/%2F/amq.default",
		map[string]any{"type": "direct"}, "", ""); resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
		t.Fatalf("声明默认交换机不应成功，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestDeclareUsesCallerPermissions 覆盖"管理面声明以调用方身份执行"：
// 账号即使有 management 标签，若它在该 vhost 上没有 configure 权限，声明队列必须被拒。
//
// 这条是"复用内核逻辑"的直接证据 —— 如果管理面绕过会话直接建对象，下面的 403 就不会出现。
func TestDeclareUsesCallerPermissions(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	if resp, raw := env.request(t, http.MethodPut, "/api/users/limited",
		map[string]any{"password": "pw", "tags": "management"}, "", ""); resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建账号应 201，实际 %d %s", resp.StatusCode, raw)
	}
	// configure 为空 = 不允许声明任何拓扑（write/read 放开，确保拦住的是 configure 这一维）
	if resp, raw := env.request(t, http.MethodPut, "/api/permissions/%2F/limited",
		map[string]any{"configure": "", "write": ".*", "read": ".*"}, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("设置权限应 204，实际 %d %s", resp.StatusCode, raw)
	}

	if resp, raw := env.request(t, http.MethodPut, "/api/queues/%2F/api.decl.denied",
		map[string]any{"durable": true}, "limited", "pw"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("无 configure 权限声明队列应 403，实际 %d %s", resp.StatusCode, raw)
	}
	if resp, raw := env.request(t, http.MethodPut, "/api/exchanges/%2F/api.decl.denied",
		map[string]any{"type": "direct", "durable": true}, "limited", "pw"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("无 configure 权限声明交换机应 403，实际 %d %s", resp.StatusCode, raw)
	}
}
