package management_test

import (
	"net/http"
	"testing"
)

// 本文件覆盖 M8-7 的 vhost 管理 API：PUT / DELETE 的对外契约（状态码、权限、约束）。
//
// 字段与状态码对齐 RabbitMQ 4.x：新建 201、重复 PUT 204、删除 204、删除不存在 404。

func TestVHostCRUD(t *testing.T) {
	env := newTestEnv(t)

	// 初始必须能列出默认 vhost（工具链会无条件拉取，返回 null 会让 UI 报错）
	var list []map[string]any
	if resp := env.getJSON(t, "/api/vhosts", &list); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/vhosts 应 200，实际 %d", resp.StatusCode)
	}
	if !vhostNames(list)["/"] {
		t.Fatalf("默认 vhost 应出现在列表里: %v", list)
	}

	// 新建 → 201
	resp, raw := env.request(t, http.MethodPut, "/api/vhosts/vtest", nil, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建 vhost 应 201，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	// 重复 PUT → 204（幂等，不报错）
	resp, raw = env.request(t, http.MethodPut, "/api/vhosts/vtest", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("重复 PUT 应 204，实际 %d（body=%s）", resp.StatusCode, raw)
	}

	// 单条读取
	var got map[string]any
	if resp := env.getJSON(t, "/api/vhosts/vtest", &got); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 单个 vhost 应 200，实际 %d", resp.StatusCode)
	}
	if got["name"] != "vtest" {
		t.Fatalf("vhost 对象 name 应为 vtest，实际 %v", got["name"])
	}

	// 列表里也要出现
	env.getJSON(t, "/api/vhosts", &list)
	if !vhostNames(list)["vtest"] {
		t.Fatalf("新建后列表应包含 vtest: %v", list)
	}

	// 删除 → 204
	resp, raw = env.request(t, http.MethodDelete, "/api/vhosts/vtest", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除 vhost 应 204，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	if resp := env.getJSON(t, "/api/vhosts/vtest", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删除后 GET 应 404，实际 %d", resp.StatusCode)
	}
	// 再删一次 → 404（不是静默成功）
	if resp, _ := env.request(t, http.MethodDelete, "/api/vhosts/vtest", nil, "", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删除不存在的 vhost 应 404，实际 %d", resp.StatusCode)
	}
}

// TestVHostDeleteDefaultRefused 覆盖安全约束：默认 vhost 不可删除，返回 400。
func TestVHostDeleteDefaultRefused(t *testing.T) {
	env := newTestEnv(t)
	// 默认 vhost 在 URL 里按 RabbitMQ 约定写作 %2F
	resp, raw := env.request(t, http.MethodDelete, "/api/vhosts/%2F", nil, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("删除默认 vhost 应 400，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	// 它必须还在
	if resp := env.getJSON(t, "/api/vhosts/%2F", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("默认 vhost 应仍然存在，GET 实际 %d", resp.StatusCode)
	}
}

// TestVHostManagementRequiresAdministrator 覆盖权限收敛：
// vhost 是全局对象，只有 administrator 标签能增删；management 标签不足。
func TestVHostManagementRequiresAdministrator(t *testing.T) {
	env := newTestEnv(t)
	if err := env.broker.UpsertUser("ops", "ops-pass", []string{"management"}); err != nil {
		t.Fatalf("创建 management 用户失败: %v", err)
	}

	resp, raw := env.request(t, http.MethodPut, "/api/vhosts/vadmin", nil, "ops", "ops-pass")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("management 用户新建 vhost 应 403，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	resp, raw = env.request(t, http.MethodDelete, "/api/vhosts/%2F", nil, "ops", "ops-pass")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("management 用户删除 vhost 应 403，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	// 被拒绝的操作不得留下副作用
	var list []map[string]any
	env.getJSON(t, "/api/vhosts", &list)
	if vhostNames(list)["vadmin"] {
		t.Fatalf("被拒绝的创建不应生效: %v", list)
	}
}

func vhostNames(list []map[string]any) map[string]bool {
	out := make(map[string]bool, len(list))
	for _, item := range list {
		if name, ok := item["name"].(string); ok {
			out[name] = true
		}
	}
	return out
}
