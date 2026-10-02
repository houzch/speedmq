package management_test

import (
	"net/http"
	"testing"
)

// 本文件覆盖三组新管理接口的 **HTTP 契约**（路径、状态码、响应形状）：
//
//   - /api/vhost-limits           vhost 级限制（对齐 RabbitMQ：写 204、删 204、未知 vhost 404、未知限制名 400）
//   - /api/feature-flags          特性开关（列表 + enable/disable，写 204、未知开关 404）
//   - /api/deprecated-features    弃用特性只读清单
//
// 这三组接口的形状都是被外部工具消费的契约，字段名错了不会报错、只会静默失效，
// 因此逐条断言。

// vhostLimitObject 是 /api/vhost-limits 的返回元素。
type vhostLimitObject struct {
	VHost string         `json:"vhost"`
	Value map[string]int `json:"value"`
}

// featureFlagObject 是 /api/feature-flags 的返回元素。
type featureFlagObject struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	Stability  string `json:"stability"`
	Desc       string `json:"desc"`
	ProvidedBy string `json:"provided_by"`
}

// deprecatedFeatureObject 是 /api/deprecated-features 的返回元素。
type deprecatedFeatureObject struct {
	Name             string `json:"name"`
	State            string `json:"state"`
	DeprecationPhase string `json:"deprecation_phase"`
	Desc             string `json:"desc"`
}

func TestVHostLimitsEndpoints(t *testing.T) {
	env := newTestEnv(t)

	var empty []vhostLimitObject
	if resp := env.getJSON(t, "/api/vhost-limits", &empty); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/vhost-limits 应 200，实际 %d", resp.StatusCode)
	}
	if len(empty) != 0 {
		t.Fatalf("初始不应有限制，实际 %+v", empty)
	}

	// 写入：204（RabbitMQ 实测是 204，不是 201）
	resp, _ := env.request(t, http.MethodPut, "/api/vhost-limits/%2F/max-queues",
		map[string]any{"value": 10}, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT /api/vhost-limits/%%2F/max-queues 应 204，实际 %d", resp.StatusCode)
	}

	// 列表形状：按 vhost 分组，值在嵌套的 value 里
	var list []vhostLimitObject
	if resp := env.getJSON(t, "/api/vhost-limits", &list); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/vhost-limits 应 200，实际 %d", resp.StatusCode)
	}
	if len(list) != 1 || list[0].VHost != "/" || list[0].Value["max-queues"] != 10 {
		t.Fatalf("列表形状不符，实际 %+v", list)
	}

	// 单 vhost 形式（路径里的 vhost 是独立注册的一条路由）
	var single []vhostLimitObject
	if resp := env.getJSON(t, "/api/vhost-limits/%2F", &single); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/vhost-limits/%%2F 应 200，实际 %d", resp.StatusCode)
	}
	if len(single) != 1 || single[0].Value["max-queues"] != 10 {
		t.Fatalf("单 vhost 列表形状不符，实际 %+v", single)
	}

	// 未知限制名 → 400；未知 vhost → 404；缺 value → 400
	resp, _ = env.request(t, http.MethodPut, "/api/vhost-limits/%2F/max-foo",
		map[string]any{"value": 1}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("未知限制名应 400，实际 %d", resp.StatusCode)
	}
	resp, _ = env.request(t, http.MethodPut, "/api/vhost-limits/nope/max-queues",
		map[string]any{"value": 1}, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("未知 vhost 应 404，实际 %d", resp.StatusCode)
	}
	resp, _ = env.request(t, http.MethodPut, "/api/vhost-limits/%2F/max-queues",
		map[string]any{}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("缺 value 应 400，实际 %d", resp.StatusCode)
	}

	// 删除：204（且重复删除仍 204，与 RabbitMQ 一致）
	resp, _ = env.request(t, http.MethodDelete, "/api/vhost-limits/%2F/max-queues", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE 应 204，实际 %d", resp.StatusCode)
	}
	resp, _ = env.request(t, http.MethodDelete, "/api/vhost-limits/%2F/max-queues", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("重复 DELETE 应 204，实际 %d", resp.StatusCode)
	}

	// 方法不支持：路径匹配但方法不对 → 405
	resp, _ = env.request(t, http.MethodPost, "/api/vhost-limits/%2F/max-queues", nil, "", "")
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST 应 405，实际 %d", resp.StatusCode)
	}
}

func TestFeatureFlagsEndpoints(t *testing.T) {
	env := newTestEnv(t)

	var flags []featureFlagObject
	if resp := env.getJSON(t, "/api/feature-flags", &flags); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/feature-flags 应 200，实际 %d", resp.StatusCode)
	}
	if len(flags) == 0 {
		t.Fatal("特性开关列表不应为空")
	}
	stateOf := func(name string) string {
		for _, f := range flags {
			if f.Name == name {
				return f.State
			}
		}
		return ""
	}
	if stateOf("quorum_queue") != "enabled" {
		t.Fatalf("quorum_queue 默认应为 enabled，实际 %q（列表 %+v）", stateOf("quorum_queue"), flags)
	}

	if resp, _ := env.request(t, http.MethodPut, "/api/feature-flags/quorum_queue/disable", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("disable 应 204，实际 %d", resp.StatusCode)
	}
	flags = nil
	env.getJSON(t, "/api/feature-flags", &flags)
	if stateOf("quorum_queue") != "disabled" {
		t.Fatalf("关闭后应为 disabled，实际 %q", stateOf("quorum_queue"))
	}

	if resp, _ := env.request(t, http.MethodPut, "/api/feature-flags/quorum_queue/enable", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("enable 应 204，实际 %d", resp.StatusCode)
	}
	flags = nil
	env.getJSON(t, "/api/feature-flags", &flags)
	if stateOf("quorum_queue") != "enabled" {
		t.Fatalf("重新开启后应为 enabled，实际 %q", stateOf("quorum_queue"))
	}

	// 未知开关 → 404（不是 204：把参数错误伪装成成功最难排查）
	if resp, _ := env.request(t, http.MethodPut, "/api/feature-flags/no_such_flag/enable", nil, "", ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("未知开关应 404，实际 %d", resp.StatusCode)
	}
}

// TestFeatureFlagWriteRequiresAdministrator 覆盖授权：management 标签可读但不可写，
// 因为它改的是**全局**能力，不是某个 vhost 内的操作。
func TestFeatureFlagWriteRequiresAdministrator(t *testing.T) {
	env := newTestEnv(t)
	if err := env.broker.UpsertUser("ffmgmt", "pw", []string{"management"}); err != nil {
		t.Fatalf("建账号失败: %v", err)
	}

	var flags []featureFlagObject
	if resp := env.getJSON(t, "/api/feature-flags", &flags); resp.StatusCode != http.StatusOK {
		t.Fatalf("管理员读取应 200，实际 %d", resp.StatusCode)
	}
	if resp, _ := env.request(t, http.MethodGet, "/api/feature-flags", nil, "ffmgmt", "pw"); resp.StatusCode != http.StatusOK {
		t.Fatalf("management 标签读取应 200，实际 %d", resp.StatusCode)
	}
	resp, _ := env.request(t, http.MethodPut, "/api/feature-flags/quorum_queue/disable", nil, "ffmgmt", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("management 标签写特性开关应 403，实际 %d", resp.StatusCode)
	}
}

func TestDeprecatedFeaturesEndpoint(t *testing.T) {
	env := newTestEnv(t)

	var items []deprecatedFeatureObject
	if resp := env.getJSON(t, "/api/deprecated-features", &items); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/deprecated-features 应 200，实际 %d", resp.StatusCode)
	}
	found := false
	for _, item := range items {
		if item.Name == "transient_nonexcl_queues" {
			found = true
			if item.State != "denied" || item.DeprecationPhase == "" || item.Desc == "" {
				t.Fatalf("transient_nonexcl_queues 的字段不完整: %+v", item)
			}
		}
	}
	if !found {
		t.Fatalf("弃用特性清单里应包含 transient_nonexcl_queues，实际 %+v", items)
	}

	// 只读：RabbitMQ 4.x 的弃用特性没有 enable/disable 接口，我们也不提供。
	resp, _ := env.request(t, http.MethodPut, "/api/deprecated-features/%2F/transient_nonexcl_queues/enable", nil, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("弃用特性不应提供写接口（应 404），实际 %d", resp.StatusCode)
	}
}
