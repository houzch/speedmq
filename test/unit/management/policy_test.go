package management_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

// 本文件覆盖 M8-6 的策略（policy）管理 API：CRUD 的对外契约，
// 以及"策略生效后队列对象要能说清是哪条策略给的"（运维据此排查）。
//
// 字段名对齐 RabbitMQ：`pattern` / `apply-to` / `definition` / `priority`。

// policyBody 组装一个策略请求体。
func policyBody(pattern, applyTo string, def map[string]any, priority int) map[string]any {
	return map[string]any{
		"pattern":    pattern,
		"apply-to":   applyTo,
		"definition": def,
		"priority":   priority,
	}
}

func TestPolicyCRUD(t *testing.T) {
	env := newTestEnv(t)

	// 初始为空（且是 200 + []，不是 404：工具链会无条件拉取）
	var list []map[string]any
	if resp := env.getJSON(t, "/api/policies", &list); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/policies 应 200，实际 %d", resp.StatusCode)
	}
	if len(list) != 0 {
		t.Fatalf("初始策略列表应为空，实际 %v", list)
	}

	// 创建（对齐 RabbitMQ：新建 201）
	resp, raw := env.request(t, http.MethodPut, "/api/policies/%2F/plen",
		policyBody(`^api\.`, "queues", map[string]any{"max-length": 3, "message-ttl": 60000}, 5), "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建策略应 201，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	// 更新同一条（204）
	resp, raw = env.request(t, http.MethodPut, "/api/policies/%2F/plen",
		policyBody(`^api\.`, "queues", map[string]any{"max-length": 4}, 5), "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("更新策略应 204，实际 %d（body=%s）", resp.StatusCode, raw)
	}

	// 单条读取：字段名与取值必须与 RabbitMQ 对齐
	var got map[string]any
	if resp := env.getJSON(t, "/api/policies/%2F/plen", &got); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 单条策略应 200，实际 %d", resp.StatusCode)
	}
	if got["name"] != "plen" || got["pattern"] != `^api\.` || got["apply-to"] != "queues" {
		t.Fatalf("策略字段不符: %v", got)
	}
	if got["priority"] != float64(5) {
		t.Fatalf("priority 应为 5，实际 %v", got["priority"])
	}
	def, ok := got["definition"].(map[string]any)
	if !ok || def["max-length"] != float64(4) {
		t.Fatalf("definition 应为更新后的内容: %v", got["definition"])
	}

	// vhost 维度列表
	var vhostList []map[string]any
	if resp := env.getJSON(t, "/api/policies/%2F", &vhostList); resp.StatusCode != http.StatusOK || len(vhostList) != 1 {
		t.Fatalf("GET /api/policies/%%2F 应返回 1 条，实际 %d 条（status=%d）", len(vhostList), resp.StatusCode)
	}

	// 删除
	resp, raw = env.request(t, http.MethodDelete, "/api/policies/%2F/plen", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE 策略应 204，实际 %d（body=%s）", resp.StatusCode, raw)
	}
	if resp := env.getJSON(t, "/api/policies/%2F/plen", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删除后 GET 应 404，实际 %d", resp.StatusCode)
	}
	// 再删一次：不存在就是 404（不是静默成功）
	resp, _ = env.request(t, http.MethodDelete, "/api/policies/%2F/plen", nil, "", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("删除不存在的策略应 404，实际 %d", resp.StatusCode)
	}
}

func TestPolicyRejectsBadInput(t *testing.T) {
	env := newTestEnv(t)

	cases := []struct {
		desc   string
		path   string
		body   map[string]any
		status int
	}{
		{
			"vhost 不存在",
			"/api/policies/%2Fnosuch/px",
			policyBody(`^a`, "queues", map[string]any{"max-length": 1}, 0),
			http.StatusNotFound,
		},
		{
			"apply-to 非法",
			"/api/policies/%2F/px",
			policyBody(`^a`, "classic", map[string]any{"max-length": 1}, 0),
			http.StatusBadRequest,
		},
		{
			"pattern 非法",
			"/api/policies/%2F/px",
			policyBody(`([`, "queues", map[string]any{"max-length": 1}, 0),
			http.StatusBadRequest,
		},
		{
			"未支持的键",
			"/api/policies/%2F/px",
			policyBody(`^a`, "queues", map[string]any{"no-such-key": 1}, 0),
			http.StatusBadRequest,
		},
		{
			"队列策略给了交换机键",
			"/api/policies/%2F/px",
			policyBody(`^a`, "queues", map[string]any{"alternate-exchange": "x"}, 0),
			http.StatusBadRequest,
		},
	}
	for _, c := range cases {
		resp, raw := env.request(t, http.MethodPut, c.path, c.body, "", "")
		if resp.StatusCode != c.status {
			t.Fatalf("%s：应 %d，实际 %d（body=%s）", c.desc, c.status, resp.StatusCode, raw)
		}
	}

	// 整个过程不应留下任何策略
	var list []map[string]any
	env.getJSON(t, "/api/policies", &list)
	if len(list) != 0 {
		t.Fatalf("被拒绝的策略不该落库，实际 %v", list)
	}
}

// TestPolicyShowsUpOnQueue：策略生效后，队列对象必须能说明"限制是哪条策略给的"。
func TestPolicyShowsUpOnQueue(t *testing.T) {
	env := newTestEnv(t)
	env.declareQueue(t, "api.pol.q", nil)

	resp, raw := env.request(t, http.MethodPut, "/api/policies/%2F/pq",
		policyBody(`^api\.`, "queues", map[string]any{"max-length": 7}, 0), "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建策略应 201，实际 %d（body=%s）", resp.StatusCode, raw)
	}

	var q map[string]any
	if resp := env.getJSON(t, "/api/queues/%2F/api.pol.q", &q); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 队列应 200，实际 %d", resp.StatusCode)
	}
	if q["policy"] != "pq" {
		t.Fatalf("队列应显示命中策略 pq，实际 %v", q["policy"])
	}
	eff, ok := q["effective_policy_definition"].(map[string]any)
	if !ok || eff["max-length"] != float64(7) {
		t.Fatalf("effective_policy_definition 应含 max-length=7，实际 %v", q["effective_policy_definition"])
	}
	// 队列自己声明的参数不受影响（策略参数是"默认值"，不写进 arguments）
	if args, ok := q["arguments"].(map[string]any); !ok || len(args) != 0 {
		t.Fatalf("队列 arguments 应保持为空，实际 %v", q["arguments"])
	}

	// 删掉策略后，队列对象不再挂策略
	if resp, _ := env.request(t, http.MethodDelete, "/api/policies/%2F/pq", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE 应 204，实际 %d", resp.StatusCode)
	}
	env.getJSON(t, "/api/queues/%2F/api.pol.q", &q)
	if q["policy"] != nil {
		t.Fatalf("策略删除后队列不应再挂策略，实际 %v", q["policy"])
	}
	if eff, ok := q["effective_policy_definition"].(map[string]any); !ok || len(eff) != 0 {
		t.Fatalf("策略删除后 effective_policy_definition 应为空，实际 %v", q["effective_policy_definition"])
	}
}

// TestPolicyResponseIsValidJSON：保证列表响应是 JSON 数组而不是 null
// （管理 UI 会直接对它做 .map）。
func TestPolicyResponseIsValidJSON(t *testing.T) {
	env := newTestEnv(t)
	_, raw := env.request(t, http.MethodGet, "/api/policies", nil, "", "")
	var arr []any
	if err := json.Unmarshal(raw, &arr); err != nil {
		t.Fatalf("响应不是 JSON 数组: %v（原始 %s）", err, raw)
	}
}
