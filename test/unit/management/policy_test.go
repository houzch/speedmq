package management_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	sdk "github.com/houzch/swiftmq/pkg/plugin"
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

// TestPolicyAlternateExchangeOverHTTP 覆盖**交换机侧**策略的完整链路：
// 经管理 API 建策略（`apply-to=exchanges`）→ 数据面观察到"未路由消息改投备用交换机"
// → 交换机对象显示命中的策略 → 删策略后兜底解除。
//
// 为什么 broker 层已有 TestPolicyAlternateExchange 还要在管理面再测一遍：
// 这里的契约是"**经 HTTP** 建的策略在数据面生效"，中间隔着 JSON 字段名（`apply-to` / `definition`）
// 与名称正则匹配，任一环节写错都会表现为"API 回 201、数据面却没生效"——只在管理面才测得出。
func TestPolicyAlternateExchangeOverHTTP(t *testing.T) {
	env := newTestEnv(t)
	const main, alt, queue = "api.ae.main", "api.ae.alt", "api.ae.q"

	// 数据面：主交换机（direct，故意不建绑定）与备用交换机（fanout）+ 队列
	declareExchange(t, env, main, sdk.ExchangeDirect)
	declareExchange(t, env, alt, sdk.ExchangeFanout)
	env.declareQueue(t, queue, nil)

	sess, err := env.broker.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()
	if err := sess.BindQueue(queue, alt, "", nil); err != nil {
		t.Fatalf("绑定备用交换机失败: %v", err)
	}

	publish := func() {
		t.Helper()
		if _, err := sess.Publish(&sdk.Message{RoutingKey: "k", Body: []byte("via-alt")}, main, "k", false); err != nil {
			t.Fatalf("发布失败: %v", err)
		}
	}
	ready := func() int {
		t.Helper()
		var q struct {
			MessagesReady int `json:"messages_ready"`
		}
		if resp := env.getJSON(t, "/api/queues/%2F/"+queue, &q); resp.StatusCode != http.StatusOK {
			t.Fatalf("读队列对象失败: %d", resp.StatusCode)
		}
		return q.MessagesReady
	}

	// 上策略之前：主交换机没有绑定，消息未路由，不会进备用队列
	publish()
	if got := ready(); got != 0 {
		t.Fatalf("未设策略时主交换机的消息不应进备用队列，实际 %d 条", got)
	}

	// 管理 API：给主交换机挂 alternate-exchange
	resp, raw := env.request(t, http.MethodPut, "/api/policies/%2F/ae",
		policyBody(`^api\.ae\.main$`, "exchanges", map[string]any{"alternate-exchange": alt}, 0), "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("新建交换机策略应 201，实际 %d %s", resp.StatusCode, raw)
	}

	// 数据面：策略生效后未路由的消息改投备用交换机 → 落到队列
	publish()
	if got := ready(); got != 1 {
		t.Fatalf("alternate-exchange 生效后消息应改投备用队列，实际 %d 条", got)
	}

	// 交换机对象要能说清是哪条策略给的
	var ex struct {
		Policy                    string         `json:"policy"`
		EffectivePolicyDefinition map[string]any `json:"effective_policy_definition"`
	}
	if resp := env.getJSON(t, "/api/exchanges/%2F/"+main, &ex); resp.StatusCode != http.StatusOK {
		t.Fatalf("读交换机对象失败: %d", resp.StatusCode)
	}
	if ex.Policy != "ae" || fmt.Sprint(ex.EffectivePolicyDefinition["alternate-exchange"]) != alt {
		t.Fatalf("交换机应显示策略 ae 与 alternate-exchange=%s，实际 %q %v", alt, ex.Policy, ex.EffectivePolicyDefinition)
	}

	// 删策略 → 兜底解除：再发布不再进备用队列
	resp, raw = env.request(t, http.MethodDelete, "/api/policies/%2F/ae", nil, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除策略应 204，实际 %d %s", resp.StatusCode, raw)
	}
	before := ready()
	publish()
	if got := ready(); got != before {
		t.Fatalf("删除策略后消息不应再进备用队列（%d → %d）", before, got)
	}
}
