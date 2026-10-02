package management_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
)

// 本文件覆盖账号管理（M8-16 之后新增的一轮）：总账号保护、首次强制改密、
// 多账号与子账号权限。
//
// 断言都落在"外部调用方能看到什么"上：whoami 的字段、保护规则返回的状态码、
// 改凭据之后旧凭据是否失效 —— 这些正是管理 UI 与运维脚本依赖的契约。

// waitUser 等账号状态满足条件。
//
// 引导跑在后台协程里（见 broker.bootstrapUsers），且 auth 表初始就带着配置里的账号
// （Root 标记来自配置），因此"元数据里的字段"（如 must_change_password）会比配置晚一步出现 ——
// 这里用"轮询 + 超时"等它落地，而不是假定 newTestEnv 返回时就已经就绪。
func waitUser(t *testing.T, env *testEnv, name string, want func(broker.UserSnapshot) bool) broker.UserSnapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if u, ok := env.broker.User(name); ok && want(u) {
			return u
		}
		if time.Now().After(deadline) {
			u, _ := env.broker.User(name)
			t.Fatalf("等待账号 %s 进入期望状态超时，当前: %+v", name, u)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitRootUser 等首次引导把账号播种为总账号（并带上待改密标记）。
func waitRootUser(t *testing.T, env *testEnv, name string) broker.UserSnapshot {
	t.Helper()
	return waitUser(t, env, name, func(u broker.UserSnapshot) bool {
		return u.Root && u.MustChangePassword
	})
}

// whoamiAs 以指定凭据读 /api/whoami。
func whoamiAs(t *testing.T, env *testEnv, user, pass string) (int, map[string]any) {
	t.Helper()
	resp, raw := env.request(t, http.MethodGet, "/api/whoami", nil, user, pass)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("解析 whoami 失败: %v（%s）", err, raw)
	}
	return resp.StatusCode, out
}

// TestWhoamiReportsRootAndMustChange 覆盖 whoami 的两个新字段：
// 管理 UI 据此决定要不要强制弹出"首次改账号名/口令"对话框。
func TestWhoamiReportsRootAndMustChange(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	code, body := whoamiAs(t, env, "guest", "guest")
	if code != http.StatusOK {
		t.Fatalf("whoami 应 200，实际 %d", code)
	}
	if body["is_root"] != true {
		t.Fatalf("内置 guest 应是总管理员，实际 is_root=%v", body["is_root"])
	}
	if body["must_change_password"] != true {
		t.Fatalf("新装实例的出厂账号应处于待改密状态，实际 %v", body["must_change_password"])
	}
}

// TestRootAccountIsProtected 覆盖总账号的三条不可：不可删除、不可禁用、不可降级。
func TestRootAccountIsProtected(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	// 删除总账号
	resp, raw := env.request(t, http.MethodDelete, "/api/users/guest", nil, "", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("删除总账号应 403，实际 %d %s", resp.StatusCode, raw)
	}
	// 禁用总账号
	resp, raw = env.request(t, http.MethodPut, "/api/users/guest",
		map[string]any{"disabled": true}, "", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("禁用总账号应 403，实际 %d %s", resp.StatusCode, raw)
	}
	// 降级总账号（移除 administrator 标签）
	resp, raw = env.request(t, http.MethodPut, "/api/users/guest",
		map[string]any{"tags": "management"}, "", "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("降级总账号应 403，实际 %d %s", resp.StatusCode, raw)
	}
	// 改一下无关字段（保持 administrator）应当放行，否则规则写过头了
	resp, raw = env.request(t, http.MethodPut, "/api/users/guest",
		map[string]any{"tags": "administrator"}, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("总账号改标签（仍保留 administrator）应 204，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestRootAccountOnlyEditableBySelf 覆盖"总账号只能由本人修改"。
func TestRootAccountOnlyEditableBySelf(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	resp, raw := env.request(t, http.MethodPut, "/api/users/second",
		map[string]any{"password": "pw", "tags": "administrator"}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建第二个管理员应 201，实际 %d %s", resp.StatusCode, raw)
	}
	// 另一个管理员不能改总账号的口令
	resp, raw = env.request(t, http.MethodPut, "/api/users/guest",
		map[string]any{"password": "hacked"}, "second", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("其他管理员改总账号应 403，实际 %d %s", resp.StatusCode, raw)
	}
	// 也不能改总账号的凭据（改名/改密）
	resp, raw = env.request(t, http.MethodPost, "/api/users/guest/credentials",
		map[string]any{"password": "hacked"}, "second", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("其他管理员改总账号凭据应 403，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestCannotDeleteCurrentLoginAccount 覆盖"不能删除当前登录账号"。
func TestCannotDeleteCurrentLoginAccount(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	resp, raw := env.request(t, http.MethodPut, "/api/users/selfops",
		map[string]any{"password": "pw", "tags": "administrator"}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建管理员应 201，实际 %d %s", resp.StatusCode, raw)
	}
	resp, raw = env.request(t, http.MethodDelete, "/api/users/selfops", nil, "selfops", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("删除当前登录账号应 403，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestMustKeepEnabledAdministrator 覆盖"至少要保留一个启用中的 administrator"。
//
// 正常部署里总账号保证了这一点；这条规则是给"配置里压根没有 administrator"的部署兜底，
// 因此这里绕过管理面策略直接构造"唯一管理员不是总账号"的状态。
func TestMustKeepEnabledAdministrator(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	if err := env.broker.UpsertUser("solo", "pw", []string{"administrator"}); err != nil {
		t.Fatalf("创建管理员失败: %v", err)
	}
	if ok, err := env.broker.DeleteUser("guest"); err != nil || !ok {
		t.Fatalf("清掉总账号失败: ok=%v err=%v", ok, err)
	}

	// 降级唯一的管理员
	resp, raw := env.request(t, http.MethodPut, "/api/users/solo",
		map[string]any{"tags": "management"}, "solo", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("降级最后一个管理员应 403，实际 %d %s", resp.StatusCode, raw)
	}
	// 禁用唯一的管理员
	resp, raw = env.request(t, http.MethodPut, "/api/users/solo",
		map[string]any{"disabled": true}, "solo", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("禁用最后一个管理员应 403，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestSelfServiceCredentialChange 覆盖首次强制改密走的路径：
// 一次请求同时改掉账号名与口令，旧凭据立即失效，待改密标记被清除，权限记录随迁。
func TestSelfServiceCredentialChange(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	resp, raw := env.request(t, http.MethodPost, "/api/users/guest/credentials",
		map[string]any{"name": "rootadmin", "password": "s3cret"}, "guest", "guest")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("改账号名与口令应 204，实际 %d %s", resp.StatusCode, raw)
	}

	// 旧凭据失效
	if code, _ := whoamiAs(t, env, "guest", "guest"); code != http.StatusUnauthorized {
		t.Fatalf("旧凭据应失效（401），实际 %d", code)
	}
	// 新凭据可用，且不再要求改密、仍是总账号
	code, body := whoamiAs(t, env, "rootadmin", "s3cret")
	if code != http.StatusOK {
		t.Fatalf("新凭据应可登录，实际 %d", code)
	}
	if body["name"] != "rootadmin" || body["is_root"] != true || body["must_change_password"] != false {
		t.Fatalf("改名后 whoami 不符: %v", body)
	}
	// 权限记录随账号一起迁移（内置 guest 在默认 vhost 上有完全权限）。
	// 注意：改名之后旧账号已不存在，后续请求必须用**新凭据**。
	if resp, _ := env.request(t, http.MethodGet, "/api/permissions/%2F/rootadmin", nil, "rootadmin", "s3cret"); resp.StatusCode != http.StatusOK {
		t.Fatalf("改名后权限记录应随迁，实际 %d", resp.StatusCode)
	}
	if resp, _ := env.request(t, http.MethodGet, "/api/permissions/%2F/guest", nil, "rootadmin", "s3cret"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("旧账号名的权限记录应已删除，实际 %d", resp.StatusCode)
	}
}

// TestRenameToExistingUserRejected 覆盖改名撞名。
func TestRenameToExistingUserRejected(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	for _, name := range []string{"dup1", "dup2"} {
		if resp, raw := env.request(t, http.MethodPut, "/api/users/"+name,
			map[string]any{"password": "pw", "tags": "management"}, "", ""); resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建 %s 应 201，实际 %d %s", name, resp.StatusCode, raw)
		}
	}
	resp, raw := env.request(t, http.MethodPost, "/api/users/dup1/credentials",
		map[string]any{"name": "dup2"}, "", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("改名撞上已存在账号应 400，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestDisabledAccountCannotLogin 覆盖禁用账号后管理面拒绝登录。
func TestDisabledAccountCannotLogin(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	resp, raw := env.request(t, http.MethodPut, "/api/users/gone",
		map[string]any{"password": "pw", "tags": "management"}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建账号应 201，实际 %d %s", resp.StatusCode, raw)
	}
	if code, _ := whoamiAs(t, env, "gone", "pw"); code != http.StatusOK {
		t.Fatalf("启用状态下应能登录，实际 %d", code)
	}
	resp, raw = env.request(t, http.MethodPut, "/api/users/gone", map[string]any{"disabled": true}, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("禁用账号应 204，实际 %d %s", resp.StatusCode, raw)
	}
	if code, _ := whoamiAs(t, env, "gone", "pw"); code != http.StatusUnauthorized {
		t.Fatalf("禁用后应拒绝登录（401），实际 %d", code)
	}
}

// TestSubAccountPermissionManageable 覆盖"子账号的权限可管理"：
// 账号 CRUD + 权限 CRUD + 权限真的约束了该账号能做什么。
func TestSubAccountPermissionManageable(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	// 建两个子账号：一个 management、一个 monitoring
	for name, tag := range map[string]string{"app1": "management", "app2": "monitoring"} {
		if resp, raw := env.request(t, http.MethodPut, "/api/users/"+name,
			map[string]any{"password": "pw", "tags": tag}, "", ""); resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建 %s 应 201，实际 %d %s", name, resp.StatusCode, raw)
		}
	}

	// 给 app1 配默认 vhost 的权限
	resp, raw := env.request(t, http.MethodPut, "/api/permissions/%2F/app1",
		map[string]any{"configure": "^app\\.", "write": "^app\\.", "read": ".*"}, "", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("设置子账号权限应 204，实际 %d %s", resp.StatusCode, raw)
	}
	var perm struct {
		User      string `json:"user"`
		Configure string `json:"configure"`
	}
	if resp := env.getJSON(t, "/api/permissions/%2F/app1", &perm); resp.StatusCode != http.StatusOK {
		t.Fatalf("读取子账号权限应 200，实际 %d", resp.StatusCode)
	}
	if perm.User != "app1" || perm.Configure != `^app\.` {
		t.Fatalf("子账号权限字段不符: %+v", perm)
	}

	// 账号列表能同时看到总账号与子账号
	var users []struct {
		Name   string `json:"name"`
		IsRoot bool   `json:"is_root"`
	}
	if resp := env.getJSON(t, "/api/users", &users); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/users 应 200，实际 %d", resp.StatusCode)
	}
	seen := map[string]bool{}
	for _, u := range users {
		seen[u.Name] = u.IsRoot
	}
	for _, name := range []string{"guest", "app1", "app2"} {
		if _, ok := seen[name]; !ok {
			t.Fatalf("账号列表缺少 %s: %+v", name, users)
		}
	}
	if !seen["guest"] || seen["app1"] {
		t.Fatalf("is_root 标记不符（应只有 guest 为 true）: %+v", users)
	}

	// 权限真的约束行为：monitoring 只能读，不能写
	if resp, _ := env.request(t, http.MethodGet, "/api/overview", nil, "app2", "pw"); resp.StatusCode != http.StatusOK {
		t.Fatalf("monitoring 账号应能读 overview，实际 %d", resp.StatusCode)
	}
	resp, raw = env.request(t, http.MethodPut, "/api/users/app2",
		map[string]any{"tags": "management"}, "app2", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("monitoring 账号不应能改账号（403），实际 %d %s", resp.StatusCode, raw)
	}

	// 删除权限 → 204
	if resp, raw := env.request(t, http.MethodDelete, "/api/permissions/%2F/app1", nil, "", ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("删除子账号权限应 204，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestNonAdminCannotChangeOthersCredentials 覆盖"改他人凭据需要 administrator"。
func TestNonAdminCannotChangeOthersCredentials(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	for name, tag := range map[string]string{"nobody": "monitoring", "victim": "management"} {
		if resp, raw := env.request(t, http.MethodPut, "/api/users/"+name,
			map[string]any{"password": "pw", "tags": tag}, "", ""); resp.StatusCode != http.StatusCreated {
			t.Fatalf("创建 %s 应 201，实际 %d %s", name, resp.StatusCode, raw)
		}
	}
	resp, raw := env.request(t, http.MethodPost, "/api/users/victim/credentials",
		map[string]any{"password": "hacked"}, "nobody", "pw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("非管理员改他人凭据应 403，实际 %d %s", resp.StatusCode, raw)
	}
}

// TestAPIGroupRestrictsManagementAPI 覆盖"管理接口功能组"权限：
// 勾选后只能访问这些组，未勾选（空）则不受限。
func TestAPIGroupRestrictsManagementAPI(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	// 只给「概览与节点」：能看概览与指标，看不到队列/连接
	resp, raw := env.request(t, http.MethodPut, "/api/users/mon",
		map[string]any{"password": "pw", "tags": "monitoring", "api_groups": []string{"overview"}}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建只读监控账号应 201，实际 %d %s", resp.StatusCode, raw)
	}

	cases := []struct {
		method, path string
		want         int
		desc         string
	}{
		{http.MethodGet, "/api/overview", http.StatusOK, "概览在授权组内"},
		{http.MethodGet, "/api/metrics", http.StatusOK, "指标同属概览组"},
		{http.MethodGet, "/api/whoami", http.StatusOK, "身份探针不受功能组收窄"},
		{http.MethodGet, "/api/queues", http.StatusForbidden, "队列属 topology，未授权"},
		{http.MethodGet, "/api/connections", http.StatusForbidden, "连接属 connections，未授权"},
		{http.MethodGet, "/api/users", http.StatusForbidden, "账号属 accounts，未授权"},
		{http.MethodGet, "/api/policies", http.StatusForbidden, "策略属 policies，未授权"},
	}
	for _, c := range cases {
		if got, body := env.request(t, c.method, c.path, nil, "mon", "pw"); got.StatusCode != c.want {
			t.Fatalf("%s：%s %s 应 %d，实际 %d %s", c.desc, c.method, c.path, c.want, got.StatusCode, body)
		}
	}

	// 未勾选功能组（空）= 不限制：内置总账号没有配功能组，照旧能访问全部
	if resp, _ := env.request(t, http.MethodGet, "/api/queues", nil, "guest", "guest"); resp.StatusCode != http.StatusOK {
		t.Fatalf("未配功能组的账号不应被收窄，实际 %d", resp.StatusCode)
	}

	// 账号列表把这组权限如实暴露给管理 UI
	var users []struct {
		Name      string   `json:"name"`
		APIGroups []string `json:"api_groups"`
	}
	if resp := env.getJSON(t, "/api/users", &users); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/users 应 200，实际 %d", resp.StatusCode)
	}
	found := false
	for _, u := range users {
		if u.Name == "mon" {
			found = len(u.APIGroups) == 1 && u.APIGroups[0] == "overview"
		}
	}
	if !found {
		t.Fatalf("账号列表未如实返回 api_groups: %+v", users)
	}
}

// TestAPIGroupUnknownValueIgnored 覆盖未知功能组名的处理：忽略而不是报错，且不会带来权限。
func TestAPIGroupUnknownValueIgnored(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	resp, raw := env.request(t, http.MethodPut, "/api/users/weird",
		map[string]any{
			"password": "pw", "tags": "monitoring",
			"api_groups": []string{"overview", "no-such-group", "overview"},
		}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("含未知功能组也应创建成功（201），实际 %d %s", resp.StatusCode, raw)
	}
	var u struct {
		APIGroups []string `json:"api_groups"`
	}
	if resp := env.getJSON(t, "/api/users/weird", &u); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 账号应 200，实际 %d", resp.StatusCode)
	}
	if len(u.APIGroups) != 1 || u.APIGroups[0] != "overview" {
		t.Fatalf("未知功能组应被忽略、重复项应去重，实际 %v", u.APIGroups)
	}
	// 未知组名不会顺带放行别的组
	if resp, _ := env.request(t, http.MethodGet, "/api/queues", nil, "weird", "pw"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("未知功能组不应放行 topology，实际 %d", resp.StatusCode)
	}
}

// TestCredentialsAlwaysAllowedAndGroupsSurviveRename 覆盖两点：
// 自助改凭据不参与功能组收窄（否则账号连自己的初始口令都改不了）；
// 改名时功能组随账号一起迁移。
func TestCredentialsAlwaysAllowedAndGroupsSurviveRename(t *testing.T) {
	env := newTestEnv(t)
	waitRootUser(t, env, "guest")

	resp, raw := env.request(t, http.MethodPut, "/api/users/rotate",
		map[string]any{
			"password": "pw", "tags": "monitoring",
			"api_groups": []string{"overview"}, "must_change_password": true,
		}, "", "")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("创建账号应 201，实际 %d %s", resp.StatusCode, raw)
	}

	// 该账号只被授权 overview，但改自己的凭据必须仍然可用
	resp, raw = env.request(t, http.MethodPost, "/api/users/rotate/credentials",
		map[string]any{"name": "rotated", "password": "pw2"}, "rotate", "pw")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("自助改凭据应 204（不受功能组收窄），实际 %d %s", resp.StatusCode, raw)
	}

	code, body := whoamiAs(t, env, "rotated", "pw2")
	if code != http.StatusOK {
		t.Fatalf("改凭据后应可登录，实际 %d", code)
	}
	if body["must_change_password"] != false {
		t.Fatalf("改密后不应再要求改密，实际 %v", body["must_change_password"])
	}

	// 功能组随改名迁移，且仍然生效
	var u struct {
		Name      string   `json:"name"`
		APIGroups []string `json:"api_groups"`
	}
	if resp := env.getJSON(t, "/api/users/rotated", &u); resp.StatusCode != http.StatusOK {
		t.Fatalf("GET 改名后的账号应 200，实际 %d", resp.StatusCode)
	}
	if len(u.APIGroups) != 1 || u.APIGroups[0] != "overview" {
		t.Fatalf("功能组应随账号迁移，实际 %v", u.APIGroups)
	}
	if resp, _ := env.request(t, http.MethodGet, "/api/overview", nil, "rotated", "pw2"); resp.StatusCode != http.StatusOK {
		t.Fatalf("改名后 overview 仍应可访问，实际 %d", resp.StatusCode)
	}
	if resp, _ := env.request(t, http.MethodGet, "/api/queues", nil, "rotated", "pw2"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("改名后 topology 仍应被拒，实际 %d", resp.StatusCode)
	}
}
