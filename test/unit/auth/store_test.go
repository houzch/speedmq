package auth_test

import (
	"testing"

	"github.com/houzch/swiftmq/internal/auth"
	"github.com/houzch/swiftmq/internal/config"
)

// 本文件覆盖内置用户表与配置之间的**隔离**语义。
//
// 起因（M8-7 排障时抓到）：NewStore 只做了外层 map 的浅拷贝，用户记录里的
// Permissions map 与 config 共用同一个对象。运行期 ApplyPermission 是原地写入，
// 于是"管理面新增的权限"会渗回 cfg.Users —— 之后首次引导的播种逻辑
// （cluster.go 的 seedConfigUsers，它遍历的正是 cfg.Users 里的权限）
// 会把这条运行期权限当成配置内容重新提交到元数据，
// 表现为"vhost 删掉了、它的权限却自己复活"。
func TestNewStoreIsolatesFromConfig(t *testing.T) {
	cfgUsers := map[string]config.User{
		"guest": {
			Password: "guest",
			Tags:     []string{"administrator"},
			Permissions: map[string]config.Permission{
				"/": {Configure: ".*", Write: ".*", Read: ".*"},
			},
		},
	}

	st := auth.NewStore(cfgUsers)

	// 运行期新增权限：不得出现在配置里。
	if !st.ApplyPermission("guest", "v2", config.Permission{Configure: ".*", Write: ".*", Read: ".*"}) {
		t.Fatalf("ApplyPermission 对已存在的用户应返回 true")
	}
	if _, leaked := cfgUsers["guest"].Permissions["v2"]; leaked {
		t.Fatalf("运行期新增的权限不应写回配置（否则首次引导会把运行期权限重新播种到元数据）")
	}
	if _, ok := st.Permissions("guest", "v2"); !ok {
		t.Fatalf("用户表本身应已记录新增的权限")
	}

	// 运行期删除权限：不得影响配置里的原始记录。
	st.ApplyDeletePermission("guest", "/")
	if _, gone := st.Permissions("guest", "/"); gone {
		t.Fatalf("删除后用户表不应再返回该权限")
	}
	if _, still := cfgUsers["guest"].Permissions["/"]; !still {
		t.Fatalf("删除运行期权限不应改到配置里的原始记录")
	}
}
