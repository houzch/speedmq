package broker

import (
	"regexp"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// permissionSet 是编译后的权限正则。
//
// 语义对齐 RabbitMQ：值是对资源名的正则，匹配成功即允许；编译失败视为拒绝。
type permissionSet struct {
	configure permissionRule
	write     permissionRule
	read      permissionRule
	user      string
}

// permissionRule 是一条编译后的权限规则。
//
// all 是**匹配一切**的快路径：默认权限就是 ".*"，而权限检查在每条消息的发布/投递路径上，
// 让 "允许一切" 这种最常见的情况去跑正则引擎是纯粹的开销（基准里占约两成 CPU）。
type permissionRule struct {
	re  *regexp.Regexp
	all bool
}

func (r permissionRule) allows(resource string) bool {
	if r.all {
		return true
	}
	return r.re != nil && r.re.MatchString(resource)
}

func newPermissionSet(p config.Permission) (*permissionSet, error) {
	// 与 RabbitMQ 一致：正则**不做隐式锚定**，是在资源名上做"包含匹配"。
	// 因此 ".*" 匹配一切，而想精确限定前缀必须自己写 "^app\\."。
	// 若在这里补上 ^...$，用户写的 "^app\\." 会变成"必须以 app. 结尾"而失效，
	// 造成"配置看起来对、权限却全部拒绝"的隐蔽故障。
	compile := func(pattern string) (permissionRule, error) {
		switch pattern {
		case "":
			return permissionRule{}, nil // 空表示不匹配任何资源
		case ".*", "^.*$":
			// 与正则语义完全等价，但不必进正则引擎。
			return permissionRule{all: true}, nil
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return permissionRule{}, err
		}
		return permissionRule{re: re}, nil
	}
	configure, err := compile(p.Configure)
	if err != nil {
		return nil, err
	}
	write, err := compile(p.Write)
	if err != nil {
		return nil, err
	}
	read, err := compile(p.Read)
	if err != nil {
		return nil, err
	}
	return &permissionSet{configure: configure, write: write, read: read}, nil
}

// allowConfigure 检查拓扑声明/删除权限。
func (s *permissionSet) allowConfigure(resource string) error {
	return s.check(s.configure, "configure", resource)
}

// allowWrite 检查写入权限（发布、绑定）。
func (s *permissionSet) allowWrite(resource string) error {
	return s.check(s.write, "write", resource)
}

// allowRead 检查读取权限（消费、拉取）。
func (s *permissionSet) allowRead(resource string) error {
	return s.check(s.read, "read", resource)
}

func (s *permissionSet) check(rule permissionRule, action, resource string) error {
	if s == nil || rule.allows(resource) {
		return nil
	}
	return plugin.Errorf(plugin.KindAccessRefused,
		"ACCESS_REFUSED - access to %s '%s' refused for user '%s'", action, resource, s.user)
}
