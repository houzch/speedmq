// Package plugin 定义 SpeedMQ 对外的稳定插件 API。
//
// 约束：插件只允许 import 本包，内核内部包一律不可见；
// 该约束由 lint 规则（depguard）强制 —— 这样即使不做热加载，也能在编译期守住边界。
package plugin

import (
	"context"
	"log/slog"
)

// APIVersion 是本包当前版本。插件通过 Plugin.APIVersion 声明自己依赖的版本。
// 发生破坏性变更时并行开 v2，v1 至少在若干 minor 内保持不变。
const APIVersion = "v1"

// Capability 是插件向内核申请的能力。未声明的能力被调用时一律拒绝。
type Capability string

const (
	// CapNetListen 允许创建监听端口（协议插件必需）。
	CapNetListen Capability = "net.listen"
	// CapStoreRead 允许读取消息存储。
	CapStoreRead Capability = "store.read"
	// CapStoreWrite 允许写入消息存储。
	CapStoreWrite Capability = "store.write"
	// CapHTTPRoute 允许注册 HTTP 路由（管理类插件）。
	CapHTTPRoute Capability = "http.route"
	// CapClusterMetadataWrite 允许修改集群元数据。
	CapClusterMetadataWrite Capability = "cluster.metadata.write"
	// CapAuthVerify 允许参与认证校验。
	CapAuthVerify Capability = "auth.verify"
)

// Plugin 是所有插件的统一入口。
type Plugin interface {
	// Name 全局唯一，如 "amqp091"、"mqtt"。
	Name() string
	// Version 语义化版本。
	Version() string
	// APIVersion 依赖的插件 API 版本，当前为 v1。
	APIVersion() string
	// Requires 依赖的其他插件名，构成 DAG；内核在加载前做环检测。
	Requires() []string
	// Capabilities 声明所需内核能力，安装/启用时审计。
	Capabilities() []Capability
	// Init 注册阶段：只向内核注册扩展点，不做耗时启动动作。
	Init(Host) error
	// Start 启动插件。调用时其依赖插件已完成 Start。
	Start(context.Context) error
	// Stop 停止插件；必须幂等，可被重复调用。
	Stop(context.Context) error
}

// Host 是内核交给插件的受控句柄：插件只能通过它访问内核。
// M1 只实现下列最小集合；Metrics / Events / 其他 Register* 按里程碑补齐。
type Host interface {
	// PluginName 返回当前宿主对应的插件名。
	PluginName() string
	// Logger 返回带插件名前缀的结构化日志器。
	Logger() *slog.Logger
	// Config 把内核配置中该插件的配置段按 JSON 语义解码到 out。
	Config(out any) error
	// RegisterProtocol 注册一个协议扩展点。
	RegisterProtocol(Protocol) error
}
