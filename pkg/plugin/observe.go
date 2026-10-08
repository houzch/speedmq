package plugin

// 本文件定义协议插件向内核上报"可观测性元数据"所用的类型。
//
// 为什么需要它：管理面要展示"连接在跑什么协议、有几个通道、多少未确认消息"，
// 而这些状态只有协议层知道（内核不实现任何具体协议）。
// 上报方式采用**回调快照**而不是高频事件推送 —— 未确认数、消费者数变化极快，
// 逐次上报会把管理开销压到正常收发路径上；管理面只在有人查询时才拉取。

// ConnectionInfo 是连接级元数据快照。
type ConnectionInfo struct {
	// Protocol 是给运维看的协议名，如 "AMQP 0-9-1"。
	Protocol string
	// ClientProperties 是客户端握手时上报的属性表（可空）。
	ClientProperties map[string]any
	// AuthMechanism 是 SASL 机制名，如 "PLAIN"。
	AuthMechanism string
	// FrameMax / ChannelMax / HeartbeatSeconds 是协商结果。
	FrameMax         uint32
	ChannelMax       uint16
	HeartbeatSeconds uint16
	// Channels 是本连接当前的通道快照。
	Channels []ChannelInfo
}

// ChannelInfo 是通道级元数据快照。
type ChannelInfo struct {
	// Number 是 AMQP 通道号。
	Number uint16
	// ConsumerCount 是该通道上的消费者数。
	ConsumerCount int
	// PrefetchCount 是该通道的 prefetch 额度（0 表示不限）。
	PrefetchCount uint16
	// Confirm 表示该通道是否处于发布确认模式。
	Confirm bool
	// Unacked 是该通道未确认的投递数。
	Unacked int
}

// State 是插件在运行期的状态（管理面展示与治理用）。
type State string

const (
	// StateEnabled 插件已启用（其监听与扩展点均在服务）。
	StateEnabled State = "enabled"
	// StateDisabled 插件已停用（内核未为其创建任何监听）。
	StateDisabled State = "disabled"
	// StateFailed 插件初始化或启动失败，已被内核隔离。
	StateFailed State = "failed"
	// StateStopped 插件已停止（内核正在退出）。
	StateStopped State = "stopped"
	// StateDown 插件在**运行期**失联（外部进程插件崩溃、连接断开且尚未恢复）。
	//
	// 与 StateFailed 的区别：failed 是"启动就没起来"（配置/契约问题，重启内核才会重试），
	// down 是"起来过、现在不在"（外部进程没了；内核照常运行，等它回来）。
	// 这个区分对运维很关键：看到 down 应该去拉起插件进程，而不是改配置。
	StateDown State = "down"
)

// StateReporter 由插件**可选**实现：向管理面报告运行期状态。
//
// 为什么是"拉"而不是"推"：状态只被管理面/指标按需读取，推模式要在内核里再加一条
// 状态变更通道与相应的并发处理；拉模式让插件在自己内部维护状态，内核零改动。
// 返回的 state 优先于内核记录的静态状态，但内核已判定为 disabled/stopped 时仍以内核为准
// （运维显式停用的插件不该显示成 down）。
type StateReporter interface {
	// ReportState 返回当前状态与一句话原因（原因可为空）。
	ReportState() (State, string)
}

// Info 是插件元数据与运行状态的快照。
//
// 它同时服务于两处：`speedmqctl plugins list/show` 与管理面 `GET /api/plugins`，
// 因此定义在对外插件 API 包里，避免内核与管理面各写一份。
type Info struct {
	// Name 是插件名，如 "amqp091"。
	Name string
	// Version 是插件版本。
	Version string
	// APIVersion 是插件依赖的插件 API 版本。
	APIVersion string
	// State 是当前运行状态。
	State State
	// Required 为 true 表示该插件启动失败会阻塞内核启动。
	Required bool
	// Builtin 表示该插件随内核编译进来（而非外部进程插件）。
	Builtin bool
	// Capabilities 是插件声明所需的内核能力。
	Capabilities []string
	// Dependencies 是插件依赖的其他插件名。
	Dependencies []string
	// Description 是一句话说明。
	Description string
	// RuntimeNote 是插件自报的运行期状态原因（StateReporter 给出；可为空）。
	RuntimeNote string
}

// Describer 由插件可选实现，向管理面提供一句话描述。
type Describer interface {
	Description() string
}
