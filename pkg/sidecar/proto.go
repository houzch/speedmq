package sidecar

import "encoding/json"

// 本文件定义控制面的消息结构（数据面是原始字节，见 frame.go 的 writeData）。

// Hello 是内核发给插件进程的握手请求。
type Hello struct {
	// Plugin 是内核在配置里声明的插件名（必须与插件自报的名字一致）。
	Plugin string `json:"plugin"`
	// ProtocolVersion 是内核的线协议版本。
	ProtocolVersion string `json:"protocol_version"`
	// KernelVersion 是内核版本，仅用于排查（插件可据此拒绝过旧的内核）。
	KernelVersion string `json:"kernel_version"`
	// APIVersion 是内核支持的插件 API 版本（pkg/plugin.APIVersion）。
	APIVersion string `json:"api_version"`
}

// HelloAck 是插件进程的握手应答。
type HelloAck struct {
	// Name / Version 是插件自报的名字与版本。
	Name    string `json:"name"`
	Version string `json:"version"`
	// APIVersion 是插件实现所依据的插件 API 版本；与内核不一致时内核拒绝加载。
	APIVersion string `json:"api_version"`
	// Capabilities 是插件声明的能力（仅用于审计展示；真正的授权仍在插件进程内自治）。
	Capabilities []string `json:"capabilities"`
	// Protocols 是插件提供的协议名（内核据此建立嗅探与服务入口）。
	Protocols []string `json:"protocols"`
	// Methods 是插件支持的方法名（供运维查看与调用前校验）。
	Methods []string `json:"methods"`
	// Deny 非空表示插件拒绝服务（例如它只支持更高的协议版本），内核据此报错隔离。
	Deny string `json:"deny,omitempty"`
}

// Call 是一次方法调用。
//
// Reverse 区分调用方向：false 是内核 → 插件的**正向**调用（插件实现 Handler.Call 处理），
// true 是插件 → 内核的**反向**调用（内核经 ClientOptions.OnCall 处理）。
type Call struct {
	ID      uint64          `json:"id"`
	Method  string          `json:"method"`
	Reverse bool            `json:"reverse,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Reply 是方法调用的结果。
//
// Reverse 必须回带 Call 的方向：**正向与反向调用的 ID 由两端各自从 1 开始自增，两个 ID 空间会撞车**，
// 又都走在同一条连接上。接收方要据此决定"这条应答去哪个 pending 表唤醒等待者"，
// 否则一个 ID 恰好相同的在途调用会被投给错误的等待者（本实现因此为两个方向各留一张 pending 表）。
type Reply struct {
	ID      uint64          `json:"id"`
	Reverse bool            `json:"reverse,omitempty"`
	OK      bool            `json:"ok"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Open 请求为一条客户端连接打开逻辑流。
type Open struct {
	Stream uint32 `json:"stream"`
	// Remote / Local 是客户端与内核侧的地址，供插件打日志或做访问控制。
	Remote string `json:"remote,omitempty"`
	Local  string `json:"local,omitempty"`
	// Peek 是嗅探阶段读到的前几个字节（可能为空）：插件可据此做更细的分支判断。
	Peek []byte `json:"peek,omitempty"`
	// Attachment 是内核侧随该流绑定的一枚不透明句柄，**不进帧**（json:"-"），插件侧永远是 nil。
	//
	// 内核用它把"流"与"这条连接的 plugin.Core"关联起来：插件随后的反向调用（如 session.open）
	// 带的是流号，内核据此定位到正确的会话。之所以搭在 Open 上，是为了让绑定发生在 kindOpen
	// 帧写出**之前**（见 ClientOptions.OnStreamOpen）——否则插件可能在绑定完成前就发起反向调用。
	Attachment any `json:"-"`
}

// OpenAck 是逻辑流的打开结果。
type OpenAck struct {
	Stream uint32 `json:"stream"`
	OK     bool   `json:"ok"`
	Error  string `json:"error,omitempty"`
}

// Close 关闭一条逻辑流。
type Close struct {
	Stream uint32 `json:"stream"`
	Reason string `json:"reason,omitempty"`
}
