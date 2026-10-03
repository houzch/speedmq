// Package sidecar 是内核侧的"外部进程插件（B 形态）宿主"。
//
// 它把 pkg/sidecar 的线协议包装成一个普通的 plugin.Plugin：
// 于是**内核不需要知道自己托管的是外部进程** —— 注册、依赖排序、能力审计、启停、
// 失败隔离、管理面展示全都复用 A 形态那套机制（这正是设计 §10.2 想要的"形态对内核透明"）。
//
// 与 A 形态的边界（**已知限制，见 README**）：
//   - 协议插件的嗅探留在内核侧（接入层职责），其余一切远程；
//   - 客户端字节流通过本机连接**代理**转发给插件进程（没有 fd 传递，跨平台一致）；
//   - 插件除字节流外，还能经**反向调用**触达内核语义（队列 / 路由 / 权限）：
//     内核侧宿主把 plugin.Session 桥成一组 RPC（见 bridge.go），插件在自己的流上
//     session.open 后即可声明/绑定/发布/消费/结算，语义与进程内协议插件完全同一套。
//     代价是每次调用多一次本机 RPC（JSON 编解码 + 内存拷贝）。
package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/houzch/swiftmq/internal/config"

	sdk "github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

// 常量：重连与启动窗口。
const (
	// initTimeout 是"内核启动时等插件进程就绪"的上限。
	//
	// 它必须存在：spawn 模式下进程刚被拉起，监听还没建立；但也必须有上限，
	// 否则一个永远起不来的插件会把内核启动拖死。
	initTimeout = 8 * time.Second
	// retryInterval 是上述窗口内的重试间隔。
	retryInterval = 200 * time.Millisecond
	// minBackoff / maxBackoff 是运行期断线后的重连退避。
	minBackoff = 500 * time.Millisecond
	maxBackoff = 10 * time.Second
)

// ProtocolSpec 是配置里声明的"这个外部插件提供哪个协议"。
//
// 嗅探规则留在内核侧：它决定"入站连接该交给谁"，与插件进程里实现的协议语义无关。
type ProtocolSpec struct {
	// Name 是协议名（全局唯一，参与嗅探优先级）。
	Name string `json:"name"`
	// Prefix 是嗅探前缀（ASCII）：连接的前几个字节等于它即认定为该协议。
	// 为空表示"不参与嗅探"，只在 Listeners 声明的专属端口上服务。
	Prefix string `json:"prefix"`
	// Listeners 是该协议的监听（可被 config.Listeners[name] 覆盖地址）。
	Listeners []ListenerSpec `json:"listeners"`
}

// ListenerSpec 是监听声明。
type ListenerSpec struct {
	Name string `json:"name"`
	Addr string `json:"addr"`
}

// Config 是配置里 `plugins.<name>.sidecar` 段。
type Config struct {
	// Address 是插件进程的地址：tcp://host:port 或 unix:///path。
	Address string `json:"address"`
	// Spawn 是内核代为启动的插件进程命令行（空表示"由外部管理，内核只连不拉"）。
	// 首个元素是可执行文件；内核退出时会终止它。
	Spawn []string `json:"spawn"`
	// Restart 是崩溃后的重启策略："always"（默认）或 "never"。
	//
	// always 的语义是"内核尽力把它拉回来"，这与 dial 模式下的自动重连是同一件事：
	// 对外表现都是"插件短暂 down，随后自己恢复"，运维不需要介入。
	Restart string `json:"restart"`
	// Protocols 声明该插件提供的协议。
	Protocols []ProtocolSpec `json:"protocols"`
	// HandshakeTimeoutSeconds / HeartbeatSeconds 覆盖默认握手与心跳周期（一般不用改）。
	HandshakeTimeoutSeconds int `json:"handshake_timeout_seconds"`
	HeartbeatSeconds        int `json:"heartbeat_seconds"`
}

// Plugin 是内核侧的 sidecar 宿主插件。
type Plugin struct {
	name string
	cfg  Config
	log  *slog.Logger
	// kernelVer 是内核版本，仅用于握手报文。
	kernelVer string

	mu     sync.Mutex
	client *sidecar.Client
	// child 是 spawn 模式下由内核拉起的进程（dial 模式下为 nil）。
	child *exec.Cmd
	state sdk.State
	// reason 是插件自报状态的原因（管理面展示用）。
	reason string
	// ack 是最近一次成功握手时插件自报的元数据（版本/协议/方法）。
	ack sidecar.HelloAck

	// sessMu 保护下列"内核语义桥"的运行时状态（与 mu 分开：桥的操作会等 RPC/落盘，
	// 不能占着状态查询用的 mu）。
	sessMu sync.Mutex
	// streams 按流号记录该流的内核操作面与已打开的会话（见 bridge.go）。
	streams map[uint32]*bridgeStream
	// deliveries 记录已回推给插件、尚未结算的投递（按投递编号全局唯一）。
	deliveries map[uint64]*pendingDelivery
	// nextDelivery 是投递编号的来源。
	nextDelivery uint64

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

var (
	_ sdk.Plugin        = (*Plugin)(nil)
	_ sdk.Describer     = (*Plugin)(nil)
	_ sdk.StateReporter = (*Plugin)(nil)
)

// FromConfig 扫描配置里带 `sidecar` 段的插件声明并构造宿主插件。
//
// kernelVersion 只用于握手报文（插件可据此拒绝过旧的内核）；
// 之所以由调用方传入而不是在这里 import 内核版本常量：本包属于插件运行时，
// 不该依赖 broker 包（依赖方向见 AGENTS.md §2）。
func FromConfig(cfg *config.Config, log *slog.Logger, kernelVersion string) ([]sdk.Plugin, error) {
	names := make([]string, 0, len(cfg.Plugins))
	for name := range cfg.Plugins {
		names = append(names, name)
	}
	sort.Strings(names)

	var out []sdk.Plugin
	for _, name := range names {
		raw := cfg.Plugins[name]
		var probe struct {
			Sidecar *Config `json:"sidecar"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			return nil, fmt.Errorf("插件 %s 配置解析失败: %w", name, err)
		}
		if probe.Sidecar == nil {
			continue
		}
		sc := *probe.Sidecar
		if sc.Address == "" {
			return nil, fmt.Errorf("插件 %s 声明了 sidecar 但缺少 address", name)
		}
		if sc.Restart == "" {
			sc.Restart = "always"
		}
		if sc.Restart != "always" && sc.Restart != "never" {
			return nil, fmt.Errorf("插件 %s 的 sidecar.restart 只能是 always 或 never，实际 %q", name, sc.Restart)
		}
		if len(sc.Spawn) > 0 && sc.Spawn[0] == "" {
			return nil, fmt.Errorf("插件 %s 的 sidecar.spawn 首个元素必须是可执行文件", name)
		}
		out = append(out, &Plugin{
			name:       name,
			cfg:        sc,
			log:        log.With("plugin", name, "form", "sidecar"),
			kernelVer:  kernelVersion,
			state:      sdk.StateDown,
			reason:     "尚未连接",
			streams:    map[uint32]*bridgeStream{},
			deliveries: map[uint64]*pendingDelivery{},
			stopCh:     make(chan struct{}),
		})
	}
	return out, nil
}

// Name 实现 sdk.Plugin。
func (p *Plugin) Name() string { return p.name }

// Version 实现 sdk.Plugin：宿主版本跟随插件自报的版本（未连接时为空）。
func (p *Plugin) Version() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ack.Version == "" {
		return "unknown"
	}
	return p.ack.Version
}

// APIVersion 实现 sdk.Plugin：外部插件自报的 API 版本（握手时已校验）。
func (p *Plugin) APIVersion() string { return sdk.APIVersion }

// Requires 实现 sdk.Plugin：外部插件不参与内核的依赖排序（它的依赖由它自己解决）。
func (p *Plugin) Requires() []string { return nil }

// Capabilities 实现 sdk.Plugin：需要内核为它创建协议监听。
func (p *Plugin) Capabilities() []sdk.Capability { return []sdk.Capability{sdk.CapNetListen} }

// Description 实现 sdk.Describer。
func (p *Plugin) Description() string {
	return fmt.Sprintf("外部进程插件（sidecar %s）", p.cfg.Address)
}

// ReportState 实现 sdk.StateReporter：状态由连接是否存活决定。
func (p *Plugin) ReportState() (sdk.State, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state, p.reason
}

// Init 实现 sdk.Plugin：握手一次（失败即视为插件不可用，交由内核隔离），并注册协议。
func (p *Plugin) Init(h sdk.Host) error {
	p.log = h.Logger()
	ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
	defer cancel()
	if err := p.connectWithRetry(ctx); err != nil {
		return err
	}
	for _, spec := range p.cfg.Protocols {
		if spec.Name == "" {
			return errors.New("sidecar.protocols 中的协议名不能为空")
		}
		if err := h.RegisterProtocol(&protocolHost{p: p, spec: spec}); err != nil {
			return err
		}
	}
	p.log.Info("外部插件已接入", "address", p.cfg.Address,
		"protocols", p.ack.Protocols, "methods", p.ack.Methods, "spawn", len(p.cfg.Spawn) > 0)
	return nil
}

// Start 实现 sdk.Plugin：启动监督循环（断线重连 / 崩溃重启）。
func (p *Plugin) Start(context.Context) error {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.supervise()
	}()
	return nil
}

// Stop 实现 sdk.Plugin：停止监督、断开连接、终止由内核拉起的进程。可重复调用。
func (p *Plugin) Stop(context.Context) error {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.mu.Lock()
	c := p.client
	p.client = nil
	p.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
	p.wg.Wait()
	// 连接关闭后回收桥上的会话与未结算投递。
	p.resetStreams()
	p.killChild()
	return nil
}

// Client 返回当前可用的连接（不可用时返回 nil）。
func (p *Plugin) Client() *sidecar.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

// ---------------------------------------------------------------------------
// 连接与监督
// ---------------------------------------------------------------------------

// connectWithRetry 在给定窗口内反复尝试连接：spawn 模式下进程刚起来时需要等它监听。
func (p *Plugin) connectWithRetry(ctx context.Context) error {
	var lastErr error
	for {
		if err := p.connect(ctx); err != nil {
			lastErr = err
		} else {
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			p.setState(sdk.StateFailed, lastErr.Error())
			return fmt.Errorf("连接外部插件 %s 失败: %w", p.name, lastErr)
		case <-time.After(retryInterval):
		}
	}
}

// connect 建立一条新连接（含 spawn 与握手）。
func (p *Plugin) connect(ctx context.Context) error {
	if err := p.spawn(); err != nil {
		return err
	}
	opts := sidecar.ClientOptions{
		Logger:       &slogLogger{log: p.log},
		OnCall:       p.onCall,
		OnStreamOpen: p.onStreamOpen,
	}
	if p.cfg.HandshakeTimeoutSeconds > 0 {
		opts.HandshakeTimeout = time.Duration(p.cfg.HandshakeTimeoutSeconds) * time.Second
	}
	if p.cfg.HeartbeatSeconds != 0 {
		opts.Heartbeat = time.Duration(p.cfg.HeartbeatSeconds) * time.Second
	}
	c, err := sidecar.Dial(ctx, p.cfg.Address, sidecar.Hello{
		Plugin:        p.name,
		KernelVersion: p.kernelVer,
		APIVersion:    sdk.APIVersion,
	}, opts)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.client = c
	p.ack = c.Ack()
	p.state = sdk.StateEnabled
	p.reason = ""
	p.mu.Unlock()
	return nil
}

// supervise 是"插件进程活着吗"的唯一裁决者：连接断开 → 标记 down → 退避重连/重启。
func (p *Plugin) supervise() {
	backoff := minBackoff
	for {
		select {
		case <-p.stopCh:
			return
		default:
		}
		c := p.Client()
		if c == nil {
			ctx, cancel := context.WithTimeout(context.Background(), initTimeout)
			err := p.connect(ctx)
			cancel()
			if err != nil {
				p.setState(sdk.StateDown, err.Error())
				p.log.Warn("外部插件不可用，稍后重试", "err", err, "backoff", backoff)
				if !p.sleep(backoff) {
					return
				}
				backoff = nextBackoff(backoff)
				continue
			}
			backoff = minBackoff
			continue
		}
		select {
		case <-p.stopCh:
			return
		case <-c.Done():
			reason := "连接已断开"
			if err := c.Err(); err != nil {
				reason = err.Error()
			}
			p.mu.Lock()
			if p.client == c {
				p.client = nil
				p.state = sdk.StateDown
				p.reason = reason
			}
			p.mu.Unlock()
			// 连接已死：回收桥上的会话与未结算投递（未结算的按"回队"处理，避免消息滞留）。
			p.resetStreams()
			// 内核进程不因插件崩溃而做任何事：只记录、标记 down，然后按策略重连/重启。
			p.log.Warn("外部插件已断开（内核不受影响）", "err", reason, "restart", p.cfg.Restart)
		}
		if p.cfg.Restart == "never" {
			// 策略为 never：断开后不再自动恢复（既不重连也不重新拉起进程），
			// 等运维介入；内核其余部分照常运行。
			<-p.stopCh
			return
		}
		if !p.sleep(backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

// sleep 在可被 stopCh 打断的前提下等待，返回 false 表示已停止。
func (p *Plugin) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-p.stopCh:
		return false
	case <-t.C:
		return true
	}
}

func (p *Plugin) setState(state sdk.State, reason string) {
	p.mu.Lock()
	p.state = state
	p.reason = reason
	p.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 进程托管（spawn 模式）
// ---------------------------------------------------------------------------

// spawn 按配置拉起插件进程。已运行时不重复拉起。
//
// 进程的 stdout/stderr 会被转发到内核日志（带 plugin 标签），
// 这是"外部进程插件还能被运维看见"的关键：否则它的日志会消失在终端里。
//
// 刻意不用 exec.CommandContext：启动上下文在 connect 返回后就会被取消，
// 那会把刚拉起来的进程一起杀掉。进程的回收交给 killChild（内核退出时调用）。
func (p *Plugin) spawn() error {
	if len(p.cfg.Spawn) == 0 {
		return nil
	}
	p.mu.Lock()
	already := p.child != nil
	p.mu.Unlock()
	if already {
		return nil
	}
	cmd := exec.Command(p.cfg.Spawn[0], p.cfg.Spawn[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("接管插件进程 stdout 失败: %w", err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("启动插件进程 %q 失败: %w", strings.Join(p.cfg.Spawn, " "), err)
	}
	p.mu.Lock()
	p.child = cmd
	p.mu.Unlock()
	p.log.Info("已拉起外部插件进程", "cmd", strings.Join(p.cfg.Spawn, " "), "pid", cmd.Process.Pid)
	go p.pumpOutput(stdout)
	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		if p.child == cmd {
			p.child = nil
		}
		restart := p.cfg.Restart
		p.mu.Unlock()
		p.log.Warn("外部插件进程已退出", "pid", cmd.Process.Pid, "err", err, "restart", restart)
	}()
	return nil
}

// killChild 终止由内核拉起的进程（内核退出时调用）。
func (p *Plugin) killChild() {
	p.mu.Lock()
	cmd := p.child
	p.child = nil
	p.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Kill(); err != nil {
		p.log.Warn("终止插件进程失败", "pid", cmd.Process.Pid, "err", err)
	}
}

// pumpOutput 把插件进程的输出按行转进内核日志。
func (p *Plugin) pumpOutput(r io.Reader) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 1024)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			for {
				idx := bytes.IndexByte(buf, '\n')
				if idx < 0 {
					break
				}
				line := strings.TrimRight(string(buf[:idx]), "\r")
				if line != "" {
					p.log.Info("插件进程输出", "line", line)
				}
				buf = buf[idx+1:]
			}
			if len(buf) > 64*1024 {
				buf = buf[:0]
			}
		}
		if err != nil {
			if len(buf) > 0 {
				p.log.Info("插件进程输出", "line", string(buf))
			}
			return
		}
	}
}

// slogLogger 把 pkg/sidecar 的最小日志接口接到内核的结构化日志上。
type slogLogger struct{ log *slog.Logger }

func (l *slogLogger) Info(msg string, args ...any) { l.log.Info(msg, args...) }
func (l *slogLogger) Warn(msg string, args ...any) { l.log.Warn(msg, args...) }

// ---------------------------------------------------------------------------
// 协议侧（内核只负责嗅探与代理）
// ---------------------------------------------------------------------------

// protocolHost 实现 sdk.Protocol：把内核收到的客户端连接代理给插件进程。
type protocolHost struct {
	p    *Plugin
	spec ProtocolSpec
}

var _ sdk.Protocol = (*protocolHost)(nil)

// Name 实现 sdk.Protocol。
func (ph *protocolHost) Name() string { return ph.spec.Name }

// DefaultListeners 实现 sdk.Protocol。
func (ph *protocolHost) DefaultListeners() []sdk.ListenerSpec {
	out := make([]sdk.ListenerSpec, 0, len(ph.spec.Listeners))
	for _, l := range ph.spec.Listeners {
		out = append(out, sdk.ListenerSpec{Name: l.Name, Addr: l.Addr})
	}
	return out
}

// Sniff 实现 sdk.Protocol：按配置声明的前缀匹配（空前缀表示不参与嗅探）。
func (ph *protocolHost) Sniff(peek []byte) bool {
	if ph.spec.Prefix == "" {
		return false
	}
	return strings.HasPrefix(string(peek), ph.spec.Prefix)
}

// Serve 实现 sdk.Protocol：把这条连接的双向字节流代理给插件进程。
//
// 注意：**内核在这里不做任何协议解析**。内核只当"搬运工"，协议语义完全在插件进程里 ——
// 这正是 B 形态的意义（插件可以用任何语言、任何实现），也是它的代价（多一次本机拷贝）。
//
// 与纯代理不同的是：这里把 core（这条连接的内核操作面）随流一并交给插件侧，
// 于是插件可以经反向调用（session.*）触达内核语义（队列/路由/权限），而不只是读写字节。
func (ph *protocolHost) Serve(ctx context.Context, conn net.Conn, core sdk.Core) error {
	c := ph.p.Client()
	if c == nil {
		_, reason := ph.p.ReportState()
		return fmt.Errorf("外部插件 %s 当前不可用（%s）", ph.p.name, reason)
	}
	stream, err := c.Open(ctx, sidecar.Open{
		Remote: addrString(conn.RemoteAddr()),
		Local:  addrString(conn.LocalAddr()),
		// 随流携带内核操作面与客户端地址（不进帧）：前者供插件反向调用时定位会话，
		// 后者供 core.authenticate 按真实来源判定 remote_access。
		Attachment: streamAttachment{core: core, remote: conn.RemoteAddr()},
	})
	if err != nil {
		// Open 失败时拿不到流号，用 core 身份反查回收可能已绑定的桥状态。
		ph.p.releaseCore(core)
		return fmt.Errorf("请求插件 %s 打开连接失败: %w", ph.p.name, err)
	}
	// 流结束：释放该流的会话、取消其消费者，并把未结算投递重新入队。
	defer ph.p.releaseStream(stream.ID())
	defer stream.Close()

	// 双向转发：任一方向结束即收尾（对端关闭 / 插件返回 / 连接断开）。
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(stream, conn)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(conn, stream)
		done <- struct{}{}
	}()
	select {
	case <-ctx.Done():
	case <-c.Done():
	case <-done:
	}
	return nil
}

func addrString(a net.Addr) string {
	if a == nil {
		return ""
	}
	return a.String()
}
