// Package management 是实现"可运维"的那一层：RabbitMQ 兼容的管理 HTTP API、
// Prometheus 指标、以及内嵌的管理 UI 静态服务。
//
// 两条设计约束（对齐设计 9.1 / 9.3 / 10.1）：
//  1. 管理 API 属于**兼容性契约**，不插件化：运维工具链（rabbitmqadmin、监控脚本、
//     管理 UI）都直接指向它，一旦可插拔，"能不能管"就成了可配置项。
//  2. UI 只消费 `/api/*`，不引入任何私有接口 —— 于是 UI 本身就是 API 兼容性的
//     持续验证者（同一套接口也给 rabbitmqadmin 用）。
package management

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/houzch/swiftmq/internal/broker"
	"github.com/houzch/swiftmq/internal/transport"
	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// PluginController 是管理面对插件治理的能力需求（由 internal/plugin.Manager 实现）。
type PluginController interface {
	// Plugins 返回全部插件的元数据与状态。
	Plugins() []sdk.Info
	// Plugin 返回单个插件。
	Plugin(name string) (sdk.Info, bool)
	// Enable 热启用插件。
	Enable(name string) error
	// Disable 热停用插件。
	Disable(name string) error
}

// Deps 是管理面的依赖集合。
type Deps struct {
	// Addr 是监听地址，如 ":15672"。
	Addr string
	// TLS 非空时管理面走 HTTPS（由 cmd 从配置构造，证书在启动时校验）。
	TLS *tls.Config
	// Broker 提供对象视图与运行期管理操作。
	Broker *broker.Broker
	// Plugins 提供插件治理能力。
	Plugins PluginController
	// Listeners 返回接入层的监听快照（/api/overview 的 listeners 字段）。
	Listeners func() []transport.ListenerSnapshot
	// Version 是本节点版本。
	Version string
	// NodeName 是节点名，如 "swiftmq@host"。
	NodeName string
	// StartedAt 是进程启动时刻（uptime 计算用）。
	StartedAt time.Time
	// UI 是管理 UI 的构建产物文件系统（web.Dist）。
	UI fs.FS
}

// Server 是管理面 HTTP 服务。
type Server struct {
	log    *slog.Logger
	deps   Deps
	routes []route
	static http.Handler

	ln   net.Listener
	http *http.Server
}

// New 构造管理面服务。
func New(log *slog.Logger, deps Deps) (*Server, error) {
	s := &Server{log: log.With("component", "management"), deps: deps}
	if deps.UI != nil {
		sub, err := fs.Sub(deps.UI, "dist")
		if err != nil {
			return nil, fmt.Errorf("管理 UI 资源不可用: %w", err)
		}
		s.static = staticHandler(sub)
	}
	s.registerRoutes()
	return s, nil
}

// Start 开始监听并提供服务。
func (s *Server) Start() error {
	var (
		ln  net.Listener
		err error
	)
	if s.deps.TLS != nil {
		ln, err = tls.Listen("tcp", s.deps.Addr, s.deps.TLS)
	} else {
		ln, err = net.Listen("tcp", s.deps.Addr)
	}
	if err != nil {
		return fmt.Errorf("管理面监听 %s 失败: %w", s.deps.Addr, err)
	}
	s.ln = ln
	s.http = &http.Server{
		Handler:           http.HandlerFunc(s.serveHTTP),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := s.http.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("管理面服务异常退出", "err", err)
		}
	}()
	s.log.Info("管理面已启动", "addr", ln.Addr().String(), "api", "/api/overview", "ui", "/")
	return nil
}

// Shutdown 优雅关闭。
func (s *Server) Shutdown(ctx context.Context) {
	if s.http == nil {
		return
	}
	_ = s.http.Shutdown(ctx)
}

// Addr 返回实际监听地址（测试用；未启动时为空）。
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// ---------------------------------------------------------------------------
// 路由：按 "/" 切段并逐段 percent-decode
// ---------------------------------------------------------------------------

type handlerFunc func(w http.ResponseWriter, r *http.Request, p params, au authUser)

// params 是路径参数（已 percent-decode）。
type params map[string]string

type route struct {
	method   string
	segments []string
	handler  handlerFunc
}

func (s *Server) handle(method, pattern string, h handlerFunc) {
	s.routes = append(s.routes, route{
		method:   method,
		segments: splitPath(pattern),
		handler:  h,
	})
}

// splitPath 把 "/api/queues/{vhost}/{name}" 切成 ["api","queues","{vhost}","{name}"]。
func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// splitRequestPath 切分请求路径并逐段 percent-decode。
//
// 必须自己解码而不能用 r.URL.Path：默认 vhost 是 "/"，客户端按 RabbitMQ 约定
// 发 `%2F`，而 net/http 已经把它解码成 "/"，于是路径段数会凭空多出一段，
// 路由与参数全错位。这里改用 EscapedPath() 保序切分后再逐段解码。
func splitRequestPath(r *http.Request) ([]string, error) {
	escaped := r.URL.EscapedPath()
	escaped = strings.Trim(escaped, "/")
	if escaped == "" {
		return nil, nil
	}
	parts := strings.Split(escaped, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		decoded, err := decodePathSegment(part)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

// match 返回匹配的处理器与路径参数。
func (s *Server) match(method string, segments []string) (handlerFunc, params, bool, bool) {
	pathMatched := false
	for _, rt := range s.routes {
		p, ok := matchSegments(rt.segments, segments)
		if !ok {
			continue
		}
		pathMatched = true
		if rt.method == method {
			return rt.handler, p, true, true
		}
	}
	return nil, nil, false, pathMatched
}

func matchSegments(pattern, actual []string) (params, bool) {
	if len(pattern) != len(actual) {
		return nil, false
	}
	p := params{}
	for i, seg := range pattern {
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			p[seg[1:len(seg)-1]] = actual[i]
			continue
		}
		if seg != actual[i] {
			return nil, false
		}
	}
	return p, true
}

// ---------------------------------------------------------------------------
// 入口：认证 → 路由 → 处理器
// ---------------------------------------------------------------------------

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	segments, err := splitRequestPath(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", "路径编码非法: "+err.Error())
		return
	}

	// 管理面路由分流：/api/* 与 /metrics 走 API，其余交给内嵌 UI。
	isAPI := len(segments) > 0 && segments[0] == "api"
	isMetrics := len(segments) == 1 && segments[0] == "metrics"
	if !isAPI && !isMetrics {
		s.serveStatic(w, r)
		return
	}

	user, err := s.authenticate(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Basic realm="SwiftMQ Management"`)
		writeError(w, http.StatusUnauthorized, "Unauthorized", err.Error())
		return
	}

	handler, p, ok, pathMatched := s.match(r.Method, segments)
	if !ok {
		if pathMatched {
			w.Header().Set("Allow", s.allowedMethods(segments))
			writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed",
				fmt.Sprintf("方法 %s 不适用于 %s", r.Method, r.URL.Path))
			return
		}
		writeError(w, http.StatusNotFound, "Object Not Found",
			"未找到管理接口 "+r.URL.Path)
		return
	}
	handler(w, r, p, user)
}

func (s *Server) allowedMethods(segments []string) string {
	seen := map[string]bool{}
	var out []string
	for _, rt := range s.routes {
		if _, ok := matchSegments(rt.segments, segments); ok && !seen[rt.method] {
			seen[rt.method] = true
			out = append(out, rt.method)
		}
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// authUser 是认证后的调用方。
type authUser struct {
	Name string
	Tags []string
	// AllVHosts 表示该用户可以看见全部 vhost（administrator / monitoring）。
	AllVHosts bool
	// VHosts 是该用户有权限记录的 vhost 集合。
	VHosts map[string]struct{}
}

// authenticate 校验 Basic Auth，并计算该用户可访问的 vhost 集合。
func (s *Server) authenticate(r *http.Request) (authUser, error) {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return authUser{}, fmt.Errorf("请提供用户名与口令（HTTP Basic Auth）")
	}
	snap, err := s.deps.Broker.VerifyUser(user, pass, remoteAddr(r))
	if err != nil {
		return authUser{}, err
	}
	au := authUser{Name: snap.Name, Tags: snap.Tags, VHosts: map[string]struct{}{}}
	for _, tag := range snap.Tags {
		switch tag {
		case "administrator", "monitoring":
			au.AllVHosts = true
		}
	}
	// 可见 vhost：与其"权限记录"一致，避免管理面泄露用户无权访问的 vhost 名。
	for _, p := range s.deps.Broker.PermissionSnapshots() {
		if p.User == snap.Name {
			au.VHosts[p.VHost] = struct{}{}
		}
	}
	return au, nil
}

func remoteAddr(r *http.Request) net.Addr {
	host, port, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return &net.TCPAddr{IP: net.ParseIP(r.RemoteAddr)}
	}
	p, _ := net.LookupPort("tcp", port)
	return &net.TCPAddr{IP: net.ParseIP(host), Port: p}
}

// ---------------------------------------------------------------------------
// 授权
// ---------------------------------------------------------------------------

// requireRead 校验管理面读权限（admin / management / monitoring）。
func (au authUser) requireRead() error {
	return au.requireTag("administrator", "management", "monitoring")
}

// requireWrite 校验管理面写权限（admin / management）。
func (au authUser) requireWrite() error {
	return au.requireTag("administrator", "management")
}

// requireAdministrator 校验 administrator 标签。
//
// 用于**全局**对象（vhost、用户）的增删：它们改变的是"谁能连到哪里、谁有什么权力"，
// 与"某个 vhost 内能不能写"不是一回事，因此不允许 management 标签代劳。
func (au authUser) requireAdministrator() error {
	return au.requireTag("administrator")
}

func (au authUser) requireTag(allow ...string) error {
	for _, tag := range au.Tags {
		for _, want := range allow {
			if tag == want {
				return nil
			}
		}
	}
	return fmt.Errorf("ACCESS_REFUSED - 用户 %s 缺少管理面所需的标签（需要 %s 之一）",
		au.Name, strings.Join(allow, " / "))
}

// canSeeVHost 判断调用方是否可见某个 vhost。
func (s *Server) canSeeVHost(au authUser, vhost string) bool {
	if au.AllVHosts {
		return true
	}
	_, ok := au.VHosts[vhost]
	return ok
}

// ---------------------------------------------------------------------------
// 响应工具
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(body); err != nil {
		// 响应头已发出，只能记日志（由调用方保证 body 可序列化）
		return
	}
}

// writeError 输出 RabbitMQ 风格的错误体：{"error":..., "reason":...}。
func writeError(w http.ResponseWriter, status int, short, reason string) {
	writeJSON(w, status, map[string]string{"error": short, "reason": reason})
}

// decodeBody 解析 JSON 请求体；空体返回零值。
func decodeBody(r *http.Request, out any) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("请求体不是合法 JSON: %w", err)
	}
	return nil
}

// readError 把内核/权限错误映射成 HTTP 状态码与错误体。
//
// 映射规则对齐 RabbitMQ 的可观察行为：权限 → 403、不存在 → 404、参数不符 → 400。
func writeKernelError(w http.ResponseWriter, err error) {
	text := err.Error()
	switch {
	case strings.Contains(text, "ACCESS_REFUSED"), strings.Contains(text, "缺少管理面所需"):
		writeError(w, http.StatusForbidden, "Access refused", text)
	case strings.Contains(text, "NOT_IMPLEMENTED"):
		writeError(w, http.StatusNotImplemented, "Not Implemented", text)
	case strings.Contains(text, "无法提交"), strings.Contains(text, "无法转发"),
		strings.Contains(text, "未获知 leader"), strings.Contains(text, "已不是 leader"):
		// 集群暂时没有 leader（选主中 / 与多数派失联）：让客户端换节点重试，而不是当成参数错误。
		writeError(w, http.StatusServiceUnavailable, "Service Unavailable", text)
	case strings.Contains(text, "NOT_FOUND"), strings.Contains(text, "不存在"), strings.Contains(text, "not found"):
		writeError(w, http.StatusNotFound, "Object Not Found", text)
	case strings.Contains(text, "PRECONDITION_FAILED"), strings.Contains(text, "不能为空"), strings.Contains(text, "非法"):
		writeError(w, http.StatusBadRequest, "Precondition Failed", text)
	case strings.Contains(text, "资源水位"):
		writeError(w, http.StatusServiceUnavailable, "Resource Blocked", text)
	default:
		writeError(w, http.StatusBadRequest, "Bad Request", text)
	}
}

// decodePathSegment 解码单个路径段。
func decodePathSegment(s string) (string, error) {
	if !strings.Contains(s, "%") {
		return s, nil
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			b.WriteByte(s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", fmt.Errorf("百分号转义不完整: %q", s)
		}
		hi, ok1 := hexVal(s[i+1])
		lo, ok2 := hexVal(s[i+2])
		if !ok1 || !ok2 {
			return "", fmt.Errorf("百分号转义非法: %q", s)
		}
		b.WriteByte(hi<<4 | lo)
		i += 2
	}
	return b.String(), nil
}

func hexVal(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}
