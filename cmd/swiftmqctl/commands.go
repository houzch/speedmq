// 本文件实现 swiftmqctl 的全部子命令与输出渲染。
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// ---------- 数据结构（对齐管理 API 契约） ----------

type objectTotals struct {
	Connections int `json:"connections"`
	Channels    int `json:"channels"`
	Queues      int `json:"queues"`
	Consumers   int `json:"consumers"`
	Exchanges   int `json:"exchanges"`
}

type queueTotals struct {
	Messages               int `json:"messages"`
	MessagesReady          int `json:"messages_ready"`
	MessagesUnacknowledged int `json:"messages_unacknowledged"`
}

type overview struct {
	ManagementVersion string       `json:"management_version"`
	RabbitMQVersion   string       `json:"rabbitmq_version"`
	ProductName       string       `json:"product_name"`
	ProductVersion    string       `json:"product_version"`
	Node              string       `json:"node"`
	ObjectTotals      objectTotals `json:"object_totals"`
	QueueTotals       queueTotals  `json:"queue_totals"`
}

type node struct {
	Name           string   `json:"name"`
	Running        bool     `json:"running"`
	Uptime         int64    `json:"uptime"`
	MemUsed        int64    `json:"mem_used"`
	MemLimit       int64    `json:"mem_limit"`
	DiskFree       int64    `json:"disk_free"`
	DiskFreeLimit  int64    `json:"disk_free_limit"`
	EnabledPlugins []string `json:"enabled_plugins"`
}

type queue struct {
	Name                   string `json:"name"`
	VHost                  string `json:"vhost"`
	Durable                bool   `json:"durable"`
	Type                   string `json:"type"`
	State                  string `json:"state"`
	Messages               int    `json:"messages"`
	MessagesReady          int    `json:"messages_ready"`
	MessagesUnacknowledged int    `json:"messages_unacknowledged"`
	Consumers              int    `json:"consumers"`
}

type connection struct {
	Name        string `json:"name"`
	User        string `json:"user"`
	VHost       string `json:"vhost"`
	Protocol    string `json:"protocol"`
	Channels    int    `json:"channels"`
	State       string `json:"state"`
	ConnectedAt int64  `json:"connected_at"`
}

type exchange struct {
	Name       string `json:"name"`
	VHost      string `json:"vhost"`
	Type       string `json:"type"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Internal   bool   `json:"internal"`
}

type binding struct {
	Source          string `json:"source"`
	VHost           string `json:"vhost"`
	Destination     string `json:"destination"`
	DestinationType string `json:"destination_type"`
	RoutingKey      string `json:"routing_key"`
}

type plugin struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	APIVersion   string   `json:"api_version"`
	State        string   `json:"state"`
	Required     bool     `json:"required"`
	Builtin      bool     `json:"builtin"`
	Capabilities []string `json:"capabilities"`
	Dependencies []string `json:"dependencies"`
	Description  string   `json:"description"`
}

// ---------- 输出辅助 ----------

// decodeJSON 解析管理 API 的 JSON 响应，失败时给出中文错误。
func decodeJSON(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("解析响应 JSON 失败: %w", err)
	}
	return nil
}

// emitJSON 以带缩进的原始 JSON 输出（-json 模式），便于脚本消费与人工阅读。
func (c *client) emitJSON(data []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, data, "", "  "); err != nil {
		// 服务端返回的不是合法 JSON（或为空）时原样输出，避免吞掉信息。
		_, werr := os.Stdout.Write(data)
		return werr
	}
	fmt.Println(buf.String())
	return nil
}

// confirm 用于写操作：-json 模式且服务端有响应体时打印 JSON，否则打印中文确认信息。
func (c *client) confirm(raw []byte, msg string) error {
	if c.jsonOut && len(bytes.TrimSpace(raw)) > 0 {
		return c.emitJSON(raw)
	}
	fmt.Println(msg)
	return nil
}

// writeTable 用 tabwriter 输出列宽自适应的对齐表格。
func writeTable(headers []string, rows [][]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	_ = w.Flush()
}

// esc 对 URL 路径片段做 percent-encode（例如 vhost "/" -> "%2F"）。
func esc(s string) string { return url.PathEscape(s) }

// humanBytes 把字节数格式化为便于阅读的 Ki/Mi/Gi 单位。
func humanBytes(n int64) string {
	if n < 0 {
		return "-"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	v := float64(n)
	i := -1
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	return fmt.Sprintf("%.2f %s", v, units[i])
}

// humanUptime 把**毫秒级**运行时长格式化为中文时长。
//
// 单位是毫秒：RabbitMQ Management API 的 uptime 就是毫秒（与 /api/nodes 对齐），
// 这里必须先换算成秒，否则会把 16 秒显示成 4 小时。
func humanUptime(uptimeMS int64) string {
	sec := uptimeMS / 1000
	if sec < 0 {
		sec = 0
	}
	d := sec / 86400
	sec %= 86400
	h := sec / 3600
	sec %= 3600
	m := sec / 60
	s := sec % 60

	var b strings.Builder
	if d > 0 {
		fmt.Fprintf(&b, "%d天", d)
	}
	if h > 0 {
		fmt.Fprintf(&b, "%d小时", h)
	}
	if m > 0 {
		fmt.Fprintf(&b, "%d分", m)
	}
	fmt.Fprintf(&b, "%d秒", s)
	return b.String()
}

// formatMillis 把毫秒级 Unix 时间戳格式化成本地时间。
func formatMillis(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	return time.UnixMilli(ms).Format("2006-01-02 15:04:05")
}

// joinOrNone 把字符串切片连接为逗号分隔文本，空切片显示"（无）"。
func joinOrNone(items []string) string {
	if len(items) == 0 {
		return "（无）"
	}
	return strings.Join(items, ", ")
}

// ---------- 参数校验辅助 ----------

// optionalArg 处理 [vhost] 这类可选单参数，返回是否提供。
func optionalArg(args []string, usage string) (string, bool, error) {
	switch len(args) {
	case 0:
		return "", false, nil
	case 1:
		return args[0], true, nil
	default:
		return "", false, usagef("用法: %s", usage)
	}
}

// wantArgs 要求参数个数恰好为 n。
func wantArgs(args []string, n int, usage string) error {
	if len(args) != n {
		return usagef("用法: %s", usage)
	}
	return nil
}

// ---------- 命令分发 ----------

func dispatch(c *client, cmd string, args []string) error {
	switch cmd {
	case "status":
		if err := wantArgs(args, 0, "status"); err != nil {
			return err
		}
		return cmdStatus(c)
	case "list_queues":
		vhost, has, err := optionalArg(args, "list_queues [vhost]")
		if err != nil {
			return err
		}
		return cmdListQueues(c, vhost, has)
	case "list_connections":
		if err := wantArgs(args, 0, "list_connections"); err != nil {
			return err
		}
		return cmdListConnections(c)
	case "list_exchanges":
		vhost, has, err := optionalArg(args, "list_exchanges [vhost]")
		if err != nil {
			return err
		}
		return cmdListExchanges(c, vhost, has)
	case "list_bindings":
		vhost, has, err := optionalArg(args, "list_bindings [vhost]")
		if err != nil {
			return err
		}
		return cmdListBindings(c, vhost, has)
	case "add_user":
		if len(args) < 2 || len(args) > 3 {
			return usagef("用法: add_user <name> <password> [tags]")
		}
		tags := "administrator"
		if len(args) == 3 {
			tags = args[2]
		}
		return cmdAddUser(c, args[0], args[1], tags)
	case "set_permissions":
		if err := wantArgs(args, 5, "set_permissions <user> <vhost> <configure> <write> <read>"); err != nil {
			return err
		}
		return cmdSetPermissions(c, args[0], args[1], args[2], args[3], args[4])
	case "close_connection":
		if len(args) < 1 || len(args) > 2 {
			return usagef("用法: close_connection <name> [reason]")
		}
		reason, hasReason := "", false
		if len(args) == 2 {
			reason, hasReason = args[1], true
		}
		return cmdCloseConnection(c, args[0], reason, hasReason)
	case "plugins":
		return cmdPlugins(c, args)
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", cmd)
		printUsage()
		return silentExit{code: 2}
	}
}

// ---------- 1. status ----------

func cmdStatus(c *client) error {
	ovRaw, err := c.get("/api/overview", nil)
	if err != nil {
		return err
	}
	nodesRaw, err := c.get("/api/nodes", nil)
	if err != nil {
		return err
	}

	if c.jsonOut {
		// 合并两个接口的原始 JSON，保持"原始 JSON 输出"的语义。
		b, err := json.Marshal(struct {
			Overview json.RawMessage `json:"overview"`
			Nodes    json.RawMessage `json:"nodes"`
		}{Overview: ovRaw, Nodes: nodesRaw})
		if err != nil {
			return fmt.Errorf("合并概览 JSON 失败: %w", err)
		}
		return c.emitJSON(b)
	}

	var ov overview
	if err := decodeJSON(ovRaw, &ov); err != nil {
		return err
	}
	var nodes []node
	if err := decodeJSON(nodesRaw, &nodes); err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "产品\t%s %s\n", ov.ProductName, ov.ProductVersion)
	if ov.ManagementVersion != "" {
		fmt.Fprintf(w, "管理版本\t%s\n", ov.ManagementVersion)
	}
	fmt.Fprintf(w, "队列消息\t就绪 %d  未确认 %d  合计 %d\n",
		ov.QueueTotals.MessagesReady, ov.QueueTotals.MessagesUnacknowledged, ov.QueueTotals.Messages)
	fmt.Fprintf(w, "对象总数\t连接 %d  通道 %d  队列 %d  消费者 %d  交换机 %d\n",
		ov.ObjectTotals.Connections, ov.ObjectTotals.Channels, ov.ObjectTotals.Queues,
		ov.ObjectTotals.Consumers, ov.ObjectTotals.Exchanges)

	if len(nodes) == 0 {
		fmt.Fprintf(w, "节点\t（无节点信息）\n")
	}
	for _, n := range nodes {
		state := "已停止"
		if n.Running {
			state = "运行中"
		}
		fmt.Fprintf(w, "节点\t%s\n", n.Name)
		fmt.Fprintf(w, "状态\t%s\n", state)
		fmt.Fprintf(w, "版本\t%s\n", ov.ProductVersion)
		fmt.Fprintf(w, "运行时长\t%s\n", humanUptime(n.Uptime))
		fmt.Fprintf(w, "内存\t%s（上限 %s）\n", humanBytes(n.MemUsed), humanBytes(n.MemLimit))
		fmt.Fprintf(w, "磁盘可用\t%s（下限 %s）\n", humanBytes(n.DiskFree), humanBytes(n.DiskFreeLimit))
	}
	return w.Flush()
}

// ---------- 2. list_queues ----------

func cmdListQueues(c *client, vhost string, hasVhost bool) error {
	q := url.Values{}
	if hasVhost {
		q.Set("vhost", vhost)
	}
	data, err := c.get("/api/queues", q)
	if err != nil {
		if isNotFound(err) && hasVhost {
			return fmt.Errorf("未找到 vhost %s", vhost)
		}
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}

	var queues []queue
	if err := decodeJSON(data, &queues); err != nil {
		return err
	}
	// 默认按 vhost、name 排序，保证输出稳定可对比。
	sort.Slice(queues, func(i, j int) bool {
		if queues[i].VHost != queues[j].VHost {
			return queues[i].VHost < queues[j].VHost
		}
		return queues[i].Name < queues[j].Name
	})
	if len(queues) == 0 {
		fmt.Println("（无队列）")
		return nil
	}

	rows := make([][]string, 0, len(queues))
	for _, item := range queues {
		rows = append(rows, []string{
			item.VHost,
			item.Name,
			strconv.Itoa(item.Messages),
			strconv.Itoa(item.MessagesReady),
			strconv.Itoa(item.MessagesUnacknowledged),
			strconv.Itoa(item.Consumers),
			strconv.FormatBool(item.Durable),
			item.Type,
		})
	}
	writeTable([]string{"vhost", "name", "messages", "messages_ready", "messages_unacknowledged", "consumers", "durable", "type"}, rows)
	return nil
}

// ---------- 3. list_connections ----------

func cmdListConnections(c *client) error {
	data, err := c.get("/api/connections", nil)
	if err != nil {
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}

	var conns []connection
	if err := decodeJSON(data, &conns); err != nil {
		return err
	}
	sort.Slice(conns, func(i, j int) bool { return conns[i].Name < conns[j].Name })
	if len(conns) == 0 {
		fmt.Println("（无连接）")
		return nil
	}

	rows := make([][]string, 0, len(conns))
	for _, item := range conns {
		rows = append(rows, []string{
			item.Name,
			item.User,
			item.VHost,
			item.Protocol,
			strconv.Itoa(item.Channels),
			item.State,
			formatMillis(item.ConnectedAt),
		})
	}
	writeTable([]string{"name", "user", "vhost", "protocol", "channels", "state", "connected_at"}, rows)
	return nil
}

// ---------- 4. list_exchanges ----------

func cmdListExchanges(c *client, vhost string, hasVhost bool) error {
	q := url.Values{}
	if hasVhost {
		q.Set("vhost", vhost)
	}
	data, err := c.get("/api/exchanges", q)
	if err != nil {
		if isNotFound(err) && hasVhost {
			return fmt.Errorf("未找到 vhost %s", vhost)
		}
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}

	var exchanges []exchange
	if err := decodeJSON(data, &exchanges); err != nil {
		return err
	}
	sort.Slice(exchanges, func(i, j int) bool {
		if exchanges[i].VHost != exchanges[j].VHost {
			return exchanges[i].VHost < exchanges[j].VHost
		}
		return exchanges[i].Name < exchanges[j].Name
	})
	if len(exchanges) == 0 {
		fmt.Println("（无交换机）")
		return nil
	}

	rows := make([][]string, 0, len(exchanges))
	for _, item := range exchanges {
		rows = append(rows, []string{
			item.VHost,
			item.Name,
			item.Type,
			strconv.FormatBool(item.Durable),
			strconv.FormatBool(item.AutoDelete),
			strconv.FormatBool(item.Internal),
		})
	}
	writeTable([]string{"vhost", "name", "type", "durable", "auto_delete", "internal"}, rows)
	return nil
}

// ---------- 5. list_bindings ----------

func cmdListBindings(c *client, vhost string, hasVhost bool) error {
	path := "/api/bindings"
	if hasVhost {
		path += "/" + esc(vhost)
	}
	data, err := c.get(path, nil)
	if err != nil {
		if isNotFound(err) && hasVhost {
			return fmt.Errorf("未找到 vhost %s", vhost)
		}
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}

	var bindings []binding
	if err := decodeJSON(data, &bindings); err != nil {
		return err
	}
	sort.Slice(bindings, func(i, j int) bool {
		a, b := bindings[i], bindings[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.Destination != b.Destination {
			return a.Destination < b.Destination
		}
		return a.RoutingKey < b.RoutingKey
	})
	if len(bindings) == 0 {
		fmt.Println("（无绑定）")
		return nil
	}

	rows := make([][]string, 0, len(bindings))
	for _, item := range bindings {
		rows = append(rows, []string{
			item.Source,
			item.Destination,
			item.DestinationType,
			item.RoutingKey,
		})
	}
	writeTable([]string{"source", "destination", "destination_type", "routing_key"}, rows)
	return nil
}

// ---------- 6. add_user ----------

func cmdAddUser(c *client, name, password, tags string) error {
	body := map[string]string{"password": password, "tags": tags}
	raw, err := c.put("/api/users/"+esc(name), nil, body)
	if err != nil {
		return err
	}
	return c.confirm(raw, fmt.Sprintf("用户 %s 已创建或更新（tags=%s）", name, tags))
}

// ---------- 7. set_permissions ----------

func cmdSetPermissions(c *client, user, vhost, configure, write, read string) error {
	body := map[string]string{"configure": configure, "write": write, "read": read}
	path := "/api/permissions/" + esc(vhost) + "/" + esc(user)
	raw, err := c.put(path, nil, body)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("未找到 vhost %s 或用户 %s", vhost, user)
		}
		return err
	}
	return c.confirm(raw, fmt.Sprintf("已为用户 %s 在 vhost %s 设置权限：configure=%s write=%s read=%s",
		user, vhost, configure, write, read))
}

// ---------- 8. close_connection ----------

func cmdCloseConnection(c *client, name, reason string, hasReason bool) error {
	q := url.Values{}
	if hasReason {
		q.Set("reason", reason)
	}
	raw, err := c.delete("/api/connections/"+esc(name), q)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("未找到连接 %s", name)
		}
		return err
	}
	return c.confirm(raw, fmt.Sprintf("连接 %s 已关闭", name))
}

// ---------- 9~12. plugins ----------

func cmdPlugins(c *client, args []string) error {
	if len(args) == 0 {
		return usagef("用法: plugins <list|show|enable|disable> [name]")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		if err := wantArgs(rest, 0, "plugins list"); err != nil {
			return err
		}
		return cmdPluginsList(c)
	case "show":
		if err := wantArgs(rest, 1, "plugins show <name>"); err != nil {
			return err
		}
		return cmdPluginsShow(c, rest[0])
	case "enable":
		if err := wantArgs(rest, 1, "plugins enable <name>"); err != nil {
			return err
		}
		return cmdPluginsToggle(c, rest[0], true)
	case "disable":
		if err := wantArgs(rest, 1, "plugins disable <name>"); err != nil {
			return err
		}
		return cmdPluginsToggle(c, rest[0], false)
	default:
		return usagef("未知的 plugins 子命令: %s（可用: list/show/enable/disable）", sub)
	}
}

func cmdPluginsList(c *client) error {
	data, err := c.get("/api/plugins", nil)
	if err != nil {
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}

	var plugins []plugin
	if err := decodeJSON(data, &plugins); err != nil {
		return err
	}
	sort.Slice(plugins, func(i, j int) bool { return plugins[i].Name < plugins[j].Name })
	if len(plugins) == 0 {
		fmt.Println("（无插件）")
		return nil
	}

	rows := make([][]string, 0, len(plugins))
	for _, p := range plugins {
		rows = append(rows, []string{p.Name, p.Version, p.APIVersion, p.State})
	}
	writeTable([]string{"name", "version", "api_version", "state"}, rows)
	return nil
}

func cmdPluginsShow(c *client, name string) error {
	data, err := c.get("/api/plugins/"+esc(name), nil)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("未找到插件 %s", name)
		}
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}

	var p plugin
	if err := decodeJSON(data, &p); err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "name\t%s\n", p.Name)
	fmt.Fprintf(w, "version\t%s\n", p.Version)
	fmt.Fprintf(w, "api_version\t%s\n", p.APIVersion)
	fmt.Fprintf(w, "state\t%s\n", p.State)
	fmt.Fprintf(w, "required\t%s\n", strconv.FormatBool(p.Required))
	fmt.Fprintf(w, "builtin\t%s\n", strconv.FormatBool(p.Builtin))
	fmt.Fprintf(w, "capabilities\t%s\n", joinOrNone(p.Capabilities))
	fmt.Fprintf(w, "dependencies\t%s\n", joinOrNone(p.Dependencies))
	fmt.Fprintf(w, "description\t%s\n", p.Description)
	return w.Flush()
}

func cmdPluginsToggle(c *client, name string, enable bool) error {
	path := "/api/plugins/" + esc(name)
	verb := "禁用"
	if enable {
		path += "/enable"
		verb = "启用"
	} else {
		path += "/disable"
	}
	raw, err := c.put(path, nil, nil)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("未找到插件 %s", name)
		}
		return err
	}
	return c.confirm(raw, fmt.Sprintf("插件 %s 已%s", name, verb))
}
