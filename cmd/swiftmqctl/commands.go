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

// clusterStatus 是 GET /api/cluster 的响应（由内核的 meta.Status 组装）。
type clusterStatus struct {
	Enabled        bool     `json:"enabled"`
	Mode           string   `json:"mode"`
	NodeID         string   `json:"node_id"`
	Role           string   `json:"role"`
	Term           uint64   `json:"term"`
	Leader         string   `json:"leader"`
	HasQuorum      bool     `json:"has_quorum"`
	Paused         bool     `json:"paused"`
	Peers          []string `json:"peers"`
	Learners       []string `json:"learners"`
	CommitIndex    uint64   `json:"commit_index"`
	LastApplied    uint64   `json:"last_applied"`
	AppliedRecords uint64   `json:"applied_records"`
	ObjectTotals   struct {
		Queues    int `json:"queues"`
		Exchanges int `json:"exchanges"`
		Bindings  int `json:"bindings"`
		Users     int `json:"users"`
	} `json:"object_totals"`
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

// vhost 是 GET /api/vhosts 的元素（只取展示需要的字段）。
type vhost struct {
	Name                   string `json:"name"`
	Messages               int    `json:"messages"`
	MessagesReady          int    `json:"messages_ready"`
	MessagesUnacknowledged int    `json:"messages_unacknowledged"`
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
	// RuntimeNote 是插件自报的运行期状态原因（如外部进程插件 down 的原因、
	// 被隔离的失败原因）；为空表示无特别说明。
	RuntimeNote string `json:"runtime_note"`
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
	case "cluster_status":
		if err := wantArgs(args, 0, "cluster_status"); err != nil {
			return err
		}
		return cmdClusterStatus(c)
	case "add_member":
		if err := wantArgs(args, 2, "add_member <node_id> <rpc_addr>"); err != nil {
			return err
		}
		return cmdAddMember(c, args[0], args[1])
	case "remove_member":
		if err := wantArgs(args, 1, "remove_member <node_id>"); err != nil {
			return err
		}
		return cmdRemoveMember(c, args[0])
	case "list_members":
		if err := wantArgs(args, 0, "list_members"); err != nil {
			return err
		}
		return cmdListMembers(c)
	case "list_queues":
		vhost, has, err := optionalArg(args, "list_queues [vhost]")
		if err != nil {
			return err
		}
		return cmdListQueues(c, vhost, has)
	case "grow_queue":
		if err := wantArgs(args, 3, "grow_queue <vhost> <name> <count>"); err != nil {
			return err
		}
		return cmdGrowQueue(c, args[0], args[1], args[2])
	case "rebalance_queue":
		if err := wantArgs(args, 2, "rebalance_queue <vhost> <name>"); err != nil {
			return err
		}
		return cmdRebalanceQueue(c, args[0], args[1])
	case "list_vhosts":
		if err := wantArgs(args, 0, "list_vhosts"); err != nil {
			return err
		}
		return cmdListVHosts(c)
	case "add_vhost":
		if err := wantArgs(args, 1, "add_vhost <name>"); err != nil {
			return err
		}
		return cmdAddVHost(c, args[0])
	case "delete_vhost":
		if err := wantArgs(args, 1, "delete_vhost <name>"); err != nil {
			return err
		}
		return cmdDeleteVHost(c, args[0])
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

// ---------- 2. cluster_status ----------

// cmdClusterStatus 展示本节点在集群中的角色与元数据共识进度。
//
// 单机部署也会输出（mode=local、role=single）：运维因此能用同一条命令
// 确认"这台机器是不是真的在集群里"，而不是靠猜测。
func cmdClusterStatus(c *client) error {
	data, err := c.get("/api/cluster", nil)
	if err != nil {
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}
	var cl clusterStatus
	if err := decodeJSON(data, &cl); err != nil {
		return err
	}

	mode := cl.Mode
	if !cl.Enabled {
		mode = "单机（cluster 未启用）"
	}
	leader := cl.Leader
	if leader == "" {
		leader = "（未选出）"
	}
	quorum, paused := "是", "正常"
	if !cl.HasQuorum {
		quorum = "否"
	}
	if cl.Paused {
		paused = "已暂停（pause_minority：与多数派失联）"
	}
	peers := "（无）"
	if len(cl.Peers) > 0 {
		peers = strings.Join(cl.Peers, ", ")
	}
	learners := "（无）"
	if len(cl.Learners) > 0 {
		learners = strings.Join(cl.Learners, ", ")
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "模式\t%s\n", mode)
	fmt.Fprintf(w, "节点\t%s\n", cl.NodeID)
	fmt.Fprintf(w, "角色\t%s\n", cl.Role)
	fmt.Fprintf(w, "任期\t%d\n", cl.Term)
	fmt.Fprintf(w, "领导者\t%s\n", leader)
	fmt.Fprintf(w, "投票成员\t%s\n", peers)
	fmt.Fprintf(w, "非投票成员\t%s\n", learners)
	fmt.Fprintf(w, "拥有多数派\t%s\n", quorum)
	fmt.Fprintf(w, "服务状态\t%s\n", paused)
	fmt.Fprintf(w, "共识进度\t提交 %d  已应用 %d  累计 %d 条\n",
		cl.CommitIndex, cl.LastApplied, cl.AppliedRecords)
	fmt.Fprintf(w, "元数据规模\t队列 %d  交换机 %d  绑定 %d  用户 %d\n",
		cl.ObjectTotals.Queues, cl.ObjectTotals.Exchanges,
		cl.ObjectTotals.Bindings, cl.ObjectTotals.Users)
	return w.Flush()
}

// ---------- 2b. 集群成员变更（M6d） ----------

// clusterMembers 是 GET /api/cluster/members 的响应。
type clusterMembers struct {
	Voters   []string `json:"voters"`
	Learners []string `json:"learners"`
}

// memberOpMinTimeout 是成员变更命令的最小 HTTP 超时。
//
// 加入一个新成员要等它把日志/快照拉过去，可能远超 CLI 默认的 10s；
// 用默认超时会表现为"命令超时但服务端仍在继续"，让人误以为失败。
const memberOpMinTimeout = 90 * time.Second

// cmdAddMember 把节点加入集群（learner → 追平 → 提升为投票成员）。
//
// 这个命令可能耗时几十秒（等新节点把日志/快照拉过去），因此 CLI 的超时要留足。
func cmdAddMember(c *client, id, addr string) error {
	if c.http.Timeout < memberOpMinTimeout {
		c.http.Timeout = memberOpMinTimeout
	}
	data, err := c.put("/api/cluster/members/"+url.PathEscape(id), nil, map[string]any{"addr": addr})
	if err != nil {
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}
	fmt.Printf("已加入集群成员 %s（%s）\n", id, addr)
	return printMembers(data)
}

// cmdRemoveMember 把节点移出集群。
func cmdRemoveMember(c *client, id string) error {
	data, err := c.delete("/api/cluster/members/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}
	fmt.Printf("已移除集群成员 %s\n", id)
	return printMembers(data)
}

// cmdListMembers 展示当前成员划分。
func cmdListMembers(c *client) error {
	data, err := c.get("/api/cluster/members", nil)
	if err != nil {
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}
	return printMembers(data)
}

func printMembers(data []byte) error {
	var m clusterMembers
	if err := decodeJSON(data, &m); err != nil {
		return err
	}
	voters, learners := "（无）", "（无）"
	if len(m.Voters) > 0 {
		voters = strings.Join(m.Voters, ", ")
	}
	if len(m.Learners) > 0 {
		learners = strings.Join(m.Learners, ", ")
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "投票成员\t%s\n", voters)
	fmt.Fprintf(w, "非投票成员\t%s\n", learners)
	return w.Flush()
}

// ---------- 3. list_queues ----------

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

// ---------- 3a. 仲裁队列副本集（M8-15） ----------

// quorumInfo 是 PUT /api/queues/{vhost}/{name}/grow 的响应。
type quorumInfo struct {
	VHost    string   `json:"vhost"`
	Queue    string   `json:"queue"`
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
	Count    int      `json:"count"`
	Voters   []string `json:"voters"`
	Learners []string `json:"learners"`
}

// rebalanceInfo 是 PUT /api/queues/{vhost}/{name}/rebalance 的响应。
type rebalanceInfo struct {
	VHost  string `json:"vhost"`
	Queue  string `json:"queue"`
	Moved  bool   `json:"moved"`
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason"`
}

// cmdGrowQueue 把一条仲裁队列的副本数扩到 count（只增不减）。
//
// 改组要等新副本追平日志，因此与成员加入同一口径地把 HTTP 超时放宽。
func cmdGrowQueue(c *client, vhost, name, countArg string) error {
	count, err := strconv.Atoi(countArg)
	if err != nil || count <= 0 {
		return usagef("count 必须是大于 0 的整数: %s", countArg)
	}
	if c.http.Timeout < memberOpMinTimeout {
		c.http.Timeout = memberOpMinTimeout
	}
	raw, err := c.put("/api/queues/"+esc(vhost)+"/"+esc(name)+"/grow", nil, map[string]any{"count": count})
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("未找到队列 %s（vhost %s）", name, vhost)
		}
		return err
	}
	if c.jsonOut {
		return c.emitJSON(raw)
	}
	var info quorumInfo
	if err := decodeJSON(raw, &info); err != nil {
		return err
	}
	fmt.Printf("仲裁队列 %s 的副本集已扩到 %d 个：%s\n", name, len(info.Replicas), joinOrNone(info.Replicas))
	fmt.Printf("投票成员：%s\n非投票成员：%s\n服务节点：%s\n",
		joinOrNone(info.Voters), joinOrNone(info.Learners), orDash(info.Leader))
	return nil
}

// cmdRebalanceQueue 把一条仲裁队列的 leader 迁到副本集中较空的节点。
func cmdRebalanceQueue(c *client, vhost, name string) error {
	raw, err := c.put("/api/queues/"+esc(vhost)+"/"+esc(name)+"/rebalance", nil, map[string]any{})
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("未找到队列 %s（vhost %s）", name, vhost)
		}
		return err
	}
	if c.jsonOut {
		return c.emitJSON(raw)
	}
	var info rebalanceInfo
	if err := decodeJSON(raw, &info); err != nil {
		return err
	}
	if info.Moved {
		fmt.Printf("仲裁队列 %s 的 leader 已从 %s 迁到 %s\n", name, orDash(info.From), orDash(info.To))
		return nil
	}
	fmt.Printf("仲裁队列 %s 的 leader 未迁移（%s）\n", name, orDash(info.Reason))
	return nil
}

// orDash 为空值返回"（未知）"，便于表格与文本输出稳定。
func orDash(s string) string {
	if s == "" {
		return "（未知）"
	}
	return s
}

// ---------- 3b. vhost 增删查（M8-7） ----------

func cmdListVHosts(c *client) error {
	data, err := c.get("/api/vhosts", nil)
	if err != nil {
		return err
	}
	if c.jsonOut {
		return c.emitJSON(data)
	}

	var vhosts []vhost
	if err := decodeJSON(data, &vhosts); err != nil {
		return err
	}
	sort.Slice(vhosts, func(i, j int) bool { return vhosts[i].Name < vhosts[j].Name })
	if len(vhosts) == 0 {
		fmt.Println("（无 vhost）")
		return nil
	}

	rows := make([][]string, 0, len(vhosts))
	for _, item := range vhosts {
		rows = append(rows, []string{
			item.Name,
			strconv.Itoa(item.Messages),
			strconv.Itoa(item.MessagesReady),
			strconv.Itoa(item.MessagesUnacknowledged),
		})
	}
	writeTable([]string{"name", "messages", "messages_ready", "messages_unacknowledged"}, rows)
	return nil
}

func cmdAddVHost(c *client, name string) error {
	raw, err := c.put("/api/vhosts/"+esc(name), nil, nil)
	if err != nil {
		return err
	}
	return c.confirm(raw, fmt.Sprintf("vhost %s 已创建或更新", name))
}

func cmdDeleteVHost(c *client, name string) error {
	raw, err := c.delete("/api/vhosts/"+esc(name), nil)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("未找到 vhost %s", name)
		}
		return err
	}
	return c.confirm(raw, fmt.Sprintf("vhost %s 及其全部内容已删除", name))
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
		rows = append(rows, []string{p.Name, p.Version, p.APIVersion, p.State, shortNote(p.RuntimeNote)})
	}
	writeTable([]string{"name", "version", "api_version", "state", "note"}, rows)
	return nil
}

// shortNote 把状态原因压到一行、限制长度：list 是总览视图，完整原因用 `plugins show` 看。
func shortNote(note string) string {
	if note == "" {
		return "-"
	}
	note = strings.ReplaceAll(note, "\n", " ")
	const max = 70
	r := []rune(note)
	if len(r) > max {
		return string(r[:max]) + "…"
	}
	return note
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
	if p.RuntimeNote != "" {
		fmt.Fprintf(w, "runtime_note\t%s\n", p.RuntimeNote)
	}
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
