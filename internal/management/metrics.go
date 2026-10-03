package management

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	sdk "github.com/houzch/swiftmq/pkg/plugin"
)

// getMetrics 输出 Prometheus 的文本暴露格式（/metrics）。
//
// 为什么手写而不是引入 prometheus/client_golang：
// 本项目的构建承诺是"零外部依赖、可离线复现"（见 Dockerfile），而抓取端只认文本格式本身。
// M5 的指标规模是几十条、标签基数很小，手写暴露格式比拉进一条依赖链更划算。
// 等到指标规模与标签基数上来（或需要 Histogram/Summary 时），再换成官方库即可 ——
// 暴露格式不变，抓取端配置不需要改。
func (s *Server) getMetrics(w http.ResponseWriter, _ *http.Request, _ params, au authUser) {
	if err := au.requireRead(); err != nil {
		writeKernelError(w, err)
		return
	}

	b := s.deps.Broker
	var sb strings.Builder

	writeMetric(&sb, "swiftmq_up", "gauge", "节点是否存活", "1")
	writeMetric(&sb, "swiftmq_build_info", "gauge", "构建信息",
		fmt.Sprintf("1{version=%q,node=%q}", escapeLabel(s.deps.Version), escapeLabel(s.deps.NodeName)))

	blocked := 0
	if b.BlockedState() {
		blocked = 1
	}
	writeMetric(&sb, "swiftmq_resource_blocked", "gauge",
		"资源水位是否阻塞了生产者（1=阻塞中）", fmt.Sprint(blocked))

	totals := b.ObjectTotals()
	writeMetric(&sb, "swiftmq_connections", "gauge", "当前连接数", fmt.Sprint(totals.Connections))
	writeMetric(&sb, "swiftmq_channels", "gauge", "当前通道数", fmt.Sprint(totals.Channels))
	writeMetric(&sb, "swiftmq_queues", "gauge", "当前队列数", fmt.Sprint(totals.Queues))
	writeMetric(&sb, "swiftmq_exchanges", "gauge", "当前交换机数", fmt.Sprint(totals.Exchanges))
	writeMetric(&sb, "swiftmq_consumers", "gauge", "当前消费者数", fmt.Sprint(totals.Consumers))

	qt := b.QueueTotals()
	writeMetric(&sb, "swiftmq_queue_messages", "gauge", "就绪消息总数", fmt.Sprint(qt.Ready))
	writeMetric(&sb, "swiftmq_queue_messages_unacknowledged", "gauge",
		"未确认消息总数", fmt.Sprint(qt.Unacknowledged))

	writeMetric(&sb, "swiftmq_process_memory_bytes", "gauge",
		"本进程向操作系统申请的内存字节数", fmt.Sprint(b.ProcessMemory()))
	if total, ok := b.MemoryTotal(); ok {
		writeMetric(&sb, "swiftmq_memory_total_bytes", "gauge", "物理内存总量", fmt.Sprint(total))
	}
	if free, err := b.DiskFree(); err == nil {
		writeMetric(&sb, "swiftmq_disk_free_bytes", "gauge", "数据目录可用空间", fmt.Sprint(free))
	}
	wm, diskLimit := b.StorageLimits()
	writeMetric(&sb, "swiftmq_memory_high_watermark", "gauge", "内存水位比例", fmt.Sprintf("%g", wm))
	writeMetric(&sb, "swiftmq_disk_free_limit_bytes", "gauge", "磁盘剩余空间下限", fmt.Sprint(diskLimit))

	// 每队列指标：vhost / queue 标签是运维定位问题的主要维度。
	sb.WriteString("# HELP swiftmq_queue_messages_ready 队列中的就绪消息数\n")
	sb.WriteString("# TYPE swiftmq_queue_messages_ready gauge\n")
	sb.WriteString("# HELP swiftmq_queue_messages_unacknowledged 队列中的未确认消息数\n")
	sb.WriteString("# TYPE swiftmq_queue_messages_unacknowledged gauge\n")
	sb.WriteString("# HELP swiftmq_queue_consumers 队列上的消费者数\n")
	sb.WriteString("# TYPE swiftmq_queue_consumers gauge\n")
	sb.WriteString("# HELP swiftmq_queue_memory_bytes 队列内存占用估算值\n")
	sb.WriteString("# TYPE swiftmq_queue_memory_bytes gauge\n")
	sb.WriteString("# HELP swiftmq_queue_messages_published_total 队列累计接收的消息数\n")
	sb.WriteString("# TYPE swiftmq_queue_messages_published_total counter\n")
	sb.WriteString("# HELP swiftmq_queue_messages_delivered_total 队列累计投递的消息数\n")
	sb.WriteString("# TYPE swiftmq_queue_messages_delivered_total counter\n")
	sb.WriteString("# HELP swiftmq_queue_messages_acked_total 队列累计确认的消息数\n")
	sb.WriteString("# TYPE swiftmq_queue_messages_acked_total counter\n")

	for _, q := range b.QueueSnapshots("") {
		// 值先经 escapeLabel 转义，再手工加引号：不能叠加 %q（会二次转义，标签值多出反斜杠）。
		labels := `{vhost="` + escapeLabel(q.VHost) + `",queue="` + escapeLabel(q.Name) + `"}`
		fmt.Fprintf(&sb, "swiftmq_queue_messages_ready%s %d\n", labels, q.Ready)
		fmt.Fprintf(&sb, "swiftmq_queue_messages_unacknowledged%s %d\n", labels, q.Unacked)
		fmt.Fprintf(&sb, "swiftmq_queue_consumers%s %d\n", labels, q.ConsumerCount)
		fmt.Fprintf(&sb, "swiftmq_queue_memory_bytes%s %d\n", labels, q.MemoryBytes)
		fmt.Fprintf(&sb, "swiftmq_queue_messages_published_total%s %d\n", labels, q.Published)
		fmt.Fprintf(&sb, "swiftmq_queue_messages_delivered_total%s %d\n", labels, q.Delivered)
		fmt.Fprintf(&sb, "swiftmq_queue_messages_acked_total%s %d\n", labels, q.Acked)
	}

	sb.WriteString("# HELP swiftmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）\n")
	sb.WriteString("# TYPE swiftmq_plugin_info gauge\n")
	sb.WriteString("# HELP swiftmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）\n")
	sb.WriteString("# TYPE swiftmq_plugin_up gauge\n")
	for _, p := range s.deps.Plugins.Plugins() {
		fmt.Fprintf(&sb, "swiftmq_plugin_info{name=\"%s\",version=\"%s\",api_version=\"%s\",state=\"%s\"} 1\n",
			escapeLabel(p.Name), escapeLabel(p.Version), escapeLabel(p.APIVersion), escapeLabel(string(p.State)))
		// up 单独给一条：用 state 标签做告警要写 "!= enabled"，
		// 而按惯例 `swiftmq_plugin_up == 0` 才是最好写、最不容易写错的形式。
		up := 0
		if p.State == sdk.StateEnabled {
			up = 1
		}
		fmt.Fprintf(&sb, "swiftmq_plugin_up{name=\"%s\",state=\"%s\"} %d\n",
			escapeLabel(p.Name), escapeLabel(string(p.State)), up)
	}

	body := sb.String()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, body); err != nil {
		s.log.Debug("写入指标响应失败", "err", err)
	}
}

func writeMetric(sb *strings.Builder, name, typ, help, value string) {
	fmt.Fprintf(sb, "# HELP %s %s\n# TYPE %s %s\n%s %s\n", name, help, name, typ, name, value)
}

// escapeLabel 转义 Prometheus 标签值里的反斜杠、双引号与换行。
//
// 队列名与 vhost 名是用户输入，不转义会让抓取端解析失败甚至注入额外的指标行。
func escapeLabel(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}
