package store

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖持久化层的关键语义：段日志 + 索引的写入与恢复、尾部半写记录的丢弃、
// 以及 fsync 档位的解析。

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestManager(t *testing.T, level FsyncLevel) *Manager {
	t.Helper()
	return NewManager(t.TempDir(), Options{Fsync: level, FlushInterval: 20 * time.Millisecond}, testLogger())
}

func persistMessage() *plugin.Message {
	return &plugin.Message{
		Exchange:   "ex",
		RoutingKey: "rk",
		Properties: plugin.Properties{
			DeliveryMode: 2,
			ContentType:  "text/plain",
			MessageID:    "m-1",
			Timestamp:    time.Unix(1700000000, 0).UTC(),
			Headers: map[string]any{
				"count":  int32(7),
				"binary": []byte{1, 2, 3},
				"nested": map[string]any{"k": "v"},
				"list":   []any{int64(1), "two"},
			},
		},
		Body: []byte("payload"),
	}
}

// TestAppendAckRecover 覆盖最基本的持久化语义：
// 已 ack 的消息不应恢复，未 ack 的消息必须恢复，且属性类型保持不变。
func TestAppendAckRecover(t *testing.T) {
	m := newTestManager(t, FsyncAlways)

	st, recovered, err := m.Open("/", "dur.q", true)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("新存储不应有可恢复消息，实际 %d 条", len(recovered))
	}

	c1, err := st.Append(1, persistMessage())
	if err != nil {
		t.Fatalf("追加消息失败: %v", err)
	}
	if err := c1.Wait(); err != nil {
		t.Fatalf("等待第一条落盘失败: %v", err)
	}

	c2, err := st.Append(2, &plugin.Message{Properties: plugin.Properties{DeliveryMode: 2}, Body: []byte("two")})
	if err != nil {
		t.Fatalf("追加第二条失败: %v", err)
	}
	if err := c2.Wait(); err != nil {
		t.Fatalf("等待第二条落盘失败: %v", err)
	}

	if err := st.Ack(1); err != nil {
		t.Fatalf("写入 ack 标记失败: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("关闭存储失败: %v", err)
	}

	// 重新打开：模拟进程重启后的恢复
	st2, recovered, err := m.Open("/", "dur.q", true)
	if err != nil {
		t.Fatalf("重新打开存储失败: %v", err)
	}
	defer st2.Close()

	if len(recovered) != 1 {
		t.Fatalf("应恢复 1 条未 ack 的消息，实际 %d 条", len(recovered))
	}
	got := recovered[0]
	if got.Seq != 2 {
		t.Fatalf("恢复的序号 = %d, want 2（序号 1 已被 ack，不应恢复）", got.Seq)
	}
	if string(got.Message.Body) != "two" {
		t.Fatalf("恢复的消息体 = %q, want %q", got.Message.Body, "two")
	}
}

// TestPropertyFidelity 覆盖属性类型保真：
// 字节数组、时间戳、嵌套表与数组必须原样回来，不能被降级成字符串。
func TestPropertyFidelity(t *testing.T) {
	m := newTestManager(t, FsyncBatch)
	st, _, err := m.Open("/", "fid.q", true)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	c, err := st.Append(1, persistMessage())
	if err != nil {
		t.Fatalf("追加消息失败: %v", err)
	}
	if err := c.Wait(); err != nil {
		t.Fatalf("等待落盘失败: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("关闭存储失败: %v", err)
	}

	st2, recovered, err := m.Open("/", "fid.q", true)
	if err != nil {
		t.Fatalf("重新打开失败: %v", err)
	}
	defer st2.Close()
	if len(recovered) != 1 {
		t.Fatalf("应恢复 1 条，实际 %d 条", len(recovered))
	}
	msg := recovered[0].Message
	if msg.Exchange != "ex" || msg.RoutingKey != "rk" {
		t.Fatalf("路由信息未保真: exchange=%q routingKey=%q", msg.Exchange, msg.RoutingKey)
	}
	if msg.Properties.ContentType != "text/plain" || msg.Properties.MessageID != "m-1" {
		t.Fatalf("基本属性未保真: %+v", msg.Properties)
	}
	if msg.Properties.DeliveryMode != 2 {
		t.Fatalf("delivery-mode 未保真: %d", msg.Properties.DeliveryMode)
	}
	if !msg.Properties.Timestamp.Equal(time.Unix(1700000000, 0).UTC()) {
		t.Fatalf("时间戳未保真: %v", msg.Properties.Timestamp)
	}
	if b, ok := msg.Properties.Headers["binary"].([]byte); !ok || len(b) != 3 {
		t.Fatalf("字节数组头未保真: %#v", msg.Properties.Headers["binary"])
	}
	// 嵌套表在 AMQP field-table 解码后可能是 map[string]any 或 codec.Table（具名类型），
	// 两者都算保真 —— 关键是它仍然是"表"，没有被降级成字符串。
	switch nested := msg.Properties.Headers["nested"].(type) {
	case map[string]any:
		if nested["k"] != "v" {
			t.Fatalf("嵌套表内容错误: %#v", nested)
		}
	case codec.Table:
		if nested["k"] != "v" {
			t.Fatalf("嵌套表内容错误: %#v", nested)
		}
	default:
		t.Fatalf("嵌套表头未保真: %#v", msg.Properties.Headers["nested"])
	}
	if l, ok := msg.Properties.Headers["list"].([]any); !ok || len(l) != 2 {
		t.Fatalf("数组头未保真: %#v", msg.Properties.Headers["list"])
	}
}

// TestCorruptTailIsDiscarded 覆盖崩溃恢复：尾部半写记录必须被丢弃，
// 且不影响前面完整记录的恢复。
func TestCorruptTailIsDiscarded(t *testing.T) {
	m := newTestManager(t, FsyncAlways)
	st, _, err := m.Open("/", "tail.q", true)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	c, err := st.Append(1, &plugin.Message{Properties: plugin.Properties{DeliveryMode: 2}, Body: []byte("keep")})
	if err != nil {
		t.Fatalf("追加消息失败: %v", err)
	}
	if err := c.Wait(); err != nil {
		t.Fatalf("等待落盘失败: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("关闭存储失败: %v", err)
	}

	segPath := filepath.Join(m.root, "vhosts", safeDirName("/"), "queues", safeDirName("tail.q"), "000001.seg")
	before, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("读取段文件状态失败: %v", err)
	}

	// 模拟"写到一半断电"：声明 64 字节载荷却只写了 3 字节
	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("打开段文件失败: %v", err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 64, 0xAA, 0xBB, 0xCC}); err != nil {
		t.Fatalf("写入半条记录失败: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("关闭段文件失败: %v", err)
	}

	st2, recovered, err := m.Open("/", "tail.q", true)
	if err != nil {
		t.Fatalf("重新打开存储失败: %v", err)
	}
	defer st2.Close()

	if len(recovered) != 1 {
		t.Fatalf("完整记录应被恢复，实际恢复 %d 条", len(recovered))
	}
	if string(recovered[0].Message.Body) != "keep" {
		t.Fatalf("恢复内容错误: %q", recovered[0].Message.Body)
	}

	after, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("读取截断后的段文件失败: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("尾部半写记录应被截断：文件大小 %d, want %d", after.Size(), before.Size())
	}
}

// TestFsyncNoneDisablesStore 覆盖 none 档位：完全不创建存储，消息不做任何持久化。
func TestFsyncNoneDisablesStore(t *testing.T) {
	m := newTestManager(t, FsyncNone)
	st, recovered, err := m.Open("/", "q", true)
	if err != nil {
		t.Fatalf("打开存储不应报错: %v", err)
	}
	if st != nil || recovered != nil {
		t.Fatalf("none 档位不应创建存储：st=%v recovered=%v", st, recovered)
	}
}

func TestParseFsync(t *testing.T) {
	cases := map[string]FsyncLevel{
		"":       FsyncOS,
		"none":   FsyncNone,
		"os":     FsyncOS,
		"batch":  FsyncBatch,
		"always": FsyncAlways,
	}
	for in, want := range cases {
		got, err := ParseFsync(in)
		if err != nil {
			t.Fatalf("ParseFsync(%q) 报错: %v", in, err)
		}
		if got != want {
			t.Fatalf("ParseFsync(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := ParseFsync("whatever"); err == nil {
		t.Fatalf("非法档位应报错")
	}
}

// TestSafeDirName 覆盖目录名编码：vhost 的 "/" 与带特殊字符的队列名都不能直接落到文件系统上。
func TestSafeDirName(t *testing.T) {
	if got := safeDirName("/"); got != "q_%2F" {
		t.Fatalf("safeDirName(/) = %q, want q_%%2F", got)
	}
	if got := safeDirName("a/b"); got == "a/b" {
		t.Fatalf("路径分隔符必须被编码，实际 %q", got)
	}
	if got := safeDirName("con"); got != "q_con" {
		t.Fatalf("safeDirName(con) = %q, want q_con（避开 Windows 保留设备名）", got)
	}
}
