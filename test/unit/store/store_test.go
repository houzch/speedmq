package store_test

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/protocol/codec"
	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// 本文件覆盖持久化层的关键语义：段日志 + 索引的写入与恢复、尾部半写记录的丢弃、
// 以及 fsync 档位的解析。

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestManager(t *testing.T, level store.FsyncLevel) *store.Manager {
	t.Helper()
	return store.NewManager(t.TempDir(), store.Options{Fsync: level, FlushInterval: 20 * time.Millisecond}, testLogger())
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
	m := newTestManager(t, store.FsyncAlways)

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
	m := newTestManager(t, store.FsyncBatch)
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
	m := newTestManager(t, store.FsyncAlways)
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
	// 段文件路径经导出 API 取得（不再依赖 Manager 的私有 root 字段）。
	dir := st.Dir()
	if err := st.Close(); err != nil {
		t.Fatalf("关闭存储失败: %v", err)
	}

	segPath := filepath.Join(dir, "000001.seg")
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
	m := newTestManager(t, store.FsyncNone)
	st, recovered, err := m.Open("/", "q", true)
	if err != nil {
		t.Fatalf("打开存储不应报错: %v", err)
	}
	if st != nil || recovered != nil {
		t.Fatalf("none 档位不应创建存储：st=%v recovered=%v", st, recovered)
	}
}

func TestParseFsync(t *testing.T) {
	cases := map[string]store.FsyncLevel{
		"":       store.FsyncOS,
		"none":   store.FsyncNone,
		"os":     store.FsyncOS,
		"batch":  store.FsyncBatch,
		"always": store.FsyncAlways,
	}
	for in, want := range cases {
		got, err := store.ParseFsync(in)
		if err != nil {
			t.Fatalf("ParseFsync(%q) 报错: %v", in, err)
		}
		if got != want {
			t.Fatalf("ParseFsync(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := store.ParseFsync("whatever"); err == nil {
		t.Fatalf("非法档位应报错")
	}
}

// TestSafeDirName 以行为方式覆盖目录名编码与路径穿越防护：
// 外部测试包看不到未导出的 safeDirName，因此改为走完整落盘路径断言可观察结果 ——
//   - 含路径分隔符与 ".." 的 vhost / 队列名，其存储目录必须始终落在数据目录之下；
//   - 不同的名字必须编码成不同目录（编码是单射），否则两个队列会共用同一目录；
//   - Windows 保留设备名不能被原样用作目录名（否则目录创建会失败）。
func TestSafeDirName(t *testing.T) {
	dataDir := t.TempDir()
	m := store.NewManager(dataDir, store.Options{Fsync: store.FsyncAlways}, testLogger())
	defer m.CloseAll()

	cleanData := filepath.Clean(dataDir)

	// openUnder 打开一个 durable 队列，并断言其存储目录位于数据目录之下。
	openUnder := func(vhost, queue string) *store.QueueStore {
		t.Helper()
		st, _, err := m.Open(vhost, queue, true)
		if err != nil {
			t.Fatalf("打开存储失败: vhost=%q queue=%q err=%v", vhost, queue, err)
		}
		if st == nil {
			t.Fatalf("durable 队列必须创建存储: vhost=%q queue=%q", vhost, queue)
		}
		dir := filepath.Clean(st.Dir())
		rel, err := filepath.Rel(cleanData, dir)
		if err != nil {
			t.Fatalf("无法计算相对路径: dir=%q dataDir=%q err=%v", dir, cleanData, err)
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Fatalf("存储目录逃逸出数据目录: dir=%q dataDir=%q", dir, cleanData)
		}
		return st
	}

	// 路径穿越：vhost 与队列名都含 ".." 与 "/"，都不得逃出数据目录。
	// 注意不用以 ".." 结尾的名字（如 vhost=".."）：safeDirName 允许 '.'，会得到形如
	// "q_.." 的目录名，而 Windows 会剥掉结尾的点，导致 MkdirAll 找不到父目录而失败
	// —— 这是平台文件名的限制，与"路径是否可控"无关，故这里用 "../../x" 仍保留穿越语义。
	openUnder("../../x", "../evil/name")

	// 编码单射：分隔符被编码后，"a/b" 与字面量 "a%2Fb" 不能落到同一目录。
	slash := openUnder("/", "a/b")
	literal := openUnder("/", "a%2Fb")
	if filepath.Clean(slash.Dir()) == filepath.Clean(literal.Dir()) {
		t.Fatalf("不同队列名编码到了同一目录: a/b 与 a%%2Fb 均为 %q", slash.Dir())
	}

	// Windows 保留设备名不得被原样用作目录名。
	con := openUnder("/", "con")
	if base := filepath.Base(con.Dir()); base == "con" || base == "CON" {
		t.Fatalf("队列名 con 被原样用作目录名（Windows 保留设备名）: %q", con.Dir())
	}
}
