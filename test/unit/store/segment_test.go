// Package store_test 的第二个文件：覆盖 M8-1 的「段轮转与空间回收」。
//
// 背景：早期实现只有一个段文件、Ack 只追加一条索引标记，消息体永远不会被回收 ——
// 长期运行的队列会把磁盘吃满。这里断言的是三条可观察的结果：
// 段会轮转、清空的段会被删除（磁盘占用回落）、以及**回收绝不误删还活着的消息**。
package store_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// testSegmentBytes 把段压到 4 KiB：默认的 8 MiB 在测试里写不出来。
const testSegmentBytes = 4096

func newSegmentManager(t *testing.T, dir string) *store.Manager {
	t.Helper()
	return store.NewManager(dir, store.Options{
		Fsync:           store.FsyncAlways,
		FlushInterval:   10 * time.Millisecond,
		SegmentMaxBytes: testSegmentBytes,
	}, testLogger())
}

func sizedMessage(n int) *plugin.Message {
	return &plugin.Message{
		Properties: plugin.Properties{DeliveryMode: 2},
		Body:       bytes.Repeat([]byte("x"), n),
	}
}

// appendAndWait 写入一条消息并等待落盘。
func appendAndWait(t *testing.T, st *store.QueueStore, seq uint64, size int) {
	t.Helper()
	c, err := st.Append(seq, sizedMessage(size))
	if err != nil {
		t.Fatalf("追加消息 %d 失败: %v", seq, err)
	}
	if err := c.Wait(); err != nil {
		t.Fatalf("等待消息 %d 落盘失败: %v", seq, err)
	}
}

// waitUntil 轮询等待条件成立（Ack 是异步刷盘的，断言不能依赖精确时序）。
func waitUntil(t *testing.T, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", desc)
}

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatalf("枚举段文件失败: %v", err)
	}
	return matches
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("统计目录大小失败: %v", err)
	}
	return total
}

// TestSegmentRotation 覆盖：段写满就轮转，不会无限往同一个文件里追加。
func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	m := newSegmentManager(t, dir)
	defer m.CloseAll()

	st, _, err := m.Open("/", "rot.q", true)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	const total = 40 // 每条 ~520 字节，4 KiB 一段 → 期望切出 5~6 个段
	for i := 1; i <= total; i++ {
		appendAndWait(t, st, uint64(i), 512)
	}
	if got := len(segFiles(t, st.Dir())); got < 3 {
		t.Fatalf("段没有按大小轮转：只有 %d 个段文件（上限 %d 字节）", got, testSegmentBytes)
	}
}

// TestSegmentReclaimAndRecovery 覆盖：清空的段被真正删除（磁盘回落），
// 且回收**不会**误删仍然存活的消息 —— 关掉重开必须能原样恢复。
func TestSegmentReclaimAndRecovery(t *testing.T) {
	dir := t.TempDir()
	m := newSegmentManager(t, dir)

	st, _, err := m.Open("/", "rec.q", true)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	const total = 40
	for i := 1; i <= total; i++ {
		appendAndWait(t, st, uint64(i), 512)
	}
	segsBefore := len(segFiles(t, st.Dir()))
	bytesBefore := dirBytes(t, st.Dir())
	if segsBefore < 3 {
		t.Fatalf("前置条件不成立：段太少（%d），无法验证回收", segsBefore)
	}

	// 确认前面的消息，留最后几条不确认 —— 它们是"必须被保住"的存活消息。
	const acked = 35
	for i := 1; i <= acked; i++ {
		if err := st.Ack(uint64(i)); err != nil {
			t.Fatalf("确认消息 %d 失败: %v", i, err)
		}
	}
	waitUntil(t, "已清空的段被回收", func() bool {
		return len(segFiles(t, st.Dir())) < segsBefore
	})
	if after := dirBytes(t, st.Dir()); after >= bytesBefore {
		t.Fatalf("磁盘占用没有回落：回收前 %d 字节，回收后 %d 字节", bytesBefore, after)
	}

	m.CloseAll()

	// 重新打开（等价于进程重启）：未确认的消息必须一条不少地回来。
	m2 := newSegmentManager(t, dir)
	defer m2.CloseAll()
	_, recovered, err := m2.Open("/", "rec.q", true)
	if err != nil {
		t.Fatalf("重新打开存储失败: %v", err)
	}
	if len(recovered) != total-acked {
		t.Fatalf("恢复出 %d 条，期望 %d 条", len(recovered), total-acked)
	}
	for _, r := range recovered {
		if r.Seq <= acked {
			t.Fatalf("已确认的消息 %d 被错误地恢复了", r.Seq)
		}
		if !bytes.Equal(r.Message.Body, bytes.Repeat([]byte("x"), 512)) {
			t.Fatalf("恢复出的消息体损坏（seq=%d，长度 %d）", r.Seq, len(r.Message.Body))
		}
	}
}

// TestIndexCompaction 覆盖：段被回收后，索引也会被压缩重写，不会自己只增不减。
func TestIndexCompaction(t *testing.T) {
	dir := t.TempDir()
	// 用 os 档位：本用例只关心文件大小，不需要每条都 fsync（否则几千次 fsync 太慢）。
	m := store.NewManager(dir, store.Options{
		Fsync:           store.FsyncOS,
		FlushInterval:   10 * time.Millisecond,
		SegmentMaxBytes: testSegmentBytes,
	}, testLogger())
	defer m.CloseAll()

	st, _, err := m.Open("/", "cmp.q", true)
	if err != nil {
		t.Fatalf("打开存储失败: %v", err)
	}
	// 反复"写入 + 立即确认"，制造大量失效的索引记录。
	const rounds = 3000
	var last *store.Commit
	for i := 1; i <= rounds; i++ {
		c, err := st.Append(uint64(i), sizedMessage(64))
		if err != nil {
			t.Fatalf("追加消息 %d 失败: %v", i, err)
		}
		if err := st.Ack(uint64(i)); err != nil {
			t.Fatalf("确认消息 %d 失败: %v", i, err)
		}
		last = c
	}
	// 等最后一条落盘：刷盘按 pending 顺序处理，因此它返回时前面的记录也都已经处理完。
	if err := last.Wait(); err != nil {
		t.Fatalf("刷盘失败: %v", err)
	}

	idxPath := filepath.Join(st.Dir(), "index", "000001.idx")
	// 未压缩时约 rounds × 25 字节（publish + ack 各一条记录）。
	waitUntil(t, "索引被压缩", func() bool {
		info, err := os.Stat(idxPath)
		return err == nil && info.Size() < int64(rounds*25)/4
	})

	// 打印实际状态，便于区分"索引没压缩"与"段没回收"两种失败。
	var detail []string
	for _, f := range segFiles(t, st.Dir()) {
		fi, _ := os.Stat(f)
		detail = append(detail, fmt.Sprintf("%s(%d bytes)", filepath.Base(f), fi.Size()))
	}
	idxInfo, _ := os.Stat(idxPath)
	t.Logf("索引 %d 字节；剩余段文件 %v", idxInfo.Size(), detail)

	waitUntil(t, "已清空的段被回收", func() bool {
		return len(segFiles(t, st.Dir())) <= 2
	})
}
