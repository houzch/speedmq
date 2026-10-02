// 本文件覆盖资源水位闸门的**可恢复性** —— 这是 M8-10 的 Linux 压测实测出来的缺陷。
//
// 缺陷现象：水位判定原先用 runtime.MemStats.Sys（"向操作系统申请过的地址空间"的高水位），
// Go 运行期几乎不会及时把它还给操作系统。于是它一旦越过阈值就再也降不回来：
// 消费者把消息全部消费完、堆已经空下来，`connection.blocked` 仍然不解除，
// 生产者被**永久**阻塞，broker 对新生产者不可用，只能重启进程。
package broker_test

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// heapInUse 读取当前进程"在用"的内存（与内核的水位口径一致）。
func heapInUse() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse + ms.StackInuse
}

// publishOnce 用管理面会话发布一条消息；被水位闸门挡住时返回错误（而不是阻塞）。
func publishOnce(t *testing.T, sess plugin.Session, queue string) error {
	t.Helper()
	_, err := sess.Publish(&plugin.Message{
		Properties: plugin.Properties{DeliveryMode: 2},
		Body:       []byte("wm"),
	}, "", queue, false)
	return err
}

// TestWatermarkGateIsRecoverable 覆盖"水位触发 → 内存回收 → 闸门必须恢复"。
//
// 判据：把阈值卡在"当前在用内存 + 余量"，先用一次大分配顶过阈值（应被阻塞），
// 再释放并 GC（应**恢复**可发布）。用 MemStats.Sys 口径时第二步不会恢复，因此本用例会失败。
func TestWatermarkGateIsRecoverable(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载默认配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	b := mustBroker(t, cfg)
	t.Cleanup(b.Close)

	sess, err := b.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()

	const queue = "watermark.q"
	if _, err := sess.DeclareQueue(plugin.QueueDeclare{Name: queue, Durable: true}); err != nil {
		t.Fatalf("声明队列失败: %v", err)
	}

	total, ok := store.TotalMemory()
	if !ok || total == 0 {
		t.Skip("拿不到物理内存总量，跳过（无法把水位比例换算成字节阈值）")
	}

	const (
		margin = 24 << 20 // 阈值 = 基线 + 24 MiB
		spike  = 96 << 20 // 一次性分配 96 MiB，足够顶过阈值
	)
	base := heapInUse()
	watermark := float64(base+margin) / float64(total)

	// 先确认"未超阈值时不阻塞"，否则后面的失败无法归因
	b.UpdateLimits(watermark, 0) // 0 = 关闭磁盘水位检查
	if err := publishOnce(t, sess, queue); err != nil {
		t.Fatalf("尚未超过阈值就已被阻塞: %v", err)
	}

	// 制造一次"临时占用"（稍后释放）
	ballast := make([]byte, spike)
	for i := 0; i < len(ballast); i += 4096 {
		ballast[i] = 1 // 真触碰，确保内存确实被占用
	}
	b.UpdateLimits(watermark, 0)
	runtime.KeepAlive(ballast)

	err = publishOnce(t, sess, queue)
	if err == nil {
		t.Fatalf("内存占用已超过水位阈值，发布本应被阻塞")
	}
	if !strings.Contains(err.Error(), "资源水位") {
		t.Fatalf("阻塞原因应是资源水位，实际: %v", err)
	}
	runtime.KeepAlive(ballast)

	// 释放内存：闸门必须恢复（这是缺陷的判定点）
	ballast = nil
	deadline := time.Now().Add(8 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		runtime.GC()
		b.UpdateLimits(watermark, 0)
		if lastErr = publishOnce(t, sess, queue); lastErr == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("内存已释放，水位闸门却没有恢复 —— 生产者被永久阻塞（这正是被测缺陷）: %v", lastErr)
}

// TestWatermarkReportsReason 覆盖水位触发时的可诊断性：
// 管理面/日志要能说清"超的是哪一项、当前值多少、阈值多少"。
func TestWatermarkReportsReason(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("加载默认配置失败: %v", err)
	}
	cfg.DataDir = t.TempDir()
	b := mustBroker(t, cfg)
	t.Cleanup(b.Close)

	// 把内存水位设成一个"必然超过"的值：阈值 = 0 字节
	b.UpdateLimits(0.0000001, 0)

	sess, err := b.SessionFor("guest", "/")
	if err != nil {
		t.Fatalf("打开会话失败: %v", err)
	}
	defer sess.Close()

	err = publishOnce(t, sess, "no.such.queue")
	if err == nil || !strings.Contains(err.Error(), "资源水位") {
		t.Fatalf("阈值极低时应当被阻塞，实际: %v", err)
	}

	// 恢复：关闭水位检查后必须能继续发布
	b.UpdateLimits(0, 0)
	if err := publishOnce(t, sess, "no.such.queue"); err != nil {
		t.Fatalf("关闭水位后应恢复可发布（未路由不算错），实际: %v", err)
	}

	// 阈值随 UpdateLimits 生效
	if wm, _ := b.StorageLimits(); wm != 0 {
		t.Fatalf("StorageLimits 应反映最新阈值，实际 %v", wm)
	}
}
