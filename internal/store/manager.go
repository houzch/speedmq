package store

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// msgStoresDir 是节点数据目录下存放消息存储的子目录（对齐设计 5.3.2 的磁盘布局）。
const msgStoresDir = "msg_stores"

// Manager 负责按 vhost / 队列维度打开与回收存储。
//
// 只有 durable 队列才有存储：非持久队列重启即丢，这是与 RabbitMQ 一致的语义。
type Manager struct {
	root string
	opts Options
	log  *slog.Logger

	mu     sync.Mutex
	open   map[*QueueStore]struct{}
	closed bool
}

// NewManager 构造存储管理器。dataDir 为节点数据目录。
func NewManager(dataDir string, opts Options, log *slog.Logger) *Manager {
	return &Manager{
		root: filepath.Join(dataDir, msgStoresDir),
		opts: opts,
		log:  log,
		open: map[*QueueStore]struct{}{},
	}
}

// Options 返回当前存储选项。
func (m *Manager) Options() Options { return m.opts }

// Enabled 表示是否真的会落盘（fsync 档位为 none 时不做任何持久化）。
func (m *Manager) Enabled() bool { return m.opts.Fsync != FsyncNone }

// Open 打开（必要时创建）某队列的存储，并返回恢复出来的消息。
//
// durable 为 false 或落盘档位为 none 时返回 nil 存储与 nil 消息。
func (m *Manager) Open(vhost, queue string, durable bool) (*QueueStore, []Recovered, error) {
	if !durable || !m.Enabled() {
		return nil, nil, nil
	}
	dir := m.queueDir(vhost, queue)
	if err := os.MkdirAll(filepath.Join(dir, "index"), 0o700); err != nil {
		return nil, nil, fmt.Errorf("创建队列存储目录失败: %w", err)
	}

	st := newQueueStore(dir, m.opts, m.log.With("vhost", vhost, "queue", queue))
	if err := st.openFiles(); err != nil {
		return nil, nil, err
	}
	recovered, err := st.recover()
	if err != nil {
		_ = st.Close()
		return nil, nil, err
	}
	// 先登记再启动刷盘协程：否则 CloseAll 可能取到不含本存储的快照，
	// 这条存储就再也不会被关闭（刷盘协程与文件句柄泄漏）。
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = st.Close()
		return nil, nil, fmt.Errorf("存储管理器已关闭")
	}
	m.open[st] = struct{}{}
	m.mu.Unlock()
	st.startFlusher()
	return st, recovered, nil
}

// CloseAll 关闭全部已打开的存储（收尾刷盘）。可重复调用。
func (m *Manager) CloseAll() {
	m.mu.Lock()
	m.closed = true
	stores := make([]*QueueStore, 0, len(m.open))
	for st := range m.open {
		stores = append(stores, st)
	}
	m.open = map[*QueueStore]struct{}{}
	m.mu.Unlock()

	for _, st := range stores {
		if err := st.Close(); err != nil {
			m.log.Warn("关闭队列存储失败", "dir", st.Dir(), "err", err)
		}
	}
}

func (m *Manager) queueDir(vhost, queue string) string {
	return filepath.Join(m.root, "vhosts", safeDirName(vhost), "queues", safeDirName(queue))
}

// vhostDir 返回某 vhost 的存储根目录（其下是各队列目录）。
func (m *Manager) vhostDir(vhost string) string {
	return filepath.Join(m.root, "vhosts", safeDirName(vhost))
}

// RemoveVHost 关闭该 vhost 下全部已打开的队列存储，并删除它的整个存储目录。
//
// 只对名称做了一次目录编码（safeDirName 是单射），因此"某存储是否属于该 vhost"
// 用路径前缀判断是可靠的；用分隔符边界比较而不是裸字符串前缀，
// 避免 vhost="a" 误伤 vhost="ab"（编码后分别为 q_a 与 q_ab）。
func (m *Manager) RemoveVHost(vhost string) error {
	base := m.vhostDir(vhost)
	prefix := base + string(filepath.Separator)

	m.mu.Lock()
	victims := make([]*QueueStore, 0, len(m.open))
	for st := range m.open {
		dir := filepath.Clean(st.Dir())
		if dir == base || startsWithSep(dir, prefix) {
			victims = append(victims, st)
			delete(m.open, st)
		}
	}
	m.mu.Unlock()

	var errs []error
	for _, st := range victims {
		if err := st.Close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭队列存储 %s 失败: %w", st.Dir(), err))
		}
	}
	if err := os.RemoveAll(base); err != nil {
		errs = append(errs, fmt.Errorf("删除 vhost 存储目录失败: %w", err))
	}
	return errors.Join(errs...)
}

// startsWithSep 判断 path 是否以 prefix 开头；调用方保证 prefix 以分隔符结尾。
func startsWithSep(path, prefix string) bool {
	return len(path) >= len(prefix) && path[:len(prefix)] == prefix
}

func indexlessPath(dir, name string) string { return filepath.Join(dir, name) }

func indexPath(dir, name string) string { return filepath.Join(dir, "index", name) }

func removeDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("删除队列存储目录失败: %w", err)
	}
	return nil
}

// SafeDirName 把 vhost / 队列名编码成安全的目录名。
//
// 导出它是为了让"与消息存储同规则"的其它持久化目录复用（例如仲裁队列的 Raft 日志目录）：
// 目录名不直接采用用户输入这条约束只在存储层实现一次，别处再抄一遍迟早会漏。
func SafeDirName(name string) string { return safeDirName(name) }

// safeDirName 把 vhost / 队列名编码成安全的目录名。
//
// 目录名一律不直接采用用户输入（设计 5.3.2）：避免路径穿越、非法字符与超长文件名。
// 统一加 "q_" 前缀还能绕开 Windows 的保留设备名（con / nul / lpt1 …）。
func safeDirName(name string) string {
	var b strings.Builder
	b.Grow(len(name) + 2)
	b.WriteString("q_")
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '-', c == '_':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
