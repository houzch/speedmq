package store

import (
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

	mu   sync.Mutex
	open map[*QueueStore]struct{}
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
	st.startFlusher()

	m.mu.Lock()
	m.open[st] = struct{}{}
	m.mu.Unlock()
	return st, recovered, nil
}

// CloseAll 关闭全部已打开的存储（收尾刷盘）。可重复调用。
func (m *Manager) CloseAll() {
	m.mu.Lock()
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

func indexlessPath(dir, name string) string { return filepath.Join(dir, name) }

func indexPath(dir, name string) string { return filepath.Join(dir, "index", name) }

func removeDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("删除队列存储目录失败: %w", err)
	}
	return nil
}

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
