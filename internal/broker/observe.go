// Package broker 的管理面只读视图：连接/通道登记、对象快照与运行期管理操作。
//
// 分工：本文件提供**数据**（快照、计数、管理操作），internal/management 只负责
// 把这些数据翻译成 RabbitMQ 兼容的 HTTP JSON。这样"内核语义"与"HTTP 契约"各自独立演进。
package broker

import (
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/meta"
	"github.com/houzch/swiftmq/internal/store"
	"github.com/houzch/swiftmq/pkg/plugin"
)

// connEntry 是一条已建立连接的登记项。
//
// 它由 NewSession 创建、由协议层的 Core.Close 注销；协议层可选地注入"快照回调"与
// "强制断开回调"（见 plugin.Core.SetConnectionProbe / SetDisconnectFunc）。
type connEntry struct {
	id        string
	name      string
	remote    net.Addr
	local     net.Addr
	createdAt time.Time

	// user / vhost 在握手过程中由 protocol session 写入，之后只读。
	user  string
	vhost string

	probe      func() plugin.ConnectionInfo
	disconnect func(reason string)
}

// ConnectionSnapshot 是连接级只读视图。
type ConnectionSnapshot struct {
	ID               string
	Name             string
	User             string
	VHost            string
	Protocol         string
	State            string
	Channels         int
	ConnectedAt      time.Time
	PeerHost         string
	PeerPort         int
	FrameMax         uint32
	HeartbeatSeconds uint16
	AuthMechanism    string
	ClientProperties map[string]any
}

// ChannelSnapshot 是通道级只读视图。
type ChannelSnapshot struct {
	ConnectionID   string
	ConnectionName string
	Number         uint16
	User           string
	VHost          string
	State          string
	ConsumerCount  int
	PrefetchCount  uint16
	Confirm        bool
	Unacked        int
}

// ObjectTotals 是对象总数（管理面 /api/overview 用）。
type ObjectTotals struct {
	Connections int
	Channels    int
	Queues      int
	Consumers   int
	Exchanges   int
}

// QueueTotals 是队列消息总数。
type QueueTotals struct {
	Messages       int
	Ready          int
	Unacknowledged int
}

// MessageTotals 是消息累计计数。
type MessageTotals struct {
	Publish uint64
	Deliver uint64
	Ack     uint64
}

// ---------------------------------------------------------------------------
// 连接登记
// ---------------------------------------------------------------------------

func (b *Broker) registerConn(id string, remote, local net.Addr) *connEntry {
	entry := &connEntry{
		id:        id,
		name:      connectionName(remote, local),
		remote:    remote,
		local:     local,
		createdAt: time.Now(),
	}
	b.connsMu.Lock()
	b.conns[entry.name] = entry
	b.connsMu.Unlock()
	return entry
}

func (b *Broker) unregisterConn(entry *connEntry) {
	if entry == nil {
		return
	}
	b.connsMu.Lock()
	// 只有当映射里还是自己时才删：连接名理论上唯一，但端口复用可能造成重名，
	// 用指针比较可以避免后建立的连接被先结束的连接误删。
	if cur, ok := b.conns[entry.name]; ok && cur == entry {
		delete(b.conns, entry.name)
	}
	b.connsMu.Unlock()
}

// connectionName 生成 RabbitMQ 风格的连接名："<peer> -> <local>"。
func connectionName(remote, local net.Addr) string {
	r := "unknown"
	if remote != nil {
		r = remote.String()
	}
	l := "unknown"
	if local != nil {
		l = local.String()
	}
	return r + " -> " + l
}

// setConnUser 记录认证用户（握手阶段调用）。
func (b *Broker) setConnUser(entry *connEntry, user string) {
	if entry == nil {
		return
	}
	b.connsMu.Lock()
	entry.user = user
	b.connsMu.Unlock()
}

// setConnVHost 记录连接打开的 vhost。
func (b *Broker) setConnVHost(entry *connEntry, vhost string) {
	if entry == nil {
		return
	}
	b.connsMu.Lock()
	entry.vhost = vhost
	b.connsMu.Unlock()
}

// setConnHooks 记录协议层注入的快照与强制断开回调。
func (b *Broker) setConnHooks(entry *connEntry, probe func() plugin.ConnectionInfo, disconnect func(string)) {
	if entry == nil {
		return
	}
	b.connsMu.Lock()
	if probe != nil {
		entry.probe = probe
	}
	if disconnect != nil {
		entry.disconnect = disconnect
	}
	b.connsMu.Unlock()
}

// Connections 返回全部连接快照（按连接名排序，保证输出稳定）。
func (b *Broker) Connections() []ConnectionSnapshot {
	b.connsMu.RLock()
	entries := make([]*connEntry, 0, len(b.conns))
	for _, e := range b.conns {
		entries = append(entries, e)
	}
	b.connsMu.RUnlock()

	out := make([]ConnectionSnapshot, 0, len(entries))
	for _, e := range entries {
		// 内核侧字段在锁内取出；probe 回调在锁外调用（持锁调用外部回调是死锁风险最大的写法）。
		b.connsMu.RLock()
		probe := e.probe
		id, name, user, vhost, createdAt, remote := e.id, e.name, e.user, e.vhost, e.createdAt, e.remote
		b.connsMu.RUnlock()

		info := plugin.ConnectionInfo{}
		if probe != nil {
			info = probe()
		}
		snap := ConnectionSnapshot{
			ID:               id,
			Name:             name,
			User:             user,
			VHost:            vhost,
			Protocol:         info.Protocol,
			State:            "running",
			Channels:         len(info.Channels),
			ConnectedAt:      createdAt,
			FrameMax:         info.FrameMax,
			HeartbeatSeconds: info.HeartbeatSeconds,
			AuthMechanism:    info.AuthMechanism,
			ClientProperties: info.ClientProperties,
		}
		if remote != nil {
			snap.PeerHost, snap.PeerPort = splitAddr(remote)
		}
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Connection 返回指定连接名的快照。
func (b *Broker) Connection(name string) (ConnectionSnapshot, bool) {
	for _, c := range b.Connections() {
		if c.Name == name {
			return c, true
		}
	}
	return ConnectionSnapshot{}, false
}

// Channels 返回全部通道快照（按连接名、通道号排序）。
func (b *Broker) Channels() []ChannelSnapshot {
	b.connsMu.RLock()
	entries := make([]*connEntry, 0, len(b.conns))
	for _, e := range b.conns {
		entries = append(entries, e)
	}
	b.connsMu.RUnlock()

	var out []ChannelSnapshot
	for _, e := range entries {
		b.connsMu.RLock()
		probe := e.probe
		eid, ename, euser, evhost := e.id, e.name, e.user, e.vhost
		b.connsMu.RUnlock()
		if probe == nil {
			continue
		}
		info := probe()
		for _, ch := range info.Channels {
			out = append(out, ChannelSnapshot{
				ConnectionID:   eid,
				ConnectionName: ename,
				Number:         ch.Number,
				User:           euser,
				VHost:          evhost,
				State:          "running",
				ConsumerCount:  ch.ConsumerCount,
				PrefetchCount:  ch.PrefetchCount,
				Confirm:        ch.Confirm,
				Unacked:        ch.Unacked,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ConnectionName != out[j].ConnectionName {
			return out[i].ConnectionName < out[j].ConnectionName
		}
		return out[i].Number < out[j].Number
	})
	return out
}

// CloseConnection 强制关闭指定连接。返回是否命中。
//
// 先尽力发出 Connection.Close（320 CONNECTION_FORCED）再关底层连接：
// 客户端据此能给出"被运维关闭"而不是"网络抖动"的判断（对齐 RabbitMQ 行为）。
func (b *Broker) CloseConnection(name, reason string) bool {
	b.connsMu.RLock()
	entry, ok := b.conns[name]
	var disconnect func(string)
	if ok {
		disconnect = entry.disconnect
	}
	b.connsMu.RUnlock()
	if !ok {
		return false
	}
	if reason == "" {
		reason = "CONNECTION_FORCED - closed via management API"
	}
	if disconnect == nil {
		// 协议层还没注入断开回调（连接尚在握手中）：无法优雅关闭，
		// 只能等它自己超时。这里明确返回 false 让调用方报 404 而不是假装成功。
		return false
	}
	disconnect(reason)
	return true
}

// BlockedState 返回当前是否因资源水位阻塞（管理面展示用）。
func (b *Broker) BlockedState() bool { return b.flow.isBlocked() }

// DiskFree 返回数据目录所在卷的可用字节数（取不到时返回错误）。
func (b *Broker) DiskFree() (uint64, error) { return store.DiskFree(b.cfg.DataDir) }

// MemoryTotal 返回物理内存总量。
func (b *Broker) MemoryTotal() (uint64, bool) { return store.TotalMemory() }

// ProcessMemory 返回本进程向操作系统申请的内存字节数。
func (b *Broker) ProcessMemory() uint64 { return processMemory() }

// StorageLimits 返回当前流控阈值（内存水位比例、磁盘剩余下限字节）。
func (b *Broker) StorageLimits() (memoryWatermark float64, diskFreeLimit uint64) {
	return math.Float64frombits(b.memWatermark.Load()), b.diskLimit.Load()
}

// StorageFsync 返回当前落盘档位名（管理面展示用）。
func (b *Broker) StorageFsync() string { return b.stores.Options().Fsync.String() }

// ---------------------------------------------------------------------------
// 对象视图
// ---------------------------------------------------------------------------

// VHostNames 返回全部 vhost 名（已排序）。
func (b *Broker) VHostNames() []string {
	out := make([]string, 0, len(b.vhostList()))
	for _, v := range b.vhostList() {
		out = append(out, v.name)
	}
	sort.Strings(out)
	return out
}

// VHostExists 判断 vhost 是否存在。
func (b *Broker) VHostExists(name string) bool {
	_, ok := b.vhostOf(name)
	return ok
}

// VHostSnapshots 返回全部 vhost 快照（按名字排序）。
func (b *Broker) VHostSnapshots() []VHostSnapshot {
	names := b.VHostNames()
	out := make([]VHostSnapshot, 0, len(names))
	for _, name := range names {
		if v, ok := b.vhostOf(name); ok {
			out = append(out, v.snapshot())
		}
	}
	return out
}

// VHostSnapshot 返回单个 vhost 的快照。
func (b *Broker) VHostSnapshot(name string) (VHostSnapshot, bool) {
	v, ok := b.vhostOf(name)
	if !ok {
		return VHostSnapshot{}, false
	}
	return v.snapshot(), true
}

// QueueSnapshots 返回队列快照；vhost 为空表示全部 vhost。
func (b *Broker) QueueSnapshots(vhost string) []QueueSnapshot {
	if vhost != "" {
		v, ok := b.vhostOf(vhost)
		if !ok {
			return nil
		}
		return v.queueSnapshots()
	}
	var out []QueueSnapshot
	for _, name := range b.VHostNames() {
		if v, ok := b.vhostOf(name); ok {
			out = append(out, v.queueSnapshots()...)
		}
	}
	return out
}

// QueueSnapshot 返回单个队列的快照。
func (b *Broker) QueueSnapshot(vhost, name string) (QueueSnapshot, bool) {
	v, ok := b.vhostOf(vhost)
	if !ok {
		return QueueSnapshot{}, false
	}
	q, ok := v.getQueue(name)
	if !ok {
		return QueueSnapshot{}, false
	}
	// 走 vhost 的快照入口（而不是 q.snapshot）：远端队列的计数需要向 Owner 取。
	return v.queueSnapshot(q), true
}

// ExchangeSnapshots 返回交换机快照；vhost 为空表示全部 vhost。
func (b *Broker) ExchangeSnapshots(vhost string) []ExchangeSnapshot {
	if vhost != "" {
		v, ok := b.vhostOf(vhost)
		if !ok {
			return nil
		}
		return v.exchangeSnapshots()
	}
	var out []ExchangeSnapshot
	for _, name := range b.VHostNames() {
		if v, ok := b.vhostOf(name); ok {
			out = append(out, v.exchangeSnapshots()...)
		}
	}
	return out
}

// ExchangeSnapshot 返回单个交换机的快照。
//
// 默认交换机在管理 API 里用 "amq.default" 指代（RabbitMQ 的约定），
// 因此这里接受两种写法："" 与 "amq.default"。
func (b *Broker) ExchangeSnapshot(vhost, name string) (ExchangeSnapshot, bool) {
	v, ok := b.vhostOf(vhost)
	if !ok {
		return ExchangeSnapshot{}, false
	}
	ex, ok := v.getExchange(normalizeExchangeName(name))
	if !ok {
		return ExchangeSnapshot{}, false
	}
	return ex.snapshot(vhost), true
}

// BindingSnapshots 返回绑定快照；vhost 为空表示全部 vhost。
func (b *Broker) BindingSnapshots(vhost string) []BindingSnapshot {
	if vhost != "" {
		v, ok := b.vhostOf(vhost)
		if !ok {
			return nil
		}
		return v.bindingSnapshots()
	}
	var out []BindingSnapshot
	for _, name := range b.VHostNames() {
		if v, ok := b.vhostOf(name); ok {
			out = append(out, v.bindingSnapshots()...)
		}
	}
	return out
}

// QueueBindings 返回指向某队列的全部绑定。
func (b *Broker) QueueBindings(vhost, queue string) []BindingSnapshot {
	var out []BindingSnapshot
	for _, bd := range b.BindingSnapshots(vhost) {
		if bd.DestinationType == "queue" && bd.Destination == queue {
			out = append(out, bd)
		}
	}
	return out
}

// ExchangeSourceBindings 返回以某交换机为 source 的绑定。
func (b *Broker) ExchangeSourceBindings(vhost, exchange string) []BindingSnapshot {
	source := normalizeExchangeName(exchange)
	var out []BindingSnapshot
	for _, bd := range b.BindingSnapshots(vhost) {
		if bd.Source == source {
			out = append(out, bd)
		}
	}
	return out
}

// ConsumerSnapshots 返回消费者快照；vhost 为空表示全部 vhost。
func (b *Broker) ConsumerSnapshots(vhost string) []ConsumerSnapshot {
	if vhost != "" {
		v, ok := b.vhostOf(vhost)
		if !ok {
			return nil
		}
		return v.consumerSnapshots()
	}
	var out []ConsumerSnapshot
	for _, name := range b.VHostNames() {
		if v, ok := b.vhostOf(name); ok {
			out = append(out, v.consumerSnapshots()...)
		}
	}
	return out
}

// ObjectTotals 汇总对象总数。
func (b *Broker) ObjectTotals() ObjectTotals {
	var t ObjectTotals
	t.Connections = len(b.Connections())
	t.Channels = len(b.Channels())
	for _, v := range b.VHostSnapshots() {
		t.Queues += v.QueueCount
		t.Exchanges += v.ExchangeCount
		t.Consumers += v.ConsumerCount
	}
	return t
}

// QueueTotals 汇总队列消息总数。
func (b *Broker) QueueTotals() QueueTotals {
	var t QueueTotals
	for _, v := range b.VHostSnapshots() {
		t.Messages += v.Messages
		t.Ready += v.MessagesReady
		t.Unacknowledged += v.MessagesUnacked
	}
	return t
}

// MessageTotals 汇总消息累计计数。
//
// 口径说明：以**队列**为统计点累加（消息真正进入队列才算 publish），
// 因此"未命中任何队列的发布"不计入 —— 与 RabbitMQ 在交换机的计数口径存在差异，
// 属于已知偏离，已在 M5 交付说明中记录。
func (b *Broker) MessageTotals() MessageTotals {
	var t MessageTotals
	for _, v := range b.VHostSnapshots() {
		t.Publish += v.Published
		t.Deliver += v.Delivered
		t.Ack += v.Acked
	}
	return t
}

// normalizeExchangeName 把管理 API 的 "amq.default" 归一为内核里的默认交换机名（空串）。
func normalizeExchangeName(name string) string {
	if name == "amq.default" {
		return defaultExchange
	}
	return name
}

// splitAddr 把 net.Addr 拆成主机与端口。
func splitAddr(addr net.Addr) (string, int) {
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String(), 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 0
	}
	return host, port
}

// ---------------------------------------------------------------------------
// 管理面运行期操作
// ---------------------------------------------------------------------------

// SessionFor 返回绑定到指定 vhost、按指定用户权限校验的操作面（管理面用）。
//
// 关键点：管理面执行 publish / get / purge / delete 时，走的是**与客户端完全相同的
// 内核语义与权限检查**，而不是另开一条特权旁路 —— 否则"管理面能做的操作"会与
// AMQP 客户端能做的逐渐分叉，最终演变成两套语义。
func (b *Broker) SessionFor(user, vhost string) (plugin.Session, error) {
	vh, ok := b.vhostOf(vhost)
	if !ok {
		return nil, plugin.Errorf(plugin.KindNotFound,
			"NOT_FOUND - vhost '%s' not found", vhost)
	}
	perm, err := compilePermission(b.auth, user, vhost)
	if err != nil {
		return nil, err
	}
	id := fmt.Sprintf("mgmt-%d", b.sessions.Add(1))
	// 管理面不做发布侧阻塞：HTTP 请求挂住对运维来说是"界面卡住"，
	// 远不如直接返回 503 + 原因来得可诊断（资源水位本身在 /api/overview 里可见）。
	sess := newVHostSession(vh, id, user, perm, b.log, func() error {
		if b.flow.isBlocked() {
			return fmt.Errorf("资源水位超限，暂不接受发布（可在 /api/overview 查看水位状态）")
		}
		return nil
	})
	return sess, nil
}

// UserSnapshots 返回用户及其权限的视图数据。
func (b *Broker) UserSnapshots() []UserSnapshot {
	out := make([]UserSnapshot, 0)
	for _, name := range b.auth.UserNames() {
		u, ok := b.auth.User(name)
		if !ok {
			continue
		}
		out = append(out, userSnapshot(name, u))
	}
	return out
}

// UserSnapshot 是用户的只读视图。
type UserSnapshot struct {
	Name         string
	Tags         []string
	RemoteAccess bool
	// Root 表示这是内置总管理员账号（不可删除/禁用/降级）。
	Root bool
	// Disabled 表示账号已被禁用（不能登录）。
	Disabled bool
	// MustChangePassword 表示该账号首次登录须先改账号名/口令。
	MustChangePassword bool
	// APIGroups 是该账号被允许访问的管理接口功能组；为空表示不限制。
	APIGroups []string
}

// userSnapshot 由用户记录组装只读视图（口令不出现在视图里）。
func userSnapshot(name string, u config.User) UserSnapshot {
	return UserSnapshot{
		Name:               name,
		Tags:               append([]string(nil), u.Tags...),
		RemoteAccess:       u.RemoteAccess,
		Root:               u.Root,
		Disabled:           u.Disabled,
		MustChangePassword: u.MustChangePassword,
		APIGroups:          append([]string(nil), u.APIGroups...),
	}
}

// User 返回单个用户。
func (b *Broker) User(name string) (UserSnapshot, bool) {
	u, ok := b.auth.User(name)
	if !ok {
		return UserSnapshot{}, false
	}
	return userSnapshot(name, u), true
}

// VerifyUser 校验管理 HTTP API 的 Basic Auth 凭证。
func (b *Broker) VerifyUser(user, password string, remote net.Addr) (UserSnapshot, error) {
	rec, err := b.auth.Verify(user, password, remote)
	if err != nil {
		return UserSnapshot{}, err
	}
	return userSnapshot(user, rec), nil
}

// UserWrite 是写账号时的字段集合。
//
// 用结构体而不是一串参数：字段会继续长（root / disabled / 待改密 / 功能组…），
// 而多个布尔参数连在一起时，调用点读起来就是 `false, true, false`，极易写反。
type UserWrite struct {
	Name      string
	Tags      []string
	APIGroups []string
	// Password 留空表示**保留原口令**（更新场景下"不填密码框"就不该把口令清空）。
	Password string
	Disabled bool
	// MustChangePassword 为 true 时该账号下次登录须先改密。
	MustChangePassword bool
}

// UpsertUser 新建或更新用户（只写"口令 + 标签"，其余字段保持原状）。
//
// 保留既有调用方（含测试）的签名；管理面要写全字段时用 PutUser。
func (b *Broker) UpsertUser(name, password string, tags []string) error {
	w := UserWrite{Name: name, Password: password, Tags: tags}
	if old, ok := b.auth.User(name); ok {
		w.APIGroups = old.APIGroups
		w.Disabled = old.Disabled
		w.MustChangePassword = old.MustChangePassword
	}
	_, err := b.PutUser(w)
	return err
}

// PutUser 新建或整体更新一个用户（管理面的写路径）。
//
// Root 标记不在这里设置：总账号归属只由首次播种与启动兜底决定，
// 允许经管理 API 提升会让"总账号"退化成一个人人可加的标签。这里只**继承**既有标记。
// 返回 created 表示本次是新建。
func (b *Broker) PutUser(w UserWrite) (created bool, err error) {
	if strings.TrimSpace(w.Name) == "" {
		return false, plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - 用户名不能为空")
	}
	old, existed := b.auth.User(w.Name)
	rec := meta.User{
		Name: w.Name,
		// 管理 API 创建的账号一律允许远端登录（与既有语义一致）。
		RemoteAccess:       true,
		Tags:               append([]string(nil), w.Tags...),
		APIGroups:          append([]string(nil), w.APIGroups...),
		Root:               old.Root,
		Disabled:           w.Disabled,
		MustChangePassword: w.MustChangePassword,
	}
	if !existed || w.Password != "" {
		rec.Password = w.Password
	}
	if err := b.submitMeta(meta.OpPutUser, rec); err != nil {
		return false, err
	}
	if err := b.awaitMeta(func() bool { _, ok := b.auth.User(w.Name); return ok }); err != nil {
		return false, err
	}
	return !existed, nil
}

// RenameUser 把账号改名，连同权限记录与 root / 禁用标记一起迁移；password 非空时同时改口令。
//
// 账号名是用户记录的主键，改名没有"原子改名"这个原语，实现是三步元数据提交：
// 写新记录 → 逐条迁权限 → 删旧记录。三个 Op 都幂等，重放不会产生额外效果，
// 因此不需要为它新增一个元数据操作类型（新增 Op 反而要把 fsm / 快照 / 集群一起改）。
//
// 改名或改口令都意味着"凭据已经换过"，因此会清掉 must_change_password。
func (b *Broker) RenameUser(oldName, newName, password string) error {
	if strings.TrimSpace(newName) == "" {
		return plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - 新用户名不能为空")
	}
	old, ok := b.auth.User(oldName)
	if !ok {
		return plugin.Errorf(plugin.KindNotFound, "NOT_FOUND - 用户 %s 不存在", oldName)
	}
	if oldName == newName {
		if password == "" {
			return nil
		}
		_, err := b.PutUser(UserWrite{
			Name: newName, Password: password, Tags: old.Tags,
			APIGroups: old.APIGroups, Disabled: old.Disabled,
		})
		return err
	}
	if _, exists := b.auth.User(newName); exists {
		return plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - 用户 %s 已存在", newName)
	}

	rec := meta.User{
		Name:         newName,
		Password:     old.Password,
		Tags:         append([]string(nil), old.Tags...),
		APIGroups:    append([]string(nil), old.APIGroups...),
		RemoteAccess: old.RemoteAccess,
		Root:         old.Root,
		Disabled:     old.Disabled,
	}
	if password != "" {
		rec.Password = password
	}
	if err := b.submitMeta(meta.OpPutUser, rec); err != nil {
		return err
	}
	for _, p := range b.metaPermissionsOf(oldName) {
		if err := b.submitMeta(meta.OpPutPermission, meta.Permission{
			User: newName, VHost: p.VHost,
			Configure: p.Configure, Write: p.Write, Read: p.Read,
		}); err != nil {
			return err
		}
		if err := b.submitMeta(meta.OpDeletePermission, meta.Permission{User: oldName, VHost: p.VHost}); err != nil {
			return err
		}
	}
	if err := b.submitMeta(meta.OpDeleteUser, meta.User{Name: oldName}); err != nil {
		return err
	}
	return b.awaitMeta(func() bool {
		_, ok := b.auth.User(newName)
		_, stale := b.auth.User(oldName)
		return ok && !stale
	})
}

// configRootUserName 返回配置里应当作为总账号的账号名。
//
// 优先取显式标记 root 的账号；一个都没有时退回"按名字排序的第一个 administrator"，
// 这样配置文件没写 root 的部署也能自动获得一个总账号（否则保护规则对它形同虚设）。
// 返回空串表示配置里根本没有 administrator。
func (b *Broker) configRootUserName() string {
	names := make([]string, 0, len(b.cfg.Users))
	for name := range b.cfg.Users {
		names = append(names, name)
	}
	sort.Strings(names)
	fallback := ""
	for _, name := range names {
		u := b.cfg.Users[name]
		if u.Root {
			return name
		}
		if fallback == "" && hasTag(u.Tags, adminTag) {
			fallback = name
		}
	}
	return fallback
}

// hasRootUser 判断当前用户表里是否已有总账号。
func (b *Broker) hasRootUser() bool {
	for _, name := range b.auth.UserNames() {
		if u, ok := b.auth.User(name); ok && u.Root {
			return true
		}
	}
	return false
}

// adoptConfigRootUser 是**升级兜底**：元数据里没有任何 root 标记时，把配置里的初始账号补标为 root。
//
// 只在内存视图上补、不写元数据 —— 集群下这个视图可能还没追平，回写会覆盖他人的真实记录；
// 内存兜底则每次启动按同一条规则重算，结果一致。首次安装不走这条路径（播种时已置位）。
func (b *Broker) adoptConfigRootUser() {
	if b.hasRootUser() {
		return
	}
	name := b.configRootUserName()
	if name == "" {
		return
	}
	rec, ok := b.auth.User(name)
	if !ok {
		b.log.Warn("元数据里没有总管理员账号，且配置里的初始账号已不存在；请在管理面核对账号与权限",
			"expected", name)
		return
	}
	rec.Root = true
	if err := b.auth.ApplyUser(name, rec); err != nil {
		b.log.Warn("补标总管理员账号失败", "user", name, "err", err)
		return
	}
	b.log.Info("已把配置里的初始账号补标为总管理员（升级兜底）", "user", name)
}

// DeleteUser 删除用户（连同其权限记录）。返回是否命中。
//
// 权限必须显式逐条删除：元数据里的 delete 不级联（见 meta/fsm.go 的 applyToState），
// 而权限是独立记录 —— 只删用户会在元数据里留下悬空的权限记录。
func (b *Broker) DeleteUser(name string) (bool, error) {
	if _, ok := b.auth.User(name); !ok {
		return false, nil
	}
	for _, rec := range b.metaPermissionsOf(name) {
		if err := b.submitMeta(meta.OpDeletePermission, meta.Permission{User: name, VHost: rec.VHost}); err != nil {
			return false, err
		}
	}
	if err := b.submitMeta(meta.OpDeleteUser, meta.User{Name: name}); err != nil {
		return false, err
	}
	if err := b.awaitMeta(func() bool { _, ok := b.auth.User(name); return !ok }); err != nil {
		return false, err
	}
	return true, nil
}

// metaPermissionsOf 返回元数据里某用户的全部权限记录。
//
// 以**元数据状态**而不是本地 auth 视图为准：后者可能因复制延迟而少几条，
// 少删的那几条会在快照恢复时变成悬空记录（引用了已不存在的用户）。
func (b *Broker) metaPermissionsOf(user string) []meta.Permission {
	if b.meta == nil {
		return nil
	}
	var out []meta.Permission
	for _, rec := range b.meta.State().Permissions {
		if rec.User == user {
			out = append(out, rec)
		}
	}
	return out
}

// PermissionSnapshots 返回全部权限记录。
func (b *Broker) PermissionSnapshots() []PermissionSnapshot {
	perms := b.auth.AllPermissions()
	out := make([]PermissionSnapshot, 0, len(perms))
	for _, p := range perms {
		out = append(out, PermissionSnapshot{
			User: p.User, VHost: p.VHost,
			Configure: p.Configure, Write: p.Write, Read: p.Read,
		})
	}
	return out
}

// PermissionSnapshot 是权限记录的只读视图。
type PermissionSnapshot struct {
	User      string
	VHost     string
	Configure string
	Write     string
	Read      string
}

// UserPermissions 返回某用户的全部权限。
func (b *Broker) UserPermissions(user string) []PermissionSnapshot {
	var out []PermissionSnapshot
	for _, p := range b.PermissionSnapshots() {
		if p.User == user {
			out = append(out, p)
		}
	}
	return out
}

// VHostPermissions 返回某 vhost 上的全部权限。
func (b *Broker) VHostPermissions(vhost string) []PermissionSnapshot {
	var out []PermissionSnapshot
	for _, p := range b.PermissionSnapshots() {
		if p.VHost == vhost {
			out = append(out, p)
		}
	}
	return out
}

// SetPermission 设置权限。正则非法时返回错误（避免"配了却永远拒绝"的静默故障）。
//
// 校验在**提交之前**做：非法正则会跟着日志复制到全集群，事后没法收回，
// 只能靠再写一条正确记录覆盖。
func (b *Broker) SetPermission(user, vhost, configure, write, read string) error {
	perm := config.Permission{Configure: configure, Write: write, Read: read}
	if err := validatePermission(perm); err != nil {
		return plugin.Errorf(plugin.KindPreconditionFailed, "PRECONDITION_FAILED - %v", err)
	}
	if _, ok := b.auth.User(user); !ok {
		return plugin.Errorf(plugin.KindNotFound, "NOT_FOUND - 用户 %s 不存在", user)
	}
	rec := meta.Permission{User: user, VHost: vhost, Configure: configure, Write: write, Read: read}
	if err := b.submitMeta(meta.OpPutPermission, rec); err != nil {
		return err
	}
	return b.awaitMeta(func() bool {
		p, ok := b.auth.Permissions(user, vhost)
		return ok && p == perm
	})
}

// DeletePermission 删除权限。返回是否命中。
func (b *Broker) DeletePermission(user, vhost string) (bool, error) {
	if _, ok := b.auth.Permissions(user, vhost); !ok {
		return false, nil
	}
	if err := b.submitMeta(meta.OpDeletePermission, meta.Permission{User: user, VHost: vhost}); err != nil {
		return false, err
	}
	if err := b.awaitMeta(func() bool { _, ok := b.auth.Permissions(user, vhost); return !ok }); err != nil {
		return false, err
	}
	return true, nil
}

// validatePermission 预编译权限正则，提前暴露写错的模式。
func validatePermission(p config.Permission) error {
	if _, err := newPermissionSet(p); err != nil {
		return err
	}
	return nil
}
