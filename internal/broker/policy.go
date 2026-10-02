package broker

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/houzch/swiftmq/internal/meta"
)

// 本文件实现 RabbitMQ 的 policy（策略）：把一组配置按**名称匹配**统一施加到一批队列/交换机上。
//
// 与 M8-4 的账号一致，策略记录也放在元数据层：单机落 state.json、集群经 Raft 复制、
// 重启后保留。区别在于"生效"这一步 —— 账号只需换掉内存里的一张表，
// 策略要**回到已有对象上重新计算**：运维改一条策略，现存的队列必须立刻跟着变，
// 否则"即时生效"就是空话（RabbitMQ 是即时生效的）。
//
// 与队列自身参数的关系：队列声明里显式写了同名参数时，**以队列为准**（对齐 RabbitMQ）——
// 策略是"批量默认值"，不该偷偷盖掉某个队列的显式声明。
//
// 只支持能真正落地的键：不支持的在**创建策略时**就明确报错（见 validatePolicyDefinition），
// 而不是存下来当摆设。

// 策略定义里的键名。与 RabbitMQ 管理 API 一致：**不带 x- 前缀**（落到队列参数时才补回）。
const (
	polMaxLength      = "max-length"
	polMaxLengthBytes = "max-length-bytes"
	polMessageTTL     = "message-ttl"
	polExpires        = "expires"
	polDeadLetterEx   = "dead-letter-exchange"
	polDeadLetterKey  = "dead-letter-routing-key"
	polOverflow       = "overflow"
	// polAlternateExchange 是交换机侧的键：消息在该交换机上未命中任何队列时的兜底交换机。
	polAlternateExchange = "alternate-exchange"
)

// PolicyApplyTo 是策略的作用对象取值（与 RabbitMQ 同名）。
const (
	ApplyToQueues        = "queues"
	ApplyToClassicQueues = "classic_queues"
	ApplyToQuorumQueues  = "quorum_queues"
	ApplyToExchanges     = "exchanges"
	ApplyToAll           = "all"
)

// objectKind 描述被策略作用的对象。
type objectKind struct {
	queue  bool // 队列（否则是交换机）
	quorum bool // 仲裁队列（仅队列有意义）
}

// compiledPolicy 是一条已编译的正则策略。
type compiledPolicy struct {
	rec meta.Policy
	re  *regexp.Regexp
}

// policySet 是本节点已知的全部策略（已按"生效顺序"排好）。
//
// 排序规则：priority 降序、其次名字降序 —— 于是 match 只需取第一个命中项，
// 同 priority 时 "名字大的胜" 是**确定**的（RabbitMQ 用"后创建的胜"，
// 但它不持久化创建顺序，重启后可能改变；这里换成可复现的规则）。
type policySet []compiledPolicy

// buildPolicySet 从元数据快照里编译出策略集合；正则非法的策略被跳过并告警
// （创建时就校验过，理论上到不了这里）。
func buildPolicySet(state meta.State, warn func(policy string, err error)) policySet {
	out := make(policySet, 0, len(state.Policies))
	for _, rec := range state.Policies {
		re, err := regexp.Compile(rec.Pattern)
		if err != nil {
			if warn != nil {
				warn(rec.Name, err)
			}
			continue
		}
		out = append(out, compiledPolicy{rec: rec, re: re})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].rec.Priority != out[j].rec.Priority {
			return out[i].rec.Priority > out[j].rec.Priority
		}
		return out[i].rec.Name > out[j].rec.Name
	})
	return out
}

// forVHost 返回作用于指定 vhost 的策略（保持生效顺序）。
func (s policySet) forVHost(vhost string) policySet {
	out := make(policySet, 0, len(s))
	for _, p := range s {
		if p.rec.VHost == vhost {
			out = append(out, p)
		}
	}
	return out
}

// match 返回作用于该对象的最优策略。
//
// 模式匹配用 Go 的 regexp.MatchString —— 与 RabbitMQ 一样是**非锚定**的
// （`^amq\.` 这种写法才是"从头匹配"，与 RabbitMQ 文档里的示例一致）。
func (s policySet) match(kind objectKind, name string) (meta.Policy, bool) {
	for _, p := range s {
		if !policyAppliesTo(p.rec.ApplyTo, kind) {
			continue
		}
		if !p.re.MatchString(name) {
			continue
		}
		return p.rec, true
	}
	return meta.Policy{}, false
}

// policyAppliesTo 判断某策略是否作用于该类对象。
func policyAppliesTo(applyTo string, kind objectKind) bool {
	switch applyTo {
	case ApplyToAll:
		return true
	case ApplyToQueues:
		return kind.queue
	case ApplyToClassicQueues:
		return kind.queue && !kind.quorum
	case ApplyToQuorumQueues:
		return kind.queue && kind.quorum
	case ApplyToExchanges:
		return !kind.queue
	}
	return false
}

// definitionToArguments 把策略定义转成队列参数（补 x- 前缀）。
//
// 只搬运本实现真正支持的键；非法键在创建策略时就被拒了（见 validatePolicyDefinition）。
func definitionToArguments(def map[string]any) map[string]any {
	if len(def) == 0 {
		return nil
	}
	out := make(map[string]any, len(def))
	for k, v := range def {
		switch k {
		case polMaxLength:
			out[argMaxLength] = v
		case polMaxLengthBytes:
			out[argMaxLengthBytes] = v
		case polMessageTTL:
			out[argMessageTTL] = v
		case polExpires:
			out[argQueueExpires] = v
		case polDeadLetterEx:
			out[argDeadLetterEx] = v
		case polDeadLetterKey:
			out[argDeadLetterKey] = v
		case polOverflow:
			out[argOverflow] = v
		}
	}
	return out
}

// mergeArguments 合并"策略参数"与"队列显式参数"：显式参数优先（对齐 RabbitMQ）。
func mergeArguments(policy, declared map[string]any) map[string]any {
	if len(policy) == 0 {
		return declared
	}
	if len(declared) == 0 {
		return policy
	}
	out := make(map[string]any, len(policy)+len(declared))
	for k, v := range policy {
		out[k] = v
	}
	for k, v := range declared {
		out[k] = v
	}
	return out
}

// copyDefinition 复制一份策略定义，避免管理面/快照持有元数据层内部 map 的引用。
func copyDefinition(def map[string]any) map[string]any {
	if len(def) == 0 {
		return nil
	}
	out := make(map[string]any, len(def))
	for k, v := range def {
		out[k] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// 校验（写路径：管理 API → 元数据）
// ---------------------------------------------------------------------------

// validatePolicyDefinition 校验一条策略的完整内容。
//
// 校验放在**创建时**而不是生效时：生效阶段（包括 Raft 重放）不允许失败，
// 而"策略里写了不被支持的东西"必须在写下去之前就报出来。
func validatePolicyDefinition(rec meta.Policy) error {
	if rec.Name == "" {
		return precondition("策略名不能为空")
	}
	if rec.Pattern == "" {
		return precondition("策略的 pattern 不能为空")
	}
	if _, err := regexp.Compile(rec.Pattern); err != nil {
		return precondition("策略 %s 的 pattern 不是合法正则: %v", rec.Name, err)
	}
	switch rec.ApplyTo {
	case ApplyToQueues, ApplyToClassicQueues, ApplyToQuorumQueues, ApplyToExchanges, ApplyToAll:
	case "":
		return precondition("策略 %s 缺少 apply-to", rec.Name)
	default:
		return precondition("策略 %s 的 apply-to 取值非法: %q（可选 %s / %s / %s / %s / %s）",
			rec.Name, rec.ApplyTo,
			ApplyToQueues, ApplyToClassicQueues, ApplyToQuorumQueues, ApplyToExchanges, ApplyToAll)
	}
	if rec.Priority < 0 {
		return precondition("策略 %s 的 priority 不能为负数", rec.Name)
	}

	kind := objectKind{queue: rec.ApplyTo != ApplyToExchanges}
	for k := range rec.Definition {
		if err := validatePolicyKey(k, rec.ApplyTo, kind); err != nil {
			return err
		}
	}
	// 定义本身还能不能解析成合法队列参数：复用声明路径的校验器，避免两套规则。
	if kind.queue {
		if _, err := parseQueueArgs(definitionToArguments(rec.Definition)); err != nil {
			return err
		}
	}
	return nil
}

// validatePolicyKey 校验单个策略键。
func validatePolicyKey(key, applyTo string, kind objectKind) error {
	if !kind.queue {
		if key == polAlternateExchange {
			return nil
		}
		return precondition("策略键 %q 不适用于交换机（目前只支持 %s）", key, polAlternateExchange)
	}
	switch key {
	case polMaxLength, polMaxLengthBytes, polMessageTTL, polExpires, polDeadLetterEx, polDeadLetterKey, polOverflow:
		return nil
	case polAlternateExchange:
		return precondition("策略键 %q 适用于交换机（apply-to=%s）", key, ApplyToExchanges)
	case "queue-type", argQueueType:
		// 队列类型在创建后不可变更：策略里给出它只会在"作用到已存在的队列"时无声失败。
		return precondition("策略不支持 queue-type：队列类型在创建后不可变更，请在声明队列时指定")
	case "max-priority":
		// 同理：优先级队列在创建后再改 max-priority 会破坏已有消息的排序契约。
		return precondition("策略不支持 max-priority：它在队列创建后不可变更，请在声明队列时指定")
	default:
		return precondition("策略键 %q 不被支持（支持：%s、%s、%s、%s、%s、%s、%s）",
			key, polMaxLength, polMaxLengthBytes, polMessageTTL, polExpires,
			polDeadLetterEx, polDeadLetterKey, polOverflow)
	}
}

// ---------------------------------------------------------------------------
// 读路径：管理面
// ---------------------------------------------------------------------------

// PolicySnapshot 是策略的只读视图。
type PolicySnapshot struct {
	VHost      string
	Name       string
	Pattern    string
	ApplyTo    string
	Definition map[string]any
	Priority   int
}

func (p PolicySnapshot) toRecord() meta.Policy {
	return meta.Policy{
		VHost: p.VHost, Name: p.Name, Pattern: p.Pattern,
		ApplyTo: p.ApplyTo, Definition: p.Definition, Priority: p.Priority,
	}
}

// Policies 返回全部 vhost 的策略（按 vhost、名字排序）。
func (b *Broker) Policies() []PolicySnapshot {
	st := b.metaState()
	out := make([]PolicySnapshot, 0, len(st.Policies))
	for _, rec := range st.Policies {
		out = append(out, policySnapshot(rec))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].VHost != out[j].VHost {
			return out[i].VHost < out[j].VHost
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// VHostPolicies 返回某个 vhost 的策略。
func (b *Broker) VHostPolicies(vhost string) []PolicySnapshot {
	var out []PolicySnapshot
	for _, p := range b.Policies() {
		if p.VHost == vhost {
			out = append(out, p)
		}
	}
	return out
}

// Policy 返回单条策略。
func (b *Broker) Policy(vhost, name string) (PolicySnapshot, bool) {
	rec, ok := b.metaState().Policies[meta.PolicyKey(vhost, name)]
	if !ok {
		return PolicySnapshot{}, false
	}
	return policySnapshot(rec), true
}

func policySnapshot(rec meta.Policy) PolicySnapshot {
	return PolicySnapshot{
		VHost: rec.VHost, Name: rec.Name, Pattern: rec.Pattern,
		ApplyTo: rec.ApplyTo, Definition: copyDefinition(rec.Definition), Priority: rec.Priority,
	}
}

// SetPolicy 新建或更新一条策略，并等待本地生效。
func (b *Broker) SetPolicy(pol PolicySnapshot) error {
	rec := pol.toRecord()
	if err := validatePolicyDefinition(rec); err != nil {
		return err
	}
	payload := rec
	payload.Definition = copyDefinition(rec.Definition)
	if err := b.submitMeta(meta.OpPutPolicy, payload); err != nil {
		return err
	}
	return b.awaitMeta(func() bool {
		cur, ok := b.metaState().Policies[meta.PolicyKey(rec.VHost, rec.Name)]
		return ok && policyEqual(cur, rec)
	})
}

// DeletePolicy 删除一条策略，并等待本地生效。返回是否命中。
func (b *Broker) DeletePolicy(vhost, name string) (bool, error) {
	if _, ok := b.Policy(vhost, name); !ok {
		return false, nil
	}
	rec := meta.Policy{VHost: vhost, Name: name}
	if err := b.submitMeta(meta.OpDeletePolicy, rec); err != nil {
		return false, err
	}
	if err := b.awaitMeta(func() bool {
		_, ok := b.metaState().Policies[meta.PolicyKey(vhost, name)]
		return !ok
	}); err != nil {
		return false, err
	}
	return true, nil
}

// policyEqual 判断两条策略是否等价（等待本地生效用）。
func policyEqual(a, b meta.Policy) bool {
	if a.Pattern != b.Pattern || a.ApplyTo != b.ApplyTo || a.Priority != b.Priority {
		return false
	}
	if len(a.Definition) != len(b.Definition) {
		return false
	}
	for k, v := range a.Definition {
		if other, ok := b.Definition[k]; !ok || fmt.Sprint(other) != fmt.Sprint(v) {
			return false
		}
	}
	return true
}

// metaState 返回元数据快照（b.meta 尚未打开时返回空状态，便于启动路径复用）。
func (b *Broker) metaState() meta.State {
	if b.meta == nil {
		return meta.State{Policies: map[string]meta.Policy{}}
	}
	return b.meta.State()
}

// ---------------------------------------------------------------------------
// 生效路径：把策略落到已有对象上
// ---------------------------------------------------------------------------

// effectiveQueueArguments 返回队列声明时应当生效的参数。
//
// 策略参数在前、队列显式参数在后：同名时显式参数覆盖策略（对齐 RabbitMQ）。
func (b *Broker) effectiveQueueArguments(vhost, name string, declared map[string]any) map[string]any {
	set := b.policySet()
	if len(set) == 0 {
		return declared
	}
	kind := objectKind{queue: true, quorum: queueKindFromArgs(declared) == queueTypeQuorum}
	rec, ok := set.forVHost(vhost).match(kind, name)
	if !ok {
		return declared
	}
	return mergeArguments(definitionToArguments(rec.Definition), declared)
}

// matchingPolicyFor 返回作用于某对象的策略（供声明路径记录展示信息）。
func (b *Broker) matchingPolicyFor(vhost string, kind objectKind, name string) (meta.Policy, bool) {
	return b.policySet().forVHost(vhost).match(kind, name)
}

// queueKindFromArgs 从声明参数里读出队列类型（策略不能改类型，因此这里只看客户端给的）。
func queueKindFromArgs(args map[string]any) string {
	if v, ok := stringArg(args, argQueueType); ok {
		return v
	}
	return queueTypeClassic
}

// queuePolicyArgs 计算队列的生效参数与命中的策略（声明与元数据恢复两条路径共用）。
//
// 两条路径共用同一段计算，是为了保证"新声明的队列"与"重启后重建的队列"参数完全一致 ——
// 分成两处实现，迟早会出现"重启后策略丢了"这种只在重启后才复现的问题。
func (b *Broker) queuePolicyArgs(vhost, name string, declared map[string]any) (queueArgs, string, map[string]any, error) {
	effective := b.effectiveQueueArguments(vhost, name, declared)
	args, err := parseQueueArgs(effective)
	if err != nil {
		return queueArgs{}, "", nil, err
	}
	kind := objectKind{queue: true, quorum: args.queueType == queueTypeQuorum}
	rec, ok := b.matchingPolicyFor(vhost, kind, name)
	if !ok {
		return args, "", nil, nil
	}
	return args, rec.Name, copyDefinition(rec.Definition), nil
}

// exchangePolicyArgs 返回交换机命中的策略与其中的 alternate-exchange。
func (b *Broker) exchangePolicyArgs(vhost, name string) (alt, policy string, def map[string]any) {
	rec, ok := b.matchingPolicyFor(vhost, objectKind{}, name)
	if !ok {
		return "", "", nil
	}
	if raw, has := rec.Definition[polAlternateExchange]; has {
		alt, _ = raw.(string)
	}
	if alt == name {
		alt = "" // 指向自己会造成路由死循环
	}
	return alt, rec.Name, copyDefinition(rec.Definition)
}

// applyPoliciesForVHost 重新计算某 vhost 内所有队列/交换机的策略归属并立即生效。
func (b *Broker) applyPoliciesForVHost(vhostName string) {
	v, ok := b.vhostOf(vhostName)
	if !ok {
		// 元数据引用了本节点不存在的 vhost：集群各节点 vhost 配置必须一致，
		// 这里只告警（与 applyBindingPut 的取舍一致：不能让一条记录拖垮应用循环）。
		b.log.Warn("策略引用了本节点不存在的 vhost，已跳过", "vhost", vhostName)
		return
	}
	v.applyPolicies(b.policySet().forVHost(vhostName))
}

// applyPolicies 把策略集合作用到本 vhost 的全部队列与交换机上。
//
// 每次都要重算**全部**对象：删除一条策略时，原本被它管着的对象必须回到"无策略"状态，
// 只做增量更新会漏掉这种情况。
func (v *vhost) applyPolicies(set policySet) {
	v.mu.RLock()
	queues := make([]*queue, 0, len(v.queues))
	for _, q := range v.queues {
		queues = append(queues, q)
	}
	exchanges := make([]*exchange, 0, len(v.exchanges))
	for _, e := range v.exchanges {
		exchanges = append(exchanges, e)
	}
	v.mu.RUnlock()

	for _, e := range exchanges {
		v.applyExchangePolicy(set, e)
	}
	for _, q := range queues {
		v.applyQueuePolicy(set, q)
	}
}

// applyQueuePolicy 重算单个队列的生效参数。
func (v *vhost) applyQueuePolicy(set policySet, q *queue) {
	kind := objectKind{queue: true, quorum: q.isQuorum()}
	rec, ok := set.match(kind, q.name)
	policyArgs := map[string]any(nil)
	if ok {
		policyArgs = definitionToArguments(rec.Definition)
	}
	effective := mergeArguments(policyArgs, q.arguments)
	args, err := parseQueueArgs(effective)
	if err != nil {
		// 策略创建时已校验过，这里失败只可能是"策略与队列自身参数叠加后非法"。
		// 不改变队列现有参数（宁可保持旧行为，也不要让队列变成半截配置）。
		v.log.Error("策略与队列参数叠加后非法，已忽略该策略",
			"queue", q.name, "policy", rec.Name, "err", err)
		return
	}
	name, def := "", map[string]any(nil)
	if ok {
		name, def = rec.Name, copyDefinition(rec.Definition)
	}
	q.applyPolicy(args, name, def)
	v.syncSweepSet(q)
}

// applyExchangePolicy 重算单个交换机的策略（目前只有 alternate-exchange）。
func (v *vhost) applyExchangePolicy(set policySet, e *exchange) {
	rec, ok := set.match(objectKind{}, e.name)
	alt, name, def := "", "", map[string]any(nil)
	if ok {
		if raw, has := rec.Definition[polAlternateExchange]; has {
			alt, _ = raw.(string)
		}
		name, def = rec.Name, copyDefinition(rec.Definition)
	}
	if alt == e.name {
		// 指向自己会造成路由死循环：与 RabbitMQ 一样忽略
		alt = ""
	}
	e.applyPolicy(alt, name, def)
}

// syncSweepSet 把队列加入/移出定时扫描集合（策略改变可能让它"需要"或"不再需要"扫描）。
func (v *vhost) syncSweepSet(q *queue) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if q.needsTimer() {
		v.sweepSet[q.name] = q
		return
	}
	delete(v.sweepSet, q.name)
}

// policySet 返回本节点当前已知的策略集合（原子快照，读侧无需加锁）。
func (b *Broker) policySet() policySet {
	if s := b.policies.Load(); s != nil {
		return *s
	}
	return nil
}

// refreshPolicies 用给定的元数据快照重建策略集合并把它作用到各 vhost。
//
// 两件事必须一起做：只更新集合、不落到对象上，运维改了策略却要等队列重建才生效。
func (b *Broker) refreshPolicies(state meta.State) {
	set := buildPolicySet(state, func(policy string, err error) {
		b.log.Error("策略的 pattern 不是合法正则，已跳过", "policy", policy, "err", err)
	})
	b.policies.Store(&set)
	for _, v := range b.vhostList() {
		b.applyPoliciesForVHost(v.name)
	}
}
