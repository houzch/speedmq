# SwiftMQ 备份与恢复

> 🌐 本文档提供多语言版本：[文档多语言索引](../i18n/README.md)

> 本文中的"实测"结论全部来自 **Windows + PowerShell 5.1** 上的一次真实演练（临时 `data_dir` 与临时端口）。
> 演练命令与关键输出原样贴在 §6。**【未验证】** 的部分会显式标注（集群备份/恢复、Docker 卷备份等）。

---

## 1. 要备份什么

`data_dir` 下**必须整份备份**，关键是下面这些（布局见 `upgrade.md` §3）：

| 路径 | 作用 | 丢了会怎样 |
| --- | --- | --- |
| `meta/state.json` | 单机元数据快照：vhost / 交换机 / 队列 / 绑定 / 用户 / 权限 / 策略 | 拓扑与账号全丢 |
| `meta/raft.log`、`meta/raft.state`、`meta/snapshot.json` | 【集群】Raft 日志 / 任期投票 / 快照+成员表 | 集群身份与元数据一致性丢失 |
| `meta/users.seeded`、`meta/vhosts.seeded` | 引导标记 | 丢了会让配置里的 users/vhosts 被**再次播种**（删掉的账号/vhost 复活） |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | 经典队列消息数据与索引 | 持久消息丢失 |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【集群】仲裁队列的 Raft 日志/快照 | 仲裁队列数据丢失 |
| 证书文件（配置里 `cert_file`/`key_file`/`ca_file` 指向的 PEM） | TLS 证书 | 与 `data_dir` 分开备份，重启后 TLS 起不来 |

> 软状态（未确认消息、消费者、prefetch 计数）**只在内存**、不落盘，备份**不包含**也不应包含它们。

---

## 2. 一致性要求：**必须先停进程**；热备**不安全**

### 2.1 结论

- ✅ **安全做法**：**停止 broker 进程**（优雅退出会做收尾刷盘），再复制 `data_dir`。
- ❌ **热备（进程在跑时直接复制文件）：不安全，不做保证。**

### 2.2 为什么热备不安全

消息存储是**两个文件**（段文件 `*.seg` 与索引文件 `index/*.idx`），二者**不是原子提交**：

- 恢复时**以索引为准**判断"哪些消息活着"，再按索引里的 `(段号, 偏移, 长度)` 去段文件读取。
- 热备时可能复制到**索引已引用、段文件还没写全**（或反之）的中间状态：
  - 索引引用了段里不存在的记录 → 该消息**读取失败被跳过**（等于丢了已确认的持久消息）；
  - 段里有记录但索引没引用 → 该消息**不被恢复**。
- 恢复虽然会用 CRC32 丢弃**尾部半写记录**，但那只覆盖"单文件尾部写坏"，**不能修复索引与段之间的不同步**。

### 2.3 关于"写入何时到磁盘"（实测观察）

- 默认 `fsync: os` + `flush_interval_ms: 200`：消息由后台刷盘协程在**至多约 200 ms** 内 `write()` 到操作系统
  （不 fsync），publisher confirm 也在此之后返回。
- 实测：发布持久消息后**立即**查段文件大小，已经能看到数据（`t=0ms seg=832`）；即"可被 OS 看到的字节"与 confirm 基本同步。
- **注意**：这只说明"到了 OS 缓冲"，**强杀进程不会丢**（进程被杀不丢 OS 缓冲），但**断电会丢**。
  要"收到 confirm 即已 fsync 落盘"，请把 `storage.fsync` 改成 `batch` / `always`。**【断电场景未实测】**

---

## 3. 备份步骤

### 3.1 单机（推荐）

```powershell
# 1) 停进程（前台：Ctrl+C；后台：Stop-Process）
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) 复制整个 data_dir（带时间戳）
$data = "C:\swiftmq\data"
Copy-Item -Recurse -Force $data "C:\backup\swiftmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) （可选）校验备份里元数据快照可解析
Get-Content "C:\backup\swiftmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 集群

- **每个节点各自备份自己的 `data_dir`**（元数据经 Raft 复制到全体，消息数据在 Owner 节点，仲裁队列副本在各自 Raft 目录）。
- 停机顺序：**一次只停一个节点**；不要同时停多个投票成员（见 `upgrade.md` §7.2）。
- 要得到**全集群一致快照**，需按顺序停全部节点后各自复制；生产中更常见的是"逐节点停/拷/起"。
- **【未验证】** 本机未做真实集群备份/恢复演练。

### 3.3 Docker（命名卷）

```powershell
# 停容器后，用一次性容器把卷内容打包复制出来
docker compose down
docker run --rm -v swiftmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/swiftmq-data.tar.gz -C /data .
```
> **【未验证】**（本机未跑 Docker）。

---

## 4. 恢复步骤

### 4.1 单机

```powershell
# 1) 确认进程已停
Get-Process -Name swiftmqd -ErrorAction SilentlyContinue

# 2) 移走（或删除）当前 data_dir，避免新旧文件混在一起
Move-Item "C:\swiftmq\data" "C:\swiftmq\data.broken"

# 3) 用备份还原
Copy-Item -Recurse -Force "C:\backup\swiftmq-YYYYMMDD-HHMMSS" "C:\swiftmq\data"

# 4) 启动
& "C:\swiftmq\swiftmqd.exe" -config "C:\swiftmq\configs\swiftmqd.json" -log-level info
```

要点：
- **必须先把旧目录移走**，不能"备份文件盖到半残留目录上"；
- 恢复的 `data_dir` 必须与备份时**同一 vhost/队列集合**（目录名是编码后的，跨机器可用）；
- **不要**借恢复之机去改配置文件里的 `vhosts`/`users`（只在首次引导生效，改了没用，见 `upgrade.md` §4.2）。

### 4.2 集群

- 恢复单个节点：按 §4.1 恢复该节点 `data_dir` 后启动，它作为既有成员重新加入并追平 Raft 日志。
- 恢复整个集群：**先恢复并启动多数派节点**（≥ 半数投票成员），集群才能选出 leader；再恢复其余节点。
- **【未验证】** 集群恢复未实测。

---

## 5. 恢复后的校验方法

用管理 API 与真实客户端交叉核对（推荐全做）：

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) 对象总数与消息总数（队列数/交换机数/绑定数/用户数；messages/ready/unacked）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) 逐队列核对 messages / messages_ready（可对照备份前记录）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vhost / 用户 / 策略 是否都在
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) 集群（单机会返回 enabled=false / mode=local / role=single）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **看启动日志**：应出现 `已从磁盘恢复队列消息 ... messages=N` 与 `队列已恢复持久化消息 ... messages=N`；N 应与备份前一致。
- **日志里的告警**：若某队列此前有消息被消费/清空过，恢复时可能出现
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` 与 `恢复时清理了无存活消息的段`。
  这些是**已结算（ack/purge）记录**的索引残留，属于**已知日志噪音，不影响数据正确性**（见 §7）。
- **真实客户端**：从队列取回消息并核对条数/内容（见 §6 第 (7) 步）。

---

## 6. 实测演练（真实命令与输出）

> 环境：`data_dir` 在临时目录，AMQP `127.0.0.1:5676`、管理面 `127.0.0.1:15677`、MQTT `127.0.0.1:1884`，
> 默认账号 `guest/guest`。启动日志：
> ```
> level=INFO msg="SwiftMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) 建 durable 拓扑 + 发 5 条持久消息（真实客户端 `amqp091-go`）**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) 建用户 / vhost / 权限 / 策略（管理 API）**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) 备份前状态（管理 API）**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) 备份前磁盘文件**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) 停进程 → 备份 → 清空 → 恢复**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) 重启后的恢复日志（关键行）**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> 同时出现若干条 `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`：
> 这些是本演练中**之前被 purge 掉的**消息留下的索引残留（已结算、段数据已回收），**不影响下面 5 条消息的恢复**。

**(7) 恢复后断言：管理 API + 真实客户端**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**结论**：durable 拓扑（交换机+队列+绑定）、5 条持久消息、用户、vhost、权限、策略**全部恢复**，
真实客户端能按原样取回全部消息。**演练通过。**

### 6.1 对照：未经历消费/purge 的队列恢复更"安静"

为区分上面的 WARN 是不是普遍现象，另做一次**受控对照**：新建 durable 队列 `clean.q`、发 3 条持久消息、
**不消费不清空**，停进程后重启：

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**无任何 WARN**。说明 WARN 只出现在"索引里还留着已结算记录"的场景（见 §7）。

---

## 7. 已知问题与限制（如实登记）

1. **恢复日志噪音（真实观察到）**：当队列历史上发生过消费/清空（消息已 ack/purge），
   其索引中仍保留对已回收记录的引用，恢复时会为每条**逐条打印 `恢复消息失败，已跳过` WARN**，
   并新建/清理一个空的 `000000.seg`（日志 `恢复时清理了无存活消息的段`）。
   **不影响数据正确性**（存活的未确认消息能正确恢复），但会**污染日志**、在大队列/高吞吐下可能刷屏。
   建议：以 `已从磁盘恢复队列消息 ... messages=N` 为准，忽略这些针对已结算记录的 WARN；
   若日志量不可接受，请反馈给内核维护者（本文档不修改代码）。
2. **热备不安全**（§2）：不要在进程运行时直接复制 `data_dir`。
3. **`fsync: os` 不保证断电不丢**：要"confirm 即落盘"请用 `batch` / `always`。
4. **口令明文**：`meta/state.json` 里用户口令是**明文**（实测可见 `"password":"drillpass"`）——
   备份文件因此**必须按敏感数据处理**（访问控制、加密存放）。详见 `security-baseline.md`。
5. **【未验证】** 集群备份/恢复、Docker 卷备份/恢复、断电场景、恢复过程中的并发写入。
