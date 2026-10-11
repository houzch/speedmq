# SpeedMQ 升级与迁移方案

> 🌐 本文档提供多语言版本：[文档多语言索引](../i18n/README.md)

> 适用版本：`1.0.0`（`broker.Version`，见 `/metrics` 的 `speedmq_build_info`）。
> 本文所有"实测"结论均来自本机真实运行；凡未实测的，均显式标注 **【未验证】**。
> 本机环境：Windows + PowerShell 5.1，Go 1.27.1 windows/386，临时 `data_dir` + 非默认端口。

---

## 1. 迁移（从 RabbitMQ 切到 SpeedMQ）

本项目定位是 **AMQP 0-9-1 协议级兼容**，因此"迁移"以**改连接地址**为主：

- 业务代码零改动，只改 `host/port/vhost`（设计文档 G3「迁移零成本」）。
- 管理工具链（`rabbitmqadmin`、管理 UI、监控脚本）指向管理面端口即可，接口形状对齐 RabbitMQ（`amq.default`、`%2F`、`{error, reason}` 等约定照搬）。
- 默认端口与 RabbitMQ 一致：AMQP `5672`、管理面 `15672`；MQTT 为 `1883`，节点间 RPC 为 `25672`。

**迁移前需自查的语义差异**（均为本仓库有意为之，依据 README / 设计文档）：

| 项 | SpeedMQ 行为 | 迁移影响 |
| --- | --- | --- |
| 瞬时（非持久且非独占）队列 | **拒绝声明**（541），`auto_delete` 不豁免 | 老客户端若依赖该类队列会失败，需改为 durable 或 exclusive |
| 默认 vhost `/` | **不可删除**（400），RabbitMQ 允许 | 自动化脚本若删默认 vhost 会失败（这是唯一主动安全约束） |
| 经典队列数据 | **不复制**，数据只在 Owner 节点 | 需要跨节点冗余请改用仲裁队列 `x-queue-type=quorum` |
| 仲裁队列 | 支持扩副本，**不支持缩容** | 规划时一次到位 |
| 插件 | 无 Erlang 插件生态，AMQP 1.0 / STOMP 未实现 | 用到这些协议的场景暂不可迁 |

**数据迁移**：SpeedMQ 与 RabbitMQ 存储格式不兼容，**不提供在线/离线数据搬运工具**。
迁移方式为"新建空 SpeedMQ → 双跑验证 → 灰度切流"。**【未验证】** 本文不含任何真实 RabbitMQ 数据搬运演练。

---

## 2. 升级总原则

1. **先备份**（见 `backup-restore.md`）——升级失败的兜底。
2. **先停进程再替换**（数据目录有单写者约束，见 §4.2）。
3. **升级后必须校验**：进程能起、`/api/overview` 能读、`/metrics` 能抓、队列消息数与备份前一致。
4. 集群升级**逐节点滚动**，一次只动一个节点（见 §5）。

---

## 3. 数据目录布局（升级/迁移的事实依据）

以本机单机实例**实测**得到的 `data_dir` 布局：

```
data/
├── meta/
│   ├── state.json        # 单机模式的元数据快照（vhost/交换机/队列/绑定/用户/权限/策略）
│   ├── users.seeded      # 引导标记：配置文件里的 users 已播种过
│   ├── vhosts.seeded     # 引导标记：配置文件里的 vhosts 已播种过
│   ├── raft.state        # 【集群模式】Raft 任期/投票
│   ├── raft.log          # 【集群模式】Raft 日志
│   └── snapshot.json     # 【集群模式】Raft 快照 + 成员表
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # 段文件（消息体 + 属性），记录格式：<len u32><crc32 u32><payload>
│   └── index/000001.idx  # 队列索引：seq-id → (段号, 段内偏移, 长度, 状态)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【集群】仲裁队列每队列一个 Raft 组（日志/快照）
```

**注意（与直觉不同的两点，均以代码/实测为准）**：

- 集群模式下 Raft 持久化文件**直接放在 `meta/` 下**（`raft.state` / `raft.log` / `snapshot.json`），
  **不存在 `meta/raft/` 子目录**。依据：`internal/raft/log.go` 的文件名常量 + `internal/broker/cluster.go`
  里 `Dir: filepath.Join(b.cfg.DataDir, "meta")`。**【已验证（2026-10-10，本机 Docker Desktop 3 节点集群）】** —— 集群节点卷备份内容直接可见 `meta/raft.log`、`meta/raft.state`、`meta/*.seeded`（未见 `meta/raft/` 子目录）。
- 目录名**不是原始 vhost / 队列名**，而是 `store.SafeDirName` 编码：加前缀 `q_`，非 `[A-Za-z0-9._-]` 字节按 `%XX` 转义。
  实测：vhost `/` → 目录 `q_%2F`，队列 `persist.q` → 目录 `q_persist.q`。
  这样设计是为避免路径穿越与 Windows 保留设备名（`con`/`nul` 等）。

---

## 4. 数据兼容性

### 4.1 旧数据能否直接读——能

- **索引格式向前兼容**：M8-1 在索引记录里加了"段号"字段（25 字节）；**旧格式（21 字节，无段号）仍可原样读取**，
  读取时等价于"只有一个段（seg=1）"，**升级不需要迁移脚本**。
  依据：`internal/store/store.go` 的 `indexEntrySize` / `legacyIndexEntrySize` 常量与 `recover()` 逻辑；README M8-1。
- **崩溃语义不变**：每条记录带长度前缀 + CRC32，恢复时**丢弃尾部半写/损坏记录**并截断。
  实测（见 `backup-restore.md` §6）：进程停止后重启，durable 队列的 5 条持久消息**全部恢复**，
  日志出现 `已从磁盘恢复队列消息 ... messages=5`。

**【已验证（2026-10-10，本机）】** 跨大版本读取已实测：用 tag `v1.1.0` 写入的 durable 队列数据在 `1.1.05` 下原样恢复（命令与输出见 §5）。

### 4.2 配置里 `vhosts` / `users` 的口径（升级时最容易踩的坑）

- 二者**只在首次引导时生效**：首次启动会把配置里的 vhosts/users 写进元数据并落下标记文件
  `meta/vhosts.seeded` / `meta/users.seeded`；**此后以元数据为准**。
- 因此**升级/换配置时，不要指望通过改配置文件来增删账号或 vhost**——改了也不生效；
  请用管理 API 或 `speedmqctl`。
- 反过来，升级**不会**用配置覆盖已有账号：运行期改过的口令不会被重启打回配置里的旧值，
  运行期删掉的账号也不会复活。依据：`cluster.go` 的播种标记逻辑；README M8-4 / M8-7。

**【已验证（2026-10-10，本机）】** 运行期改动确实不被配置覆盖：升级前经管理 API 把 `guest` 口令改为 `rotated`，
用 `1.1.05` + 同一 `data_dir` 重启后，`guest/guest`（配置里的旧口令）返回 **401**、`guest/rotated` 返回 **200**。

### 4.3 段轮转与磁盘回收

- 消息按大小分段（默认 8 MiB），**段内消息全部 ack 且段已封口后整段删除**，索引随之压缩重写。
- 升级不改动该行为；旧实例留下的单段文件在新的段轮转逻辑下照常工作。

---

## 5. 二进制升级（裸机）

> **【已验证（2026-10-10，本机）】** 跨大版本真机演练：用 tag `v1.1.0` 构建的旧二进制写入
> durable 交换机/队列/绑定 + 5 条持久消息 + vhost/账号/权限/策略并运行期改口令；停进程后改用当前 `1.1.05`
> 二进制 + **同一 `data_dir` + 同一端口**启动 —— 进程正常起、`/api/queues` 对象与消息数一致、真实 AMQP
> 客户端取回全部 5 条持久消息、`/metrics` 报 `speedmq_build_info{version="1.1.05"}`。
> 以下步骤仍为通用流程，可套用于任意相邻版本；**降级**见 §8。

### 5.1 步骤

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) 停进程（优雅退出会收尾刷盘；见 §4「一致性」）
#    若以前台方式运行：Ctrl+C；若以服务方式：Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) 备份数据目录（务必在进程停止后）
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) 替换二进制（把新版本 speedmqd.exe / speedmqctl.exe 放到原路径）
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) 启动
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) 校验：进程存活 + 管理 API 能读
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 升级后校验清单

- 启动日志出现 `SpeedMQ 启动中 ... version=<新版本>` 与 `管理面已启动`；
- `/api/overview` 的 `object_totals` / `queue_totals` 与备份前一致（对照 `backup-restore.md` §5）；
- `/api/queues` 中每条 durable 队列的 `messages` / `messages_ready` 与备份前一致；
- `/metrics` 可抓取且 `speedmq_plugin_up{name="amqp091"} 1`、`{name="mqtt"} 1`。

---

## 6. 镜像升级（容器）

镜像约 13 MB（静态链接二进制 + alpine），**以非 root（uid 10001）运行**，数据目录挂载在 `/var/lib/speedmq`。

```powershell
# 1) 拉/构建新镜像（tag 用新版本号，避免 old/new 混淆）
docker build -t speedmq:1.0.0 .

# 2) 停旧容器（compose 会保留命名卷 speedmq-data）
docker compose down

# 3) 起新版本（compose 文件里把 image 改成新 tag）
docker compose up -d

# 4) 状态与日志
docker compose ps
docker compose logs -f --tail 100
```

> **容器内一次性任务**（例如在容器内跑 `speedmqctl`）：`docker compose ...` 的 `run` 在非交互环境必须加 `-T`，
> 否则会因申请 TTY 失败：
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

数据持久化依赖 compose 的**命名卷** `speedmq-data`，容器重建不丢数据（M4 起真正落盘）。
升级前若需备份卷内容，等价于备份 `/var/lib/speedmq`（见 `backup-restore.md` §3.2）。

> **【已验证（2026-10-10，本机 Docker Desktop）】** 已做真实镜像升级演练：本地构建旧镜像 `speedmq:1.1.0-old`（git tag `v1.1.0`）与新镜像 `speedmq:1.1.05-new`（当前源码），用单机 compose 起旧镜像 → 建 durable 队列 `persist.q` 并发布 5 条持久消息（`delivery_mode=2`）→ `docker compose down`（**不加 `-v`**，命名卷 `speedmq-data` 保留）→ compose 的镜像 tag 换成新镜像后 `docker compose up -d`。
> 结果：新容器正常起、启动日志出现 `已从磁盘恢复队列消息 ... queue=persist.q messages=5`、`/metrics` 报 `speedmq_build_info{version="1.1.05"}`、`/api/overview` 的 `queue_totals.messages=5` 与队列 `persist.q` 的 `messages=5` 升级前后一致，且真实 AMQP 客户端（`rabbitmq/amqp091-go`）逐条取回全部 5 条持久消息（`deliveryMode=2`）。
> 注：`v1.1.0` 是改名前的 `speedmq` 品牌镜像（二进制 `speedmqd`、指标 `speedmq_build_info`、默认数据目录 `/var/lib/speedmq`），与现版命名不同；演练中以配置 `data_dir=/var/lib/speedmq` 为准、并为命名卷预置属主（旧镜像内无该可写路径）。这不影响"换 tag 重建容器、命名卷数据不丢"的结论。

---

## 7. 灰度与回滚

### 7.1 单机

- **灰度**：SpeedMQ 单机没有内建"新旧同进程双版本"能力。可行的灰度是**旁路影子**：
  新版本实例先用**只读消费/影子队列**挂到同一份上游流量上观察，确认无误后再切换写入方。
- **回滚**：
  1. 停新版本进程；
  2. 换回旧二进制；
  3. 若新版本已写入过数据，**必须用升级前备份恢复 `data_dir`**（见下）。
  **不会**出现"新版本已写过、旧版本直接读"的保证——跨版本降级见 §8。

### 7.2 集群（滚动升级）

平台侧不提供"一键滚动升级"，需按下面顺序人工逐个节点操作：

1. **一次只升级一个节点**：停该节点 → 备份其 `data_dir` → 换二进制 → 启动 → 等待它重新加入并追平
   （`speedmqctl cluster_status` / `GET /api/cluster` 看 `role`、`commit_index`/`last_applied`）。
2. **顺序建议**：先升 **learner / 非投票成员**（对多数派无影响），再升 **follower**，最后升 **leader**
   （升级 leader 会触发一次选主，期间有短暂不可写）。
3. **停机对多数派的影响**（关键）：
   - 3 节点集群：**同时最多停 1 个**投票成员，停 2 个即失去多数派，`pause_minority` 下**整个集群暂停服务**。
   - 2 节点集群：停 1 个就失去多数派，**不具备滚动升级能力**（建议至少 3 节点）。
   - 因此滚动升级时**严禁一次停多个投票成员**。
4. **成员变更与升级不要并做**：成员变更**无 joint consensus**，一次只允许一个未提交的配置变更；
   升级期间请避免同时 `add_member` / `remove_member`。
5. 升级完成后核对 `GET /api/cluster` 的 `object_totals` 与升级前一致。

> **【已验证（2026-10-10，本机 Docker Desktop 3 节点集群）】** 已做真实集群的**跨版本**滚动升级演练：基线为本地构建的
> `speedmq:1.1.0-old`（git tag `v1.1.0`），逐节点（n1 → n2 → n3，一次一个：先停、再备份其命名卷、再换镜像启动）升级到当前 `1.1.05`
> （本地构建 `speedmq:1.1.05-new`，compose 用逐节点 override 切镜像）。全程集群始终保有 leader 与多数派——停当前 leader 时约 0.6 s 选出新 leader、
> `pause_minority` 未触发暂停、停机期间无外部中断；升级后 `GET /api/cluster` 的 `object_totals` 与升级前一致（`queues:1`），
> 3 副本仲裁队列 `qroll` 的 25 条持久消息计数不变且可逐条读回，另发布 5 条新消息后共 30 条可全部取回。
> 上述顺序仍成立于 Raft 的通用约束，本次演练覆盖 3 节点场景。**【已验证（2026-10-10，本机 Docker Desktop）2 节点停 1 个即失去多数派】** 另起一个最小 2 节点集群（n1/n2，各自 `peers` 只含这两台，`partition_policy=pause_minority`）实测：两节点均在线时 `GET /api/cluster` 为 `has_quorum=true / paused=false`（n1 leader、n2 follower）；`docker stop` 掉 n2 后，n1 在约 2 s 内变为 `has_quorum=false / paused=true` —— 即停 1 个投票成员就失去多数派、`pause_minority` 暂停服务，**印证 2 节点不具备滚动升级能力**；`docker start` 恢复 n2 后 n1 在约 2 s 内回到 `has_quorum=true / paused=false`（服务自动恢复）。

> **【已验证（2026-10-11，本机 Docker Desktop 3 节点集群）】网络分区下的滚动升级已实测**（`v1.1.0` → `1.1.05` 跨版本，分区用双网络拓扑：节点间 Raft RPC 只走静态 IP 的 `cluster-net`，客户端/管理面走 `client-net`，`docker network disconnect/connect` 切分区）：
> - **分区 follower**：多数派侧在分区期间持续保有 quorum、正常发布/消费，并在分区持续状态下完成一个多数派节点的"停→换镜像→起→追平"；混合版本（1.1.0 + 1.1.05）集群可服务。恢复分区后被隔离节点（旧版本）重新加入并追平，消息按唯一 ID 集合对账 35/35 无丢失。
> - **分区 leader**：多数派约 **630 ms** 选出新 leader；被隔离旧 leader 保持 `role=leader` 但 `has_quorum=false / paused=true`（`pause_minority` 生效，不主动抬任期）；分区中升级多数派侧另一节点成功；恢复后旧 leader 重新加入，无 split-brain、无数据分叉。
> - **无 PreVote 的预期行为（如实记录）**：被隔离 follower 立即反复竞选、任期约每 2 s +2（元数据与队列两个 Raft 组交替抬升）；恢复瞬间以高任期**抢主**（实测 1.4 s 内当选）——有 leader 切换扰动但无正确性问题。
> - **两个运维坑**：① `peers` 若用主机名且容器同时挂第二张可达网络，Raft 流量会绕路造成"假分区"（被隔离节点一分多钟仍 `has_quorum=true`）——节点间 RPC 必须只走一张网（peers 直指该网静态 IP）；② 恢复时 `docker network connect` 必须带 `--ip <原IP>`，否则节点拿到新 IP 后多数派仍拨旧地址，被隔离节点带落后日志以膨胀任期反复竞选（实测把集群 term 从 320 拖到 488、元数据 commit 从 13 冲到 97），并可能出现 leader `quorum=true` 却短暂 `paused=true` 的瞬态。
> - 分区期间少数派侧管理 API 可读（便于观测），AMQP 发布被拒（`pause_minority` 生效）。

---

## 8. 不支持 / 未验证的部分（明确列出）

- **跨大版本降级：不支持、未验证**。若新版本已用新格式/新语义写入数据，**没有**"回退到旧二进制照读"的保证；
  回滚只能靠升级前备份。
- **配置格式不变**：仍为 JSON + `SPEEDMQ_*` 环境变量。**YAML 配置尚未支持**（需引入解析依赖，M8-17 待评估），
  升级不会带来 YAML。
- **在线插件/协议热升级**：插件随内核编译进来（A 形态）或按配置 `spawn` 拉起（B 形态），
  升级内核 = 重启进程；**没有**原地热替换二进制的机制。
- **存储引擎原地迁移**：段轮转/索引压缩是运行期后台行为，**没有**独立的"数据迁移/压缩"命令。
- **磁盘写满下的集群升级**：**未验证**。（网络分区下的滚动升级已于 2026-10-11 实测，见 §7 末尾注记。）
- 本文档**未包含**任何 SpeedMQ 与其他 broker（RabbitMQ）之间的数据搬运验证。
