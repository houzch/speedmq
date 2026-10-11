# SpeedMQ Upgrade and Migration Plan

> Applies to version: `1.0.0` (`broker.Version`, see `speedmq_build_info` in `/metrics`).
> All "measured" conclusions in this document come from actual runs on this machine; anything not measured is explicitly marked **【Not verified】**.
> This machine's environment: Windows + PowerShell 5.1, Go 1.27.1 windows/386, temporary `data_dir` + non-default ports.

---

## 1. Migration (switching from RabbitMQ to SpeedMQ)

The positioning of this project is **AMQP 0-9-1 protocol-level compatibility**, so "migration" is mainly about **changing the connection address**:

- Zero changes to business code; only `host/port/vhost` is changed (design document G3 "zero-cost migration").
- The management toolchain (`rabbitmqadmin`, management UI, monitoring scripts) just needs to point at the management-plane port; the API shape aligns with RabbitMQ (conventions such as `amq.default`, `%2F`, `{error, reason}` are carried over as-is).
- Default ports match RabbitMQ: AMQP `5672`, management plane `15672`; MQTT is `1883`, and inter-node RPC is `25672`.

**Semantic differences to check before migrating** (all intentional in this repository, per the README / design documents):

| Item | SpeedMQ behavior | Migration impact |
| --- | --- | --- |
| Transient (non-durable and non-exclusive) queues | **Declaration is refused** (541); `auto_delete` is not exempt | Old clients that rely on such queues will fail; switch to durable or exclusive |
| Default vhost `/` | **Cannot be deleted** (400), whereas RabbitMQ allows it | Automation scripts that delete the default vhost will fail (this is the only deliberate safety constraint) |
| Classic queue data | **Not replicated**; data resides only on the owner node | For cross-node redundancy, switch to quorum queues `x-queue-type=quorum` |
| Quorum queues | Support adding replicas, **do not support shrinking** | Plan it right the first time |
| Plugins | No Erlang plugin ecosystem; AMQP 1.0 / STOMP are not implemented | Scenarios using these protocols cannot be migrated for now |

**Data migration**: SpeedMQ and RabbitMQ storage formats are incompatible, and **no online/offline data migration tool is provided**.
The migration approach is "create an empty SpeedMQ → dual-run validation → gradual traffic cutover". **【Not verified】** This document contains no real RabbitMQ data-migration drill.

---

## 2. General upgrade principles

1. **Back up first** (see `backup-restore.md`) — the fallback if the upgrade fails.
2. **Stop the process before replacing anything** (the data directory has a single-writer constraint; see §4.2).
3. **Verification is mandatory after the upgrade**: the process starts, `/api/overview` is readable, `/metrics` can be scraped, and the queue message counts match those before the backup.
4. Cluster upgrades are **rolled node by node**, touching only one node at a time (see §5).

---

## 3. Data directory layout (the factual basis for upgrade/migration)

The `data_dir` layout **measured** on this machine's single-node instance:

```
data/
├── meta/
│   ├── state.json        # Metadata snapshot for single-node mode (vhost/exchanges/queues/bindings/users/permissions/policies)
│   ├── users.seeded      # Bootstrap marker: the users in the config file have been seeded
│   ├── vhosts.seeded     # Bootstrap marker: the vhosts in the config file have been seeded
│   ├── raft.state        # 【Cluster mode】Raft term/vote
│   ├── raft.log          # 【Cluster mode】Raft log
│   └── snapshot.json     # 【Cluster mode】Raft snapshot + member table
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # Segment file (message body + properties); record format: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # Queue index: seq-id → (segment number, in-segment offset, length, status)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【Cluster】one Raft group per quorum queue (log/snapshot)
```

**Note (two points that defy intuition, both based on code/measurement)**:

- In cluster mode, the Raft persistence files are **placed directly under `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  and **there is no `meta/raft/` subdirectory**. Basis: the file-name constants in `internal/raft/log.go` plus `internal/broker/cluster.go`'s
  `Dir: filepath.Join(b.cfg.DataDir, "meta")`. **【Verified (2026-10-10, local Docker Desktop 3-node cluster)】** — the backup contents of the cluster node volume directly show `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (no `meta/raft/` subdirectory was seen).
- Directory names are **not the original vhost / queue names** but are encoded by `store.SafeDirName`: a `q_` prefix is added, and bytes outside `[A-Za-z0-9._-]` are escaped as `%XX`.
  Measured: vhost `/` → directory `q_%2F`, queue `persist.q` → directory `q_persist.q`.
  This design avoids path traversal and Windows reserved device names (`con`/`nul`, etc.).

---

## 4. Data compatibility

### 4.1 Can old data be read directly — yes

- **The index format is backward compatible**: M8-1 added a "segment number" field (25 bytes) to the index record; **the old format (21 bytes, without a segment number) can still be read as-is**,
  and reading it is equivalent to "there is only one segment (seg=1)", so **the upgrade requires no migration script**.
  Basis: the `indexEntrySize` / `legacyIndexEntrySize` constants and the `recover()` logic in `internal/store/store.go`; README M8-1.
- **Crash semantics are unchanged**: each record carries a length prefix + CRC32, and recovery **discards half-written/corrupted records at the tail** and truncates.
  Measured (see `backup-restore.md` §6): after stopping and restarting the process, all 5 persistent messages of the durable queue were **fully recovered**,
  and the log showed `已从磁盘恢复队列消息 ... messages=5`.

**【Verified (2026-10-10, local)】** Cross-major-version reads have been measured: durable queue data written with tag `v1.1.0` is restored as-is under `1.1.05` (commands and output in §5).

### 4.2 The semantics of `vhosts` / `users` in the configuration (the easiest pitfall during upgrades)

- Both **take effect only on first bootstrap**: on the first start, the vhosts/users in the configuration are written into the metadata and marker files
  `meta/vhosts.seeded` / `meta/users.seeded` are dropped; **from then on the metadata is authoritative**.
- Therefore, **when upgrading/changing configuration, do not expect to add or remove accounts or vhosts by editing the config file** — changing it has no effect;
  use the management API or `speedmqctl` instead.
- Conversely, an upgrade does **not** overwrite existing accounts with the configuration: a password changed at runtime will not be reverted to the old value in the configuration by a restart,
  and an account deleted at runtime will not come back to life. Basis: the seeding-marker logic in `cluster.go`; README M8-4 / M8-7.

**【Verified (2026-10-10, local)】** Runtime changes are indeed not overwritten by the configuration: before the upgrade the `guest` password was changed to `rotated` via the management API, and after restarting with `1.1.05` + the same `data_dir`, `guest/guest` (the old password in the config) returned **401** while `guest/rotated` returned **200**.

### 4.3 Segment rotation and disk reclamation

- Messages are segmented by size (default 8 MiB); **once all messages in a segment are ack'd and the segment is sealed, the whole segment is deleted**, and the index is compacted and rewritten accordingly.
- Upgrades do not change this behavior; single-segment files left by an old instance keep working under the new segment rotation logic.

---

## 5. Binary upgrade (bare metal)

> **【Verified (2026-10-10, local)】** Cross-major-version drill on real hardware: the old binary built from tag `v1.1.0` wrote durable exchanges/queues/bindings + 5 persistent messages + vhosts/accounts/permissions/policies and had a password changed at runtime; after stopping the process, it was started with the current `1.1.05` binary + **the same `data_dir` + the same ports** — the process started normally, the objects and message counts in `/api/queues` matched, a real AMQP client retrieved all 5 persistent messages, and `/metrics` reported `speedmq_build_info{version="1.1.05"}`.
> The following steps are still a general procedure and can be applied to any adjacent versions; for **downgrade** see §8.

### 5.1 Steps

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) Stop the process (a graceful shutdown performs the final flush; see §4 "Consistency")
#    If run in the foreground: Ctrl+C; if run as a service: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Back up the data directory (be sure to do this after the process has stopped)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Replace the binaries (put the new speedmqd.exe / speedmqctl.exe in the original paths)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) Start
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) Verify: process is alive + management API is readable
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Post-upgrade verification checklist

- The startup log shows `SpeedMQ 启动中 ... version=<新版本>` and `管理面已启动`;
- `object_totals` / `queue_totals` in `/api/overview` match those before the backup (compare with `backup-restore.md` §5);
- For each durable queue in `/api/queues`, `messages` / `messages_ready` match those before the backup;
- `/metrics` can be scraped and shows `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Image upgrade (container)

The image is about 13 MB (statically linked binary + alpine), **runs as non-root (uid 10001)**, and mounts the data directory at `/var/lib/speedmq`.

```powershell
# 1) Pull/build the new image (use the new version number as the tag to avoid old/new confusion)
docker build -t speedmq:1.0.0 .

# 2) Stop the old container (compose keeps the named volume speedmq-data)
docker compose down

# 3) Start the new version (change image to the new tag in the compose file)
docker compose up -d

# 4) Status and logs
docker compose ps
docker compose logs -f --tail 100
```

> **One-off tasks inside the container** (for example, running `speedmqctl` inside the container): `docker compose ...`'s `run` must be given `-T` in a non-interactive environment,
> otherwise it fails because it cannot allocate a TTY:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

Data persistence relies on the compose **named volume** `speedmq-data`, so recreating the container does not lose data (truly persisted to disk since M4).
If you need to back up the volume contents before an upgrade, it is equivalent to backing up `/var/lib/speedmq` (see `backup-restore.md` §3.2).

> **【Verified (2026-10-10, local Docker Desktop)】** A real image-upgrade drill was done: locally build the old image `speedmq:1.1.0-old` (git tag `v1.1.0`) and the new image `speedmq:1.1.05-new` (current source), start the old image with single-node compose → create durable queue `persist.q` and publish 5 persistent messages (`delivery_mode=2`) → `docker compose down` (**without `-v`**, so the named volume `speedmq-data` is kept) → change the image tag in compose to the new image and `docker compose up -d`.
> Result: the new container started normally, the startup log showed `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, `/metrics` reported `speedmq_build_info{version="1.1.05"}`, `queue_totals.messages=5` in `/api/overview` and `messages=5` of queue `persist.q` were the same before and after the upgrade, and a real AMQP client (`rabbitmq/amqp091-go`) retrieved all 5 persistent messages one by one (`deliveryMode=2`).
> Note: `v1.1.0` is the pre-rename `speedmq`-branded image (binary `speedmqd`, metric `speedmq_build_info`, default data directory `/var/lib/speedmq`), which differs in naming from the current version; in the drill we relied on the configuration `data_dir=/var/lib/speedmq` and pre-set the owner for the named volume (that writable path does not exist in the old image). This does not affect the conclusion that "recreating the container with a new tag does not lose the named-volume data".

---

## 7. Canary rollout and rollback

### 7.1 Single node

- **Canary**: a single-node SpeedMQ has no built-in "two versions in the same process" capability. A feasible canary is a **sidecar shadow**:
  hook a new-version instance onto the same upstream traffic first using **read-only consumption / shadow queues**, observe it, and only then switch the writer over.
- **Rollback**:
  1. Stop the new-version process;
  2. Swap back to the old binary;
  3. If the new version has already written data, you **must restore `data_dir` from the pre-upgrade backup** (see below).
  There is **no** guarantee that "the new version has written and the old version reads it directly" — for cross-version downgrades see §8.

### 7.2 Cluster (rolling upgrade)

The platform does not provide "one-click rolling upgrade"; you must operate node by node manually in the following order:

1. **Upgrade only one node at a time**: stop that node → back up its `data_dir` → swap the binary → start it → wait for it to rejoin and catch up
   (check `role`, `commit_index`/`last_applied` via `speedmqctl cluster_status` / `GET /api/cluster`).
2. **Suggested order**: upgrade **learners / non-voting members** first (no effect on the majority), then **followers**, and finally the **leader**
   (upgrading the leader triggers an election, during which writes are briefly unavailable).
3. **Impact of downtime on the majority** (critical):
   - 3-node cluster: **stop at most 1** voting member at a time; stopping 2 loses the majority, and under `pause_minority` the **entire cluster pauses service**.
   - 2-node cluster: stopping 1 loses the majority, and it **has no rolling-upgrade capability** (at least 3 nodes are recommended).
   - Therefore, during a rolling upgrade it is **strictly forbidden to stop multiple voting members at once**.
4. **Do not combine membership changes with upgrades**: membership changes have **no joint consensus**, and only one uncommitted configuration change is allowed at a time;
   during an upgrade, avoid `add_member` / `remove_member` at the same time.
5. After the upgrade completes, verify that `object_totals` from `GET /api/cluster` match those before the upgrade.

> **【Verified (2026-10-10, local Docker Desktop 3-node cluster)】** A real **cross-version** rolling-upgrade drill of a cluster was done: the baseline was the locally built `speedmq:1.1.0-old` (git tag `v1.1.0`), upgraded node by node (n1 → n2 → n3, one at a time: stop first, then back up its named volume, then swap the image and start) to the current `1.1.05` (locally built `speedmq:1.1.05-new`, with compose switching images via a per-node override). Throughout, the cluster always retained a leader and a majority — when the current leader was stopped, a new leader was elected in about 0.6 s, `pause_minority` did not trigger a pause, and there was no external interruption during the downtime; after the upgrade, `object_totals` from `GET /api/cluster` matched those before the upgrade (`queues:1`), the 25 persistent messages of the 3-replica quorum queue `qroll` kept the same count and could be read back one by one, and after publishing another 5 new messages, all 30 could be retrieved.
> The order above still holds as a general Raft constraint, and this drill covered the 3-node scenario. **【Verified (2026-10-10, local Docker Desktop): a 2-node cluster loses the majority when 1 node is stopped】** A separate minimal 2-node cluster (n1/n2, each with `peers` containing only these two, `partition_policy=pause_minority`) was measured: when both nodes were online, `GET /api/cluster` was `has_quorum=true / paused=false` (n1 leader, n2 follower); after `docker stop` of n2, n1 became `has_quorum=false / paused=true` within about 2 s — i.e., stopping 1 voting member loses the majority and `pause_minority` pauses service, **confirming that a 2-node cluster has no rolling-upgrade capability**; after `docker start` restoring n2, n1 returned to `has_quorum=true / paused=false` within about 2 s (service recovered automatically).

> **【Verified (2026-10-11, local Docker Desktop 3-node cluster)】Rolling upgrades under a network partition have been measured** (`v1.1.0` → `1.1.05` cross-version; the partition used a dual-network topology: inter-node Raft RPC goes only over `cluster-net` with static IPs, clients/management traffic goes over `client-net`, and `docker network disconnect/connect` toggles the partition):
> - **Partitioned follower**: the majority side kept quorum throughout the partition, published/consumed normally, and completed a "stop → swap image → start → catch up" of one majority node while the partition persisted; the mixed-version (1.1.0 + 1.1.05) cluster remained serviceable. After the partition healed, the isolated node (old version) rejoined and caught up; message reconciliation by unique-ID set was 35/35 with no loss.
> - **Partitioned leader**: the majority elected a new leader in about **630 ms**; the isolated old leader kept `role=leader` but with `has_quorum=false / paused=true` (`pause_minority` in effect; it did not proactively bump its term); upgrading another majority-side node during the partition succeeded; after healing, the old leader rejoined with no split-brain and no data divergence.
> - **Expected behavior without PreVote (faithfully recorded)**: the isolated follower campaigned repeatedly and immediately, its term rising about +2 every 2 s (the metadata and queue Raft groups alternating); at the moment of healing it **seized leadership** with its higher term (elected within 1.4 s in the drill) — a leader-switch disturbance, but no correctness issue.
> - **Two operational pitfalls**: ① if `peers` uses hostnames and the containers are also attached to a second reachable network, Raft traffic detours and causes a "false partition" (the isolated node still showed `has_quorum=true` after more than a minute) — inter-node RPC must use exactly one network (with `peers` pointing directly at that network's static IPs); ② on healing, `docker network connect` must carry `--ip <original IP>`, otherwise the node gets a new IP while the majority keeps dialing the old address, and the isolated node — carrying a stale log — campaigns repeatedly with an inflated term (in the drill it dragged the cluster term from 320 to 488 and the metadata commit from 13 to 97), and a transient state may appear where the leader shows `quorum=true` yet briefly `paused=true`.
> - During the partition, the minority side's management API stayed readable (useful for observation), while AMQP publishing was rejected (`pause_minority` in effect).

---

## 8. Unsupported / unverified parts (explicitly listed)

- **Cross-major-version downgrade: unsupported, unverified**. If the new version has written data with a new format/new semantics, there is **no** guarantee that "you can fall back to the old binary and read it as-is";
  rollback can only rely on the pre-upgrade backup.
- **The configuration format is unchanged**: still JSON + `SPEEDMQ_*` environment variables. **YAML configuration is not yet supported** (it would require introducing a parsing dependency, M8-17 pending evaluation),
  and upgrading will not bring YAML.
- **Online plugin/protocol hot upgrade**: plugins are either compiled into the kernel (form A) or spawned per configuration (form B),
  so upgrading the kernel = restarting the process; there is **no** mechanism for in-place hot replacement of the binary.
- **In-place storage engine migration**: segment rotation/index compaction is a runtime background behavior; there is **no** standalone "data migration/compaction" command.
- **Cluster upgrade with a full disk**: **not verified**. (Rolling upgrades under a network partition were measured on 2026-10-11; see the note at the end of §7.)
- This document **does not include** any validation of data movement between SpeedMQ and other brokers (RabbitMQ).
