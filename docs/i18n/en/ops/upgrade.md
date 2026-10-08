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
  `Dir: filepath.Join(b.cfg.DataDir, "meta")`. **【Cluster layout not measured】** (only a single-node instance was run on this machine).
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

### 4.2 The semantics of `vhosts` / `users` in the configuration (the easiest pitfall during upgrades)

- Both **take effect only on first bootstrap**: on the first start, the vhosts/users in the configuration are written into the metadata and marker files
  `meta/vhosts.seeded` / `meta/users.seeded` are dropped; **from then on the metadata is authoritative**.
- Therefore, **when upgrading/changing configuration, do not expect to add or remove accounts or vhosts by editing the config file** — changing it has no effect;
  use the management API or `speedmqctl` instead.
- Conversely, an upgrade does **not** overwrite existing accounts with the configuration: a password changed at runtime will not be reverted to the old value in the configuration by a restart,
  and an account deleted at runtime will not come back to life. Basis: the seeding-marker logic in `cluster.go`; README M8-4 / M8-7.

### 4.3 Segment rotation and disk reclamation

- Messages are segmented by size (default 8 MiB); **once all messages in a segment are ack'd and the segment is sealed, the whole segment is deleted**, and the index is compacted and rewritten accordingly.
- Upgrades do not change this behavior; single-segment files left by an old instance keep working under the new segment rotation logic.

---

## 5. Binary upgrade (bare metal)

> No **cross-version drill on real hardware** was performed on this machine (the repository currently has only one version, `1.0.0`, with no older binary to upgrade from). The following steps are a **same-version replay verification + general procedure** for capabilities this repository already has, with cross-version parts marked **【Not verified】**.

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
If you need to back up the volume contents before an upgrade, it is equivalent to backing up `/var/lib/speedmq` (see `backup-restore.md` §3.2). **【Image upgrade not measured】** (Docker was not run on this machine).

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

> **【Not verified】** No rolling-upgrade drill on a real cluster was performed on this machine (neither the cluster path nor containers were run); the order above comes from general constraints of
> messaging middleware and Raft, together with the implementation facts of `pause_minority` / membership changes in this repository, and is not a measured conclusion from this machine.

---

## 8. Unsupported / unverified parts (explicitly listed)

- **Cross-major-version downgrade: unsupported, unverified**. If the new version has written data with a new format/new semantics, there is **no** guarantee that "you can fall back to the old binary and read it as-is";
  rollback can only rely on the pre-upgrade backup.
- **The configuration format is unchanged**: still JSON + `SPEEDMQ_*` environment variables. **YAML configuration is not yet supported** (it would require introducing a parsing dependency, M8-17 pending evaluation),
  and upgrading will not bring YAML.
- **Online plugin/protocol hot upgrade**: plugins are either compiled into the kernel (form A) or spawned per configuration (form B),
  so upgrading the kernel = restarting the process; there is **no** mechanism for in-place hot replacement of the binary.
- **In-place storage engine migration**: segment rotation/index compaction is a runtime background behavior; there is **no** standalone "data migration/compaction" command.
- **Cluster upgrades under a real network**: this repository only did reduced-scale chaos (process-level kill), and did **not** do upgrade drills under network partitions or a full disk.
- This document **does not include** any validation of data movement between SpeedMQ and other brokers (RabbitMQ).
