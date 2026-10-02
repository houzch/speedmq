# SwiftMQ Backup and Restore

> All "measured" conclusions in this document come from a single real drill on **Windows + PowerShell 5.1** (temporary `data_dir` and temporary ports).
> The drill commands and key output are pasted verbatim in §6. Parts marked **【Not verified】** are explicitly flagged (cluster backup/restore, Docker volume backup, etc.).

---

## 1. What to back up

Everything under `data_dir` **must be backed up in full**; the critical items are the following (for the layout see `upgrade.md` §3):

| Path | Purpose | What happens if lost |
| --- | --- | --- |
| `meta/state.json` | Single-node metadata snapshot: vhost / exchanges / queues / bindings / users / permissions / policies | The entire topology and accounts are lost |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【Cluster】Raft log / term vote / snapshot + member table | Cluster identity and metadata consistency are lost |
| `meta/users.seeded`, `meta/vhosts.seeded` | Bootstrap markers | Losing them causes the users/vhosts in the configuration to be **seeded again** (deleted accounts/vhosts come back to life) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Classic queue message data and index | Persistent messages are lost |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【Cluster】Raft log/snapshot of quorum queues | Quorum queue data is lost |
| Certificate files (the PEM files pointed to by `cert_file`/`key_file`/`ca_file` in the configuration) | TLS certificates | Backed up separately from `data_dir`; after a restart, TLS fails to start |

> Soft state (unacknowledged messages, consumers, prefetch counts) exists **only in memory** and is not persisted to disk; backups **do not** and should not include it.

---

## 2. Consistency requirements: **the process must be stopped first**; hot backup is **unsafe**

### 2.1 Conclusion

- ✅ **Safe approach**: **stop the broker process** (a graceful shutdown performs the final flush), then copy `data_dir`.
- ❌ **Hot backup (copying files directly while the process is running): unsafe, not guaranteed.**

### 2.2 Why hot backup is unsafe

Message storage consists of **two files** (the segment file `*.seg` and the index file `index/*.idx`), and the two are **not committed atomically**:

- During recovery, the **index is authoritative** for determining "which messages are alive", and then reads are performed from the segment file using the `(segment number, offset, length)` entries in the index.
- A hot backup may capture an intermediate state where **the index already references [a record] but the segment file has not been fully written** (or vice versa):
  - The index references a record that does not exist in the segment → that message **fails to read and is skipped** (equivalent to losing an already-confirmed persistent message);
  - A record exists in the segment but the index does not reference it → that message **is not recovered**.
- Although recovery uses CRC32 to discard **half-written records at the tail**, that only covers "a corrupted tail in a single file" and **cannot fix desynchronization between the index and the segment**.

### 2.3 On "when writes reach disk" (measured observations)

- With the default `fsync: os` + `flush_interval_ms: 200`, messages are `write()`n to the operating system by a background flush goroutine within **at most about 200 ms**
  (without fsync), and publisher confirms are also returned after this.
- Measured: checking the segment file size **immediately** after publishing a persistent message already shows the data (`t=0ms seg=832`); that is, the "bytes visible to the OS" are essentially in sync with the confirm.
- **Note**: this only means the data "reached the OS buffer" — **killing the process forcefully does not lose it** (killing the process does not lose the OS buffer), but **a power failure will**.
  To have "fsync on disk as soon as the confirm is received", change `storage.fsync` to `batch` / `always`. **【Power-failure scenario not measured】**

---

## 3. Backup steps

### 3.1 Single node (recommended)

```powershell
# 1) Stop the process (foreground: Ctrl+C; background: Stop-Process)
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Copy the entire data_dir (with a timestamp)
$data = "C:\swiftmq\data"
Copy-Item -Recurse -Force $data "C:\backup\swiftmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (Optional) verify that the metadata snapshot in the backup can be parsed
Get-Content "C:\backup\swiftmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Each node backs up its own `data_dir`** (metadata is replicated to all nodes via Raft, message data resides on the owner node, and quorum queue replicas reside in their respective Raft directories).
- Shutdown order: **stop only one node at a time**; do not stop multiple voting members at the same time (see `upgrade.md` §7.2).
- To obtain a **cluster-wide consistent snapshot**, you must stop all nodes in order and then copy each one; in production, the more common approach is "stop/copy/start node by node".
- **【Not verified】** No real cluster backup/restore drill was performed on this machine.

### 3.3 Docker (named volume)

```powershell
# After stopping the container, use a one-off container to pack and copy out the volume contents
docker compose down
docker run --rm -v swiftmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/swiftmq-data.tar.gz -C /data .
```
> **【Not verified】** (Docker was not run on this machine).

---

## 4. Restore steps

### 4.1 Single node

```powershell
# 1) Confirm the process is stopped
Get-Process -Name swiftmqd -ErrorAction SilentlyContinue

# 2) Move away (or delete) the current data_dir to avoid mixing old and new files
Move-Item "C:\swiftmq\data" "C:\swiftmq\data.broken"

# 3) Restore from the backup
Copy-Item -Recurse -Force "C:\backup\swiftmq-YYYYMMDD-HHMMSS" "C:\swiftmq\data"

# 4) Start
& "C:\swiftmq\swiftmqd.exe" -config "C:\swiftmq\configs\swiftmqd.json" -log-level info
```

Key points:
- **The old directory must be moved away first**; you cannot "copy the backup files over a partially leftover directory";
- The restored `data_dir` must have the **same set of vhosts/queues** as at backup time (directory names are encoded, so they work across machines);
- **Do not** take the restore as an opportunity to modify `vhosts`/`users` in the configuration file (they only take effect on first bootstrap; changing them does nothing — see `upgrade.md` §4.2).

### 4.2 Cluster

- Restoring a single node: restore that node's `data_dir` as in §4.1 and then start it; it rejoins as an existing member and catches up on the Raft log.
- Restoring the entire cluster: **first restore and start a majority of nodes** (≥ half of the voting members) so the cluster can elect a leader; then restore the remaining nodes.
- **【Not verified】** Cluster restore was not measured.

---

## 5. How to verify after restore

Cross-check using the management API and a real client (doing all of them is recommended):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Totals of objects and messages (queue count/exchange count/binding count/user count; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) Check messages / messages_ready per queue (compare against records taken before the backup)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) Whether vhost / users / policies are all present
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Cluster (a single node returns enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Check the startup log**: you should see `已从磁盘恢复队列消息 ... messages=N` and `队列已恢复持久化消息 ... messages=N`; N should match the value before the backup.
- **Warnings in the log**: if a queue previously had messages consumed/purged, recovery may show
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` and `恢复时清理了无存活消息的段`.
  These are index remnants of **already-settled (ack/purge) records**, and are **known log noise that does not affect data correctness** (see §7).
- **A real client**: fetch messages back from the queue and verify the count/content (see §6 step (7)).

---

## 6. Measured drill (real commands and output)

> Environment: `data_dir` in a temporary directory, AMQP `127.0.0.1:5676`, management plane `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> default account `guest/guest`. Startup log:
> ```
> level=INFO msg="SwiftMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Create durable topology + publish 5 persistent messages (real client `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Create user / vhost / permission / policy (management API)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) State before backup (management API)**

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

**(4) Disk files before backup**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Stop process → backup → wipe → restore**

```
listeners still up: 0                       # 5676/1884/15677 are all closed
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # the original data_dir was deleted, simulating data loss
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Recovery log after restart (key lines)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Several lines like `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"` also appear:
> these are index remnants left by messages that were **previously purged** in this drill (already settled, segment data reclaimed), and **do not affect the recovery of the 5 messages below**.

**(7) Post-restore assertions: management API + real client**

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

**Conclusion**: the durable topology (exchanges + queues + bindings), the 5 persistent messages, users, vhosts, permissions, and policies are **all restored**,
and a real client can fetch all messages back exactly as they were. **The drill passed.**

### 6.1 Comparison: a queue that has not gone through consumption/purge recovers more "quietly"

To determine whether the WARNs above are a general phenomenon, a separate **controlled comparison** was done: create a new durable queue `clean.q`, publish 3 persistent messages,
**without consuming or purging**, then stop the process and restart:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**No WARN at all.** This shows the WARNs only occur in the scenario where "settled records still remain in the index" (see §7).

---

## 7. Known issues and limitations (faithfully recorded)

1. **Recovery log noise (really observed)**: when a queue has historically undergone consumption/purging (messages already ack'd/purged),
   its index still retains references to reclaimed records; during recovery a `恢复消息失败，已跳过` WARN is **printed one by one for each such record**,
   and an empty `000000.seg` is created/cleaned up (log `恢复时清理了无存活消息的段`).
   This **does not affect data correctness** (surviving unacknowledged messages are recovered correctly), but it **pollutes the log** and may flood it with large queues / high throughput.
   Suggestion: rely on `已从磁盘恢复队列消息 ... messages=N` and ignore these WARNs for settled records;
   if the log volume is unacceptable, please report it to the kernel maintainers (this document does not modify the code).
2. **Hot backup is unsafe** (§2): do not copy `data_dir` while the process is running.
3. **`fsync: os` does not guarantee survival of a power failure**: for "fsync on disk as soon as the confirm is received", use `batch` / `always`.
4. **Plaintext passwords**: user passwords in `meta/state.json` are **plaintext** (measured: `"password":"drillpass"` is visible) —
   backup files must therefore **be handled as sensitive data** (access control, encrypted storage). See `security-baseline.md` for details.
5. **【Not verified】** Cluster backup/restore, Docker volume backup/restore, power-failure scenarios, and concurrent writes during recovery.
