# Backup at Restore ng SpeedMQ

> Ang lahat ng konklusyong "nasubok sa aktwal" sa dokumentong ito ay nagmula sa isang tunay na drill sa **Windows + PowerShell 5.1** (pansamantalang `data_dir` at pansamantalang ports).
> Ang mga command ng drill at mahahalagang output ay nakadikit nang buo sa §6. Ang mga bahaging **【hindi pa na-verify】** ay tahasang nakamarkahan (cluster backup/restore, Docker volume backup, atbp.).

---

## 1. Ano ang dapat i-backup

Sa ilalim ng `data_dir`, **kailangang i-backup ang buong bagay**, at ang mga sumusunod ang kritikal (para sa layout tingnan ang `upgrade.md` §3):

| Path | Ginagawa | Ano ang mangyayari kapag nawala |
| --- | --- | --- |
| `meta/state.json` | Snapshot ng metadata sa single-node: vhost / exchanges / queues / bindings / users / permissions / policies | Mawawala ang lahat ng topology at account |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【Cluster】Raft log / term votes / snapshot + member table | Mawawala ang cluster identity at consistency ng metadata |
| `meta/users.seeded`, `meta/vhosts.seeded` | Bootstrap markers | Kapag nawala, ang users/vhosts sa config ay **muli na namang ise-seed** (bubuhayin muli ang mga naburang account/vhost) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Data at index ng mensahe ng classic queues | Mawawala ang persistent messages |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【Cluster】Raft log/snapshot ng quorum queues | Mawawala ang data ng quorum queues |
| Certificates (ang PEM na tinutukoy ng `cert_file`/`key_file`/`ca_file` sa config) | TLS certificates | Kung i-backup nang hiwalay sa `data_dir`, hindi na magsisimula ang TLS pagkatapos ng restart |

> Ang soft state (mga hindi pa naka-confirm na mensahe, consumers, prefetch counts) ay **nasa memory lamang** at hindi isinusulat sa disk; ang backup ay **hindi kasama** at hindi rin dapat isama ang mga ito.

---

## 2. Kinakailangan sa consistency: **kailangang itigil muna ang process**; ang hot backup ay **hindi ligtas**

### 2.1 Konklusyon

- ✅ **Ligtas na paraan**: **itigil ang broker process** (ang graceful exit ay gagawa ng huling flush), pagkatapos kopyahin ang `data_dir`.
- ❌ **Hot backup (direktang pagkopya ng file habang tumatakbo ang process): hindi ligtas, walang garantiya.**

### 2.2 Bakit hindi ligtas ang hot backup

Ang message storage ay **dalawang file** (segment file na `*.seg` at index file na `index/*.idx`), at ang dalawa ay **hindi atomic commit**:

- Sa oras ng recovery, **ang index ang batayan** sa pagtukoy kung "alin ang mga buhay na mensahe", pagkatapos ay babasahin sa segment file gamit ang `(segment number, offset, length)` mula sa index.
- Sa hot backup, maaaring makopya ang isang intermediate state kung saan **na-reference na ng index, ngunit hindi pa kumpleto ang pagsulat sa segment file** (o kabaliktaran):
  - Kung ang index ay nag-reference ng record na wala sa segment → **mabibigo ang pagbasa at lalaktawan** ang mensaheng iyon (katumbas ng pagkawala ng isang naka-confirm na persistent message);
  - Kung may record sa segment ngunit hindi naka-reference ng index → **hindi mare-recover** ang mensaheng iyon.
- Bagaman gagamit ang recovery ng CRC32 upang itapon ang **half-written record sa dulo**, sinasaklaw lang nito ang "sirang pagsulat sa dulo ng iisang file", **hindi nito kayang ayusin ang hindi pagkakatugma sa pagitan ng index at segment**.

### 2.3 Tungkol sa "kailan naabot ng write ang disk" (aktwal na naobserbahan)

- Default na `fsync: os` + `flush_interval_ms: 200`: ang mensahe ay `write()` ng background flush goroutine papunta sa operating system sa **loob ng hindi hihigit sa mga 200 ms**
  (hindi fsync), at pagkatapos nito bumabalik ang publisher confirm.
- Aktwal na nasubok: pagkatapos mag-publish ng persistent message, **agad** na tinitingnan ang laki ng segment file, nakikita na ang data (`t=0ms seg=832`); ibig sabihin, ang "bytes na nakikita ng OS" ay halos kasabay ng confirm.
- **Tandaan**: ipinapaliwanag lang nito na "nakarating na sa OS buffer", **hindi mawawala kapag sapilitang pinatay ang process** (hindi nawawala ang OS buffer kapag pinatay ang process), ngunit **mawawala kapag nawalan ng kuryente**.
  Kung gusto mong "sa sandaling matanggap ang confirm ay naka-fsync na sa disk", palitan ang `storage.fsync` sa `batch` / `always`. **【hindi pa nasubok ang power-loss scenario】**

---

## 3. Mga hakbang sa backup

### 3.1 Single-node (inirerekomenda)

```powershell
# 1) Itigil ang process (foreground: Ctrl+C; background: Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Kopyahin ang buong data_dir (may timestamp)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (Opsyonal) I-verify na mabe-parse ang metadata snapshot sa backup
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Bawat node ay mag-backup ng sarili nitong `data_dir`** (ang metadata ay kinokopya ng Raft sa lahat, ang message data ay nasa Owner node, at ang replicas ng quorum queues ay nasa kani-kanilang Raft directory).
- Pagkakasunod-sunod ng pagtigil: **isang node lang sa isang pagkakataon**; huwag itigil nang sabay ang maraming voting member (tingnan ang `upgrade.md` §7.2).
- Upang makakuha ng **consistent snapshot ng buong cluster**, kailangang itigil nang sunud-sunod ang lahat ng node at kopyahin ang bawat isa; sa production, mas karaniwan ang "itigil/kopyahin/patakbuhin ang bawat node".
- **【hindi pa na-verify】** Walang tunay na cluster backup/restore drill na ginawa sa makinang ito.

### 3.3 Docker (named volume)

```powershell
# Pagkatapos itigil ang container, gamitin ang one-off container upang i-package at kopyahin ang nilalaman ng volume
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【hindi pa na-verify】** (hindi pinatakbo ang Docker sa makinang ito).

---

## 4. Mga hakbang sa restore

### 4.1 Single-node

```powershell
# 1) Kumpirmahin na naka-stop na ang process
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) Ilipat (o burahin) ang kasalukuyang data_dir upang maiwasang maghalo ang luma at bagong file
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) I-restore gamit ang backup
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) Simulan
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

Mga mahahalagang punto:
- **Kailangang ilipat muna ang lumang directory**, hindi maaaring "patungan ng backup file ang kalahating natirang directory";
- Ang nire-restore na `data_dir` ay dapat **parehong set ng vhost/queue** tulad ng noong backup (ang mga pangalan ng directory ay naka-encode, kaya magagamit sa ibang makina);
- **Huwag** samantalahin ang restore upang baguhin ang `vhosts`/`users` sa config file (epektibo lang ito sa unang bootstrap, walang silbi ang pagbabago, tingnan ang `upgrade.md` §4.2).

### 4.2 Cluster

- Pag-restore ng iisang node: i-restore ang `data_dir` ng node na iyon ayon sa §4.1 at simulan ito; muling sasali ito bilang umiiral na member at hahabol sa Raft log.
- Pag-restore ng buong cluster: **i-restore at simulan muna ang majority ng mga node** (≥ kalahati ng voting members) upang makapili ng leader ang cluster; pagkatapos ay i-restore ang iba pang node.
- **【hindi pa na-verify】** Hindi pa nasubok ang cluster restore.

---

## 5. Paraan ng pag-verify pagkatapos ng restore

I-cross-check gamit ang management API at tunay na client (inirerekomendang gawin lahat):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Kabuuang bilang ng object at kabuuang bilang ng mensahe (bilang ng queue/exchange/binding/user; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) I-verify ang messages / messages_ready ng bawat queue (maihahambing sa rekord bago ang backup)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) Kung naroon pa ang lahat ng vhost / user / policy
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Cluster (sa single-node, magbabalik ang enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Tingnan ang startup log**: dapat lumabas ang `已从磁盘恢复队列消息 ... messages=N` at `队列已恢复持久化消息 ... messages=N`; ang N ay dapat kapareho ng bago ang backup.
- **Mga babala sa log**: kung ang isang queue ay may mga mensaheng dating kinonsumo/binura, maaaring lumabas sa restore ang
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` at `恢复时清理了无存活消息的段`.
  Ang mga ito ay index residue ng **mga na-settle (ack/purge) na record**, at itinuturing na **kilalang log noise, hindi nakakaapekto sa kawastuhan ng data** (tingnan ang §7).
- **Tunay na client**: kunin muli ang mga mensahe mula sa queue at i-verify ang bilang/nilalaman (tingnan ang §6 hakbang (7)).

---

## 6. Aktwal na drill (tunay na command at output)

> Kapaligiran: ang `data_dir` ay nasa pansamantalang directory, AMQP `127.0.0.1:5676`, management plane `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> default na account na `guest/guest`. Startup log:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Gumawa ng durable topology + magpadala ng 5 persistent messages (tunay na client na `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Gumawa ng user / vhost / permissions / policy (management API)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Katayuan bago ang backup (management API)**

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

**(4) Mga file sa disk bago ang backup**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Itigil ang process → mag-backup → linisin → i-restore**

```
listeners still up: 0                       # sarado na ang lahat ng 5676/1884/15677
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # binura ang orihinal na data_dir, ginagaya ang pagkawala ng data
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Recovery log pagkatapos ng restart (mga kritikal na linya)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Kasabay nito, lumitaw ang ilang `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> Ang mga ito ay index residue na iniwan ng mga mensaheng **dating na-purge** sa drill na ito (na-settle na, na-reclaim na ang segment data), **hindi nakakaapekto sa recovery ng 5 mensahe sa ibaba**.

**(7) Assertion pagkatapos ng restore: management API + tunay na client**

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

**Konklusyon**: ang durable topology (exchanges+queues+bindings), 5 persistent messages, users, vhost, permissions, policies ay **lahat na-restore**,
at nakukuha ng tunay na client ang lahat ng mensahe nang tulad ng dati. **Pumasa ang drill.**

### 6.1 Paghahambing: ang recovery ng queue na hindi dumaan sa consumption/purge ay mas "tahimik"

Upang makita kung ang WARN sa itaas ay pangkalahatang pangyayari, gumawa ng isa pang **kontroladong paghahambing**: gumawa ng bagong durable queue na `clean.q`, magpadala ng 3 persistent messages,
**hindi mag-consume at hindi maglinis**, itigil ang process, pagkatapos ay i-restart:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Walang kahit anong WARN**. Ipinapakita nito na ang WARN ay lumilitaw lamang sa sitwasyon kung saan "may natitirang na-settle na record sa index" (tingnan ang §7).

---

## 7. Mga kilalang isyu at limitasyon (tapat na naitala)

1. **Log noise sa recovery (aktwal na naobserbahan)**: Kapag ang isang queue ay may dating consumption/purge sa kasaysayan (na-ack/purge na ang mga mensahe),
   ang index nito ay nagpapanatili pa rin ng reference sa mga na-reclaim na record, at sa oras ng recovery ay **isa-isang nagpi-print ng `恢复消息失败，已跳过` WARN** para sa bawat isa,
   at gumagawa/naglilinis ng isang walang laman na `000000.seg` (log na `恢复时清理了无存活消息的段`).
   **Hindi nakakaapekto sa kawastuhan ng data** (tama ang recovery ng mga buhay na unacknowledged message), ngunit **nagpaparumi sa log** at maaaring magpatuloy sa pag-screena sa malalaking queue/mataas na throughput.
   Rekomendasyon: gamitin ang `已从磁盘恢复队列消息 ... messages=N` bilang batayan, at huwag pansinin ang mga WARN na ito para sa na-settle na mga record;
   kung hindi katanggap-tanggap ang dami ng log, magbigay ng feedback sa maintainer ng kernel (hindi binabago ng dokumentong ito ang code).
2. **Hindi ligtas ang hot backup** (§2): huwag direktang kopyahin ang `data_dir` habang tumatakbo ang process.
3. **Hindi ginagarantiyahan ng `fsync: os` na hindi mawawala kapag nawalan ng kuryente**: kung gusto mong "confirm agad na nakasulat sa disk", gamitin ang `batch` / `always`.
4. **Plaintext ang passwords**: ang password ng user sa `meta/state.json` ay **plaintext** (aktwal na makikita ang `"password":"drillpass"`)——
   kaya ang backup file ay **kailangang ituring na sensitibong data** (access control, naka-encrypt na imbakan). Tingnan ang `security-baseline.md`.
5. **【hindi pa na-verify】** Cluster backup/restore, Docker volume backup/restore, power-loss scenario, concurrent writes sa panahon ng recovery.
