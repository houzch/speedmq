# Plano sa Upgrade at Migration ng SwiftMQ

> Naaangkop na bersyon: `1.0.0` (`broker.Version`, tingnan ang `swiftmq_build_info` sa `/metrics`).
> Ang lahat ng konklusyong "nasubok sa aktwal" sa dokumentong ito ay nagmula sa tunay na pagtakbo sa makinang ito; ang mga hindi pa nasusubok ay tahasang nakamarkahan ng **【hindi pa na-verify】**.
> Kapaligiran ng makinang ito: Windows + PowerShell 5.1, Go 1.27.1 windows/386, pansamantalang `data_dir` + hindi default na ports.

---

## 1. Migration (mula sa RabbitMQ papuntang SwiftMQ)

Ang posisyon ng proyektong ito ay **protocol-level compatibility sa AMQP 0-9-1**, kaya ang "migration" ay pangunahing **pagpapalit ng connection address**:

- Zero na pagbabago sa business code, palitan lang ang `host/port/vhost` (design doc G3 「zero-cost migration」).
- Ang management toolchain (`rabbitmqadmin`, management UI, monitoring scripts) ay itutok lamang sa management plane port; ang hugis ng interface ay naka-align sa RabbitMQ (kinopya ang mga convention tulad ng `amq.default`, `%2F`, `{error, reason}`).
- Katulad ng RabbitMQ ang mga default port: AMQP `5672`, management plane `15672`; ang MQTT ay `1883`, at ang inter-node RPC ay `25672`.

**Mga pagkakaiba sa semantics na kailangang i-self-check bago ang migration** (lahat ay sinasadya ng repository na ito, base sa README / design doc):

| Item | Gawi ng SwiftMQ | Epekto sa migration |
| --- | --- | --- |
| Transient (hindi durable at hindi exclusive) na queue | **Tumatanggi sa deklarasyon** (541), hindi exempt ang `auto_delete` | Mabibigo ang lumang client kung umaasa ito sa ganitong uri ng queue, kailangang gawing durable o exclusive |
| Default vhost na `/` | **Hindi mabubura** (400), pinapayagan ito ng RabbitMQ | Mabibigo ang automation script kung buburahin ang default vhost (ito ang tanging aktibong security constraint) |
| Data ng classic queue | **Hindi kinokopya**, nasa Owner node lamang ang data | Kung kailangan ng cross-node redundancy, gamitin ang quorum queue na `x-queue-type=quorum` |
| Quorum queue | Sinusuportahan ang pagdagdag ng replica, **hindi sinusuportahan ang pagbawas** | Planuhin nang tama sa isang beses |
| Plugin | Walang Erlang plugin ecosystem, hindi pa naipatupad ang AMQP 1.0 / STOMP | Hindi pa maaaring i-migrate ang mga scenario na gumagamit ng mga protocol na ito |

**Data migration**: Hindi compatible ang storage format ng SwiftMQ at RabbitMQ, **walang ibinibigay na online/offline data-moving tool**.
Ang paraan ng migration ay "gumawa ng bagong walang laman na SwiftMQ → dual-run verification → canary traffic cutover". **【hindi pa na-verify】** Walang anumang tunay na RabbitMQ data-moving drill sa dokumentong ito.

---

## 2. Pangkalahatang mga prinsipyo ng upgrade

1. **Mag-backup muna** (tingnan ang `backup-restore.md`) —— fallback kapag nabigo ang upgrade.
2. **Itigil muna ang process bago magpalit** (may single-writer constraint ang data directory, tingnan ang §4.2).
3. **Kailangang i-verify pagkatapos ng upgrade**: nakakapagsimula ang process, nababasa ang `/api/overview`, nakukuha ang `/metrics`, at ang bilang ng mensahe ng queue ay kapareho ng bago ang backup.
4. Ang cluster upgrade ay **rolling bawat node**, isang node lang sa isang pagkakataon (tingnan ang §5).

---

## 3. Layout ng data directory (batayan ng katotohanan para sa upgrade/migration)

Ang layout ng `data_dir` na **aktwal na nakuha** mula sa single-node instance sa makinang ito:

```
data/
├── meta/
│   ├── state.json        # Metadata snapshot ng single-node mode (vhost/exchanges/queues/bindings/users/permissions/policies)
│   ├── users.seeded      # Bootstrap marker: na-seed na ang users sa config file
│   ├── vhosts.seeded     # Bootstrap marker: na-seed na ang vhosts sa config file
│   ├── raft.state        # 【Cluster mode】Raft term/votes
│   ├── raft.log          # 【Cluster mode】Raft log
│   └── snapshot.json     # 【Cluster mode】Raft snapshot + member table
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # Segment file (message body + attributes), format ng record: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # Index ng queue: seq-id → (segment number, offset sa segment, length, state)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【Cluster】Bawat quorum queue ay may isang Raft group (log/snapshot)
```

**Tandaan (dalawang punto na taliwas sa intuwisyon, batay sa code/aktwal na pagsusubok)**:

- Sa cluster mode, ang Raft persistence files ay **direktang nasa ilalim ng `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  at **walang `meta/raft/` subdirectory**. Base: ang filename constants sa `internal/raft/log.go` + ang `Dir: filepath.Join(b.cfg.DataDir, "meta")` sa `internal/broker/cluster.go`. **【hindi pa nasubok ang cluster layout】** (single-node instance lang ang pinatakbo sa makinang ito).
- Ang pangalan ng directory ay **hindi ang orihinal na vhost / queue name**, kundi ang encoding ng `store.SafeDirName`: nagdadagdag ng prefix na `q_`, at ang mga byte na hindi `[A-Za-z0-9._-]` ay ini-escape gamit ang `%XX`.
  Aktwal na nasubok: vhost `/` → directory `q_%2F`, queue `persist.q` → directory `q_persist.q`.
  Dinisenyo ito upang maiwasan ang path traversal at mga reserved device name ng Windows (`con`/`nul` atbp.).

---

## 4. Pagkakatugma ng data

### 4.1 Mababasa ba nang direkta ang lumang data——Oo

- **Forward compatible ang index format**: nagdagdag ang M8-1 ng "segment number" field (25 bytes) sa index record; **mababasa pa rin nang tulad ng dati ang lumang format (21 bytes, walang segment number)**,
  at sa pagbasa ay katumbas ng "isang segment lamang (seg=1)", **hindi kailangan ng migration script ang upgrade**.
  Base: ang `indexEntrySize` / `legacyIndexEntrySize` constants at ang `recover()` logic sa `internal/store/store.go`; README M8-1.
- **Hindi nagbabago ang crash semantics**: bawat record ay may length prefix + CRC32, at sa recovery ay **itinatapon at pinuputol ang half-written/sirang record sa dulo**.
  Aktwal na nasubok (tingnan ang `backup-restore.md` §6): pagkatapos ng pagtigil at pag-restart ng process, **lahat na-recover** ang 5 persistent messages ng durable queue,
  at lumitaw sa log ang `已从磁盘恢复队列消息 ... messages=5`.

### 4.2 Ang pagtrato sa `vhosts` / `users` sa config (ang pinakamadaling mapitakang bitag sa upgrade)

- Ang dalawa ay **epektibo lamang sa unang bootstrap**: sa unang pag-start, isusulat ang vhosts/users sa config papunta sa metadata at gagawa ng marker files na
  `meta/vhosts.seeded` / `meta/users.seeded`; **mula noon, ang metadata ang batayan**.
- Kaya **sa oras ng upgrade/pagpapalit ng config, huwag umasang makakapagdagdag o makakapagbura ng account o vhost sa pamamagitan ng pagbabago ng config file**——kahit baguhin ay walang epekto;
  gamitin ang management API o `swiftmqctl`.
- Sa kabaligtaran, **hindi** i-override ng upgrade ang mga umiiral na account gamit ang config: ang password na binago sa runtime ay hindi ibabalik ng restart sa lumang halaga sa config,
  at ang account na binura sa runtime ay hindi na muling bubuhayin. Base: ang seeding marker logic sa `cluster.go`; README M8-4 / M8-7.

### 4.3 Segment rotation at disk reclaim

- Hinahati ang mga mensahe ayon sa laki (default na 8 MiB), at **kapag na-ack na ang lahat ng mensahe sa loob ng segment at na-seal na ang segment, binubura ang buong segment**, at ang index ay siksik na muling isinusulat.
- Hindi binabago ng upgrade ang gawi na ito; ang single-segment file na iniwan ng lumang instance ay gumagana pa rin nang normal sa ilalim ng bagong segment rotation logic.

---

## 5. Binary upgrade (bare metal)

> **Walang cross-version na drill sa tunay na makina** sa makinang ito (isang bersyon lang ang kasalukuyang nasa repository na `1.0.0`, walang lumang binary na mai-upgrade). Ang mga hakbang sa ibaba ay **same-version replay verification + pangkalahatang proseso** ng mga kakayahang mayroon na ang repository na ito, at ang cross-version na bahagi ay nakamarkahan ng **【hindi pa na-verify】**.

### 5.1 Mga hakbang

```powershell
$base = "C:\swiftmq"
$data = "$base\data"

# 1) Itigil ang process (ang graceful exit ay gagawa ng huling flush; tingnan ang §4「Consistency」)
#    Kung tumatakbo sa foreground: Ctrl+C; kung bilang service: Stop-Service / Stop-Process
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) I-backup ang data directory (siguraduhing pagkatapos nang huminto ang process)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Palitan ang binary (ilagay ang bagong bersyon ng swiftmqd.exe / swiftmqctl.exe sa orihinal na path)
#    Copy-Item .\new\swiftmqd.exe $base\swiftmqd.exe -Force

# 4) Simulan
& "$base\swiftmqd.exe" -config "$base\configs\swiftmqd.json" -log-level info

# 5) I-verify: buhay ang process + nababasa ang management API
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Checklist ng verification pagkatapos ng upgrade

- Lumalabas sa startup log ang `SwiftMQ 启动中 ... version=<新版本>` at `管理面已启动`;
- Ang `object_totals` / `queue_totals` ng `/api/overview` ay kapareho ng bago ang backup (ihambing sa `backup-restore.md` §5);
- Ang `messages` / `messages_ready` ng bawat durable queue sa `/api/queues` ay kapareho ng bago ang backup;
- Nakukuha ang `/metrics` at `swiftmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Image upgrade (container)

Ang image ay mga 13 MB (statically linked binary + alpine), **tumatakbo bilang non-root (uid 10001)**, at ang data directory ay naka-mount sa `/var/lib/swiftmq`.

```powershell
# 1) I-pull/i-build ang bagong image (gamitin ang bagong version number bilang tag, upang maiwasan ang pagkalito ng old/new)
docker build -t swiftmq:1.0.0 .

# 2) Itigil ang lumang container (pinapanatili ng compose ang named volume na swiftmq-data)
docker compose down

# 3) Simulan ang bagong bersyon (palitan ang image sa compose file gamit ang bagong tag)
docker compose up -d

# 4) Status at logs
docker compose ps
docker compose logs -f --tail 100
```

> **One-off task sa loob ng container** (halimbawa, pagpapatakbo ng `swiftmqctl` sa loob ng container): ang `run` ng `docker compose ...` ay obligadong may `-T` sa non-interactive na kapaligiran,
> kung hindi ay mabibigo dahil sa paghingi ng TTY:
> ```powershell
> docker compose run -T --rm broker swiftmqctl -user guest -pass guest status
> ```

Ang data persistence ay umaasa sa **named volume** na `swiftmq-data` ng compose; hindi nawawala ang data kapag muling itinayo ang container (totoong nagsusulat sa disk mula M4).
Kung kailangang i-backup ang nilalaman ng volume bago ang upgrade, katumbas ito ng pag-backup ng `/var/lib/swiftmq` (tingnan ang `backup-restore.md` §3.2). **【hindi pa nasubok ang image upgrade】** (hindi pinatakbo ang Docker sa makinang ito).

---

## 7. Canary at rollback

### 7.1 Single-node

- **Canary**: Walang built-in na kakayanan ang single-node SwiftMQ para sa "luma at bago sa parehong process". Ang praktikal na canary ay ang **sidecar shadow**:
  unang i-attach ang bagong bersyon ng instance sa parehong upstream traffic gamit ang **read-only consumption/shadow queue** upang obserbahan, at pagkatapos makumpirma na walang problema, saka palitan ang writer.
- **Rollback**:
  1. Itigil ang bagong bersyon na process;
  2. Ibalik ang lumang binary;
  3. Kung nagsulat na ng data ang bagong bersyon, **kailangang i-restore ang `data_dir` gamit ang backup bago ang upgrade** (tingnan sa ibaba).
  **Walang** garantiya na "nagsulat na ang bagong bersyon, at direktang mababasa ng lumang bersyon"——para sa cross-version downgrade tingnan ang §8.

### 7.2 Cluster (rolling upgrade)

Walang ibinibigay ang platform side na "one-click rolling upgrade"; kailangang manu-manong paisa-isang node ayon sa sumusunod na pagkakasunod-sunod:

1. **Isang node lang sa isang pagkakataon**: itigil ang node → i-backup ang `data_dir` nito → palitan ang binary → simulan → hintayin itong muling sumali at humabol
   (`swiftmqctl cluster_status` / `GET /api/cluster` upang tingnan ang `role`, `commit_index`/`last_applied`).
2. **Rekomendadong pagkakasunod-sunod**: i-upgrade muna ang **learner / non-voting member** (walang epekto sa majority), pagkatapos ang **follower**, at panghuli ang **leader**
   (ang pag-upgrade sa leader ay magti-trigger ng isang halalan, at may maikling panahon na hindi makakasulat).
3. **Epekto ng pagtigil sa majority** (kritikal):
   - 3-node cluster: **pinakamaraming 1** na voting member ang maaaring itigil nang sabay; kapag 2 ang itinigil, mawawala ang majority, at sa ilalim ng `pause_minority` ay **pansamantalang hihinto ang buong serbisyo ng cluster**.
   - 2-node cluster: isang node lang ang itinigil, mawawala na ang majority, **walang kakayahan sa rolling upgrade** (inirerekomenda ang hindi bababa sa 3 node).
   - Kaya sa rolling upgrade, **mahigpit na ipinagbabawal ang pagtigil ng maraming voting member nang sabay**.
4. **Huwag pagsabayin ang pagbabago ng member at ang upgrade**: ang pagbabago ng member ay **walang joint consensus**, isang uncommitted configuration change lamang ang pinapayagan sa isang pagkakataon;
   sa panahon ng upgrade, iwasan ang sabay na `add_member` / `remove_member`.
5. Pagkatapos ng upgrade, i-verify na ang `object_totals` ng `GET /api/cluster` ay kapareho ng bago ang upgrade.

> **【hindi pa na-verify】** Walang tunay na cluster rolling upgrade drill na ginawa sa makinang ito (hindi pinatakbo ang cluster path at container); ang pagkakasunod-sunod sa itaas ay nagmula sa pangkalahatang constraint ng message middleware
> at Raft gayundin sa katotohanan ng implementasyon ng `pause_minority` / pagbabago ng member sa repository na ito, at hindi konklusyon mula sa aktwal na pagsusubok sa makinang ito.

---

## 8. Mga bahaging hindi sinusuportahan / hindi pa na-verify (tahasang nakalista)

- **Cross-major-version downgrade: hindi sinusuportahan, hindi pa na-verify**. Kung ang bagong bersyon ay nagsulat na ng data gamit ang bagong format/bagong semantics, **walang** garantiya na "makakabalik sa lumang binary at mababasa pa rin";
  ang rollback ay maaasahan lamang sa backup bago ang upgrade.
- **Hindi nagbabago ang format ng config**: JSON + `SWIFTMQ_*` environment variables pa rin. **Hindi pa sinusuportahan ang YAML config** (kailangan ng parsing dependency, nasa pagsusuri pa ang M8-17),
  at walang YAML na dadalhin ang upgrade.
- **Online plugin/protocol hot upgrade**: ang plugin ay kinokompile kasama ng kernel (form A) o pinapataas gamit ang `spawn` ayon sa config (form B);
  ang pag-upgrade ng kernel = pag-restart ng process; **walang** mekanismo para sa in-place hot replacement ng binary.
- **In-place migration ng storage engine**: ang segment rotation/index compaction ay runtime background na gawi; **walang** independiyenteng command na "data migration/compaction".
- **Cluster upgrade sa ilalim ng tunay na network**: scaled-down chaos (process-level kill) lamang ang ginawa ng repository na ito, at **hindi** ginawa ang upgrade drill sa ilalim ng network partition o punong disk.
- Ang dokumentong ito ay **hindi naglalaman** ng anumang verification ng data transfer sa pagitan ng SwiftMQ at ibang broker (RabbitMQ).
