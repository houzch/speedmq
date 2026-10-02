# SwiftMQ back-up en herstel

> Alle "in de praktijk geteste" conclusies in dit document komen uit één echte oefening op **Windows + PowerShell 5.1** (tijdelijke `data_dir` en tijdelijke poorten).
> De oefenopdrachten en belangrijke uitvoer staan ongewijzigd in §6. Delen die **【niet geverifieerd】** zijn, worden expliciet gemarkeerd (clusterback-up/-herstel, back-up van Docker-volumes, enz.).

---

## 1. Wat je moet back-uppen

Onder `data_dir` **moet alles in zijn geheel worden geback-upt**; de belangrijkste onderdelen zijn de volgende (zie `upgrade.md` §3 voor de indeling):

| Pad | Functie | Wat er gebeurt als het verloren gaat |
| --- | --- | --- |
| `meta/state.json` | Metadatasnapshot voor één node: vhost / exchanges / queues / bindings / gebruikers / rechten / policies | Topologie en accounts gaan volledig verloren |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【cluster】Raft-log / termijnstemming / snapshot + ledentabel | Clusteridentiteit en consistentie van metadata gaan verloren |
| `meta/users.seeded`, `meta/vhosts.seeded` | Bootstrap-markeringen | Bij verlies worden de users/vhosts uit de configuratie **opnieuw geseed** (verwijderde accounts/vhosts komen terug) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Berichtgegevens en index van klassieke queues | Persistente berichten gaan verloren |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【cluster】Raft-log/snapshot van quorumqueues | Gegevens van quorumqueues gaan verloren |
| Certificaatbestanden (de PEM-bestanden waarnaar `cert_file`/`key_file`/`ca_file` in de configuratie verwijzen) | TLS-certificaten | Los van `data_dir` back-uppen, anders start TLS niet na een herstart |

> Zachte status (onbevestigde berichten, consumers, prefetch-tellers) staat **alleen in het geheugen** en wordt niet naar schijf geschreven; de back-up **bevat** deze dan ook niet en zou ze ook niet moeten bevatten.

---

## 2. Consistentievereiste: **het proces moet eerst stoppen**; hot back-up is **onveilig**

### 2.1 Conclusie

- ✅ **Veilige werkwijze**: **stop het brokerproces** (een sierlijke afsluiting doet een laatste flush naar schijf) en kopieer daarna `data_dir`.
- ❌ **Hot back-up (bestanden direct kopiëren terwijl het proces draait): onveilig, geen garantie.**

### 2.2 Waarom hot back-up onveilig is

De berichtopslag bestaat uit **twee bestanden** (het segmentbestand `*.seg` en het indexbestand `index/*.idx`) en deze worden **niet atomair vastgelegd**:

- Bij herstel is de **index leidend** om te bepalen "welke berichten nog leven", waarna op basis van `(段号, 偏移, 长度)` in de index uit het segmentbestand wordt gelezen.
- Bij een hot back-up kun je een tussentoestand kopiëren waarin **de index al verwijst naar een segment dat nog niet volledig is weggeschreven** (of omgekeerd):
  - De index verwijst naar een record dat niet in het segment bestaat → dat bericht **mislukt bij het lezen en wordt overgeslagen** (gelijk aan het verliezen van een bevestigd persistent bericht);
  - Er staat wel een record in het segment maar de index verwijst er niet naar → dat bericht **wordt niet hersteld**.
- Hoewel het herstel met CRC32 **half-geschreven records aan het einde** weggooit, dekt dat alleen "een kapot geschreven staart in één enkel bestand" en **kan het de desynchronisatie tussen index en segment niet repareren**.

### 2.3 Over "wanneer een schrijfactie naar schijf gaat" (praktijkobservatie)

- Standaard `fsync: os` + `flush_interval_ms: 200`: berichten worden door een achtergrond-flush-goroutine binnen **ten hoogste ongeveer 200 ms** met `write()` naar het besturingssysteem geschreven
  (zonder fsync), en de publisher confirm wordt daarna geretourneerd.
- Praktijkmeting: direct na het publiceren van een persistent bericht is de bestandsgrootte van het segment al zichtbaar (`t=0ms seg=832`); met andere woorden, "de bytes die het OS kan zien" lopen vrijwel synchroon met de confirm.
- **Let op**: dit betekent alleen dat het "in de OS-buffer is beland"; **het hard killen van het proces leidt niet tot verlies** (het doden van het proces wist de OS-buffer niet), maar **een stroomstoring wel**.
  Wil je "bij ontvangst van de confirm al naar schijf gefsynct", zet dan `storage.fsync` op `batch` / `always`. **【stroomstoringscenario niet getest】**

---

## 3. Back-upprocedure

### 3.1 Enkele node (aanbevolen)

```powershell
# 1) Stop het proces (voorgrond: Ctrl+C; achtergrond: Stop-Process)
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Kopieer de volledige data_dir (met tijdstempel)
$data = "C:\swiftmq\data"
Copy-Item -Recurse -Force $data "C:\backup\swiftmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (optioneel) Controleer of de metadatasnapshot in de back-up parseerbaar is
Get-Content "C:\backup\swiftmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Elke node back-upt zijn eigen `data_dir`** (metadata wordt via Raft naar alle nodes gerepliceerd, berichtgegevens staan op de Owner-node, en de replicaat van quorumqueues staat in de eigen Raft-map).
- Volgorde van stoppen: **stop slechts één node per keer**; stop niet meerdere stemgerechtigde leden tegelijk (zie `upgrade.md` §7.2).
- Voor een **consistente snapshot van het hele cluster** moet je alle nodes na elkaar stoppen en elk afzonderlijk kopiëren; in productie komt "node voor node stoppen/kopiëren/starten" vaker voor.
- **【niet geverifieerd】** Op deze machine is geen echte clusterback-up/-hersteloefening uitgevoerd.

### 3.3 Docker (benoemd volume)

```powershell
# Stop de container en pak de inhoud van het volume uit en kopieer die eruit met een eenmalige container
docker compose down
docker run --rm -v swiftmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/swiftmq-data.tar.gz -C /data .
```
> **【niet geverifieerd】** (Docker is op deze machine niet uitgevoerd).

---

## 4. Herstelprocedure

### 4.1 Enkele node

```powershell
# 1) Controleer dat het proces is gestopt
Get-Process -Name swiftmqd -ErrorAction SilentlyContinue

# 2) Verplaats (of verwijder) de huidige data_dir om te voorkomen dat oude en nieuwe bestanden door elkaar raken
Move-Item "C:\swiftmq\data" "C:\swiftmq\data.broken"

# 3) Herstel met de back-up
Copy-Item -Recurse -Force "C:\backup\swiftmq-YYYYMMDD-HHMMSS" "C:\swiftmq\data"

# 4) Starten
& "C:\swiftmq\swiftmqd.exe" -config "C:\swiftmq\configs\swiftmqd.json" -log-level info
```

Belangrijke punten:
- **De oude map moet eerst worden verplaatst**; je mag back-upbestanden niet "over een half achtergebleven map heen kopiëren";
- De herstelde `data_dir` moet de **zelfde verzameling vhosts/queues** hebben als tijdens de back-up (de mapnamen zijn gecodeerd en werken ook op een andere machine);
- **Profiteer niet** van het herstel om `vhosts`/`users` in het configuratiebestand te wijzigen (deze werken alleen bij de eerste bootstrap en wijzigingen hebben geen effect, zie `upgrade.md` §4.2).

### 4.2 Cluster

- Eén node herstellen: herstel de `data_dir` van die node volgens §4.1 en start hem; hij voegt zich als bestaand lid weer toe en haalt de Raft-log in.
- Het hele cluster herstellen: **herstel en start eerst een meerderheid van de nodes** (≥ de helft van de stemgerechtigde leden), zodat het cluster een leader kan kiezen; herstel daarna de overige nodes.
- **【niet geverifieerd】** Clusterherstel is niet in de praktijk getest.

---

## 5. Verificatiemethode na herstel

Kruiscontroleer met de beheer-API en een echte client (alle stappen worden aanbevolen):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Totalen van objecten en berichten (aantal queues/exchanges/bindings/gebruikers; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) Controleer per queue messages / messages_ready (vergelijk met de registratie vóór de back-up)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) Of vhost / gebruikers / policies allemaal aanwezig zijn
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Cluster (een enkele node retourneert enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Bekijk het opstartlog**: er zouden `已从磁盘恢复队列消息 ... messages=N` en `队列已恢复持久化消息 ... messages=N` moeten verschijnen; N moet gelijk zijn aan vóór de back-up.
- **Waarschuwingen in het log**: als een queue eerder berichten heeft gehad die zijn geconsumeerd/gewist, kunnen bij het herstel
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` en `恢复时清理了无存活消息的段` verschijnen.
  Dit zijn indexresten van **reeds afgewikkelde (ack/purge) records** en vormen **bekende logruis die de correctheid van de gegevens niet aantast** (zie §7).
- **Echte client**: haal de berichten uit de queue en controleer het aantal/de inhoud (zie stap (7) in §6).

---

## 6. Praktijkoefening (echte opdrachten en uitvoer)

> Omgeving: `data_dir` in een tijdelijke map, AMQP `127.0.0.1:5676`, beheervlak `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> standaardaccount `guest/guest`. Opstartlog:
> ```
> level=INFO msg="SwiftMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Een durable topologie aanmaken + 5 persistente berichten publiceren (echte client `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Gebruiker / vhost / rechten / policies aanmaken (beheer-API)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Status vóór de back-up (beheer-API)**

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

**(4) Schijfbestanden vóór de back-up**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Proces stoppen → back-up → leegmaken → herstellen**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Herstellog na herstart (belangrijke regels)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Tegelijk verschijnen enkele regels `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> dit zijn indexresten van berichten die in deze oefening ** eerder zijn gepurged** (reeds afgewikkeld, segmentgegevens zijn teruggewonnen), en ze **hebben geen invloed op het herstel van de 5 onderstaande berichten**.

**(7) Assertie na herstel: beheer-API + echte client**

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

**Conclusie**: de durable topologie (exchange + queue + binding), 5 persistente berichten, gebruikers, vhosts, rechten en policies zijn **allemaal hersteld**,
en de echte client kan alle berichten ongewijzigd terughalen. **De oefening is geslaagd.**

### 6.1 Controle: herstel van queues zonder consumptie/purge is "rustiger"

Om te bepalen of de bovenstaande WARN een algemeen verschijnsel is, is een extra **gecontroleerde vergelijking** uitgevoerd: maak een nieuwe durable queue `clean.q` aan, publiceer 3 persistente berichten,
**zonder te consumeren of te wissen**, stop het proces en start opnieuw:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Geen enkele WARN**. Dit toont aan dat WARN alleen optreedt in scenario's waarin "de index nog afgewikkelde records bevat" (zie §7).

---

## 7. Bekende problemen en beperkingen (naar waarheid vastgelegd)

1. **Herstellogruis (echt waargenomen)**: wanneer een queue in het verleden consumptie/leegmaken heeft gekend (berichten zijn ack/purge),
   blijft de index verwijzingen naar teruggewonnen records bevatten, en bij het herstel wordt **voor elk daarvan `恢复消息失败，已跳过` WARN afgedrukt**,
   en wordt een leeg `000000.seg` aangemaakt/opgeruimd (log `恢复时清理了无存活消息的段`).
   Dit **beïnvloedt de correctheid van de gegevens niet** (nog levende onbevestigde berichten worden correct hersteld), maar **vervuilt het log** en kan bij grote queues/hoge doorvoer het scherm vollopen.
   Aanbeveling: ga uit van `已从磁盘恢复队列消息 ... messages=N` en negeer deze WARN voor afgewikkelde records;
   als de loghoeveelheid onaanvaardbaar is, meld dit dan aan de kernel-onderhouder (dit document wijzigt geen code).
2. **Hot back-up is onveilig** (§2): kopieer `data_dir` niet direct terwijl het proces draait.
3. **`fsync: os` garandeert niet dat een stroomstoring geen verlies veroorzaakt**: gebruik `batch` / `always` voor "bij confirm al naar schijf".
4. **Wachtwoorden in platte tekst**: gebruikerswachtwoorden in `meta/state.json` staan in **platte tekst** (in de praktijk is `"password":"drillpass"` zichtbaar) ——
   back-upbestanden moeten daarom **als gevoelige gegevens worden behandeld** (toegangscontrole, versleutelde opslag). Zie `security-baseline.md`.
5. **【niet geverifieerd】** Clusterback-up/-herstel, back-up/herstel van Docker-volumes, stroomstoringsscenario's en gelijktijdige schrijfacties tijdens het herstel.
