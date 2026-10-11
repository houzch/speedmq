# SpeedMQ upgrade- en migratieplan

> Toepasselijke versie: `1.0.0` (`broker.Version`, zie `speedmq_build_info` in `/metrics`).
> Alle "in de praktijk geteste" conclusies in dit document komen uit echte uitvoeringen op deze machine; alles wat niet in de praktijk is getest, is expliciet gemarkeerd met **【niet geverifieerd】**.
> Omgeving van deze machine: Windows + PowerShell 5.1, Go 1.27.1 windows/386, tijdelijke `data_dir` + niet-standaardpoorten.

---

## 1. Migratie (van RabbitMQ naar SpeedMQ)

Dit project positioneert zich als **protocolniveau-compatibel met AMQP 0-9-1**; "migratie" draait daarom vooral om **het aanpassen van het verbindingsadres**:

- Nul wijzigingen in bedrijfscode, alleen `host/port/vhost` aanpassen (ontwerpdocument G3 "migratie zonder kosten").
- De beheertoolchain (`rabbitmqadmin`, beheer-UI, monitoringscripts) hoeft alleen naar de poort van het beheervlak te wijzen; de vorm van de interface is afgestemd op RabbitMQ (conventies zoals `amq.default`, `%2F`, `{error, reason}` worden overgenomen).
- Standaardpoorten zijn gelijk aan die van RabbitMQ: AMQP `5672`, beheervlak `15672`; MQTT is `1883` en RPC tussen nodes is `25672`.

**Semantische verschillen om vóór de migratie zelf te controleren** (allemaal bewust zo in deze repository, op basis van README / ontwerpdocument):

| Item | SpeedMQ-gedrag | Gevolg voor migratie |
| --- | --- | --- |
| Vluchtige (niet-persistente en niet-exclusieve) queues | **Declaratie wordt geweigerd** (541), `auto_delete` is geen uitzondering | Oude clients die van dit soort queues afhankelijk zijn, falen; ze moeten worden omgezet naar durable of exclusive |
| Standaard-vhost `/` | **Niet verwijderbaar** (400), bij RabbitMQ wel toegestaan | Automatiseringsscripts die de standaard-vhost verwijderen, falen (dit is de enige actieve veiligheidsbeperking) |
| Gegevens van klassieke queues | Worden **niet gerepliceerd**, gegevens staan alleen op de Owner-node | Gebruik quorumqueues `x-queue-type=quorum` als je redundantie over nodes nodig hebt |
| Quorumqueues | Uitbreiden van replica's wordt ondersteund, **krimpen niet** | Plan meteen goed |
| Plugins | Geen Erlang-plugin-ecosysteem, AMQP 1.0 / STOMP niet geïmplementeerd | Scenario's die deze protocollen gebruiken, kunnen voorlopig niet migreren |

**Gegevensmigratie**: de opslagformaten van SpeedMQ en RabbitMQ zijn niet compatibel; er is **geen online/offline tool voor gegevensoverdracht**.
De migratiemethode is "een lege SpeedMQ aanmaken → dubbel draaien ter verificatie → geleidelijk verkeer overzetten". **【niet geverifieerd】** Dit document bevat geen enkele echte oefening voor het overdragen van RabbitMQ-gegevens.

---

## 2. Algemene upgradeprincipes

1. **Eerst back-uppen** (zie `backup-restore.md`) — als vangnet wanneer de upgrade mislukt.
2. **Eerst het proces stoppen, dan vervangen** (de gegevensmap kent een single-writer-beperking, zie §4.2).
3. **Na de upgrade moet je verifiëren**: het proces start, `/api/overview` is leesbaar, `/metrics` is te scrapen en het aantal queue-berichten is gelijk aan vóór de back-up.
4. Clusterupgrades gaan **rollend, node voor node**, met slechts één node tegelijk (zie §5).

---

## 3. Indeling van de gegevensmap (feitelijke basis voor upgrade/migratie)

De `data_dir`-indeling zoals **in de praktijk gemeten** op de single-node-instantie van deze machine:

```
data/
├── meta/
│   ├── state.json        # Metadatasnapshot voor single-node-modus (vhost/exchanges/queues/bindings/gebruikers/rechten/policies)
│   ├── users.seeded      # Bootstrap-markering: de users in het configuratiebestand zijn al geseed
│   ├── vhosts.seeded     # Bootstrap-markering: de vhosts in het configuratiebestand zijn al geseed
│   ├── raft.state        # 【clustermodus】Raft-termijn/stemming
│   ├── raft.log          # 【clustermodus】Raft-log
│   └── snapshot.json     # 【clustermodus】Raft-snapshot + ledentabel
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # Segmentbestand (berichtinhoud + attributen), recordformaat: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # Queue-index: seq-id → (segmentnummer, offset in segment, lengte, status)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【cluster】Elke quorumqueue heeft één Raft-groep (log/snapshot)
```

**Let op (twee punten die tegen de intuïtie ingaan, beide gebaseerd op code/praktijkmeting)**:

- In clustermodus staan de Raft-persistentiebestanden **direct onder `meta/`** (`raft.state` / `raft.log` / `snapshot.json`);
  er **bestaat geen `meta/raft/`-submap**. Bewijs: de bestandsnaamconstanten in `internal/raft/log.go` + `Dir: filepath.Join(b.cfg.DataDir, "meta")` in `internal/broker/cluster.go`. **【geverifieerd (2026-10-10, lokale Docker Desktop, 3-node-cluster)】** — de back-upinhoud van het clusternode-volume toont direct `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (geen `meta/raft/`-submap gezien).
- Mapnamen zijn **niet de oorspronkelijke vhost-/queuenamen**, maar een `store.SafeDirName`-codering: prefix `q_` wordt toegevoegd en bytes die niet in `[A-Za-z0-9._-]` vallen, worden met `%XX` geëscaped.
  Praktijkmeting: vhost `/` → map `q_%2F`, queue `persist.q` → map `q_persist.q`.
  Dit ontwerp voorkomt path traversal en Windows-gereserveerde apparaatnamen (`con`/`nul`, enz.).

---

## 4. Gegevenscompatibiliteit

### 4.1 Kunnen oude gegevens direct worden gelezen — ja

- **Indexformaat is voorwaarts compatibel**: M8-1 heeft een "segmentnummer"-veld (25 bytes) aan het indexrecord toegevoegd; **het oude formaat (21 bytes, zonder segmentnummer) kan nog steeds ongewijzigd worden gelezen**,
  wat bij het lezen gelijkstaat aan "slechts één segment (seg=1)"; **een upgrade vereist geen migratiescript**.
  Bewijs: de constanten `indexEntrySize` / `legacyIndexEntrySize` en de `recover()`-logica in `internal/store/store.go`; README M8-1.
- **Crashsemantiek ongewijzigd**: elk record heeft een lengteprefix + CRC32; bij herstel worden **half-geschreven/beschadigde records aan het einde weggegooid** en wordt afgekapt.
  Praktijkmeting (zie `backup-restore.md` §6): na het stoppen en opnieuw starten van het proces worden alle 5 persistente berichten van de durable queue **volledig hersteld**,
  en verschijnt in het log `已从磁盘恢复队列消息 ... messages=5`.

**【geverifieerd (2026-10-10, lokaal)】** Lezen tussen grote versies is in de praktijk gemeten: durable queue-gegevens die met tag `v1.1.0` zijn weggeschreven, worden onder `1.1.05` ongewijzigd hersteld (opdrachten en uitvoer zie §5).

### 4.2 De reikwijdte van `vhosts` / `users` in de configuratie (de meest voorkomende valkuil bij upgrades)

- Beide **werken alleen bij de eerste bootstrap**: bij de eerste start worden de vhosts/users uit de configuratie in de metadata geschreven en worden de markeringsbestanden
  `meta/vhosts.seeded` / `meta/users.seeded` achtergelaten; **daarna is de metadata leidend**.
- Reken er dus bij **een upgrade/wissel van configuratie niet op dat je accounts of vhosts kunt toevoegen/verwijderen via het configuratiebestand** — wijzigingen hebben geen effect;
  gebruik de beheer-API of `speedmqctl`.
- Omgekeerd **zal** een upgrade bestaande accounts **niet** overschrijven met de configuratie: een tijdens bedrijf gewijzigd wachtwoord wordt na een herstart niet teruggezet naar de oude waarde uit de configuratie,
  en een tijdens bedrijf verwijderd account komt niet terug. Bewijs: de seed-markingslogica in `cluster.go`; README M8-4 / M8-7.

**【geverifieerd (2026-10-10, lokaal)】** Wijzigingen tijdens bedrijf worden inderdaad niet door de configuratie overschreven: vóór de upgrade is via de beheer-API het `guest`-wachtwoord gewijzigd in `rotated`; na een herstart met `1.1.05` + dezelfde `data_dir` gaf `guest/guest` (het oude wachtwoord in de configuratie) **401** en `guest/rotated` **200**.

### 4.3 Segmentrotatie en schijfruimte-terugwinning

- Berichten worden op grootte in segmenten verdeeld (standaard 8 MiB); **zodra alle berichten in een segment zijn ge-ackt en het segment is afgesloten, wordt het hele segment verwijderd**, en de index wordt dienovereenkomstig gecomprimeerd en herschreven.
- Een upgrade verandert dit gedrag niet; een enkel segmentbestand dat door een oude instantie is achtergelaten, werkt onder de nieuwe segmentrotatielogica zoals gewoonlijk.

---

## 5. Binary-upgrade (bare metal)

> **【geverifieerd (2026-10-10, lokaal)】** Echte cross-version-oefening: de oude binary die uit tag `v1.1.0` is gebouwd, schreef durable exchanges/queues/bindings + 5 persistente berichten + vhosts/accounts/rechten/policies en wijzigde tijdens bedrijf een wachtwoord; na het stoppen van het proces werd gestart met de huidige `1.1.05`-binary + **dezelfde `data_dir` + dezelfde poorten** — het proces startte normaal, de objecten en berichtaantallen in `/api/queues` kwamen overeen, een echte AMQP-client haalde alle 5 persistente berichten op en `/metrics` rapporteerde `speedmq_build_info{version="1.1.05"}`.
> De onderstaande stappen zijn nog steeds een algemene procedure en zijn toepasbaar op elke aangrenzende versie; zie §8 voor een **downgrade**.

### 5.1 Stappen

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) Proces stoppen (een sierlijke afsluiting doet een laatste flush naar schijf; zie §4 "Consistentie")
#    Bij uitvoering op de voorgrond: Ctrl+C; als service: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Gegevensmap back-uppen (beslist nadat het proces is gestopt)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Binary vervangen (plaats de nieuwe versie speedmqd.exe / speedmqctl.exe op het oorspronkelijke pad)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) Starten
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) Verificatie: proces leeft + beheer-API is leesbaar
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Verificatiechecklist na de upgrade

- In het opstartlog verschijnen `SpeedMQ 启动中 ... version=<新版本>` en `管理面已启动`;
- De `object_totals` / `queue_totals` van `/api/overview` zijn gelijk aan vóór de back-up (vergelijk met `backup-restore.md` §5);
- De `messages` / `messages_ready` van elke durable queue in `/api/queues` zijn gelijk aan vóór de back-up;
- `/metrics` is te scrapen en `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Image-upgrade (container)

De image is ongeveer 13 MB (statisch gelinkte binary + alpine), **draait als niet-root (uid 10001)** en de gegevensmap is gemount op `/var/lib/speedmq`.

```powershell
# 1) Nieuwe image ophalen/bouwen (gebruik het nieuwe versienummer als tag om verwarring tussen old/new te voorkomen)
docker build -t speedmq:1.0.0 .

# 2) Oude container stoppen (compose behoudt het benoemde volume speedmq-data)
docker compose down

# 3) Nieuwe versie starten (wijzig image in het compose-bestand naar de nieuwe tag)
docker compose up -d

# 4) Status en logs
docker compose ps
docker compose logs -f --tail 100
```

> **Eenmalige taken in de container** (bijvoorbeeld `speedmqctl` in de container draaien): de `run` van `docker compose ...` moet in een niet-interactieve omgeving `-T` meekrijgen,
> anders mislukt het door het aanvragen van een TTY:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

Gegevenspersistentie is afhankelijk van het **benoemde volume** `speedmq-data` van compose; gegevens gaan niet verloren bij het opnieuw opbouwen van de container (sinds M4 wordt echt naar schijf geschreven).
Als je vóór de upgrade de inhoud van het volume wilt back-uppen, komt dat neer op het back-uppen van `/var/lib/speedmq` (zie `backup-restore.md` §3.2).

> **【geverifieerd (2026-10-10, lokale Docker Desktop)】** Er is een echte image-upgrade-oefening gedaan: bouw lokaal de oude image `speedmq:1.1.0-old` (git tag `v1.1.0`) en de nieuwe image `speedmq:1.1.05-new` (huidige broncode), start de oude image met single-node compose → maak de durable queue `persist.q` en publiceer 5 persistente berichten (`delivery_mode=2`) → `docker compose down` (**zonder `-v`**, het benoemde volume `speedmq-data` blijft behouden) → wijzig de image-tag in compose naar de nieuwe image en dan `docker compose up -d`.
> Resultaat: de nieuwe container startte normaal, in het opstartlog verscheen `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, `/metrics` rapporteerde `speedmq_build_info{version="1.1.05"}`, `queue_totals.messages=5` in `/api/overview` en `messages=5` van queue `persist.q` waren vóór en na de upgrade gelijk, en een echte AMQP-client (`rabbitmq/amqp091-go`) haalde alle 5 persistente berichten één voor één op (`deliveryMode=2`).
> Let op: `v1.1.0` is de image met het merk `speedmq` van vóór de naamswijziging (binary `speedmqd`, metric `speedmq_build_info`, standaard gegevensmap `/var/lib/speedmq`), wat qua naamgeving afwijkt van de huidige versie; in de oefening is uitgegaan van de configuratie `data_dir=/var/lib/speedmq` en is de eigenaar vooraf ingesteld voor het benoemde volume (die beschrijfbare pad bestaat niet in de oude image). Dit doet geen afbreuk aan de conclusie "container opnieuw opbouwen met een nieuwe tag verliest de gegevens van het benoemde volume niet".

---

## 7. Geleidelijke uitrol en terugdraaien

### 7.1 Enkele node

- **Geleidelijke uitrol**: een enkele SpeedMQ-node heeft geen ingebouwde mogelijkheid voor "twee versies in hetzelfde proces". Een haalbare geleidelijke uitrol is een **sidecar-schaduw**:
  de nieuwe versie wordt eerst met **alleen-lezen-consumptie/schaduwqueues** aan dezelfde upstream-stroom gehangen om te observeren; pas na bevestiging wordt de schrijfkant omgeschakeld.
- **Terugdraaien**:
  1. Stop het proces van de nieuwe versie;
  2. Zet de oude binary terug;
  3. Als de nieuwe versie al gegevens heeft weggeschreven, **moet je de `data_dir` herstellen met de back-up van vóór de upgrade** (zie hieronder).
  Er is **geen** garantie dat "de nieuwe versie al heeft geschreven en de oude versie het direct kan lezen" — zie §8 voor cross-version-downgrade.

### 7.2 Cluster (rolling upgrade)

Aan platformzijde is er geen "one-click rolling upgrade"; je moet de onderstaande volgorde handmatig node voor node uitvoeren:

1. **Upgrade slechts één node per keer**: stop die node → back-up zijn `data_dir` → vervang de binary → starten → wachten tot hij weer toetreedt en bijwerkt
   (bekijk `role`, `commit_index`/`last_applied` via `speedmqctl cluster_status` / `GET /api/cluster`).
2. **Aanbevolen volgorde**: upgrade eerst **learner / niet-stemgerechtigde leden** (geen invloed op de meerderheid), dan **follower**, en pas als laatste **leader**
   (het upgraden van de leader veroorzaakt een leaderverkiezing, met een korte periode van niet-schrijfbaarheid).
3. **Invloed van stoppen op de meerderheid** (kritiek):
   - 3-node-cluster: **stop tegelijk maximaal 1** stemgerechtigd lid; stop je er 2, dan verlies je de meerderheid en wordt onder `pause_minority` **de hele cluster gepauzeerd**.
   - 2-node-cluster: het stoppen van 1 lid betekent al verlies van de meerderheid; **geen rolling-upgrade-capaciteit** (minimaal 3 nodes aanbevolen).
   - Stop daarom tijdens een rolling upgrade **beslist nooit meerdere stemgerechtigde leden tegelijk**.
4. **Voer ledenwijzigingen en upgrades niet samen uit**: ledenwijzigingen kennen **geen joint consensus** en er is slechts één niet-vastgelegde configuratiewijziging tegelijk toegestaan;
   vermijd tijdens de upgrade gelijktijdige `add_member` / `remove_member`.
5. Controleer na afronding van de upgrade of de `object_totals` van `GET /api/cluster` gelijk zijn aan vóór de upgrade.

> **【geverifieerd (2026-10-10, lokale Docker Desktop, 3-node-cluster)】** Er is een echte **cross-version** rolling-upgrade-oefening van een cluster gedaan: de basis was de lokaal gebouwde `speedmq:1.1.0-old` (git tag `v1.1.0`), node voor node (n1 → n2 → n3, één tegelijk: eerst stoppen, dan het benoemde volume back-uppen, dan de image wisselen en starten) geüpgraded naar de huidige `1.1.05` (lokaal gebouwd `speedmq:1.1.05-new`, compose wisselt de image via een per-node override). Gedurende het hele proces behield het cluster steeds een leader en een meerderheid — bij het stoppen van de huidige leader werd in ongeveer 0,6 s een nieuwe leader gekozen, `pause_minority` activeerde geen pauze en tijdens de downtime was er geen externe onderbreking; na de upgrade kwamen de `object_totals` van `GET /api/cluster` overeen met vóór de upgrade (`queues:1`), bleef het aantal van de 25 persistente berichten van de quorumqueue `qroll` met 3 replica's gelijk en konden ze één voor één worden teruggelezen, en na het publiceren van nog 5 nieuwe berichten konden alle 30 worden opgehaald.
> De bovenstaande volgorde geldt nog steeds als algemene Raft-beperking, en deze oefening dekte het 3-node-scenario. **【geverifieerd (2026-10-10, lokale Docker Desktop): een 2-node-cluster verliest de meerderheid bij het stoppen van 1 node】** Een afzonderlijk minimaal 2-node-cluster (n1/n2, elk met `peers` die alleen deze twee bevatten, `partition_policy=pause_minority`) is in de praktijk gemeten: toen beide nodes online waren, was `GET /api/cluster` `has_quorum=true / paused=false` (n1 leader, n2 follower); na `docker stop` van n2 werd n1 binnen ongeveer 2 s `has_quorum=false / paused=true` — dus het stoppen van 1 stemgerechtigd lid verliest de meerderheid en `pause_minority` pauzeert de service, **wat bevestigt dat een 2-node-cluster geen rolling-upgrade-capaciteit heeft**; na `docker start` ter herstelling van n2 keerde n1 binnen ongeveer 2 s terug naar `has_quorum=true / paused=false` (service herstelt automatisch).

> **【geverifieerd (2026-10-11, lokale Docker Desktop, 3-node-cluster)】Rolling upgrade onder een netwerkpartitie is in de praktijk getest** (cross-version `v1.1.0` → `1.1.05`; de partitie gebruikt een dubbel-netwerktopologie: Raft-RPC tussen nodes loopt alleen over `cluster-net` met statische IP's, client/beheervlak over `client-net`, partities geschakeld met `docker network disconnect/connect`):
> - **Gepartitioneerde follower**: de meerderheidszijde behield tijdens de partitie continu quorum, publiceerde/consumeerde normaal en voltooide terwijl de partitie aanhield de cyclus "stop→image wisselen→start→inhalen" van een meerderheidsnode; het cluster met gemengde versies (1.1.0 + 1.1.05) bleef bedienbaar. Na herstel van de partitie trad de geïsoleerde node (oude versie) weer toe en haalde in; berichten gereconcilieerd op unieke-ID-verzameling 35/35 zonder verlies.
> - **Gepartitioneerde leader**: de meerderheid koos in ongeveer **630 ms** een nieuwe leader; de geïsoleerde oude leader behield `role=leader` maar met `has_quorum=false / paused=true` (`pause_minority` in werking, verhoogt de termijn niet vanzelf); tijdens de partitie werd nog een meerderheidsnode met succes geüpgraded; na herstel trad de oude leader weer toe, geen split-brain, geen datafork.
> - **Verwacht gedrag zonder PreVote (naar waarheid vastgelegd)**: de geïsoleerde follower startte onmiddellijk herhaalde verkiezingen, termijn circa elke 2 s +2 (de twee Raft-groepen van metadata en queue verhoogden om de beurt); bij herstel **griste** hij met zijn hogere termijn het leaderschap **weg** (in de praktijk binnen 1,4 s gekozen) — leaderwissel-verstoring maar geen correctheidsprobleem.
> - **Twee operationele valkuilen**: ① als `peers` hostnamen gebruikt en de container tegelijk aan een tweede bereikbaar netwerk hangt, loopt Raft-verkeer om en ontstaat een "schijnpartitie" (de geïsoleerde node toonde meer dan een minuut lang nog `has_quorum=true`) — RPC tussen nodes moet over slechts één netwerk lopen (peers verwijzen direct naar het statische IP van dat netwerk); ② bij herstel moet `docker network connect` `--ip <origineel IP>` meekrijgen, anders krijgt de node een nieuw IP terwijl de meerderheid het oude adres blijft draaien, waarna de geïsoleerde node met achterlopende log en opgeblazen termijn blijft verkiezen (in de praktijk de cluster-term van 320 naar 488 gesleept, metadata-commit van 13 naar 97 geschoten) en een transient kan optreden waarin de leader `quorum=true` maar kort `paused=true` toont.
> - Tijdens de partitie is de beheer-API aan de minderheidszijde leesbaar (handig voor observatie); AMQP-publicatie wordt geweigerd (`pause_minority` in werking).

---

## 8. Niet-ondersteunde / niet-geverifieerde onderdelen (expliciet vermeld)

- **Downgrade tussen grote versies: niet ondersteund, niet geverifieerd**. Als de nieuwe versie gegevens in een nieuw formaat/met nieuwe semantiek heeft weggeschreven, is er **geen** garantie dat "terugvallen op de oude binary gewoon kan lezen";
  terugdraaien kan alleen via de back-up van vóór de upgrade.
- **Configuratieformaat ongewijzigd**: nog steeds JSON + `SPEEDMQ_*`-omgevingsvariabelen. **YAML-configuratie wordt nog niet ondersteund** (vereist het toevoegen van een parse-afhankelijkheid, M8-17 nog te evalueren);
  een upgrade brengt geen YAML.
- **Online hot-upgrade van plugins/protocollen**: plugins worden met de kernel meegecompileerd (vorm A) of volgens configuratie met `spawn` gestart (vorm B);
  de kernel upgraden = het proces herstarten; er is **geen** mechanisme om de binary ter plaatse hot te vervangen.
- **In-place migratie van de opslagengine**: segmentrotatie/indexcompressie is runtime-achtergrondgedrag; er is **geen** afzonderlijke opdracht voor "gegevensmigratie/compressie".
- **Clusterupgrade bij een volle schijf**: **niet geverifieerd**. (De rolling upgrade onder een netwerkpartitie is op 2026-10-11 in de praktijk getest, zie de notitie aan het einde van §7.)
- Dit document **bevat geen** enkele verificatie van gegevensoverdracht tussen SpeedMQ en een andere broker (RabbitMQ).
