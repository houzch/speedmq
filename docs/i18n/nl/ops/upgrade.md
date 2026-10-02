# SwiftMQ upgrade- en migratieplan

> Toepasselijke versie: `1.0.0` (`broker.Version`, zie `swiftmq_build_info` in `/metrics`).
> Alle "in de praktijk geteste" conclusies in dit document komen uit echte uitvoeringen op deze machine; alles wat niet in de praktijk is getest, is expliciet gemarkeerd met **【niet geverifieerd】**.
> Omgeving van deze machine: Windows + PowerShell 5.1, Go 1.27.1 windows/386, tijdelijke `data_dir` + niet-standaardpoorten.

---

## 1. Migratie (van RabbitMQ naar SwiftMQ)

Dit project positioneert zich als **protocolniveau-compatibel met AMQP 0-9-1**; "migratie" draait daarom vooral om **het aanpassen van het verbindingsadres**:

- Nul wijzigingen in bedrijfscode, alleen `host/port/vhost` aanpassen (ontwerpdocument G3 "migratie zonder kosten").
- De beheertoolchain (`rabbitmqadmin`, beheer-UI, monitoringscripts) hoeft alleen naar de poort van het beheervlak te wijzen; de vorm van de interface is afgestemd op RabbitMQ (conventies zoals `amq.default`, `%2F`, `{error, reason}` worden overgenomen).
- Standaardpoorten zijn gelijk aan die van RabbitMQ: AMQP `5672`, beheervlak `15672`; MQTT is `1883` en RPC tussen nodes is `25672`.

**Semantische verschillen om vóór de migratie zelf te controleren** (allemaal bewust zo in deze repository, op basis van README / ontwerpdocument):

| Item | SwiftMQ-gedrag | Gevolg voor migratie |
| --- | --- | --- |
| Vluchtige (niet-persistente en niet-exclusieve) queues | **Declaratie wordt geweigerd** (541), `auto_delete` is geen uitzondering | Oude clients die van dit soort queues afhankelijk zijn, falen; ze moeten worden omgezet naar durable of exclusive |
| Standaard-vhost `/` | **Niet verwijderbaar** (400), bij RabbitMQ wel toegestaan | Automatiseringsscripts die de standaard-vhost verwijderen, falen (dit is de enige actieve veiligheidsbeperking) |
| Gegevens van klassieke queues | Worden **niet gerepliceerd**, gegevens staan alleen op de Owner-node | Gebruik quorumqueues `x-queue-type=quorum` als je redundantie over nodes nodig hebt |
| Quorumqueues | Uitbreiden van replica's wordt ondersteund, **krimpen niet** | Plan meteen goed |
| Plugins | Geen Erlang-plugin-ecosysteem, AMQP 1.0 / STOMP niet geïmplementeerd | Scenario's die deze protocollen gebruiken, kunnen voorlopig niet migreren |

**Gegevensmigratie**: de opslagformaten van SwiftMQ en RabbitMQ zijn niet compatibel; er is **geen online/offline tool voor gegevensoverdracht**.
De migratiemethode is "een lege SwiftMQ aanmaken → dubbel draaien ter verificatie → geleidelijk verkeer overzetten". **【niet geverifieerd】** Dit document bevat geen enkele echte oefening voor het overdragen van RabbitMQ-gegevens.

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
  er **bestaat geen `meta/raft/`-submap**. Bewijs: de bestandsnaamconstanten in `internal/raft/log.go` + `Dir: filepath.Join(b.cfg.DataDir, "meta")` in `internal/broker/cluster.go`. **【clusterindeling niet in de praktijk getest】** (op deze machine is alleen een single-node-instantie gedraaid).
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

### 4.2 De reikwijdte van `vhosts` / `users` in de configuratie (de meest voorkomende valkuil bij upgrades)

- Beide **werken alleen bij de eerste bootstrap**: bij de eerste start worden de vhosts/users uit de configuratie in de metadata geschreven en worden de markeringsbestanden
  `meta/vhosts.seeded` / `meta/users.seeded` achtergelaten; **daarna is de metadata leidend**.
- Reken er dus bij **een upgrade/wissel van configuratie niet op dat je accounts of vhosts kunt toevoegen/verwijderen via het configuratiebestand** — wijzigingen hebben geen effect;
  gebruik de beheer-API of `swiftmqctl`.
- Omgekeerd **zal** een upgrade bestaande accounts **niet** overschrijven met de configuratie: een tijdens bedrijf gewijzigd wachtwoord wordt na een herstart niet teruggezet naar de oude waarde uit de configuratie,
  en een tijdens bedrijf verwijderd account komt niet terug. Bewijs: de seed-markingslogica in `cluster.go`; README M8-4 / M8-7.

### 4.3 Segmentrotatie en schijfruimte-terugwinning

- Berichten worden op grootte in segmenten verdeeld (standaard 8 MiB); **zodra alle berichten in een segment zijn ge-ackt en het segment is afgesloten, wordt het hele segment verwijderd**, en de index wordt dienovereenkomstig gecomprimeerd en herschreven.
- Een upgrade verandert dit gedrag niet; een enkel segmentbestand dat door een oude instantie is achtergelaten, werkt onder de nieuwe segmentrotatielogica zoals gewoonlijk.

---

## 5. Binary-upgrade (bare metal)

> Op deze machine is **geen echte cross-version-oefening uitgevoerd** (de repository heeft momenteel slechts één versie `1.0.0` en geen oudere binary om naar te upgraden). De onderstaande stappen zijn een **herhalingsverificatie van dezelfde versie + een algemene procedure** voor de capaciteiten die deze repository al heeft; het cross-version-gedeelte is gemarkeerd met **【niet geverifieerd】**.

### 5.1 Stappen

```powershell
$base = "C:\swiftmq"
$data = "$base\data"

# 1) Proces stoppen (een sierlijke afsluiting doet een laatste flush naar schijf; zie §4 "Consistentie")
#    Bij uitvoering op de voorgrond: Ctrl+C; als service: Stop-Service / Stop-Process
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Gegevensmap back-uppen (beslist nadat het proces is gestopt)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Binary vervangen (plaats de nieuwe versie swiftmqd.exe / swiftmqctl.exe op het oorspronkelijke pad)
#    Copy-Item .\new\swiftmqd.exe $base\swiftmqd.exe -Force

# 4) Starten
& "$base\swiftmqd.exe" -config "$base\configs\swiftmqd.json" -log-level info

# 5) Verificatie: proces leeft + beheer-API is leesbaar
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Verificatiechecklist na de upgrade

- In het opstartlog verschijnen `SwiftMQ 启动中 ... version=<新版本>` en `管理面已启动`;
- De `object_totals` / `queue_totals` van `/api/overview` zijn gelijk aan vóór de back-up (vergelijk met `backup-restore.md` §5);
- De `messages` / `messages_ready` van elke durable queue in `/api/queues` zijn gelijk aan vóór de back-up;
- `/metrics` is te scrapen en `swiftmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Image-upgrade (container)

De image is ongeveer 13 MB (statisch gelinkte binary + alpine), **draait als niet-root (uid 10001)** en de gegevensmap is gemount op `/var/lib/swiftmq`.

```powershell
# 1) Nieuwe image ophalen/bouwen (gebruik het nieuwe versienummer als tag om verwarring tussen old/new te voorkomen)
docker build -t swiftmq:1.0.0 .

# 2) Oude container stoppen (compose behoudt het benoemde volume swiftmq-data)
docker compose down

# 3) Nieuwe versie starten (wijzig image in het compose-bestand naar de nieuwe tag)
docker compose up -d

# 4) Status en logs
docker compose ps
docker compose logs -f --tail 100
```

> **Eenmalige taken in de container** (bijvoorbeeld `swiftmqctl` in de container draaien): de `run` van `docker compose ...` moet in een niet-interactieve omgeving `-T` meekrijgen,
> anders mislukt het door het aanvragen van een TTY:
> ```powershell
> docker compose run -T --rm broker swiftmqctl -user guest -pass guest status
> ```

Gegevenspersistentie is afhankelijk van het **benoemde volume** `swiftmq-data` van compose; gegevens gaan niet verloren bij het opnieuw opbouwen van de container (sinds M4 wordt echt naar schijf geschreven).
Als je vóór de upgrade de inhoud van het volume wilt back-uppen, komt dat neer op het back-uppen van `/var/lib/swiftmq` (zie `backup-restore.md` §3.2). **【image-upgrade niet in de praktijk getest】** (Docker is op deze machine niet uitgevoerd).

---

## 7. Geleidelijke uitrol en terugdraaien

### 7.1 Enkele node

- **Geleidelijke uitrol**: een enkele SwiftMQ-node heeft geen ingebouwde mogelijkheid voor "twee versies in hetzelfde proces". Een haalbare geleidelijke uitrol is een **sidecar-schaduw**:
  de nieuwe versie wordt eerst met **alleen-lezen-consumptie/schaduwqueues** aan dezelfde upstream-stroom gehangen om te observeren; pas na bevestiging wordt de schrijfkant omgeschakeld.
- **Terugdraaien**:
  1. Stop het proces van de nieuwe versie;
  2. Zet de oude binary terug;
  3. Als de nieuwe versie al gegevens heeft weggeschreven, **moet je de `data_dir` herstellen met de back-up van vóór de upgrade** (zie hieronder).
  Er is **geen** garantie dat "de nieuwe versie al heeft geschreven en de oude versie het direct kan lezen" — zie §8 voor cross-version-downgrade.

### 7.2 Cluster (rolling upgrade)

Aan platformzijde is er geen "one-click rolling upgrade"; je moet de onderstaande volgorde handmatig node voor node uitvoeren:

1. **Upgrade slechts één node per keer**: stop die node → back-up zijn `data_dir` → vervang de binary → starten → wachten tot hij weer toetreedt en bijwerkt
   (bekijk `role`, `commit_index`/`last_applied` via `swiftmqctl cluster_status` / `GET /api/cluster`).
2. **Aanbevolen volgorde**: upgrade eerst **learner / niet-stemgerechtigde leden** (geen invloed op de meerderheid), dan **follower**, en pas als laatste **leader**
   (het upgraden van de leader veroorzaakt een leaderverkiezing, met een korte periode van niet-schrijfbaarheid).
3. **Invloed van stoppen op de meerderheid** (kritiek):
   - 3-node-cluster: **stop tegelijk maximaal 1** stemgerechtigd lid; stop je er 2, dan verlies je de meerderheid en wordt onder `pause_minority` **de hele cluster gepauzeerd**.
   - 2-node-cluster: het stoppen van 1 lid betekent al verlies van de meerderheid; **geen rolling-upgrade-capaciteit** (minimaal 3 nodes aanbevolen).
   - Stop daarom tijdens een rolling upgrade **beslist nooit meerdere stemgerechtigde leden tegelijk**.
4. **Voer ledenwijzigingen en upgrades niet samen uit**: ledenwijzigingen kennen **geen joint consensus** en er is slechts één niet-vastgelegde configuratiewijziging tegelijk toegestaan;
   vermijd tijdens de upgrade gelijktijdige `add_member` / `remove_member`.
5. Controleer na afronding van de upgrade of de `object_totals` van `GET /api/cluster` gelijk zijn aan vóór de upgrade.

> **【niet geverifieerd】** Op deze machine is geen echte rolling upgrade van een cluster uitgevoerd (noch het clusterpad noch containers zijn gedraaid); de bovenstaande volgorde komt voort uit algemene beperkingen van message brokers
> en Raft, plus de implementatiefeiten van `pause_minority` / ledenwijzigingen in deze repository, en is geen op deze machine gemeten conclusie.

---

## 8. Niet-ondersteunde / niet-geverifieerde onderdelen (expliciet vermeld)

- **Downgrade tussen grote versies: niet ondersteund, niet geverifieerd**. Als de nieuwe versie gegevens in een nieuw formaat/met nieuwe semantiek heeft weggeschreven, is er **geen** garantie dat "terugvallen op de oude binary gewoon kan lezen";
  terugdraaien kan alleen via de back-up van vóór de upgrade.
- **Configuratieformaat ongewijzigd**: nog steeds JSON + `SWIFTMQ_*`-omgevingsvariabelen. **YAML-configuratie wordt nog niet ondersteund** (vereist het toevoegen van een parse-afhankelijkheid, M8-17 nog te evalueren);
  een upgrade brengt geen YAML.
- **Online hot-upgrade van plugins/protocollen**: plugins worden met de kernel meegecompileerd (vorm A) of volgens configuratie met `spawn` gestart (vorm B);
  de kernel upgraden = het proces herstarten; er is **geen** mechanisme om de binary ter plaatse hot te vervangen.
- **In-place migratie van de opslagengine**: segmentrotatie/indexcompressie is runtime-achtergrondgedrag; er is **geen** afzonderlijke opdracht voor "gegevensmigratie/compressie".
- **Clusterupgrade onder een echt netwerk**: deze repository heeft alleen kleinschalige chaos uitgevoerd (kill op procesniveau); er is **geen** upgrade-oefening gedaan onder netwerkpartitie of een volle schijf.
- Dit document **bevat geen** enkele verificatie van gegevensoverdracht tussen SwiftMQ en een andere broker (RabbitMQ).
