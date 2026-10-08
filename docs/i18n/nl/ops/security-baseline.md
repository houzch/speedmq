# SpeedMQ beveiligingsbaseline (checklist met aanvinkvakjes)

> Principe: **alleen capaciteiten die deze repository daadwerkelijk heeft worden beschreven**. Bij elk item staat "waarom je het moet doen + hoe je verifieert dat het gedaan is"; de verificatieopdrachten zijn allemaal uitvoerbaar.
> Items gemarkeerd met **【geverifieerd】** zijn op deze machine (Windows + PowerShell 5.1, `1.0.0`) **daadwerkelijk uitgevoerd**;
> **【niet geverifieerd】** betekent niet uitgevoerd of momenteel niet mogelijk — er wordt nooit gedaan alsof.
> Alle opdrachten worden in `/bin/sh`-stijl als curl-versie gegeven, met daarbij een PowerShell-versie (gebruik voor PowerShell 5.1
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Authenticatie en toegangscontrole

### A-1. Het standaardaccount `guest/guest` wijzigen 【geverifieerd】

- **Waarom**: standaard is `guest/guest` ingebouwd (tag `administrator`); blootstelling naar buiten staat gelijk aan de deur wagenwijd openzetten.
- **Hoe**: wijzig het wachtwoord / verwijder het account tijdens bedrijf; **wijzig niet** het configuratiebestand (`users` werkt alleen bij de eerste bootstrap).

```bash
# Wachtwoord wijzigen
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# Of verwijder het standaardaccount direct (zorg eerst dat er een nieuwe beheerder is aangemaakt)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Hoe verifiëren**: na de wijziging moet het oude wachtwoord 401 geven en het nieuwe 200.
  **【geverifieerd】** Werkelijke uitvoer op deze machine:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Standaardaccount gewijzigd/verwijderd

### A-2. Minimaal rechten: `configure` / `write` / `read`-regexes per vhost 【geverifieerd】

- **Waarom**: de drie categorieën zijn afgestemd op RabbitMQ —— `configure` beheert het declareren/verwijderen van topologie, `write` beheert publiceren en binden, `read` beheert consumeren en pullen;
  onbevoegde toegang geeft 403. Geef een bedrijfsaccount alleen wat het nodig heeft.
- **Hoe**: `PUT /api/permissions/{vhost}/{user}`, bijvoorbeeld alleen-lezen consumeren: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Hoe verifiëren**: probeer met een beperkt account een onbevoegde bewerking; je moet 403 `ACCESS_REFUSED` krijgen.
  **【geverifieerd】** Op deze machine is met een echte AMQP-client met een gebruiker met `configure="^$"` een exchange gedeclareerd; meting:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Elk bedrijfsaccount krijgt alleen de noodzakelijke regexes, en niet de tags `administrator`/`management`

### A-3. De reikwijdte van de tag `administrator` (impliciete volledige rechten) —— met zorg toekennen 【geverifieerd】

- **Waarom**: **een gebruiker met de tag `administrator` heeft volledige rechten op alle vhosts die hij kan zien, zonder dat er een rechtenrecord nodig is**
  (afgestemd op de gemeten reikwijdte van RabbitMQ, zie README / ontwerp M8-7). Met andere woorden: zodra deze tag is toegekend,
  doen de rechtenregexes niet meer mee — dit is de hoogste bevoegdheid.
- **Hoe**: geef `administrator` alleen aan beheervlak-/beheeraccounts; geef bedrijfsaccounts nooit een tag en laat ze uitsluitend via rechtenregexes werken.

- **Hoe verifiëren (impliciete rechten demonstreren)**: op een nieuwe vhost **zonder enig rechtenrecord** moet een `administrator`-gebruiker direct kunnen werken.
  **【geverifieerd】** Meting op deze machine: na het aanmaken van vhost `drillvh` (zonder enig rechtenrecord) kon `guest` (administrator) daar met succes topologie declareren:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] De tag `administrator` wordt slechts aan zeer weinig beheeraccounts toegekend

### A-4. `remote_access`: accounts beperken tot alleen lokale aanmelding 【niet geverifieerd (op dezelfde machine kan geen externe bron worden gesimuleerd)】

- **Waarom**: afgestemd op RabbitMQ staat de ingebouwde `guest` standaard alleen lokale aanmelding toe; bij een externe uitrol moet je ervoor zorgen dat de bron van bevoorrechte accounts beperkt is.
- **Hoe / reikwijdte (belangrijke beperking)**:
  - `remote_access` kan alleen in `users.<name>.remote_access` van het **configuratiebestand** worden gezet en **werkt alleen bij de eerste bootstrap**;
  - **accounts die via de beheer-API / `speedmqctl` worden aangemaakt, hebben altijd `remote_access=true`** (aanmelding vanaf elke bron toegestaan) ——
    op basis van de `UpsertUser`-commentaar in `internal/broker/observe.go` en de gemeten `"remote_access":true` in `meta/state.json`.
    Met andere woorden: **de API kan een account momenteel niet beperken tot alleen lokaal**.
- **Hoe verifiëren**: maak vanaf **een andere host** (niet `127.0.0.1`) met dat account verbinding; je moet 403 krijgen; een lokale verbinding moet slagen.
  **【niet geverifieerd】**: in de omgeving van deze machine kan geen echte externe bron worden geconstrueerd; niet in de praktijk getest.
- [ ] De bronbeperking van bevoorrechte accounts is volgens de bovenstaande reikwijdte beoordeeld (let op: accounts die via de API worden aangemaakt, staan standaard open voor extern)

---

## B. Transportbeveiliging (TLS)

TLS-configuratieopties (de toegangslaag en het beheervlak **delen** dezelfde set velden): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. TLS inschakelen en bij een foutieve configuratie de start weigeren 【geverifieerd】

- **Waarom**: certificaten worden **bij het opstarten** gelezen en gevalideerd — een foutieve configuratie weigert onmiddellijk de start, in plaats van pas zichtbaar te worden wanneer de eerste client verbinding maakt.
- **Hoe**: geef `cert_file` + `key_file` op in `listeners.<plugin>[].tls` of `management.tls` (**beide** vereist om in te schakelen).

- **Hoe verifiëren**: start met een foutieve configuratie; het moet onmiddellijk mislukken.
  **【geverifieerd】** Drie foutieve configuraties op deze machine, allemaal `exit=1`, start geweigerd:

  ```
  badtls1: speedmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: speedmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: speedmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Hoe verifiëren (positief/negatief)**: een TLS-client kan verbinden, een plaintext-client die verbinding maakt met de TLS-poort wordt geweigerd.
  **【geverifieerd】** Op deze machine is een TLS-instantie gestart (`amqp091` via TLS) en is met een echte client een probe uitgevoerd:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] Voor protocolpoorten die naar buiten gericht zijn, is TLS ingeschakeld

### B-2. `min_version` minimaal 1.2 【geverifieerd】

- **Waarom**: te oude TLS-versies uitschakelen; standaard is `1.2`, optioneel `1.2` / `1.3`.
- **Hoe verifiëren**: zet `min_version` op `1.0`; het opstarten moet een fout geven (zie de `badtls2`-uitvoer in B-1).
- [ ] `min_version` is `1.2` of `1.3`

### B-3. Wederzijdse authenticatie `client_auth: require_and_verify` (mTLS) 【gedeeltelijk geverifieerd】

- **Waarom**: verlangt dat de client een certificaat toont en laat valideren, om te voorkomen dat ongeautoriseerde clients verbinding maken met de protocolpoorten.
- **Hoe**: configureer `ca_file` + `client_auth: require_and_verify` (dit vereist dat `ca_file` ook wordt geleverd).
- **【geverifieerd】**: TLS end-to-end en het geweigerde pad zijn met echte clients geverifieerd (B-1). **mTLS (het vereisen en valideren van clientcertificaten) is op deze machine niet afzonderlijk geoefend**.
- [ ] Poorten die mTLS nodig hebben, zijn geconfigureerd met `require_and_verify` + `ca_file`

### B-4. TLS voor het beheervlak 【niet geverifieerd】

- **Waarom**: het beheervlak verzendt wachtwoorden via Basic Auth en moet dus worden versleuteld.
- **Hoe**: `management.tls` gebruikt dezelfde velden als de protocol-listeners.
- **【niet geverifieerd】**: in de oefening op deze machine was het beheervlak gebonden aan een lokale plaintext-poort; er is geen afzonderlijke HTTPS voor het beheervlak gestart.
- [ ] TLS is ingeschakeld voor het beheervlak (of het is strikt beperkt tot een vertrouwd netwerk)

---

## C. Beperken van het aanvalsoppervlak

### C-1. Het luisterbereik van het beheervlak beperken 【geverifieerd (luisteradres in de praktijk gemeten)】

- **Waarom**: het beheervlak luistert standaard op `:15672` (alle netwerkkaarten). Bij een externe uitrol moet je aan een intern/loopback-adres binden of de bron met een firewall beperken.
- **Hoe**: stel `management.addr` in op `127.0.0.1:15672` of een intern adres; of schakel het volledig uit met `management.enabled=false`
  (daarna is er geen beheerpoort meer, maar is `speedmqctl` ook niet meer beschikbaar).
- **Hoe verifiëren**:
  **【geverifieerd】** Op deze machine is het beheervlak ingesteld op `127.0.0.1:15677`; het gemeten luisteradres was inderdaad loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] Het bindingsadres van het beheervlak is beperkt (of uitgeschakeld)

### C-2. Alleen de noodzakelijke protocolpoorten openen 【niet geverifieerd】

- **Waarom**: standaard staan zowel AMQP `5672` als MQTT `1883` open; schakel MQTT uit als je het niet gebruikt om het aanvalsoppervlak te verkleinen.
- **Hoe**: `plugins.mqtt.enabled=false` (of verwijder het uit `listeners`); uitschakelen houdt in dat **de echte poort wordt gesloten**, niet slechts een statusbit wordt gewijzigd.
- **Hoe verifiëren**: na het uitschakelen luistert de betreffende poort niet meer (niet zichtbaar met `Get-NetTCPConnection -State Listen`).
  **【niet geverifieerd】**: in de oefening op deze machine stonden beide protocollen open; het verdwijnen van de poort na het uitschakelen is niet afzonderlijk geverifieerd.
- [ ] Ongebruikte protocolplugins zijn uitgeschakeld

---

## D. Beveiligingsversterking van de containeruitvoering

Feiten over de image in deze repository (`Dockerfile`): statisch gelinkte binary + alpine, **draait als niet-root (uid 10001, gebruiker `speedmq`)**,
en de gegevensmap `/var/lib/speedmq` is een volume. `docker-compose.yml` gebruikt een **benoemd volume** voor persistentie, **alleen-lezen mount** van de configuratie en logrotatie.

### D-1. Draaien als niet-root 【niet geverifieerd (Docker is op deze machine niet uitgevoerd)】

- **Waarom**: minimale rechten om de impact van een containerontsnapping te verkleinen.
- **Hoe**: de image is standaard al uid 10001; overschrijf dit **niet** met `--user root`.
- **Hoe verifiëren**: `docker compose run -T --rm broker id` moet `uid=10001` tonen. (in een niet-interactieve omgeving moet `run` `-T` meekrijgen)
- [ ] De container draait als niet-root (niet overschreven met root)

### D-2. Alleen-lezen rootbestandssysteem + resourcelimieten + capaciteiten beperken (aanbevolen, niet standaard ingeschakeld in de compose van deze repository) 【niet geverifieerd】

- **Waarom**: een alleen-lezen rootbestandssysteem voorkomt het tijdens bedrijf wijzigen van de binary; resourcelimieten voorkomen dat één container de host platlegt; het beperken van capabilities verkleint het aanvalsoppervlak van de kernel.
- **Hoe** (voorbeeld, voeg naar behoefte samen in de `broker`-service van compose):

```yaml
services:
  broker:
    read_only: true
    tmpfs:
      - /tmp
    security_opt:
      - no-new-privileges:true
    cap_drop: ["ALL"]
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 8g
```

- **Hoe verifiëren**: een poging om in de container naar het rootpad te schrijven moet mislukken (alleen-lezen); met `docker inspect` zijn de resourcelimieten zichtbaar.
  **【niet geverifieerd】** (Docker is op deze machine niet uitgevoerd); en **bij een alleen-lezen rootbestandssysteem moet je bevestigen dat `data_dir` op een beschrijfbaar volume staat**, anders kan de kernel niet naar schijf schrijven.
- [ ] Het alleen-lezen rootbestandssysteem en de resourcelimieten zijn beoordeeld (let op: `data_dir` moet op een beschrijfbaar volume staan)

---

## E. Bekende beperkingen (momenteel echt niet mogelijk; reken er niet op)

Het volgende zijn allemaal **feitelijke hiaten**; erken ze expliciet in het beveiligingsontwerp en ga er niet van uit dat ze bestaan:

1. **Wachtwoorden worden in platte tekst opgeslagen en gekopieerd**. Het veld `password` in `meta/state.json` staat in platte tekst (**【geverifieerd】** in de praktijk is
   `"password":"drillpass"` zichtbaar); ook in het configuratiebestand staat het in platte tekst. Er is **geen** wachtwoordhashing (hashing en externe authenticatiebackends zijn voorbehouden aan authenticatieplugins).
   → Gevolg: **de gegevensmap en back-upbestanden zijn gelijk aan gevoelige inloggegevens** en moeten met bestandsrechten en versleuteling worden beschermd.
2. **Er is geen auditlog**. Toevoegingen/wijzigingen/verwijderingen op het beheervlak worden wel in het gewone log geschreven (zoals `管理面更新用户 actor=... user=...`),
   maar er is **geen** afzonderlijke, niet-manipuleerbare auditstream en ook geen compliance-niveau registratie van "wie heeft wanneer wat gewijzigd".
3. **Geen externe authenticatie zoals LDAP / OAuth2 / JWT**. v1 heeft alleen ingebouwd `PLAIN` / `AMQPLAIN`
   (`auth.Store.Mechanisms()` retourneert in de praktijk alleen deze twee).
4. **SASL `EXTERNAL` is niet geïmplementeerd**: zelfs met mTLS geconfigureerd, gebruikt de protocollaag **nog steeds PLAIN-wachtwoordauthenticatie**
   (de stap "met een clientcertificaat zonder wachtwoord" ontbreekt). Het certificaat is alleen transportlaagvalidatie.
5. **`remote_access` kan niet via de API worden ingesteld**: accounts die via de beheer-API/CLI zijn aangemaakt, staan altijd open voor externe aanmelding (zie A-4);
   een enkel account kan niet tot alleen lokaal worden beperkt.
6. **Het beheervlak heeft geen afzonderlijke bronwhitelist / geen rate limiting**: het aanvalsoppervlak kan alleen worden beperkt via bindingsadressen, firewalls en TLS.
7. **Geen pluginsandbox**: vorm A-plugins draaien in hetzelfde proces als de kernel; vorm B externe plugins hebben wel procesisolatie, maar **het datavlak loopt via een lokale verbindingsproxy**
   en plugins kunnen kernelsemantiek aanroepen (beperkt door vhost- en rechtenvalidatie); dit is **geen** veiligheidssandbox.
8. **Het beheervlak kent slechts drie niveaus van tags: `administrator`/`management`/`monitoring`**, zonder fijnmazigere RBAC per resource.

---

## F. Overzichtslijst

- [ ] A-1 Standaardaccount gewijzigd/verwijderd 【procedure geverifieerd】
- [ ] A-2 Minimaal rechten voor bedrijfsaccounts (regexes), geen beheerderstag 【403-pad geverifieerd】
- [ ] A-3 De tag `administrator` wordt alleen aan beheeraccounts toegekend 【reikwijdte van impliciete rechten geverifieerd】
- [ ] A-4 Bronbeperking van bevoorrechte accounts beoordeeld (let op: accounts via de API staan standaard open voor extern)
- [ ] B-1 TLS ingeschakeld op externe poorten, foutieve configuratie weigert de start 【geverifieerd】
- [ ] B-2 `min_version` ≥ 1.2 【geverifieerd】
- [ ] B-3 Poorten die mTLS nodig hebben, geconfigureerd met `require_and_verify` + `ca_file`
- [ ] B-4 TLS ingeschakeld voor het beheervlak
- [ ] C-1 Bindingsadres van het beheervlak beperkt 【luisteradres geverifieerd】
- [ ] C-2 Ongebruikte protocolplugins uitgeschakeld
- [ ] D-1 Container draait als niet-root
- [ ] D-2 Alleen-lezen rootbestandssysteem / resourcelimieten / capaciteiten beperken beoordeeld
- [ ] E Bekende beperkingen (platte-tekstwachtwoorden, geen audit, geen LDAP/OAuth2, SASL EXTERNAL niet geïmplementeerd) zijn in het beveiligingsontwerp erkend
