# SpeedMQ Sicherheits-Hardening-Baseline (Checkliste zum Abhaken)

> Prinzip: **Es werden nur Fähigkeiten beschrieben, die dieses Repository tatsächlich besitzt**. Zu jedem Punkt wird angegeben, „warum man es tun sollte + wie man verifiziert, dass es getan wurde“; alle Verifikationsbefehle sind ausführbar.
> Mit **【Verifiziert】** gekennzeichnete Punkte bedeuten, dass sie auf diesem Rechner (Windows + PowerShell 5.1, `1.0.0`) **tatsächlich ausgeführt wurden**;
> **【Nicht verifiziert】** bedeutet, dass sie nicht ausgeführt wurden oder derzeit nicht möglich sind – es wird niemals etwas vorgetäuscht.
> Alle Befehle werden im `/bin/sh`-Stil als curl-Version angegeben, ergänzt um eine PowerShell-Version (bei PowerShell 5.1 bitte
> `Invoke-WebRequest ... -UseBasicParsing` verwenden).

---

## A. Authentifizierung und Zugriffskontrolle

### A-1. Standardkonto `guest/guest` ändern 【Verifiziert】

- **Warum**: Standardmäßig sind `guest/guest` integriert (Tag `administrator`); wer dies nach außen exponiert, öffnet damit Tür und Tor.
- **Vorgehen**: Zur Laufzeit Passwort ändern / Konto löschen; **nicht** die Konfigurationsdatei ändern (`users` wird nur beim ersten Bootstrap wirksam).

```bash
# Passwort ändern
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# oder das Standardkonto direkt löschen (zuerst sicherstellen, dass ein neuer Administrator angelegt ist)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Verifikation**: Nach der Änderung muss das alte Passwort 401 und das neue 200 liefern.
  **【Verifiziert】** Messausgabe auf diesem Rechner:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Standardkonto geändert/gelöscht

### A-2. Minimale Rechte: `configure` / `write` / `read`-Regexes pro vhost 【Verifiziert】

- **Warum**: Die Dreiteilung ist an RabbitMQ angeglichen – `configure` regelt Topologie-Deklaration/-Löschung, `write` regelt Publish und Binding, `read` regelt Konsum und Fetch;
  unzulässige Zugriffe liefern 403. Gib Anwendungskonten nur das, was sie brauchen.
- **Vorgehen**: `PUT /api/permissions/{vhost}/{user}`, z. B. nur konsumierend lesen: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Verifikation**: Versuche mit einem eingeschränkten Konto eine unzulässige Operation, sie sollte 403 `ACCESS_REFUSED` liefern.
  **【Verifiziert】** Auf diesem Rechner wurde mit einem echten AMQP-Client als Benutzer mit `configure="^$"` ein Exchange deklariert, Messung:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Jedem Anwendungskonto nur die notwendigen Regexes gewährt, ohne Vergabe des `administrator`/`management`-Tags

### A-3. Die Handhabung des `administrator`-Tags (implizite Vollmacht) – mit Vorsicht vergeben 【Verifiziert】

- **Warum**: **Benutzer mit dem Tag `administrator` besitzen auf allen für sie sichtbaren vhosts vollständige Rechte, ohne dass Berechtigungsdatensätze nötig sind**
  (angeglichen an die gemessene Handhabung von RabbitMQ, siehe README / Design M8-7). Anders gesagt: Sobald dieser Tag vergeben ist,
  wirken die Berechtigungs-Regex nicht mehr – er ist die höchste Berechtigung.
- **Vorgehen**: Nur Management-/Betriebskonten erhalten `administrator`; Anwendungskonten bekommen grundsätzlich keinen Tag, sondern nur Berechtigungs-Regexes.

- **Verifikation (Demonstration der impliziten Berechtigung)**: Für einen neuen vhost **ohne jegliche Berechtigungsdatensätze** sollte der `administrator`-Benutzer direkt nutzbar sein.
  **【Verifiziert】** Messung auf diesem Rechner: Nach dem Anlegen des neuen vhost `drillvh` (ohne Berechtigungsdatensätze) hat `guest` (administrator) darauf erfolgreich eine Topologie deklariert:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] Der `administrator`-Tag ist nur an sehr wenige Betriebskonten vergeben

### A-4. `remote_access`: Konten auf lokale Anmeldung beschränken 【Nicht verifiziert (auf demselben Rechner lässt sich keine Remote-Quelle simulieren)】

- **Warum**: An RabbitMQ angeglichen; das integrierte `guest` erlaubt standardmäßig nur die lokale Anmeldung; bei externer Bereitstellung sollte die Quelle privilegierter Konten eingeschränkt werden.
- **Vorgehen / Handhabung (wichtige Einschränkung)**:
  - `remote_access` kann nur in der **Konfigurationsdatei** unter `users.<name>.remote_access` gesetzt werden und **wird nur beim ersten Bootstrap wirksam**;
  - **Über die Management-API / `speedmqctl` erstellte Konten haben grundsätzlich `remote_access=true`** (Anmeldung von beliebiger Quelle erlaubt) –
    Beleg: der `UpsertUser`-Kommentar in `internal/broker/observe.go` und die Messung in `meta/state.json` von
    `"remote_access":true`. Mit anderen Worten: **Die API kann derzeit ein Konto nicht auf „nur lokal“ beschränken**.
- **Verifikation**: Von **einem anderen Host** (nicht `127.0.0.1`) aus mit diesem Konto verbinden; es sollte 403 liefern; die lokale Verbindung sollte erfolgreich sein.
  **【Nicht verifiziert】**: In der Umgebung dieses Rechners lässt sich keine echte Remote-Quelle erzeugen, daher nicht gemessen.
- [ ] Die Quellenbeschränkung privilegierter Konten wurde gemäß obiger Handhabung bewertet (beachte: über die API erstellte Konten haben standardmäßig Remote-Zugriff)

---

## B. Transportsicherheit (TLS)

TLS-Konfigurationsschlüssel (die Zugriffsebene und die Management-Ebene **teilen** denselben Feldsatz): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. TLS aktivieren; bei Fehlkonfiguration wird der Start abgelehnt 【Verifiziert】

- **Warum**: Das Zertifikat wird **beim Start** gelesen und geprüft – bei Fehlkonfiguration wird der Start sofort abgelehnt, statt dies erst beim Verbinden des ersten Clients zu offenbaren.
- **Vorgehen**: In `listeners.<plugin>[].tls` oder `management.tls` `cert_file` + `key_file` angeben (nur wenn **beide** angegeben sind, wird TLS aktiviert).

- **Verifikation**: Mit einer fehlerhaften Konfiguration starten, es sollte sofort fehlschlagen.
  **【Verifiziert】** Messung mit drei fehlerhaften Konfigurationen auf diesem Rechner, alle `exit=1`, Start abgelehnt:

  ```
  badtls1: speedmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: speedmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: speedmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Verifikation (positiv/negativ)**: Ein TLS-Client kann sich verbinden, ein Klartext-Client wird beim Verbinden mit dem TLS-Port abgelehnt.
  **【Verifiziert】** Auf diesem Rechner wurde eine TLS-Instanz gestartet (`amqp091` über TLS) und mit einer echten Client-Probe getestet:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] Für nach außen offene Protokollports ist TLS aktiviert

### B-2. `min_version` mindestens 1.2 【Verifiziert】

- **Warum**: Zu alte TLS-Versionen werden deaktiviert; Standard ist `1.2`, wählbar sind `1.2` / `1.3`.
- **Verifikation**: Setze `min_version` auf `1.0`, der Start sollte einen Fehler melden (siehe `badtls2`-Ausgabe in B-1).
- [ ] `min_version` ist `1.2` oder `1.3`

### B-3. Gegenseitige Authentifizierung `client_auth: require_and_verify` (mTLS) 【Teilweise verifiziert】

- **Warum**: Der Client muss ein Zertifikat vorlegen und es wird geprüft, um unbefugten Client-Zugang zu den Protokollports zu verhindern.
- **Vorgehen**: `ca_file` + `client_auth: require_and_verify` konfigurieren (Letztere erfordern die gleichzeitige Angabe von `ca_file`).
- **【Verifiziert】**: TLS-End-to-End und der abgelehnte Pfad wurden mit echten Clients verifiziert (B-1). **mTLS (Anforderung und Prüfung von Client-Zertifikaten) wurde auf diesem Rechner nicht separat getestet**.
- [ ] Für Ports, die mTLS benötigen, sind `require_and_verify` + `ca_file` konfiguriert

### B-4. TLS der Management-Ebene 【Nicht verifiziert】

- **Warum**: Die Management-Ebene überträgt das Passwort per Basic Auth und muss daher verschlüsselt werden.
- **Vorgehen**: `management.tls` verwendet dieselben Felder wie die Protokoll-Listener.
- **【Nicht verifiziert】**: Im Test auf diesem Rechner war die Management-Ebene an einen lokalen Klartextport gebunden; es wurde kein separates HTTPS für die Management-Ebene gestartet.
- [ ] Für die Management-Ebene ist TLS aktiviert (oder sie ist strikt auf ein vertrauenswürdiges Netzwerk beschränkt)

---

## C. Reduzierung der Angriffsfläche

### C-1. Einschränkung des Lauschbereichs der Management-Ebene 【Verifiziert (Lauschadresse gemessen)】

- **Warum**: Die Management-Ebene lauscht standardmäßig auf `:15672` (alle Netzwerkkarten). Bei externer Bereitstellung sollte sie an eine interne/Loopback-Adresse gebunden oder die Quelle per Firewall eingeschränkt werden.
- **Vorgehen**: `management.addr` auf `127.0.0.1:15672` oder eine interne Adresse setzen; oder `management.enabled=false`, um sie vollständig zu deaktivieren
  (danach gibt es keinen Management-Port, aber `speedmqctl` ist dann ebenfalls nicht verfügbar).
- **Verifikation**:
  **【Verifiziert】** Auf diesem Rechner wurde die Management-Ebene auf `127.0.0.1:15677` konfiguriert; die gemessene Lauschadresse ist tatsächlich Loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] Die Bindungsadresse der Management-Ebene ist eingeschränkt (oder sie ist deaktiviert)

### C-2. Nur die notwendigen Protokollports öffnen 【Nicht verifiziert】

- **Warum**: Standardmäßig sind AMQP `5672` und MQTT `1883` gleichzeitig offen; wird MQTT nicht genutzt, abschalten, um die Angriffsfläche zu verkleinern.
- **Vorgehen**: `plugins.mqtt.enabled=false` (oder aus `listeners` entfernen); die Deaktivierung **schließt den echten Port**, nicht nur ein Statusbit.
- **Verifikation**: Nach der Deaktivierung lauscht der entsprechende Port nicht mehr (in `Get-NetTCPConnection -State Listen` nicht zu sehen).
  **【Nicht verifiziert】**: Im Test auf diesem Rechner waren beide Protokolle geöffnet; das Verschwinden des Ports nach dem Schließen wurde nicht separat verifiziert.
- [ ] Nicht verwendete Protokoll-Plugins sind deaktiviert

---

## D. Hardening des Containerbetriebs

Fakten zum Image dieses Repositories (`Dockerfile`): statisch gelinktes Binary + alpine, **läuft als Nicht-root (uid 10001, Benutzer `speedmq`)**,
das Datenverzeichnis `/var/lib/speedmq` ist ein Volume. `docker-compose.yml` verwendet **benannte Volumes** zur Persistenz, **schreibgeschütztes Mounten** der Konfiguration und Log-Rotation.

### D-1. Betrieb als Nicht-root 【Nicht verifiziert (Docker wurde auf diesem Rechner nicht ausgeführt)】

- **Warum**: Minimale Rechte, um das Ausmaß nach einem Container-Escape zu verringern.
- **Vorgehen**: Das Image verwendet standardmäßig bereits uid 10001; **nicht** mit `--user root` überschreiben.
- **Verifikation**: `docker compose run -T --rm broker id` sollte `uid=10001` anzeigen. (Bei `run` muss in nicht-interaktiver Umgebung `-T` angegeben werden)
- [ ] Der Container läuft als Nicht-root (nicht mit root überschrieben)

### D-2. Schreibgeschütztes Root-Dateisystem + Ressourcenlimits + Capability-Trimming (empfohlen, im compose dieses Repositories nicht standardmäßig aktiviert) 【Nicht verifiziert】

- **Warum**: Ein schreibgeschütztes Root-Dateisystem verhindert eine Manipulation des Binarys zur Laufzeit; Ressourcenlimits verhindern, dass ein einzelner Container den Host lahmlegt; Capability-Trimming verkleinert die Angriffsfläche des Kernels.
- **Vorgehen** (Beispiel, nach Bedarf in den `broker`-Service der compose-Datei übernehmen):

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

- **Verifikation**: Der Versuch, im Container in den Root-Pfad zu schreiben, sollte fehlschlagen (schreibgeschützt); mit `docker inspect` sind die Ressourcenlimits sichtbar.
  **【Nicht verifiziert】** (Docker wurde auf diesem Rechner nicht ausgeführt); zudem **muss bei einem schreibgeschützten Root-Dateisystem bestätigt werden, dass `data_dir` auf einem beschreibbaren Volume liegt**, sonst kann der Kernel nicht auf die Platte schreiben.
- [ ] Schreibgeschütztes Root-Dateisystem und Ressourcenlimits wurden bewertet (beachte: `data_dir` muss auf einem beschreibbaren Volume liegen)

---

## E. Bekannte Einschränkungen (derzeit tatsächlich nicht möglich, nicht darauf verlassen)

Die folgenden Punkte sind alle **faktische Lücken**; sie sollten im Sicherheitsdesign ausdrücklich anerkannt und nicht vorausgesetzt werden:

1. **Passwörter werden im Klartext gespeichert und kopiert**. Das `password`-Feld in `meta/state.json` ist Klartext (**【Verifiziert】** in der Messung sichtbar als
   `"password":"drillpass"`); in der Konfigurationsdatei ebenfalls Klartext. Es gibt **keinen** Passwort-Hash (Hashing und externe Authentifizierungs-Backends bleiben Auth-Plugins überlassen).
   → Folge: **Datenverzeichnis und Sicherungsdateien sind gleichwertig mit sensiblen Zugangsdaten** und müssen durch Dateiberechtigungen und Verschlüsselung geschützt werden.
2. **Es gibt kein Audit-Log**. Hinzufügen/Ändern/Löschen in der Management-Ebene wird in regulären Logs protokolliert (z. B. `管理面更新用户 actor=... user=...`),
   aber es gibt **keinen** eigenständigen, manipulationssicheren Audit-Stream und keine compliance-taugliche Aufzeichnung von „wer hat wann was geändert“.
3. **Es gibt keine externe Authentifizierung wie LDAP / OAuth2 / JWT**. In v1 sind nur `PLAIN` / `AMQPLAIN` integriert
   (`auth.Store.Mechanisms()` gibt in der Messung nur diese beiden zurück).
4. **SASL `EXTERNAL` ist nicht implementiert**: Auch bei konfiguriertem mTLS verwendet die Protokollebene **weiterhin die PLAIN-Passwortauthentifizierung**
   (der Schritt „passwortfrei per Client-Zertifikat“ fehlt). Zertifikate dienen nur der Prüfung auf Transportebene.
5. **`remote_access` kann nicht über die API gesetzt werden**: Über Management-API/CLI erstellte Konten erlauben grundsätzlich Remote-Anmeldung (siehe A-4);
   ein einzelnes Konto kann nicht auf „nur lokal“ beschränkt werden.
6. **Die Management-Ebene hat keine eigenständige Quell-Whitelist / kein Rate-Limiting**: Die Angriffsfläche lässt sich nur über Bindungsadresse, Firewall und TLS einschränken.
7. **Keine Plugin-Sandbox**: Form-A-Plugins laufen im selben Prozess wie der Kernel; Form-B-externe Plugins haben zwar Prozessisolation, doch die **Datenebene läuft über einen lokalen Verbindungs-Proxy**,
   und Plugins können Kernel-Semantik aufrufen (unterliegt der vhost- und Berechtigungsprüfung); es ist **keine** Sicherheits-Sandbox.
8. **Die Tags der Management-Ebene umfassen nur die drei Stufen `administrator`/`management`/`monitoring`**, es gibt kein feineres per-resource RBAC.

---

## F. Zusammenfassende Checkliste

- [ ] A-1 Standardkonto geändert/gelöscht 【Verifizierter Ablauf】
- [ ] A-2 Minimale Rechte (Regex) für Anwendungskonten, ohne Administrator-Tag 【Verifizierter 403-Pfad】
- [ ] A-3 Der `administrator`-Tag ist nur an Betriebskonten vergeben 【Verifizierte Handhabung der impliziten Berechtigung】
- [ ] A-4 Quellenbeschränkung privilegierter Konten bewertet (beachte: über die API erstellte Konten haben standardmäßig Remote-Zugriff)
- [ ] B-1 TLS für nach außen offene Ports aktiviert; Fehlkonfiguration lehnt den Start ab 【Verifiziert】
- [ ] B-2 `min_version` ≥ 1.2 【Verifiziert】
- [ ] B-3 Für Ports, die mTLS benötigen, `require_and_verify` + `ca_file` konfiguriert
- [ ] B-4 TLS für die Management-Ebene aktiviert
- [ ] C-1 Bindungsadresse der Management-Ebene eingeschränkt 【Lauschadresse verifiziert】
- [ ] C-2 Nicht verwendete Protokoll-Plugins deaktiviert
- [ ] D-1 Container läuft als Nicht-root
- [ ] D-2 Schreibgeschütztes Root-Dateisystem / Ressourcenlimits / Capability-Trimming bewertet
- [ ] E Bekannte Einschränkungen (Klartext-Passwörter, kein Audit, kein LDAP/OAuth2, SASL EXTERNAL nicht implementiert) wurden im Sicherheitsdesign anerkannt
