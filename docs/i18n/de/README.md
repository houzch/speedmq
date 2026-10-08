<!-- i18n-switcher -->
[简体中文](../../../README-cn.md) | [繁體中文](../zh-TW/README.md) | [English](../../../README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | **Deutsch** | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SpeedMQ

Ein in Go geschriebener **RabbitMQ-kompatibler** Message-Broker. Bestehende RabbitMQ-Clients können **ohne Code-Änderung und ohne SDK-Wechsel** angebunden werden – es genügt, die Verbindungsadresse zu ändern.

## Einführung

- **Protokollkompatibilität**: AMQP 0-9-1 (inkl. RabbitMQ-Erweiterungen) und MQTT 3.1.1; Kompatibilitätsbasis ist die **RabbitMQ-4.3-Semantik**.
- **Einfache Bereitstellung**: ein einziges Binary / ein einziger Container, die Management-UI ist bereits eingebettet; kein zusätzliches Nginx, keine Datenbank und keine Node-Laufzeit erforderlich.
- **Ausreichend für den Betrieb**: Management-UI (Queues / Exchanges / Verbindungen / Kontoberechtigungen / virtuelle Hosts / Policies / Limits / Cluster), Prometheus `/metrics`, Kommandozeile `speedmqctl`.
- **Standardports**: `5672` (AMQP), `1883` (MQTT), `15672` (Management-UI / HTTP-API / Metriken).

Bereits vorhandene Fähigkeiten: Persistenz (Segment-Log + fsync-Stufen + Crash-Recovery), Publish-Bestätigungen, TTL / Dead-Letter / Längenbegrenzung, Consumer-Prioritäten, Direct Reply-To, Cluster (Raft-Metadaten + Quorum-Queues + knotenübergreifende Weiterleitung), Hot-Start/Stop von Plugins.

***

## Schnellstart

### Variante 1: Docker (empfohlen)

**Ohne Klonen des Repos: einfach das Image ziehen und starten:**

```bash
docker run -d --name speedmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v speedmq-data:/var/lib/speedmq \
  houzch/speedmq:1.1.05
```

Das Image wird an zwei Stellen mit identischem Inhalt veröffentlicht (nimm die schnellere): Docker Hub `houzch/speedmq` und GitHub GHCR `ghcr.io/houzch/speedmq`; beide bieten `linux/amd64` und `linux/arm64`.

- Die Daten liegen im benannten Volume `speedmq-data` und überleben ein Neuerstellen des Containers.
- Stoppen / Entfernen: `docker stop speedmq`, `docker rm speedmq` (das Daten-Volume bleibt erhalten).

**Zum Ändern der Konfiguration oder für den Betrieb mit compose das Repo klonen:**

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
docker compose pull && docker compose up -d   # Nutzt das veröffentlichte Image; mit up -d --build wird lokal gebaut

docker compose ps        # Status sollte Up (healthy) sein
docker compose logs -f   # Logs mitverfolgen
```

- Die Konfiguration wird schreibgeschützt aus `configs/speedmqd.json` gemountet; Änderungen wirken nach `docker compose restart`.
- Stoppen: `docker compose down` (Daten bleiben); `docker compose down -v` (löscht auch die Daten).

### Variante 2: Lokales Binary (erfordert Go 1.24+)

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
go build -o bin/speedmqd ./cmd/speedmqd
go build -o bin/speedmqctl ./cmd/speedmqctl
./bin/speedmqd -config configs/speedmqd.json
```

> Die Build-Artefakte der Management-UI werden nicht eingecheckt. Wenn du die UI verwenden willst, führe zuerst in `web/` `npm ci && npm run build` aus;
> ohne Build kannst du ebenfalls starten und Nachrichten senden/empfangen, beim Aufruf von `/` erscheint dann jedoch der Hinweis „Management-UI nicht gebaut“.

### Erste Anmeldung (unbedingt zuerst das Standardkonto ändern)

| Einstieg | Adresse / Zugangsdaten |
| --- | --- |
| Management-UI | <http://localhost:15672/> (Benutzername `guest`, Passwort `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (Konto wie oben) |

Das Hauptkonto einer neu installierten Instanz trägt die Markierung „Passwortänderung bei erster Anmeldung erzwingen“: Nach der Anmeldung in der Management-UI wird **erzwungen, dass Kontoname und Passwort gleichzeitig geändert werden**; erst danach ist der Zugang zum Backend möglich.

Alternativ lässt sich das direkt über die API erledigen (geeignet für Automatisierung):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<neues Passwort>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ Das Standardkonto `guest/guest` verhält sich wie bei RabbitMQ: **Anmeldung ist nur vom lokalen Rechner erlaubt**. Für Verbindungen von außerhalb des Containers / aus der Ferne muss in der Konfiguration für diesen Benutzer `remote_access` aktiviert werden (die Beispielkonfiguration hat dies für Container-Szenarien bereits aktiviert).
> **Sobald der Dienst von außen erreichbar ist, wechsle unverzüglich die Zugangsdaten.**

### Anbindung deiner Anwendung (nur Verbindungsadresse ändern)

```python
# Python (pika)
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go（amqp091-go）
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (mosquitto-Client)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

Die Management-HTTP-API ist mit `rabbitmqadmin` kompatibel; die Einträge „Neue Queue / neuer Exchange“ in der Management-UI entsprechen den Standard-Deklarationsendpunkten, die auch per Skript genutzt werden können:

```bash
# Queue deklarieren (Quorum-Queue wird über arguments: {"x-queue-type":"quorum"} ausgedrückt)
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Täglicher Betrieb

| Vorgang | Einstieg |
| --- | --- |
| Management-UI | <http://localhost:15672/>: Queues / Exchanges / Verbindungen / Kontoberechtigungen / virtuelle Hosts / Policies / Limits / Feature-Flags / Cluster; oben rechts lassen sich automatisches Aktualisieren und die **Oberflächensprache** einstellen |
| Monitoring-Metriken | <http://localhost:15672/metrics> (Prometheus-Textformat, Authentifizierung erforderlich); Dashboards und Alarme siehe [docs/ops/monitoring](ops/monitoring/README.md) |
| Kommandozeile | `./bin/speedmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (Hot-Deaktivierung, der Port wird sofort geschlossen) |
| Health-Check | `nc -z 127.0.0.1 15672` (in compose ist bereits ein healthcheck integriert) |
| Sicherung und Wiederherstellung | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Upgrade | [docs/ops/upgrade.md](ops/upgrade.md) |
| Sicherheitsbaseline | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Häufig verwendete Konfiguration (vollständiges Beispiel siehe [configs/speedmqd.json](../../../configs/speedmqd.json), kann auch über `SPEEDMQ_*`-Umgebungsvariablen überschrieben werden):

| Konfigurationsschlüssel | Beschreibung | Standard |
| --- | --- | --- |
| `data_dir` | Datenverzeichnis (Nachrichten + Metadaten), **unbedingt persistieren** | `data` |
| `listeners` | Lauschadressen der einzelnen Protokolle, TLS konfigurierbar | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Lauschadresse der Management-UI / -API | `:15672` |
| `management.language` | Standardsprache der Management-UI; leer lassen = automatische Auswahl anhand der Zeitzone des Standorts | automatisch |
| `storage.fsync` | Persistenzstufe `none / os / batch / always` (bestimmt zugleich den Zeitpunkt der Bestätigung) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Ressourcenschwellen: Bei Auslösung werden Producer blockiert, **keine Nachricht geht verloren** | `0.4` / 50 MiB |
| `users` | Integrierte Benutzertabelle (Passwort + Tags + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Mehrknoten-Cluster (standardmäßig deaktiviert); Mitgliederänderungen mit `speedmqctl add_member` | deaktiviert |

> Ein Port ist möglicherweise belegt: Ändere einfach `listeners` / `management.addr` auf andere Ports.

***

## Projektstruktur

```
speedmq/
├── cmd/
│   ├── speedmqd/        # Einstiegspunkt des Broker-Prozesses (das ist das Programm, das läuft)
│   └── speedmqctl/      # Betriebs-CLI (nutzt die Management-HTTP-API, entkoppelt von der Kernel-Version)
├── internal/            # Kernel-Implementierung
│   ├── protocol/        # Protokoll-Plugins: amqp091, mqtt (Codec / Methoden / Sessions)
│   ├── broker/          # Kernel: vhost, Exchanges, Queues, Dead-Letter, Flusskontrolle, Management-Ansichten
│   ├── store/           # Persistenz: Segment-Log, Queue-Index, Crash-Recovery
│   ├── raft/ meta/      # Cluster: eigenentwickeltes Raft und Metadaten-Replikation
│   ├── management/      # Management-HTTP-API + Prometheus-Metriken + eingebetteter UI-Statikdienst
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # Stabiler externer Vertrag: Plugin-API (plugin) und Wire-Protokoll für externe Prozess-Plugins (sidecar)
├── web/                 # Frontend-Projekt der Management-UI (Vue 3 + Vite); Artefakte werden beim Build via go:embed ins Binary eingebettet
├── configs/             # Beispielkonfiguration
├── docs/ops/            # Betriebsdokumentation: Sicherung/Wiederherstellung / Upgrade / Sicherheitsbaseline / Monitoring
├── Dockerfile、docker-compose.yml
└── speedmq-logo.PNG、1280X1280.PNG（交流群二维码）
```

***

## Mitwirken

Issues und Pull Requests sind willkommen. Das Fundament dieses Projekts ist die **Protokollkompatibilität**, daher:

- Beschreibe bei Bugfixes das entsprechende RabbitMQ-Verhalten (Version, Client, Reproduktionsschritte);
- Füge bei Änderungen mit Protokolldetails die Vergleichsergebnisse mit RabbitMQ bei;
- Stelle vor dem Commit sicher, dass `go build ./...`, `go vet ./...`, `go test ./...` und `gofmt -l .` alle erfolgreich durchlaufen.

***

## Lizenz

Dieses Projekt steht unter der [Apache License 2.0](../../../LICENSE).

Die Nutzung, Änderung und Weitergabe (auch kommerziell) ist erlaubt; Copyright- und Lizenzhinweise müssen beibehalten werden, und es wird keinerlei Gewährleistung übernommen.

Copyright 2026 houzch (siehe [NOTICE](../../../NOTICE))

***

## Danksagung

Die AMQP-0-9-1-Protokollspezifikation und die Verhaltenssemantik von [RabbitMQ](https://www.rabbitmq.com/) sind die Referenzbasis für die Kompatibilitätsarbeit dieses Projekts. Dieses Projekt ist eine unabhängige Implementierung, steht in keiner Verbindung zu den offiziellen RabbitMQ-Verantwortlichen und verwendet keinen deren Code.

***

## Der Chatgruppe beitreten

Scanne den QR-Code, um der SpeedMQ-Chatgruppe beizutreten; Fragen kannst du direkt in der Gruppe stellen:

![SpeedMQ-Chatgruppe](../../../1280X1280.PNG)
