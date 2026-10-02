<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | **Nederlands** | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

Een **RabbitMQ-compatibele** message broker geschreven in Go. Bestaande RabbitMQ-clients hebben **geen codewijziging en geen andere SDK** nodig; alleen het aanpassen van het verbindingsadres is voldoende.

## Inleiding

- **Protocolcompatibel**: AMQP 0-9-1 (inclusief RabbitMQ-extensies) en MQTT 3.1.1; de compatibiliteitsbasis is **de semantiek van RabbitMQ 4.3**.
- **Eenvoudige uitrol**: één binary / één container, de beheer-UI is al ingebouwd; er is geen extra Nginx, database of Node-runtime nodig.
- **Voldoende voor beheer**: beheer-UI (queues / exchanges / verbindingen / accountrechten / virtual hosts / policies / limieten / cluster), Prometheus `/metrics`, opdrachtregel `swiftmqctl`.
- **Standaardpoorten**: `5672` (AMQP), `1883` (MQTT), `15672` (beheer-UI / HTTP API / metrics).

Reeds aanwezige mogelijkheden: persistentie (segmentlog + fsync-niveaus + crashherstel), publicatiebevestiging, TTL / dead letter / lengtelimiet, consumentprioriteit, Direct Reply-To, cluster (Raft-metadata + quorumqueues + doorsturen tussen nodes), hot start/stop van plugins.

***

## Snel aan de slag

### Methode 1: Docker (aanbevolen)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose up -d --build

docker compose ps        # de status moet Up (healthy) zijn
docker compose logs -f   # logs volgen
```

Equivalente pure docker:

```bash
docker build -t swiftmq:1.0.0 .
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  swiftmq:1.0.0
```

- Gegevens komen terecht in het benoemde volume `swiftmq-data` en gaan niet verloren bij het opnieuw opbouwen van de container; de configuratie wordt alleen-lezen gemount vanuit `configs/swiftmqd.json`; na een wijziging wordt deze actief met `docker compose restart`.
- Stoppen: `docker compose down` (gegevens blijven behouden); `docker compose down -v` (gegevens worden ook verwijderd).

### Methode 2: Lokaal binary (vereist Go 1.24+)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> De build-artefacten van de beheer-UI worden niet in de repository opgenomen. Wil je de UI gebruiken, voer dan eerst in `web/` `npm ci && npm run build` uit;
> zonder build kun je nog steeds normaal starten en berichten verzenden en ontvangen; alleen zal bij het bezoeken van `/` de melding "beheer-UI niet gebouwd" verschijnen.

### Eerste aanmelding (wijzig beslist eerst het standaardaccount)

| Ingang | Adres / inloggegevens |
| --- | --- |
| Beheer-UI | <http://localhost:15672/> (gebruikersnaam `guest`, wachtwoord `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (account zoals hierboven) |

Het hoofdaccount van een nieuwe installatie heeft de markering "verplichte wachtwoordwijziging bij eerste aanmelding": na aanmelding via de beheer-UI word je **verplicht zowel de accountnaam als het wachtwoord te wijzigen**; pas daarna krijg je toegang tot de beheeromgeving.

Je kunt dit ook direct via de API doen (geschikt voor automatisering):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ De standaard `guest/guest` gedraagt zich hetzelfde als bij RabbitMQ: **alleen aanmelding vanaf de lokale machine is toegestaan**. Voor verbindingen van buiten de container / op afstand moet je in de configuratie `remote_access` voor deze gebruiker inschakelen (de voorbeeldconfiguratie heeft dit al ingeschakeld voor de containerscenario's).
> **Zodra de service van buitenaf bereikbaar is, vervang dan onmiddellijk de inloggegevens.**

### Je applicatie aansluiten (alleen het verbindingsadres aanpassen)

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
# MQTT (mosquitto-client)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

De beheer-HTTP-API is compatibel met `rabbitmqadmin`; "Nieuwe queue / exchange toevoegen" in de beheer-UI is precies het standaard declaratie-eindpunt, wat ook met scripts kan:

```bash
# Een queue declareren (voor een quorumqueue gebruik je arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Dagelijks beheer

| Onderwerp | Ingang |
| --- | --- |
| Beheer-UI | <http://localhost:15672/>: queues / exchanges / verbindingen / accountrechten / virtual hosts / policies / limieten / feature flags / cluster; rechtsboven kun je automatisch verversen en de **interfacetaal** instellen |
| Metrics | <http://localhost:15672/metrics> (Prometheus-tekst, authenticatie vereist); zie [docs/ops/monitoring](ops/monitoring/README.md) voor dashboards en alerts |
| Opdrachtregel | `./bin/swiftmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (hot uitschakelen, de poort wordt onmiddellijk gesloten) |
| Healthcheck | `nc -z 127.0.0.1 15672` (compose heeft al een ingebouwde healthcheck) |
| Back-up en herstel | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Upgraden | [docs/ops/upgrade.md](ops/upgrade.md) |
| Beveiligingsbaseline | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Veelgebruikte configuratie (zie [configs/swiftmqd.json](../../../configs/swiftmqd.json) voor een volledig voorbeeld; kan ook worden overschreven met `SWIFTMQ_*`-omgevingsvariabelen):

| Configuratieoptie | Beschrijving | Standaard |
| --- | --- | --- |
| `data_dir` | Gegevensmap (berichten + metadata), **moet beslist persistent zijn** | `data` |
| `listeners` | Luisteradressen per protocol, TLS mogelijk | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Luisteradres van beheer-UI / API | `:15672` |
| `management.language` | Standaardtaal van de beheer-UI; leeg laten betekent automatisch kiezen op basis van de tijdzone van de uitrol | Automatisch |
| `storage.fsync` | Schrijfniveau `none / os / batch / always` (bepaalt ook het moment van confirm) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Resourcewatermerken: bij activering worden producers geblokkeerd, **berichten gaan niet verloren** | `0.4` / 50 MiB |
| `users` | Ingebouwde gebruikerstabel (wachtwoord + tags + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Cluster met meerdere nodes (standaard uitgeschakeld), ledenwijzigingen via `swiftmqctl add_member` | Uitgeschakeld |

> Poorten kunnen bezet zijn: vervang ze eenvoudig door andere poorten via `listeners` / `management.addr`.

***

## Projectstructuur

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # brokerproces-ingang (dit is wat je moet draaien)
│   └── swiftmqctl/      # beheer-CLI (via beheer-HTTP-API, losgekoppeld van de kernelversie)
├── internal/            # kernelimplementatie
│   ├── protocol/        # protocolplugins: amqp091, mqtt (codering/decodering / methoden / sessies)
│   ├── broker/          # kernel: vhost, exchanges, queues, dead letter, flow control, beheerweergave
│   ├── store/           # persistentie: segmentlog, queue-index, crashherstel
│   ├── raft/ meta/      # cluster: eigen Raft en metadata-replicatie
│   ├── management/      # beheer-HTTP-API + Prometheus-metrics + statische service voor de ingebouwde UI
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # extern stabiel contract: plugin-API (plugin) en wire-protocol voor externe procesplugins (sidecar)
├── web/                 # frontendproject van de beheer-UI (Vue 3 + Vite), artefacten worden bij de build via go:embed in de binary opgenomen
├── configs/             # voorbeeldconfiguraties
├── docs/ops/            # beheerdocumentatie: back-up en herstel / upgrade / beveiligingsbaseline / monitoring
├── Dockerfile, docker-compose.yml
└── swiftmq-logo.PNG, 1280X1280.PNG (QR-code van de communitygroep)
```

***

## Bijdragen

Issues en Pull Requests zijn welkom. De bestaansgrond van dit project is **protocolcompatibiliteit**; daarom:

- Vermeld bij het oplossen van een bug het bijbehorende RabbitMQ-gedrag (versie, client, reproductiestappen);
- Voeg bij wijzigingen die protocoldetails raken het vergelijkingsresultaat met RabbitMQ toe;
- Zorg ervoor dat `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .` allemaal slagen voordat je indient.

***

## Licentie

Dit project valt onder de [Apache License 2.0](../../../LICENSE).

Gebruik, wijziging en distributie (inclusief commercieel gebruik) zijn toegestaan, mits de auteursrechts- en licentiemelding behouden blijven, en er wordt geen enkele garantie gegeven.

Copyright 2026 houzch (zie [NOTICE](../../../NOTICE))

***

## Dankbetuiging

De AMQP 0-9-1-protocolspecificatie en de gedragssemantiek van [RabbitMQ](https://www.rabbitmq.com/) vormen de referentie voor het compatibiliteitswerk van dit project. Dit project is een onafhankelijke implementatie, heeft geen banden met de officiële RabbitMQ en gebruikt de code ervan niet.

***

## Lid worden van de communitygroep

Scan de QR-code om lid te worden van de SwiftMQ-communitygroep; bij vragen kun je het direct in de groep stellen:

![SwiftMQ-communitygroep](../../../1280X1280.PNG)
