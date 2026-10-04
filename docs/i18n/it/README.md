<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | **Italiano** | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

Middleware di messaggistica **compatibile con RabbitMQ** scritto in Go. I client RabbitMQ esistenti si collegano **senza modificare il codice né cambiare SDK**: basta cambiare l'indirizzo di connessione.

## Introduzione

- **Compatibilità di protocollo**: AMQP 0-9-1 (incluse le estensioni di RabbitMQ) e MQTT 3.1.1; la base di compatibilità è la **semantica di RabbitMQ 4.3**.
- **Distribuzione semplice**: un unico binario / un unico container, con l'interfaccia di gestione già integrata; non servono Nginx, database o runtime Node aggiuntivi.
- **Operatività adeguata**: interfaccia di gestione (code / exchange / connessioni / account e permessi / virtual host / policy / limiti / cluster), Prometheus `/metrics`, riga di comando `swiftmqctl`.
- **Porte predefinite**: `5672` (AMQP), `1883` (MQTT), `15672` (UI di gestione / HTTP API / metriche).

Funzionalità già disponibili: persistenza (log a segmenti + profili fsync + ripristino dopo crash), conferme di pubblicazione, TTL / dead letter / limiti di lunghezza, priorità dei consumatori, Direct Reply-To, cluster (metadati Raft + code quorum + inoltro tra nodi), avvio e arresto a caldo dei plugin.

***

## Avvio rapido

### Metodo 1: Docker (consigliato)

**Senza clonare il repository: scarica l immagine e avviala.**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.1.02
```

L immagine è pubblicata in due posti con lo stesso contenuto (usa quello più veloce per te): Docker Hub `houzch/swiftmq` e GitHub GHCR `ghcr.io/houzch/swiftmq`; entrambi offrono `linux/amd64` e `linux/arm64`.

- I dati finiscono nel volume denominato `swiftmq-data`, che sopravvive alla ricreazione del container.
- Stop / rimozione: `docker stop swiftmq`, `docker rm swiftmq` (il volume dei dati resta).

**Per modificare la configurazione o usare compose, clona il repository:**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # Usa l immagine pubblicata; sostituisci con up -d --build per compilare in locale

docker compose ps        # Lo stato deve essere Up (healthy)
docker compose logs -f   # Segui i log
```

- La configurazione è montata in sola lettura da `configs/swiftmqd.json`; le modifiche hanno effetto dopo `docker compose restart`.
- Stop: `docker compose down` (conserva i dati); `docker compose down -v` (elimina anche i dati).

### Metodo 2: binario locale (richiede Go 1.24+)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> Gli artefatti di build dell'interfaccia di gestione non sono versionati. Se vuoi usare l'UI, esegui prima `npm ci && npm run build` in `web/`;
> anche senza compilare l'avvio e lo scambio di messaggi funzionano, solo che accedendo a `/` comparirà il messaggio «interfaccia di gestione non compilata».

### Primo accesso (cambia subito l'account predefinito)

| Accesso | Indirizzo / credenziali |
| --- | --- |
| UI di gestione | <http://localhost:15672/> (utente `guest`, password `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (stesse credenziali) |

All'account principale di un'installazione nuova è associato il flag «cambio password obbligatorio al primo accesso»: dopo il login nell'interfaccia di gestione viene **richiesto obbligatoriamente di modificare insieme nome utente e password**, e solo dopo si può entrare nel backoffice.

In alternativa puoi farlo direttamente tramite l'API (adatto all'automazione):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ Il valore predefinito `guest/guest` si comporta come RabbitMQ: **consente il login solo dalla macchina locale**. Per collegarsi dall'esterno del container / da remoto occorre abilitare `remote_access` per quell'utente nella configurazione (la configurazione di esempio lo abilita già per lo scenario container).
> **Non appena il servizio è raggiungibile dall'esterno, cambia immediatamente le credenziali.**

### Integrare la tua applicazione (basta cambiare l'indirizzo di connessione)

```python
# Python（pika）
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go（amqp091-go）
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (client mosquitto)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

L'HTTP API di gestione è compatibile con `rabbitmqadmin`; «nuova coda / nuovo exchange» nell'interfaccia di gestione non è altro che l'endpoint di dichiarazione standard, quindi anche gli script possono farlo:

```bash
# Dichiara la coda (per le code quorum usa arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Operazioni quotidiane

| Voce | Accesso |
| --- | --- |
| UI di gestione | <http://localhost:15672/>: code / exchange / connessioni / account e permessi / virtual host / policy / limiti / feature flag / cluster; in alto a destra puoi impostare l'aggiornamento automatico e la **lingua dell'interfaccia** |
| Metriche di monitoraggio | <http://localhost:15672/metrics> (formato testuale Prometheus, richiede autenticazione); dashboard e avvisi in [docs/ops/monitoring](ops/monitoring/README.md) |
| Riga di comando | `./bin/swiftmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (disattivazione a caldo, la porta si chiude immediatamente) |
| Controllo di salute | `nc -z 127.0.0.1 15672` (compose include già un healthcheck) |
| Backup e ripristino | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Aggiornamento | [docs/ops/upgrade.md](ops/upgrade.md) |
| Baseline di sicurezza | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Configurazioni comuni (esempio completo in [configs/swiftmqd.json](../../../configs/swiftmqd.json); è anche possibile sovrascriverle con le variabili d'ambiente `SWIFTMQ_*`):

| Voce di configurazione | Descrizione | Predefinito |
| --- | --- | --- |
| `data_dir` | Directory dei dati (messaggi + metadati), **da persistere assolutamente** | `data` |
| `listeners` | Indirizzi di ascolto per ciascun protocollo, con TLS opzionale | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Indirizzo di ascolto di UI di gestione / API | `:15672` |
| `management.language` | Lingua predefinita dell'interfaccia di gestione; se vuota viene scelta automaticamente in base al fuso orario del luogo di distribuzione | automatica |
| `storage.fsync` | Profilo di scrittura su disco `none / os / batch / always` (determina anche il momento della conferma) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Livelli di risorsa: al superamento il produttore viene bloccato, **senza perdere messaggi** | `0.4` / 50 MiB |
| `users` | Tabella utenti integrata (password + tag + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Cluster multi-nodo (disattivato per impostazione predefinita); per modificare i membri usa `swiftmqctl add_member` | disattivato |

> La porta potrebbe essere occupata: basta sostituirla con un'altra tramite `listeners` / `management.addr`.

***

## Struttura del progetto

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # punto di ingresso del processo broker (è quello da eseguire)
│   └── swiftmqctl/      # CLI operativa (passa dall'HTTP API di gestione, disaccoppiata dalla versione del kernel)
├── internal/            # implementazione del kernel
│   ├── protocol/        # plugin di protocollo: amqp091, mqtt (codifica/decodifica / metodi / sessioni)
│   ├── broker/          # kernel: vhost, exchange, code, dead letter, controllo di flusso, viste del piano di gestione
│   ├── store/           # persistenza: log a segmenti, indici delle code, ripristino dopo crash
│   ├── raft/ meta/      # cluster: Raft proprietario e replica dei metadati
│   ├── management/      # HTTP API di gestione + metriche Prometheus + servizio statico dell'UI integrata
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # contratto stabile pubblico: API dei plugin (plugin) e protocollo wire dei plugin come processi esterni (sidecar)
├── web/                 # progetto frontend dell'interfaccia di gestione (Vue 3 + Vite), i cui artefatti vengono incorporati nel binario tramite go:embed in fase di build
├── configs/             # configurazioni di esempio
├── docs/ops/            # documentazione operativa: backup e ripristino / aggiornamento / baseline di sicurezza / monitoraggio
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG（交流群二维码）
```

***

## Contribuire

Siamo lieti di ricevere Issue e Pull Request. La ragion d'essere di questo progetto è la **compatibilità di protocollo**, perciò:

- per correggere un bug, descrivi il comportamento corrispondente di RabbitMQ (versione, client, passi di riproduzione);
- per modifiche che riguardano dettagli di protocollo, allega un confronto con RabbitMQ;
- prima di inviare, assicurati che `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .` passino tutti.

***

## Licenza

Questo progetto è distribuito con la [Apache License 2.0](../../../LICENSE).

Ne sono consentiti l'uso, la modifica e la distribuzione (incluso l'uso commerciale), a condizione di conservare le note di copyright e di licenza, senza alcuna garanzia.

Copyright 2026 houzch (vedi [NOTICE](../../../NOTICE))

***

## Riconoscimenti

La specifica del protocollo AMQP 0-9-1 e la semantica comportamentale di [RabbitMQ](https://www.rabbitmq.com/) costituiscono il riferimento per il lavoro di compatibilità di questo progetto. Questo progetto è un'implementazione indipendente, non ha alcun rapporto di affiliazione ufficiale con RabbitMQ e non ne utilizza il codice.

***

## Unisciti al gruppo

Scansiona il codice per unirti al gruppo di discussione SwiftMQ; se hai domande puoi porle direttamente nel gruppo:

![Gruppo di discussione SwiftMQ](../../../1280X1280.PNG)
