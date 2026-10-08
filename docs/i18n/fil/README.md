<!-- i18n-switcher -->
[简体中文](../../../README-cn.md) | [繁體中文](../zh-TW/README.md) | [English](../../../README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | **Filipino**

# SpeedMQ

Isang **RabbitMQ-compatible** na message middleware na isinulat gamit ang Go. Ang mga kasalukuyang RabbitMQ client ay **hindi kailangang magbago ng code o magpalit ng SDK** — sapat na ang palitan ang connection address upang maka-connect.

## Panimula

- **Pagkakatugma ng protocol**: AMQP 0-9-1 (kasama ang RabbitMQ extensions) at MQTT 3.1.1; ang baseline ng pagkakatugma ay ang **RabbitMQ 4.3 semantics**.
- **Simpleng deployment**: isang binary / isang container, naka-embed na ang management UI, hindi na kailangan ng dagdag na Nginx, database o Node runtime.
- **Sapat para sa operations**: management UI (queues / exchanges / connections / account permissions / virtual hosts / policies / limits / cluster), Prometheus `/metrics`, command-line `speedmqctl`.
- **Default ports**: `5672` (AMQP), `1883` (MQTT), `15672` (management UI / HTTP API / metrics).

Mga kakayahang mayroon na: persistence (segment log + fsync tiers + crash recovery), publisher confirms, TTL / dead-letter / length limits, consumer priority, Direct Reply-To, clustering (Raft metadata + quorum queues + cross-node forwarding), hot start/stop ng plugins.

***

## Mabilis na Pagsisimula

### Paraan 1: Docker (inirerekomenda)

**Hindi na kailangang i-clone ang repo — kunin lang ang image at patakbuhin:**

```bash
docker run -d --name speedmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v speedmq-data:/var/lib/speedmq \
  houzch/speedmq:1.1.05
```

Nailalathala ang image sa dalawang lugar na pareho ang nilalaman (piliin ang mas mabilis para sa iyo): Docker Hub `houzch/speedmq` at GitHub GHCR `ghcr.io/houzch/speedmq`; parehong may `linux/amd64` at `linux/arm64`.

- Nasa named volume na `speedmq-data` ang data, hindi nawawala kahit muling gawin ang container.
- Itigil / alisin: `docker stop speedmq`, `docker rm speedmq` (nananatili ang data volume).

**Kung kailangang baguhin ang configuration o gumamit ng compose, i-clone ang repo:**

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
docker compose pull && docker compose up -d   # Gumagamit ng nailathalang image; palitan ng up -d --build para mag-build nang lokal

docker compose ps        # Dapat Up (healthy) ang status
docker compose logs -f   # Sundan ang logs
```

- Read-only na naka-mount ang configuration mula sa `configs/speedmqd.json`; epektibo ang pagbabago pagkatapos ng `docker compose restart`.
- Itigil: `docker compose down` (nananatili ang data); `docker compose down -v` (kasama ang data na buburahin).

### Paraan 2: Lokal na binary (kailangan ang Go 1.24+)

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
go build -o bin/speedmqd ./cmd/speedmqd
go build -o bin/speedmqctl ./cmd/speedmqctl
./bin/speedmqd -config configs/speedmqd.json
```

> Ang build artifact ng management UI ay hindi kasama sa repository. Kung gusto mong gamitin ang UI, patakbuhin muna ang `npm ci && npm run build` sa `web/`;
> Kahit hindi mag-build, normal pa ring magsisimula ang pagpapadala at pagtanggap ng mga mensahe, ipapakita lamang ng pagbisita sa `/` ang 「hindi pa naka-build ang management UI」.

### Unang pag-login (siguraduhing palitan muna ang default account)

| Entry point | Address / credentials |
| --- | --- |
| Management UI | <http://localhost:15672/> (username na `guest`, password na `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (parehong account sa itaas) |

Ang superuser account ng bagong install na instance ay may markang 「sapilitang pagpapalit ng password sa unang pag-login」: pagkatapos mag-login sa management UI, **sapilitang hihilingin na palitan nang sabay ang username at password**, at makakapasok lamang sa backend pagkatapos itong palitan.

Maaari ring gawin nang direkta sa pagtawag sa API (angkop para sa automation):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ Ang default na `guest/guest` ay katulad ng gawi ng RabbitMQ: **tanging lokal na pag-login lamang ang pinapayagan**. Para sa koneksyon mula sa labas ng container / remotely, kailangang i-enable ang `remote_access` para sa user na iyon sa config (na-enable na ito ng halimbawang config para sa container scenario).
> **Kapag na-access na ang serbisyo mula sa labas, agad na palitan ang mga credential.**

### Pagkonekta sa iyong app (palitan lang ang connection address)

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
# MQTT (mosquitto client)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

Ang management HTTP API ay compatible sa `rabbitmqadmin`; ang 「Magdagdag ng queue / exchange」 sa management UI ay ang standard na declaration endpoint, kaya ring gawin ng script:

```bash
# Idedeklara ang queue (para sa quorum queue, ginagamit ang arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Pang-araw-araw na operations

| Bagay | Entry point |
| --- | --- |
| Management UI | <http://localhost:15672/>: queues / exchanges / connections / account permissions / virtual hosts / policies / limits / feature flags / cluster; sa kanang itaas na sulok ay maaaring itakda ang auto-refresh at ang **wika ng interface** |
| Monitoring metrics | <http://localhost:15672/metrics> (Prometheus text, kailangan ng auth); para sa dashboard at alerts tingnan ang [docs/ops/monitoring](ops/monitoring/README.md) |
| Command line | `./bin/speedmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (hot disable, agad na magsasara ang port) |
| Health check | `nc -z 127.0.0.1 15672` (may built-in nang healthcheck ang compose) |
| Backup at restore | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Upgrade | [docs/ops/upgrade.md](ops/upgrade.md) |
| Security baseline | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Mga karaniwang config (para sa kumpletong halimbawa tingnan ang [configs/speedmqd.json](../../../configs/speedmqd.json), maaari ring i-override gamit ang `SPEEDMQ_*` environment variables):

| Config item | Paglalarawan | Default |
| --- | --- | --- |
| `data_dir` | Data directory (mga mensahe + metadata), **siguraduhing naka-persist** | `data` |
| `listeners` | Listen address ng bawat protocol, maaaring i-configure ang TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Listen address ng management UI / API | `:15672` |
| `management.language` | Default na wika ng management UI; kung walang laman, awtomatikong pipiliin batay sa timezone ng deployment site | Awtomatiko |
| `storage.fsync` | Tier ng pagsusulat sa disk na `none / os / batch / always` (tinutukoy din ang timing ng confirm) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Resource watermark: kapag na-trigger ay hinaharang ang mga producer, **hindi nawawala ang mga mensahe** | `0.4` / 50 MiB |
| `users` | Built-in na user table (password + tags + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Multi-node cluster (naka-off bilang default), para sa pagbabago ng member gamitin ang `speedmqctl add_member` | Naka-off |

> Maaaring abala ang port: palitan lang ng ibang port gamit ang `listeners` / `management.addr`.

***

## Estruktura ng Proyekto

```
speedmq/
├── cmd/
│   ├── speedmqd/        # Entry point ng broker process (ito ang dapat patakbuhin)
│   └── speedmqctl/      # Operations CLI (dumadaan sa management HTTP API, decoupled sa kernel version)
├── internal/            # Implementasyon ng kernel
│   ├── protocol/        # Protocol plugins: amqp091, mqtt (encode/decode / methods / sessions)
│   ├── broker/          # Kernel: vhost, exchanges, queues, dead-letter, flow control, management-plane views
│   ├── store/           # Persistence: segment log, queue index, crash recovery
│   ├── raft/ meta/      # Cluster: sariling-gawang Raft at metadata replication
│   ├── management/      # Management HTTP API + Prometheus metrics + embedded static UI service
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # Stable external contract: plugin API (plugin) at wire protocol ng external process plugin (sidecar)
├── web/                 # Frontend project ng management UI (Vue 3 + Vite), ang build artifact ay isinasama sa binary sa pamamagitan ng go:embed sa oras ng build
├── configs/             # Halimbawang config
├── docs/ops/            # Operations docs: backup-restore / upgrade / security baseline / monitoring
├── Dockerfile、docker-compose.yml
└── speedmq-logo.PNG、1280X1280.PNG（交流群二维码）
```

***

## Pag-aambag

Malugod na tinatanggap ang mga Issue at Pull Request. Ang pundasyon ng proyektong ito ay ang **pagkakatugma ng protocol**, kaya:

- Sa pag-aayos ng bug, ilarawan ang katumbas na gawi ng RabbitMQ (version, client, mga hakbang sa pag-reproduce);
- Para sa mga pagbabagong may kinalaman sa detalye ng protocol, isama ang resulta ng paghahambing sa RabbitMQ;
- Bago mag-submit, siguraduhing pumasa ang `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .`.

***

## Lisensya

Ginagamit ng proyektong ito ang [Apache License 2.0](../../../LICENSE).

Pinapayagan ang paggamit, pagbabago, at pamamahagi (kasama ang komersyal na paggamit), kailangang panatilihin ang copyright at notice ng lisensya, at walang ibinibigay na anumang warranty.

Copyright 2026 houzch (tingnan ang [NOTICE](../../../NOTICE))

***

## Pasasalamat

Ang detalye ng AMQP 0-9-1 protocol at ang behavior semantics ng [RabbitMQ](https://www.rabbitmq.com/) ang batayan ng paghahambing para sa compatibility work ng proyektong ito. Ang proyektong ito ay isang independiyenteng implementasyon, walang kaugnayan sa opisyal na RabbitMQ, at hindi gumamit ng kanilang code.

***

## Sumali sa Community Group

I-scan ang QR code upang sumali sa SpeedMQ community group; maaari kang magtanong nang direkta sa grupo kung may problema:

![SpeedMQ community group](../../../1280X1280.PNG)
