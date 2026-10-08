<!-- i18n-switcher -->
[简体中文](README-cn.md) | [繁體中文](docs/i18n/zh-TW/README.md) | **English** | [日本語](docs/i18n/ja/README.md) | [한국어](docs/i18n/ko/README.md) | [Español](docs/i18n/es/README.md) | [Deutsch](docs/i18n/de/README.md) | [Français](docs/i18n/fr/README.md) | [العربية](docs/i18n/ar/README.md) | [Русский](docs/i18n/ru/README.md) | [Italiano](docs/i18n/it/README.md) | [Nederlands](docs/i18n/nl/README.md) | [Português](docs/i18n/pt/README.md) | [Bahasa Indonesia](docs/i18n/id/README.md) | [ไทย](docs/i18n/th/README.md) | [Tiếng Việt](docs/i18n/vi/README.md) | [Bahasa Melayu](docs/i18n/ms/README.md) | [Filipino](docs/i18n/fil/README.md)

# SpeedMQ

A **RabbitMQ-compatible** messaging middleware written in Go. Existing RabbitMQ clients can connect by **changing only the connection address — no code changes, no SDK swap**.

## Introduction

- **Protocol compatible**: AMQP 0-9-1 (including RabbitMQ extensions) and MQTT 3.1.1; the compatibility baseline is **RabbitMQ 4.3 semantics**.
- **Simple to deploy**: one binary / one container, with the management UI already embedded; no extra Nginx, database, or Node runtime is needed.
- **Enough for operations**: management UI (queues / exchanges / connections / account permissions / virtual hosts / policies / limits / cluster), Prometheus `/metrics`, and the `speedmqctl` command line.
- **Default ports**: `5672` (AMQP), `1883` (MQTT), `15672` (management UI / HTTP API / metrics).

Capabilities already available: persistence (segment log + fsync level + crash recovery), publisher confirms, TTL / dead-lettering / length limits, consumer priorities, Direct Reply-To, clustering (Raft metadata + quorum queues + cross-node forwarding), and hot start/stop of plugins.

***

## Quick Start

### Option 1: Docker (recommended)

**No need to clone the repo — just pull the image and run it:**

```bash
docker run -d --name speedmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v speedmq-data:/var/lib/speedmq \
  houzch/speedmq:1.1.05
```

The image is published in two places with identical content (pick whichever is faster for you): Docker Hub `houzch/speedmq` and GitHub GHCR `ghcr.io/houzch/speedmq`; both provide `linux/amd64` and `linux/arm64`.

- Data lands in the named volume `speedmq-data`, which survives container recreation.
- Stop / remove: `docker stop speedmq`, `docker rm speedmq` (the data volume is kept).

**To change the configuration or use docker compose, clone the repo:**

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
docker compose pull && docker compose up -d   # Use the published image; switch to up -d --build to build locally

docker compose ps        # Status should be Up (healthy)
docker compose logs -f   # Follow the logs
```

- The configuration is mounted read-only from `configs/speedmqd.json`; changes take effect after `docker compose restart`.
- Stop: `docker compose down` (keeps data); `docker compose down -v` (deletes data too).

### Option 2: Local binary (requires Go 1.24+)

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
go build -o bin/speedmqd ./cmd/speedmqd
go build -o bin/speedmqctl ./cmd/speedmqctl
./bin/speedmqd -config configs/speedmqd.json
```

> The management UI build artifacts are not committed. If you want to use the UI, first run `npm ci && npm run build` in `web/`;
> you can still start and send/receive messages without building it — visiting `/` will just show "management UI not built".

### First login (be sure to change the default account first)

| Entry | Address / credentials |
| --- | --- |
| Management UI | <http://localhost:15672/> (username `guest`, password `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (same credentials) |

On a freshly installed instance, the root account carries a "force password change on first login" flag: after logging in to the management UI, you are **required to change both the account name and the password**; only then can you enter the admin console.

You can also do it directly via the API (suitable for automation):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ The default `guest/guest` behaves the same as RabbitMQ: **only local login is allowed**. To connect from outside the container / remotely, you must enable `remote_access` for that user in the configuration (the sample config already enables it for container scenarios).
> **As soon as the service is reachable externally, change the credentials immediately.**

### Connect your application (just change the connection address)

```python
# Python (pika)
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go (amqp091-go)
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (mosquitto client)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

The management HTTP API is compatible with `rabbitmqadmin`; the management UI's "add queue / exchange" actions are exactly the standard declaration endpoints, so scripts can do the same:

```bash
# Declare a queue (for a quorum queue, express it via arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Routine operations

| Item | Entry |
| --- | --- |
| Management UI | <http://localhost:15672/>: queues / exchanges / connections / account permissions / virtual hosts / policies / limits / feature flags / cluster; auto-refresh and the **UI language** can be set in the top-right corner |
| Monitoring metrics | <http://localhost:15672/metrics> (Prometheus text, authentication required); for dashboards and alerts see [docs/ops/monitoring](docs/ops/monitoring/README.md) |
| Command line | `./bin/speedmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (hot-disable; the port closes immediately) |
| Health check | `nc -z 127.0.0.1 15672` (the compose setup already includes a healthcheck) |
| Backup and restore | [docs/ops/backup-restore.md](docs/ops/backup-restore.md) |
| Upgrade | [docs/ops/upgrade.md](docs/ops/upgrade.md) |
| Security baseline | [docs/ops/security-baseline.md](docs/ops/security-baseline.md) |

Common configuration (for a complete example see [configs/speedmqd.json](configs/speedmqd.json), which can also be overridden with `SPEEDMQ_*` environment variables):

| Setting | Description | Default |
| --- | --- | --- |
| `data_dir` | Data directory (messages + metadata), **be sure to persist it** | `data` |
| `listeners` | Listen addresses for each protocol; TLS can be configured | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Management UI / API listen address | `:15672` |
| `management.language` | Default language of the management UI; if left empty, it is chosen automatically based on the deployment time zone | Auto |
| `storage.fsync` | Flush level `none / os / batch / always` (also determines confirm timing) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Resource watermarks: when triggered, producers are blocked, **without dropping messages** | `0.4` / 50 MiB |
| `users` | Built-in user table (password + tags + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Multi-node cluster (disabled by default); use `speedmqctl add_member` to change members | Disabled |

> Ports may already be in use: just switch to other ports via `listeners` / `management.addr`.

***

## Project structure

```
speedmq/
├── cmd/
│   ├── speedmqd/        # broker process entry point (this is the one to run)
│   └── speedmqctl/      # operations CLI (goes through the management HTTP API, decoupled from the kernel version)
├── internal/            # kernel implementation
│   ├── protocol/        # protocol plugins: amqp091, mqtt (encode/decode / methods / sessions)
│   ├── broker/          # kernel: vhost, exchanges, queues, dead-lettering, flow control, management-plane views
│   ├── store/           # persistence: segment log, queue index, crash recovery
│   ├── raft/ meta/      # clustering: in-house Raft and metadata replication
│   ├── management/      # management HTTP API + Prometheus metrics + embedded UI static serving
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # stable external contract: plugin API (plugin) and out-of-process plugin wire protocol (sidecar)
├── web/                 # management UI frontend project (Vue 3 + Vite); artifacts are embedded into the binary via go:embed at build time
├── configs/             # sample configuration
├── docs/ops/            # operations docs: backup-restore / upgrade / security baseline / monitoring
├── Dockerfile, docker-compose.yml
└── speedmq-logo.PNG, 1280X1280.PNG (community group QR code)
```

***

## Contributing

Issues and Pull Requests are welcome. Since **protocol compatibility** is the foundation of this project:

- When fixing a bug, please describe the corresponding RabbitMQ behavior (version, client, reproduction steps);
- For changes involving protocol details, please include the comparison results against RabbitMQ;
- Before submitting, make sure `go build ./...`, `go vet ./...`, `go test ./...`, and `gofmt -l .` all pass.

***

## License

This project is licensed under the [Apache License 2.0](LICENSE).

Use, modification, and distribution (including commercial use) are permitted, provided that the copyright and license notices are retained, and no warranty of any kind is provided.

Copyright 2026 houzch (see [NOTICE](NOTICE))

***

## Acknowledgements

The AMQP 0-9-1 protocol specification and the behavioral semantics of [RabbitMQ](https://www.rabbitmq.com/) are the reference baseline for this project's compatibility work. This project is an independent implementation, is not affiliated with the official RabbitMQ project, and does not use its code.

***

## Join the community group

Scan the QR code to join the SpeedMQ community group; you can ask questions directly there:

![SpeedMQ community group](1280X1280.PNG)
