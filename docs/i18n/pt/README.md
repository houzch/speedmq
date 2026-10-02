<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | **Português** | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

Middleware de mensagens **compatível com RabbitMQ** escrito em Go. Os clientes RabbitMQ existentes se conectam **sem alterar código nem trocar de SDK**, bastando mudar o endereço de conexão.

## Visão geral

- **Compatibilidade de protocolo**: AMQP 0-9-1 (com extensões do RabbitMQ) e MQTT 3.1.1; a linha de base de compatibilidade é a **semântica do RabbitMQ 4.3**.
- **Implantação simples**: um binário / um contêiner, com a UI de gerenciamento já embutida, sem precisar de Nginx, banco de dados ou runtime Node adicionais.
- **Operação suficiente**: UI de gerenciamento (filas / exchanges / conexões / permissões de contas / hosts virtuais / políticas / limites / cluster), `/metrics` do Prometheus, linha de comando `swiftmqctl`.
- **Portas padrão**: `5672` (AMQP), `1883` (MQTT), `15672` (UI de gerenciamento / API HTTP / métricas).

Recursos já disponíveis: persistência (log de segmentos + níveis de fsync + recuperação de falhas), confirmação de publicação, TTL / dead letter / limite de tamanho, prioridade de consumidores, Direct Reply-To, cluster (metadados Raft + filas quórum + encaminhamento entre nós), ativação/desativação de plugins a quente.

***

## Início rápido

### Opção 1: Docker (recomendado)

**Sem clonar o repositório: baixe a imagem e execute.**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.1.0
```

A imagem é publicada em dois locais com o mesmo conteúdo (use o mais rápido para você): Docker Hub `houzch/swiftmq` e GitHub GHCR `ghcr.io/houzch/swiftmq`; ambos oferecem `linux/amd64` e `linux/arm64`.

- Os dados ficam no volume nomeado `swiftmq-data`, que sobrevive à recriação do contêiner.
- Parar / remover: `docker stop swiftmq`, `docker rm swiftmq` (o volume de dados é mantido).

**Para alterar a configuração ou usar compose, clone o repositório:**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # Usa a imagem publicada; troque para up -d --build para compilar localmente

docker compose ps        # O status deve ser Up (healthy)
docker compose logs -f   # Acompanhar os logs
```

- A configuração é montada somente leitura a partir de `configs/swiftmqd.json`; as alterações valem após `docker compose restart`.
- Parar: `docker compose down` (mantém os dados); `docker compose down -v` (apaga os dados também).

### Opção 2: binário local (requer Go 1.24+)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> O artefato de build da UI de gerenciamento não é versionado. Se quiser usar a UI, execute antes `npm ci && npm run build` em `web/`;
> mesmo sem fazer o build, o serviço inicia e troca mensagens normalmente, apenas o acesso a `/` mostrará o aviso "UI de gerenciamento não compilada".

### Primeiro login (troque a conta padrão antes de qualquer coisa)

| Entrada | Endereço / credenciais |
| --- | --- |
| UI de gerenciamento | <http://localhost:15672/> (usuário `guest`, senha `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (mesmas credenciais) |

A conta principal de uma instância recém-instalada carrega a marca "troca de senha obrigatória no primeiro login": após entrar na UI de gerenciamento, é **obrigatório alterar ao mesmo tempo o nome da conta e a senha**; só depois disso o acesso ao painel é liberado.

Também é possível fazer isso diretamente pela API (útil para automação):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<nova senha>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ O padrão `guest/guest` se comporta igual ao do RabbitMQ: **só permite login local**. Para conectar de fora do contêiner / remotamente, é preciso habilitar `remote_access` para esse usuário na configuração (a configuração de exemplo já o habilita para o cenário de contêiner).
> **Assim que o serviço ficar acessível externamente, troque as credenciais imediatamente.**

### Conecte sua aplicação (basta mudar o endereço de conexão)

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
# MQTT (cliente mosquitto)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

A API HTTP de gerenciamento é compatível com o `rabbitmqadmin`; o "adicionar fila / exchange" da UI de gerenciamento é exatamente o endpoint de declaração padrão, e scripts também conseguem fazer o mesmo:

```bash
# Declarar fila (filas quórum são expressas com arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Operação do dia a dia

| Item | Entrada |
| --- | --- |
| UI de gerenciamento | <http://localhost:15672/>: filas / exchanges / conexões / permissões de contas / hosts virtuais / políticas / limites / feature flags / cluster; no canto superior direito é possível configurar a atualização automática e o **idioma da interface** |
| Métricas de monitoramento | <http://localhost:15672/metrics> (texto Prometheus, requer autenticação); painéis e alertas em [docs/ops/monitoring](ops/monitoring/README.md) |
| Linha de comando | `./bin/swiftmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (desativação a quente, a porta fecha imediatamente) |
| Verificação de saúde | `nc -z 127.0.0.1 15672` (o compose já inclui healthcheck) |
| Backup e restauração | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Atualização | [docs/ops/upgrade.md](ops/upgrade.md) |
| Linha de base de segurança | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Configurações comuns (exemplo completo em [configs/swiftmqd.json](../../../configs/swiftmqd.json), também sobreponíveis por variáveis de ambiente `SWIFTMQ_*`):

| Item de configuração | Descrição | Padrão |
| --- | --- | --- |
| `data_dir` | Diretório de dados (mensagens + metadados), **é imprescindível persistir** | `data` |
| `listeners` | Endereços de escuta de cada protocolo, com TLS opcional | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Endereço de escuta da UI de gerenciamento / API | `:15672` |
| `management.language` | Idioma padrão da UI de gerenciamento; se vazio, escolhido automaticamente pelo fuso horário do local de implantação | automático |
| `storage.fsync` | Nível de gravação em disco `none / os / batch / always` (também determina o momento do confirm) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Níveis de recursos: ao serem atingidos, bloqueiam os produtores, **sem perder mensagens** | `0.4` / 50 MiB |
| `users` | Tabela de usuários embutida (senha + tags + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Cluster multinó (desativado por padrão); mudanças de membros com `swiftmqctl add_member` | desativado |

> As portas podem estar em uso: basta trocá-las por outras via `listeners` / `management.addr`.

***

## Estrutura do projeto

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # Ponto de entrada do processo broker (é este que você executa)
│   └── swiftmqctl/      # CLI de operação (usa a API HTTP de gerenciamento, desacoplada da versão do núcleo)
├── internal/            # Implementação do núcleo
│   ├── protocol/        # Plugins de protocolo: amqp091, mqtt (codificação/decodificação / métodos / sessões)
│   ├── broker/          # Núcleo: vhost, exchanges, filas, dead letter, controle de fluxo, visão do plano de gerenciamento
│   ├── store/           # Persistência: log de segmentos, índice de filas, recuperação de falhas
│   ├── raft/ meta/      # Cluster: Raft próprio e replicação de metadados
│   ├── management/      # API HTTP de gerenciamento + métricas Prometheus + serviço estático da UI embutida
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # Contratos estáveis públicos: API de plugins (plugin) e protocolo de linha de plugins de processo externo (sidecar)
├── web/                 # Front-end da UI de gerenciamento (Vue 3 + Vite), artefatos embutidos no binário via go:embed na build
├── configs/             # Configurações de exemplo
├── docs/ops/            # Documentação de operação: backup/restauração / atualização / linha de base de segurança / monitoramento
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG (QR code do grupo de chat)
```

***

## Contribuição

Issues e Pull Requests são bem-vindos. A razão de existir deste projeto é a **compatibilidade de protocolo**, portanto:

- Ao corrigir um bug, descreva o comportamento correspondente do RabbitMQ (versão, cliente, passos de reprodução);
- Alterações que envolvam detalhes de protocolo devem vir acompanhadas do resultado da comparação com o RabbitMQ;
- Antes de enviar, garanta que `go build ./...`, `go vet ./...`, `go test ./...` e `gofmt -l .` passem.

***

## Licença

Este projeto usa a [Apache License 2.0](../../../LICENSE).

É permitido usar, modificar e distribuir (inclusive para uso comercial), desde que os avisos de copyright e licença sejam mantidos, e sem qualquer garantia.

Copyright 2026 houzch (ver [NOTICE](../../../NOTICE))

***

## Agradecimentos

A especificação do protocolo AMQP 0-9-1 e a semântica de comportamento do [RabbitMQ](https://www.rabbitmq.com/) são a base de referência para o trabalho de compatibilidade deste projeto. Este projeto é uma implementação independente, sem vínculo com o RabbitMQ oficial, e não utiliza seu código.

***

## Entre no grupo de chat

Escaneie o código para entrar no grupo de chat do SwiftMQ; se tiver dúvidas, pode perguntar diretamente no grupo:

![Grupo de chat do SwiftMQ](../../../1280X1280.PNG)
