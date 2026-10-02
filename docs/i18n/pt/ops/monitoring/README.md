# Monitoramento e alertas do SwiftMQ

Este diretório fornece templates de monitoramento prontos para uso:

| Arquivo | Função |
| --- | --- |
| `prometheus-alerts.yml` | Regras de alerta do Prometheus (`groups: - name: swiftmq`) |
| `grafana-dashboard.json` | Painel do Grafana importável (os painéis cobrem os sinais-chave descritos abaixo) |
| `README.md` | Uso, lista de métricas, significado e tratamento de cada alerta, lacunas conhecidas |

---

## 1. Como usar

### 1.1 Coleta (Prometheus)

O plano de gerenciamento (por padrão `:15672`) expõe o formato de texto do Prometheus em `/metrics`, **requer Basic Auth**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: swiftmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> Recomenda-se criar uma conta somente leitura dedicada ao monitoramento (a tag `monitoring` já basta para ler as métricas) e não reutilizar a senha do administrador.

Verifique se a coleta está funcionando (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Regras de alerta

Coloque `prometheus-alerts.yml` no diretório de regras do Prometheus, referencie-o em `prometheus.yml` e faça reload:

```yaml
rule_files:
  - "rules/swiftmq-alerts.yml"
```

As regras usam `job="swiftmq"` de forma uniforme; se o nome do seu job for diferente, substitua no arquivo inteiro.

### 1.3 Painel do Grafana

Importe `grafana-dashboard.json` via **Dashboards → Import → upload do JSON**, escolhendo sua fonte de dados Prometheus na importação
(no painel ela é referenciada pela variável `${DS_PROMETHEUS}`). A variável de template `DS_PROMETHEUS` é atribuída no mapeamento da importação.

**【não verificado】** Não foi subida uma instância do Grafana nesta máquina, então não houve validação de importação real; o JSON só passou por validação de sintaxe (14 painéis, análise bem-sucedida).

---

## 2. Trecho real de `/metrics` (evidência)

A seguir está a **saída real** do `/metrics` da instância `1.0.0` desta máquina (já existia uma fila durable `persist.q`,
por isso apareceram as métricas por fila com as labels `vhost`/`queue`):

```
# HELP swiftmq_up 节点是否存活
# TYPE swiftmq_up gauge
swiftmq_up 1
# HELP swiftmq_build_info 构建信息
# TYPE swiftmq_build_info gauge
swiftmq_build_info 1{version="1.0.0",node="swiftmq@DESKTOP-HBDCVPA"}
# HELP swiftmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE swiftmq_resource_blocked gauge
swiftmq_resource_blocked 0
# HELP swiftmq_connections 当前连接数
# TYPE swiftmq_connections gauge
swiftmq_connections 0
# HELP swiftmq_channels 当前通道数
# TYPE swiftmq_channels gauge
swiftmq_channels 0
# HELP swiftmq_queues 当前队列数
# TYPE swiftmq_queues gauge
swiftmq_queues 0
# HELP swiftmq_exchanges 当前交换机数
# TYPE swiftmq_exchanges gauge
swiftmq_exchanges 6
# HELP swiftmq_consumers 当前消费者数
# TYPE swiftmq_consumers gauge
swiftmq_consumers 0
# HELP swiftmq_queue_messages 就绪消息总数
# TYPE swiftmq_queue_messages gauge
swiftmq_queue_messages 0
# HELP swiftmq_queue_messages_unacknowledged 未确认消息总数
# TYPE swiftmq_queue_messages_unacknowledged gauge
swiftmq_queue_messages_unacknowledged 0
# HELP swiftmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE swiftmq_process_memory_bytes gauge
swiftmq_process_memory_bytes 1564672
# HELP swiftmq_memory_total_bytes 物理内存总量
# TYPE swiftmq_memory_total_bytes gauge
swiftmq_memory_total_bytes 34181279744
# HELP swiftmq_disk_free_bytes 数据目录可用空间
# TYPE swiftmq_disk_free_bytes gauge
swiftmq_disk_free_bytes 240091688960
# HELP swiftmq_memory_high_watermark 内存水位比例
# TYPE swiftmq_memory_high_watermark gauge
swiftmq_memory_high_watermark 0.4
# HELP swiftmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE swiftmq_disk_free_limit_bytes gauge
swiftmq_disk_free_limit_bytes 52428800
# HELP swiftmq_queue_messages_ready 队列中的就绪消息数
# TYPE swiftmq_queue_messages_ready gauge
# HELP swiftmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE swiftmq_queue_messages_unacknowledged gauge
# HELP swiftmq_queue_consumers 队列上的消费者数
# TYPE swiftmq_queue_consumers gauge
# HELP swiftmq_queue_memory_bytes 队列内存占用估算值
# TYPE swiftmq_queue_memory_bytes gauge
# HELP swiftmq_queue_messages_published_total 队列累计接收的消息数
# TYPE swiftmq_queue_messages_published_total counter
# HELP swiftmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE swiftmq_queue_messages_delivered_total counter
# HELP swiftmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE swiftmq_queue_messages_acked_total counter
swiftmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
swiftmq_queue_consumers{vhost="/",queue="persist.q"} 0
swiftmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
swiftmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
swiftmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP swiftmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE swiftmq_plugin_info gauge
# HELP swiftmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE swiftmq_plugin_up gauge
swiftmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="amqp091",state="enabled"} 1
swiftmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Lista de métricas (todas realmente existem, origem `internal/management/metrics.go`)

| Métrica | Tipo | Labels | Semântica |
| --- | --- | --- | --- |
| `swiftmq_up` | gauge | — | O processo se declara vivo (atualmente sempre 1) |
| `swiftmq_build_info` | gauge | `version`,`node` | Informações de build, value sempre 1 |
| `swiftmq_resource_blocked` | gauge | — | Se o nível de recursos bloqueou os produtores (1=bloqueando) |
| `swiftmq_connections` | gauge | — | Número atual de conexões |
| `swiftmq_channels` | gauge | — | Número atual de canais |
| `swiftmq_queues` | gauge | — | Número atual de filas |
| `swiftmq_exchanges` | gauge | — | Número atual de exchanges |
| `swiftmq_consumers` | gauge | — | Número atual de consumidores |
| `swiftmq_queue_messages` | gauge | — | Total **global** de mensagens prontas |
| `swiftmq_queue_messages_unacknowledged` | gauge | — | Total **global** de mensagens não confirmadas |
| `swiftmq_process_memory_bytes` | gauge | — | Memória **em uso** do processo (`HeapInuse+StackInuse`) |
| `swiftmq_memory_total_bytes` | gauge | — | Total de memória física |
| `swiftmq_disk_free_bytes` | gauge | — | Espaço disponível no diretório de dados |
| `swiftmq_memory_high_watermark` | gauge | — | Proporção do nível de memória |
| `swiftmq_disk_free_limit_bytes` | gauge | — | Limite mínimo de espaço em disco |
| `swiftmq_queue_messages_ready` | gauge | `vhost`,`queue` | Número de mensagens prontas de uma fila |
| `swiftmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Número de mensagens não confirmadas de uma fila |
| `swiftmq_queue_consumers` | gauge | `vhost`,`queue` | Número de consumidores de uma fila |
| `swiftmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Estimativa de uso de memória de uma fila |
| `swiftmq_queue_messages_published_total` | counter | `vhost`,`queue` | Total de mensagens recebidas acumuladas pela fila |
| `swiftmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Total de mensagens entregues acumuladas pela fila |
| `swiftmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Total de mensagens confirmadas acumuladas pela fila |
| `swiftmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Metadados do plugin, value sempre 1 |
| `swiftmq_plugin_up` | gauge | `name`,`state` | Se o plugin está em serviço (1=enabled, 0=outros) |

### 3.1 Cuidados de uso (para não escrever errado)

- **Duas famílias de séries com o mesmo nome e cardinalidades diferentes**: `swiftmq_queue_messages_unacknowledged` tem **tanto** uma série global sem labels
  **quanto** séries por fila com labels; já no lado de "prontas", a global se chama `swiftmq_queue_messages` e a por fila se chama
  `swiftmq_queue_messages_ready` (os nomes são assimétricos). Ao escrever regras, use `{queue=~".+"}` para pegar explicitamente só a família por fila.
- **Critério de `swiftmq_process_memory_bytes`**: a implementação é `MemStats.HeapInuse + StackInuse` (**memória em uso**),
  o mesmo critério usado na decisão do nível de memória do núcleo; porém o texto do seu `# HELP` diz "número de bytes de memória solicitados ao sistema operacional", **o texto não bate com o critério real**,
  e vale o que este documento diz.
- **As séries por fila só aparecem quando a fila existe**: após excluir a fila, a série desaparece (vira `stale` no lado do Prometheus).
  Alertas que envolvam "a fila deveria existir mas não tem dados" podem usar `absent()` em conjunto ou o `or vector(0)` do Grafana.
- **Os counter zeram após reiniciar o processo**: `*_total` é acumulado dentro do processo e reinicia em 0; use `rate()`/`increase()`,
  e não defina limiares diretamente sobre valores absolutos.
- **Não há sinal de cluster no metrics**: ver §5.

---

## 4. Significado dos alertas e tratamento sugerido (correspondente a `prometheus-alerts.yml`)

| Alerta | Condição de disparo | Significado | Tratamento sugerido |
| --- | --- | --- | --- |
| `SwiftMQScrapeDown` | `up{job="swiftmq"} == 0` por 1m | O alvo de coleta está totalmente inacessível | Verifique processo/porta/rede/autenticação; reinicie e veja o log de inicialização |
| `SwiftMQProcessNotUp` | `swiftmq_up == 0` por 1m | A coleta funcionou, mas o processo se declara não vivo | Item de último recurso; veja o log de saída anormal |
| `SwiftMQPluginDown` | `swiftmq_plugin_up == 0` por 2m | Plugin disabled/failed/down | `swiftmqctl plugins show <name>` para ver o `runtime_note`; plugins externos com `restart=always` normalmente se recuperam sozinhos |
| `SwiftMQResourceBlocked` | `swiftmq_resource_blocked == 1` por 5m | Nível de memória/disco atingido, produtores bloqueados | Verifique o nível de memória e o espaço em disco; confirme se os consumidores estão avançando |
| `SwiftMQMemoryWatermarkHigh` | Uso de memória em uso > 0.9× nível por 10m | Aproximando-se do nível de memória | Reduza o acúmulo / acelere o consumo, para evitar disparar o bloqueio |
| `SwiftMQDiskFreeLow` | Disponível < 1.5× limite de disco por 10m | O diretório de dados está quase cheio | Amplie/limpe; ao atingir o limite, os produtores serão bloqueados |
| `SwiftMQQueueBacklogGrowing` | Prontas >10000 e crescendo monotonicamente por 15m | A fila segue acumulando | Amplie consumidores / verifique o consumidor; investigue anomalias de dead letter/TTL |
| `SwiftMQQueueNoConsumers` | Consumidores=0 e há mensagens prontas por 15m | Ninguém consumindo | Verifique o processo do lado consumidor; confirme se o consumidor não caiu |
| `SwiftMQUnackedPileUp` | Não confirmadas >1000 por 15m | Consumidor travado / não dá ack | Verifique a lógica de processamento do consumidor e o prefetch; se necessário feche a conexão para reentregar |
| `SwiftMQConnectionSpike` | Conexões >10000 por 10m | Número de conexões anormal | Verifique vazamento de conexões; o cliente deve reutilizar conexões |

> Os limiares (10000 / 1000 etc.) são **valores iniciais**; ajuste conforme a escala das suas filas e as características do seu negócio.

---

## 5. Lacuna conhecida: o cluster "perdeu a maioria / sem leader" e atualmente não há métricas

- **Fato**: `/metrics` **não** tem nenhuma métrica de cluster (não existe `swiftmq_cluster_*`). O estado do cluster só aparece no JSON de `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Por isso** o `prometheus-alerts.yml` **deliberadamente não escreve** alertas baseados em métricas de cluster — se escrevesse, **nunca disparariam**
  (o Prometheus não dá erro por um nome de métrica inexistente), o que seria uma entrega "que parece certa, mas é inútil".
- **Solução própria** (escolha uma das duas; ambas precisam ser construídas fora do SwiftMQ e não fazem parte do escopo deste repositório):
  1. use um exporter JSON genérico para coletar `/api/cluster` e mapeá-lo para métricas personalizadas (como `swiftmq_cluster_has_quorum`) e então alertar sobre essa métrica;
  2. use um script de sonda que chame `/api/cluster` periodicamente e dispare o alerta quando `has_quorum=false` ou `paused=true`.
- Critério dos limiares relacionados: `has_quorum=false` significa perda de contato com a maioria; com `pause_minority` (padrão), nesse momento **o serviço é pausado e as conexões são encerradas**.

---

## 6. Outros itens **não verificados**

- O painel do Grafana **não foi importado e validado em um Grafana real** (só passou por validação de sintaxe JSON).
- As regras de alerta **não foram carregadas e validadas em um Prometheus/Alertmanager real** (o Prometheus não foi iniciado nesta máquina).
  Mas os **nomes das métricas nas regras foram conferidos linha a linha com a saída real de `/metrics`** (ver §2/§3), então não há o problema de "nome escrito errado que nunca dispara".
