# SpeedMQ: мониторинг и оповещения

В этом каталоге приведены готовые к использованию шаблоны мониторинга:

| Файл | Назначение |
| --- | --- |
| `prometheus-alerts.yml` | Правила оповещений Prometheus (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | Импортируемый дашборд Grafana (панели охватывают перечисленные ниже ключевые сигналы) |
| `README.md` | Использование, список метрик, смысл и реакция на каждое оповещение, известные пробелы |

---

## 1. Как использовать

### 1.1 Сбор метрик (Prometheus)

Управляющая панель (по умолчанию `:15672`) предоставляет `/metrics` в текстовом формате Prometheus, **требуется Basic Auth**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: speedmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> Рекомендуется завести для мониторинга отдельную учётную запись только для чтения (тега `monitoring` достаточно для чтения метрик), не переиспользуйте пароль администратора.

Проверка корректности сбора (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Правила оповещений

Поместите `prometheus-alerts.yml` в каталог правил Prometheus, добавьте ссылку в `prometheus.yml` и выполните reload:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

В правилах везде используется `job="speedmq"`; если имя вашего job отличается, замените его по всему тексту.

### 1.3 Дашборд Grafana

`grafana-dashboard.json` импортируется через **Dashboards → Import → загрузить JSON**, при импорте выберите ваш источник данных Prometheus
(в дашборде он используется через переменную `${DS_PROMETHEUS}`). Переменная шаблона `DS_PROMETHEUS` задаётся в сопоставлении при импорте.

**【Не проверялось】** Экземпляр Grafana локально не запускался, реальная проверка импорта не выполнялась; для этого JSON выполнена только проверка синтаксиса JSON (14 панелей, разбор прошёл успешно).

---

## 2. Реальный фрагмент `/metrics` (доказательство)

Ниже приведён **реальный вывод** `/metrics` экземпляра `1.0.0` на локальной машине (была создана durable-очередь `persist.q`,
поэтому появились метрики per-queue с метками `vhost`/`queue`):

```
# HELP speedmq_up 节点是否存活
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info 构建信息
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections 当前连接数
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels 当前通道数
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues 当前队列数
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges 当前交换机数
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers 当前消费者数
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages 就绪消息总数
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged 未确认消息总数
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes 物理内存总量
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes 数据目录可用空间
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark 内存水位比例
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready 队列中的就绪消息数
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers 队列上的消费者数
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes 队列内存占用估算值
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total 队列累计接收的消息数
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Список метрик (все реально существуют, источник `internal/management/metrics.go`)

| Метрика | Тип | Метки | Семантика |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | Процесс сам сообщает о том, что жив (сейчас всегда 1) |
| `speedmq_build_info` | gauge | `version`,`node` | Информация о сборке, value всегда 1 |
| `speedmq_resource_blocked` | gauge | — | Блокирует ли порог ресурсов производителей (1=блокирует) |
| `speedmq_connections` | gauge | — | Текущее число подключений |
| `speedmq_channels` | gauge | — | Текущее число каналов |
| `speedmq_queues` | gauge | — | Текущее число очередей |
| `speedmq_exchanges` | gauge | — | Текущее число обменников |
| `speedmq_consumers` | gauge | — | Текущее число потребителей |
| `speedmq_queue_messages` | gauge | — | **Глобальное** общее число готовых сообщений |
| `speedmq_queue_messages_unacknowledged` | gauge | — | **Глобальное** общее число неподтверждённых сообщений |
| `speedmq_process_memory_bytes` | gauge | — | Память, **используемая** процессом (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | Общий объём физической памяти |
| `speedmq_disk_free_bytes` | gauge | — | Свободное пространство каталога данных |
| `speedmq_memory_high_watermark` | gauge | — | Доля порога памяти |
| `speedmq_disk_free_limit_bytes` | gauge | — | Нижний предел свободного места на диске |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | Число готовых сообщений некоторой очереди |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Число неподтверждённых сообщений некоторой очереди |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | Число потребителей некоторой очереди |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Оценка потребления памяти некоторой очередью |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | Суммарное число принятых сообщений очередью |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Суммарное число доставленных сообщений очередью |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Суммарное число подтверждённых сообщений очередью |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Метаданные плагина, value всегда 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | Обслуживает ли плагин (1=enabled, 0=прочее) |

### 3.1 Замечания по использованию (чтобы избежать ошибок)

- **Два семейства рядов с одинаковым именем, но разной кардинальностью**: у `speedmq_queue_messages_unacknowledged` **есть и**
  глобальный ряд без меток, **и** ряд per-queue с метками; при этом на стороне «готовых» глобальный называется `speedmq_queue_messages`, а per-queue — 
  `speedmq_queue_messages_ready` (имена асимметричны). При написании правил используйте `{queue=~".+"}`, чтобы явно брать только семейство per-queue.
- **Трактовка `speedmq_process_memory_bytes`**: реализация — это `MemStats.HeapInuse + StackInuse` (**используемая память**),
  та же трактовка, что и при определении порога памяти ядром; но текст его `# HELP` написан как «число байт памяти, запрошенных у операционной системы», **текст не соответствует фактической трактовке**,
  ориентируйтесь на этот документ.
- **Ряды per-queue появляются только при существовании очереди**: после удаления очереди этот ряд исчезает (на стороне Prometheus он станет stale).
  Оповещения вида «очередь должна существовать, но данных нет» можно дополнить через `absent()` или `or vector(0)` в Grafana.
- **counter обнуляется после перезапуска процесса**: `*_total` — это накопление в пределах процесса, при перезапуске начинается с 0; используйте `rate()`/`increase()`,
  не задавайте пороги напрямую по абсолютным значениям.
- **Кластерные сигналы без метрик**: см. §5.

---

## 4. Смысл оповещений и рекомендуемые действия (соответствуют `prometheus-alerts.yml`)

| Оповещение | Условие срабатывания | Смысл | Рекомендуемые действия |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | Цель сбора полностью недоступна | Проверить процесс/порт/сеть/аутентификацию; перезапустить и посмотреть журнал запуска |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | Сбор успешен, но процесс сам сообщает, что не жив | Подстраховывающий пункт, проверить журнал аварийного завершения |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | Плагин disabled/failed/down | `speedmqctl plugins show <name>` посмотреть `runtime_note`; внешний плагин с `restart=always` обычно восстанавливается сам |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | Сработал порог памяти/диска, производители заблокированы | Проверить порог памяти и свободное место на диске; убедиться, что потребители продвигаются |
| `SpeedMQMemoryWatermarkHigh` | доля используемой памяти > 0.9×порога 10m | Приближение к порогу памяти | Снизить накопление/ускорить потребление, предотвратить блокировку |
| `SpeedMQDiskFreeLow` | свободно < 1.5×нижнего предела диска 10m | Каталог данных почти полон | Расширить/очистить; при достижении нижнего предела производители будут заблокированы |
| `SpeedMQQueueBacklogGrowing` | готовых >10000 и монотонный рост 15m | Очередь постоянно накапливается | Расширить потребителей / проверить потребителя; выявить аномалии dead-letter/TTL |
| `SpeedMQQueueNoConsumers` | потребителей=0 и есть готовые сообщения 15m | Потребления нет | Проверить процесс потребителя; убедиться, что потребитель не отключился |
| `SpeedMQUnackedPileUp` | неподтверждённых >1000 15m | Потребитель завис / не подтверждает ack | Проверить логику обработки у потребителя и prefetch; при необходимости закрыть соединение и переотправить |
| `SpeedMQConnectionSpike` | подключений >10000 10m | Аномальное число подключений | Проверить утечку подключений; клиенты должны переиспользовать соединения |

> Пороги (10000 / 1000 и т. п.) — это **начальные значения**, скорректируйте их под масштаб ваших очередей и особенности нагрузки.

---

## 5. Известный пробел: метрик «потеря большинства / отсутствие leader» для кластера пока нет

- **Факт**: в `/metrics` **нет** никаких кластерных метрик (нет `speedmq_cluster_*`). Состояние кластера есть только в JSON `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Поэтому** в `prometheus-alerts.yml` **намеренно не написаны** оповещения на основе кластерных метрик — даже если бы написали, они **никогда бы не сработали**
  (Prometheus не выдаёт ошибку из-за отсутствия имени метрики), а это была бы поставка, «выглядящая правильно, но фактически неработающая».
- **Способ реализовать самому** (выберите одно из двух; оба нужно строить вне SpeedMQ и они не входят в этот репозиторий):
  1. собирать `/api/cluster` универсальным JSON exporter и отображать в собственные метрики (например, `speedmq_cluster_has_quorum`), затем оповещать по этой метрике;
  2. периодически вызывать `/api/cluster` скриптом-пробником и отправлять оповещение при `has_quorum=false` или `paused=true`.
- Соответствующая трактовка порогов: `has_quorum=false` означает потерю связи с большинством; при `pause_minority` (по умолчанию) в этом случае **обслуживание приостанавливается и соединения разрываются**.

---

## 6. Прочие **непроверенные** пункты

- Дашборд Grafana **не проверялся импортом в реальном Grafana** (пройдена только проверка синтаксиса JSON).
- Правила оповещений **не проверялись загрузкой в реальный Prometheus/Alertmanager** (локально Prometheus не запускался).
  Но **имена метрик в правилах сверены по очереди с реальным выводом** `/metrics` (см. §2/§3), проблемы «неверное имя приводит к тому, что оповещение никогда не срабатывает» нет.
