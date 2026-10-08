# SpeedMQ 모니터링과 알림

이 디렉터리는 바로 사용할 수 있는 모니터링 템플릿을 제공합니다:

| 파일 | 역할 |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus 알림 규칙(`groups: - name: speedmq`) |
| `grafana-dashboard.json` | 가져올 수 있는 Grafana 대시보드(패널이 아래 핵심 신호를 커버) |
| `README.md` | 사용법, 지표 목록, 각 알림의 의미와 대응, 알려진 결함 |

---

## 1. 사용 방법

### 1.1 스크레이프(Prometheus)

관리면(기본 `:15672`)이 Prometheus 텍스트 형식 `/metrics`를 노출하며, **Basic Auth가 필요합니다**:

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

> 모니터링 전용으로 읽기 전용 계정을 따로 만드는 것을 권장합니다(`monitoring` 태그만으로 지표를 읽을 수 있음). 관리자 비밀번호를 재사용하지 마세요.

스크레이프가 정상인지 검증(PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 알림 규칙

`prometheus-alerts.yml`을 Prometheus의 규칙 디렉터리에 넣고, `prometheus.yml`에서 참조한 뒤 reload합니다:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

규칙에서는 일관되게 `job="speedmq"`를 사용합니다. job 이름이 다르면 전체에서 바꾸세요.

### 1.3 Grafana 대시보드

`grafana-dashboard.json`은 **Dashboards → Import → JSON 업로드**로 가져오며, 가져올 때 사용할 Prometheus 데이터 소스를 선택합니다
(대시보드에서는 `${DS_PROMETHEUS}` 변수로 참조). 템플릿 변수 `DS_PROMETHEUS`는 가져오기 매핑에서 값이 할당됩니다.

**【미검증】** 로컬에서 Grafana 인스턴스를 띄우지 않아 실제 가져오기 검증을 하지 않았으며, 해당 JSON은 JSON 문법 검증만 수행했습니다(패널 14개, 파싱 통과).

---

## 2. `/metrics` 실제 일부(증거)

다음은 로컬 `1.0.0` 인스턴스 `/metrics`의 **실제 출력**입니다(durable 큐 `persist.q`를 하나 만들어서
`vhost`/`queue` 레이블이 있는 per-queue 지표가 나타났습니다):

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

## 3. 지표 목록(모두 실제 존재, 출처 `internal/management/metrics.go`)

| 지표 | 유형 | 레이블 | 시맨틱 |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | 프로세스가 스스로 보고하는 생존 여부(현재 항상 1) |
| `speedmq_build_info` | gauge | `version`,`node` | 빌드 정보, value는 항상 1 |
| `speedmq_resource_blocked` | gauge | — | 자원 워터마크가 프로듀서를 차단했는지(1=차단 중) |
| `speedmq_connections` | gauge | — | 현재 연결 수 |
| `speedmq_channels` | gauge | — | 현재 채널 수 |
| `speedmq_queues` | gauge | — | 현재 큐 수 |
| `speedmq_exchanges` | gauge | — | 현재 익스체인지 수 |
| `speedmq_consumers` | gauge | — | 현재 소비자 수 |
| `speedmq_queue_messages` | gauge | — | **전역** 준비 메시지 총수 |
| `speedmq_queue_messages_unacknowledged` | gauge | — | **전역** 미확인 메시지 총수 |
| `speedmq_process_memory_bytes` | gauge | — | 프로세스 **사용 중** 메모리(`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | 물리 메모리 총량 |
| `speedmq_disk_free_bytes` | gauge | — | 데이터 디렉터리 여유 공간 |
| `speedmq_memory_high_watermark` | gauge | — | 메모리 워터마크 비율 |
| `speedmq_disk_free_limit_bytes` | gauge | — | 디스크 잔여 하한 |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | 특정 큐의 준비 메시지 수 |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | 특정 큐의 미확인 메시지 수 |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | 특정 큐의 소비자 수 |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | 특정 큐의 메모리 사용 추정 |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | 큐가 누적 수신한 메시지 수 |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | 큐가 누적 전달한 메시지 수 |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | 큐가 누적 확인한 메시지 수 |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | 플러그인 메타데이터, value는 항상 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | 플러그인이 서비스 중인지(1=enabled, 0=기타) |

### 3.1 사용 시 주의(잘못 작성 방지)

- **이름은 같지만 기저가 다른 두 계열**: `speedmq_queue_messages_unacknowledged`는 **전역 무레이블 계열도** 있고
  **per-queue 레이블 계열도** 있습니다. 반면 "준비" 쪽은 전역이 `speedmq_queue_messages`, per-queue가
  `speedmq_queue_messages_ready`입니다(이름이 비대칭). 규칙을 작성할 때는 `{queue=~".+"}`로 per-queue 계열만 명확히 선택하세요.
- **`speedmq_process_memory_bytes`의 규칙**: 구현은 `MemStats.HeapInuse + StackInuse`(**사용 중 메모리**)이며,
  커널 메모리 워터마크 판정과 같은 규칙입니다. 다만 `# HELP` 문구는 "운영체제에 요청한 메모리 바이트 수"라고 적혀 있어 **문구가 실제 규칙과 부합하지 않으므로**,
  이 문서를 기준으로 하세요.
- **per-queue 계열은 큐가 존재할 때만 나타남**: 큐를 삭제하면 해당 계열이 사라집니다(Prometheus 쪽에서는 stale이 됨).
  "큐가 있어야 하는데 데이터가 없음"에 관한 알림은 `absent()` 또는 Grafana의 `or vector(0)`와 함께 사용할 수 있습니다.
- **counter는 프로세스 재시작 후 0으로 초기화**: `*_total`은 프로세스 내 누적이라 재시작하면 0부터 시작합니다. `rate()`/`increase()`를 사용하고,
  절댓값에 직접 임계값을 설정하지 마세요.
- **metrics에 없는 클러스터 신호**: §5 참고.

---

## 4. 알림 의미와 권장 대응(`prometheus-alerts.yml`에 대응)

| 알림 | 트리거 조건 | 의미 | 권장 대응 |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | 스크레이프 대상 전체가 도달 불가 | 프로세스/포트/네트워크/인증 확인, 재시작 후 기동 로그 확인 |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | 스크레이프는 성공했지만 프로세스가 스스로 비생존 보고 | 안전망 항목, 비정상 종료 로그 확인 |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | 플러그인이 disabled/failed/down | `speedmqctl plugins show <name>`로 `runtime_note` 확인. 외부 플러그인은 `restart=always`로 대개 자가 복구됨 |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | 메모리/디스크 워터마크 도달, 프로듀서 차단됨 | 메모리 워터마크와 디스크 여유 확인, 소비자가 진행 중인지 확인 |
| `SpeedMQMemoryWatermarkHigh` | 사용 중 메모리 비율 > 0.9×워터마크 10m | 메모리 워터마크에 근접 | 적체 감소/소비 속도 향상으로 차단 유발 방지 |
| `SpeedMQDiskFreeLow` | 여유 < 1.5×디스크 하한 10m | 데이터 디렉터리가 거의 가득 참 | 용량 확장/정리; 하한에 도달하면 프로듀서 차단 |
| `SpeedMQQueueBacklogGrowing` | 준비 >10000 이고 15m 동안 단조 증가 | 큐에 적체가 지속 | 소비자 확장 / 소비 측 확인, 데드레터/TTL 이상 점검 |
| `SpeedMQQueueNoConsumers` | 소비자=0 이고 준비 메시지 존재 15m | 소비하는 쪽이 없음 | 소비 측 프로세스 확인, 소비자가 끊기지 않았는지 확인 |
| `SpeedMQUnackedPileUp` | 미확인 >1000 15m | 소비자가 멈춤/ack 안 함 | 소비자 처리 로직과 prefetch 확인, 필요 시 연결을 닫고 재전달 |
| `SpeedMQConnectionSpike` | 연결 >10000 10m | 연결 수 이상 | 연결 누수 확인, 클라이언트는 연결을 재사용해야 함 |

> 임계값(10000 / 1000 등)은 **출발점 값**이며, 큐 규모와 비즈니스 특성에 맞게 조정하세요.

---

## 5. 알려진 결함: 클러스터 "다수파 상실 / leader 없음"에 대한 지표가 현재 없음

- **사실**: `/metrics`에는 클러스터류 지표가 **전혀 없습니다**(`speedmq_cluster_*` 없음). 클러스터 상태는 `GET /api/cluster`의 JSON에만 있습니다:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **따라서** `prometheus-alerts.yml`에는 클러스터 지표 기반 알림을 **의도적으로 쓰지 않았습니다** — 써도 **절대 트리거되지 않으며**
  (Prometheus는 지표 이름이 없다고 오류를 내지 않음), 이는 "맞아 보이지만 실제로는 무효"인 결과물이기 때문입니다.
- **자체 구축 방안**(둘 중 하나, 모두 SpeedMQ 외부에 구축해야 하며 이 저장소 범위 밖):
  1. 범용 JSON exporter로 `/api/cluster`를 스크레이프해 사용자 정의 지표(예: `speedmq_cluster_has_quorum`)로 매핑한 뒤, 그 지표에 알림;
  2. 프로브 스크립트로 주기적으로 `/api/cluster`를 호출해 `has_quorum=false` 또는 `paused=true`일 때 알림 발생.
- 관련 임계값 규칙: `has_quorum=false`는 다수파와의 연결 상실을 의미하며, `pause_minority`(기본)에서는 이때 **서비스가 일시 중단되고 연결이 끊깁니다**.

---

## 6. 기타 **미검증** 항목

- Grafana 대시보드는 **실제 Grafana에서 가져오기 검증을 하지 않았습니다**(JSON 문법 검증만 통과).
- 알림 규칙은 **실제 Prometheus/Alertmanager에서 로드 검증을 하지 않았습니다**(로컬에서 Prometheus를 띄우지 않음).
  다만 규칙의 **지표 이름은 `/metrics` 실제 출력과 하나씩 대조**했으므로(§2/§3 참고), "이름 오기로 절대 트리거되지 않는" 문제는 없습니다.
