# SpeedMQ 백업과 복구

> 이 문서의 "실측" 결론은 모두 **Windows + PowerShell 5.1**에서의 실제 리허설(임시 `data_dir`과 임시 포트)에서 나온 것입니다.
> 리허설 명령과 핵심 출력은 §6에 그대로 실었습니다. **【미검증】** 부분은 명시적으로 표시합니다(클러스터 백업/복구, Docker 볼륨 백업 등).

---

## 1. 무엇을 백업해야 하는가

`data_dir` 아래를 **전체 백업해야 합니다**. 핵심은 다음 항목입니다(레이아웃은 `upgrade.md` §3 참고):

| 경로 | 역할 | 분실 시 결과 |
| --- | --- | --- |
| `meta/state.json` | 단일 노드 메타데이터 스냅샷: vhost / 익스체인지 / 큐 / 바인딩 / 사용자 / 권한 / 정책 | 토폴로지와 계정이 모두 유실 |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【클러스터】Raft 로그 / 임기 투표 / 스냅샷+멤버 테이블 | 클러스터 신원과 메타데이터 일관성 유실 |
| `meta/users.seeded`, `meta/vhosts.seeded` | 부트스트랩 마커 | 분실 시 설정의 users/vhosts가 **다시 시딩**됨(삭제한 계정/vhost가 되살아남) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | 클래식 큐 메시지 데이터와 인덱스 | 영속 메시지 유실 |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【클러스터】쿼럼 큐의 Raft 로그/스냅샷 | 쿼럼 큐 데이터 유실 |
| 인증서 파일(설정의 `cert_file`/`key_file`/`ca_file`가 가리키는 PEM) | TLS 인증서 | `data_dir`와 별도로 백업해야 하며, 그렇지 않으면 재시작 후 TLS 기동 불가 |

> 소프트 상태(미확인 메시지, 소비자, prefetch 카운트)는 **메모리에만** 존재하고 디스크에 기록되지 않으며, 백업에 **포함되지 않고** 포함되어서도 안 됩니다.

---

## 2. 일관성 요구 사항: **반드시 프로세스를 먼저 중지**해야 하며, 핫 백업은 **안전하지 않음**

### 2.1 결론

- ✅ **안전한 방법**: **broker 프로세스를 중지**한 뒤(우아한 종료 시 마무리 플러시 수행) `data_dir`을 복사합니다.
- ❌ **핫 백업(프로세스가 실행 중일 때 파일을 직접 복사): 안전하지 않으며 보장하지 않습니다.**

### 2.2 핫 백업이 안전하지 않은 이유

메시지 저장소는 **두 개의 파일**(세그먼트 파일 `*.seg`과 인덱스 파일 `index/*.idx`)이며, 둘은 **원자적으로 커밋되지 않습니다**:

- 복구 시 **인덱스를 기준으로** "어떤 메시지가 살아 있는지" 판단한 뒤, 인덱스의 `(세그먼트 번호, 오프셋, 길이)`에 따라 세그먼트 파일에서 읽습니다.
- 핫 백업 시 **인덱스는 참조하지만 세그먼트 파일은 아직 다 기록되지 않은**(또는 그 반대) 중간 상태를 복사할 수 있습니다:
  - 인덱스가 세그먼트에 없는 레코드를 참조 → 해당 메시지는 **읽기 실패로 건너뛰어짐**(확인된 영속 메시지를 잃은 것과 같음);
  - 세그먼트에는 레코드가 있지만 인덱스가 참조하지 않음 → 해당 메시지는 **복구되지 않음**.
- 복구 시 CRC32로 **꼬리 부분에 반쯤 기록된 레코드**를 버리긴 하지만, 이는 "단일 파일 꼬리 기록 손상"만 커버하며 **인덱스와 세그먼트 간의 불일치는 복구하지 못합니다**.

### 2.3 "쓰기가 언제 디스크에 도달하는가"(실측 관찰)

- 기본 `fsync: os` + `flush_interval_ms: 200`: 메시지는 백그라운드 플러시 코루틴이 **최대 약 200 ms** 내에 운영체제로 `write()`하며
  (fsync는 하지 않음), publisher confirm도 그 이후에 반환됩니다.
- 실측: 영속 메시지를 발행한 후 **즉시** 세그먼트 파일 크기를 조회하면 이미 데이터를 볼 수 있습니다(`t=0ms seg=832`). 즉 "OS가 볼 수 있는 바이트"와 confirm은 거의 동기화됩니다.
- **주의**: 이는 "OS 버퍼에 도달했다"는 것만 의미하며, **프로세스를 강제 종료해도 유실되지 않고**(프로세스가 죽어도 OS 버퍼는 유실되지 않음), **전원이 꺼지면 유실됩니다**.
  "confirm을 받은 즉시 fsync로 디스크에 기록"되길 원하면 `storage.fsync`를 `batch` / `always`로 변경하세요. **【정전 시나리오 미검증】**

---

## 3. 백업 절차

### 3.1 단일 노드(권장)

```powershell
# 1) 프로세스 중지(포그라운드: Ctrl+C, 백그라운드: Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) data_dir 전체 복사(타임스탬프 포함)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (선택) 백업의 메타데이터 스냅샷이 파싱 가능한지 검증
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 클러스터

- **각 노드는 자신의 `data_dir`을 따로 백업합니다**(메타데이터는 Raft로 전체에 복제되고, 메시지 데이터는 Owner 노드에, 쿼럼 큐 복제본은 각자의 Raft 디렉터리에 있습니다).
- 중지 순서: **한 번에 한 노드만 중지**하며, 여러 투표 멤버를 동시에 중지하지 마세요(`upgrade.md` §7.2 참고).
- **클러스터 전체의 일관된 스냅샷**을 얻으려면 모든 노드를 순서대로 중지한 뒤 각각 복사해야 합니다. 프로덕션에서는 "노드별 중지/복사/기동"이 더 흔합니다.
- **【미검증】** 로컬에서 실제 클러스터 백업/복구 리허설을 하지 않았습니다.

### 3.3 Docker(명명 볼륨)

```powershell
# 컨테이너를 중지한 뒤, 일회성 컨테이너로 볼륨 내용을 묶어 복사해 꺼냅니다
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【미검증】**(로컬에서 Docker를 실행하지 않았습니다).

---

## 4. 복구 절차

### 4.1 단일 노드

```powershell
# 1) 프로세스가 중지되었는지 확인
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) 현재 data_dir을 옮기거나 삭제해 신·구 파일이 섞이지 않게 합니다
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) 백업으로 복원
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) 기동
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

핵심:
- **반드시 먼저 이전 디렉터리를 옮겨야** 하며, "백업 파일을 일부 남은 디렉터리 위에 덮어쓰는" 방식은 안 됩니다.
- 복구한 `data_dir`은 백업 시점과 **동일한 vhost/큐 집합**이어야 합니다(디렉터리 이름은 인코딩되어 있어 다른 머신에서도 사용 가능).
- 복구를 기회 삼아 설정 파일의 `vhosts`/`users`를 바꾸지 **마세요**(최초 부트스트랩 시에만 적용되며 바꿔도 소용없습니다. `upgrade.md` §4.2 참고).

### 4.2 클러스터

- 단일 노드 복구: §4.1에 따라 해당 노드의 `data_dir`을 복구한 뒤 기동하면, 기존 멤버로서 다시 합류해 Raft 로그를 따라잡습니다.
- 클러스터 전체 복구: **먼저 다수파 노드를 복구하고 기동해야**(≥ 과반수 투표 멤버) 클러스터가 leader를 선출할 수 있으며, 이후 나머지 노드를 복구합니다.
- **【미검증】** 클러스터 복구는 실측하지 않았습니다.

---

## 5. 복구 후 검증 방법

관리 API와 실제 클라이언트로 교차 확인합니다(전부 수행 권장):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) 객체 총수와 메시지 총수(큐 수/익스체인지 수/바인딩 수/사용자 수; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) 큐별로 messages / messages_ready 확인(백업 전 기록과 대조 가능)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vhost / 사용자 / 정책이 모두 있는지
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) 클러스터(단일 노드는 enabled=false / mode=local / role=single 반환)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **기동 로그 확인**: `已从磁盘恢复队列消息 ... messages=N`와 `队列已恢复持久化消息 ... messages=N`가 나타나야 하며, N은 백업 전과 일치해야 합니다.
- **로그의 경고**: 특정 큐에 이전에 소비/비움된 메시지가 있었다면, 복구 시
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"`와 `恢复时清理了无存活消息的段`가 나타날 수 있습니다.
  이들은 **이미 정산(ack/purge)된 레코드**의 인덱스 잔여물로, **알려진 로그 노이즈이며 데이터 정확성에 영향을 주지 않습니다**(§7 참고).
- **실제 클라이언트**: 큐에서 메시지를 가져와 개수/내용을 확인합니다(§6 (7) 단계 참고).

---

## 6. 실측 리허설(실제 명령과 출력)

> 환경: `data_dir`은 임시 디렉터리, AMQP `127.0.0.1:5676`, 관리면 `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> 기본 계정 `guest/guest`. 기동 로그:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) durable 토폴로지 생성 + 영속 메시지 5건 발행(실제 클라이언트 `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) 사용자 / vhost / 권한 / 정책 생성(관리 API)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) 백업 전 상태(관리 API)**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) 백업 전 디스크 파일**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) 프로세스 중지 → 백업 → 비우기 → 복구**

```
listeners still up: 0                       # 5676/1884/15677 모두 닫힘
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 원래 data_dir을 삭제해 데이터 유실을 시뮬레이션
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) 재시작 후 복구 로그(핵심 라인)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> 동시에 여러 줄의 `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`가 나타납니다.
> 이들은 이번 리허설에서 **이전에 purge된** 메시지가 남긴 인덱스 잔여물(이미 정산되어 세그먼트 데이터는 회수됨)이며, **아래 5건 메시지의 복구에는 영향을 주지 않습니다**.

**(7) 복구 후 검증: 관리 API + 실제 클라이언트**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**결론**: durable 토폴로지(익스체인지+큐+바인딩), 영속 메시지 5건, 사용자, vhost, 권한, 정책이 **모두 복구**되었고,
실제 클라이언트가 원래대로 모든 메시지를 가져올 수 있었습니다. **리허설 통과.**

### 6.1 대조: 소비/purge를 거치지 않은 큐 복구는 더 "조용함"

위 WARN이 일반적인 현상인지 구분하기 위해 **통제 대조**를 한 번 더 수행했습니다: durable 큐 `clean.q`를 새로 만들고 영속 메시지 3건을 발행한 뒤,
**소비도 비우기도 하지 않고** 프로세스를 중지했다가 재시작:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**WARN이 전혀 없습니다**. 이는 WARN이 "인덱스에 아직 정산된 레코드가 남아 있는" 시나리오에서만 나타남을 보여줍니다(§7 참고).

---

## 7. 알려진 문제와 제한(사실 그대로 등록)

1. **복구 로그 노이즈(실제 관찰됨)**: 큐 이력에 소비/비우기가 있었던 경우(메시지가 ack/purge됨),
   그 인덱스에는 회수된 레코드에 대한 참조가 남아 있어, 복구 시 각 항목마다 **`恢复消息失败，已跳过` WARN을 하나씩 출력**하고,
   빈 `000000.seg`를 새로 만들거나 정리합니다(로그 `恢复时清理了无存活消息的段`).
   **데이터 정확성에는 영향을 주지 않지만**(살아 있는 미확인 메시지는 올바르게 복구됨), **로그를 오염시키며** 대형 큐/고처리량에서는 화면을 도배할 수 있습니다.
   권장: `已从磁盘恢复队列消息 ... messages=N`를 기준으로 삼고, 정산된 레코드에 대한 이 WARN들은 무시하세요.
   로그량이 감당할 수 없다면 커널 유지보수자에게 알려 주세요(이 문서는 코드를 수정하지 않습니다).
2. **핫 백업은 안전하지 않음**(§2): 프로세스 실행 중에 `data_dir`을 직접 복사하지 마세요.
3. **`fsync: os`는 정전 시 유실되지 않음을 보장하지 않음**: "confirm 즉시 디스크 기록"을 원하면 `batch` / `always`를 사용하세요.
4. **비밀번호 평문**: `meta/state.json`의 사용자 비밀번호는 **평문**입니다(실측으로 `"password":"drillpass"` 확인 가능) —
   따라서 백업 파일은 **반드시 민감 데이터로 취급**해야 합니다(접근 제어, 암호화 보관). 자세한 내용은 `security-baseline.md` 참고.
5. **【미검증】** 클러스터 백업/복구, Docker 볼륨 백업/복구, 정전 시나리오, 복구 과정 중의 동시 쓰기.
