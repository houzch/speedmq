# SpeedMQ 업그레이드 및 마이그레이션 방안

> 적용 버전: `1.0.0`(`broker.Version`, `/metrics`의 `speedmq_build_info` 참고).
> 이 문서의 모든 "실측" 결론은 로컬 실제 실행에서 나온 것입니다. 실측하지 않은 것은 모두 **【미검증】**으로 명시합니다.
> 로컬 환경: Windows + PowerShell 5.1, Go 1.27.1 windows/386, 임시 `data_dir` + 비기본 포트.

---

## 1. 마이그레이션(RabbitMQ에서 SpeedMQ로 전환)

이 프로젝트는 **AMQP 0-9-1 프로토콜 수준 호환**을 목표로 하므로, "마이그레이션"은 **접속 주소 변경**이 중심입니다:

- 비즈니스 코드는 변경 없이 `host/port/vhost`만 바꾸면 됩니다(설계 문서 G3 "마이그레이션 제로 비용").
- 관리 도구 체인(`rabbitmqadmin`, 관리 UI, 모니터링 스크립트)은 관리면 포트를 가리키면 되며, 인터페이스 형태는 RabbitMQ에 맞춰져 있습니다(`amq.default`, `%2F`, `{error, reason}` 등 규약을 그대로 따름).
- 기본 포트는 RabbitMQ와 동일합니다: AMQP `5672`, 관리면 `15672`; MQTT는 `1883`, 노드 간 RPC는 `25672`.

**마이그레이션 전 스스로 확인해야 할 시맨틱 차이**(모두 이 저장소에서 의도한 것으로, README / 설계 문서 근거):

| 항목 | SpeedMQ 동작 | 마이그레이션 영향 |
| --- | --- | --- |
| 일시적(비영속이며 비독점) 큐 | **선언 거부**(541), `auto_delete`도 예외 없음 | 구형 클라이언트가 이런 큐에 의존하면 실패하므로 durable 또는 exclusive로 변경 필요 |
| 기본 vhost `/` | **삭제 불가**(400), RabbitMQ는 허용 | 자동화 스크립트가 기본 vhost를 삭제하면 실패(이는 유일한 능동적 보안 제약) |
| 클래식 큐 데이터 | **복제하지 않음**, 데이터는 Owner 노드에만 존재 | 노드 간 중복이 필요하면 쿼럼 큐 `x-queue-type=quorum`으로 변경 |
| 쿼럼 큐 | 복제본 확장 지원, **축소 미지원** | 계획 시 한 번에 정확히 산정 |
| 플러그인 | Erlang 플러그인 생태계 없음, AMQP 1.0 / STOMP 미구현 | 이들 프로토콜을 쓰는 시나리오는 당분간 이전 불가 |

**데이터 마이그레이션**: SpeedMQ는 RabbitMQ와 저장 형식이 호환되지 않으며, **온라인/오프라인 데이터 이관 도구를 제공하지 않습니다**.
마이그레이션 방식은 "빈 SpeedMQ 신규 구축 → 이중 운영 검증 → 점진적 트래픽 전환"입니다. **【미검증】** 이 문서에는 실제 RabbitMQ 데이터 이관 리허설이 포함되어 있지 않습니다.

---

## 2. 업그레이드 총칙

1. **먼저 백업**(`backup-restore.md` 참고) — 업그레이드 실패 시의 안전장치.
2. **프로세스를 먼저 중지한 뒤 교체**(데이터 디렉터리에 단일 작성자 제약이 있음, §4.2 참고).
3. **업그레이드 후 반드시 검증**: 프로세스 기동 가능, `/api/overview` 조회 가능, `/metrics` 스크레이프 가능, 큐 메시지 수가 백업 전과 일치.
4. 클러스터 업그레이드는 **노드별 롤링**으로, 한 번에 한 노드만 대상으로 합니다(§5 참고).

---

## 3. 데이터 디렉터리 레이아웃(업그레이드/마이그레이션의 사실 근거)

로컬 단일 노드 인스턴스에서 **실측**한 `data_dir` 레이아웃:

```
data/
├── meta/
│   ├── state.json        # 단일 노드 모드의 메타데이터 스냅샷(vhost/익스체인지/큐/바인딩/사용자/권한/정책)
│   ├── users.seeded      # 부트스트랩 마커: 설정 파일의 users가 이미 시딩됨
│   ├── vhosts.seeded     # 부트스트랩 마커: 설정 파일의 vhosts가 이미 시딩됨
│   ├── raft.state        # 【클러스터 모드】Raft 임기/투표
│   ├── raft.log          # 【클러스터 모드】Raft 로그
│   └── snapshot.json     # 【클러스터 모드】Raft 스냅샷 + 멤버 테이블
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # 세그먼트 파일(메시지 본문 + 속성), 레코드 형식: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # 큐 인덱스: seq-id → (세그먼트 번호, 세그먼트 내 오프셋, 길이, 상태)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【클러스터】쿼럼 큐는 큐마다 하나의 Raft 그룹(로그/스냅샷)
```

**주의(직관과 다른 두 가지, 모두 코드/실측 기준)**:

- 클러스터 모드에서 Raft 영속화 파일은 **`meta/` 아래에 직접 위치**하며(`raft.state` / `raft.log` / `snapshot.json`),
  **`meta/raft/` 하위 디렉터리는 존재하지 않습니다**. 근거: `internal/raft/log.go`의 파일명 상수 + `internal/broker/cluster.go`의
  `Dir: filepath.Join(b.cfg.DataDir, "meta")`. **【검증 완료（2026-10-10, 로컬 Docker Desktop 3 노드 클러스터）】** —— 클러스터 노드 볼륨 백업 내용에서 `meta/raft.log`, `meta/raft.state`, `meta/*.seeded`가 직접 보이며(`meta/raft/` 하위 디렉터리는 보이지 않음).
- 디렉터리 이름은 **원래 vhost / 큐 이름이 아니라** `store.SafeDirName` 인코딩입니다: 접두사 `q_`를 붙이고, `[A-Za-z0-9._-]`가 아닌 바이트는 `%XX`로 이스케이프합니다.
  실측: vhost `/` → 디렉터리 `q_%2F`, 큐 `persist.q` → 디렉터리 `q_persist.q`.
  이렇게 설계한 이유는 경로 탈출과 Windows 예약 장치 이름(`con`/`nul` 등)을 피하기 위해서입니다.

---

## 4. 데이터 호환성

### 4.1 구형 데이터를 직접 읽을 수 있는가 — 가능

- **인덱스 형식은 상위 호환**: M8-1에서 인덱스 레코드에 "세그먼트 번호" 필드(25 바이트)를 추가했으며, **구형 형식(21 바이트, 세그먼트 번호 없음)도 그대로 읽을 수 있습니다**.
  읽을 때 "세그먼트가 하나뿐(seg=1)"인 것과 동일하게 취급되며, **업그레이드에 마이그레이션 스크립트가 필요하지 않습니다**.
  근거: `internal/store/store.go`의 `indexEntrySize` / `legacyIndexEntrySize` 상수와 `recover()` 로직; README M8-1.
- **크래시 시맨틱 불변**: 각 레코드에 길이 접두사 + CRC32가 있으며, 복구 시 **꼬리 부분의 반쯤 기록되거나 손상된 레코드를 버리고** 잘라냅니다.
  실측(`backup-restore.md` §6 참고): 프로세스를 중지한 뒤 재시작하면 durable 큐의 영속 메시지 5건이 **모두 복구**되고,
  로그에 `已从磁盘恢复队列消息 ... messages=5`가 나타납니다.

**【검증 완료（2026-10-10, 로컬）】** 메이저 버전 간 읽기를 실측했습니다: tag `v1.1.0`으로 기록한 durable 큐 데이터가 `1.1.05`에서 그대로 복구됩니다(명령과 출력은 §5 참고).

### 4.2 설정의 `vhosts` / `users` 규칙(업그레이드 시 가장 빠지기 쉬운 함정)

- 둘 다 **최초 부트스트랩 시에만 적용**됩니다: 최초 기동 시 설정의 vhosts/users를 메타데이터에 기록하고 마커 파일
  `meta/vhosts.seeded` / `meta/users.seeded`를 남기며, **이후에는 메타데이터가 기준**이 됩니다.
- 따라서 **업그레이드/설정 교체 시 설정 파일을 수정해 계정이나 vhost를 추가/삭제하려 해서는 안 됩니다** — 바꿔도 적용되지 않습니다.
  관리 API나 `speedmqctl`을 사용하세요.
- 반대로 업그레이드가 설정으로 기존 계정을 **덮어쓰지 않습니다**: 런타임에 변경한 비밀번호는 재시작해도 설정의 이전 값으로 되돌아가지 않고,
  런타임에 삭제한 계정도 되살아나지 않습니다. 근거: `cluster.go`의 시딩 마커 로직; README M8-4 / M8-7.

**【검증 완료（2026-10-10, 로컬）】** 런타임 변경은 실제로 설정으로 덮어쓰이지 않습니다: 업그레이드 전 관리 API로 `guest` 비밀번호를 `rotated`로 변경하고, `1.1.05` + 동일한 `data_dir`로 재시작한 뒤 `guest/guest`(설정의 이전 비밀번호)는 **401**을, `guest/rotated`는 **200**을 반환했습니다.

### 4.3 세그먼트 로테이션과 디스크 회수

- 메시지는 크기 기준으로 세그먼트화되며(기본 8 MiB), **세그먼트 내 메시지가 모두 ack되고 세그먼트가 봉인되면 세그먼트 전체를 삭제**하고 인덱스도 함께 압축 재작성됩니다.
- 업그레이드는 이 동작을 변경하지 않으며, 구형 인스턴스가 남긴 단일 세그먼트 파일도 새로운 세그먼트 로테이션 로직에서 정상 동작합니다.

---

## 5. 바이너리 업그레이드(베어메탈)

> **【검증 완료（2026-10-10, 로컬）】** 메이저 버전 간 실장비 리허설: tag `v1.1.0`으로 빌드한 구형 바이너리로 durable 익스체인지/큐/바인딩 + 영속 메시지 5건 + vhost/계정/권한/정책을 기록하고 런타임에 비밀번호를 변경; 프로세스 중지 후 현재 `1.1.05` 바이너리 + **동일한 `data_dir` + 동일 포트**로 기동 —— 프로세스가 정상 기동되고, `/api/queues`의 객체와 메시지 수가 일치하며, 실제 AMQP 클라이언트가 영속 메시지 5건을 모두 가져오고, `/metrics`가 `speedmq_build_info{version="1.1.05"}`를 보고합니다. 아래 단계는 여전히 범용 절차로 임의의 인접 버전에 적용할 수 있습니다; **다운그레이드**는 §8 참고.

### 5.1 단계

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) 프로세스 중지(우아한 종료 시 마무리 플러시 수행; §4 "일관성" 참고)
#    포그라운드로 실행 중이면 Ctrl+C, 서비스 방식이면 Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) 데이터 디렉터리 백업(반드시 프로세스 중지 후)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) 바이너리 교체(새 버전 speedmqd.exe / speedmqctl.exe를 원래 경로에 배치)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) 기동
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) 검증: 프로세스 생존 + 관리 API 조회 가능
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 업그레이드 후 검증 체크리스트

- 기동 로그에 `SpeedMQ 启动中 ... version=<新版本>`와 `管理面已启动`가 나타남;
- `/api/overview`의 `object_totals` / `queue_totals`가 백업 전과 일치(`backup-restore.md` §5 대조);
- `/api/queues`에서 각 durable 큐의 `messages` / `messages_ready`가 백업 전과 일치;
- `/metrics`를 스크레이프할 수 있고 `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. 이미지 업그레이드(컨테이너)

이미지는 약 13 MB(정적 링크 바이너리 + alpine)이며, **비 root(uid 10001)로 실행**되고 데이터 디렉터리는 `/var/lib/speedmq`에 마운트됩니다.

```powershell
# 1) 새 이미지 가져오기/빌드(tag는 새 버전 번호를 사용해 old/new 혼동 방지)
docker build -t speedmq:1.0.0 .

# 2) 구형 컨테이너 중지(compose는 명명 볼륨 speedmq-data를 유지)
docker compose down

# 3) 새 버전 기동(compose 파일에서 image를 새 tag로 변경)
docker compose up -d

# 4) 상태와 로그
docker compose ps
docker compose logs -f --tail 100
```

> **컨테이너 내 일회성 작업**(예: 컨테이너 내에서 `speedmqctl` 실행): `docker compose ...`의 `run`은 비대화형 환경에서 반드시 `-T`를 붙여야 하며,
> 그렇지 않으면 TTY 할당 실패로 오류가 납니다:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

데이터 영속화는 compose의 **명명 볼륨** `speedmq-data`에 의존하며, 컨테이너를 재생성해도 데이터가 유실되지 않습니다(M4부터 실제 디스크 기록).
업그레이드 전에 볼륨 내용을 백업해야 한다면 `/var/lib/speedmq`를 백업하는 것과 동일합니다(`backup-restore.md` §3.2 참고).

> **【검증 완료（2026-10-10, 로컬 Docker Desktop）】** 실제 이미지 업그레이드 리허설을 수행했습니다: 로컬에서 구형 이미지 `speedmq:1.1.0-old`(git tag `v1.1.0`)와 새 이미지 `speedmq:1.1.05-new`(현재 소스)를 빌드하고, 단일 노드 compose로 구형 이미지를 기동 → durable 큐 `persist.q`를 생성해 영속 메시지 5건을 발행(`delivery_mode=2`) → `docker compose down`(**`-v`를 붙이지 않아** 명명 볼륨 `speedmq-data` 유지) → compose의 image tag를 새 이미지로 바꾼 뒤 `docker compose up -d`.
> 결과: 새 컨테이너가 정상 기동하고, 기동 로그에 `已从磁盘恢复队列消息 ... queue=persist.q messages=5`가 나타나며, `/metrics`가 `speedmq_build_info{version="1.1.05"}`를 보고하고, `/api/overview`의 `queue_totals.messages=5`와 큐 `persist.q`의 `messages=5`가 업그레이드 전후로 일치했으며, 실제 AMQP 클라이언트(`rabbitmq/amqp091-go`)가 영속 메시지 5건을 하나씩 모두 가져왔습니다(`deliveryMode=2`).
> 주석: `v1.1.0`은 개명 전 `speedmq` 브랜드 이미지(바이너리 `speedmqd`, 지표 `speedmq_build_info`, 기본 데이터 디렉터리 `/var/lib/speedmq`)로 현행 버전과 명명이 다릅니다; 리허설에서는 설정 `data_dir=/var/lib/speedmq`를 기준으로 하고 명명 볼륨에 소유자를 미리 설정했습니다(구형 이미지 내에는 이 쓰기 가능 경로가 없음). 이는 "tag를 바꿔 컨테이너를 재생성해도 명명 볼륨 데이터는 유실되지 않는다"는 결론에 영향을 주지 않습니다.

---

## 7. 점진적 배포와 롤백

### 7.1 단일 노드

- **점진적 배포**: SpeedMQ 단일 노드에는 "신·구 버전 동시 프로세스" 능력이 내장되어 있지 않습니다. 가능한 방식은 **우회 섀도**입니다:
  새 버전 인스턴스를 먼저 **읽기 전용 소비/섀도 큐**로 동일한 업스트림 트래픽에 연결해 관찰하고, 이상이 없으면 작성자를 전환합니다.
- **롤백**:
  1. 새 버전 프로세스 중지;
  2. 구형 바이너리로 되돌림;
  3. 새 버전이 이미 데이터를 기록했다면 **업그레이드 전 백업으로 `data_dir`을 복구해야 합니다**(아래 참고).
  "새 버전이 이미 기록했는데 구 버전이 그대로 읽는" 보장은 **없습니다** — 버전 간 다운그레이드는 §8 참고.

### 7.2 클러스터(롤링 업그레이드)

플랫폼 쪽에서 "원클릭 롤링 업그레이드"를 제공하지 않으므로, 아래 순서대로 사람이 노드를 하나씩 조작해야 합니다:

1. **한 번에 한 노드만 업그레이드**: 해당 노드 중지 → `data_dir` 백업 → 바이너리 교체 → 기동 → 재합류 및 따라잡기 대기
   (`speedmqctl cluster_status` / `GET /api/cluster`에서 `role`, `commit_index`/`last_applied` 확인).
2. **순서 권장**: 먼저 **learner / 비투표 멤버**를 올리고(다수파에 영향 없음), 그다음 **follower**, 마지막으로 **leader**를 올립니다
   (leader 업그레이드는 선출을 한 번 유발하며, 그동안 잠시 쓰기가 불가능).
3. **중지가 다수파에 미치는 영향**(핵심):
   - 3 노드 클러스터: **동시에 최대 1개**의 투표 멤버만 중지 가능하며, 2개를 중지하면 다수파를 잃어 `pause_minority`에서 **클러스터 전체가 서비스를 일시 중단**합니다.
   - 2 노드 클러스터: 1개만 중지해도 다수파를 잃어 **롤링 업그레이드 능력이 없습니다**(최소 3 노드 권장).
   - 따라서 롤링 업그레이드 시 **여러 투표 멤버를 한 번에 중지하는 것은 엄금**합니다.
4. **멤버 변경과 업그레이드를 함께 하지 마세요**: 멤버 변경에는 **joint consensus가 없으며**, 한 번에 하나의 미커밋 설정 변경만 허용됩니다.
   업그레이드 중에는 `add_member` / `remove_member`를 동시에 수행하지 마세요.
5. 업그레이드 완료 후 `GET /api/cluster`의 `object_totals`가 업그레이드 전과 일치하는지 확인합니다.

> **【검증 완료（2026-10-10, 로컬 Docker Desktop 3 노드 클러스터）】** 실제 클러스터의 **버전 간** 롤링 업그레이드 리허설을 수행했습니다: 베이스라인은 로컬에서 빌드한 `speedmq:1.1.0-old`(git tag `v1.1.0`)이며, 노드별(n1 → n2 → n3, 한 번에 하나: 먼저 중지, 그다음 명명 볼륨 백업, 그다음 이미지 교체 후 기동)로 현재 `1.1.05`까지 업그레이드했습니다(로컬에서 `speedmq:1.1.05-new` 빌드, compose는 노드별 override로 이미지 전환). 전 과정에서 클러스터는 항상 leader와 다수파를 유지했습니다 —— 현재 leader를 중지할 때 약 0.6 s 만에 새 leader가 선출되었고, `pause_minority`가 일시 중단을 트리거하지 않았으며, 중지 기간에 외부 중단이 없었습니다; 업그레이드 후 `GET /api/cluster`의 `object_totals`가 이전과 일치했고(`queues:1`), 3 복제본 쿼럼 큐 `qroll`의 영속 메시지 25건 수가 그대로이며 하나씩 읽어낼 수 있고, 새 메시지 5건을 추가 발행한 뒤 총 30건을 모두 가져올 수 있었습니다.
> 위 순서는 Raft의 일반적 제약으로 여전히 성립하며, 이번 리허설은 3 노드 시나리오를 대상으로 합니다. **【검증 완료（2026-10-10, 로컬 Docker Desktop）2 노드에서 1개 중지하면 다수파 상실】** 별도로 최소 2 노드 클러스터(n1/n2, 각 `peers`에 이 두 노드만 포함, `partition_policy=pause_minority`)를 구성해 실측했습니다: 두 노드가 모두 온라인이면 `GET /api/cluster`는 `has_quorum=true / paused=false`(n1 leader, n2 follower); n2를 `docker stop`하면 n1이 약 2 s 내에 `has_quorum=false / paused=true`가 됩니다 —— 즉 투표 멤버 1개만 중지해도 다수파를 잃고 `pause_minority`가 서비스를 일시 중단하며, **2 노드는 롤링 업그레이드 능력이 없음을 입증**합니다; `docker start`로 n2를 복구하면 n1이 약 2 s 내에 `has_quorum=true / paused=false`로 돌아옵니다(서비스 자동 복구).

> **【검증 완료（2026-10-11, 로컬 Docker Desktop 3 노드 클러스터）】네트워크 분할 상황의 롤링 업그레이드 실측**(`v1.1.0` → `1.1.05` 버전 간, 분할은 이중 네트워크 토폴로지 사용: 노드 간 Raft RPC는 정적 IP의 `cluster-net`만 경유하고, 클라이언트/관리면은 `client-net`을 경유하며, `docker network disconnect/connect`로 분할을 전환):
> - **분할된 follower**: 다수파 측은 분할 기간 내내 quorum을 유지하며 정상적으로 발행/소비했고, 분할이 지속되는 상태에서 다수파 노드 하나의 "중지→이미지 교체→기동→따라잡기"를 완료했습니다; 혼합 버전(1.1.0 + 1.1.05) 클러스터도 서비스 가능했습니다. 분할 복구 후 격리된 노드(구 버전)가 다시 합류해 따라잡았고, 메시지는 고유 ID 집합 대사에서 35/35 유실 없음이었습니다.
> - **분할된 leader**: 다수파는 약 **630 ms** 만에 새 leader를 선출했습니다; 격리된 구 leader는 `role=leader`를 유지하지만 `has_quorum=false / paused=true`였습니다(`pause_minority` 적용, 스스로 임기를 올리지 않음); 분할 중 다수파 측 다른 노드의 업그레이드에 성공했습니다; 복구 후 구 leader가 다시 합류했으며 split-brain이나 데이터 분기는 없었습니다.
> - **PreVote가 없을 때의 예상 동작(사실대로 기록)**: 격리된 follower는 즉시 반복적으로 선출을 시도해 임기가 약 2초마다 +2씩 올랐습니다(메타데이터와 큐 두 Raft 그룹이 번갈아 상승); 복구 순간 높은 임기로 **leader 자리를 빼앗았고**(실측 1.4 s 내 당선) —— leader 전환 소동은 있지만 정확성 문제는 없었습니다.
> - **두 가지 운영 함정**: ① `peers`에 호스트명을 쓰고 컨테이너가 도달 가능한 두 번째 네트워크에도 연결되어 있으면 Raft 트래픽이 우회해 "가짜 분할"이 생깁니다(격리된 노드가 1분 넘게 `has_quorum=true` 유지) —— 노드 간 RPC는 반드시 한 네트워크만 경유해야 합니다(peers에 해당 네트워크의 정적 IP 직접 지정); ② 복구 시 `docker network connect`에 반드시 `--ip <원래 IP>`를 붙여야 하며, 그렇지 않으면 노드가 새 IP를 받은 뒤에도 다수파는 이전 주소로 계속 접속하고, 격리된 노드는 뒤처진 로그를 가진 채 부풀려진 임기로 반복 선출을 시도합니다(실측에서 클러스터 term이 320에서 488로, 메타데이터 commit이 13에서 97로 치솟음). leader가 `quorum=true`이면서도 잠시 `paused=true`가 되는 일시 상태도 나타날 수 있습니다.
> - 분할 기간 동안 소수파 측 관리 API는 읽기 가능(관측에 유용)하고, AMQP 발행은 거부됩니다(`pause_minority` 적용).

---

## 8. 지원하지 않음 / 미검증 부분(명시적으로 나열)

- **메이저 버전 간 다운그레이드: 미지원, 미검증**. 새 버전이 새 형식/새 시맨틱으로 데이터를 기록했다면 "구형 바이너리로 되돌려 그대로 읽는" 보장은 **없으며**,
  롤백은 업그레이드 전 백업에만 의존할 수 있습니다.
- **설정 형식 불변**: 여전히 JSON + `SPEEDMQ_*` 환경 변수입니다. **YAML 설정은 아직 미지원**(파서 의존성 도입 필요, M8-17 검토 예정)이며,
  업그레이드로 YAML이 생기지 않습니다.
- **온라인 플러그인/프로토콜 핫 업그레이드**: 플러그인은 커널과 함께 컴파일되거나(A 형태) 설정에 따라 `spawn`으로 기동되며(B 형태),
  커널 업그레이드 = 프로세스 재시작입니다. 제자리에서 바이너리를 핫 교체하는 메커니즘은 **없습니다**.
- **저장 엔진 제자리 마이그레이션**: 세그먼트 로테이션/인덱스 압축은 런타임 백그라운드 동작이며, 독립적인 "데이터 마이그레이션/압축" 명령은 **없습니다**.
- **디스크 가득 참 상황의 클러스터 업그레이드**: **미검증**. (네트워크 분할 상황의 롤링 업그레이드는 2026-10-11에 실측했습니다. §7 끝의 주석 참고.)
- 이 문서는 SpeedMQ와 다른 broker(RabbitMQ) 간 데이터 이관 검증을 **포함하지 않습니다**.
