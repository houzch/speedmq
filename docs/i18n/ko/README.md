<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | **한국어** | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

Go로 작성된 **RabbitMQ 호환** 메시지 미들웨어입니다. 기존 RabbitMQ 클라이언트는 **코드 수정도, SDK 교체도 없이** 접속 주소만 바꾸면 바로 연결할 수 있습니다.

## 소개

- **프로토콜 호환**: AMQP 0-9-1(RabbitMQ 확장 포함)과 MQTT 3.1.1이며, 호환 기준선은 **RabbitMQ 4.3 시맨틱**입니다.
- **간단한 배포**: 바이너리 하나 / 컨테이너 하나면 되고, 관리 UI가 내장되어 있어 별도의 Nginx, 데이터베이스, Node 런타임이 필요 없습니다.
- **충분한 운영 기능**: 관리 UI(큐 / 익스체인지 / 연결 / 계정 권한 / 가상 호스트 / 정책 / 제한 / 클러스터), Prometheus `/metrics`, 명령줄 `swiftmqctl`.
- **기본 포트**: `5672`(AMQP), `1883`(MQTT), `15672`(관리 UI / HTTP API / 지표).

이미 갖춘 기능: 영속화(세그먼트 로그 + fsync 등급 + 크래시 복구), 발행 확인, TTL / 데드레터 / 길이 제한, 소비자 우선순위, Direct Reply-To, 클러스터(Raft 메타데이터 + 쿼럼 큐 + 노드 간 포워딩), 플러그인 핫 시작/정지.

***

## 빠른 시작

### 방법 1: Docker(권장)

**저장소를 clone하지 않고 이미지를 바로 받아 실행할 수 있습니다:**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.1.0
```

이미지는 두 곳에 동일하게 배포됩니다(더 빠른 쪽을 사용하세요): Docker Hub `houzch/swiftmq`, GitHub GHCR `ghcr.io/houzch/swiftmq`. 두 곳 모두 `linux/amd64`와 `linux/arm64`를 제공합니다.

- 데이터는 이름 있는 볼륨 `swiftmq-data`에 저장되어 컨테이너를 다시 만들어도 유지됩니다.
- 중지/삭제: `docker stop swiftmq`, `docker rm swiftmq`(데이터 볼륨은 유지됩니다).

**설정을 바꾸거나 compose로 운영하려면 저장소를 clone하세요:**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # 게시된 이미지 사용. 로컬에서 빌드하려면 up -d --build 로 변경

docker compose ps        # 상태가 Up (healthy) 여야 합니다
docker compose logs -f   # 로그 따라가기
```

- 설정은 `configs/swiftmqd.json`을 읽기 전용으로 마운트하며, 수정 후 `docker compose restart` 로 반영됩니다.
- 중지: `docker compose down`(데이터 유지), `docker compose down -v`(데이터까지 삭제).

### 방법 2: 로컬 바이너리(Go 1.24+ 필요)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> 관리 UI 빌드 산출물은 저장소에 포함되지 않습니다. UI를 사용하려면 먼저 `web/`에서 `npm ci && npm run build`를 실행하세요.
> 빌드하지 않아도 정상적으로 기동해 메시지를 주고받을 수 있으며, 다만 `/`에 접속하면 "관리 UI가 빌드되지 않음"이라는 안내가 표시됩니다.

### 최초 로그인(반드시 기본 계정을 먼저 변경하세요)

| 진입점 | 주소 / 자격 증명 |
| --- | --- |
| 관리 UI | <http://localhost:15672/>(사용자 이름 `guest`, 비밀번호 `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883`(계정은 위와 동일) |

새로 설치한 인스턴스의 총괄 계정에는 "최초 로그인 시 비밀번호 강제 변경" 표시가 있습니다. 관리 UI로 로그인하면 **계정 이름과 비밀번호를 동시에 변경하도록 강제**되며, 변경을 마쳐야 백엔드에 진입할 수 있습니다.

API를 직접 호출해 처리할 수도 있습니다(자동화에 적합):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ 기본 `guest/guest`는 RabbitMQ 동작과 동일하게 **로컬 호스트에서만 로그인할 수 있습니다**. 컨테이너 외부 / 원격 연결은 설정에서 해당 사용자에게 `remote_access`를 활성화해야 합니다(예시 설정은 컨테이너 시나리오에 맞게 이미 활성화되어 있습니다).
> **서비스가 외부에서 접근 가능해지는 즉시 자격 증명을 교체하세요.**

### 애플리케이션 연결(접속 주소만 변경하면 됨)

```python
# Python(pika)
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go（amqp091-go）
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT(mosquitto 클라이언트)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

관리 HTTP API는 `rabbitmqadmin`과 호환되며, 관리 UI의 "큐 / 익스체인지 추가"가 곧 표준 선언 엔드포인트이므로 스크립트로도 동일하게 할 수 있습니다:

```bash
# 큐 선언(쿼럼 큐는 arguments: {"x-queue-type":"quorum"}로 표현)
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### 일상 운영

| 항목 | 진입점 |
| --- | --- |
| 관리 UI | <http://localhost:15672/>: 큐 / 익스체인지 / 연결 / 계정 권한 / 가상 호스트 / 정책 / 제한 / 기능 플래그 / 클러스터. 오른쪽 상단에서 자동 새로 고침과 **인터페이스 언어**를 설정할 수 있습니다 |
| 모니터링 지표 | <http://localhost:15672/metrics>(Prometheus 텍스트, 인증 필요); 패널과 알림은 [모니터링](ops/monitoring/README.md) 참고 |
| 명령줄 | `./bin/swiftmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091`(핫 비활성화, 포트 즉시 닫힘) |
| 헬스 체크 | `nc -z 127.0.0.1 15672`(compose에 healthcheck 내장) |
| 백업과 복구 | [백업 및 복구](ops/backup-restore.md) |
| 업그레이드 | [업그레이드](ops/upgrade.md) |
| 보안 기준선 | [보안 기준선](ops/security-baseline.md) |

자주 쓰는 설정(전체 예시는 [configs/swiftmqd.json](../../../configs/swiftmqd.json) 참고, `SWIFTMQ_*` 환경 변수로 덮어쓸 수도 있음):

| 설정 항목 | 설명 | 기본값 |
| --- | --- | --- |
| `data_dir` | 데이터 디렉터리(메시지 + 메타데이터), **반드시 영속화** | `data` |
| `listeners` | 각 프로토콜 수신 주소, TLS 설정 가능 | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | 관리 UI / API 수신 주소 | `:15672` |
| `management.language` | 관리 UI 기본 언어, 비워 두면 배포 지역 시간대에 따라 자동 선택 | 자동 |
| `storage.fsync` | 디스크 기록 등급 `none / os / batch / always`(confirm 시점도 함께 결정) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | 리소스 워터마크: 도달 시 프로듀서를 차단하며, **메시지를 유실하지 않음** | `0.4` / 50 MiB |
| `users` | 내장 사용자 테이블(비밀번호 + 태그 + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | 다중 노드 클러스터(기본 비활성), 멤버 변경은 `swiftmqctl add_member` 사용 | 비활성 |

> 포트가 사용 중일 수 있습니다. `listeners` / `management.addr`로 다른 포트로 변경하면 됩니다.

***

## 프로젝트 구조

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # broker 프로세스 진입점(실행해야 하는 것)
│   └── swiftmqctl/      # 운영 CLI(관리 HTTP API를 사용하며 커널 버전과 분리)
├── internal/            # 커널 구현
│   ├── protocol/        # 프로토콜 플러그인: amqp091, mqtt(인코딩/디코딩 / 메서드 / 세션)
│   ├── broker/          # 커널: vhost, 익스체인지, 큐, 데드레터, 흐름 제어, 관리면 뷰
│   ├── store/           # 영속화: 세그먼트 로그, 큐 인덱스, 크래시 복구
│   ├── raft/ meta/      # 클러스터: 자체 개발 Raft와 메타데이터 복제
│   ├── management/      # 관리 HTTP API + Prometheus 지표 + 내장 UI 정적 서빙
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # 대외 안정 계약: 플러그인 API(plugin)와 외부 프로세스 플러그인 와이어 프로토콜(sidecar)
├── web/                 # 관리 UI 프런트엔드 프로젝트(Vue 3 + Vite), 산출물은 빌드 시 go:embed로 바이너리에 포함
├── configs/             # 예시 설정
├── docs/ops/            # 운영 문서: 백업 복구 / 업그레이드 / 보안 기준선 / 모니터링
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG（交流群二维码）
```

***

## 기여

Issue와 Pull Request를 환영합니다. 이 프로젝트의 존재 이유는 **프로토콜 호환**이므로:

- 버그 수정 시 대응하는 RabbitMQ 동작(버전, 클라이언트, 재현 절차)을 설명해 주세요.
- 프로토콜 세부 사항을 다루는 변경은 RabbitMQ와의 대조 결과를 첨부해 주세요.
- 제출 전 `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .`가 모두 통과하는지 확인하세요.

***

## 라이선스

이 프로젝트는 [Apache License 2.0](../../../LICENSE)를 사용합니다.

사용, 수정, 배포(상업적 사용 포함)가 허용되며, 저작권 및 라이선스 고지를 유지해야 하고 어떠한 보증도 제공하지 않습니다.

Copyright 2026 houzch([NOTICE](../../../NOTICE) 참고)

***

## 감사의 글

AMQP 0-9-1 프로토콜 규격과 [RabbitMQ](https://www.rabbitmq.com/)의 동작 시맨틱은 이 프로젝트의 호환성 작업 기준입니다. 이 프로젝트는 독립 구현이며 RabbitMQ 공식과 소속 관계가 없고, 그 코드를 사용하지 않았습니다.

***

## 교류 그룹 참여

QR 코드를 스캔해 SwiftMQ 교류 그룹에 참여하세요. 궁금한 점이 있으면 그룹에서 바로 질문할 수 있습니다:

![SwiftMQ 교류 그룹](../../../1280X1280.PNG)
