<!-- i18n-switcher -->
[简体中文](../../../README-cn.md) | [繁體中文](../zh-TW/README.md) | [English](../../../README.md) | **日本語** | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SpeedMQ

Go で書かれた **RabbitMQ 互換**のメッセージミドルウェアです。既存の RabbitMQ クライアントは**コードを変更せず、SDK も差し替えず**、接続先アドレスを変えるだけで接続できます。

## 概要

- **プロトコル互換**：AMQP 0-9-1（RabbitMQ 拡張を含む）と MQTT 3.1.1。互換のベースラインは **RabbitMQ 4.3 のセマンティクス**です。
- **導入が簡単**：バイナリ 1 個 / コンテナ 1 個で、管理 UI も内蔵済み。追加の Nginx、データベース、Node ランタイムは不要です。
- **運用に十分**：管理 UI（キュー / エクスチェンジ / 接続 / アカウント権限 / 仮想ホスト / ポリシー / 制限 / クラスタ）、Prometheus `/metrics`、コマンドライン `speedmqctl`。
- **デフォルトポート**：`5672`（AMQP）、`1883`（MQTT）、`15672`（管理 UI / HTTP API / メトリクス）。

既に備えている機能：永続化（セグメントログ + fsync 段階 + クラッシュリカバリ）、パブリッシュ確認、TTL / デッドレター / 長さ制限、コンシューマ優先度、Direct Reply-To、クラスタ（Raft メタデータ + クォーラムキュー + ノード間転送）、プラグインのホットスタート/ストップ。

***

## クイックスタート

### 方法 1：Docker（推奨）

**リポジトリを clone せず、イメージをそのまま取得して起動できます：**

```bash
docker run -d --name speedmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v speedmq-data:/var/lib/speedmq \
  houzch/speedmq:1.1.03
```

イメージは 2 か所に同一内容で公開しています（速い方をお使いください）：Docker Hub `houzch/speedmq`、GitHub GHCR `ghcr.io/houzch/speedmq`。どちらも `linux/amd64` と `linux/arm64` を提供します。

- データは名前付きボリューム `speedmq-data` に保存され、コンテナを作り直しても失われません。
- 停止／削除：`docker stop speedmq`、`docker rm speedmq`（データボリュームは残ります）。

**設定を変更したり compose で運用したりする場合は、リポジトリを clone してください：**

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
docker compose pull && docker compose up -d   # 公開済みイメージを使用。ローカルでビルドするなら up -d --build に変更

docker compose ps        # ステータスは Up (healthy) になるはず
docker compose logs -f   # ログを追う
```

- 設定は `configs/speedmqd.json` から読み取り専用でマウントされ、変更後は `docker compose restart` で反映されます。
- 停止：`docker compose down`（データは保持）；`docker compose down -v`（データも削除）。

### 方法 2：ローカルバイナリ（Go 1.24+ が必要）

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
go build -o bin/speedmqd ./cmd/speedmqd
go build -o bin/speedmqctl ./cmd/speedmqctl
./bin/speedmqd -config configs/speedmqd.json
```

> 管理 UI のビルド成果物はリポジトリに含まれません。UI を使う場合は、先に `web/` で `npm ci && npm run build` を実行してください；
> ビルドしなくてもメッセージの送受信は正常に起動できますが、`/` にアクセスすると「管理 UI 未ビルド」と表示されるだけです。

### 初回ログイン（必ずデフォルトアカウントを先に変更）

| 入口 | アドレス / 認証情報 |
| --- | --- |
| 管理 UI | <http://localhost:15672/>（ユーザー名 `guest`、パスワード `guest`） |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883`（アカウントは同上） |

新規インストールしたインスタンスの総アカウントには「初回ログイン時の強制パスワード変更」フラグが付いています。管理 UI にログインすると**アカウント名とパスワードの同時変更が必須**となり、変更後にのみバックエンドへ入れます。

API を直接呼んで完了させることもできます（自動化に適しています）：

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ デフォルトの `guest/guest` は RabbitMQ の挙動と一致し、**本機からのログインのみ許可**されます。コンテナ外 / リモートからの接続には、設定でそのユーザーに `remote_access` を有効にする必要があります（サンプル設定ではコンテナのシナリオ向けに既に有効化済み）。
> **サービスが外部からアクセス可能になったら、直ちに認証情報を変更してください。**

### あなたのアプリケーションへの接続（接続先アドレスを変えるだけ）

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
# MQTT（mosquitto クライアント）
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

管理 HTTP API は `rabbitmqadmin` と互換です。管理 UI の「キューの追加 / エクスチェンジの追加」は標準の宣言エンドポイントそのものなので、スクリプトでも同様に操作できます：

```bash
# キューの宣言（クォーラムキューは arguments: {"x-queue-type":"quorum"} で表現）
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### 日常運用

| 事項 | 入口 |
| --- | --- |
| 管理 UI | <http://localhost:15672/>：キュー / エクスチェンジ / 接続 / アカウント権限 / 仮想ホスト / ポリシー / 制限 / フィーチャーフラグ / クラスタ。右上で自動更新と**画面言語**を設定できます |
| 監視メトリクス | <http://localhost:15672/metrics>（Prometheus テキスト、認証が必要）；ダッシュボードとアラートは [docs/ops/monitoring](ops/monitoring/README.md) を参照 |
| コマンドライン | `./bin/speedmqctl status`、`list_queues`、`plugins list`、`plugins disable amqp091`（ホット停止、ポートは即座に閉じられる） |
| ヘルスチェック | `nc -z 127.0.0.1 15672`（compose に healthcheck を内蔵済み） |
| バックアップと復元 | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| アップグレード | [docs/ops/upgrade.md](ops/upgrade.md) |
| セキュリティベースライン | [docs/ops/security-baseline.md](ops/security-baseline.md) |

よく使う設定（完全なサンプルは [configs/speedmqd.json](../../../configs/speedmqd.json) を参照。`SPEEDMQ_*` 環境変数で上書きも可能）：

| 設定項目 | 説明 | デフォルト |
| --- | --- | --- |
| `data_dir` | データディレクトリ（メッセージ + メタデータ）、**必ず永続化** | `data` |
| `listeners` | 各プロトコルのリッスンアドレス、TLS を設定可能 | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | 管理 UI / API のリッスンアドレス | `:15672` |
| `management.language` | 管理 UI のデフォルト言語；空欄ならデプロイ先のタイムゾーンに応じて自動選択 | 自動 |
| `storage.fsync` | ディスク書き込み段階 `none / os / batch / always`（confirm のタイミングも同時に決まる） | `os` |
| `storage.memory_high_watermark`、`storage.disk_free_limit` | リソース水位：達すると生産者がブロックされ、**メッセージは失われない** | `0.4` / 50 MiB |
| `users` | 内蔵ユーザーテーブル（パスワード + タグ + `remote_access`） | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | マルチノードクラスタ（デフォルトは無効）、メンバー変更は `speedmqctl add_member` | 無効 |

> ポートが使用中の可能性があります：`listeners` / `management.addr` を他のポートに変えれば済みます。

***

## プロジェクト構成

```
speedmq/
├── cmd/
│   ├── speedmqd/        # broker プロセスエントリ（実行するのはこれ）
│   └── speedmqctl/      # 運用 CLI（管理 HTTP API 経由、カーネル版とは分離）
├── internal/            # カーネル実装
│   ├── protocol/        # プロトコルプラグイン：amqp091、mqtt（エンコード/デコード / メソッド / セッション）
│   ├── broker/          # カーネル：vhost、エクスチェンジ、キュー、デッドレター、フロー制御、管理面ビュー
│   ├── store/           # 永続化：セグメントログ、キューインデックス、クラッシュリカバリ
│   ├── raft/ meta/      # クラスタ：自作 Raft とメタデータレプリケーション
│   ├── management/      # 管理 HTTP API + Prometheus メトリクス + 内蔵 UI 静的配信
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # 対外安定契約：プラグイン API（plugin）と外部プロセスプラグインの線プロトコル（sidecar）
├── web/                 # 管理 UI フロントエンドプロジェクト（Vue 3 + Vite）、成果物はビルド時に go:embed でバイナリに埋め込み
├── configs/             # サンプル設定
├── docs/ops/            # 運用ドキュメント：バックアップ復元 / アップグレード / セキュリティベースライン / 監視
├── Dockerfile、docker-compose.yml
└── speedmq-logo.PNG、1280X1280.PNG（交流グループの QR コード）
```

***

## コントリビューション

Issue と Pull Request を歓迎します。本プロジェクトの立脚点は**プロトコル互換**であるため：

- バグ修正の際は、対応する RabbitMQ の挙動（バージョン、クライアント、再現手順）を明記してください；
- プロトコルの詳細に関わる変更では、RabbitMQ との比較結果を添付してください；
- 提出前に `go build ./...`、`go vet ./...`、`go test ./...`、`gofmt -l .` がすべて通ることを確認してください。

***

## ライセンス

本プロジェクトは [Apache License 2.0](../../../LICENSE) を採用しています。

使用、変更、配布（商用利用を含む）が許可されますが、著作権とライセンスの表示を保持する必要があり、いかなる保証も提供しません。

Copyright 2026 houzch（[NOTICE](../../../NOTICE) を参照）

***

## 謝辞

AMQP 0-9-1 プロトコル仕様と [RabbitMQ](https://www.rabbitmq.com/) の挙動セマンティクスは、本プロジェクトの互換性作業における比較の基準です。本プロジェクトは独立した実装であり、RabbitMQ 公式とは所属関係になく、そのコードも使用していません。

***

## 交流グループへの参加

QR コードをスキャンして SpeedMQ 交流グループに参加してください。質問があればグループで直接聞けます：

![SpeedMQ 交流グループ](../../../1280X1280.PNG)
