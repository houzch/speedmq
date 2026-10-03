# SwiftMQ 外部プロセスプラグイン開発ガイド —— Python

> **対象読者**：Python で SwiftMQ の外部プロセスプラグイン（sidecar）を書く開発者。
> **先に読む**：[外部プロセスプラグイン（sidecar）開発ガイド](plugin-development.md) —— メンタルモデル、設定フィールド、ワイヤプロトコル総表はそちらにあります；
> 本ドキュメントは **Python でどう実装するか**、および本機で実測した手順と結果だけを扱います。
> **サンプルプロジェクト**：ワークスペース `swiftmq-plugin/python/sidecar_plugin.py`（標準ライブラリのみ、サードパーティ依存ゼロ）。

---

## 1. 動かすとどうなるか

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

3 つの要点（間違えやすいので先に覚えておく）：

1. **あなたのプロセスがサーバー**：ローカルアドレスをリッスンし、カーネルが接続してくるのを待つ（`plugins.<名前>.sidecar.address`）。
2. **対外ビジネスポートはカーネルが開く**：クライアントが接続するのはカーネルのポートで、バイトがあなたにプロキシされる（`protocols[].listeners`）。
3. **`prefix` は非空でなければならない**：カーネルはプレフィックスでスニッフして「この接続を誰に渡すか」を決めます。`prefix` が空は**スニッフに参加しない**ことを意味し、
   それ自身のリスナー上でも接続はあなたに渡されません（実測：接続は即座に切断されます）。プレフィックスの長さは ≤ 8 バイト、ASCII。

---

## 2. 3 ステップで動かす

### 第 1 ステップ：設定でプラグインを宣言する

`swiftmqd.json`（**実際の設定は標準 JSON、コメント不可**）：

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/swiftmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` は**カーネルがあなたに接続するアドレス**（カーネルがクライアント、プラグインがサーバー）。
- `spawn` を空にすると = カーネルは接続するだけで起動せず、プロセスは自分で管理する（systemd / supervisor / compose）。
- `prefix` は**非空でなければならない**：クライアントが送る先頭バイトはこれで始まる必要がある（カーネルはプレフィックスでスニッフして接続を誰に渡すか決める）。
- `listeners` は対外ポートで、カーネルが開く（クライアントが接続するのはカーネルであり、あなたではない）。
- クロスコンテナデプロイでは `address` に**サービス名**を使い（例 `tcp://py-sidecar:19001`）、プラグインは `0.0.0.0` をリッスンする。

### 第 2 ステップ：起動

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### 第 3 ステップ：検証

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. 実装の要点

### 3.1 フレーミング（唯一自分で正しく書く必要があるバイトレイヤ）

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

データフレームのペイロード = `4 バイトビッグエンディアンのストリーム番号 + 生のバイト`；制御プレーンのペイロードは JSON。

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 ハンドシェイクとハートビート

カーネルが接続すると**先に Hello を送る**ので、あなたは `HelloAck` フレームを返さなければなりません；カーネルは
`name == 設定内のプラグイン名` かつ `api_version == カーネルの APIVersion`（現在 `v1`）を検証します。
その後カーネルは 2 秒ごとに `Ping` を送るので、`Pong` を返せばよい（返さないと死んだと判定されます）。

### 3.3 論理ストリーム

`kindOpen` が到着 → **まず `OpenAck` を返す**、次にサービスを開始；`kindData` が到着 → そのまま（またはプロトコルで解析して）`kindData` に書き戻す；処理終了 → `kindClose` を送る。1 本のストリーム = 1 本のクライアント接続。

### 3.4 逆方向呼び出しとカーネルセマンティクスブリッジ

プラグイン → カーネルの呼び出しは `kindCall` かつ `"reverse": true` で行い、カーネルは同じ接続で `kindReply` を返します。
**順序が重要**：

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` は**省けません**：接続のカーネル操作面は認証前に身元を持たず、いきなり `session.open` すると拒否されます
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。パラメータはあなたのプロトコルで解析した SASL レスポンスです：

```python
plain = b"\x00" + user.encode() + b"\x00" + password.encode()
ident = call("core.authenticate", {
    "stream": stream_id, "mechanism": "PLAIN",
    "response": base64.b64encode(plain).decode(),   # 字节在 JSON 里是 base64
})
# 拿到的会话与进程内协议插件完全同一套语义
call("session.open", {"stream": stream_id, "vhost": "/"})
q = call("session.declare_queue", {"stream": stream_id, "exclusive": True, "auto_delete": True})
call("session.publish", {"stream": stream_id, "routing_key": q["name"],
                         "message": {"body": base64.b64encode(b"hi").decode()}})
call("session.consume", {"stream": stream_id, "queue": q["name"], "prefetch": 32})
```

消費の配信はカーネルが**順方向に回推**し（`kindCall`、`method = "session.deliver"`）、処理後に
`session.settle` で確定します（`ack` / `requeue` / `reject`、配信番号はグローバルに一意で、ストリーム番号は不要）。

### 3.5 並行モデル（Python 版）

| 役割 | スレッド |
| --- | --- |
| フレーム読み取りループ | カーネル接続ごとに 1 つ |
| ストリーム処理 | ストリームごとに 1 つ（したがって複数のクライアント接続を並行処理可能） |
| 順方向呼び出し処理 | 呼び出しごとに 1 つ |

**必ず注意**：読み取りループ内で逆方向呼び出しの応答を同期的に待っては**いけません**（デッドロックします）—— 順方向呼び出し（`session.deliver`）
は独立したスレッドに投げて処理する必要があります。処理中にさらに `session.settle` を発行する可能性があるからです。フレームの書き込みはロックで直列化しなければなりません。

---

## 4. コードウォークスルー（サンプルプロジェクト）

`swiftmq-plugin/python/sidecar_plugin.py` は約 320 行、主要な関数：

| 位置 | 役割 |
| --- | --- |
| `read_frame` / `Conn.send` | フレーミングの読み書き（長さプレフィックス + kind） |
| `Conn.call` | 逆方向呼び出し：番号 → 送信 → 応答待ち（`reverse=true` と `id` でマッチング） |
| `Conn.serve` | フレーム読み取りループとディスパッチ |
| `Conn._handle_hello` | プラグイン名/API バージョンを検証し HelloAck を返す |
| `Conn._handle_open` / `_serve_stream` / `_echo` | ストリームライフサイクルとエコー |
| `Conn._dispatch` | 順方向呼び出しの処理：`session.deliver`（settle 含む）、`stats` |
| `Conn._session_demo` | 認証 + キュー宣言 + パブリッシュ + コンシューム |

---

## 5. 実測（本機で再現）

環境：Windows + Python 3.12；カーネルは Docker（`swiftmq:1.1.01`）で動作、プラグインはホストマシンで動作、
カーネルは `tcp://host.docker.internal:19001` でそれに接続。

```
plugin=py-sidecar state=enabled           # /api/plugins
echo=[PYhello]                            # 客户端连内核端口 19002 发 "PYhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
内核已接入 plugin=py-sidecar peer=('127.0.0.1', 52864)
握手完成 plugin=py-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-9e5d0d695639bad12ce919
收到投递（session.deliver） queue=amq.gen-9e5d0d695639bad12ce919 delivery_id=1 body=hello from python sidecar
流已打开 plugin=py-sidecar stream=1 remote=172.17.0.1:43856 local=172.17.0.2:19002
```

カバーした経路：**ハンドシェイク → 認証 → セマンティクスブリッジ（宣言/パブリッシュ/コンシューム）→ 配信の回推 → 確定 → バイトストリームのエコー**。

---

## 6. Python 特有の注意点

- **`time.sleep` でハートビートを待たない**：読み取りループはブロッキング式で、カーネルの Ping で接続を維持すればよい；
  socket に読み取りタイムアウトを設定した場合は、タイムアウトを「接続終了」として扱うのを忘れない（カーネルが `kill -9` されると socket がすぐに閉じない可能性がある）。
- **`json.dumps` は既定でスペースを入れる**：サンプルが `separators=(",", ":")` を使うのはログを見やすくするためだけで、プロトコル自体は要求しない。
- **バイトは base64**：`message.body`、`core.authenticate.response` は JSON 内では base64 文字列なので、
  `base64.b64encode/decode` を忘れないこと。
- **フレーム書き込みはロックが必要**：ハートビート、応答、データブロックは異なるスレッドから来るため、書き込みが交錯すると接続全体が壊れる（サンプルは `threading.Lock` を使用）。
- `asyncio` でもよいが、「書き込みの直列化 + 読み取りループをブロックしない」を保証する必要があり、考え方はスレッド版と同じ。

---

## 7. 応用

- プラグインに独自の管理画面を付けたい：設定に `console_url` を追加すると、管理 UI の「プラグイン管理」ページに直通入口が表示される
  （メインドキュメント §5.8 参照）。
- プラグインを独立サービスにして systemd / K8s で管理：`spawn: []` + `restart: "never"` で、外部から起動する。
- 複数プロトコルの共存が必要：同じ 1 つのリスナー上で複数のプラグインが異なる `prefix` を使うか、それぞれ専用ポートを開く。
