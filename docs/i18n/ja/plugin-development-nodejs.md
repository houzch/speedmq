# SwiftMQ 外部プロセスプラグイン開発ガイド —— Node.js

> **対象読者**：Node.js で SwiftMQ の外部プロセスプラグイン（sidecar）を書く開発者。
> **先に読む**：[外部プロセスプラグイン（sidecar）開発ガイド](plugin-development.md)（メンタルモデル / 設定フィールド / ワイヤプロトコル総表）。
> **サンプルプロジェクト**：ワークスペース `swiftmq-plugin/nodejs/index.js`（Node 標準ライブラリのみ、**npm 依存不要**）。

---

## 1. 動かすとどうなるか

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

3 つの要点：**あなたのプロセスがサーバー**（カーネルの接続を待つ）；**対外ポートはカーネルが開く**（設定 `protocols[].listeners`）；
**`prefix` は非空でなければならない**（空プレフィックス = スニッフに参加せず、接続はあなたに渡されない；実測では即座に切断、≤8 バイト ASCII）。

---

## 2. 3 ステップで動かす

### 第 1 ステップ：設定

`swiftmqd.json`（**実際の設定は標準 JSON、コメント不可**）：

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/swiftmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### 第 2 ステップ：起動

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### 第 3 ステップ：検証

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
```

---

## 3. 実装の要点

### 3.1 フレーミング

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Node では `Buffer` を使う：受信したバイトを蓄積し、1 フレーム分たまったら切り出して処理する。

```js
drain() {
  while (this.buf.length >= 5) {
    const len = this.buf.readUInt32BE(0)
    if (this.buf.length < 4 + len) return      // 帧还没收全
    const kind = this.buf.readUInt8(4)
    const payload = this.buf.subarray(5, 4 + len)
    this.buf = this.buf.subarray(4 + len)
    this.dispatch(kind, payload)
  }
}
```

1 本のストリーム = 1 本のクライアント接続；データフレームのペイロードは `4 バイトビッグエンディアンのストリーム番号 + 生のバイト`。

### 3.2 ハンドシェイクとハートビート

カーネルが**先に Hello を送り**、あなたは `HelloAck` を返す；カーネルは `name` と `api_version`（現在 `v1`）を検証してから接続する。
その後 2 秒ごとに 1 つの `Ping`、`Pong` を返せばよい（読み取りループがついでに処理するので、タイマーは不要）。

### 3.3 非同期モデル（Node 版）

シングルスレッドのイベントループなので、「書き込みの交錯」問題は自然に避けられる—— ただし**読み取りループで逆方向呼び出しを await しない**よう注意：

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` は `async`：さらに `await call('session.settle', …)` する可能性があるため、決して同期的な待ちにしてはいけません。

### 3.4 セマンティクスブリッジ（先に認証が必要）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` は省けません：接続のカーネル操作面は認証前に身元を持たず、いきなり `session.open` すると拒否されます
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

```js
const ident = await call('core.authenticate', {
  stream, mechanism: 'PLAIN',
  response: Buffer.concat([Buffer.from(`\x00${user}\x00`), Buffer.from(password)]).toString('base64'),
})
await call('session.open', { stream, vhost: '/' })
const q = await call('session.declare_queue', { stream, exclusive: true, auto_delete: true })
await call('session.publish', { stream, routing_key: q.name,
  message: { body: Buffer.from('hi').toString('base64') } })
await call('session.consume', { stream, queue: q.name, prefetch: 32 })
```

配信はカーネルが**順方向に回推**し（`method = "session.deliver"`）、処理後に `session.settle`
（`ack` / `requeue` / `reject`；配信番号はグローバルに一意で、ストリーム番号は伴わない）。

---

## 4. コードウォークスルー（サンプルプロジェクト）

`swiftmq-plugin/nodejs/index.js` は約 330 行：

| 位置 | 役割 |
| --- | --- |
| `u32()` / `Conn.send()` | フレーミングの読み書き |
| `Conn.drain()` / `dispatch()` | フレーム単位の解析とディスパッチ |
| `Conn.call()` | 逆方向呼び出し（`Promise` + `pending` テーブル、`reverse=true` と `id` でマッチング） |
| `Stream` | ストリームの読み取り側：`push/end/read` で非同期キューを構成 |
| `handleHello` | 検証して HelloAck を返す |
| `handleForwardCall` / `handleMethod` | 順方向呼び出し（`session.deliver` + settle、`stats`） |
| `sessionDemo` | 認証 + 宣言 + パブリッシュ + コンシューム |

---

## 5. 実測（本機で再現）

Windows + Node v24；カーネルは Docker（`swiftmq:1.1.01`）、プラグインはホストマシン（`tcp://host.docker.internal:19011`）。

```
plugin=node-sidecar state=enabled         # /api/plugins
echo=[NDhello]                            # 客户端连内核端口 19012 发 "NDhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=node-sidecar addr=0.0.0.0:19011 version=0.1.0
内核已接入 plugin=node-sidecar peer=127.0.0.1:50987
握手完成 plugin=node-sidecar kernel=1.1.01
认证通过 user=guest
收到投递（session.deliver） queue=amq.gen-893688b01c62b2ad5763a7 delivery_id=1 body=hello from node sidecar
session 演示完成 queue=amq.gen-893688b01c62b2ad5763a7
流已打开 plugin=node-sidecar stream=1 remote=172.17.0.1:40450 local=172.17.0.2:19012
```

カバー：**ハンドシェイク → 認証 → セマンティクスブリッジ → 配信の回推 → 確定 → バイトストリームのエコー**。

---

## 6. Node.js 特有の注意点

- **`socket.write` は 1 回で 1 フレーム書き込む**：サンプルはフレーム全体を 1 つの `Buffer` に組み立ててから書くため、追加のロックは不要；
  1 フレームを複数回の `write` に分割する場合は、自分で順序を保証する必要がある。
- **`stream.on('data')` の chunk 境界とフレームは無関係**：自分でバッファを蓄積しなければならない（`drain()` 参照）。
- **base64**：`message.body`、`core.authenticate.response` は JSON 内では base64 文字列
  （`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`）。
- **`drain()` 内で `await` しない**：それは同期的なフレーム切り出し関数；非同期処理は `handleForwardCall` に任せる。
- **ESM vs CJS**：サンプルは `node index.js` でそのまま動かせるよう CommonJS（`require`）を使用；ESM にするには `import` に変えるだけ。

---

## 7. 応用

- プラグイン独自の管理画面：設定に `console_url`（メインドキュメント §5.8）を追加すると、管理 UI の「プラグイン管理」ページに直通入口が表示される。
- 独立デプロイ（K8s / systemd）：`spawn: []` + `address: "tcp://<サービス名>:19011"`、コンテナ内で `0.0.0.0` をリッスン。
- ポート多重化：複数のプロトコルにそれぞれ異なる `prefix` を与え、カーネルがプレフィックスで接続を各プラグインにディスパッチする。
