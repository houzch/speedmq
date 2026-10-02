# SwiftMQ バックアップと復元

> 本書の「実測」結論はすべて **Windows + PowerShell 5.1** 上での 1 回の実運用ドリル（一時的な `data_dir` と一時ポート）によるものです。
> ドリルのコマンドと主要な出力は §6 にそのまま貼り付けています。**【未検証】** の部分は明示的に注記します（クラスタのバックアップ/復元、Docker ボリュームのバックアップなど）。

---

## 1. 何をバックアップするか

`data_dir` 配下は**全体をバックアップする必要があります**。特に重要なのは以下です（レイアウトは `upgrade.md` §3 を参照）：

| パス | 役割 | 失うとどうなるか |
| --- | --- | --- |
| `meta/state.json` | 単機メタデータスナップショット：vhost / エクスチェンジ / キュー / バインディング / ユーザー / 権限 / ポリシー | トポロジとアカウントがすべて失われる |
| `meta/raft.log`、`meta/raft.state`、`meta/snapshot.json` | 【クラスタ】Raft ログ / 任期投票 / スナップショット+メンバー表 | クラスタの同一性とメタデータの一貫性が失われる |
| `meta/users.seeded`、`meta/vhosts.seeded` | ブートストラップマーカー | 失うと設定の users/vhosts が**再度シード**される（削除したアカウント/vhost が復活） |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | クラシックキューのメッセージデータとインデックス | 永続メッセージが失われる |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【クラスタ】クォーラムキューの Raft ログ/スナップショット | クォーラムキューのデータが失われる |
| 証明書ファイル（設定の `cert_file`/`key_file`/`ca_file` が指す PEM） | TLS 証明書 | `data_dir` とは別にバックアップすること。失うと再起動後に TLS が起動しない |

> ソフトステート（未確認メッセージ、コンシューマ、prefetch カウント）は**メモリ上のみ**でディスクに書き込まれず、バックアップに**含まれません**し、含めるべきでもありません。

---

## 2. 一貫性の要件：**必ず先にプロセスを停止**；ホットバックアップは**安全でない**

### 2.1 結論

- ✅ **安全な方法**：**broker プロセスを停止**（正常終了時に最終フラッシュを行う）してから `data_dir` をコピーします。
- ❌ **ホットバックアップ（プロセス実行中にファイルを直接コピー）：安全ではなく、保証しません。**

### 2.2 なぜホットバックアップが安全でないか

メッセージストアは**2 つのファイル**（セグメントファイル `*.seg` とインデックスファイル `index/*.idx`）で、両者は**アトミックにコミットされません**：

- 復元時は**インデックスを基準**に「どのメッセージが生きているか」を判断し、インデックス内の `(セグメント番号, オフセット, 長さ)` に従ってセグメントファイルを読み取ります。
- ホットバックアップでは、**インデックスが参照済みなのにセグメントファイルが書き込み完了していない**（またはその逆）中間状態をコピーする可能性があります：
  - インデックスがセグメントに存在しないレコードを参照 → そのメッセージは**読み取り失敗でスキップ**される（確認済みの永続メッセージを失うのと同じ）；
  - セグメントにレコードがあるがインデックスが参照していない → そのメッセージは**復元されない**。
- 復元時には CRC32 で**末尾の書きかけレコード**を破棄しますが、それは「単一ファイル末尾の書き込み破損」のみをカバーし、**インデックスとセグメント間の不整合は修復できません**。

### 2.3 「書き込みはいつディスクに届くか」について（実測観察）

- デフォルトの `fsync: os` + `flush_interval_ms: 200`：メッセージはバックグラウンドのフラッシュコルーチンによって**最大約 200 ms** 以内に OS へ `write()` されます
  （fsync はしない）。publisher confirm もこの後に返されます。
- 実測：永続メッセージを publish した**直後**にセグメントファイルサイズを確認すると、既にデータが見えています（`t=0ms seg=832`）。つまり「OS から見えるバイト」と confirm はほぼ同期しています。
- **注意**：これは「OS バッファに届いた」ことのみを示し、**プロセスを強制 kill しても失われません**（プロセスが kill されても OS バッファは失われない）が、**電源断では失われます**。
  「confirm を受信した時点で fsync によりディスクに書き込まれている」ようにしたい場合は、`storage.fsync` を `batch` / `always` に変更してください。**【電源断シナリオは未実測】**

---

## 3. バックアップ手順

### 3.1 単機（推奨）

```powershell
# 1) プロセスを停止（フォアグラウンド：Ctrl+C；バックグラウンド：Stop-Process）
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) data_dir 全体をコピー（タイムスタンプ付き）
$data = "C:\swiftmq\data"
Copy-Item -Recurse -Force $data "C:\backup\swiftmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) （任意）バックアップ内のメタデータスナップショットが解析可能か検証
Get-Content "C:\backup\swiftmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 クラスタ

- **各ノードがそれぞれ自分の `data_dir` をバックアップします**（メタデータは Raft で全体に複製され、メッセージデータは Owner ノードに、クォーラムキューのレプリカは各自の Raft ディレクトリにあります）。
- 停止順序：**一度に 1 ノードのみ停止**してください。複数の投票メンバーを同時に停止しないでください（`upgrade.md` §7.2 を参照）。
- **クラスタ全体で一貫したスナップショット**を得るには、全ノードを順に停止してからそれぞれコピーする必要があります。本番では「ノードごとに停止/コピー/起動」がより一般的です。
- **【未検証】** 本機では実際のクラスタバックアップ/復元ドリルを行っていません。

### 3.3 Docker（名前付きボリューム）

```powershell
# コンテナ停止後、一時コンテナでボリューム内容をアーカイブしてコピー
docker compose down
docker run --rm -v swiftmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/swiftmq-data.tar.gz -C /data .
```
> **【未検証】**（本機では Docker を実行していません）。

---

## 4. 復元手順

### 4.1 単機

```powershell
# 1) プロセスが停止していることを確認
Get-Process -Name swiftmqd -ErrorAction SilentlyContinue

# 2) 現在の data_dir を退避（または削除）し、新旧ファイルが混ざらないようにする
Move-Item "C:\swiftmq\data" "C:\swiftmq\data.broken"

# 3) バックアップから復元
Copy-Item -Recurse -Force "C:\backup\swiftmq-YYYYMMDD-HHMMSS" "C:\swiftmq\data"

# 4) 起動
& "C:\swiftmq\swiftmqd.exe" -config "C:\swiftmq\configs\swiftmqd.json" -log-level info
```

要点：
- **必ず先に旧ディレクトリを退避**してください。「バックアップファイルを中途半端に残ったディレクトリに上書きする」ことはできません；
- 復元する `data_dir` は、バックアップ時と**同一の vhost/キュー集合**でなければなりません（ディレクトリ名はエンコード後のもので、マシン間で利用可能）；
- 復元を機に設定ファイルの `vhosts`/`users` を**変更しないでください**（初回ブートストラップ時のみ有効で、変更しても意味がありません。`upgrade.md` §4.2 を参照）。

### 4.2 クラスタ

- 単一ノードの復元：§4.1 に従ってそのノードの `data_dir` を復元してから起動すると、既存メンバーとして再参加し Raft ログに追いつきます。
- クラスタ全体の復元：**まず多数派ノード（投票メンバーの半数以上）を復元して起動**し、クラスタが leader を選出できるようにしてから、残りのノードを復元します。
- **【未検証】** クラスタ復元は未実測です。

---

## 5. 復元後の検証方法

管理 API と実クライアントでクロスチェックします（すべて実施を推奨）：

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) オブジェクト総数とメッセージ総数（キュー数/エクスチェンジ数/バインディング数/ユーザー数；messages/ready/unacked）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) キューごとに messages / messages_ready を照合（バックアップ前の記録と比較可能）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vhost / ユーザー / ポリシー がすべて存在するか
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) クラスタ（単機では enabled=false / mode=local / role=single を返す）
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **起動ログを見る**：`已从磁盘恢复队列消息 ... messages=N` と `队列已恢复持久化消息 ... messages=N` が出現するはずです；N はバックアップ前と一致するはずです。
- **ログ内の警告**：あるキューが以前にメッセージを消費/クリアされていた場合、復元時に
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` と `恢复时清理了无存活消息的段` が出現することがあります。
  これらは**決済済み（ack/purge）レコード**のインデックス残骸で、**既知のログノイズであり、データの正確性には影響しません**（§7 を参照）。
- **実クライアント**：キューからメッセージを取り出し、件数/内容を照合します（§6 の (7) を参照）。

---

## 6. 実測ドリル（実際のコマンドと出力）

> 環境：`data_dir` は一時ディレクトリ、AMQP `127.0.0.1:5676`、管理面 `127.0.0.1:15677`、MQTT `127.0.0.1:1884`、
> デフォルトアカウント `guest/guest`。起動ログ：
> ```
> level=INFO msg="SwiftMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) durable トポロジを作成 + 5 件の永続メッセージを送信（実クライアント `amqp091-go`）**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) ユーザー / vhost / 権限 / ポリシー を作成（管理 API）**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) バックアップ前の状態（管理 API）**

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

**(4) バックアップ前のディスクファイル**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) プロセス停止 → バックアップ → クリア → 復元**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) 再起動後の復元ログ（重要行）**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> 同時に `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"` が数行出現します：
> これらは本ドリルで**以前に purge された**メッセージが残したインデックス残骸（決済済み、セグメントデータは回収済み）で、**以下の 5 件のメッセージの復元には影響しません**。

**(7) 復元後のアサーション：管理 API + 実クライアント**

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

**結論**：durable トポロジ（エクスチェンジ+キュー+バインディング）、5 件の永続メッセージ、ユーザー、vhost、権限、ポリシーが**すべて復元**され、
実クライアントが元のまま全メッセージを取り出せました。**ドリル合格。**

### 6.1 対照：消費/purge を経験していないキューの復元はより「静か」

上の WARN が普遍的な現象か切り分けるため、別途**制御された対照**を実施：新規 durable キュー `clean.q` を作成し、3 件の永続メッセージを送信、
**消費もクリアもせず**、プロセス停止後に再起動：

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**WARN は一切なし**。WARN は「インデックスに決済済みレコードが残っている」シーンでのみ発生することが分かります（§7 を参照）。

---

## 7. 既知の問題と制限（正直に記載）

1. **復元ログのノイズ（実際に観察）**：キューで過去に消費/クリアが発生した場合（メッセージが ack/purge 済み）、
   そのインデックスには回収済みレコードへの参照が残り、復元時に各レコードごとに**`恢复消息失败，已跳过` WARN を 1 行ずつ出力**し、
   空の `000000.seg` を新規作成/クリーンアップします（ログ `恢复时清理了无存活消息的段`）。
   **データの正確性には影響しません**（生存する未確認メッセージは正しく復元される）が、**ログを汚染**し、大規模キュー/高スループットでは画面を埋め尽くす可能性があります。
   推奨：`已从磁盘恢复队列消息 ... messages=N` を基準とし、決済済みレコードに関するこれらの WARN は無視してください；
   ログ量が許容できない場合はカーネルメンテナにフィードバックしてください（本書はコードを変更しません）。
2. **ホットバックアップは安全でない**（§2）：プロセス実行中に `data_dir` を直接コピーしないでください。
3. **`fsync: os` は電源断で失わないことを保証しない**：「confirm 即ディスク書き込み」にしたい場合は `batch` / `always` を使用してください。
4. **パスワードが平文**：`meta/state.json` 内のユーザーパスワードは**平文**です（実測で `"password":"drillpass"` が確認できる）——
   そのためバックアップファイルは**機密データとして扱う必要があります**（アクセス制御、暗号化保存）。詳細は `security-baseline.md` を参照。
5. **【未検証】** クラスタのバックアップ/復元、Docker ボリュームのバックアップ/復元、電源断シナリオ、復元中の並行書き込み。
