# SpeedMQ Sao lưu và phục hồi

> Các kết luận "đo thực tế" trong tài liệu này đều đến từ một buổi diễn tập thực tế trên **Windows + PowerShell 5.1** (dùng `data_dir` tạm và cổng tạm).
> Các lệnh diễn tập và đầu ra quan trọng được dán nguyên văn ở §6.
> Phần **【chưa kiểm chứng】** sẽ được đánh dấu rõ ràng (như tình huống mất điện thật trên máy vật lý).
>
> **【đã kiểm chứng (2026-10-11, cluster 3 node Docker Desktop máy cục bộ)】** Kill cứng tiến trình (`docker kill`) và ghi đồng thờitrong quá trình phục hồi đã được đo thực tế, biên bản xem §6.2 / §6.3; cách hiểu "mất điện" ở §2.3 và danh sách trạng thái kiểm chứng ở §7 đã được cập nhật đồng bộ.

---

## 1. Cần sao lưu những gì

Trong `data_dir` **phải sao lưu toàn bộ**, những thứ then chốt là các mục dưới đây (cách bố trí xem `upgrade.md` §3):

| Đường dẫn | Tác dụng | Mất thì sẽ thế nào |
| --- | --- | --- |
| `meta/state.json` | Ảnh chụp siêu dữ liệu chế độ máy đơn: vhost / exchange / queue / binding / người dùng / quyền / policy | Mất toàn bộ topology và tài khoản |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【cluster】Nhật ký Raft / bỏ phiếu nhiệm kỳ / ảnh chụp + bảng thành viên | Mất danh tính cluster và tính nhất quán siêu dữ liệu |
| `meta/users.seeded`, `meta/vhosts.seeded` | Dấu hiệu khởi tạo | Mất sẽ khiến users/vhosts trong cấu hình bị **gieo hạt lại** (tài khoản/vhost đã xóa sống lại) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Dữ liệu và chỉ mục message của queue cổ điển | Mất message bền vững |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【cluster】Nhật ký/ảnh chụp Raft của queue trọng tài | Mất dữ liệu queue trọng tài |
| Tệp chứng chỉ (PEM mà `cert_file`/`key_file`/`ca_file` trong cấu hình trỏ tới) | Chứng chỉ TLS | Sao lưu riêng khỏi `data_dir`, sau khi khởi động lại TLS không lên được |

> Trạng thái mềm (message chưa xác nhận, consumer, bộ đếm prefetch) **chỉ nằm trong bộ nhớ**, không ghi xuống đĩa, bản sao lưu **không bao gồm** và cũng không nên bao gồm chúng.

---

## 2. Yêu cầu nhất quán: **phải dừng tiến trình trước**; sao lưu nóng **không an toàn**

### 2.1 Kết luận

- ✅ **Cách làm an toàn**: **dừng tiến trình broker** (thoát êm sẽ thực hiện ghi đĩa kết thúc), rồi sao chép `data_dir`.
- ❌ **Sao lưu nóng (sao chép tệp trực tiếp khi tiến trình đang chạy): không an toàn, không đảm bảo.**

### 2.2 Tại sao sao lưu nóng không an toàn

Kho message là **hai tệp** (tệp phân đoạn `*.seg` và tệp chỉ mục `index/*.idx`), hai tệp này **không được ghi theo kiểu nguyên tử**:

- Khi phục hồi **lấy chỉ mục làm chuẩn** để phán đoán "message nào còn sống", rồi theo `(số phân đoạn, độ lệch, độ dài)` trong chỉ mục để đọc từ tệp phân đoạn.
- Khi sao lưu nóng có thể sao chép phải trạng thái trung gian **chỉ mục đã tham chiếu nhưng tệp phân đoạn chưa ghi đầy** (hoặc ngược lại):
  - Chỉ mục tham chiếu bản ghi không tồn tại trong phân đoạn → message đó **đọc thất bại và bị bỏ qua** (tương đương mất message bền vững đã xác nhận);
  - Phân đoạn có bản ghi nhưng chỉ mục không tham chiếu → message đó **không được phục hồi**.
- Phục hồi tuy dùng CRC32 để loại bỏ **bản ghi ghi dở ở phần đuôi**, nhưng điều đó chỉ bao phủ "phần đuôi một tệp bị ghi hỏng", **không thể sửa được sự không đồng bộ giữa chỉ mục và phân đoạn**.

### 2.3 Về "khi nào ghi xuống đĩa" (quan sát đo thực tế)

- Mặc định `fsync: os` + `flush_interval_ms: 200`: message được coroutine ghi đĩa chạy nền `write()` tới hệ điều hành trong **tối đa khoảng 200 ms**
  (không fsync), publisher confirm cũng trả về sau đó.
- Đo thực tế: sau khi publish message bền vững, **kiểm tra ngay** kích thước tệp phân đoạn đã có thể thấy dữ liệu (`t=0ms seg=832`); tức là "số byte mà OS có thể thấy" gần như đồng bộ với confirm.
- **Lưu ý**: điều này chỉ cho thấy "đã vào bộ đệm OS", **kill cứng tiến trình sẽ không mất** (tiến trình bị kill không mất bộ đệm OS), nhưng **mất điện sẽ mất**.
  Muốn "nhận confirm là đã fsync xuống đĩa", hãy đổi `storage.fsync` thành `batch` / `always`.
  **【đã kiểm chứng (2026-10-11, cluster 3 node Docker Desktop máy cục bộ)】** Kill cứng node bằng `docker kill` (SIGKILL) trong khi ghi, đo thực tế cả hai mức đều không mất message đã confirm:
  `fsync: os` kill follower (50.000 message confirm toàn bộ, đối soát missing=0), `fsync: always` kill leader của queue trọng tài (200.000 message confirm toàn bộ,
  missing=0; ngoài ra 1.355 message đã xuống đĩa nhưng chưa confirm được phát lại, ngữ nghĩa at-least-once cho phép, phía consumer khử trùng theo message ID là idempotent).
  Điểm cộng: ở mức mặc định, dùng `wsl --shutdown` để dừng khẩn cấp toàn bộ Docker VM trong khi publish (gần tương đương mất điện toàn máy, mất bộ đệm OS), sau khi phục hồi 69.669 message đã confirm
  đối soát theo ID missing=0. **Vẫn chưa kiểm chứng**: mất điện thật trên máy vật lý (mất nguồn điện của host, tầng cache đĩa), không thể mô phỏng trên máy này.

---

## 3. Các bước sao lưu

### 3.1 Máy đơn (khuyến nghị)

```powershell
# 1) dừng tiến trình (foreground: Ctrl+C; background: Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) sao chép toàn bộ data_dir (kèm dấu thời gian)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (tùy chọn) kiểm tra ảnh chụp siêu dữ liệu trong bản sao lưu có thể phân tích được
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Mỗi node tự sao lưu `data_dir` của mình** (siêu dữ liệu được Raft sao chép tới toàn bộ, dữ liệu message nằm ở node Owner, bản sao queue trọng tài nằm trong thư mục Raft của từng node).
- Thứ tự dừng: **mỗi lần chỉ dừng một node**; không dừng đồng thời nhiều thành viên bỏ phiếu (xem `upgrade.md` §7.2).
- Muốn có **ảnh chụp nhất quán toàn cluster**, cần lần lượt dừng tất cả các node rồi sao chép từng node; trong sản xuất phổ biến hơn là "dừng/sao chép/khởi động từng node".
- **【đã kiểm chứng (2026-10-10, cluster 3 node Docker Desktop máy cục bộ)】** Trên cluster compose 3 node, "dừng/sao chép/khởi động" từng node: mỗi node dùng container alpine dùng một lần đóng gói named volume của mình
  (`docker run --rm -v <volume>:/data -v <thư mục host>:/backup alpine tar czf ...`), gói chứa `meta/raft.log`, `meta/raft.state`, `meta/*.seeded`, `quorum/...`;
  sau phục hồi node đó tham gia lại với tư cách thành viên sẵn có và đuổi kịp (xem §4.2, bằng chứng như `upgrade.md` §7.2).

### 3.3 Docker (volume có tên)

```powershell
# sau khi dừng container, dùng container dùng một lần để đóng gói và sao chép nội dung volume ra ngoài
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【đã kiểm chứng (2026-10-10, Docker Desktop máy cục bộ)】** Lệnh này đã được thực thi thực tế trên named volume của cluster 3 node (gói chứa `meta/`, `quorum/`),
> và dựa vào đó hoàn thành diễn tập phục hồi volume của một node (xem §4.2).

---

## 4. Các bước phục hồi

### 4.1 Máy đơn

```powershell
# 1) xác nhận tiến trình đã dừng
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) di chuyển (hoặc xóa) data_dir hiện tại, tránh tệp cũ và mới lẫn lộn
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) phục hồi bằng bản sao lưu
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) khởi động
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

Điểm mấu chốt:
- **Phải di chuyển thư mục cũ đi trước**, không được "đè tệp sao lưu lên thư mục còn sót lại một nửa";
- `data_dir` được phục hồi phải có **cùng tập vhost/queue** như khi sao lưu (tên thư mục là tên đã mã hóa, có thể dùng xuyên máy);
- **Không** nhân dịp phục hồi để sửa `vhosts`/`users` trong tệp cấu hình (chỉ có hiệu lực khi khởi tạo lần đầu, sửa cũng vô ích, xem `upgrade.md` §4.2).

### 4.2 Cluster

- Phục hồi một node: theo §4.1 phục hồi `data_dir` của node đó rồi khởi động, nó sẽ tham gia lại với tư cách thành viên sẵn có và đuổi kịp nhật ký Raft.
- Phục hồi toàn bộ cluster: **khôi phục và khởi động các node đa số trước** (≥ một nửa thành viên bỏ phiếu), cluster mới bầu được leader; sau đó phục hồi các node còn lại.
- **【đã kiểm chứng (2026-10-10, cluster 3 node Docker Desktop máy cục bộ)】** Đo thực tế "dừng n3 → xóa trắng volume của nó → ghi đè bằng bản sao lưu ở §3.3 → khởi động lại":
  n3 tham gia lại với tư cách follower và đuổi kịp leader (`commit_index == last_applied`), số message của queue trọng tài và bảng thành viên giữ nguyên.

---

## 5. Phương pháp kiểm tra sau khi phục hồi

Dùng API quản trị và client thực tế để đối chiếu chéo (khuyến nghị làm tất cả):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) tổng số đối tượng và tổng số message (số queue/số exchange/số binding/số người dùng; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) đối chiếu từng queue messages / messages_ready (có thể so với ghi chép trước khi sao lưu)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vhost / người dùng / policy có đủ cả không
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) cluster (máy đơn sẽ trả về enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Xem log khởi động**: phải xuất hiện `已从磁盘恢复队列消息 ... messages=N` và `队列已恢复持久化消息 ... messages=N`; N phải nhất quán với trước khi sao lưu.
- **Cảnh báo trong log**: nếu một queue nào đó trước đây từng có message bị consume/xóa trống, khi phục hồi có thể xuất hiện
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` và `恢复时清理了无存活消息的段`.
  Đây là tàn dư chỉ mục của **bản ghi đã kết toán (ack/purge)**, thuộc dạng **nhiễu log đã biết, không ảnh hưởng tính đúng đắn của dữ liệu** (xem §7).
- **Client thực tế**: lấy message từ queue về và đối chiếu số lượng/nội dung (xem bước (7) ở §6).

---

## 6. Diễn tập đo thực tế (lệnh và đầu ra thực)

> Môi trường: `data_dir` ở thư mục tạm, AMQP `127.0.0.1:5676`, mặt quản trị `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> tài khoản mặc định `guest/guest`. Log khởi động:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Tạo topology durable + gửi 5 message bền vững (client thực tế `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Tạo người dùng / vhost / quyền / policy (API quản trị)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Trạng thái trước khi sao lưu (API quản trị)**

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

**(4) Tệp trên đĩa trước khi sao lưu**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Dừng tiến trình → sao lưu → xóa trống → phục hồi**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Log phục hồi sau khi khởi động lại (các dòng then chốt)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Đồng thời xuất hiện một số dòng `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> Đây là tàn dư chỉ mục do các message **trước đó đã bị purge** trong buổi diễn tập này để lại (đã kết toán, dữ liệu phân đoạn đã được thu hồi), **không ảnh hưởng đến việc phục hồi 5 message dưới đây**.

**(7) Khẳng định sau phục hồi: API quản trị + client thực tế**

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

**Kết luận**: topology durable (exchange + queue + binding), 5 message bền vững, người dùng, vhost, quyền, policy **đều được phục hồi toàn bộ**,
client thực tế có thể lấy lại toàn bộ message nguyên trạng. **Diễn tập đạt.**

### 6.1 Đối chứng: queue chưa từng bị consume/purge thì phục hồi "yên ắng" hơn

Để phân biệt WARN ở trên có phải hiện tượng phổ biến hay không, làm thêm một lần **đối chứng có kiểm soát**: tạo mới queue durable `clean.q`, gửi 3 message bền vững,
**không consume không xóa trống**, dừng tiến trình rồi khởi động lại:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Không có WARN nào**. Điều này cho thấy WARN chỉ xuất hiện trong tình huống "trong chỉ mục còn lưu bản ghi đã kết toán" (xem §7).

### 6.2 Kill cứng node trong khi ghi (`docker kill`, 2026-10-11)

Cluster 3 node (`partition_policy=pause_minority`), queue trọng tài 3 bản sao `pwr.q`; client bật publisher confirms, mỗi message mang ID duy nhất, các ID đã confirm được ghi xuống đĩa để đối soát.

**Lượt 1 (`fsync: os`, kill follower n3)**: đang publish 50.000 message thì `docker kill pwr-n3`:

```
DONE published=50000 confirmed=50000 nacked=0 retries=0 elapsed=28.267s   # 杀后速率无可见变化
docker start pwr-n3 → 三节点 commit_index==last_applied==12，has_quorum=true
DRAIN total=50000 unique=50000 duplicates=0
confirmed_unique=50000 consumed_unique=50000 missing=0 extra=0
```

**Lượt 2 (`fsync: always`, kill leader trọng tài n1)**: đang publish 200.000 message thì `docker kill pwr-n1`:

```
DONE published=200000 confirmed=200000 nacked=3843 retries=3843 elapsed=2m7.494s
# nack 来自转发到已死 leader 的发布（"与节点 n1 通信失败"），客户端重投全部成功
docker start pwr-n1 → 三节点 commit_index==last_applied==16
DRAIN 对账：confirmed_unique=436785 consumed_unique=438140 missing=0 extra=1355
# extra=已落盘但 confirm 未返回的消息，at-least-once 允许，按 ID 去重即幂等
```

**Lượt 1b (`fsync: os`, `wsl --shutdown` dừng khẩn cấp toàn bộ Docker VM, gần tương đương mất điện toàn máy)**: dừng khẩn cấp trong khi publish, phía host đã ghi nhận 69.669 dòng confirm;
sau khi VM được tạo lại và cluster phục hồi:

```
{"name":"pwr.q","messages":69669,"messages_ready":69669}
DRAIN total=69669 unique=69669 duplicates=0 ；missing=0 extra=0
```

Lưu ý: `wsl --shutdown` chỉ mô phỏng gần đúng "mất bộ đệm OS", **không tương đương mất điện máy vật lý** (mất nguồn điện của host, tầng cache đĩa chưa được bao phủ, vẫn chưa kiểm chứng).

### 6.3 Ghi đồng thời trong quá trình phục hồi (2026-10-11)

Trên cluster `fsync: always`: `docker stop` follower n2 → publish tích lũy 50.000 message (confirm toàn bộ) → `docker start pwr-n2`,
**ngay cùng giây đó** tiếp tục publish 20.000 message vào các node đa số:

```
DONE published=20000 confirmed=20000 nacked=0 retries=0 elapsed=5.978s   # 约 3,300~6,400 条/秒
n2 日志：msg=已安装快照 component=broker index=4096 term=4              # 约 4 秒经快照追平
三节点 commit_index==last_applied==20；queue_totals.messages=82000       # 与四批发布总数精确相符
DRAIN total=82000 unique=80000 missing=0 extra=0                          # 2,000 重复为同 ID 重发批次的预期重复
```

Kết luận: ghi đồng thời trong lúc đuổi kịp không mất, không sai, đếm chính xác.

---

## 7. Vấn đề và giới hạn đã biết (ghi nhận trung thực)

1. **Nhiễu log phục hồi (quan sát thực tế)**: khi queue trong lịch sử từng xảy ra consume/xóa trống (message đã ack/purge),
   trong chỉ mục của nó vẫn giữ tham chiếu tới bản ghi đã thu hồi, khi phục hồi sẽ **in từng dòng WARN `恢复消息失败，已跳过` cho mỗi bản ghi**,
   và tạo mới/dọn một `000000.seg` rỗng (log `恢复时清理了无存活消息的段`).
   **Không ảnh hưởng tính đúng đắn của dữ liệu** (message chưa xác nhận còn sống được phục hồi đúng), nhưng sẽ **làm nhiễu log**, với queue lớn/throughput cao có thể tràn màn hình.
   Khuyến nghị: lấy `已从磁盘恢复队列消息 ... messages=N` làm chuẩn, bỏ qua các WARN nhắm vào bản ghi đã kết toán này;
   nếu lượng log không thể chấp nhận, hãy phản hồi cho người bảo trì nhân (tài liệu này không sửa mã).
2. **Sao lưu nóng không an toàn** (§2): không sao chép trực tiếp `data_dir` khi tiến trình đang chạy.
3. **`fsync: os` không đảm bảo mất điện không mất**: muốn "confirm là xuống đĩa" hãy dùng `batch` / `always`.
4. **Mật khẩu dạng văn bản thuần**: mật khẩu người dùng trong `meta/state.json` là **văn bản thuần** (đo thực tế thấy được `"password":"drillpass"`) ——
   do đó tệp sao lưu **phải được xử lý như dữ liệu nhạy cảm** (kiểm soát truy cập, lưu trữ mã hóa). Xem chi tiết ở `security-baseline.md`.
5. **【Trạng thái kiểm chứng】** Kill cứng tiến trình (bao gồm kill leader trọng tài trong khi ghi, cả hai mức `fsync: os`/`always`) và ghi đồng thời trong quá trình phục hồi (trong lúc đuổi kịp tiếp tục publish với khoảng 4.000~6.000 message/giây, đuổi kịp qua snapshot trong 4 giây, đếm chính xác, đối soát missing=0) — **【đã kiểm chứng (2026-10-11, cluster 3 node Docker Desktop máy cục bộ)】**; dùng `wsl --shutdown` dừng khẩn cấp toàn bộ Docker VM (gần tương đương mất điện toàn máy), mẫu một lần cho thấy message đã confirm không mất, nhưng **mất điện thật trên máy vật lý vẫn chưa kiểm chứng**; sao lưu/phục hồi cluster từng node và sao lưu/phục hồi Docker named volume — **【đã kiểm chứng (2026-10-10, cluster 3 node Docker Desktop máy cục bộ)】** (xem §3.2 / §3.3 / §4.2).
6. **Quan sát kèm theo (ghi nhận trung thực trong diễn tập 2026-10-11, không chặn)**: ① sau khi kill cứng leader trọng tài, đường chuyển tiếp publish của follower từng tiếp tục trỏ tới leader đã chết khoảng 76 giây (trong thời gian đó publish mới bị nack, client phát lại thành công, không mất message đã confirm) —— có cửa sổ chập chờn về khả dụng; ② queue lớn (438 nghìn message) drain một lần quan sát thấy phát lại khuếch đại khoảng 153 lần (ngữ nghĩa at-least-once vẫn đúng, cuối cùng dọn về 0 chính xác); ③ trong lúc queue lớn đuổi kịp, khởi động consumer qua follower thỉnh thoảng gặp `i/o timeout`, kết nối trực tiếp leader thì bình thường.
