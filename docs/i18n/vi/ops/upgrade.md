# Phương án nâng cấp và di trú SwiftMQ

> Phiên bản áp dụng: `1.0.0` (`broker.Version`, xem `swiftmq_build_info` của `/metrics`).
> Tất cả kết luận "đo thực tế" trong tài liệu này đều đến từ chạy thật trên máy cục bộ; những gì chưa đo thực tế đều được đánh dấu rõ ràng **【chưa kiểm chứng】**.
> Môi trường máy cục bộ: Windows + PowerShell 5.1, Go 1.27.1 windows/386, `data_dir` tạm + cổng không mặc định.

---

## 1. Di trú (chuyển từ RabbitMQ sang SwiftMQ)

Định vị của dự án này là **tương thích ở mức giao thức AMQP 0-9-1**, do đó "di trú" chủ yếu là **đổi địa chỉ kết nối**:

- Mã nghiệp vụ không cần thay đổi, chỉ đổi `host/port/vhost` (tài liệu thiết kế G3「di trú không tốn chi phí」).
- Chuỗi công cụ quản trị (`rabbitmqadmin`, UI quản trị, script giám sát) chỉ cần trỏ tới cổng mặt quản trị, hình dạng giao diện căn chỉnh theo RabbitMQ (các quy ước như `amq.default`, `%2F`, `{error, reason}` được bê nguyên).
- Cổng mặc định nhất quán với RabbitMQ: AMQP `5672`, mặt quản trị `15672`; MQTT là `1883`, RPC giữa các node là `25672`.

**Khác biệt ngữ nghĩa cần tự kiểm tra trước khi di trú** (đều là chủ ý của kho này, căn cứ theo README / tài liệu thiết kế):

| Mục | Hành vi SwiftMQ | Ảnh hưởng di trú |
| --- | --- | --- |
| Queue tạm thời (không bền vững và không độc quyền) | **Từ chối khai báo** (541), `auto_delete` không được miễn trừ | Client cũ nếu phụ thuộc loại queue này sẽ thất bại, cần đổi thành durable hoặc exclusive |
| vhost mặc định `/` | **Không thể xóa** (400), RabbitMQ cho phép | Script tự động hóa nếu xóa vhost mặc định sẽ thất bại (đây là ràng buộc an toàn chủ động duy nhất) |
| Dữ liệu queue cổ điển | **Không sao chép**, dữ liệu chỉ ở node Owner | Cần dự phòng xuyên node hãy đổi sang queue trọng tài `x-queue-type=quorum` |
| Queue trọng tài | Hỗ trợ mở rộng bản sao, **không hỗ trợ thu hẹp** | Khi quy hoạch cần tính toán một lần cho đủ |
| Plugin | Không có hệ sinh thái plugin Erlang, AMQP 1.0 / STOMP chưa được hiện thực | Tình huống dùng các giao thức này tạm thời chưa thể di trú |

**Di trú dữ liệu**: Định dạng lưu trữ của SwiftMQ và RabbitMQ không tương thích, **không cung cấp công cụ vận chuyển dữ liệu trực tuyến/ngoại tuyến**.
Cách di trú là "tạo SwiftMQ rỗng mới → chạy song song để kiểm chứng → chuyển lưu lượng theo kiểu gray". **【chưa kiểm chứng】** Tài liệu này không bao gồm bất kỳ diễn tập vận chuyển dữ liệu RabbitMQ thực tế nào.

---

## 2. Nguyên tắc chung khi nâng cấp

1. **Sao lưu trước** (xem `backup-restore.md`) —— phương án dự phòng khi nâng cấp thất bại.
2. **Dừng tiến trình trước rồi thay thế** (thư mục dữ liệu có ràng buộc người ghi đơn, xem §4.2).
3. **Sau khi nâng cấp phải kiểm tra**: tiến trình khởi động được, `/api/overview` đọc được, `/metrics` thu thập được, số message của queue nhất quán với trước khi sao lưu.
4. Nâng cấp cluster **cuốn chiếu từng node**, mỗi lần chỉ động vào một node (xem §5).

---

## 3. Bố cục thư mục dữ liệu (căn cứ thực tế cho nâng cấp/di trú)

Bố cục `data_dir` thu được từ **đo thực tế** instance máy đơn trên máy cục bộ:

```
data/
├── meta/
│   ├── state.json        # ảnh chụp siêu dữ liệu chế độ máy đơn (vhost/exchange/queue/binding/người dùng/quyền/policy)
│   ├── users.seeded      # dấu hiệu khởi tạo: users trong tệp cấu hình đã được gieo hạt
│   ├── vhosts.seeded     # dấu hiệu khởi tạo: vhosts trong tệp cấu hình đã được gieo hạt
│   ├── raft.state        # 【chế độ cluster】Nhiệm kỳ/bỏ phiếu Raft
│   ├── raft.log          # 【chế độ cluster】Nhật ký Raft
│   └── snapshot.json     # 【chế độ cluster】Ảnh chụp Raft + bảng thành viên
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # tệp phân đoạn (nội dung message + thuộc tính), định dạng bản ghi: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # chỉ mục queue: seq-id → (số phân đoạn, độ lệch trong phân đoạn, độ dài, trạng thái)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【cluster】queue trọng tài mỗi queue một nhóm Raft (nhật ký/ảnh chụp)
```

**Lưu ý (hai điểm khác với trực giác, đều lấy mã/đo thực tế làm chuẩn)**:

- Trong chế độ cluster, tệp lưu trữ bền vững của Raft **đặt trực tiếp dưới `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  **không tồn tại thư mục con `meta/raft/`**. Căn cứ: hằng số tên tệp của `internal/raft/log.go` + `internal/broker/cluster.go`
  với `Dir: filepath.Join(b.cfg.DataDir, "meta")`. **【bố cục cluster chưa đo thực tế】** (máy cục bộ chỉ chạy instance máy đơn).
- Tên thư mục **không phải tên vhost / queue gốc**, mà là mã hóa `store.SafeDirName`: thêm tiền tố `q_`, các byte không thuộc `[A-Za-z0-9._-]` được escape theo `%XX`.
  Đo thực tế: vhost `/` → thư mục `q_%2F`, queue `persist.q` → thư mục `q_persist.q`.
  Thiết kế như vậy để tránh path traversal và tên thiết bị dành riêng của Windows (`con`/`nul` v.v.).

---

## 4. Tính tương thích dữ liệu

### 4.1 Dữ liệu cũ có đọc trực tiếp được không —— được

- **Định dạng chỉ mục tương thích tiến**: M8-1 đã thêm trường "số phân đoạn" (25 byte) vào bản ghi chỉ mục; **định dạng cũ (21 byte, không có số phân đoạn) vẫn đọc được nguyên trạng**,
  khi đọc tương đương với "chỉ có một phân đoạn (seg=1)", **nâng cấp không cần script di trú**.
  Căn cứ: hằng số `indexEntrySize` / `legacyIndexEntrySize` và logic `recover()` của `internal/store/store.go`; README M8-1.
- **Ngữ nghĩa sự cố không đổi**: mỗi bản ghi có tiền tố độ dài + CRC32, khi phục hồi **loại bỏ bản ghi ghi dở/hỏng ở phần đuôi** và cắt bớt.
  Đo thực tế (xem `backup-restore.md` §6): sau khi dừng tiến trình rồi khởi động lại, 5 message bền vững của queue durable **được phục hồi toàn bộ**,
  log xuất hiện `已从磁盘恢复队列消息 ... messages=5`.

### 4.2 Cách hiểu về `vhosts` / `users` trong cấu hình (cái bẫy dễ vấp nhất khi nâng cấp)

- Cả hai **chỉ có hiệu lực khi khởi tạo lần đầu**: lần khởi động đầu tiên sẽ ghi vhosts/users trong cấu hình vào siêu dữ liệu và để lại tệp dấu hiệu
  `meta/vhosts.seeded` / `meta/users.seeded`; **sau đó lấy siêu dữ liệu làm chuẩn**.
- Do đó **khi nâng cấp/đổi cấu hình, đừng trông mong thêm bớt tài khoản hay vhost bằng cách sửa tệp cấu hình** —— sửa cũng không có hiệu lực;
  hãy dùng API quản trị hoặc `swiftmqctl`.
- Ngược lại, nâng cấp **sẽ không** dùng cấu hình để ghi đè tài khoản sẵn có: mật khẩu đã đổi trong lúc chạy sẽ không bị khởi động lại đẩy về giá trị cũ trong cấu hình,
  tài khoản đã xóa trong lúc chạy cũng không sống lại. Căn cứ: logic dấu hiệu gieo hạt của `cluster.go`; README M8-4 / M8-7.

### 4.3 Luân chuyển phân đoạn và thu hồi đĩa

- Message được phân đoạn theo kích thước (mặc định 8 MiB), **khi toàn bộ message trong phân đoạn đã ack và phân đoạn đã đóng lại thì xóa cả phân đoạn**, chỉ mục theo đó được nén và viết lại.
- Nâng cấp không thay đổi hành vi này; tệp phân đoạn đơn do instance cũ để lại vẫn hoạt động bình thường dưới logic luân chuyển phân đoạn mới.

---

## 5. Nâng cấp binary (máy trần)

> Máy cục bộ **chưa diễn tập thật xuyên phiên bản** (kho hiện chỉ có một phiên bản `1.0.0`, không có binary cũ để nâng cấp). Các bước dưới đây là **kiểm chứng tái hiện cùng phiên bản + quy trình chung** cho các khả năng kho này đã có, phần xuyên phiên bản được đánh dấu **【chưa kiểm chứng】**.

### 5.1 Các bước

```powershell
$base = "C:\swiftmq"
$data = "$base\data"

# 1) dừng tiến trình (thoát êm sẽ ghi đĩa kết thúc; xem §4「nhất quán」)
#    nếu chạy kiểu foreground: Ctrl+C; nếu chạy kiểu service: Stop-Service / Stop-Process
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) sao lưu thư mục dữ liệu (nhớ làm sau khi tiến trình đã dừng)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) thay thế binary (đặt swiftmqd.exe / swiftmqctl.exe phiên bản mới vào đường dẫn cũ)
#    Copy-Item .\new\swiftmqd.exe $base\swiftmqd.exe -Force

# 4) khởi động
& "$base\swiftmqd.exe" -config "$base\configs\swiftmqd.json" -log-level info

# 5) kiểm tra: tiến trình còn sống + API quản trị đọc được
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Danh sách kiểm tra sau khi nâng cấp

- Log khởi động xuất hiện `SwiftMQ 启动中 ... version=<新版本>` và `管理面已启动`;
- `object_totals` / `queue_totals` của `/api/overview` nhất quán với trước khi sao lưu (đối chiếu `backup-restore.md` §5);
- `messages` / `messages_ready` của mỗi queue durable trong `/api/queues` nhất quán với trước khi sao lưu;
- `/metrics` thu thập được và `swiftmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Nâng cấp image (container)

Image khoảng 13 MB (binary liên kết tĩnh + alpine), **chạy với quyền non-root (uid 10001)**, thư mục dữ liệu được mount tại `/var/lib/swiftmq`.

```powershell
# 1) kéo/build image mới (tag dùng số phiên bản mới, tránh nhầm lẫn old/new)
docker build -t swiftmq:1.0.0 .

# 2) dừng container cũ (compose sẽ giữ lại volume có tên swiftmq-data)
docker compose down

# 3) khởi động phiên bản mới (đổi image trong tệp compose sang tag mới)
docker compose up -d

# 4) trạng thái và log
docker compose ps
docker compose logs -f --tail 100
```

> **Tác vụ dùng một lần trong container** (ví dụ chạy `swiftmqctl` trong container): `run` của `docker compose ...` trong môi trường phi tương tác bắt buộc phải thêm `-T`,
> nếu không sẽ thất bại do xin cấp TTY:
> ```powershell
> docker compose run -T --rm broker swiftmqctl -user guest -pass guest status
> ```

Việc lưu trữ bền vững dữ liệu phụ thuộc **volume có tên** `swiftmq-data` của compose, tạo lại container không mất dữ liệu (từ M4 mới thực sự ghi xuống đĩa).
Nếu cần sao lưu nội dung volume trước khi nâng cấp, tương đương với sao lưu `/var/lib/swiftmq` (xem `backup-restore.md` §3.2). **【nâng cấp image chưa đo thực tế】** (máy cục bộ chưa chạy Docker).

---

## 7. Gray và rollback

### 7.1 Máy đơn

- **Gray**: SwiftMQ máy đơn không có sẵn khả năng "hai phiên bản cũ/mới trong cùng tiến trình". Cách gray khả thi là **bóng mờ song song**:
  instance phiên bản mới trước tiên dùng **consume chỉ đọc/queue bóng** móc vào cùng một nguồn lưu lượng thượng nguồn để quan sát, xác nhận không vấn đề rồi mới chuyển bên ghi.
- **Rollback**:
  1. dừng tiến trình phiên bản mới;
  2. đổi lại binary cũ;
  3. nếu phiên bản mới đã ghi dữ liệu, **phải dùng bản sao lưu trước khi nâng cấp để phục hồi `data_dir`** (xem dưới).
  **Sẽ không** có đảm bảo kiểu "phiên bản mới đã ghi rồi, phiên bản cũ đọc trực tiếp" —— hạ cấp xuyên phiên bản xem §8.

### 7.2 Cluster (nâng cấp cuốn chiếu)

Phía nền tảng không cung cấp "nâng cấp cuốn chiếu một nút", cần thao tác thủ công từng node theo thứ tự dưới đây:

1. **Mỗi lần chỉ nâng cấp một node**: dừng node đó → sao lưu `data_dir` của nó → đổi binary → khởi động → chờ nó tham gia lại và đuổi kịp
   (`swiftmqctl cluster_status` / `GET /api/cluster` xem `role`, `commit_index`/`last_applied`).
2. **Thứ tự khuyến nghị**: nâng **learner / thành viên không bỏ phiếu** trước (không ảnh hưởng tới đa số), rồi nâng **follower**, cuối cùng nâng **leader**
   (nâng leader sẽ kích hoạt một lần bầu chọn leader, trong thời gian đó có khoảng ngắn không ghi được).
3. **Ảnh hưởng của việc dừng tới đa số** (then chốt):
   - Cluster 3 node: **đồng thời dừng tối đa 1** thành viên bỏ phiếu, dừng 2 là mất đa số, dưới `pause_minority` **toàn bộ cluster tạm dừng phục vụ**.
   - Cluster 2 node: dừng 1 là mất đa số, **không có khả năng nâng cấp cuốn chiếu** (khuyến nghị tối thiểu 3 node).
   - Do đó khi nâng cấp cuốn chiếu **nghiêm cấm dừng nhiều thành viên bỏ phiếu cùng lúc**.
4. **Đừng làm thay đổi thành viên song song với nâng cấp**: thay đổi thành viên **không có joint consensus**, mỗi lần chỉ cho phép một thay đổi cấu hình chưa commit;
   trong thời gian nâng cấp hãy tránh đồng thời `add_member` / `remove_member`.
5. Sau khi nâng cấp xong, đối chiếu `object_totals` của `GET /api/cluster` nhất quán với trước khi nâng cấp.

> **【chưa kiểm chứng】** Máy cục bộ chưa diễn tập nâng cấp cuốn chiếu trên cluster thật (cả đường đi cluster và container đều chưa chạy); thứ tự trên đến từ các ràng buộc chung của
> middleware nhắn tin và Raft cũng như sự thật hiện thực `pause_minority` / thay đổi thành viên của kho này, không phải kết luận đo thực tế trên máy cục bộ.

---

## 8. Phần không hỗ trợ / chưa kiểm chứng (liệt kê rõ ràng)

- **Hạ cấp xuyên đại phiên bản: không hỗ trợ, chưa kiểm chứng**. Nếu phiên bản mới đã ghi dữ liệu bằng định dạng mới/ngữ nghĩa mới, **không có** đảm bảo "quay lại binary cũ đọc nguyên trạng";
  rollback chỉ có thể dựa vào bản sao lưu trước khi nâng cấp.
- **Định dạng cấu hình không đổi**: vẫn là JSON + biến môi trường `SWIFTMQ_*`. **Cấu hình YAML chưa được hỗ trợ** (cần đưa vào dependency phân tích, M8-17 đang chờ đánh giá),
  nâng cấp sẽ không mang lại YAML.
- **Nâng cấp nóng plugin/giao thức trực tuyến**: plugin được biên dịch kèm nhân (dạng A) hoặc `spawn` khởi động theo cấu hình (dạng B),
  nâng cấp nhân = khởi động lại tiến trình; **không có** cơ chế thay nóng binary tại chỗ.
- **Di trú tại chỗ engine lưu trữ**: luân chuyển phân đoạn/nén chỉ mục là hành vi chạy nền trong lúc chạy, **không có** lệnh "di trú/nén dữ liệu" độc lập.
- **Nâng cấp cluster dưới mạng thật**: kho này chỉ làm chaos thu nhỏ (kill ở mức tiến trình), **chưa làm** diễn tập nâng cấp dưới phân vùng mạng, đĩa ghi đầy.
- Tài liệu này **không bao gồm** bất kỳ kiểm chứng vận chuyển dữ liệu nào giữa SwiftMQ và broker khác (RabbitMQ).
