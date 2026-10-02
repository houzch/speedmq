# Đường cơ sở tăng cường bảo mật SwiftMQ (danh sách có thể tick chọn)

> Nguyên tắc: **chỉ viết những khả năng kho này thực sự có**. Mỗi mục đưa ra "tại sao cần làm + làm sao kiểm tra đã làm", lệnh kiểm tra đều chạy được.
> Mục được đánh dấu **【đã kiểm chứng】** nghĩa là **đã thực sự thực thi** trên máy cục bộ (Windows + PowerShell 5.1, `1.0.0`);
> **【chưa kiểm chứng】** nghĩa là chưa thực thi hoặc hiện tại không làm được, tuyệt đối không giả vờ.
> Tất cả lệnh đưa ra phiên bản curl theo phong cách `/bin/sh`, kèm phiên bản PowerShell (PowerShell 5.1 hãy dùng
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Xác thực và kiểm soát truy cập

### A-1. Đổi tài khoản mặc định `guest/guest` 【đã kiểm chứng】

- **Tại sao**: mặc định tích hợp sẵn `guest/guest` (tag `administrator`), phơi ra bên ngoài chẳng khác nào mở toang cửa.
- **Làm thế nào**: đổi mật khẩu / xóa tài khoản trong lúc chạy, **đừng** sửa tệp cấu hình (`users` chỉ có hiệu lực khi khởi tạo lần đầu).

```bash
# đổi mật khẩu
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# hoặc xóa thẳng tài khoản mặc định (trước tiên đảm bảo đã tạo xong quản trị viên mới)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Cách kiểm tra**: sau khi đổi, mật khẩu cũ phải 401, mật khẩu mới 200.
  **【đã kiểm chứng】** Đầu ra đo thực tế trên máy cục bộ:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Tài khoản mặc định đã đổi/đã xóa

### A-2. Quyền tối thiểu: regex `configure` / `write` / `read` theo vhost 【đã kiểm chứng】

- **Tại sao**: ba loại căn chỉnh theo RabbitMQ —— `configure` quản khai báo/xóa topology, `write` quản publish và binding, `read` quản consume và pull;
  vượt quyền trả về 403. Với tài khoản nghiệp vụ chỉ mở những gì nó cần.
- **Làm thế nào**: `PUT /api/permissions/{vhost}/{user}`, ví dụ chỉ đọc consume: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Cách kiểm tra**: dùng tài khoản bị giới hạn thử thao tác vượt quyền, phải nhận 403 `ACCESS_REFUSED`.
  **【đã kiểm chứng】** Máy cục bộ dùng client AMQP thực tế với người dùng `configure="^$"` khai báo exchange, đo thực tế:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Mỗi tài khoản nghiệp vụ chỉ cấp regex cần thiết, và không cấp tag `administrator`/`management`

### A-3. Cách hiểu về tag `administrator` (quyền đầy đủ ngầm định) —— cấp phát cẩn trọng 【đã kiểm chứng】

- **Tại sao**: **người dùng có tag `administrator` có quyền đầy đủ trên tất cả vhost mà họ thấy được, không cần bản ghi quyền**
  (căn chỉnh theo cách hiểu đo thực tế của RabbitMQ, xem README / thiết kế M8-7). Nói cách khác, chỉ cần cấp tag này,
  regex quyền không còn tác dụng —— nó là quyền cao nhất.
- **Làm thế nào**: chỉ tài khoản mặt quản trị/vận hành mới cấp `administrator`; tài khoản nghiệp vụ nhất loạt không cấp tag, chỉ đi theo regex quyền.

- **Cách kiểm tra (minh họa quyền ngầm định)**: với một vhost mới **không có bất kỳ bản ghi quyền nào**, người dùng `administrator` phải dùng được trực tiếp.
  **【đã kiểm chứng】** Đo thực tế trên máy cục bộ: sau khi tạo mới vhost `drillvh` (chưa tạo bản ghi quyền nào), `guest` (administrator) khai báo topology trên đó thành công:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] Tag `administrator` chỉ cấp cho số rất ít tài khoản vận hành

### A-4. `remote_access`: giới hạn tài khoản chỉ đăng nhập từ máy cục bộ 【chưa kiểm chứng (cùng máy không thể mô phỏng nguồn từ xa)】

- **Tại sao**: căn chỉnh theo RabbitMQ, `guest` tích hợp mặc định chỉ cho phép đăng nhập từ máy cục bộ; khi triển khai ra ngoài nên đảm bảo nguồn của tài khoản đặc quyền bị giới hạn.
- **Làm thế nào / cách hiểu (giới hạn quan trọng)**:
  - `remote_access` chỉ có thể viết trong `users.<name>.remote_access` của **tệp cấu hình**, **chỉ có hiệu lực khi khởi tạo lần đầu**;
  - **Tài khoản tạo qua API quản trị / `swiftmqctl` nhất loạt `remote_access=true`** (cho phép đăng nhập từ mọi nguồn) ——
    căn cứ theo chú thích `UpsertUser` của `internal/broker/observe.go` và `"remote_access":true` đo thực tế trong `meta/state.json`. Nói cách khác **API hiện tại không thể giới hạn một tài khoản chỉ đăng nhập từ máy cục bộ**.
- **Cách kiểm tra**: dùng tài khoản đó kết nối từ **một máy chủ khác** (không phải `127.0.0.1`), phải bị 403; kết nối từ máy cục bộ phải thành công.
  **【chưa kiểm chứng】**: môi trường máy cục bộ không thể tạo nguồn từ xa thực, chưa đo thực tế.
- [ ] Giới hạn nguồn của tài khoản đặc quyền đã được đánh giá theo cách hiểu trên (lưu ý tạo tài khoản qua API mặc định mở nguồn từ xa)

---

## B. Bảo mật truyền tải (TLS)

Các mục cấu hình TLS (lớp truy cập và mặt quản trị **dùng chung** cùng một bộ trường): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. Bật TLS và cấu hình sai là từ chối khởi động 【đã kiểm chứng】

- **Tại sao**: chứng chỉ được đọc và kiểm tra **khi khởi động** —— cấu hình sai là từ chối khởi động ngay, chứ không đợi client đầu tiên kết nối mới lộ ra.
- **Làm thế nào**: cung cấp `cert_file` + `key_file` trong `listeners.<plugin>[].tls` hoặc `management.tls` (**đưa cả hai** mới bật).

- **Cách kiểm tra**: khởi động bằng cấu hình sai, phải thất bại ngay.
  **【đã kiểm chứng】** Ba cấu hình sai đo thực tế trên máy cục bộ, tất cả `exit=1`, từ chối khởi động:

  ```
  badtls1: swiftmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: swiftmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: swiftmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Cách kiểm tra (xuôi/ngược)**: client TLS kết nối được, client văn bản thuần kết nối cổng TLS sẽ bị từ chối.
  **【đã kiểm chứng】** Máy cục bộ dựng instance TLS (`amqp091` đi qua TLS), dùng probe client thực tế:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] Cổng giao thức hướng ngoài đã bật TLS

### B-2. `min_version` tối thiểu 1.2 【đã kiểm chứng】

- **Tại sao**: vô hiệu hóa phiên bản TLS quá cũ; mặc định là `1.2`, có thể chọn `1.2` / `1.3`.
- **Cách kiểm tra**: viết `min_version` thành `1.0`, khởi động phải báo lỗi (xem đầu ra `badtls2` ở B-1).
- [ ] `min_version` là `1.2` hoặc `1.3`

### B-3. Xác thực hai chiều `client_auth: require_and_verify` (mTLS) 【kiểm chứng một phần】

- **Tại sao**: yêu cầu client xuất trình và kiểm tra chứng chỉ, ngăn client chưa được ủy quyền truy cập cổng giao thức.
- **Làm thế nào**: cấu hình `ca_file` + `client_auth: require_and_verify` (hai cái sau yêu cầu đồng thời cung cấp `ca_file`).
- **【đã kiểm chứng】**: đường end-to-end TLS và đường bị từ chối đã được kiểm chứng bằng client thực tế (B-1). **mTLS (yêu cầu và kiểm tra chứng chỉ client) máy cục bộ chưa diễn tập riêng**.
- [ ] Cổng cần mTLS đã cấu hình `require_and_verify` + `ca_file`

### B-4. TLS mặt quản trị 【chưa kiểm chứng】

- **Tại sao**: mặt quản trị truyền mật khẩu qua Basic Auth, bắt buộc phải mã hóa.
- **Làm thế nào**: `management.tls` dùng cùng trường như listener giao thức.
- **【chưa kiểm chứng】**: diễn tập trên máy cục bộ buộc mặt quản trị vào cổng văn bản thuần cục bộ, chưa dựng riêng HTTPS cho mặt quản trị.
- [ ] Mặt quản trị đã bật TLS (hoặc được giới hạn nghiêm ngặt trong mạng tin cậy)

---

## C. Thu hẹp bề mặt phơi nhiễm

### C-1. Thu hẹp phạm vi lắng nghe của mặt quản trị 【đã kiểm chứng (đo thực tế địa chỉ lắng nghe)】

- **Tại sao**: mặt quản trị mặc định `:15672` (mọi card mạng). Triển khai ra ngoài nên buộc vào địa chỉ mạng nội bộ/loopback, hoặc dùng firewall giới hạn nguồn.
- **Làm thế nào**: cấu hình `management.addr` thành `127.0.0.1:15672` hoặc địa chỉ mạng nội bộ; hoặc `management.enabled=false` để tắt hẳn
  (sau khi tắt không có cổng quản trị, nhưng `swiftmqctl` cũng theo đó không dùng được).
- **Cách kiểm tra**:
  **【đã kiểm chứng】** Máy cục bộ cấu hình mặt quản trị thành `127.0.0.1:15677`, đo thực tế địa chỉ lắng nghe đúng là loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] Địa chỉ bind mặt quản trị đã thu hẹp (hoặc đã ngừng dùng)

### C-2. Cổng giao thức chỉ mở cái cần thiết 【chưa kiểm chứng】

- **Tại sao**: mặc định đồng thời mở AMQP `5672` và MQTT `1883`; không dùng MQTT thì tắt đi, giảm bề mặt tấn công.
- **Làm thế nào**: `plugins.mqtt.enabled=false` (hoặc bỏ khỏi `listeners`); ngừng dùng là **đóng cổng thật**, không chỉ đổi bit trạng thái.
- **Cách kiểm tra**: sau khi ngừng dùng, cổng tương ứng không còn lắng nghe (`Get-NetTCPConnection -State Listen` không thấy).
  **【chưa kiểm chứng】**: diễn tập trên máy cục bộ mở cả hai giao thức, chưa kiểm chứng riêng việc cổng biến mất sau khi tắt.
- [ ] Plugin giao thức không dùng đã bị vô hiệu hóa

---

## D. Tăng cường vận hành container

Sự thật về image của kho (`Dockerfile`): binary liên kết tĩnh + alpine, **chạy với quyền non-root (uid 10001, người dùng `swiftmq`)**,
thư mục dữ liệu `/var/lib/swiftmq` là volume. `docker-compose.yml` dùng **volume có tên** để lưu trữ bền vững, cấu hình **mount chỉ đọc**, luân chuyển log.

### D-1. Chạy non-root 【chưa kiểm chứng (máy cục bộ chưa chạy Docker)】

- **Tại sao**: quyền tối thiểu, giảm phạm vi ảnh hưởng sau khi container thoát ra.
- **Làm thế nào**: image mặc định đã là uid 10001; **đừng** dùng `--user root` để ghi đè.
- **Cách kiểm tra**: `docker compose run -T --rm broker id` phải hiển thị `uid=10001`. (`run` trong môi trường phi tương tác bắt buộc thêm `-T`)
- [ ] Container chạy non-root (chưa ghi đè bằng root)

### D-2. Hệ thống tệp gốc chỉ đọc + hạn mức tài nguyên + cắt tỉa capability (khuyến nghị, compose của kho chưa bật mặc định) 【chưa kiểm chứng】

- **Tại sao**: hệ thống tệp gốc chỉ đọc có thể ngăn việc sửa đổi binary trong lúc chạy; hạn mức tài nguyên ngăn một container đơn làm sập máy chủ; cắt tỉa capabilities giảm bề mặt tấn công nhân.
- **Làm thế nào** (ví dụ, hợp nhất vào service `broker` của compose theo nhu cầu):

```yaml
services:
  broker:
    read_only: true
    tmpfs:
      - /tmp
    security_opt:
      - no-new-privileges:true
    cap_drop: ["ALL"]
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 8g
```

- **Cách kiểm tra**: trong container thử ghi đường dẫn gốc phải thất bại (chỉ đọc); `docker inspect` thấy được hạn mức tài nguyên.
  **【chưa kiểm chứng】** (máy cục bộ chưa chạy Docker); và **hệ thống tệp gốc chỉ đọc cần xác nhận `data_dir` nằm trên volume ghi được**, nếu không nhân không thể ghi xuống đĩa.
- [ ] Đã đánh giá hệ thống tệp gốc chỉ đọc và hạn mức tài nguyên (lưu ý `data_dir` phải ở trên volume ghi được)

---

## E. Giới hạn đã biết (hiện tại thực sự không làm được, đừng trông mong)

Dưới đây đều là **khoảng trống mang tính sự thật**, hãy thừa nhận rõ ràng trong thiết kế bảo mật, đừng giả định chúng tồn tại:

1. **Mật khẩu được lưu trữ và sao chép dạng văn bản thuần**. Trường `password` trong `meta/state.json` là văn bản thuần (**【đã kiểm chứng】** đo thực tế thấy được
   `"password":"drillpass"`); trong tệp cấu hình cũng là văn bản thuần. **Không có** băm mật khẩu (băm và backend xác thực bên ngoài để dành cho plugin xác thực).
   → Hậu quả: **thư mục dữ liệu và tệp sao lưu tương đương thông tin xác thực nhạy cảm**, phải làm bảo vệ quyền tệp và mã hóa.
2. **Không có log kiểm toán**. Việc thêm/xóa/sửa trên mặt quản trị sẽ in log thông thường (như `管理面更新用户 actor=... user=...`),
   nhưng **không có** luồng kiểm toán độc lập, không thể sửa đổi, cũng không có bản ghi cấp tuân thủ kiểu "ai đã sửa cái gì vào lúc nào".
3. **Không có xác thực bên ngoài như LDAP / OAuth2 / JWT**. v1 tích hợp chỉ có `PLAIN` / `AMQPLAIN`
   (`auth.Store.Mechanisms()` đo thực tế chỉ trả về hai cái này).
4. **SASL `EXTERNAL` chưa được hiện thực**: kể cả cấu hình mTLS, lớp giao thức **vẫn đi xác thực mật khẩu PLAIN**
   (bước "dùng chứng chỉ client để miễn mật khẩu" không có). Chứng chỉ chỉ là kiểm tra ở lớp truyền tải.
5. **`remote_access` không thể đặt qua API**: tài khoản tạo bằng API/CLI quản trị nhất loạt cho phép đăng nhập từ xa (xem A-4),
   không thể giới hạn một tài khoản đơn chỉ đăng nhập từ máy cục bộ.
6. **Mặt quản trị không có danh sách trắng nguồn độc lập / không có giới hạn luồng**: chỉ có thể dựa vào địa chỉ bind, firewall, TLS để thu hẹp bề mặt phơi nhiễm.
7. **Không có sandbox plugin**: plugin dạng A cùng tiến trình với nhân; plugin ngoài dạng B tuy có cô lập tiến trình, nhưng **mặt dữ liệu đi qua proxy kết nối cục bộ**,
   và plugin có thể gọi ngữ nghĩa nhân (bị ràng buộc bởi vhost và kiểm tra quyền), **không phải** sandbox bảo mật.
8. **Tag mặt quản trị chỉ có ba mức `administrator`/`management`/`monitoring`**, không có RBAC theo từng tài nguyên chi tiết hơn.

---

## F. Danh sách tổng hợp

- [ ] A-1 Tài khoản mặc định đã đổi/đã xóa 【đã kiểm chứng quy trình】
- [ ] A-2 Tài khoản nghiệp vụ quyền tối thiểu (regex), không có tag quản trị 【đã kiểm chứng đường 403】
- [ ] A-3 Tag `administrator` chỉ cấp cho tài khoản vận hành 【đã kiểm chứng cách hiểu quyền ngầm định】
- [ ] A-4 Giới hạn nguồn tài khoản đặc quyền đã đánh giá (lưu ý tạo tài khoản qua API mặc định mở nguồn từ xa)
- [ ] B-1 Cổng hướng ngoài bật TLS, cấu hình sai là từ chối khởi động 【đã kiểm chứng】
- [ ] B-2 `min_version` ≥ 1.2 【đã kiểm chứng】
- [ ] B-3 Cổng cần mTLS cấu hình `require_and_verify` + `ca_file`
- [ ] B-4 Mặt quản trị bật TLS
- [ ] C-1 Địa chỉ bind mặt quản trị thu hẹp 【đã kiểm chứng địa chỉ lắng nghe】
- [ ] C-2 Plugin giao thức không dùng bị vô hiệu hóa
- [ ] D-1 Container chạy non-root
- [ ] D-2 Hệ thống tệp gốc chỉ đọc / hạn mức tài nguyên / cắt tỉa capability đã đánh giá
- [ ] E Giới hạn đã biết (mật khẩu văn bản thuần, không kiểm toán, không LDAP/OAuth2, SASL EXTERNAL chưa hiện thực) đã được thừa nhận trong thiết kế bảo mật
