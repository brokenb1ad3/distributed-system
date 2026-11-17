# Triển khai

Hệ thống gồm 1 tracker và nhiều peer. Mỗi node là một tiến trình Python độc lập, chỉ dùng thư viện chuẩn
(Python ≥ 3.10), giao tiếp qua mạng: peer với tracker qua HTTP (cổng 8000), peer với peer qua TCP (mỗi peer một cổng).
Có hai cách triển khai:

| Cách | Khi nào dùng | Công cụ |
|---|---|---|
| Nhiều tiến trình trên **một máy** | Demo, chạy thử, kiểm thử | `scripts/start_demo.ps1` (Windows) hoặc chạy tay theo README gốc |
| Mỗi node trên **một máy riêng trong LAN** | Thể hiện hệ thống chạy trên nhiều máy thật | `deploy/run_node.ps1` (Windows), `deploy/run_node.sh` (Linux/macOS) |

## 1. Một máy

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start_demo.ps1
```

Script mở Windows Terminal chia 6 ô: tracker, 2 seeder, 2 leecher và một ô công cụ (gây lỗi, kiểm tra hash, ...).
Chi tiết ở mục "Demo nhanh" trong README gốc.

## 2. Nhiều máy trong LAN

Ví dụ ba máy cùng một mạng Wi-Fi hoặc mạng dây:

| Máy | Vai trò | Ví dụ IP |
|---|---|---|
| A | tracker | 192.168.1.10 |
| B | peer-1, chia sẻ file | 192.168.1.11 |
| C | peer-2, tải file | 192.168.1.12 |

Có thể thêm máy D, E... chạy peer-3, peer-4 theo cách giống máy C. Một máy cũng có thể vừa chạy tracker vừa chạy
peer (mở hai cửa sổ).

### Bước 1: chuẩn bị trên mọi máy

1. Cài Python ≥ 3.10 và lấy mã nguồn (`git clone` repo).
2. Chép `.env.example` thành `.env` và đặt **cùng một** `TRACKER_TOKEN` trên tất cả các máy. Peer có token sai bị
   tracker từ chối (401).
3. Mở cổng trên tường lửa (Windows, chạy PowerShell bằng quyền Administrator, chỉ cần làm một lần):

   ```powershell
   powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role firewall
   ```

   Lệnh này thêm quy tắc "P2P DS02" cho cổng TCP 8000 và 9001–9010. Linux: mở các cổng tương ứng nếu có tường lửa
   (ví dụ `sudo ufw allow 8000,9001:9010/tcp`).

Script tự dò IP LAN của máy. Nếu máy có nhiều card mạng và dò sai, xem IP bằng `ipconfig` (Windows) hoặc `ip addr`
(Linux) rồi truyền vào `-Ip <địa chỉ>` (hoặc biến `IP=<địa chỉ>` với script `.sh`).

### Bước 2: máy A chạy tracker

```powershell
powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role tracker
```

Script in ra IP của máy A, các máy khác dùng IP này cho `-TrackerIp`.

### Bước 3: máy B chia sẻ file

```powershell
python scripts\make_sample.py 20      # nếu chưa có file để chia sẻ
powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role seed -TrackerIp 192.168.1.10 -PeerId peer-1 -File sample\test.bin
```

Ghi lại dòng `FILE_ID=...` in ra.

### Bước 4: máy C (và các máy khác) tải file

```powershell
powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role get -TrackerIp 192.168.1.10 -PeerId peer-2 -FileId <FILE_ID>
```

Mỗi peer phải có `-PeerId` khác nhau. Tải xong, peer tiếp tục làm seeder cho các máy khác. File tải về nằm ở
`data/<PEER_ID>/downloads/`.

Linux/macOS dùng `deploy/run_node.sh` với cùng các bước:

```bash
./deploy/run_node.sh tracker                                         # máy A
./deploy/run_node.sh seed 192.168.1.10 peer-1 9001 sample/test.bin   # máy B
./deploy/run_node.sh get  192.168.1.10 peer-2 9001 <FILE_ID>         # máy C
./deploy/run_node.sh list 192.168.1.10                               # xem các file tracker đang quản lý
```

### Tham số của `run_node.ps1`

| Tham số | Ý nghĩa |
|---|---|
| `-Role` | `tracker`, `seed`, `get`, `serve` (phục vụ file đã có), `list` (liệt kê file trên tracker) hoặc `firewall` (mở cổng trên tường lửa, cần quyền Administrator) |
| `-TrackerIp` | IP máy chạy tracker; bỏ trống nếu tracker chạy ngay trên máy này |
| `-TrackerPort` | Cổng tracker, mặc định 8000 |
| `-PeerId` | Tên peer, duy nhất trong mạng (`peer-1`, `peer-2`, ...) |
| `-Port` | Cổng upload của peer, mặc định 9001 |
| `-File`, `-FileId` | File để chia sẻ (`seed`) hoặc mã file cần tải (`get`) |
| `-Ip` | Ép dùng IP này cho máy hiện tại thay vì tự dò |

Script chỉ đặt các biến môi trường mà code đã hỗ trợ: `PEER_ID`, `PEER_PORT`, `PEER_HOST` (IP LAN của máy, để peer
khác kết nối tới), `BIND_HOST=0.0.0.0`, `TRACKER_URL`; tracker dùng `TRACKER_HOST=0.0.0.0`. Các tham số khác (`PEER_TTL`,
`MAX_PARALLEL`, ...) đọc từ `.env` như bình thường.

### Gây lỗi và kiểm tra

- Tắt đột ngột một peer: đóng cửa sổ của nó hoặc rút mạng máy đó. Các peer đang tải ghi `SOURCE_FAILED` rồi chuyển
  nguồn; tracker ghi `PEER_EXPIRED` sau `PEER_TTL` giây (mặc định 10).
- Kiểm tra file tải về: `Get-FileHash data\peer-2\downloads\test.bin` trên máy C, so với `Get-FileHash sample\test.bin`
  trên máy B.

### Sự cố thường gặp

| Hiện tượng | Nguyên nhân và cách xử lý |
|---|---|
| Peer báo `không kết nối được tracker` | Sai `-TrackerIp`, tracker chưa chạy, hoặc tường lửa máy A chặn cổng 8000 |
| Peer có trong danh sách nhưng không ai tải được từ nó (`SOURCE_FAILED ... CONNECT`) | Tường lửa máy đó chặn cổng upload, hoặc `PEER_HOST` sai do dò nhầm IP: chạy lại với `-Ip` |
| Các máy cùng Wi-Fi nhưng không kết nối được với nhau | Mạng (thường là Wi-Fi công cộng, Wi-Fi trường) bật chế độ cô lập máy khách; dùng mạng khác, ví dụ điểm phát Wi-Fi từ điện thoại |
| Tracker trả 401 | `TRACKER_TOKEN` trong `.env` của peer khác với máy tracker |
| `only one usage of each socket address` | Cổng đã có tiến trình khác dùng: đổi `-Port` hoặc tắt tiến trình cũ |
