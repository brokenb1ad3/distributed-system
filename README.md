# DS02: Hệ thống chia sẻ file P2P mô phỏng BitTorrent

Bài tập lớn môn Hệ thống phân tán. File được chia thành các chunk và tải song song từ nhiều peer.
Peer nào đã có chunk nào thì phục vụ lại chunk đó cho peer khác, kể cả khi chưa tải xong.
Tracker chỉ quản lý metadata và danh sách peer, **không bao giờ gửi nội dung file**.

## Kiến trúc

```
                 ┌───────────────────────────────┐
                 │ Tracker (HTTP/JSON, 1 process) │  metadata + danh sách peer + lease
                 └──────▲──────────────▲─────────┘
       register/announce│              │announce/peers        (HTTP)
        ┌───────────────┴──┐        ┌──┴───────────────┐
        │ peer-1 (seeder)  │◄──────►│ peer-2 (leecher) │      (TCP: GET_CHUNK / CHUNK / ERROR)
        └────────▲─────────┘        └────────▲─────────┘
                 │        ┌──────────────────┴┐
                 └───────►│ peer-3 (leecher)  │
                          └───────────────────┘
```

| Thành phần | File | Trách nhiệm |
|---|---|---|
| Tracker | `src/tracker/server.py` | Đăng ký metadata, nhận announce (kiêm heartbeat), trả danh sách peer, xóa peer hết lease, xác thực token |
| Metadata | `src/common/metadata.py` | Tạo và kiểm tra metadata: hash từng chunk, hash toàn file, `file_id` |
| Giao thức | `src/common/protocol.py` | Frame TCP giữa các peer, giới hạn kích thước |
| Lưu trữ | `src/peer/storage.py` | Chia chunk, lưu chunk, `state.json` để resume, ghép file và kiểm tra hash |
| Upload | `src/peer/uploader.py` | TCP server phục vụ `GET_CHUNK`, giới hạn số kết nối |
| Download | `src/peer/downloader.py` | Tải song song, rarest-first, timeout, retry, chuyển nguồn, loại chunk sai |
| Peer | `src/peer/main.py` | Ghép các phần trên thành lệnh `seed`, `get`, `serve`, `list` |

Chi tiết giao thức, mô hình nhất quán và cơ chế chịu lỗi: [docs/PROTOCOL.md](docs/PROTOCOL.md).

## Yêu cầu môi trường

- Python 3.10 trở lên. Code chạy **chỉ dùng thư viện chuẩn**, không cần cài gói nào.
- `pytest` để chạy test: `pip install -r requirements.txt`
- Windows Terminal (có sẵn trên Windows 11) để dùng `scripts/start_demo.ps1`; không có thì script mở các cửa sổ PowerShell riêng.

## Chạy nhanh: demo tự động

Script khởi động tracker và các peer dưới dạng **tiến trình riêng** trên máy, giao tiếp qua HTTP/TCP, rồi tự kiểm tra kết quả.

```bash
python scripts/demo.py all            # chạy cả 7 kịch bản
python scripts/demo.py failover       # hoặc từng kịch bản: basic partial failover corrupt resume concurrent bench
```

Log của từng node nằm ở `demo_run/<kịch bản>/logs/<node>.log`. Thêm `--quiet` để ẩn log trên màn hình.

## Chạy thủ công (mỗi node một cửa sổ terminal)

```bash
cp .env.example .env                  # rồi đổi TRACKER_TOKEN
python scripts/make_sample.py 20      # tạo sample/test.bin (20 MB)
cd src
```

PowerShell (Windows):

```powershell
# Cửa sổ 1: tracker
python -m tracker.server

# Cửa sổ 2: peer-1 chia sẻ file, in ra FILE_ID
$env:PEER_ID="peer-1"; $env:PEER_PORT="9001"; python -m peer.main seed ..\sample\test.bin

# Cửa sổ 3 và 4: peer-2, peer-3 tải file
$env:PEER_ID="peer-2"; $env:PEER_PORT="9002"; python -m peer.main get <FILE_ID>
$env:PEER_ID="peer-3"; $env:PEER_PORT="9003"; python -m peer.main get <FILE_ID>
```

Linux/macOS: thay bằng `PEER_ID=peer-1 PEER_PORT=9001 python -m peer.main seed ../sample/test.bin`.

File tải về nằm ở `data/<PEER_ID>/downloads/`. Chạy trên nhiều máy trong LAN: xem [deploy/README.md](deploy/README.md).

## Demo nhanh bằng `scripts/start_demo.ps1` (Windows)

Script mở sẵn một cửa sổ Windows Terminal chia 6 ô, mỗi ô là một node chạy như một tiến trình riêng:

```
| tracker  | peer-1 (seeder) | peer-3 (leecher) |
| công cụ  | peer-2 (seeder) | peer-4 (leecher) |
```

Mỗi ô đã đặt sẵn `PEER_ID`, `PEER_PORT`, `UPLOAD_DELAY` và hiện sẵn lệnh của node đó. Trước khi mở, script tự tắt các
tiến trình python còn chiếm cổng 8000, 9001–9007, xóa dữ liệu cũ trong `data/`, tạo `.env` nếu chưa có, tạo file mẫu
và tính sẵn `FILE_ID`.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\start_demo.ps1
```

Tùy chọn: `-SizeMB 20` (kích thước file mẫu, mặc định 40), `-Delay 0.5` (trễ upload mỗi chunk, mặc định 1 giây),
`-Keep` (giữ dữ liệu của lần chạy trước). Đợi khoảng 10 giây cho tới khi dựng xong 6 ô, không bấm vào cửa sổ trong lúc này.

Thứ tự thao tác:

1. Ô **tracker** → Enter.
2. Ô **peer-1** → Enter: tạo metadata và chia sẻ file.
3. Ô **peer-2** → Enter: seeder thứ hai.
4. Ô **peer-3**, ngay sau đó ô **peer-4** → Enter: hai peer cùng tải song song từ nhiều nguồn và chia chunk cho nhau.
5. Khi đang tải dở, ở ô **công cụ** gõ `kill1`: tắt đột ngột peer-1, các peer tự chuyển nguồn, tracker báo
   `PEER_EXPIRED` sau khoảng 10 giây.
6. Khi peer-3, peer-4 hiện `DOWNLOAD_COMPLETE`, gõ `hash`: SHA-256 của file tải về trùng với file gốc.
7. Gõ `evil`, bấm vào ô peer-5 vừa hiện ra và nhấn Enter; quay lại ô công cụ gõ `get6`: chunk sai bị loại
   (`CHUNK_REJECTED`).
8. (Tùy chọn) Gõ `get7`, bấm Ctrl+C giữa chừng rồi gõ `get7` lần nữa: tải tiếp từ chỗ dừng (`RESUME`).

| Lệnh trong ô công cụ | Tác dụng |
|---|---|
| `peers` | Xem tracker đang biết những peer nào, mỗi peer có bao nhiêu chunk |
| `kill1` | Tắt đột ngột peer-1 |
| `killport N` | Tắt đột ngột tiến trình trên cổng N (9001–9007) |
| `hash` | So SHA-256 file gốc với các file đã tải về |
| `evil` | Mở thêm ô peer-5 độc hại (luôn gửi chunk sai, chỉ dùng để demo) |
| `get6` | peer-6 tải file ngay trong ô công cụ |
| `get7` | peer-7 tải file, dùng để thử resume |

Ô nào chạy sai thì bấm vào ô đó, Ctrl+C rồi gõ `go` để chạy lại. Đóng cửa sổ Windows Terminal để tắt toàn bộ.
Script chỉ mở sẵn các node cho tiện; mỗi ô vẫn là một tiến trình Python độc lập, giao tiếp qua HTTP và TCP, giống
hệt khi tự gõ lệnh ở từng cửa sổ.

## Demo bằng bảng điều khiển web (`scripts/dashboard.py`)

Toàn bộ thao tác demo làm trên một trang web chạy trên máy cục bộ: bật/tắt từng node bằng nút bấm, xem log của
từng node, bản đồ chunk của từng peer và kiểm tra hash.

```powershell
python scripts\dashboard.py
```

Trình duyệt tự mở `http://localhost:8080`. Khi khởi động, script dọn dữ liệu của lần chạy trước, tạo `.env` nếu chưa
có, tạo file mẫu 40 MB (160 chunk) và tính sẵn `FILE_ID`. Mỗi nút "Bật" khởi động một tiến trình Python độc lập bằng
đúng lệnh như khi chạy thủ công (`python -m tracker.server`, `python -m peer.main seed ...`,
`python -m peer.main get <FILE_ID>`); nút "Tắt đột ngột" kết thúc hẳn tiến trình, không cho nó kịp báo tracker.
Các node vẫn chỉ giao tiếp với nhau qua HTTP và TCP; trang web không tham gia truyền chunk.

Trang gồm: hàng nút điều khiển; bảng peer và bản đồ chunk (đọc từ API `GET /files`, `GET /files/{file_id}/peers` của
tracker) kèm hàng "Độ phổ biến"; cột sự kiện (vào mạng, tải xong, rời mạng, hết lease); tám khung log, mỗi khung chứa
toàn bộ log của một tiến trình, có tô màu và tùy chọn "Chỉ hiện dòng quan trọng".

| Nút | Tác dụng |
|---|---|
| ↺ Dọn sạch, làm lại | Tắt mọi node, xóa dữ liệu và log, đưa về trạng thái ban đầu |
| 1 · Bật tracker | Khởi động tracker |
| 2 · Bật peer-1 (seed) | Seeder gốc tạo metadata và chia sẻ file |
| 3 · Bật peer-2 (seed) | Seeder thứ hai, cùng `FILE_ID` |
| 4 · Bật peer-3 + peer-4 | Hai leecher tải đồng thời và trao đổi chunk cho nhau |
| 5 · Tắt đột ngột peer-1 | Nguồn chết khi đang tải: `SOURCE_FAILED`, `CHUNK_RETRY` ở leecher, `PEER_EXPIRED` ở tracker |
| 6 · Kiểm tra hash | So SHA-256 file gốc với các file đã tải về |
| 7 · Bật peer-5 độc hại | Peer luôn gửi chunk hỏng (chỉ dùng để demo) |
| 8 · Bật peer-6 | Leecher tải khi có peer độc hại: `CHUNK_REJECTED` |
| 9 · Bật peer-7 | Tắt đột ngột rồi bật lại để thử tải tiếp (`RESUME`) |

Tùy chọn: `--size-mb` (kích thước file mẫu, mặc định 40), `--delay` (trễ upload mỗi chunk, mặc định 1 giây),
`--port` (cổng của trang, mặc định 8080), `--no-browser`. Nhấn Ctrl+C ở cửa sổ chạy script để tắt toàn bộ node.
Log của các node nằm ở `demo_run/web_logs/`.

## Triển khai trên nhiều máy (LAN)

Mỗi node có thể chạy trên một máy riêng trong cùng mạng: máy A chạy tracker, các máy khác chạy peer.
`deploy/run_node.ps1` tự dò IP LAN và đặt các biến môi trường cần thiết:

```powershell
powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role tracker                                                        # máy A
powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role seed -TrackerIp <IP máy A> -PeerId peer-1 -File sample\test.bin  # máy B
powershell -ExecutionPolicy Bypass -File deploy\run_node.ps1 -Role get  -TrackerIp <IP máy A> -PeerId peer-2 -FileId <FILE_ID>      # máy C
```

Mọi máy dùng chung `TRACKER_TOKEN` trong `.env` và cần mở cổng trên tường lửa. Hướng dẫn đầy đủ (chuẩn bị, tường lửa,
gây lỗi, xử lý sự cố): [deploy/README.md](deploy/README.md).

## Kiểm thử

```bash
pip install -r requirements.txt
pytest -q                             # 25 test: 10 chức năng, 3 đồng thời, 5 lỗi/phục hồi, 7 bảo mật
python scripts/demo.py bench          # thí nghiệm hiệu năng
```

Danh sách đầy đủ: [tests/TEST_PLAN.md](tests/TEST_PLAN.md).

## Kịch bản demo (theo mục 6.4 của đề)

| # | Yêu cầu của đề | Lệnh | Bằng chứng trong log |
|---|---|---|---|
| 1 | Tạo metadata và chia sẻ file | `demo.py basic` | `METADATA_CREATED`, `FILE_SHARED`, `FILE_REGISTERED` |
| 2 | Tải song song từ ≥ 2 peer | `demo.py basic` | `CHUNK_RECEIVED ... from=peer-1` và `from=peer-2` xen kẽ |
| 3 | Upload khi chưa tải xong | `demo.py partial` | `CHUNK_SENT ... (mình đang partial 8/64)` ở peer-2 |
| 4 | Tắt một nguồn khi đang tải | `demo.py failover` | `SOURCE_FAILED peer=peer-1`, tracker `PEER_EXPIRED` |
| 5 | Chuyển sang nguồn khác | `demo.py failover` | `CHUNK_RETRY ... xin lại từ nguồn khác`, sau đó `from=peer-2` |
| 6 | Phát hiện chunk sai | `demo.py corrupt` | `CHUNK_REJECTED ... hash=SAI` |
| 7 | Ghép file và kiểm tra hash cuối | mọi kịch bản | `FILE_VERIFIED`, `DOWNLOAD_COMPLETE` |
| + | Resume sau khi khởi động lại | `demo.py resume` | `RESUME ... khôi phục 22/64 chunk` |
| + | Đồng thời | `demo.py concurrent` | `UPLOAD_BUSY`, 3 leecher cùng hoàn tất |

## Bảo mật

- Peer xác thực với tracker bằng token dùng chung (`X-Peer-Token`), so sánh bằng `hmac.compare_digest`.
- Token đọc từ biến môi trường hoặc `.env`; `.env` nằm trong `.gitignore`.
- Kiểm tra mọi dữ liệu đầu vào: metadata, announce, frame TCP. Chặn path traversal trong tên file.
- Giới hạn kích thước: body HTTP 2 MB, header frame 4 KB, payload 1 MB, chunk tối đa 1 MB.
- Chống mạo danh `peer_id`; mọi chunk đều được kiểm tra SHA-256 trước khi ghi.
- Hạn chế đã biết: token dùng chung cho mọi peer, kết nối chưa mã hóa (TLS là hướng phát triển).

## Phân công

| Thành viên | MSSV | Phụ trách | File chính |
|---|---|---|---|
| Đặng Gia Bảo | B23DCCN069 | Tracker, phát hiện peer, xác thực, triển khai | `src/tracker/`, `src/peer/tracker_client.py`, `src/common/config.py`, `deploy/`, `tests/test_security.py` |
| Vũ Thế Anh | B23DCDT021 | Metadata, giao thức, lưu trữ, resume, upload | `src/common/metadata.py`, `protocol.py`, `log.py`, `src/peer/storage.py`, `uploader.py`, `tests/test_functional.py` |
| Nguyễn Duy Khánh | B23DCVT226 | Tải song song, chịu lỗi, demo, hiệu năng | `src/peer/downloader.py`, `main.py`, `scripts/demo.py`, `tests/test_concurrency.py`, `test_fault.py` |

## Hạn chế và hướng phát triển

Tracker là điểm lỗi đơn (chưa có bản sao); chưa có DHT, magnet link, choking/tit-for-tat, TLS.
