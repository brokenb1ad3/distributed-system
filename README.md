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
- Docker (không bắt buộc) nếu muốn chạy mỗi node một container.

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

File tải về nằm ở `data/<PEER_ID>/downloads/`. Chạy trên nhiều máy trong LAN: đặt `TRACKER_URL` trỏ tới máy chạy tracker
và `PEER_HOST` là IP LAN của từng máy.

## Chạy bằng Docker

Mỗi node là một container trên mạng `p2pnet`: `tracker`, `peer1` (seeder của `sample/test.bin`) và
`peer2`, `peer3`, `peer4` (tự tải file khi khởi động, vừa tải vừa upload cho nhau, tải xong thì tiếp tục seed).

```bash
cp .env.example .env                  # rồi đổi TRACKER_TOKEN
python scripts/make_sample.py 20      # tạo sample/test.bin (20 MB)
docker compose -f deploy/docker-compose.yml --env-file .env up --build
```

Chọn file để tải bằng biến `FILE_ID` (đặt trong `.env` hoặc biến môi trường khi gọi `docker compose`):

| `FILE_ID` | peer2, peer3, peer4 làm gì |
|---|---|
| để trống (mặc định) | hỏi tracker và tải file đầu tiên có trên đó |
| `<16 ký tự hex>` | tải đúng file đó; `FILE_ID` là giá trị peer1 in ra ở dòng `FILE_ID=...` |

Cả hai trường hợp, peer chờ tối đa `FILE_WAIT_SECONDS` (mặc định 120) cho tới khi tracker có file, vì các container
khởi động cùng lúc. Phần này do `deploy/peer_entry.py` xử lý rồi gọi `python -m peer.main get <FILE_ID>`.

Kiểm tra kết quả (mở terminal khác; `DC` là viết tắt của `docker compose -f deploy/docker-compose.yml --env-file .env`):

```bash
DC logs | grep -E "FILE_VERIFIED|DOWNLOAD_COMPLETE"     # PowerShell: DC logs | Select-String "FILE_VERIFIED|DOWNLOAD_COMPLETE"
DC exec peer2 python -m peer.main list                  # các file tracker đang quản lý
DC exec peer2 sha256sum /data/downloads/test.bin        # so với sha256 của sample/test.bin trên máy
```

Log mẫu của một lần chạy (file 20 MB, 80 chunk). `sources[...]` cho thấy mỗi peer lấy chunk từ cả seeder lẫn hai peer
đang tải dở:

```
peer-3 | FILE_VERIFIED     | file=e785284ba9a58d29 sha256=e8c5d9a24ba5e4bd… khớp metadata
peer-3 | DOWNLOAD_COMPLETE | ... time=3.773s sources[peer-1=55 peer-2=12 peer-4=13] retries=0 rejected=0
peer-2 | DOWNLOAD_COMPLETE | ... time=3.88s  sources[peer-1=56 peer-3=7 peer-4=17] retries=0 rejected=0
peer-4 | DOWNLOAD_COMPLETE | ... time=4.394s sources[peer-1=65 peer-2=8 peer-3=7] retries=0 rejected=0
```

Gây lỗi và phục hồi:

```bash
DC kill -s SIGKILL peer1      # tắt đột ngột seeder khi đang tải (hoặc ./scripts/fault_kill_peer.sh peer1)
                              # -> SOURCE_FAILED, CHUNK_RETRY ở các peer; PEER_EXPIRED ở tracker sau 10s
DC up -d peer1                # bật lại seeder -> các peer tải nốt chunk còn thiếu, FILE_VERIFIED
DC restart peer3              # khởi động lại peer -> đọc lại /data: RESUME hoặc ALREADY_COMPLETE
DC down -v                    # dừng và xóa dữ liệu để chạy lại từ đầu
```

Nếu tắt peer1 khi nó là nguồn duy nhất của một số chunk thì các peer còn lại dừng ở mức đã tải được và chờ;
chúng chỉ hoàn tất khi peer1 (hoặc một peer có đủ chunk) quay lại trong `STALL_TIMEOUT` (60s).

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
| A: ... | ... | Tracker, phát hiện peer, xác thực, triển khai | `src/tracker/`, `src/peer/tracker_client.py`, `src/common/config.py`, `deploy/`, `tests/test_security.py` |
| B: ... | ... | Metadata, giao thức, lưu trữ, resume, upload | `src/common/metadata.py`, `protocol.py`, `log.py`, `src/peer/storage.py`, `uploader.py`, `tests/test_functional.py` |
| C: ... | ... | Tải song song, chịu lỗi, demo, hiệu năng | `src/peer/downloader.py`, `main.py`, `scripts/demo.py`, `tests/test_concurrency.py`, `test_fault.py` |

## Hạn chế và hướng phát triển

Tracker là điểm lỗi đơn (chưa có bản sao); chưa có DHT, magnet link, choking/tit-for-tat, TLS
