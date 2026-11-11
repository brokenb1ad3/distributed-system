# DS02: Hệ thống chia sẻ file P2P mô phỏng BitTorrent

Môn Hệ thống phân tán. File được chia thành các chunk, tải song song từ nhiều peer.
Tracker chỉ quản lý metadata và danh sách peer, **không bao giờ gửi nội dung file**.

## Kiến trúc

```
                 ┌──────────────────────────┐
                 │  Tracker (FastAPI/HTTP)  │  metadata + danh sách peer + lease
                 └──────▲─────────▲─────────┘
          announce/peers│         │announce/peers
        ┌───────────────┴──┐   ┌──┴───────────────┐
        │ peer-1 (seeder)  │◄─►│ peer-2 (leecher) │   TCP: GET_CHUNK / CHUNK
        └────────▲─────────┘   └────────▲─────────┘
                 └──────────►┌──────────┴───────┐
                             │ peer-3 (leecher) │
                             └──────────────────┘
```

Chi tiết giao thức: [docs/PROTOCOL.md](docs/PROTOCOL.md)

## Cấu trúc thư mục

```
src/common/    protocol.py (frame TCP), metadata.py, log.py
src/tracker/   app.py (tracker)
src/peer/      main.py, uploader.py, downloader.py, storage.py, tracker_client.py
deploy/        Dockerfile, docker-compose.yml
tests/         pytest + TEST_PLAN.md
scripts/       tạo file mẫu, gây lỗi
docs/          PROTOCOL.md, report.pdf, slides.pdf
```

## Chạy

```bash
cp .env.example .env              # đổi TRACKER_TOKEN
python scripts/make_sample.py 20  # tạo sample/test.bin (20 MB)
docker compose -f deploy/docker-compose.yml --env-file .env up --build
# tải file từ peer khác:
docker compose -f deploy/docker-compose.yml exec peer2 python -m peer.main get <file_id>
```

Chạy test: `pip install -r requirements.txt && pytest -q`

## Phân công

| Thành viên | MSSV | Phụ trách |
|---|---|---|
| A: ... | ... | Tracker, tracker_client, xác thực, Docker, script gây lỗi |
| B: ... | ... | Metadata, chia/ghép chunk, storage/resume, uploader |
| C: ... | ... | Downloader (song song, rarest-first, retry, chuyển nguồn), đo hiệu năng |

## Kịch bản demo

1. Tạo metadata và chia sẻ file (peer-1 seed)
2. peer-2, peer-3 cùng tải: log cho thấy chunk đến từ nhiều nguồn
3. peer-3 lấy chunk từ peer-2 khi peer-2 chưa tải xong
4. `scripts/fault_kill_peer.sh peer1`: tự chuyển sang nguồn khác
5. Peer độc hại gửi chunk sai: chunk bị loại
6. Ghép file và kiểm tra `file_hash`
7. Kill rồi chạy lại một leecher: resume từ chỗ đang dừng
