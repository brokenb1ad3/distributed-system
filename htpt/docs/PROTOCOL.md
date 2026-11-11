# Đặc tả giao thức (bản v0.1, cả nhóm phải thống nhất trước khi code)

## 1. Định danh

| Thực thể | Định danh | Cách tạo |
|---|---|---|
| Peer | `peer_id` | Lấy từ biến môi trường `PEER_ID` (vd `peer-1`), không trùng nhau |
| File | `file_id` | `sha256(file_hash + name + chunk_size)`, lấy 16 ký tự hex đầu |
| Chunk | `(file_id, index)` | `index` đánh số từ 0 |
| Message | `msg_id` | `uuid4().hex`, dùng để chống trùng và ghép request với response |

## 2. Metadata (file `<tên>.meta.json`, tương tự file .torrent)

```json
{
  "file_id": "a1b2c3d4e5f60718",
  "name": "video.mp4",
  "size": 10485760,
  "chunk_size": 262144,
  "num_chunks": 40,
  "chunk_hashes": ["<sha256 hex chunk 0>", "..."],
  "file_hash": "<sha256 hex toàn bộ file>",
  "tracker": "http://tracker:8000",
  "created_at": "2026-11-12T10:00:00Z"
}
```

## 3. Peer ↔ Tracker (HTTP/JSON)

Mọi request phải có header `X-Peer-Token: <TRACKER_TOKEN>`. Sai token thì trả `401`.
Tracker **chỉ** lưu metadata và danh sách peer, **không bao giờ** lưu hoặc gửi nội dung file.

| Method | Path | Body / Trả về | Ý nghĩa |
|---|---|---|---|
| POST | `/files` | body: metadata → `201` | Seeder đăng ký file |
| GET | `/files/{file_id}` | → metadata | Leecher lấy metadata |
| POST | `/announce` | body: xem dưới → `{"interval": 5}` | Báo các chunk đang có, kiêm luôn heartbeat |
| GET | `/files/{file_id}/peers` | → `[{peer_id, host, port, chunks, last_seen}]` | Danh sách peer còn sống |

Body của announce:
```json
{ "peer_id": "peer-2", "host": "peer2", "port": 9000, "file_id": "...",
  "chunks": [0, 1, 5, 7], "event": "started|update|completed|stopped" }
```

- Peer gửi announce mỗi `ANNOUNCE_INTERVAL` giây (mặc định 5s).
- Tracker xóa peer nếu sau `PEER_TTL` giây (mặc định 15s) không thấy announce. Đây là cơ chế lease/heartbeat.
- Khi nhận `event=stopped`, tracker xóa peer ngay.

## 4. Peer ↔ Peer (TCP, frame tự định nghĩa)

```
+----------------+------------------+---------------------+
| 4 byte (uint32)|  JSON header     |  payload (bytes)    |
| độ dài header  |  UTF-8           |  dài = payload_len  |
+----------------+------------------+---------------------+
```

- Giới hạn: header ≤ 4 KB, payload ≤ `MAX_CHUNK_SIZE` (1 MB). Vượt quá thì đóng kết nối và ghi log cảnh báo.
- Mỗi request có `msg_id`, response mang lại đúng `msg_id` đó.

| type | Hướng | Trường header | Payload |
|---|---|---|---|
| `GET_CHUNK` | leecher → seeder | `msg_id, from, file_id, index` | không có |
| `CHUNK` | seeder → leecher | `msg_id, file_id, index, payload_len, sha256` | dữ liệu chunk |
| `ERROR` | seeder → leecher | `msg_id, code, reason` | không có |

Mã lỗi (`code`): `NOT_FOUND` (không có chunk), `BUSY` (quá giới hạn upload), `BAD_REQUEST`.

## 5. Ngữ nghĩa và chịu lỗi khi tải

- **Timeout** mỗi request `GET_CHUNK`: `CHUNK_TIMEOUT` (mặc định 5s).
- **Retry có giới hạn:** tối đa `MAX_RETRIES` (3) lần cho mỗi chunk. Mỗi lần retry ưu tiên một peer **khác** peer vừa lỗi.
- **Hash sai:** loại chunk, đánh dấu peer đó là nghi ngờ (bỏ qua 30s), rồi tải lại từ peer khác.
- **Chống trùng:** chunk đã có (đã đúng hash) thì bỏ qua mọi response đến sau. Response mang `msg_id` lạ thì bỏ.
- **Chọn chunk:** rarest-first, tức chunk ít peer có nhất được tải trước.
- **Giới hạn đồng thời:** tải tối đa `MAX_PARALLEL` (4) chunk cùng lúc, phục vụ tối đa `MAX_UPLOADS` (8) kết nối.
- **Resume:** file `<data>/<file_id>.state.json` lưu danh sách chunk đã xác minh. Khi khởi động lại, chương trình kiểm tra lại hash các chunk này rồi tải tiếp phần còn thiếu.

## 6. Định dạng log (mọi node đều dùng)

```
2026-11-12T10:00:01.123Z | peer-2 | CHUNK_RECEIVED | file=a1b2 idx=5 from=peer-1 bytes=262144 ok
```
Cấu trúc: `thời gian | node | loại sự kiện | chi tiết + kết quả`.
