# Đặc tả giao thức (v1.0)

## 1. Định danh

| Thực thể | Định danh | Cách tạo |
|---|---|---|
| Peer | `peer_id` | Lấy từ biến môi trường `PEER_ID` (vd `peer-1`), không trùng nhau |
| File | `file_id` | `sha256("<file_hash>:<name>:<chunk_size>")`, lấy 16 ký tự hex đầu. Tracker và leecher đều tính lại để kiểm tra |
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

Mọi request (trừ `/health`) phải có header `X-Peer-Token: <TRACKER_TOKEN>`. Sai token thì trả `401`.
Tracker **chỉ** lưu metadata và danh sách peer, **không bao giờ** lưu hoặc gửi nội dung file.
Body tối đa 2 MB (`413` nếu vượt).

| Method | Path | Body / Trả về | Ý nghĩa |
|---|---|---|---|
| GET | `/health` | → `{"status":"ok"}` | Kiểm tra tracker còn sống |
| POST | `/files` | body: metadata → `201` (mới) hoặc `200` (đã có, idempotent) | Seeder đăng ký file |
| GET | `/files` | → `[{file_id, name, size, num_chunks, peers}]` | Liệt kê file |
| GET | `/files/{file_id}` | → metadata | Leecher lấy metadata |
| POST | `/announce` | body: xem dưới → `{"interval": 2, "ttl": 10}` | Báo các chunk đang có, kiêm heartbeat |
| GET | `/files/{file_id}/peers` | → `[{peer_id, host, port, chunks, last_seen, age}]` | Danh sách peer còn sống |

Body của announce:
```json
{ "peer_id": "peer-2", "host": "peer2", "port": 9000, "file_id": "...",
  "chunks": [0, 1, 5, 7], "event": "started|update|completed|stopped" }
```

- Peer gửi announce mỗi `ANNOUNCE_INTERVAL` giây (mặc định 2s).
- **Lease:** tracker xóa peer nếu quá `PEER_TTL` giây (mặc định 10s) không thấy announce.
- Khi nhận `event=stopped`, tracker xóa peer ngay.
- **Chống mạo danh:** một `peer_id` đang sống ở địa chỉ khác thì announce bị từ chối (`409`).
- Mã lỗi: `400` dữ liệu sai, `401` sai token, `404` không có file, `409` xung đột, `413` body quá lớn.
- Metadata được lưu xuống `TRACKER_DB` nên tracker khởi động lại không mất. Danh sách peer là trạng thái mềm,
  tự xây lại nhờ announce định kỳ. Nếu tracker mất cả metadata (trả `404` cho announce), peer tự đăng ký lại.

## 4. Peer ↔ Peer (TCP, frame tự định nghĩa)

```
+----------------+------------------+---------------------+
| 4 byte (uint32)|  JSON header     |  payload (bytes)    |
| độ dài header  |  UTF-8           |  dài = payload_len  |
+----------------+------------------+---------------------+
```

- Giới hạn: header ≤ 4 KB, payload ≤ 1 MB. Vượt quá, sai JSON hoặc sai `type` thì đóng kết nối và ghi log `PROTOCOL_VIOLATION`.
- Mỗi lần xin chunk dùng một kết nối TCP riêng.
- Mỗi request có `msg_id`, response mang lại đúng `msg_id` đó.

| type | Hướng | Trường header | Payload |
|---|---|---|---|
| `GET_CHUNK` | leecher → seeder | `msg_id, from, file_id, index` | không có |
| `CHUNK` | seeder → leecher | `msg_id, from, file_id, index, payload_len, sha256` | dữ liệu chunk |
| `ERROR` | seeder → leecher | `msg_id, code, reason` | không có |

Mã lỗi (`code`): `NOT_FOUND` (không có chunk), `BUSY` (quá giới hạn upload), `BAD_REQUEST`.

## 5. Ngữ nghĩa và chịu lỗi khi tải

- **Mô hình giao nhận:** at-least-once kết hợp chống trùng. Một chunk có thể được xin nhiều lần,
  nhưng chỉ được ghi đúng một lần (kiểm tra trong `ChunkStore.put` dưới khóa).
- **Mô hình nhất quán:** hội tụ cuối cùng (eventual). Mọi peer tải xong đều có file giống hệt nhau,
  được bảo đảm bằng hash từng chunk và hash toàn file. Danh sách peer ở tracker có thể cũ tối đa `PEER_TTL` giây.
- **Timeout** mỗi request `GET_CHUNK` (tính cả kết nối): `CHUNK_TIMEOUT` (mặc định 5s).
- **Retry có giới hạn:** tối đa `MAX_RETRIES` (3) lần cho mỗi chunk, mỗi lần ưu tiên một nguồn **khác** nguồn vừa lỗi.
  Hết lượt thì chunk bị hoãn 2s rồi thử lại. Nếu 60s không tải thêm được chunk nào, phiên tải dừng với lỗi.
- **Nguồn hỏng** (timeout, mất kết nối, sai giao thức): tạm ngưng nguồn đó 5s và chuyển nguồn khác ngay,
  không cần đợi tracker xóa.
- **Hash sai:** loại chunk, đánh dấu nguồn là nghi ngờ (bỏ qua 30s), tải lại từ nguồn khác.
- **Chống trùng và sai thứ tự:** response phải khớp `msg_id`, `file_id`, `index` của request, nếu không thì bỏ.
  Chunk đã có thì bỏ qua. Thứ tự tải không quan trọng vì file được ghép theo `index`.
- **Chọn chunk:** rarest-first, tức chunk ít peer có nhất được tải trước.
- **Giới hạn đồng thời:** tải tối đa `MAX_PARALLEL` (4) chunk cùng lúc, tối đa `PER_PEER_LIMIT` (2) request tới
  một peer, và mỗi peer phục vụ tối đa `MAX_UPLOADS` (8) kết nối (vượt thì trả `BUSY`).
- **Resume:** `<DATA_DIR>/<file_id>/state.json` lưu danh sách chunk đã xác minh, ghi theo kiểu file tạm rồi rename.
  Khi khởi động lại, chương trình kiểm tra lại hash các chunk trên đĩa rồi chỉ tải phần còn thiếu.
- **Tracker sập:** phiên tải đang chạy tiếp tục với các nguồn đã biết; announce thử lại với backoff tối đa 10s.

## 6. Định dạng log (mọi node đều dùng)

```
2026-11-12T10:00:01.123Z | peer-2 | CHUNK_RECEIVED | file=a1b2 idx=5 from=peer-1 bytes=262144 ok
```
Cấu trúc: `thời gian | node | loại sự kiện | chi tiết + kết quả`.

Các sự kiện chính: `FILE_REGISTERED`, `PEER_JOINED`, `PEER_ANNOUNCE`, `PEER_LEFT`, `PEER_EXPIRED`, `AUTH_FAILED` (tracker);
`METADATA_CREATED`, `FILE_SHARED`, `RESUME`, `SOURCE_DISCOVERED`, `CHUNK_RECEIVED`, `CHUNK_SENT`, `CHUNK_REJECTED`,
`CHUNK_RETRY`, `SOURCE_FAILED`, `UPLOAD_BUSY`, `FILE_VERIFIED`, `DOWNLOAD_COMPLETE` (peer).
