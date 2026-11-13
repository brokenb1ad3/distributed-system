# Kế hoạch kiểm thử

Đề yêu cầu tối thiểu 8 chức năng + 2 đồng thời + 2 lỗi/phục hồi + 2 bảo mật + 1 hiệu năng.
Chạy toàn bộ: `pytest -q` (25 test, dùng tracker và peer thật trên TCP localhost).

| ID | Loại | Kịch bản | Hàm test | Người |
|---|---|---|---|---|
| F1 | Chức năng | Metadata đủ trường, đúng hash từng chunk và cả file | `test_functional.py::test_F1_*` | B |
| F2 | Chức năng | Chia rồi ghép lại, file giống hệt bản gốc | `test_F2_*` | B |
| F3 | Chức năng | Seeder đăng ký file, leecher lấy metadata; đăng ký lại là idempotent | `test_F3_*` | A |
| F4 | Chức năng | Announce, tìm peer, rời mạng chủ động (`stopped`) | `test_F4_*` | A |
| F5 | Chức năng | Tải 1 chunk qua TCP | `test_F5_*` | C |
| F6 | Chức năng | Tải song song từ 2 peer, cả hai nguồn đều được dùng | `test_F6_*` | C |
| F7 | Chức năng | Peer mới có nửa file vẫn upload cho peer khác | `test_F7_*` | B |
| F8 | Chức năng | Rarest-first chọn đúng chunk hiếm nhất | `test_F8_*` | C |
| F9 | Chức năng | Tracker không có đường dẫn nào trả nội dung file | `test_F9_*` | A |
| F10 | Chức năng | Đóng gói và đọc frame TCP (header, payload, độ dài) | `test_protocol_frame_roundtrip` | B |
| C1 | Đồng thời | 3 leecher cùng tải từ seeder giới hạn 2 kết nối, đều hội tụ | `test_concurrency.py::test_C1_*` | C |
| C2 | Đồng thời | 20 coroutine cùng ghi 1 chunk, chỉ 1 lần ghi thành công | `test_C2_*` | B |
| C3 | Đồng thời | 30 thread announce cùng lúc, tracker không mất cập nhật | `test_C3_*` | A |
| R1 | Lỗi | Nguồn chết giữa chừng, tự chuyển nguồn, tải xong | `test_fault.py::test_R1_*` | C |
| R2 | Lỗi | Khởi động lại: resume từ state.json, chunk hỏng trên đĩa bị loại | `test_R2_*` | B |
| R3 | Lỗi | Peer ngừng heartbeat, tracker xóa sau PEER_TTL | `test_R3_*` | A |
| R4 | Lỗi | Nguồn không trả lời (mất message): timeout rồi retry nguồn khác | `test_R4_*` | C |
| R5 | Lỗi | Tracker sập giữa lúc tải: vẫn tải xong; tracker bật lại còn metadata | `test_R5_*` | A |
| S1 | Bảo mật | Thiếu hoặc sai token: 401 | `test_security.py::test_S1_*` | A |
| S2 | Bảo mật | Peer độc hại gửi chunk sai: bị loại, peer bị nghi ngờ | `test_S2_*` | C |
| S3 | Bảo mật | Frame quá lớn, sai JSON, sai loại: đóng kết nối | `test_S3_*` | B |
| S4 | Bảo mật | Metadata sai, path traversal trong tên file: 400 | `test_S4_*` | B |
| S5 | Bảo mật | Mạo danh peer_id (409), announce sai dữ liệu (400) | `test_S5_*` | A |
| S6 | Bảo mật | Body quá 2 MB: 413 | `test_S6_*` | A |
| S7 | Bảo mật | Đọc `.env`: bỏ chú thích cuối dòng, giữ giá trị trong dấu nháy, không ghi đè biến môi trường | `test_S7_*` | A |
| P1 | Hiệu năng | Thời gian tải với 1, 2, 3, 4 nguồn | `python scripts/demo.py bench` | C |

## Kịch bản demo bằng tiến trình thật (`python scripts/demo.py <tên>`)

| Tên | Yêu cầu demo trong đề |
|---|---|
| `basic` | Tạo metadata và chia sẻ; tải song song từ ≥ 2 peer; ghép file và kiểm tra hash cuối |
| `partial` | Peer upload khi chưa tải xong |
| `failover` | Tắt một nguồn giữa chừng; chuyển sang nguồn khác |
| `corrupt` | Phát hiện chunk sai |
| `resume` | Tải tiếp sau khi khởi động lại (khuyến khích) |
| `concurrent` | Kịch bản đồng thời |
| `bench` | Thí nghiệm hiệu năng |
