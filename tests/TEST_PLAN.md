# Kế hoạch kiểm thử (đề yêu cầu tối thiểu 8 + 2 + 2 + 2 + 1)

| ID | Loại | Kịch bản | Người | Trạng thái |
|---|---|---|---|---|
| F1 | Chức năng | Tạo metadata đúng số chunk, hash | B | |
| F2 | Chức năng | Chia rồi ghép lại, file giống hệt bản gốc | B | |
| F3 | Chức năng | Seeder đăng ký file lên tracker | A | |
| F4 | Chức năng | Announce và lấy danh sách peer | A | |
| F5 | Chức năng | Tải 1 chunk từ 1 peer | C | |
| F6 | Chức năng | Tải cả file từ ≥ 2 peer (log cho thấy chunk đến từ nhiều nguồn) | C | |
| F7 | Chức năng | Peer upload chunk khi chưa tải xong | B | |
| F8 | Chức năng | Rarest-first chọn đúng chunk hiếm nhất | C | |
| C1 | Đồng thời | 3 leecher cùng tải từ 1 seeder (giới hạn MAX_UPLOADS) | B | |
| C2 | Đồng thời | Nhiều luồng nhận cùng 1 chunk, chỉ ghi 1 lần (chống trùng) | C | |
| R1 | Lỗi | Kill seeder giữa chừng, tự chuyển nguồn, tải xong | C | |
| R2 | Lỗi | Kill leecher rồi khởi động lại, resume từ state.json | B | |
| R3 | Lỗi | Peer chết, tracker xóa sau PEER_TTL | A | |
| S1 | Bảo mật | Sai token, tracker trả 401 | A | |
| S2 | Bảo mật | Peer độc hại gửi chunk sai, chunk bị loại, peer bị đánh dấu nghi ngờ | C | |
| S3 | Bảo mật | Frame quá lớn hoặc sai định dạng, kết nối bị đóng | B | |
| P1 | Hiệu năng | Thời gian tải file 50 MB với 1, 2, 3, 4 nguồn | C | |
