"""Điểm khởi chạy của container peer tải file (peer2, peer3, peer4 trong docker-compose.yml).

Container chỉ chạy MỘT tiến trình peer, nên tiến trình đó phải vừa tải vừa upload trên cổng PEER_PORT.
Vì vậy thay cho `serve` (chiếm cổng nhưng không tải gì), script này chọn file rồi chạy `peer.main get`:

    FILE_ID=<16 ký tự hex>   tải đúng file đó
    FILE_ID để trống         tải file đầu tiên tracker đang quản lý

Cả hai trường hợp đều chờ tới khi tracker có file (tối đa FILE_WAIT_SECONDS), vì các container khởi động
cùng lúc và seeder cần thời gian băm file trước khi đăng ký; `get` gặp 404 thì thoát ngay chứ không thử lại.
Chạy lại container thì `get` thấy dữ liệu cũ trong /data và tự resume, không cần chờ tracker.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

WAIT_SECONDS = int(os.getenv("FILE_WAIT_SECONDS", "120"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))


def log(msg: str) -> None:
    print(f"[peer_entry] {os.getenv('PEER_ID', '?')}: {msg}", flush=True)


def tracker_files() -> list[str]:
    """Các file_id tracker đang quản lý; rỗng nếu chưa có file hoặc tracker chưa sẵn sàng."""
    out = subprocess.run([sys.executable, "-m", "peer.main", "list"], capture_output=True, text=True)
    if out.returncode != 0:
        return []
    return [line.split()[0] for line in out.stdout.splitlines() if line.split()]


def main() -> None:
    wanted = os.getenv("FILE_ID", "").strip()
    file_id = wanted
    if wanted and (DATA_DIR / wanted / "meta.json").exists():
        log(f"FILE_ID={wanted} đã có dữ liệu trong {DATA_DIR}, tiếp tục từ đó")
    else:
        log(f"chờ tracker có file {wanted}" if wanted else "FILE_ID trống, chờ tracker có file được chia sẻ")
        deadline = time.monotonic() + WAIT_SECONDS
        while True:
            files = tracker_files()
            if wanted in files or (not wanted and files):
                file_id = wanted or files[0]
                break
            if time.monotonic() > deadline:
                raise SystemExit(f"[peer_entry] sau {WAIT_SECONDS}s tracker vẫn chưa có file {wanted or 'nào'}")
            time.sleep(1)
        log(f"FILE_ID={file_id} (lấy từ {'biến môi trường' if wanted else 'tracker'})")
    # thay tiến trình để peer nhận SIGTERM trực tiếp khi `docker compose stop`
    os.execv(sys.executable, [sys.executable, "-m", "peer.main", "get", file_id])


if __name__ == "__main__":
    main()
