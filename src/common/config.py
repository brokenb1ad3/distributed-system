"""Đọc cấu hình từ biến môi trường và file .env (không cần thư viện ngoài)."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def load_dotenv(path: Path | None = None) -> None:
    """Nạp file .env ở thư mục gốc repo. Biến đã có trong môi trường thì KHÔNG bị ghi đè."""
    path = path or ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ.setdefault(key.strip(), _clean_value(val))


def _clean_value(raw: str) -> str:
    """Bỏ dấu nháy bao quanh và chú thích cuối dòng của một giá trị trong .env.

    `10   # giây`      -> `10`       (chú thích bắt đầu bằng '#' đứng sau khoảng trắng, hoặc ở đầu giá trị)
    `"a # b"  # giây`  -> `a # b`    (trong dấu nháy thì '#' là một phần của giá trị)
    `abc#123`          -> `abc#123`  ('#' dính liền với chữ không phải chú thích, ví dụ token)
    """
    val = raw.strip()
    if val[:1] in ('"', "'"):
        end = val.find(val[0], 1)
        if end != -1:
            return val[1:end]
    if val.startswith("#"):
        return ""
    for sep in (" #", "\t#"):
        cut = val.find(sep)
        if cut != -1:
            val = val[:cut]
    return val.strip()


def env_str(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise SystemExit(f"[config] thiếu biến môi trường {name} (xem .env.example)")
    return val


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))
