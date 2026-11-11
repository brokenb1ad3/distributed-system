"""Tạo file mẫu ngẫu nhiên để demo. Dùng: python scripts/make_sample.py 20  (MB)"""
import os
import sys
from pathlib import Path

mb = int(sys.argv[1]) if len(sys.argv) > 1 else 20
out = Path(__file__).resolve().parent.parent / "sample" / "test.bin"
out.parent.mkdir(exist_ok=True)
with open(out, "wb") as f:
    for _ in range(mb):
        f.write(os.urandom(1024 * 1024))
print(f"wrote {out} ({mb} MB)")
