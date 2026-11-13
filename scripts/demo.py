"""Chạy các kịch bản demo bằng TIẾN TRÌNH THẬT trên máy (1 tracker + nhiều peer, giao tiếp qua TCP/HTTP).

    python scripts/demo.py basic        tạo metadata, chia sẻ, tải song song từ 2 peer, ghép + kiểm tra hash
    python scripts/demo.py partial      peer upload khi CHƯA tải xong
    python scripts/demo.py failover     tắt 1 nguồn giữa chừng -> tự chuyển nguồn khác
    python scripts/demo.py corrupt      peer độc hại gửi chunk sai -> bị loại
    python scripts/demo.py resume       kill leecher giữa chừng -> chạy lại -> tải tiếp
    python scripts/demo.py concurrent   3 leecher cùng tải từ 1 seeder bị giới hạn kết nối
    python scripts/demo.py bench        đo thời gian tải với 1, 2, 3, 4 nguồn
    python scripts/demo.py all          chạy lần lượt tất cả

Tùy chọn: --size-mb 16  --delay 0.2  --quiet (không in log của các node ra màn hình)
Log từng node nằm ở demo_run/<kịch bản>/logs/<node>.log
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))
from common.metadata import create_metadata  # noqa: E402

TOKEN = "demo-token-khong-dung-that"
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def say(msg: str) -> None:
    print(f"\n>>> {msg}", flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(1 << 20):
            h.update(block)
    return h.hexdigest()


class Cluster:
    def __init__(self, name: str, args: argparse.Namespace):
        self.name = name
        self.args = args
        self.dir = ROOT / "demo_run" / name
        shutil.rmtree(self.dir, ignore_errors=True)
        (self.dir / "logs").mkdir(parents=True)
        self.tracker_port = args.base_port
        self.tracker_url = f"http://127.0.0.1:{self.tracker_port}"
        self.procs: dict[str, subprocess.Popen] = {}
        self.sample = self.dir / "sample.bin"
        with open(self.sample, "wb") as f:
            for _ in range(args.size_mb):
                f.write(os.urandom(1 << 20))
        self.meta = create_metadata(self.sample, self.tracker_url)
        self.file_id = self.meta["file_id"]
        self.total = self.meta["num_chunks"]

    # ---------- tiến trình ----------
    def _env(self, node: str, extra: dict | None = None) -> dict:
        env = dict(os.environ, PYTHONPATH=str(SRC), PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8",
                   TRACKER_TOKEN=TOKEN, TRACKER_URL=self.tracker_url,
                   LOG_FILE=str(self.dir / "logs" / f"{node}.log"))
        env.update({k: str(v) for k, v in (extra or {}).items()})
        return env

    def _spawn(self, node: str, argv: list[str], env: dict) -> subprocess.Popen:
        out = subprocess.DEVNULL if self.args.quiet else None
        proc = subprocess.Popen([sys.executable, "-m", *argv], cwd=SRC, env=env, stdout=out, stderr=out)
        self.procs[node] = proc
        return proc

    def start_tracker(self, ttl: float = 4.0) -> None:
        env = self._env("tracker", {"TRACKER_HOST": "127.0.0.1", "TRACKER_PORT": self.tracker_port, "PEER_TTL": ttl,
                                    "ANNOUNCE_INTERVAL": 1, "TRACKER_LOG_FILE": self.dir / "logs" / "tracker.log",
                                    "TRACKER_DB": self.dir / "tracker_db.json"})
        self._spawn("tracker", ["tracker.server"], env)
        for _ in range(50):
            try:
                OPENER.open(self.tracker_url + "/health", timeout=1)
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("tracker không khởi động được")

    def peer(self, n: int, *cmd: str, **extra) -> subprocess.Popen:
        node = f"peer-{n}"
        env = self._env(node, {"PEER_ID": node, "PEER_HOST": "127.0.0.1", "BIND_HOST": "127.0.0.1",
                               "PEER_PORT": self.args.base_port + n, "DATA_DIR": self.dir / node,
                               "ANNOUNCE_INTERVAL": 1, "UPLOAD_DELAY": self.args.delay, **extra})
        return self._spawn(node, ["peer.main", *cmd], env)

    def kill(self, n: int) -> None:
        node = f"peer-{n}"
        say(f"KILL {node} (tắt đột ngột, không kịp báo tracker)")
        self.procs[node].kill()
        self.procs[node].wait()

    def stop_all(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.terminate()
        for p in self.procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    # ---------- quan sát ----------
    def peers(self) -> dict[str, int]:
        req = urllib.request.Request(f"{self.tracker_url}/files/{self.file_id}/peers",
                                     headers={"X-Peer-Token": TOKEN})
        try:
            return {p["peer_id"]: len(p["chunks"]) for p in json.loads(OPENER.open(req, timeout=2).read())}
        except OSError:
            return {}

    def wait_have(self, n: int, at_least: int, timeout: float = 60) -> int:
        """Đợi tới khi tracker thấy peer-n có ít nhất `at_least` chunk."""
        end = time.time() + timeout
        while time.time() < end:
            have = self.peers().get(f"peer-{n}", -1)
            if have >= at_least:
                return have
            time.sleep(0.1)
        raise RuntimeError(f"peer-{n} không đạt {at_least} chunk sau {timeout}s")

    def wait_local(self, n: int, at_least: int, timeout: float = 60) -> int:
        """Đợi tới khi trên đĩa của peer-n có ít nhất `at_least` chunk."""
        chunk_dir = self.dir / f"peer-{n}" / self.file_id / "chunks"
        end = time.time() + timeout
        while time.time() < end:
            have = len(list(chunk_dir.glob("*.chunk"))) if chunk_dir.exists() else 0
            if have >= at_least:
                return have
            time.sleep(0.05)
        raise RuntimeError(f"peer-{n} không đạt {at_least} chunk trên đĩa sau {timeout}s")

    def wait_exit(self, n: int, timeout: float = 120) -> int:
        return self.procs[f"peer-{n}"].wait(timeout=timeout)

    def log(self, node: str) -> str:
        path = self.dir / "logs" / f"{node}.log"
        return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""

    def count(self, node: str, event_name: str, contains: str = "") -> int:
        return sum(1 for line in self.log(node).splitlines() if f"| {event_name} |" in line and contains in line)

    def sources(self, n: int) -> dict[str, int]:
        """Số chunk peer-n nhận từ mỗi nguồn, đếm trong log CHUNK_RECEIVED."""
        return dict(Counter(re.findall(r"\| CHUNK_RECEIVED \|.*? from=(\S+)", self.log(f"peer-{n}"))))

    def downloaded_ok(self, n: int) -> bool:
        out = self.dir / f"peer-{n}" / "downloads" / self.meta["name"]
        return out.exists() and sha256_file(out) == self.meta["file_hash"]

    def stats(self, n: int) -> dict:
        return json.loads((self.dir / f"peer-{n}" / self.file_id / "stats.json").read_text(encoding="utf-8"))


def check(results: list, label: str, ok: bool, detail: str = "") -> None:
    results.append(ok)
    print(f"    [{'PASS' if ok else 'FAIL'}] {label}" + (f"  ({detail})" if detail else ""), flush=True)


# ====================================================================== kịch bản
def scenario_basic(args) -> bool:
    c, r = Cluster("basic", args), []
    try:
        say("Khởi động tracker; peer-1 tạo metadata và chia sẻ file")
        c.start_tracker()
        c.peer(1, "seed", str(c.sample))
        c.wait_have(1, c.total)
        meta_file = c.dir / "peer-1" / f"{c.meta['name']}.meta.json"
        check(r, "tạo metadata đủ trường", meta_file.exists() and json.loads(meta_file.read_text())["num_chunks"] == c.total,
              f"file_id={c.file_id} chunks={c.total}")
        say("peer-2 tải file từ peer-1 rồi trở thành seeder")
        c.peer(2, "get", c.file_id)
        c.wait_have(2, c.total)
        say("peer-3 tải song song từ peer-1 và peer-2")
        c.peer(3, "get", c.file_id, "--exit")
        check(r, "peer-3 tải xong (exit code 0)", c.wait_exit(3) == 0)
        src = c.sources(3)
        check(r, "peer-3 nhận chunk từ ít nhất 2 nguồn", len(src) >= 2 and sum(src.values()) == c.total, str(src))
        check(r, "ghép file và hash cuối khớp metadata", c.downloaded_ok(3) and c.count("peer-3", "FILE_VERIFIED") == 1)
        check(r, "tracker không hề gửi nội dung file", "CHUNK" not in c.log("tracker"))
    finally:
        c.stop_all()
    return all(r)


def scenario_partial(args) -> bool:
    c, r = Cluster("partial", args), []
    try:
        say("peer-1 seed; peer-2 bắt đầu tải; peer-3 vào sau khi peer-2 mới có vài chunk")
        c.start_tracker()
        c.peer(1, "seed", str(c.sample))
        c.wait_have(1, c.total)
        c.peer(2, "get", c.file_id)
        have = c.wait_have(2, 4)
        check(r, "peer-2 mới tải một phần khi peer-3 vào", have < c.total, f"peer-2 có {have}/{c.total}")
        c.peer(3, "get", c.file_id, "--exit")
        check(r, "peer-3 tải xong", c.wait_exit(3) == 0 and c.downloaded_ok(3))
        c.wait_have(2, c.total)
        sent_partial = c.count("peer-2", "CHUNK_SENT", "partial")
        check(r, "peer-2 đã upload trong lúc CHƯA tải xong", sent_partial > 0,
              f"{sent_partial} chunk gửi đi khi còn 'partial'; peer-3 nhận từ {c.sources(3)}")
    finally:
        c.stop_all()
    return all(r)


def scenario_failover(args) -> bool:
    c, r = Cluster("failover", args), []
    try:
        say("peer-1 và peer-2 đều là seeder; peer-3 bắt đầu tải")
        c.start_tracker()
        c.peer(1, "seed", str(c.sample))
        c.peer(2, "seed", str(c.sample))
        c.wait_have(1, c.total), c.wait_have(2, c.total)
        c.peer(3, "get", c.file_id, "--exit")
        have = c.wait_local(3, c.total // 4)
        c.kill(1)
        t_kill = time.time()
        check(r, "nguồn bị tắt khi peer-3 đang tải dở", have < c.total, f"peer-3 có {have}/{c.total}")
        check(r, "peer-3 vẫn tải xong và hash đúng", c.wait_exit(3) == 0 and c.downloaded_ok(3))
        check(r, "peer-3 phát hiện nguồn hỏng và chuyển nguồn", c.count("peer-3", "SOURCE_FAILED", "peer=peer-1") > 0,
              f"SOURCE_FAILED x{c.count('peer-3', 'SOURCE_FAILED')}, nhận từ {c.sources(3)}")
        while "peer-1" in c.peers() and time.time() - t_kill < 15:
            time.sleep(0.1)
        check(r, "tracker xóa peer-1 khi hết lease", c.count("tracker", "PEER_EXPIRED", "peer=peer-1") == 1,
              f"sau {time.time() - t_kill:.1f}s")
    finally:
        c.stop_all()
    return all(r)


def scenario_corrupt(args) -> bool:
    c, r = Cluster("corrupt", args), []
    try:
        say("peer-1 trung thực; peer-2 ĐỘC HẠI (CORRUPT_RATE=1: luôn gửi chunk hỏng); peer-3 tải")
        c.start_tracker()
        c.peer(1, "seed", str(c.sample))
        c.peer(2, "seed", str(c.sample), CORRUPT_RATE=1)
        c.wait_have(1, c.total), c.wait_have(2, c.total)
        c.peer(3, "get", c.file_id, "--exit")
        check(r, "peer-3 vẫn tải xong", c.wait_exit(3) == 0)
        rejected = c.count("peer-3", "CHUNK_REJECTED", "from=peer-2")
        check(r, "phát hiện và loại chunk sai hash", rejected > 0, f"CHUNK_REJECTED x{rejected}")
        check(r, "không chunk nào của peer-2 được nhận", "peer-2" not in c.sources(3), str(c.sources(3)))
        check(r, "file cuối đúng hash", c.downloaded_ok(3))
    finally:
        c.stop_all()
    return all(r)


def scenario_resume(args) -> bool:
    c, r = Cluster("resume", args), []
    try:
        say("peer-1 seed; peer-3 tải rồi bị kill giữa chừng")
        c.start_tracker()
        c.peer(1, "seed", str(c.sample))
        c.wait_have(1, c.total)
        c.peer(3, "get", c.file_id, "--exit")
        c.wait_local(3, c.total // 3)
        c.kill(3)
        state = json.loads((c.dir / "peer-3" / c.file_id / "state.json").read_text())
        before = len(state["have"])
        check(r, "trạng thái đã lưu xuống đĩa trước khi bị kill", 0 < before < c.total, f"state.json có {before}/{c.total}")
        say("Khởi động lại peer-3 với cùng lệnh")
        c.peer(3, "get", c.file_id, "--exit")
        check(r, "peer-3 tải xong sau khi khởi động lại", c.wait_exit(3) == 0 and c.downloaded_ok(3))
        m = re.search(r"\| RESUME \|.*?khôi phục (\d+)/", c.log("peer-3"))
        check(r, "resume đúng số chunk đã có", bool(m) and int(m.group(1)) == before, f"RESUME {m.group(1) if m else '?'}")
        second_run = c.stats(3)
        got = sum(second_run["by_source"].values())
        check(r, "lần 2 chỉ tải phần còn thiếu", got == c.total - before, f"tải thêm {got} chunk")
    finally:
        c.stop_all()
    return all(r)


def scenario_concurrent(args) -> bool:
    c, r = Cluster("concurrent", args), []
    try:
        say("peer-1 seed với MAX_UPLOADS=3; peer-2, peer-3, peer-4 cùng lúc tải")
        c.start_tracker()
        c.peer(1, "seed", str(c.sample), MAX_UPLOADS=3)
        c.wait_have(1, c.total)
        for n in (2, 3, 4):
            c.peer(n, "get", c.file_id, "--exit")
        codes = [c.wait_exit(n) for n in (2, 3, 4)]
        check(r, "cả 3 leecher tải xong", codes == [0, 0, 0])
        check(r, "cả 3 file giống hệt bản gốc", all(c.downloaded_ok(n) for n in (2, 3, 4)))
        busy = c.count("peer-1", "UPLOAD_BUSY")
        check(r, "seeder giới hạn kết nối đồng thời", busy > 0, f"UPLOAD_BUSY x{busy}")
        swapped = {n: {k: v for k, v in c.sources(n).items() if k != "peer-1"} for n in (2, 3, 4)}
        check(r, "các leecher trao đổi chunk cho nhau", any(swapped.values()), str(swapped))
    finally:
        c.stop_all()
    return all(r)


def scenario_bench(args) -> bool:
    rows = []
    for n_sources in (1, 2, 3, 4):
        c = Cluster(f"bench_{n_sources}", args)
        try:
            c.start_tracker()
            for n in range(1, n_sources + 1):
                c.peer(n, "seed", str(c.sample))
            for n in range(1, n_sources + 1):
                c.wait_have(n, c.total)
            c.peer(9, "get", c.file_id, "--exit", MAX_PARALLEL=8, PER_PEER_LIMIT=2)
            if c.wait_exit(9) != 0 or not c.downloaded_ok(9):
                raise RuntimeError("tải thất bại")
            s = c.stats(9)
            rows.append((n_sources, s["elapsed"], s["bytes"] / s["elapsed"] / 1e6, s["by_source"]))
        finally:
            c.stop_all()
    say(f"KẾT QUẢ: file {args.size_mb} MB, {rows and c.total} chunk, mỗi nguồn giới hạn 2 kết nối, "
        f"trễ upload {args.delay}s/chunk")
    lines = ["| Số nguồn | Thời gian (s) | Tốc độ (MB/s) | Tăng tốc | Phân bố chunk |", "|---|---|---|---|---|"]
    for n, t, mbps, by in rows:
        lines.append(f"| {n} | {t:.2f} | {mbps:.2f} | x{rows[0][1] / t:.2f} | {by} |")
    table = "\n".join(lines)
    print(table)
    (ROOT / "demo_run" / "benchmark.md").write_text(table + "\n", encoding="utf-8")
    return rows[-1][1] < rows[0][1]


SCENARIOS = {"basic": scenario_basic, "partial": scenario_partial, "failover": scenario_failover,
             "corrupt": scenario_corrupt, "resume": scenario_resume, "concurrent": scenario_concurrent,
             "bench": scenario_bench}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=[*SCENARIOS, "all"])
    ap.add_argument("--size-mb", type=int, default=16)
    ap.add_argument("--delay", type=float, default=0.2, help="trễ upload mỗi chunk (giả lập băng thông)")
    ap.add_argument("--base-port", type=int, default=18000)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    summary = {}
    for name in names:
        print(f"\n{'=' * 20} KỊCH BẢN: {name} {'=' * 20}", flush=True)
        try:
            summary[name] = SCENARIOS[name](args)
        except Exception as e:
            print(f"    [FAIL] lỗi khi chạy: {e!r}")
            summary[name] = False
    print(f"\n{'=' * 20} TỔNG KẾT {'=' * 20}")
    for name, ok in summary.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    raise SystemExit(0 if all(summary.values()) else 1)


if __name__ == "__main__":
    main()
