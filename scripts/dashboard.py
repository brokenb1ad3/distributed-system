"""Bảng điều khiển web cho buổi demo: bật/tắt từng node, xem log, bản đồ chunk, kiểm tra hash.

    python scripts/dashboard.py                      # mở http://localhost:8080
    python scripts/dashboard.py --size-mb 20 --delay 0.5
    python scripts/dashboard.py --port 8090 --no-browser

Mỗi nút "Bật" khởi động một TIẾN TRÌNH Python độc lập, đúng như khi gõ tay:
    python -m tracker.server
    python -m peer.main seed ..\\sample\\test.bin
    python -m peer.main get <FILE_ID>
Các node vẫn chỉ nói chuyện với nhau qua mạng (HTTP với tracker, TCP giữa các peer).
Trang web chỉ làm hai việc: khởi động / tắt tiến trình, và hiển thị log cùng dữ liệu đọc từ API của tracker.
"""
from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import os
import random
import re
import shutil
import string
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
SAMPLE = ROOT / "sample" / "test.bin"
DATA = ROOT / "data"
LOGS = ROOT / "demo_run" / "web_logs"       # demo_run/ đã nằm trong .gitignore
sys.path.insert(0, str(SRC))

# .env phải có trước khi đọc cấu hình (tạo từ .env.example với token ngẫu nhiên nếu chưa có)
_env = ROOT / ".env"
if not _env.exists() and (ROOT / ".env.example").exists():
    tok = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))
    txt = (ROOT / ".env.example").read_text(encoding="utf-8")
    _env.write_text(re.sub(r"(?m)^TRACKER_TOKEN=.*$", f"TRACKER_TOKEN={tok}", txt), encoding="utf-8")

from common.config import env_str, load_dotenv  # noqa: E402
from common.metadata import create_metadata     # noqa: E402

load_dotenv()
TRACKER = env_str("TRACKER_URL", "http://127.0.0.1:8000").rstrip("/")
TRACKER_PORT = urlparse(TRACKER).port or 8000
TOKEN = env_str("TRACKER_TOKEN")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))
ARGS = argparse.Namespace(size_mb=40, delay=1.0)

# ------------------------------------------------------------------ các node của buổi demo
NODES = {
    "tracker": {"title": "Tracker", "role": "Danh bạ: metadata và danh sách peer", "kind": "tracker"},
    "peer-1": {"title": "peer-1", "role": "Seeder gốc", "kind": "seed", "port": 9001},
    "peer-2": {"title": "peer-2", "role": "Seeder thứ hai", "kind": "seed", "port": 9002},
    "peer-3": {"title": "peer-3", "role": "Leecher", "kind": "get", "port": 9003},
    "peer-4": {"title": "peer-4", "role": "Leecher (cùng lúc với peer-3)", "kind": "get", "port": 9004},
    "peer-5": {"title": "peer-5", "role": "ĐỘC HẠI: luôn gửi chunk hỏng", "kind": "seed", "port": 9005, "corrupt": True},
    "peer-6": {"title": "peer-6", "role": "Leecher khi có peer độc hại", "kind": "get", "port": 9006},
    "peer-7": {"title": "peer-7", "role": "Leecher để thử tắt rồi chạy lại (resume)", "kind": "get", "port": 9007},
}


def tracker_get(path: str):
    req = urllib.request.Request(TRACKER + path, headers={"X-Peer-Token": TOKEN})
    with OPENER.open(req, timeout=2) as resp:
        return json.loads(resp.read())


class Control:
    """Khởi động, tắt và theo dõi các tiến trình node."""

    def __init__(self):
        self.lock = threading.Lock()
        self.procs: dict[str, subprocess.Popen] = {}
        self.notes: list[dict] = []          # sự kiện do người demo bấm nút
        self.file_id: str | None = None
        self.hashes: list[dict] = []
        self.killed: set[str] = set()       # node do người demo chủ động tắt

    def note(self, kind: str, text: str):
        self.notes.append({"t": time.strftime("%H:%M:%S"), "kind": kind, "text": text})
        del self.notes[:-40]

    def ensure_sample(self):
        want = ARGS.size_mb * 1024 * 1024
        if not SAMPLE.exists() or SAMPLE.stat().st_size != want:
            SAMPLE.parent.mkdir(parents=True, exist_ok=True)
            with open(SAMPLE, "wb") as f:
                for _ in range(ARGS.size_mb):
                    f.write(os.urandom(1024 * 1024))
            self.file_id = None
        if self.file_id is None:
            self.file_id = create_metadata(SAMPLE, TRACKER)["file_id"]

    def status(self, name: str) -> dict:
        p = self.procs.get(name)
        if p is None:
            return {"state": "off"}
        code = p.poll()
        if code is None:
            return {"state": "running", "pid": p.pid}
        return {"state": "exited", "pid": p.pid, "code": code}

    def start(self, name: str) -> str:
        node = NODES[name]
        if self.status(name)["state"] == "running":
            return f"{name} đang chạy rồi"
        self.ensure_sample()
        LOGS.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8", TRACKER_URL=TRACKER, TRACKER_TOKEN=TOKEN)
        if node["kind"] == "tracker":
            env.update(TRACKER_HOST="0.0.0.0", TRACKER_PORT=str(TRACKER_PORT), PEER_TTL="10", ANNOUNCE_INTERVAL="2",
                       TRACKER_LOG_FILE=str(LOGS / "tracker.log"), TRACKER_DB=str(DATA / "tracker" / "tracker_db.json"))
            argv = [sys.executable, "-m", "tracker.server"]
        else:
            delay = 0.0 if node.get("corrupt") else ARGS.delay
            env.update(PEER_ID=name, PEER_PORT=str(node["port"]), PEER_HOST="127.0.0.1", BIND_HOST="0.0.0.0",
                       ANNOUNCE_INTERVAL="2", UPLOAD_DELAY=str(delay), CORRUPT_RATE="1" if node.get("corrupt") else "0",
                       DATA_DIR=str(DATA / name), LOG_FILE=str(LOGS / f"{name}.log"))
            argv = [sys.executable, "-m", "peer.main"]
            argv += ["seed", str(SAMPLE)] if node["kind"] == "seed" else ["get", self.file_id]
        self.killed.discard(name)
        out = open(LOGS / f"{name}.out", "ab")
        self.procs[name] = subprocess.Popen(argv, cwd=SRC, env=env, stdout=out, stderr=subprocess.STDOUT)
        out.close()
        self.note("start", f"Khởi động {name} (PID {self.procs[name].pid}): {' '.join(argv[1:])}".replace(str(ROOT), "."))
        return "ok"

    def kill(self, name: str) -> str:
        st = self.status(name)
        if st["state"] != "running":
            return f"{name} không chạy"
        self.killed.add(name)
        self.procs[name].kill()          # tắt ĐỘT NGỘT, không cho tiến trình kịp báo tracker
        self.procs[name].wait(timeout=5)
        self.note("kill", f"Tắt ĐỘT NGỘT {name} (PID {st['pid']}), không kịp báo tracker")
        return "ok"

    def kill_all(self):
        for name in list(self.procs):
            if self.status(name)["state"] == "running":
                self.procs[name].kill()
        for p in self.procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    def reset(self) -> str:
        self.kill_all()
        time.sleep(0.5)
        self.procs.clear()
        for d in (DATA, LOGS):
            shutil.rmtree(d, ignore_errors=True)
        self.hashes = []
        self.killed.clear()
        self.ensure_sample()
        WATCHER.clear()
        self.notes.clear()
        self.note("reset", f"Dọn sạch dữ liệu cũ. File mẫu {ARGS.size_mb} MB, FILE_ID = {self.file_id}")
        return "ok"

    def check_hash(self) -> str:
        def sha(p: Path) -> str:
            h = hashlib.sha256()
            with open(p, "rb") as f:
                while b := f.read(1 << 20):
                    h.update(b)
            return h.hexdigest()
        rows = [{"name": "File gốc (sample/test.bin)", "sha": sha(SAMPLE)}] if SAMPLE.exists() else []
        for p in sorted(DATA.glob("*/downloads/*")):
            rows.append({"name": f"{p.parent.parent.name} tải về", "sha": sha(p)})
        self.hashes = rows
        same = len(rows) > 1 and len({r["sha"] for r in rows}) == 1
        self.note("hash", f"Kiểm tra SHA-256 {len(rows)} file: " + ("TẤT CẢ TRÙNG NHAU" if same else "xem bảng kết quả"))
        return "ok"

    def log_lines(self, name: str, since: int) -> tuple[list[str], int]:
        """Các dòng log từ vị trí `since` trở đi, và tổng số dòng hiện có (trình duyệt tự nối thêm)."""
        log = LOGS / ("tracker.log" if name == "tracker" else f"{name}.log")
        if not log.exists():
            return [], 0
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
        if since > len(lines):
            since = 0
        return lines[since:], len(lines)

    def log_errors(self, name: str) -> list[str]:
        lines: list[str] = []
        st = self.status(name)
        if st["state"] == "exited" and st["code"] not in (0, None) and name not in self.killed:
            out = LOGS / f"{name}.out"
            if out.exists():                 # tiến trình tự lỗi (ví dụ trùng cổng): hiện thông báo lỗi, bỏ các dòng log đã có
                extra = [x for x in out.read_text(encoding="utf-8", errors="replace").splitlines()
                         if x.strip() and not re.match(r"^\d{4}-\d\d-\d\dT", x)]
                lines += [x for x in extra[-6:]]
        return lines


class Watcher:
    """Đọc API tracker: danh sách file, peer, chunk; phát hiện peer vào và biến mất."""

    def __init__(self):
        self.clear()

    def clear(self):
        self.lock = threading.Lock()
        self.known: dict[tuple[str, str], dict] = {}
        self.gone: dict[tuple[str, str], dict] = {}
        self.events: list[dict] = []
        self.meta: dict[str, dict] = {}

    def _event(self, kind: str, text: str):
        self.events.append({"t": time.strftime("%H:%M:%S"), "kind": kind, "text": text})
        del self.events[:-40]

    def snapshot(self) -> dict:
        try:
            files = tracker_get("/files")
        except (urllib.error.URLError, OSError, ValueError):
            return {"ok": False, "files": []}
        now = time.time()
        out = []
        with self.lock:
            for f in files:
                fid, n = f["file_id"], f["num_chunks"]
                if fid not in self.meta:
                    self.meta[fid] = True
                    self._event("file", f"Tracker nhận metadata {f['name']}: {n} chunk")
                try:
                    peers = tracker_get(f"/files/{fid}/peers")
                except (urllib.error.URLError, OSError, ValueError):
                    peers = []
                seen = set()
                for p in peers:
                    key = (fid, p["peer_id"])
                    seen.add(key)
                    old = self.known.get(key)
                    if old is None:
                        self.gone.pop(key, None)
                        self._event("join", f"{p['peer_id']} vào mạng, có {len(p['chunks'])}/{n} chunk")
                    elif len(old["chunks"]) < n <= len(p["chunks"]):
                        self._event("done", f"{p['peer_id']} có đủ {n}/{n} chunk, trở thành seeder")
                    self.known[key] = dict(p, seen_at=now)
                for key in [k for k in self.known if k[0] == fid and k not in seen]:
                    p = self.known.pop(key)
                    if now - p["seen_at"] + p.get("age", 0) > 4:
                        self._event("expired", f"{p['peer_id']} ngừng báo danh quá 10 s, tracker xóa (PEER_EXPIRED)")
                        reason = "hết lease"
                    else:
                        self._event("left", f"{p['peer_id']} rời mạng (PEER_LEFT)")
                        reason = "rời mạng"
                    self.gone[key] = dict(p, gone_at=now, reason=reason)
                for key in [k for k, g in self.gone.items() if now - g["gone_at"] > 20]:
                    del self.gone[key]
                rows = [dict(p, state="online") for p in peers]
                rows += [dict(g, state="gone", ago=round(now - g["gone_at"])) for k, g in self.gone.items() if k[0] == fid]
                rows.sort(key=lambda r: int(r["peer_id"].split("-")[-1]) if r["peer_id"].split("-")[-1].isdigit() else 99)
                out.append({"file_id": fid, "name": f["name"], "size": f["size"], "num_chunks": n, "peers": rows})
        return {"ok": True, "files": out}


CONTROL = Control()
WATCHER = Watcher()
atexit.register(CONTROL.kill_all)


def state(since: dict[str, int]) -> dict:
    snap = WATCHER.snapshot()
    events = sorted(WATCHER.events + CONTROL.notes, key=lambda e: e["t"], reverse=True)[:40]
    nodes = []
    for k in NODES:
        new, total = CONTROL.log_lines(k, since.get(k, 0))
        nodes.append(dict(name=k, **{kk: v for kk, v in NODES[k].items() if kk != "kind"}, **CONTROL.status(k),
                          log=new, total=total, errors=CONTROL.log_errors(k)))
    return {"tracker_ok": snap["ok"], "files": snap["files"], "events": events, "nodes": nodes,
            "file_id": CONTROL.file_id, "hashes": CONTROL.hashes, "size_mb": ARGS.size_mb, "delay": ARGS.delay}


PAGE = r"""<!DOCTYPE html>
<html lang="vi"><head><meta charset="utf-8"><title>DS02 · Bảng điều khiển demo P2P</title>
<style>
:root{--ink:#0F2B3A;--teal:#0E9F8E;--amber:#F2A33A;--red:#D9534F;--steel:#3B6E8F;--mute:#5A6B78;--panel:#EEF4F5;--line:#D7E2E5;--term:#14191F}
*{box-sizing:border-box}body{margin:0;font-family:Calibri,"Segoe UI",Arial,sans-serif;color:#1B1F24;background:#fff}
header{background:var(--ink);color:#fff;padding:12px 24px;display:flex;align-items:baseline;gap:16px}
header h1{font-family:Cambria,Georgia,serif;font-size:24px;margin:0}header .sub{color:#BFE3DE;font-size:14px}
header .clock{margin-left:auto;font-size:14px;color:#BFE3DE}
.steps{display:flex;flex-wrap:wrap;gap:8px;padding:12px 24px;background:var(--panel);border-bottom:1px solid var(--line)}
button{font:inherit;font-size:15px;font-weight:700;border:0;border-radius:8px;padding:8px 14px;cursor:pointer;color:#fff;background:var(--teal)}
button:hover{filter:brightness(1.08)}button.kill{background:var(--red)}button.ghost{background:#fff;color:var(--ink);border:1px solid var(--line)}
button.amber{background:var(--amber)}button.steel{background:var(--steel)}button:disabled{opacity:.4;cursor:default}
.steps .n{display:inline-block;width:20px;height:20px;border-radius:10px;background:rgba(255,255,255,.25);text-align:center;line-height:20px;margin-right:6px;font-size:12px}
main{display:grid;grid-template-columns:1fr 340px;gap:20px;padding:16px 24px}
.file h2{font-size:18px;margin:0 0 2px;color:var(--ink)}.meta{color:var(--mute);font-size:13px;margin-bottom:8px}
table{width:100%;border-collapse:collapse;table-layout:fixed}th{text-align:left;font-size:12px;color:var(--mute);font-weight:600;padding:4px 6px;border-bottom:2px solid var(--line)}
td{padding:6px;border-bottom:1px solid var(--line);vertical-align:middle;font-size:14px}
.name{font-weight:700;font-size:15px;color:var(--ink)}.addr{color:var(--mute);font-size:12px}
.badge{display:inline-block;padding:2px 9px;border-radius:12px;font-size:12px;font-weight:700;color:#fff;white-space:nowrap}
.seed{background:var(--teal)}.leech{background:var(--steel)}.stale{background:var(--amber)}.gone{background:var(--red)}
.grid{display:grid;grid-template-columns:repeat(40,1fr);gap:2px}.c{height:8px;border-radius:2px;background:#E3EAEC}.c.on{background:var(--teal)}
tr.goneRow td{opacity:.45}tr.goneRow .c.on{background:var(--red)}
.pct{font-weight:700;color:var(--ink)}
.empty{color:var(--mute);font-size:15px;padding:16px 0}
aside{background:var(--panel);border-radius:12px;padding:12px 16px;max-height:470px;overflow:auto}
aside h3,.sec h3{margin:0 0 8px;font-family:Cambria,Georgia,serif;color:var(--ink);font-size:17px}
.ev{font-size:13px;padding:5px 0;border-bottom:1px solid var(--line)}.ev .t{color:var(--mute);margin-right:6px}
.ev .k{font-weight:700}.ev.join .k{color:var(--teal)}.ev.done .k{color:var(--steel)}.ev.expired .k,.ev.kill .k{color:var(--red)}
.ev.left .k{color:var(--amber)}.ev.start .k,.ev.file .k,.ev.reset .k,.ev.hash .k{color:var(--ink)}
.sec{padding:4px 24px 16px}
.nodes{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}
.node{background:var(--term);border-radius:10px;overflow:hidden;display:flex;flex-direction:column;min-height:230px}
.node .hd{display:flex;align-items:center;gap:8px;padding:8px 10px;background:#1E2731;color:#fff}
.node .hd b{font-size:15px}.node .hd .r{font-size:12px;color:#9FB3BF;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.dot{width:10px;height:10px;border-radius:5px;background:#55606B}.dot.running{background:#2ECC9A}.dot.exited{background:var(--amber)}
.node .hd button{font-size:12px;padding:4px 9px}
.log{font-family:Consolas,"Courier New",monospace;font-size:11.5px;line-height:1.35;color:#C9D4DA;padding:6px 10px;height:280px;overflow:auto;white-space:pre-wrap;word-break:break-all}
.log div{padding:1px 0}.quiet .log .noise{display:none}.errs{color:#FF7A73;font-family:Consolas,monospace;font-size:11.5px;padding:4px 10px;border-top:1px solid #3A2222;white-space:pre-wrap}
.tog{font-size:15px;font-weight:400;color:var(--mute);margin-left:14px;cursor:pointer}
.log .e{font-weight:700}.l-ok{color:#5BE0B3}.l-bad{color:#FF7A73}.l-warn{color:#FFC25C}.l-info{color:#7FC8F8}.l-err{color:#FF7A73}
.hash table{max-width:900px}.hash td{font-family:Consolas,monospace;font-size:13px}.same{color:var(--teal);font-weight:700}.diff{color:var(--red);font-weight:700}
</style></head><body>
<header><h1>Bảng điều khiển demo P2P</h1><span class="sub" id="sub"></span><span class="clock" id="clock"></span></header>
<div class="steps">
 <button class="ghost" onclick="act('reset')">↺ Dọn sạch, làm lại</button>
 <button onclick="act('start','tracker')"><span class="n">1</span>Bật tracker</button>
 <button onclick="act('start','peer-1')"><span class="n">2</span>Bật peer-1 (seed)</button>
 <button onclick="act('start','peer-2')"><span class="n">3</span>Bật peer-2 (seed)</button>
 <button class="steel" onclick="act('start','peer-3');setTimeout(()=>act('start','peer-4'),300)"><span class="n">4</span>Bật peer-3 + peer-4 (tải)</button>
 <button class="kill" onclick="act('kill','peer-1')"><span class="n">5</span>Tắt đột ngột peer-1</button>
 <button class="ghost" onclick="act('hash')"><span class="n" style="background:var(--panel);color:var(--ink)">6</span>Kiểm tra hash</button>
 <button class="amber" onclick="act('start','peer-5')"><span class="n">7</span>Bật peer-5 độc hại</button>
 <button class="steel" onclick="act('start','peer-6')"><span class="n">8</span>Bật peer-6 (tải)</button>
 <button class="steel" onclick="act('start','peer-7')"><span class="n">9</span>Bật peer-7 (resume)</button>
</div>
<main><section id="files"></section><aside><h3>Sự kiện</h3><div id="events"></div></aside></main>
<div class="sec hash" id="hash"></div>
<div class="sec"><h3>Log của từng node (mỗi khung là một tiến trình riêng, giữ toàn bộ log)<label class="tog"><input type="checkbox" id="quiet" onchange="document.body.classList.toggle('quiet',this.checked)"> Chỉ hiện dòng quan trọng (ẩn gửi/nhận chunk, báo danh)</label></h3><div class="nodes" id="nodes"></div></div>
<script>
const KIND={join:"vào mạng",done:"tải xong",expired:"hết lease",left:"rời mạng",file:"metadata",start:"khởi động",kill:"tắt đột ngột",reset:"dọn sạch",hash:"hash"};
function esc(s){return String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]))}
async function act(action,node){await fetch("/api/action",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({action,node})});tick()}
function grid(n,set){let h='<div class="grid">';for(let i=0;i<n;i++)h+='<div class="c'+(set.has(i)?" on":"")+'"></div>';return h+"</div>"}
function heat(n,peers){const cnt=new Array(n).fill(0);peers.filter(p=>p.state==="online").forEach(p=>p.chunks.forEach(i=>cnt[i]++));
 const mx=Math.max(1,...cnt);let h='<div class="grid">';cnt.forEach(c=>{h+='<div class="c" style="background:'+(c?'rgba(14,159,142,'+(0.25+0.75*c/mx)+')':'#F6D5D3')+'"></div>'});return h+"</div>"}
const CLS=[[/FILE_VERIFIED|DOWNLOAD_COMPLETE|ALREADY_COMPLETE|SEEDING/,"l-ok"],[/SOURCE_FAILED|CHUNK_REJECTED|PEER_EXPIRED|CHUNK_FAILED|DOWNLOAD_ABORTED|AUTH_FAILED|CHUNK_CORRUPTED/,"l-bad"],
 [/CHUNK_RETRY|UPLOAD_BUSY|TRACKER_UNREACHABLE|PEER_LEFT|SOURCE_GONE|WAITING/,"l-warn"],[/METADATA|FILE_REGISTERED|FILE_SHARED|PEER_JOINED|RESUME|TRACKER_START|SOURCE_DISCOVERED|DOWNLOAD_START/,"l-info"]];
const NOISE=/^(CHUNK_SENT|CHUNK_RECEIVED|PEER_ANNOUNCE|ANNOUNCE_OK|CHUNK_DUPLICATE)$/;
function lineDiv(line){const d=document.createElement("div");const m=line.match(/^\S+ \| [^|]+\| (\S+) \|/);
 if(m&&NOISE.test(m[1]))d.className="noise";d.innerHTML=fmt(line);return d}
function fmt(line){
 const m=line.match(/^(\S+) \| [^|]+\| (\S+) \| (.*)$/);if(!m)return esc(line);
 let t=m[1];const d=new Date(t);if(!isNaN(d))t=d.toLocaleTimeString("vi-VN");let c="";for(const [re,k] of CLS)if(re.test(m[2])){c=k;break}
 const det=m[3].replace(/\b(file|sha256)=[0-9a-f]{12,}…?/g,"").replace(/\s+/g," ");
 return '<span style="color:#7D8B95">'+t+'</span> <span class="e '+c+'">'+esc(m[2])+'</span> '+esc(det)}
const scrolled={},since={};
function render(d){
 document.getElementById("clock").textContent=new Date().toLocaleTimeString("vi-VN");
 document.getElementById("sub").textContent="File mẫu "+d.size_mb+" MB · trễ upload "+d.delay+" s/chunk · FILE_ID "+(d.file_id||"(chưa tính)");
 const F=document.getElementById("files");
 if(!d.tracker_ok)F.innerHTML='<div class="empty">Tracker chưa chạy. Bấm <b>1 · Bật tracker</b>.</div>';
 else if(!d.files.length)F.innerHTML='<div class="empty">Tracker chưa có file nào. Bấm <b>2 · Bật peer-1</b>.</div>';
 else F.innerHTML=d.files.map(f=>{const n=f.num_chunks;
  const rows=f.peers.map(p=>{const k=p.chunks.length,pct=Math.round(100*k/n);let b;
   if(p.state==="gone")b='<span class="badge gone">'+esc(p.reason)+'</span>';else if(p.age>4)b='<span class="badge stale">mất tín hiệu '+p.age+' s</span>';
   else if(k===n)b='<span class="badge seed">seeder</span>';else b='<span class="badge leech">đang tải</span>';
   return '<tr class="'+(p.state==="gone"?"goneRow":"")+'"><td><div class="name">'+esc(p.peer_id)+'</div><div class="addr">'+esc(p.host)+':'+p.port+'</div></td><td>'+b+'</td><td class="pct">'+k+'/'+n+' ('+pct+'%)</td><td>'+grid(n,new Set(p.chunks))+'</td></tr>'}).join("");
  return '<div class="file"><h2>'+esc(f.name)+'</h2><div class="meta">'+(f.size/1048576).toFixed(0)+' MB · '+n+' chunk · '+f.peers.filter(p=>p.state==="online").length+' peer online · dữ liệu đọc từ API tracker</div>'+
  '<table><colgroup><col style="width:120px"><col style="width:150px"><col style="width:120px"><col></colgroup><tr><th>Peer</th><th>Trạng thái</th><th>Đã có</th><th>Bản đồ chunk (mỗi ô một chunk)</th></tr>'+rows+
  '<tr><td><div class="name">Độ phổ biến</div><div class="addr">số peer có chunk</div></td><td></td><td></td><td>'+heat(n,f.peers)+'</td></tr></table></div>'}).join("");
 document.getElementById("events").innerHTML=(d.events||[]).map(e=>'<div class="ev '+e.kind+'"><span class="t">'+e.t+'</span><span class="k">'+(KIND[e.kind]||"")+'</span> · '+esc(e.text)+'</div>').join("")||'<div class="ev">Chưa có sự kiện.</div>';
 const H=document.getElementById("hash");
 if(d.hashes&&d.hashes.length){const same=new Set(d.hashes.map(h=>h.sha)).size===1;
  H.innerHTML='<h3>Kiểm tra SHA-256 '+(same?'<span class="same">· tất cả trùng nhau</span>':'<span class="diff">· có file khác nhau</span>')+'</h3><table><colgroup><col style="width:240px"><col></colgroup>'+
   d.hashes.map(h=>'<tr><td style="font-family:Calibri,Arial">'+esc(h.name)+'</td><td>'+esc(h.sha)+'</td></tr>').join("")+'</table>'}else H.innerHTML="";
 const N=document.getElementById("nodes");
 d.nodes.forEach(nd=>{let el=document.getElementById("n-"+nd.name);
  if(!el){el=document.createElement("div");el.className="node";el.id="n-"+nd.name;el.innerHTML='<div class="hd"><span class="dot"></span><b></b><span class="r"></span><button class="b1"></button></div><div class="log"></div><div class="errs"></div>';N.appendChild(el);
   const lg=el.querySelector(".log");lg.addEventListener("scroll",()=>{scrolled[nd.name]=lg.scrollTop+lg.clientHeight<lg.scrollHeight-8})}
  el.querySelector(".dot").className="dot "+nd.state;el.querySelector("b").textContent=nd.title;
  el.querySelector(".r").textContent=nd.role+(nd.pid?" · PID "+nd.pid:"")+(nd.state==="exited"?" · đã dừng":"");
  const b=el.querySelector(".b1");if(nd.state==="running"){b.textContent="Tắt đột ngột";b.className="b1 kill";b.onclick=()=>act("kill",nd.name)}
  else{b.textContent=nd.state==="off"?"Bật":"Bật lại";b.className="b1";b.onclick=()=>act("start",nd.name)}
  const lg=el.querySelector(".log");
  if(nd.total<(since[nd.name]||0)||!lg.dataset.init){lg.innerHTML="";lg.dataset.init="1";since[nd.name]=0;scrolled[nd.name]=false}
  if(nd.log.length){const ph=lg.querySelector(".ph");if(ph)ph.remove();nd.log.forEach(l=>lg.appendChild(lineDiv(l)))}
  else if(!nd.total&&!lg.querySelector(".ph"))lg.innerHTML='<div class="ph" style="color:#55606B">(chưa chạy)</div>';
  since[nd.name]=nd.total;
  const er=el.querySelector(".errs");er.style.display=nd.errors.length?"block":"none";er.textContent=nd.errors.join("\n");
  if(!scrolled[nd.name])lg.scrollTop=lg.scrollHeight});
}
let busy=false;
async function tick(){if(busy)return;busy=true;try{const q=Object.keys(since).map(k=>k+":"+since[k]).join(",");
 const r=await fetch("/api/state?since="+q);render(await r.json())}catch(e){}finally{busy=false}}
tick();setInterval(tick,1000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj):
        self._send(200, json.dumps(obj, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self):
        if self.path == "/":
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path.startswith("/api/state"):
            since = {}
            q = urlparse(self.path).query
            for part in q.split("since=", 1)[-1].split(",") if "since=" in q else []:
                k, _, v = part.partition(":")
                if v.isdigit():
                    since[k] = int(v)
            with CONTROL.lock:
                self._json(state(since))
        else:
            self._send(404, b"not found", "text/plain")

    def do_POST(self):
        if self.path != "/api/action":
            return self._send(404, b"not found", "text/plain")
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0") or 0)) or b"{}")
            action, node = body.get("action"), body.get("node")
            with CONTROL.lock:
                if action == "start" and node in NODES:
                    msg = CONTROL.start(node)
                elif action == "kill" and node in NODES:
                    msg = CONTROL.kill(node)
                elif action == "reset":
                    msg = CONTROL.reset()
                elif action == "hash":
                    msg = CONTROL.check_hash()
                else:
                    msg = "hành động không hợp lệ"
            self._json({"result": msg})
        except Exception as e:   # không để một thao tác lỗi làm sập bảng điều khiển
            self._json({"result": f"lỗi: {e}"})


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--size-mb", type=int, default=40, help="kích thước file mẫu (MB)")
    ap.add_argument("--delay", type=float, default=1.0, help="trễ upload mỗi chunk (giây), để kịp theo dõi")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()
    ARGS.size_mb, ARGS.delay = args.size_mb, args.delay
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    print("Chuẩn bị file mẫu...", flush=True)
    CONTROL.reset()
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://localhost:{args.port}"
    print(f"Bảng điều khiển: {url}   (tracker {TRACKER})   Ctrl+C để tắt tất cả", flush=True)
    if not args.no_browser:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        CONTROL.kill_all()
        print("Đã tắt mọi node.", flush=True)


if __name__ == "__main__":
    main()
