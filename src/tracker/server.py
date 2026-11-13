"""Tracker: chỉ quản lý METADATA và DANH SÁCH PEER. Không bao giờ lưu hay gửi nội dung file.

Chạy (đứng ở thư mục src/):   python -m tracker.server
Biến môi trường: TRACKER_TOKEN (bắt buộc), TRACKER_HOST, TRACKER_PORT, PEER_TTL, TRACKER_DB, LOG_FILE

API (JSON, mọi request phải có header X-Peer-Token):
    GET  /health                    -> {"status": "ok"}         (không cần token)
    POST /files                     đăng ký metadata
    GET  /files                     danh sách file (id, name, size, số peer)
    GET  /files/<file_id>           metadata
    POST /announce                  peer báo chunk đang có, kiêm heartbeat
    GET  /files/<file_id>/peers     danh sách peer còn sống
"""
from __future__ import annotations

import contextlib
import hmac
import json
import logging
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from common.config import env_float, env_int, env_str, load_dotenv
from common.log import event, get_logger
from common.metadata import MetadataError, validate_metadata

MAX_BODY = 2 * 1024 * 1024
PEER_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
HOST_RE = re.compile(r"^[A-Za-z0-9.\-:]{1,255}$")
EVENTS = {"started", "update", "completed", "stopped"}


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


class TrackerState:
    """Trạng thái của tracker. Mọi truy cập đều đi qua self.lock vì
    ThreadingHTTPServer xử lý mỗi request trong một thread riêng."""

    def __init__(self, ttl: float, db_path: Path | None, log: logging.Logger):
        self.ttl = ttl
        self.db_path = db_path
        self.log = log
        self.lock = threading.Lock()
        self.files: dict[str, dict] = {}                 # file_id -> metadata
        self.swarm: dict[str, dict[str, dict]] = {}      # file_id -> peer_id -> info
        self._load_db()

    # ----- lưu metadata để tracker khởi động lại không mất (danh sách peer là "soft state",
    #       tự được xây lại nhờ announce định kỳ nên không cần lưu) -----
    def _load_db(self) -> None:
        if not self.db_path or not self.db_path.exists():
            return
        try:
            raw = json.loads(self.db_path.read_text(encoding="utf-8"))
            for fid, meta in raw.get("files", {}).items():
                self.files[fid] = validate_metadata(meta)
                self.swarm[fid] = {}
            event(self.log, "DB_LOADED", f"files={len(self.files)} path={self.db_path}")
        except (OSError, json.JSONDecodeError, MetadataError) as e:
            event(self.log, "DB_LOAD_FAILED", str(e), logging.WARNING)

    def _save_db(self) -> None:
        if not self.db_path:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.db_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"files": self.files}), encoding="utf-8")
        os.replace(tmp, self.db_path)

    # ----- nghiệp vụ -----
    def register(self, body: object) -> tuple[int, dict]:
        try:
            meta = validate_metadata(body)
        except MetadataError as e:
            raise ApiError(400, f"metadata không hợp lệ: {e}") from e
        fid = meta["file_id"]
        with self.lock:
            old = self.files.get(fid)
            if old is not None:
                if old["file_hash"] != meta["file_hash"] or old["chunk_hashes"] != meta["chunk_hashes"]:
                    raise ApiError(409, "file_id đã tồn tại với nội dung khác")
                return 200, {"file_id": fid, "status": "exists"}   # idempotent
            self.files[fid] = meta
            self.swarm[fid] = {}
            self._save_db()
        event(self.log, "FILE_REGISTERED", f"file={fid} name={meta['name']} size={meta['size']} chunks={meta['num_chunks']}")
        return 201, {"file_id": fid, "status": "registered"}

    def get_meta(self, fid: str) -> dict:
        with self.lock:
            meta = self.files.get(fid)
        if meta is None:
            raise ApiError(404, "không có file này")
        return meta

    def list_files(self) -> list[dict]:
        now = time.time()
        with self.lock:
            return [{"file_id": fid, "name": m["name"], "size": m["size"], "num_chunks": m["num_chunks"],
                     "peers": sum(1 for p in self.swarm[fid].values() if now - p["last_seen"] <= self.ttl)}
                    for fid, m in self.files.items()]

    def announce(self, body: object) -> dict:
        if not isinstance(body, dict):
            raise ApiError(400, "body phải là JSON object")
        pid, host, port = body.get("peer_id"), body.get("host"), body.get("port")
        fid, chunks, ev = body.get("file_id"), body.get("chunks"), body.get("event", "update")
        if not isinstance(pid, str) or not PEER_ID_RE.match(pid):
            raise ApiError(400, "peer_id không hợp lệ")
        if not isinstance(host, str) or not HOST_RE.match(host):
            raise ApiError(400, "host không hợp lệ")
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise ApiError(400, "port không hợp lệ")
        if ev not in EVENTS:
            raise ApiError(400, "event không hợp lệ")
        if not isinstance(chunks, list):
            raise ApiError(400, "chunks phải là danh sách")

        now = time.time()
        with self.lock:
            meta = self.files.get(fid) if isinstance(fid, str) else None
            if meta is None:
                raise ApiError(404, "không có file này")
            n = meta["num_chunks"]
            if len(chunks) > n or not all(isinstance(i, int) and not isinstance(i, bool) and 0 <= i < n for i in chunks):
                raise ApiError(400, "chỉ số chunk ngoài phạm vi")
            peers = self.swarm[fid]
            old = peers.get(pid)

            if ev == "stopped":
                if old and (old["host"], old["port"]) == (host, port):
                    del peers[pid]
                    log_args = ("PEER_LEFT", f"file={fid} peer={pid}")
                else:
                    log_args = None
            else:
                # Chống mạo danh: một peer_id đang sống ở địa chỉ khác thì không được ghi đè.
                if old and (old["host"], old["port"]) != (host, port) and now - old["last_seen"] <= self.ttl:
                    raise ApiError(409, f"peer_id {pid} đang được dùng bởi {old['host']}:{old['port']}")
                have = sorted(set(chunks))
                peers[pid] = {"peer_id": pid, "host": host, "port": port, "chunks": have, "last_seen": now}
                if old is None:
                    log_args = ("PEER_JOINED", f"file={fid} peer={pid} addr={host}:{port} have={len(have)}/{n}")
                elif ev != "update" or len(old["chunks"]) != len(have):
                    log_args = ("PEER_ANNOUNCE", f"file={fid} peer={pid} event={ev} have={len(have)}/{n}")
                else:
                    log_args = None
        if log_args:
            event(self.log, *log_args)
        return {"interval": None, "ttl": self.ttl}

    def peers_of(self, fid: str) -> list[dict]:
        now = time.time()
        with self.lock:
            if fid not in self.files:
                raise ApiError(404, "không có file này")
            return [dict(p, age=round(now - p["last_seen"], 1))
                    for p in self.swarm[fid].values() if now - p["last_seen"] <= self.ttl]

    def reap(self) -> None:
        """Lease hết hạn: peer không announce trong PEER_TTL giây bị coi là đã rời mạng."""
        now = time.time()
        expired = []
        with self.lock:
            for fid, peers in self.swarm.items():
                for pid in [p for p, info in peers.items() if now - info["last_seen"] > self.ttl]:
                    del peers[pid]
                    expired.append((fid, pid))
        for fid, pid in expired:
            event(self.log, "PEER_EXPIRED", f"file={fid} peer={pid} (không heartbeat quá {self.ttl}s)")


class Handler(BaseHTTPRequestHandler):
    server_version = "P2PTracker/1.0"
    protocol_version = "HTTP/1.1"
    timeout = 10        # giây: client gửi chậm hoặc không gửi gì thì bị ngắt, không giữ thread mãi

    # tắt log mặc định của http.server, dùng logger riêng
    def log_message(self, *args) -> None:
        pass

    @property
    def state(self) -> TrackerState:
        return self.server.state  # type: ignore[attr-defined]

    def _send(self, status: int, obj: object) -> None:
        data = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _check_auth(self) -> None:
        token = self.headers.get("X-Peer-Token", "")
        if not hmac.compare_digest(token.encode(), self.server.token.encode()):  # type: ignore[attr-defined]
            event(self.state.log, "AUTH_FAILED", f"from={self.client_address[0]} path={self.path}", logging.WARNING)
            raise ApiError(401, "token không hợp lệ")

    def _read_json(self) -> object:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise ApiError(400, "Content-Length không hợp lệ")
        if length <= 0:
            raise ApiError(400, "thiếu body")
        if length > MAX_BODY:
            raise ApiError(413, "body quá lớn")
        raw = self.rfile.read(length)
        self._body_read = True
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ApiError(400, "body không phải JSON hợp lệ")

    def _drain_body(self) -> None:
        """Đọc bỏ body chưa đọc trước khi trả lỗi và đóng kết nối.

        Đóng socket khi còn dữ liệu chưa đọc thì hệ điều hành gửi RST thay cho FIN; trên Windows client có thể
        nhận RST trước khi đọc được phản hồi và thấy "mất kết nối" thay cho mã lỗi thật (ví dụ 401).
        Body lớn hơn MAX_BODY (trường hợp 413) thì không đọc, để giữ giới hạn bộ nhớ.
        """
        if self._body_read:
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return
        if 0 < length <= MAX_BODY:
            with contextlib.suppress(OSError):      # client ngắt hoặc quá timeout: vẫn trả lỗi như thường
                self.rfile.read(length)
            self._body_read = True

    def _dispatch(self, method: str) -> None:
        self._body_read = False
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if method == "GET" and path == "/health":
                return self._send(200, {"status": "ok"})
            self._check_auth()
            parts = path.strip("/").split("/")
            if method == "POST" and parts == ["files"]:
                status, body = self.state.register(self._read_json())
                return self._send(status, body)
            if method == "POST" and parts == ["announce"]:
                body = self.state.announce(self._read_json())
                body["interval"] = self.server.announce_interval  # type: ignore[attr-defined]
                return self._send(200, body)
            if method == "GET" and parts == ["files"]:
                return self._send(200, self.state.list_files())
            if method == "GET" and len(parts) == 2 and parts[0] == "files":
                return self._send(200, self.state.get_meta(parts[1]))
            if method == "GET" and len(parts) == 3 and parts[0] == "files" and parts[2] == "peers":
                return self._send(200, self.state.peers_of(parts[1]))
            raise ApiError(404, "không có endpoint này")
        except ApiError as e:
            if e.status not in (401, 404):
                event(self.state.log, "BAD_REQUEST", f"{method} {path} -> {e.status} {e.message}", logging.WARNING)
            self._drain_body()
            self.close_connection = True      # body quá lớn thì chưa đọc -> không tái dùng kết nối
            self._send(e.status, {"error": e.message})
        except (TimeoutError, ConnectionError):     # client gửi body quá chậm hoặc ngắt giữa chừng
            self.close_connection = True
        except Exception as e:  # không để một request lỗi làm sập tracker
            event(self.state.log, "INTERNAL_ERROR", f"{method} {path}: {e!r}", logging.ERROR)
            self._send(500, {"error": "lỗi nội bộ"})

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")


class TrackerHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128      # hàng đợi kết nối: nhiều peer announce cùng lúc không bị từ chối


def make_server(host: str, port: int, token: str, ttl: float = 10.0, announce_interval: int = 2,
                db_path: Path | None = None, log: logging.Logger | None = None) -> ThreadingHTTPServer:
    if not token:
        raise SystemExit("TRACKER_TOKEN không được để trống")
    log = log or get_logger("tracker")
    httpd = TrackerHTTPServer((host, port), Handler)
    httpd.state = TrackerState(ttl, db_path, log)          # type: ignore[attr-defined]
    httpd.token = token                                    # type: ignore[attr-defined]
    httpd.announce_interval = announce_interval            # type: ignore[attr-defined]

    def reaper() -> None:
        while True:
            time.sleep(1)
            httpd.state.reap()                             # type: ignore[attr-defined]

    threading.Thread(target=reaper, daemon=True, name="reaper").start()
    return httpd


def main() -> None:
    load_dotenv()
    log = get_logger("tracker", os.getenv("TRACKER_LOG_FILE"))
    host = os.getenv("TRACKER_HOST", "0.0.0.0")
    port = env_int("TRACKER_PORT", 8000)
    db = os.getenv("TRACKER_DB", "./data/tracker/tracker_db.json")
    httpd = make_server(host, port, env_str("TRACKER_TOKEN"), env_float("PEER_TTL", 10),
                        env_int("ANNOUNCE_INTERVAL", 2), Path(db), log)
    event(log, "TRACKER_START", f"listen={host}:{port} ttl={httpd.state.ttl}s db={db}")  # type: ignore[attr-defined]
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        event(log, "TRACKER_STOP", "bye")
        httpd.server_close()


if __name__ == "__main__":
    main()
