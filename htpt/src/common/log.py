"""Logger dùng chung, đúng định dạng: thời gian | node | sự kiện | chi tiết."""
import logging
import sys
from datetime import datetime, timezone


class _Fmt(logging.Formatter):
    def __init__(self, node: str):
        super().__init__()
        self.node = node

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds")
        event = getattr(record, "event", record.levelname)
        return f"{ts} | {self.node} | {event} | {record.getMessage()}"


def get_logger(node: str, logfile: str | None = None) -> logging.Logger:
    log = logging.getLogger(node)
    if log.handlers:
        return log
    log.setLevel(logging.INFO)
    handlers = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile, encoding="utf-8"))
    for h in handlers:
        h.setFormatter(_Fmt(node))
        log.addHandler(h)
    return log


def event(log: logging.Logger, name: str, msg: str, level: int = logging.INFO) -> None:
    """Ví dụ: event(log, "CHUNK_RECEIVED", "file=ab idx=5 from=peer-1 ok")"""
    log.log(level, msg, extra={"event": name})
