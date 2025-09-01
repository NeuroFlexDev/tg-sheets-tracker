# bot/logsetup.py
import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Any, Dict

DEFAULT_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_JSON = os.getenv("LOG_JSON", "false").lower() in {"1", "true", "yes"}
LOG_FILE = os.getenv("LOG_FILE", "").strip()  # например: ./bot.log
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "1048576"))  # 1 MiB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        }
        # стеки и эксепшны
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        # добавочные поля (extra=...)
        for k, v in getattr(record, "__dict__", {}).items():
            if k.startswith("_") or k in payload or k in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process",
            ):
                continue
            try:
                json.dumps(v)  # проверка сериализуемости
                payload[k] = v
            except Exception:
                payload[k] = str(v)
        return json.dumps(payload, ensure_ascii=False)

def setup_logging():
    root = logging.getLogger()
    if root.handlers:
        return  # уже настроено

    level = getattr(logging, DEFAULT_LEVEL, logging.INFO)

    root.setLevel(level)
    root.handlers.clear()

    if LOG_JSON:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)5s | %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )

    # консоль
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    # файл (опционально)
    if LOG_FILE:
        fh = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)

    # чуть приглушим болтливые либы
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("gspread").setLevel(logging.INFO)

    logging.getLogger(__name__).info(
        "Logging initialized",
        extra={"level_set": DEFAULT_LEVEL, "json": LOG_JSON, "file": LOG_FILE or None}
    )
