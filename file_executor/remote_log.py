"""Ship WARNING+ records to Feishu via a subprocess.

`emit()` writes one JSON line to the relay subprocess's stdin and returns. All
network I/O — and any failure mode of `requests`/HTTPS — lives in the subprocess.
A hung or crashed webhook cannot stall, lock, or leak into the trading process.

Lifecycle: `start()` at process start spawns `python -m file_executor.feishu_relay`
and attaches the handler at WARNING. `stop()` at shutdown closes stdin so the
relay drains and exits.
"""

import json
import logging
import subprocess
import sys
import time

from . import config

local_log = logging.getLogger("remote_logging.internal")
local_log.propagate = False                                       # never re-enter the handler

_proc: subprocess.Popen | None = None


# ── handler ───────────────────────────────────────────────────────────

class FeishuHandler(logging.Handler):
    def __init__(self, proc: subprocess.Popen) -> None:
        super().__init__()
        self.proc = proc

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = json.dumps({
                "level": record.levelname,
                "ts":    time.strftime("%H:%M:%S", time.localtime(record.created)),
                "name":  record.name,
                "msg":   record.getMessage(),
            }, ensure_ascii=False) + "\n"
            stdin = self.proc.stdin
            if stdin is None or stdin.closed:
                return
            stdin.write(line)
            stdin.flush()
        except (BrokenPipeError, OSError, ValueError):
            pass                                                  # subprocess died; drop


# ── lifecycle ─────────────────────────────────────────────────────────

def start() -> subprocess.Popen | None:
    global _proc
    if not config.FEISHU_WEBHOOKS:
        local_log.info("GMX_FEISHU_WEBHOOKS empty; remote logging disabled")
        return None

    _proc = subprocess.Popen(
        [sys.executable, "-m", "file_executor.feishu_relay", *config.FEISHU_WEBHOOKS],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=sys.stderr,                                        # relay errors visible in our log
        text=True,
        encoding="utf-8",
        bufsize=1,                                                # line-buffered stdin
    )

    handler = FeishuHandler(_proc)
    handler.setLevel(logging.WARNING)
    logging.getLogger().addHandler(handler)
    return _proc


def stop() -> None:
    """Close stdin so the relay's reader hits EOF, then wait briefly."""
    global _proc
    if _proc is None:
        return
    try:
        if _proc.stdin and not _proc.stdin.closed:
            _proc.stdin.close()
    except Exception:
        pass
    try:
        _proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _proc.kill()
    _proc = None
