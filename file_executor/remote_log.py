"""Background log shipper to Feishu webhook(s).

Non-blocking: `emit()` does one `queue.put_nowait` and returns. The HTTP round-trip
happens entirely on the relay thread. The trading flow does not slow down even
if every webhook is unreachable.

Aggregation: relay drains every `REMOTE_LOG_INTERVAL` seconds; one POST per tick
per webhook keeps us comfortably under Feishu's per-bot rate limit.

No recursion: failures inside the relay log via a non-propagating internal logger,
so a Feishu outage cannot generate logs that re-enqueue and loop.
"""

import logging
import queue
import threading
import time

import requests

from . import config, state

local_log = logging.getLogger("remote_logging.internal")
local_log.propagate = False

_SIZE_CAP_BYTES = 25 * 1024


# ── handler ───────────────────────────────────────────────────────────

class FeishuHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.q: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=config.REMOTE_LOG_QUEUE_MAX)
        self.dropped: int = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.q.put_nowait(record)
        except queue.Full:
            self.dropped += 1                                 # surfaced on next flush


# ── formatting ────────────────────────────────────────────────────────

def _format_batch(records: list[logging.LogRecord], dropped: int) -> str:
    lines: list[str] = []
    used = 0
    for i, r in enumerate(records):
        line = (
            f"[{r.levelname}] "
            f"{time.strftime('%H:%M:%S', time.localtime(r.created))} "
            f"{r.name}: {r.getMessage()}"
        )
        encoded_len = len(line.encode("utf-8")) + 1                       # +1 for newline
        if used + encoded_len > _SIZE_CAP_BYTES:
            lines.append(f"... ({len(records) - i} more lines truncated)")
            break
        lines.append(line)
        used += encoded_len
    if dropped:
        lines.append(f"... ({dropped} records dropped at queue cap)")
    return "\n".join(lines)


# ── relay ─────────────────────────────────────────────────────────────

def _relay_loop(handler: FeishuHandler) -> None:
    while not state.stop_event.is_set():
        records: list[logging.LogRecord] = []
        while True:
            try:
                records.append(handler.q.get_nowait())
            except queue.Empty:
                break

        dropped = handler.dropped
        handler.dropped = 0

        if records or dropped:
            text = _format_batch(records, dropped)
            payload = {"msg_type": "text", "content": {"text": text}}
            for url in config.FEISHU_WEBHOOKS:
                try:
                    requests.post(url, json=payload, timeout=5)
                except Exception:
                    local_log.exception("feishu POST failed (%s...)", url[:40])

        state.stop_event.wait(config.REMOTE_LOG_INTERVAL)


# ── lifecycle ─────────────────────────────────────────────────────────

def start() -> threading.Thread | None:
    if not config.FEISHU_WEBHOOKS:
        local_log.info("GMX_FEISHU_WEBHOOKS empty; remote logging disabled")
        return None

    handler = FeishuHandler()
    handler.setLevel(logging.WARNING)
    logging.getLogger().addHandler(handler)

    t = threading.Thread(target=_relay_loop, args=(handler,),
                         name="remote-log-relay", daemon=True)
    t.start()
    state.worker_threads.append(t)
    return t
