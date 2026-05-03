"""Subprocess: drain stdin, batch, POST to Feishu webhooks every TICK_SEC.

Spawned by `remote_log.start()` with FEISHU_WEBHOOKS in argv. The parent writes
one JSON object per line to our stdin; we batch and POST. EOF (parent closed
stdin) ⇒ final flush + exit.

stdin line shape (UTF-8, terminated with \\n):
    {"level":"WARN","ts":"12:34:56","name":"...","msg":"..."}

This runs as a *separate process* so a hung webhook, a stuck request, or a leak
in `requests` cannot affect the trading loop.
"""

import json
import os
import queue
import sys
import threading
import time

import requests

_SIZE_CAP_BYTES = 25 * 1024


def _format(records: list[dict], dropped: int) -> str:
    lines: list[str] = []
    used = 0
    for i, r in enumerate(records):
        line = f"[{r.get('level','?')}] {r.get('ts','')} {r.get('name','')}: {r.get('msg','')}"
        n = len(line.encode("utf-8")) + 1
        if used + n > _SIZE_CAP_BYTES:
            lines.append(f"... ({len(records) - i} more lines truncated)")
            break
        lines.append(line)
        used += n
    if dropped:
        lines.append(f"... ({dropped} records dropped)")
    return "\n".join(lines)


def main() -> int:
    webhooks = sys.argv[1:]
    if not webhooks:
        return 0
    tick_sec = int(os.environ.get("GMX_REMOTE_LOG_INTERVAL", "30"))

    q: queue.SimpleQueue = queue.SimpleQueue()
    eof = threading.Event()

    def _reader() -> None:
        for raw in sys.stdin:
            try:
                q.put(json.loads(raw))
            except Exception as e:
                print(f"feishu_relay: parse fail: {e!r} line={raw!r}", file=sys.stderr)
        eof.set()

    threading.Thread(target=_reader, name="feishu-stdin", daemon=True).start()

    while True:
        deadline = time.monotonic() + tick_sec
        records: list[dict] = []
        while True:
            wait = deadline - time.monotonic()
            if wait <= 0:
                break
            try:
                records.append(q.get(timeout=wait))
            except queue.Empty:
                break

        if records:
            text = _format(records, dropped=0)
            payload = {"msg_type": "text", "content": {"text": text}}
            for url in webhooks:
                try:
                    requests.post(url, json=payload, timeout=5)
                except Exception as e:
                    print(f"feishu_relay: POST {url[:40]}... failed: {e!r}", file=sys.stderr)

        if eof.is_set() and q.empty():
            return 0


if __name__ == "__main__":
    sys.exit(main())
