# Remote Logging

A `logging.Handler` that ships log records to one or more Feishu bot webhooks. Runs on a background thread. **`emit()` never blocks the caller** — it does one `queue.put_nowait` and returns.

```
log.warning(...)                  ← any thread
       │
       ▼
   queue.Queue (bounded)
       │
       ▼
   relay thread  ── every 30s ──▶  POST batch to each webhook
```

The HTTP round-trip happens entirely on the relay thread. The trading flow does not slow down even if Feishu is unreachable.

## Aggregation

* Relay thread sleeps in `TICK_SEC` (default 30) intervals via `stop_event.wait(TICK_SEC)`.
* Each tick: drain the queue, format all drained records into a single Feishu message, POST to every configured webhook.
* Worst-case latency for any single record is one tick. This is the deliberate trade-off for staying well under Feishu custom-bot rate limits (~5 msg/sec, ~100 msg/min per bot — one POST per 30s leaves ample headroom even with multiple webhooks).
* If the queue is empty for a tick, no POST is made.

## Loop

```Python
import os, queue, threading, time, logging
import requests

WEBHOOKS  = [u for u in os.environ.get("GMX_FEISHU_WEBHOOKS", "").split(",") if u]
TICK_SEC  = int(os.environ.get("GMX_REMOTE_LOG_INTERVAL", "30"))
QUEUE_MAX = int(os.environ.get("GMX_REMOTE_LOG_QUEUE_MAX", "10000"))
SIZE_CAP  = 25 * 1024                       # bytes per Feishu message (Feishu limit ~30KB)

local_log = logging.getLogger("remote_logging.internal")
local_log.propagate = False                 # do NOT route through the root → FeishuHandler loop

class FeishuHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.q: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=QUEUE_MAX)
        self.dropped: int = 0

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.q.put_nowait(record)
        except queue.Full:
            self.dropped += 1               # surfaced in the next flush as a trailer

def relay_loop(handler: FeishuHandler, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        records: list[logging.LogRecord] = []
        while True:
            try:    records.append(handler.q.get_nowait())
            except queue.Empty: break

        dropped, handler.dropped = handler.dropped, 0
        if records or dropped:
            text = format_batch(records, dropped)
            for url in WEBHOOKS:
                try:
                    requests.post(url, json={"msg_type": "text", "content": {"text": text}}, timeout=5)
                except Exception:
                    local_log.exception("feishu POST failed (%s...)", url[:40])

        stop_event.wait(TICK_SEC)


def format_batch(records: list[logging.LogRecord], dropped: int) -> str:
    lines: list[str] = []
    used = 0
    for r in records:
        line = f"[{r.levelname}] {time.strftime('%H:%M:%S', time.localtime(r.created))} {r.name}: {r.getMessage()}"
        if used + len(line) + 1 > SIZE_CAP:
            lines.append(f"... ({len(records) - len(lines)} more lines truncated)")
            break
        lines.append(line); used += len(line) + 1
    if dropped:
        lines.append(f"... ({dropped} records dropped at queue cap)")
    return "\n".join(lines)
```

## Setup

Attach once at startup, alongside the cycle timer and connector:

```Python
def init_remote_logging(stop_event: threading.Event) -> threading.Thread | None:
    if not WEBHOOKS:
        local_log.info("GMX_FEISHU_WEBHOOKS empty; remote logging disabled")
        return None

    handler = FeishuHandler()
    handler.setLevel(logging.WARNING)             # only WARN and above go remote
    logging.getLogger().addHandler(handler)

    t = threading.Thread(target=relay_loop, args=(handler, stop_event),
                         name="remote-log-relay", daemon=True)
    t.start()
    return t
```

The local stderr / file handlers stay attached to the root logger and receive every level — remote is an **addition**, not a replacement.

## Config (env)

| var                          | required | default  | meaning                                                                        |
| ---------------------------- | -------- | -------- | ------------------------------------------------------------------------------ |
| `GMX_FEISHU_WEBHOOKS`        | no       | empty    | comma-separated Feishu webhook URLs. **Empty (0 webhooks) ⇒ remote logging disabled.** N webhooks ⇒ each batch POSTed to every URL. |
| `GMX_REMOTE_LOG_INTERVAL`    | no       | `30`     | seconds between flushes.                                                       |
| `GMX_REMOTE_LOG_QUEUE_MAX`   | no       | `10000`  | max queued records. Overflow is dropped and counted.                           |

## Failure handling

| event                              | action                                                                                |
| ---------------------------------- | ------------------------------------------------------------------------------------- |
| no webhooks configured             | handler is not attached; relay thread is not started; trading flow keeps running.     |
| queue full (caller bursting)       | `emit()` drops silently, increments `dropped`. Next flush appends `(N records dropped)`. |
| webhook HTTP error / timeout       | logged via the local `remote_logging.internal` logger; skip that webhook for this tick. Other webhooks still POSTed. |
| webhook returns 429 / 5xx          | same as any error — batch is lost for that webhook. Don't retry inside the tick (would lengthen the next batch's latency). |
| message > `SIZE_CAP`               | truncate at the boundary, append `... (N more lines truncated)`.                       |
| relay thread crashes               | exception escapes to the local logger via the daemon-thread default; relay stays dead until next process boot. The trading flow is unaffected. |

## Concurrency

* `FeishuHandler.emit()` is invoked from every thread (main, cycle, connector, SDK callbacks). `queue.Queue.put_nowait` is thread-safe; no extra lock is needed.
* Single relay thread, `daemon=True`, cooperative shutdown via the same `stop_event` shared with the connector and other workers (see [`../../coding-style.md`](../../coding-style.md)).
* `handler.dropped` is read-and-reset on the relay thread and incremented on writers. Under contention a few drops may be miscounted across a tick boundary. Acceptable — it's a diagnostic, not a control signal.

## Pitfalls

* **Recursion is the easy bug.** The HTTP-failure log uses `remote_logging.internal` with `propagate=False` so it cannot loop back through the root logger and re-enqueue itself. Don't change that.
* **Webhook URLs are secrets.** They contain a Feishu-issued bot token in the path. Never log the full URL, never include it in error messages, never expose it in stack traces. Log a prefix (`url[:40]`) or a hash only.
* **Don't ship INFO remote.** Default level is `WARNING`. INFO traffic at 30s aggregation will fill `SIZE_CAP` quickly and crowd out the events that actually need attention.
* **Don't make `emit` smart.** No formatting, no filtering, no retries inside `emit` — keep it strictly `put_nowait`. Anything else risks blocking the caller, which is the one rule remote logging exists to protect.
* **Startup ordering.** Attach the handler **after** the local handlers are configured, otherwise startup logs go remote-only if the relay isn't ready.
