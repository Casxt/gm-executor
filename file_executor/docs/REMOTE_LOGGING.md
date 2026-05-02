# Remote Logging

A `logging.Handler` that ships log records to one or more Feishu bot webhooks via a background relay thread. **`emit()` never blocks** — `queue.put_nowait` and return.

```
log.warning(...)  ──▶  queue.Queue (bounded)  ──▶  relay thread  ──every 30s──▶  POST batch to each webhook
```

The HTTP round-trip is fully on the relay thread; trading flow is unaffected even if Feishu is unreachable.

## Aggregation

* Relay sleeps `TICK_SEC` (default 30s) via `stop_event.wait(TICK_SEC)`.
* Per tick: drain queue, format all records into one Feishu message, POST to every webhook. Empty tick ⇒ no POST.
* Worst-case per-record latency is one tick — deliberate trade-off to stay under Feishu's ~5/sec, ~100/min rate limit.

## Loop

```python
WEBHOOKS  = [u for u in os.environ.get("GMX_FEISHU_WEBHOOKS", "").split(",") if u]
TICK_SEC  = int(os.environ.get("GMX_REMOTE_LOG_INTERVAL", "30"))
QUEUE_MAX = int(os.environ.get("GMX_REMOTE_LOG_QUEUE_MAX", "10000"))
SIZE_CAP  = 25 * 1024                                # bytes per message (Feishu ~30KB cap)

local_log = logging.getLogger("remote_logging.internal")
local_log.propagate = False                          # MUST stay False — root would loop back into FeishuHandler

class FeishuHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.q = queue.Queue(maxsize=QUEUE_MAX)
        self.dropped = 0

    def emit(self, record):
        try: self.q.put_nowait(record)
        except queue.Full: self.dropped += 1         # surfaced in next flush trailer

def relay_loop(handler, stop_event):
    while not stop_event.is_set():
        records = []
        while True:
            try: records.append(handler.q.get_nowait())
            except queue.Empty: break

        dropped, handler.dropped = handler.dropped, 0
        if records or dropped:
            text = format_batch(records, dropped)
            for url in WEBHOOKS:
                try: requests.post(url, json={"msg_type":"text","content":{"text":text}}, timeout=5)
                except Exception: local_log.exception("feishu POST failed (%s...)", url[:40])

        stop_event.wait(TICK_SEC)
```

`format_batch` produces `[LEVEL] HH:MM:SS logger: msg` per record, truncates at `SIZE_CAP` with a trailer, and appends `(N records dropped)` if any.

## Setup

```python
def init_remote_logging(stop_event):
    if not WEBHOOKS:
        local_log.info("GMX_FEISHU_WEBHOOKS empty; remote logging disabled")
        return None

    handler = FeishuHandler()
    handler.setLevel(logging.WARNING)                # only WARN+ goes remote
    logging.getLogger().addHandler(handler)

    t = threading.Thread(target=relay_loop, args=(handler, stop_event),
                         name="remote-log-relay", daemon=True)
    t.start()
    return t
```

Local stderr/file handlers stay attached and receive every level — remote is an addition, not a replacement.

## Config (env)

| var                        | default | meaning                                                   |
| -------------------------- | ------- | --------------------------------------------------------- |
| `GMX_FEISHU_WEBHOOKS`      | empty   | comma-separated webhook URLs. Empty ⇒ remote disabled.    |
| `GMX_REMOTE_LOG_INTERVAL`  | `30`    | seconds between flushes.                                  |
| `GMX_REMOTE_LOG_QUEUE_MAX` | `10000` | max queued records; overflow is dropped + counted.        |

## Failure handling

| event                        | action                                                                  |
| ---------------------------- | ----------------------------------------------------------------------- |
| no webhooks configured       | handler not attached, relay not started; trading flow unaffected        |
| queue full                   | `emit()` drops silently, increments `dropped`; next flush appends trailer |
| webhook HTTP error / timeout | log via `remote_logging.internal`; skip that webhook; others still POSTed |
| webhook 429/5xx              | same; don't retry inside the tick (would extend next batch's latency)   |
| message > `SIZE_CAP`         | truncate at boundary, append `... (N more lines truncated)`             |
| relay thread crashes         | unhandled exception kills the daemon; relay stays dead until restart    |

## Pitfalls

* **Recursion is the easy bug.** `local_log.propagate = False` keeps HTTP-failure logs from looping back through root → `FeishuHandler`. Don't change that.
* **Webhook URLs are secrets.** Path contains a Feishu-issued token. Never log full URLs — log a `url[:40]` prefix only.
* **Don't ship INFO remote.** Default `WARNING`. INFO at 30s aggregation fills `SIZE_CAP` fast and crowds out signal.
* **Keep `emit` dumb.** No formatting, filtering, or retries inside `emit` — strictly `put_nowait`. Anything else risks blocking the caller.
* **Startup ordering.** Attach remote handler **after** local handlers, so startup logs reach stderr regardless of relay readiness.
