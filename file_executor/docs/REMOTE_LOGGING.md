# Remote Logging

A `logging.Handler` that ships WARNING+ records to one or more Feishu bot webhooks via a **subprocess**. `emit()` writes one JSON line to the relay's stdin and returns.

```
log.warning(...)  ──▶  emit()  ──json line──▶  feishu_relay stdin
                                                    │
                                                    ▼ every 30s
                                              POST batch to each webhook
```

The HTTP round-trip lives entirely in the subprocess. A hung webhook, a stuck `requests` call, or any failure inside the HTTP stack cannot stall, lock, or leak into the trading process.

## Why a subprocess

Earlier this was a daemon thread. Two reasons it became a subprocess:

* **Process isolation.** Network I/O, TLS, and `requests` retry/timeout edges have nothing to do with order placement. Putting them in a separate process means a webhook problem cannot reach the trading code at all — not via GIL, not via memory, not via uncaught exceptions.
* **Operational simplicity.** The relay can be killed and restarted without touching the executor.

## Aggregation

* Subprocess flushes every `TICK_SEC` (default 30s).
* Per tick: drain everything currently in its in-memory queue, format into one Feishu message, POST to every webhook. Empty tick ⇒ no POST.
* Worst-case per-record delivery latency is one tick. Keeps us under Feishu's ~5/sec, ~100/min bot rate limit.

## Subprocess (`feishu_relay.py`)

```python
# python -m file_executor.feishu_relay <url> [<url> ...]

q  = queue.SimpleQueue()
eof = threading.Event()

def reader():
    for raw in sys.stdin:                           # blocks; releases GIL on read
        q.put(json.loads(raw))
    eof.set()

threading.Thread(target=reader, daemon=True).start()

while True:
    deadline = time.monotonic() + TICK_SEC
    records  = []
    while (wait := deadline - time.monotonic()) > 0:
        try: records.append(q.get(timeout=wait))
        except queue.Empty: break

    if records:
        text = format_batch(records)                # truncated at SIZE_CAP
        for url in webhooks:
            try: requests.post(url, json={"msg_type":"text","content":{"text":text}}, timeout=5)
            except Exception as e: print(f"POST failed: {e!r}", file=sys.stderr)

    if eof.is_set() and q.empty(): return 0          # parent closed stdin → drain → exit
```

Two threads inside the subprocess (reader + main). They are *inside the subprocess*, so they do not contend with the trading process's GIL.

## Handler (`remote_log.py`)

```python
class FeishuHandler(logging.Handler):
    def __init__(self, proc): self.proc = proc; super().__init__()
    def emit(self, record):
        line = json.dumps({"level": record.levelname,
                           "ts":    time.strftime("%H:%M:%S", time.localtime(record.created)),
                           "name":  record.name,
                           "msg":   record.getMessage()}, ensure_ascii=False) + "\n"
        try:
            self.proc.stdin.write(line); self.proc.stdin.flush()
        except (BrokenPipeError, OSError, ValueError):
            pass                                        # relay died; drop silently

def start():
    if not FEISHU_WEBHOOKS: return None
    proc = subprocess.Popen(
        [sys.executable, "-m", "file_executor.feishu_relay", *FEISHU_WEBHOOKS],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=sys.stderr,
        text=True, encoding="utf-8", bufsize=1,
    )
    h = FeishuHandler(proc); h.setLevel(logging.WARNING)
    logging.getLogger().addHandler(h)
    return proc

def stop():
    proc.stdin.close()                                  # EOF wakes the reader
    proc.wait(timeout=10)                               # else proc.kill()
```

`bufsize=1` (line-buffered) means each `flush()` ships one line through the OS pipe (default ~64 KB buffer). At WARNING-rate this never fills.

## Config (env)

| var                       | default | meaning                                             |
| ------------------------- | ------- | --------------------------------------------------- |
| `GMX_FEISHU_WEBHOOKS`     | empty   | comma-separated webhook URLs. Empty ⇒ relay not spawned. |
| `GMX_REMOTE_LOG_INTERVAL` | `30`    | seconds between flushes (set in subprocess env).    |

## Failure handling

| event                        | action                                                           |
| ---------------------------- | ---------------------------------------------------------------- |
| no webhooks configured       | relay not spawned; handler not attached                          |
| relay subprocess crashes     | `emit()` writes hit `BrokenPipeError`; dropped silently. No restart — operator notices via stderr. |
| webhook HTTP error / timeout | logged to subprocess stderr (visible in main log); skip that URL |
| webhook 429 / 5xx            | same; no retry within tick                                       |
| message > SIZE_CAP           | truncated with `... (N more lines truncated)` trailer            |
| pipe buffer full             | `emit()` blocks on `stdin.write` until subprocess drains. Sized so this is unreachable at WARNING rate; if it happens, that's a bug. |
| executor shutdown            | `remote_log.stop()` closes stdin → relay drains queue → exits    |

## Pitfalls

* **Don't log INFO remote.** Default WARNING. INFO at 30s aggregation fills SIZE_CAP fast and crowds out signal.
* **Webhook URLs are secrets.** The path contains a Feishu-issued token. Subprocess only logs `url[:40]` prefixes on POST failure.
* **Recursion.** `local_log.propagate = False` keeps internal failures from looping back through root → `FeishuHandler`. Don't change that.
* **Startup ordering.** Spawn the relay (and attach handler) **after** local handlers, so stdout/stderr always sees the early lines regardless of relay state.
