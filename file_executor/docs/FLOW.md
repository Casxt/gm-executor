# Execution Flow

Run by `gm.api.run(mode=MODE_LIVE, ...)`. The SDK's `timer(...)` ([GM\_SDK.md](./GM_SDK.md)) fires every `GMX_POLL_SECONDS`. Each fire is one **cycle**: scan `pending/`, retire expired/invalid batches, reconcile the single active batch (at most one).

* Batch schema: [SCHEMA.md](./SCHEMA.md)
* Per-batch order log: [ORDER\_RECORD.md](./ORDER_RECORD.md)

## Layout

The orders root is set by **`GMX_ORDERS_DIR`** (default `./orders`). All path references below — `pending/`, `finished/`, `expired/`, `failed/` — are relative to that root. The pseudo-code uses `ORDERS = os.environ.get("GMX_ORDERS_DIR", "./orders")` and joins from there; never hard-code `orders/`.

```
$GMX_ORDERS_DIR/
  pending/    # 0 or 1 active batch + optional not-yet-valid ones
  finished/   # batch + log, after match
  expired/    # batch + log, after expires_at; live orders cancelled first
  failed/     # batch only (no log existed); parse / schema error
```

`<batch_id>.json` is the batch; `<batch_id>.order_record.jsonl` is its sibling JSONL log.

## Cycle

Two passes. **Pass 1** cleans `pending/`: invalid → `failed/`, expired → `expired/` (after cancelling its live orders). Not-yet-valid batches stay put — they're scheduled work for a future cycle. **Invariant: no two** **`pending/`** **batches have overlapping** **`[valid_at, expires_at]`** **windows.** Violation ⇒ log error + skip the cycle (operator must intervene). **Pass 2** reconciles the (at most one) currently-active batch.

```
ORDERS = os.environ.get("GMX_ORDERS_DIR", "./orders")    # resolved once at startup

def run_cycle():
    now = unix_now()

    # one broker snapshot per cycle
    positions  = { (p.symbol, p.side): p.volume for p in get_positions() }
    unfinished = defaultdict(list)
    for o in get_unfinished_orders(): unfinished[o.symbol].append(o)

    # ── pass 1: clean up pending/ ────────────────────────────────────────
    seen, active = [], []
    for path in glob(f"{ORDERS}/pending/*.json"):
        try:    doc = parse_and_validate(path)
        except: move(path, f"{ORDERS}/failed/"); continue

        if now > doc.expires_at:
            cancel_alive(doc.batch_id, unfinished)    # cancel this batch's still-open cl_ord_ids
            move_pair(doc.batch_id, f"{ORDERS}/expired/")
            continue

        seen.append(doc)
        if now >= doc.valid_at:
            active.append(doc)
        else:
            log.info(f"scheduled: {doc.batch_id} valid_at={doc.valid_at} now={now}")

    # ── invariant: no two pending batches' [valid_at, expires_at] overlap ─
    seen.sort(key=lambda d: d.valid_at)
    for a, b in zip(seen, seen[1:]):
        if a.expires_at >= b.valid_at:           # a starts no later than b; overlap iff a ends at/after b starts
            log.error(f"overlap: {a.batch_id} <-> {b.batch_id}; operator must intervene"); return

    if not active:
        log.info("idle: no active batch"); return

    # ── pass 2: reconcile ────────────────────────────────────────────────
    doc = active[0]
    reconcile(doc, positions, unfinished)
    if matched(doc, positions, unfinished):
        move_pair(doc.batch_id, f"{ORDERS}/finished/")
```

`move_pair(batch_id, dst)` moves both `<batch_id>.json` and `<batch_id>.order_record.jsonl` (if present).

## Reconciliation (one batch, per order)

**Rule: any unfinished order on the symbol ⇒ skip the symbol this cycle.** Never cancel-and-resubmit. Just wait.

For each `order` in `doc.orders`, exactly one branch:

1. **`unfinished[order.symbol]`** **empty** — `held = positions.get((order.symbol, Long), 0)`; `diff = order.target − held`. If non-zero, call `order_volume(order.symbol, |diff|, Buy if diff>0 else Sell, order.order_type, Open, order.price)` and append a `submit` event.
2. **Any** **`cl_ord_id`** **NOT in this batch's submit log** — foreign order. `log.error`, skip the symbol.
3. **All** **`cl_ord_id`s ours** — `log.info`, skip the symbol. Per entry, also `log.error` if `now − created_at > 600s`.
4. **Exception in 1–3** — `log.exception`, skip the symbol. Next cycle retries.

Each symbol is in exactly one state per cycle: empty / ours / foreign. Only empty submits.

Submit is fire-and-forget. `cancel` events only come from `cancel_alive(...)` during pass 1 expiry; reconciliation never cancels.

## `matched(doc, positions, unfinished)`

True iff every `order` satisfies:

* `positions.get((order.symbol, Long), 0) == order.target`, and
* no `cl_ord_id` in `unfinished[order.symbol]` belongs to this batch.

Foreign orders don't block matched.

## Concurrency

The SDK fires `on_xxx` callbacks on **its own threads**, asynchronously, at any moment — including while a cycle is in progress. Two rules keep this manageable:

1. **Callbacks do no real work.** They append one line, set one event, set one flag. A callback that takes more than a few milliseconds risks blocking the SDK's dispatch loop and delaying the next event.
2. **`run_cycle()` runs on a thread we own**, not on the SDK's timer thread. `on_timer` only signals; a dedicated worker picks up the signal and runs the cycle. This way the timer thread always returns immediately, and a slow cycle never starves event delivery.

### Threads

| thread           | owner | what it does                                                                |
| ---------------- | ----- | --------------------------------------------------------------------------- |
| main             | us    | calls `gm.run()`, blocks until shutdown                                     |
| SDK timer        | gm    | fires `on_timer` → `cycle_pending.set()` → returns                          |
| SDK callbacks    | gm    | fire `on_order_status`, `on_execution_report`, the connect/disconnect pairs |
| `cycle_worker`   | us    | waits on `cycle_pending`, runs `run_cycle()`                                |
| `connector`      | us    | git pulls, copies into `pending/` (see [CONNECTOR.md](./CONNECTOR.md))      |
| remote-log relay | us    | drains queue, POSTs to Feishu (see [REMOTE_LOGGING.md](./REMOTE_LOGGING.md))|

**Only `cycle_worker` calls trading APIs** (`order_volume`, `get_positions`, `get_unfinished_orders`). Single-caller by construction — no extra serialisation needed for those.

### Locks

* `log_lock` — serialises **writes** to any `order_record.jsonl`.
* `log_move_lock` — serialises **path changes** (`move_pair`) and path lookups (`locate_record`).
* Lock order: `log_move_lock` then `log_lock`. Never the reverse.

No `cycle_lock` — only `cycle_worker` runs the cycle, so two cycles can't overlap by construction.

### Wiring

```Python
stop_event    = threading.Event()
cycle_pending = threading.Event()
log_lock      = threading.Lock()
log_move_lock = threading.Lock()

def init(context) -> None:
    set_token(GM_TOKEN)
    rebuild_clord_index()
    threading.Thread(target=cycle_worker, args=(stop_event, cycle_pending),
                     name="cycle-worker", daemon=True).start()
    timer(timer_func=on_timer, period=POLL_INTERVAL_MS, start_delay=0)

# ── SDK threads: tiny, never raise ─────────────────────────────────────
def on_timer(context) -> None:
    cycle_pending.set()                              # one signal, no work

def on_order_status(context, order) -> None:
    try:    append_status(order)
    except Exception: log.exception("on_order_status failed")

def on_execution_report(context, execrpt) -> None:
    try:    append_trade(execrpt)
    except Exception: log.exception("on_execution_report failed")

# ── our worker thread ──────────────────────────────────────────────────
def cycle_worker(stop_event: threading.Event, cycle_pending: threading.Event) -> None:
    consecutive_failures = 0
    while not stop_event.is_set():
        cycle_pending.wait()                         # block until signalled
        if stop_event.is_set(): return
        try:
            run_cycle()
            consecutive_failures = 0
        except Exception:
            consecutive_failures += 1
            backoff = min(60, 2 ** consecutive_failures)        # 2, 4, 8, … 60 s
            log.exception("cycle failed (#%d); backing off %ds", consecutive_failures, backoff)
            stop_event.wait(backoff)                 # interruptible sleep
        finally:
            cycle_pending.clear()                    # drop signals raised during the run
```

### Coalescing

`cycle_pending.set()` is idempotent. Any `set()` arriving during a run is wiped by the post-run `clear()` — the cycle in progress already reconciles from authoritative state (broker + filesystem), so re-running immediately would just repeat the same work. The worker only runs again on a fresh signal arriving after `clear()`, which the SDK timer and order-event callbacks reliably produce.

### Recovery

`run_cycle()` is **stateless across runs**: every cycle reads its inputs from the broker and the filesystem, computes a diff, acts, returns. It owns no state that survives the call. So any exception inside it is just lost work — the next cycle starts fresh and reconciles whatever is now true. Four things make this safe:

* **`try / except Exception`** around `run_cycle()` catches everything that can escape, logs with `exception()` (full traceback), and keeps the loop alive. The next signal triggers a fresh cycle.
* **`consecutive_failures` counter** — reset on success, incremented on every catch. Used to back off so a persistent failure doesn't spin.
* **Exponential backoff** capped at 60 s (`2, 4, 8, 16, 32, 60, 60, …`) — slow enough to stop flooding the log when the broker or disk is genuinely down, fast enough that the first transient blip costs only a couple of seconds.
* **`stop_event.wait(backoff)`** instead of `time.sleep(backoff)` — shutdown wakes the worker immediately even while it's mid-backoff. Never sleep on a non-interruptible primitive inside a worker.

All resources held during a cycle — `log_lock`, `log_move_lock`, file handles — **must** be acquired with `with`. An exception unwinding the stack must release them automatically. Stranding `log_lock` would freeze every callback that appends to `order_record.jsonl`, and the next cycle would see no order events arriving even though the broker is sending them. The existing `append()` and `replay()` already follow this rule; new I/O paths must too.

Note what is **not** included: no thread-level supervisor wrapping `cycle_worker`. The worker can only die from a `BaseException` that `except Exception` doesn't catch (`SystemExit`, `KeyboardInterrupt`) — both indicate the process itself is shutting down, and a restarted worker on a dying interpreter would be more harmful than helpful. If a real bug somehow kills the worker, the operator sees cycles stop and restarts the process.

## Log writing

Three writers (cycle thread + both callbacks). Every append is durable on disk before the locks release — `flush` + `fsync` before close, so a crash right after `append` returns cannot lose the line.

```Python
def append(batch_id, event):
    line = json.dumps(event, separators=(",",":")) + "\n"
    with log_move_lock:                          # path stable for this whole block
        path = locate_record(batch_id)
        with log_lock:                           # exclusive writer
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()                        # Python buffer → OS
                os.fsync(f.fileno())             # OS page cache → disk
```

`log_move_lock` is held for the whole block — `locate_record` does three `stat`s and we mustn't have a `move_pair` slip in between the lookup and the open (the path we found would no longer exist; on Windows, opening would also fail because the file got renamed). `log_lock` is the writer-exclusion lock; it spans only the open + write + fsync + close so concurrent appends serialise but slow `fsync`s don't block moves.

`move_pair(batch_id, dst)` symmetrically acquires `log_move_lock` (and only that) for the rename.

### Routing — `cl_ord_id → batch_id → file path`

Callbacks have `cl_ord_id`. The map `clord_index: cl_ord_id → batch_id`:

* **Startup** — `rebuild_clord_index()` scans every `*.order_record.jsonl` under `$GMX_ORDERS_DIR/{pending,finished,expired}/` for `submit` lines.
* **Live** — each successful `order_volume(...)` registers its entry inside the same `log_lock` section as the `submit` write, so map and line update atomically.

`locate_record(batch_id)` tries `pending/`, `finished/`, `expired/` and returns the first hit. Caller must hold `log_move_lock`. Miss ⇒ warn + drop (only possible after manual tampering).

### Reading the log (replay)

Both the cycle's per-batch replay (to identify our own `cl_ord_id`s and check ages) and `rebuild_clord_index()` at startup parse the JSONL file:

```Python
def replay(path):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:    out.append(json.loads(line))
            except json.JSONDecodeError:
                log.warning(f"corrupt line in {path}: {line!r}")
                continue            # tolerate one partial last line after a crash
    return out
```

Why this is safe — what could go wrong with concurrent / interrupted writes:

| risk                                                          | actual exposure                                                                                                                                                                                 |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| reader sees a half-written line while a writer is mid-`write` | none in steady state: writers hold `log_lock` for the whole open→fsync→close. Cycle replay also acquires `log_lock` (briefly, just to copy the file or take a snapshot length) before iterating |
| reader sees a half-written line **after a crash**             | possible only on the very last line; the `try/except` skips it. All preceding lines are durable thanks to `fsync`                                                                               |
| two writers interleave bytes within one line                  | impossible: `log_lock` serialises full append calls                                                                                                                                             |
| line not yet `\n`-terminated when reader stops                | impossible: `f.write(line)` writes the whole `"...\n"` in one call before `flush`/`fsync`                                                                                                       |
| Unicode split across writes                                   | impossible: each line is a single `f.write` of a single str                                                                                                                                     |

The replay never modifies the file, so concurrent replays are fine and never block writers (other than the brief `log_lock` they hold to take their snapshot).

### Truth table — concurrency cases

| event arrives                                 | state at append time                | outcome                                          |
| --------------------------------------------- | ----------------------------------- | ------------------------------------------------ |
| two writers race for the same file            | both hold valid `cl_ord_id`s        | `log_lock` serialises in lock-acquire order      |
| `status` / `trade` for a known `cl_ord_id`    | batch in `pending/`                 | append in `pending/`                             |
| `status` / `trade` for a known `cl_ord_id`    | batch moved to `finished`/`expired` | append wherever the file is now                  |
| `status` / `trade` during process boot        | `clord_index` not yet rebuilt       | impossible — `init` rebuilds before timer starts |
| `status` / `trade` for an unknown `cl_ord_id` | no submit line anywhere             | warn + drop                                      |

### Truth table — what does the cycle *do* for one order?

Two inputs: broker snapshot (`positions`, `unfinished`) + this batch's submit log (ownership only).

| `unfinished[symbol]`          | log says about those `cl_ord_id`s | action                                                                    |
| ----------------------------- | --------------------------------- | ------------------------------------------------------------------------- |
| empty                         | —                                 | submit `target − held` if non-zero; else done                             |
| has entries, **all** ours     | every `cl_ord_id` in submit lines | log info + skip symbol; per entry, log error if `now − created_at > 600s` |
| has entries, **any** not ours | one or more missing from log      | log error + skip symbol — foreign order, operator must intervene          |

A dropped callback can't break this — `unfinished` is the live signal. A dropped `submit` line (crash between `order_volume` return and the write) makes its `cl_ord_id` look foreign → error + skip; once it fills or the operator clears it, the next cycle's diff is correct.

## Lifecycle

`gm.run()` hosts the strategy. Everything that happens to it surfaces either as the timer firing or as one of the callbacks below. We register the minimum and have a defined response for the rest. See [GM_SDK.md](./GM_SDK.md) for the per-callback object shapes.

**Connection guard.** When the trade channel drops, the SDK auto-reconnects, but during the gap `get_positions()` / `get_unfinished_orders()` may return **empty** (not raise) — and acting on that empty snapshot would diff against zero and submit huge orders. A `trade_channel_up: threading.Event` (set at startup, cleared on disconnect, re-set on reconnect) gates the cycle:

```Python
trade_channel_up = threading.Event()
trade_channel_up.set()                              # optimistic at startup; first heartbeat confirms

def run_cycle() -> None:
    if not trade_channel_up.is_set():
        log.warning("skipping cycle: trade channel down"); return
    ...                                             # rest of the cycle
```

### Event table

| event                                       | when fires                                                                | action                                                                                          |
| ------------------------------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `init(context)`                             | once after `run()` connects                                               | `set_token`; rebuild `clord_index`; start connector + remote-log threads; register the timer    |
| `on_order_status(context, order)`           | every `status` change                                                     | append one `status` event to the batch's `order_record.jsonl` (under `log_lock`)                |
| `on_execution_report(context, execrpt)`     | every fill (`exec_type=15`) and every cancel-rejection (`exec_type=19`)   | append one `trade` event to the batch's `order_record.jsonl`                                    |
| `on_trade_data_connected(context)`          | trade channel up (initial or after reconnect)                             | `trade_channel_up.set()`; log info                                                              |
| `on_trade_data_disconnected(context)`       | trade channel drops (SDK auto-reconnects)                                 | `trade_channel_up.clear()`; log warning. Cycle skips itself until restored                      |
| `on_account_status(context, account)`       | account state changes (connected / logged-in / disconnected / error)     | log. On `disconnected` / `error`, clear `trade_channel_up`; on `logged-in`, set it             |
| `on_market_data_connected(context)`         | market-data channel up                                                    | log info — we don't subscribe to ticks or bars                                                  |
| `on_market_data_disconnected(context)`      | market-data channel down                                                  | log info                                                                                        |
| `on_error(context, code, info)`             | SDK-level error (network glitch, RPC error, ...)                          | log `code` + `info`; never raise out. SDK keeps running                                         |
| `on_tick`, `on_bar`                         | —                                                                         | not registered                                                                                  |
| `on_backtest_finished`                      | backtest only                                                             | n/a — we always run `MODE_LIVE`                                                                 |
| `SIGINT` / `SIGTERM` (operator)             | external signal to main thread                                            | main sets `stop_event` → workers exit → call SDK `stop()` → return                              |

**Two rules every callback obeys**:

1. **Append-only side effect.** A callback only ever appends to an `order_record.jsonl` (or toggles `trade_channel_up`). It never calls `run_cycle`, never submits, never cancels.
2. **Never raise.** An uncaught exception in a callback can take down the SDK's dispatch loop. Wrap every callback body in `try / except Exception: log.exception(...)`.

### Shutdown

`gm.run()` blocks the main thread; the workers (connector, remote-log relay, SDK internals) do not receive signals. Wrap `run()`:

```Python
def main() -> None:
    try:
        run(strategy_id=..., filename=..., mode=MODE_LIVE, token=GM_TOKEN)
    except KeyboardInterrupt:
        log.info("SIGINT received; shutting down")
    finally:
        stop_event.set()                            # wakes connector + remote-log relay
        for t in worker_threads:
            t.join(timeout=10)                      # daemon, abrupt-killed if join overruns
        try: stop()                                 # ask the SDK to close cleanly
        except Exception: log.exception("gm.stop() failed")
```

In-flight orders are **never** cancelled at shutdown — that's neither the cycle's nor `stop()`'s job. The broker keeps them on the book; on the next startup, the cycle's pass-1 expiry handler cancels anything whose batch has gone past `expires_at`. A clean shutdown and a crash look identical to the broker.

`order_record.jsonl` is durable per append (`fsync` before close), so a crash anywhere never loses a recorded event.

## Failure handling

Never halt on data or trading errors. Per-order exceptions stay local; cycle-wide exceptions early-return and retry next fire. A batch leaves `pending/` only as **finished**, **expired**, or **invalid**.

## Config (env)

* `GM_TOKEN`         — auth, passed to `set_token`.
* `GM_ACCOUNT_ID`    — default account when a batch omits `account_id`.
* `GMX_ORDERS_DIR`   — orders root containing `pending/`, `finished/`, `expired/`, `failed/`. Default `./orders`. Resolved once at startup; every path lookup, glob, move, and rebuild scan in this document is relative to it.
* `GMX_POLL_SECONDS` — timer interval, default 30.

