# Execution Flow

`gm.api.run()` hosts the strategy. Its `timer(...)` fires every `GMX_POLL_SECONDS`; each fire is one **cycle** that scans `pending/`, retires expired/invalid batches, and reconciles the single active batch.

* Schema: [SCHEMA.md](./SCHEMA.md)
* Per-batch log: [ORDER_RECORD.md](./ORDER_RECORD.md)

## Layout

```
$GMX_ORDERS_DIR/        # default ./orders
  pending/              # 0 or 1 active batch + future-dated ones
  finished/             # batch + log, after match
  expired/              # batch + log, after expires_at; live orders cancelled first
  failed/               # batch only, parse / schema error
```

Per batch: `<batch_id>.json` + `<batch_id>.order_record.jsonl`.

## Cycle

Two passes. Pass 1 cleans `pending/`; pass 2 reconciles.

```python
def run_cycle():
    now = unix_now()
    positions, unfinished = broker_snapshot()           # one call per cycle

    seen, active = pass_one(now, unfinished)            # invalid → failed/, expired → expired/+cancel
    if has_overlap(seen): return                        # invariant: no two pending windows overlap
    if not active:        return

    doc = active[0]
    reconcile(doc, positions, unfinished)
    if matched(doc, positions, unfinished):
        move_pair(doc.batch_id, FINISHED_DIR)
```

**Pass-1 invariant**: no two `pending/` batches have overlapping `[valid_at, expires_at]` windows. Violation ⇒ skip the cycle (operator must intervene).

## Reconciliation

Per order, exactly one branch — **never cancel-and-resubmit, just wait**:

| `unfinished[symbol]`    | action                                                                     |
| ----------------------- | -------------------------------------------------------------------------- |
| empty                   | submit `target − held` if non-zero; else done                              |
| all `cl_ord_id`s ours   | log info + skip; per stuck entry (`age > 600s`), log error                 |
| any `cl_ord_id` foreign | log error + skip; operator must intervene                                  |

Submit is fire-and-forget. The cycle never cancels — `cancel` events come only from pass-1 expiry.

`matched(doc, positions, unfinished)` is true iff every order's held volume equals its target AND no `cl_ord_id` in `unfinished[symbol]` belongs to this batch. Foreign orders don't block matched.

## Threads

| thread                | owner | what it does                                                              |
| --------------------- | ----- | ------------------------------------------------------------------------- |
| main                  | us    | calls `gm.run()`, blocks until shutdown                                   |
| SDK timer / callbacks | gm    | tiny bodies — set events, snapshot + enqueue                              |
| `cycle-worker`        | us    | waits on `cycle_pending`, runs `run_cycle()`                              |
| `connector`           | us    | git pulls + mirror into `pending/` ([CONNECTOR.md](./CONNECTOR.md))       |
| `callback-processor`  | us    | drains the SDK→drain queue, appends to `order_record.jsonl`               |
| remote-log relay      | us    | drains queue, POSTs to Feishu ([REMOTE_LOGGING.md](./REMOTE_LOGGING.md))  |

Only `cycle-worker` calls trading APIs (`order_volume`, `get_position`, `get_unfinished_orders`) — single-caller by construction.

## Callbacks

SDK fires callbacks on its own threads at any moment. Two rules:

1. **Snapshot + enqueue, never block.** `on_order_status` and `on_execution_report` copy fields off the SDK object (which the SDK may reuse after return), put a dict on the queue, and return. No locks acquired in the SDK thread.
2. **Never raise.** An uncaught exception kills the SDK's dispatch loop. Wrap every body in `try / except Exception: log.exception(...)`.

```python
_event_queue = queue.SimpleQueue()

def on_order_status(context, order):
    try:
        log.info("recv status: cl_ord_id=%s ...", order.cl_ord_id, ...)
        _event_queue.put({"kind": "status", "ts_ms": unix_now_ms(), ...})
    except Exception: log.exception("on_order_status enqueue failed")

def _drain_loop():                                       # callback-processor thread
    while True:
        evt = _event_queue.get()
        if evt is None: return                           # sentinel from stop()
        try:
            with batch_state_lock:                       # serialise behind any in-flight cycle
                batch_id = clord_index.get(evt["cl_ord_id"])
                if batch_id is None: warn_foreign(evt); continue
                order_log.append(batch_id, build_event(evt))
        except Exception: log.exception("processor failed: %r", evt)
```

The split decouples SDK dispatch from our locks. Events delivered while a cycle holds `batch_state_lock` sit safely in the queue; the drain processes them after the cycle releases. No event for our own orders is ever lost; "foreign" warnings now genuinely mean "not ours".

## Locks

* `log_lock` — serialises **writes** to any `order_record.jsonl`. Brief: open → write → fsync → close.
* `batch_state_lock` — serialises **everything that observes or changes which directory a batch lives in**: cross-dir moves, path lookups, the connector's `is_terminal`+`atomic_copy`, the entire `run_cycle`, and every drained callback event. Reentrant — the cycle re-acquires it via `order_log` helpers.
* Lock order: `batch_state_lock` then `log_lock`. Never reverse.

## Routing — `cl_ord_id → batch_id`

`clord_index: dict[cl_ord_id, batch_id]`:

* **Startup** — `rebuild_clord_index()` scans every `*.order_record.jsonl` under live dirs for `submit` lines.
* **Live** — `cycle-worker` writes the entry inside its `batch_state_lock` section. The drain reads under the same lock, so it always observes a fully-committed index. Miss ⇒ foreign cl_ord_id; warn + drop.

`locate_record(batch_id)` tries `pending/` → `finished/` → `expired/` and returns the first hit. Caller must hold `batch_state_lock`.

## Recovery

`run_cycle()` is **stateless across runs**: every cycle reads broker + filesystem fresh, computes a diff, acts, returns. An exception inside is just lost work — the next cycle reconciles whatever is now true.

`cycle-worker` wraps the call in `try / except Exception`, increments `consecutive_failures`, and `stop_event.wait(backoff)` (capped at 60s). Resources are always released by `with` — stranding `batch_state_lock` would freeze the connector and the drain.

A dropped event can't break correctness — `unfinished` from the broker is the live signal. A dropped `submit` line (crash between `order_volume` return and write) makes its `cl_ord_id` look foreign on the next cycle ⇒ error + skip; once it fills or the operator clears it, the diff catches up.

## Connection guard

When the trade channel drops, the SDK auto-reconnects, but during the gap `get_position()` may return empty (not raise) — and acting on empty would diff against zero and submit huge orders. `trade_channel_up: threading.Event` (set at startup, cleared on disconnect) gates the cycle:

```python
def run_cycle():
    if not trade_channel_up.is_set():
        log.warning("skipping cycle: trade channel down"); return
    if state.trade_channel_up_at > 0 \
       and time.time() - state.trade_channel_up_at < CYCLE_GRACE_SECONDS:
        log.info("skipping cycle: reconnect grace ..."); return    # see "Reconnect replay"
    if state.last_cycle_end_at > 0 \
       and time.time() - state.last_cycle_end_at < CYCLE_GRACE_SECONDS:
        log.info("skipping cycle: min gap ..."); return            # let drain/connector breathe
    ...
```

`trade_channel_up_at` is stamped on every `on_trade_data_connected`. While within `CYCLE_GRACE_SECONDS`, the cycle skips — costs one cycle at cold start, lets reconnect replay settle.

`last_cycle_end_at` is stamped at every cycle end. Same window enforces a min gap so other threads get uncontended time when a cycle exceeds the timer interval.

## Reconnect replay

GM server restarts daily around ~08:50 China time, dropping the trade channel for a few seconds. **On reconnect, the broker replays recent order statuses and execution reports** — `on_order_status` and `on_execution_report` fire for every order from the past ~24h, in one burst.

The drain handles this naturally: events queue, process under `batch_state_lock`, append to whichever batch each `cl_ord_id` belongs to (via `clord_index`). No special handling needed.

Operational notes:

* Burst depth observed: ~50–100 events for a normal day; drain pending climbs to many minutes before catching up
* `clord_index` is rebuilt from live `.order_record.jsonl` files at startup, so replays for batches still under `pending/finished/expired/` route correctly
* If a `cl_ord_id` is replayed for a batch whose record was lost (crash between `order_volume` return and submit append), it surfaces as `status for foreign cl_ord_id ...; dropping`. Harmless — broker is still source of truth on the next cycle's `get_unfinished_orders()`.
* Filled orders are replayed too: a `status=Filled` line, then `exec_type=Trade` execrpt(s). Both append to the historical batch's log; reconciliation has long since moved that batch to `finished/`.

## Lifecycle

`init(context)` runs once after `run()` connects:
1. `ensure_dirs()` + `rebuild_clord_index()`
2. `callbacks.start()` — drain worker begins (after index rebuild, so queued events from before init see a populated index)
3. `worker.start()` + `connector.start()`
4. `timer(...)`

Shutdown wraps `run()`:

```python
def main():
    try: run(...)
    except KeyboardInterrupt: log.info("SIGINT received; shutting down")
    finally:
        stop_event.set()                                 # wakes cycle, connector
        callbacks.stop()                                 # sentinel wakes drain
        for t in worker_threads: t.join(timeout=10)
        try: stop()
        except Exception: log.exception("gm.stop() failed")
```

In-flight orders are **never** cancelled at shutdown. The broker keeps them; on next startup, pass-1 expiry cancels anything past `expires_at`. A clean shutdown and a crash look identical to the broker.

## Config (env)

| var                | required | default    | meaning                                              |
| ------------------ | -------- | ---------- | ---------------------------------------------------- |
| `GM_TOKEN`         | **yes**  | —          | auth, passed to `set_token`                          |
| `GM_ACCOUNT_ID`    | no       | —          | default when a batch omits `account_id`              |
| `GMX_ORDERS_DIR`   | no       | `./orders` | orders root for `pending/`, `finished/`, `expired/`, `failed/` |
| `GMX_POLL_SECONDS` | no       | `30`       | timer interval                                       |
| `GMX_CYCLE_GRACE_SECONDS` | no | `30` | cycle skip window after each `on_trade_data_connected` AND min gap between cycles |
