# Order Record (per-batch log)

Per-batch append-only **JSONL** log: `<batch_id>.order_record.jsonl`, kept next to `<batch_id>.json`. Two writers append to it: the **cycle worker** (`submit`, `cancel`) and the **callback-processor** drain (`status`, `trade`). Never edited, truncated, or rotated.

The log is the **only** persistent state the executor keeps. There is no separate ledger / state.json. The broker (via `get_unfinished_orders()` and `get_position()`) is the source of *current* truth; the log is the source of *historical* truth and the **only** place `order_id ↔ cl_ord_id` is recorded.

## Lifecycle

The log is created the first time the cycle submits an order for the batch. It then moves with the JSON file:

| transition | both files go to   | live orders               |
| ---------- | ------------------ | ------------------------- |
| matched    | `orders/finished/` | none — they all filled    |
| expired    | `orders/expired/`  | cancelled before the move |
| invalid    | `orders/failed/`   | log never existed         |

Callbacks may still fire after a move (e.g. a late `Canceled` after `expired/`). The drain routes by `cl_ord_id → batch_id` (via `clord_index`) and appends wherever the file currently lives.

## Events (one JSON object per line, separated by `\n`)

Every line carries `ts_ms` — unix epoch **milliseconds** (UTC). Status/trade events use the **callback timestamp** (when SDK delivered the event), captured in the SDK thread before enqueue, so it reflects broker timing rather than drain backlog.

### `submit` — cycle worker, after `order_volume(...)` returns

```jsonc
{"ts_ms":1745996400123,"event":"submit","order_id":"o1",
 "symbol":"SHSE.600000","side":"buy","position_effect":"open","volume":50,
 "order_type":"market","price":0,"cl_ord_id":"GMX-...."}
```

| field             | meaning                                                       |
| ----------------- | ------------------------------------------------------------- |
| `order_id`        | the `id` from the batch's `orders[]`                          |
| `cl_ord_id`       | SDK-issued client order id; the live identifier going forward |
| `position_effect` | `"open"` (buy) or `"close"` (sell) — A-shares                 |
| others            | parameters passed to `order_volume(...)`                      |

This is the only line that anchors `order_id ↔ cl_ord_id`. Without it, no other event can be attributed to a batch order.

### `cancel` — cycle worker, after `order_cancel(...)` is dispatched

```jsonc
{"ts_ms":1745996401456,"event":"cancel","cl_ord_id":"GMX-...."}
```

Records *intent*. Broker accepts ⇒ a `status` line with `Canceled` arrives later; refuses ⇒ a `trade` line with `exec_type=19` arrives.

### `status` — drain, from `on_order_status`

```jsonc
{"ts_ms":1745996402789,"event":"status","cl_ord_id":"GMX-....",
 "symbol":"SHSE.600000","status":3,"status_text":"Filled",
 "filled_volume":50}
```

When `status==8 (Rejected)`, three extra fields appear: `ord_rej_reason` (numeric), `ord_rej_reason_text` (constant name), `ord_rej_reason_detail` (broker's human-readable string — often the most informative for debugging).

Multiple `status` lines per `cl_ord_id` are normal (`PendingNew → New → PartiallyFilled → Filled`). Replay keeps the latest.

### `trade` — drain, from `on_execution_report`

Two variants distinguished by `exec_type`. Fields that don't apply are omitted.

**Fill** (`exec_type=15`):

```jsonc
{"ts_ms":1745996402812,"event":"trade","cl_ord_id":"GMX-....",
 "broker_order_id":"B-...","exec_id":"E-...",
 "exec_type":15,"exec_type_text":"Trade","symbol":"SHSE.600000",
 "side":1,"side_text":"buy","volume":30,"price":11.42,"amount":342.6,
 "broker_ts_ms":1745996402800}
```

**Cancel-rejection** (`exec_type=19`):

```jsonc
{"ts_ms":1745996402812,"event":"trade","cl_ord_id":"GMX-....",
 "broker_order_id":"B-...","exec_id":"E-...",
 "exec_type":19,"exec_type_text":"CancelRejected","symbol":"SHSE.600000",
 "ord_rej_reason":5,"ord_rej_reason_text":"...","ord_rej_reason_detail":"已成交，无法撤单",
 "broker_ts_ms":1745996402800}
```

| field             | when            | meaning                                                  |
| ----------------- | --------------- | -------------------------------------------------------- |
| `broker_order_id` | always          | broker's 柜台委托 ID; cross-reference with statements    |
| `exec_id`         | always          | broker's unique execution id; idempotency key            |
| `side*`,`volume`,`price`,`amount` | fills only      | chunk details                                            |
| `ord_rej_reason*` | rejections only | reject code, text, broker description                    |
| `broker_ts_ms`    | always          | broker-side time; diff with `ts_ms` for callback latency |

`status` carries `filled_volume` (broker's running total), so `trade` lines are diagnostic — they let an audit reconstruct chunk-by-chunk fills, but the cycle never needs them. `exec_type=19` is how a cancel-rejection surfaces — without it a stuck `cancel` would look like a silent no-op.

## Replay

Per `cl_ord_id`, scan top-to-bottom and reduce:

| derived            | from                                                |
| ------------------ | --------------------------------------------------- |
| `order_id`         | the (only) `submit` line                            |
| `latest_status`    | latest `status` line; missing ⇒ "no broker ack yet" |
| `filled_volume`    | latest `status.filled_volume`; missing ⇒ `0`        |
| `cancel_requested` | any `cancel` line exists                            |
| `cancel_refused`   | any `trade` line with `exec_type==19`               |

Per-`cl_ord_id` state:

| `latest_status`                      | `cancel_requested` | meaning                            | live? |
| ------------------------------------ | ------------------ | ---------------------------------- | ----- |
| missing                              | no                 | submit landed, no broker ack yet   | live  |
| `PendingNew`/`New`/`PartiallyFilled` | no                 | resting on the book                | live  |
| `PendingNew`/`New`/`PartiallyFilled` | yes                | cancel pending (or refused)        | live  |
| `Filled`/`Canceled`/`Rejected`/`Expired` | any            | terminal, won't fill more          | dead  |

The cycle then cross-checks every "live" `cl_ord_id` against `get_unfinished_orders()`: present ⇒ live (truth); missing ⇒ terminal-by-loss. **The broker reply wins.** The log is a hint that links `order_id` to `cl_ord_id` and detects already-issued cancels; it never overrides a live broker query.

## Rules

* **Append-only.** Never seek backwards, never overwrite a line.
* **One file per batch.** Two batches never share a log.
* **Filename is identity.** `<batch_id>.order_record.jsonl` matches `<batch_id>.json`.
* **Out-of-order tolerant.** Any interleaving of writers is legal; replay reduces deterministically.

## Durability

`f.write(line)` only — **no `os.fsync`, no explicit `flush`**. `close()` flushes Python's userspace buffer to the OS, so a crash never loses an already-written line; power-cut may lose a few. The broker is the source of current truth (reconciled every cycle); the log is audit.

## File-IO shape

The cycle worker uses `order_log.cycle_session(batch_id)` — one open fd held across all appends in one cycle, closed at cycle end. The callback-drain path uses `order_log.append(batch_id, event)` — open/write/close per event. See [FLOW.md § Per-cycle log writes](./FLOW.md) for the rationale (cloud-disk metadata-IOPS throttling, 2026-05-03 measurement).
