# Order Record (per-batch log)

A per-batch append-only **JSONL** log: `<batch_id>.order_record.jsonl`, kept next to the batch JSON. Three writers append to it: the cycle thread (`submit`, `cancel`) and two SDK callbacks (`status`, `trade`). Never edited, truncated, or rotated.

The log is the **only** persistent state the executor keeps. There is no separate ledger / state.json. The broker (via `get_unfinished_orders()` and `get_position()`) is the source of *current* truth; the log is the source of *historical* truth and the **only** place `order_id ↔ cl_ord_id` is recorded.

## Lifecycle

The log is created the first time the executor submits an order for the batch. It then moves with the JSON file:

| transition | both files go to   | live orders               |
| ---------- | ------------------ | ------------------------- |
| matched    | `orders/finished/` | none — they all filled    |
| expired    | `orders/expired/`  | cancelled before the move |
| invalid    | `orders/failed/`   | log never existed         |

Callbacks may still fire after a move (e.g. a late `Canceled` after we moved the batch to `expired/`). The writer routes by `cl_ord_id → batch_id` and appends wherever the file currently lives. See [FLOW.md → Log writing](./FLOW.md#log-writing).

## Events (one JSON object per line, separated by `\n`)

Every line carries `ts_ms` — unix epoch **milliseconds** (UTC) at the moment the line was written. Millisecond resolution is required because callback events for a single `cl_ord_id` can arrive within the same wall-clock second; the timestamp must distinguish them.

### `submit` — written by **cycle thread**, after `order_volume(...)` returns

```JSON
{"ts_ms": 1745996400123, "event": "submit", "order_id": "o1",
 "symbol": "SHSE.600000", "side": "buy", "volume": 50,
 "order_type": "market", "price": 0, "cl_ord_id": "GMX-...."}
```

| field       | meaning                                                       |
| ----------- | ------------------------------------------------------------- |
| `ts_ms`     | unix milliseconds (UTC) when the line was written             |
| `order_id`  | the `id` from the batch's `orders[]`                          |
| `cl_ord_id` | SDK-issued client order id; the live identifier going forward |
| others      | the parameters passed to `order_volume(...)`                  |

This is the only line that anchors `order_id ↔ cl_ord_id`. Without it, no other event can be attributed to a batch order.

### `cancel` — written by **cycle thread**, after `order_cancel(...)` is dispatched

```JSON
{"ts_ms": 1745996401456, "event": "cancel", "cl_ord_id": "GMX-...."}
```

Records *intent*. The broker may accept (we'll see a `status` line with `Canceled`) or refuse (we'll see a `trade` line with `exec_type=19`).

### `status` — written by `on_order_status(context, order)`

```JSON
{"ts_ms": 1745996402789, "event": "status", "cl_ord_id": "GMX-....",
 "status": 3, "status_text": "Filled",
 "filled_volume": 50, "ord_rej_reason": 0}
```

| field            | meaning                                                                                                                    |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `status`         | numeric `OrderStatus` (see [GM\_SDK.md](./GM_SDK.md#orderstatus-lifecycle-of-one-cl_ord_id)) — the field replay logic uses |
| `status_text`    | the constant name (`"PendingNew"`, `"New"`, `"Filled"`, `"Canceled"`, ...) — for human reading only, never parsed          |
| `filled_volume`  | broker's running total filled — monotonic, never decreases                                                                 |
| `ord_rej_reason` | numeric `OrderRejectReason`, only meaningful when `status == 8` (Rejected)                                                 |

Multiple `status` lines per `cl_ord_id` are normal (e.g. `10 → 1 → 2 → 3`). The replay rule keeps the latest.

### `trade` — written by `on_execution_report(context, execrpt)`

Two variants, distinguished by `exec_type`. Fields that don't apply to a variant are omitted, not zeroed.

**Fill** (`exec_type=15`):

```JSON
{"ts_ms": 1745996402812, "event": "trade", "cl_ord_id": "GMX-....",
 "broker_order_id": "B-...", "exec_id": "E-...",
 "exec_type": 15, "exec_type_text": "Trade",
 "symbol": "SHSE.600000", "side": 1, "side_text": "buy",
 "volume": 30, "price": 11.42, "amount": 342.6,
 "broker_ts_ms": 1745996402800}
```

**Cancel-rejection** (`exec_type=19`):

```JSON
{"ts_ms": 1745996402812, "event": "trade", "cl_ord_id": "GMX-....",
 "broker_order_id": "B-...", "exec_id": "E-...",
 "exec_type": 19, "exec_type_text": "CancelRejected",
 "symbol": "SHSE.600000",
 "ord_rej_reason": 5, "ord_rej_reason_detail": "已成交，无法撤单",
 "broker_ts_ms": 1745996402800}
```

| field                   | when present    | meaning                                                                                           |
| ----------------------- | --------------- | ------------------------------------------------------------------------------------------------- |
| `ts_ms`                 | always          | unix ms (UTC) when **we** wrote the line                                                          |
| `cl_ord_id`             | always          | links back to the `submit` line                                                                   |
| `broker_order_id`       | always          | broker's 柜台委托 ID (`execrpt.order_id`); for ops cross-reference with broker statements             |
| `exec_id`               | always          | broker's unique execution id; the idempotency key                                                 |
| `exec_type`             | always          | `15` = fill chunk, `19` = cancel was refused                                                      |
| `exec_type_text`        | always          | `"Trade"` or `"CancelRejected"` — for human reading only, never parsed                            |
| `symbol`                | always          | redundant with `submit` but cheap and aids `grep`                                                 |
| `side`, `side_text`     | fills only      | `1`/`"buy"` or `2`/`"sell"`                                                                       |
| `volume`                | fills only      | shares filled in this chunk                                                                       |
| `price`                 | fills only      | execution price of this chunk                                                                     |
| `amount`                | fills only      | yuan in this chunk (`execrpt.amount`)                                                             |
| `ord_rej_reason`        | rejections only | numeric `OrderRejectReason`                                                                       |
| `ord_rej_reason_detail` | rejections only | broker's human-readable description                                                               |
| `broker_ts_ms`          | always          | broker-side event time (`execrpt.created_at`) in unix ms — diff with `ts_ms` for callback latency |

`status` already carries `filled_volume` (broker's running total), so `trade` lines are diagnostic — they let an audit reconstruct chunk-by-chunk fills, but the cycle never needs them. `exec_type=19` lines are how a cancel-rejection surfaces: status will not move to `Canceled`, so without this line a stuck `cancel` would look like a silent no-op.

## Replay (truth table)

The log produces one row per `cl_ord_id`. Scan top-to-bottom and reduce:

| derived field      | from                                                |
| ------------------ | --------------------------------------------------- |
| `order_id`         | the (only) `submit` line for this `cl_ord_id`       |
| `submit_args`      | same `submit` line                                  |
| `latest_status`    | latest `status` line; missing ⇒ "no broker ack yet" |
| `filled_volume`    | latest `status.filled_volume`; missing ⇒ `0`        |
| `cancel_requested` | any `cancel` line exists                            |
| `cancel_refused`   | any `trade` line with `exec_type == 19`             |

From those, the per-`cl_ord_id` state is one of:

| `latest_status`                      | `cancel_requested` | meaning                                | live? |
| ------------------------------------ | ------------------ | -------------------------------------- | ----- |
| missing                              | no                 | submit just landed; no broker ack yet  | live  |
| `PendingNew`/`New`/`PartiallyFilled` | no                 | resting on the book                    | live  |
| `PendingNew`/`New`/`PartiallyFilled` | yes                | cancel pending (or refused; see below) | live  |
| `Filled`                             | any                | done; `filled_volume == submit.volume` | dead  |
| `Canceled`/`Rejected`/`Expired`      | any                | dead, will not fill more               | dead  |

Then per `order_id`: walk all `cl_ord_id`s in submit order; only the **latest** matters for the next cycle's diff. The cycle cross-checks every "live" `cl_ord_id` against `get_unfinished_orders()`:

* present on broker → live (truth).
* missing on broker → terminal-by-loss (treat as dead even if no `status` line arrived).

The broker reply wins. The log is a hint that lets us link `order_id` to `cl_ord_id` and detect already-issued cancels; it never overrides a live broker query.

## Rules

* **Append-only.** Never seek backwards, never overwrite a line.
* **One file per batch.** Two batches never share a log.
* **Filename is identity.** `<batch_id>.order_record.jsonl` matches `<batch_id>.json` exactly.
* **Out-of-order tolerant.** Any interleaving of writers is legal; replay reduces deterministically.

