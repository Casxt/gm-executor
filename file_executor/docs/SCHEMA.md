# Order Batch Schema

A batch is one JSON document. Every order is **a target, not a delta**: the value declares what the position should look like *after* execution.

A batch is a **declarative final state**, not a one-shot script. Each batch lives as `orders/pending/<batch_id>.json`. The executor re-runs the batch every timer cycle until every order's `target` matches the held position, then retires it (moves the JSON and its log to `orders/finished/`). A failure on one order does not stop the others — the cycle finishes the rest, the failed one is retried on the next cycle.

Stocks-only (A-shares). The executor derives `side` / `position_effect` / `position_side` from current vs target.

## Document

```jsonc
{
  "schema_version": "1",                            // must equal "1"
  "batch_id":       "20260430-rebalance-001",       // globally unique; matches the filename
  "valid_at":       1745990400,                     // unix epoch seconds (UTC); cycle skips before this time
  "expires_at":     1745996400,                     // unix epoch seconds (UTC); cancel + retire after this time
  "account_id":     "<string>",                     // optional; default account if absent
  "orders":         [ /* >=1 */ ]
}
```

The file MUST be named `<batch_id>.json`. The executor maintains a sibling append-only log `<batch_id>.order_record.jsonl` (see [ORDER\_RECORD.md](./ORDER_RECORD.md)).

## Order

| field        | type    | rule                                                                   |
| ------------ | ------- | ---------------------------------------------------------------------- |
| `id`         | string  | unique within the batch                                                |
| `symbol`     | string  | `^(SHSE\|SZSE\|BJSE)\.[A-Za-z0-9]+$`                                   |
| `target`     | integer | absolute share count to hold; `>= 0`                                   |
| `order_type` | string  | `market` or `limit`                                                    |
| `price`      | number  | required iff `order_type=limit`, must be `> 0`; forbidden for `market` |

No other keys allowed at the top level or on an order.

## Validation

1. JSON parses.
2. Top-level shape exactly as above; no unknown keys.
3. `schema_version == "1"`.
4. `batch_id` matches `^[A-Za-z0-9._:-]{1,64}$` and equals the filename without `.json`.
5. `valid_at`, `expires_at` are non-negative integers (UTC seconds), `valid_at < expires_at`.
6. `orders` is non-empty; every `id` is unique; every `symbol` is unique within the batch.
7. Each order: only the fields above; `target >= 0`; `order_type` valid; `price` rule holds; `symbol` matches the regex.

If any check fails: file is moved to `orders/failed/`. The executor moves on; one bad batch never halts the program.

## Operating constraint: non-overlapping windows in `pending/`

`orders/pending/` may hold any number of batches **as long as no two have overlapping** **`[valid_at, expires_at]`** **windows**. The executor checks this each cycle; on overlap it logs an error and skips the cycle without touching anything (operator must intervene). The non-overlap rule guarantees at most one batch is ever active at a given `now`; future-dated batches are allowed to sit in `pending/` and the cycle simply skips them until their `valid_at`.

## Example

```JSON
{
  "schema_version": "1",
  "batch_id":   "20260430-rebalance-001",
  "valid_at":   1745990400,
  "expires_at": 1745996400,
  "orders": [
    { "id": "o1", "symbol": "SHSE.600000", "target": 200,  "order_type": "market" },
    { "id": "o2", "symbol": "SZSE.000001", "target": 8000, "order_type": "limit", "price": 12.50 },
    { "id": "o3", "symbol": "SHSE.600519", "target": 100,  "order_type": "market" }
  ]
}
```

