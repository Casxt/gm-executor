# GM SDK — surface used by gm-executor

Subset of `gm.api` we actually call. Source: [https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D/%E5%9F%BA%E6%9C%AC%E5%87%BD%E6%95%B0.html](https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D/%E5%9F%BA%E6%9C%AC%E5%87%BD%E6%95%B0.html) and the trading-functions page on the same site.

## Lifecycle

### `run(strategy_id='', filename='', mode=MODE_UNKNOWN, token='', serv_addr='', ...)`

Entrypoint. We only care about:

| param           | meaning                                                |
| --------------- | ------------------------------------------------------ |
| `strategy_id` | system-issued strategy id                              |
| `filename`    | path to this script (the SDK re-imports it internally) |
| `mode`        | **`MODE_LIVE`** for real / paper trading       |
| `token`       | auth token (machine-bound)                             |
| `serv_addr`   | optional server endpoint override                      |

All `backtest_*` parameters are unused — gm-executor never runs in backtest mode (and `timer` is not available in backtest).

### `init(context)`

First callback after `run()` connects. Called once. This is where we:

* `set_token(...)` (if not passed via `run`)
* register the polling timer
* stash mutable state on `context` (`context.tick_lock`, `context.timer_id`, ...)

`context` is the SDK-supplied object passed to **every** callback — the conventional place for shared state.

### `stop()`

Stops the strategy. Optional; we generally exit by raising on a halt condition.

## Timer (the heart of our polling loop)

### `timer(timer_func, period, start_delay) -> {"timer_status": int, "timer_id": int}`

Schedules a periodic callback. **Live / simulation only** — not available in backtest mode.

| param           | type | unit | range             | meaning                           |
| --------------- | ---- | ---- | ----------------- | --------------------------------- |
| `timer_func`  | fn   | —   | —                | `def fn(context): ...`          |
| `period`      | int  | ms   | `[1, 43200000]` | interval between fires (max 12 h) |
| `start_delay` | int  | ms   | `[0, 43200000]` | delay before the first fire       |

Returns `{"timer_status": 0, "timer_id": <int>}` on success. Save the `timer_id` if you may want to cancel later.

```Python
def init(context):
    context.timer_id = timer(timer_func=on_timer, period=30_000, start_delay=0)

def on_timer(context):
    ...
```

### `timer_stop(timer_id) -> bool`

Cancels an active timer. Returns `True` on success.

```Python
ret = timer(...)
timer_stop(ret["timer_id"])
```

### `schedule(schedule_func, date_rule, time_rule)` — **not used**

Calendar-based scheduling (`1d`/`1w`/`1m` at `HH:MM:SS`). We use `timer` instead because we want a fixed wall-clock interval independent of market session boundaries.

## Auth

### `set_token(token: str)`

Must be called before any data/order API. Token is bound to the machine that generated it. We read it from the `GM_TOKEN` env var; never put it in the batch document.

## Trading

### `order_volume(symbol, volume, side, order_type, position_effect, price=0, trigger_type=0, stop_price=0, order_duration=OrderDuration_Unknown, order_qualifier=OrderQualifier_Unknown, account='') -> list[Order]`

The only order-placement function gm-executor calls. Diff is computed by us, so all sizing happens here. 11 params total — only the first 6 matter for our flow; the rest cover futures conditionals and multi-account routing.

| param               | type  | meaning                                                                                                                                                                                |
| ------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `symbol`          | str   | `EXCHANGE.CODE`, e.g. `SHSE.600000`. Single symbol per call; bad code → broker rejects with `委托代码不正确`                                                                    |
| `volume`          | int   | share count.**Stocks: ≥100 to buy, ≥1 to sell.** Broker rounds down (`向下取整`); we still send integer lots                                                                 |
| `side`            | int   | `OrderSide_Buy = 1` / `OrderSide_Sell = 2`                                                                                                                                         |
| `order_type`      | int   | `OrderType_Limit = 1` / `OrderType_Market = 2`                                                                                                                                     |
| `position_effect` | int   | A-shares: always `PositionEffect_Open = 1` (broker derives close from side). Futures uses the full matrix — we never hit that path                                                  |
| `price`           | float | limit price when `order_type=Limit`. **For SHSE market orders this is the protection price** and is required (buy: ≤涨停; sell: ≥跌停). 2 decimals for stocks. Default `0` |
| `trigger_type`    | int   | futures conditional trigger kind. Default `0` (non-conditional). **Unused — A-shares**                                                                                        |
| `stop_price`      | float | futures conditional trigger price. Default `0`. **Unused**                                                                                                                     |
| `order_duration`  | int   | TIF (`OrderDuration_*`). Default `OrderDuration_Unknown` — broker treats as standard day order. **Unused**                                                                  |
| `order_qualifier` | int   | exchange-specific qualifier (`OrderQualifier_*`, e.g. IOC/FOK/best-five). Default `OrderQualifier_Unknown`. **Unused**                                                       |
| `account`         | str   | account id / name. Default `''` → SDK's default account. **Unused** (we run single-account)                                                                                   |

Returns a list of `Order` objects, each with at least `cl_ord_id` and an initial `status`. Status updates arrive via the callbacks below.

**Error surface:** unknown enum names raise `NameError`; missing required positional args raise `TypeError`. These fire **before** any broker round-trip, so they surface synchronously at the call site (not via `on_order_status` with `status=Rejected`). Rejection from the broker (insufficient funds, halted symbol, illegal price, ...) still arrives as a `Rejected` status event.

### `order_cancel(wait_cancel_orders)` — **not used in the cycle**

Pass a single `Order` or a list. Available for ad-hoc operator use; gm-executor's submit phase never cancels (it is fire-and-forget).

### Constants we reference

| group              | values                                                                                            |
| ------------------ | ------------------------------------------------------------------------------------------------- |
| `OrderSide`      | `Buy=1`, `Sell=2`                                                                             |
| `OrderType`      | `Limit=1`, `Market=2`                                                                         |
| `PositionEffect` | `Open=1`, `Close=2`, `CloseToday=3`, `CloseYesterday=4` (only `Open` used for A-shares) |
| `PositionSide`   | `Long=1`, `Short=2`                                                                           |

### `OrderStatus` (lifecycle of one `cl_ord_id`)

Imported as `OrderStatus_*` from `gm.api` (canonical defs in `gm/enum.py`). Always use the named constant — never the raw int.

| constant                       | value | zh     | meaning                                            | class    |
| ------------------------------ | ----- | ------ | -------------------------------------------------- | -------- |
| `OrderStatus_PendingNew`     | `10`| 待报   | accepted by SDK, not yet sent to broker            | live     |
| `OrderStatus_New`            | `1` | 已报   | broker accepted; resting on the book               | live     |
| `OrderStatus_PartiallyFilled`| `2` | 部成   | partial fill; remainder still on the book          | live     |
| `OrderStatus_Filled`         | `3` | 已成   | fully filled                                       | terminal |
| `OrderStatus_Canceled`       | `5` | 已撤   | cancelled (broker confirmed)                       | terminal |
| `OrderStatus_Rejected`       | `8` | 已拒绝 | broker refused; see `ord_rej_reason`             | terminal |
| `OrderStatus_Expired`        | `12`| 已过期 | broker timed it out (e.g. session close, GTD past) | terminal |

`OrderStatus_DoneForDay` (`4`), `_PendingCancel` (`6`), `_Stopped` (`7`), `_Suspended` (`9`), `_Calculated` (`11`), `_AcceptedForBidding` (`13`), `_PendingReplace` (`14`) also exist in the SDK but never fire for our A-shares flow. The enum stops at `14` — there is no `PendingTrigger`/`Triggered` constant in this SDK build.

A `cl_ord_id` only ever moves **live → terminal** and never back. Once terminal, no further status events fire for it. A-shares only see `PendingNew → New → (PartiallyFilled* →) Filled` on the happy path.

`OrderRejectReason` gives the cause when `status == Rejected`: insufficient funds (`2`), insufficient position (`3`), illegal price/volume (`6`,`7`), non-trading session (`13`), throttled (`15`), and others. We log it but never treat any specific code specially — rejection is just terminal.

### `ExecType` (execution-report kind)

Imported as `ExecType_*` from `gm.api`. Always use the named constant.

| constant                  | value | zh       | meaning                                                                       |
| ------------------------- | ----- | -------- | ----------------------------------------------------------------------------- |
| `ExecType_Trade`        | `15`| 成交     | a fill chunk (one trade against the order book)                               |
| `ExecType_CancelRejected` | `19`| 撤单被拒 | a `order_cancel(...)` was refused (e.g. already filled / already cancelled) |

We never see other `ExecType` values in practice for our workflow.

## Query

| function                       | returns                                | used in                            |
| ------------------------------ | -------------------------------------- | ---------------------------------- |
| `get_position(symbol, side)` | one position record or `None`        | plan phase                         |
| `get_positions()`            | all current positions                  | plan phase (cached once per cycle) |
| `get_unfinished_orders()`    | currently active (non-terminal) orders | diagnostics only                   |
| `get_orders()`               | all orders for the day                 | diagnostics                        |

## Callbacks (registered, kept tiny)

| callback                                  | when fires                                                          | body                                                      |
| ----------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------- |
| `init(context)`                         | once after `run()` connects                                       | `set_token`, register the timer                         |
| `on_order_status(context, order)`       | every `status` change of an order (incl. PendingNew→New→Filled) | append a `status` event to the batch's `order_record` |
| `on_execution_report(context, execrpt)` | each fill chunk and each cancel-rejection                           | append a `trade` event to the batch's `order_record`  |
| `on_trade_data_disconnected(context)`   | broker channel drops                                                | log only; SDK auto-reconnects                             |
| `on_trade_data_connected(context)`      | broker channel restored                                             | log only                                                  |
| `on_account_status(context, account)`   | account-level connection state changes (login, disconnect, error)   | log + toggle `state.trade_channel_up`                   |
| `on_error(context, code, info)`         | SDK-level error (e.g. trade msg service down)                       | log only                                                  |
| `on_bar` / `on_tick`                  | —                                                                  | not registered                                            |

### `Order` (passed to `on_order_status`)

35 fields total; the ones we look at:

| field                                                       | meaning                                                           |
| ----------------------------------------------------------- | ----------------------------------------------------------------- |
| `cl_ord_id`                                               | 委托客户端 ID — fixed at submit, never changes; our key          |
| `order_id`                                                | 委托柜台 ID — broker's order id; useful for ops cross-reference  |
| `ex_ord_id`                                               | 委托交易所 ID — exchange's order id                              |
| `symbol`, `side`, `order_type`, `price`, `volume` | the original submit parameters                                    |
| `status`                                                  | numeric `OrderStatus` (the field that changed and triggered us) |
| `filled_volume`                                           | running total filled — broker truth, monotonic                   |
| `filled_vwap`                                             | volume-weighted average fill price                                |
| `filled_amount`                                           | running total filled amount (yuan)                                |
| `filled_commission`                                       | running total commission paid                                     |
| `ord_rej_reason`                                          | numeric `OrderRejectReason`, only set when `status == 8`      |
| `ord_rej_reason_detail`                                   | human-readable rejection description                              |
| `created_at`, `updated_at`                              | broker-side timestamps (`datetime.datetime`)                    |

We never read `target_volume`/`target_value`/`target_percent`/`order_business`/`order_style`/etc. — those are SDK convenience for percent/value-based ordering, which we don't use.

### `ExecRpt` (passed to `on_execution_report`)

18 fields total. Distinguish the two `exec_type` variants — only fills (`15`) carry `price`/`volume`/`amount`; only rejections (`19`) carry meaningful `ord_rej_reason`/`ord_rej_reason_detail`.

| field                                                         | meaning                                                        |
| ------------------------------------------------------------- | -------------------------------------------------------------- |
| `cl_ord_id`                                                 | links back to the submit                                       |
| `order_id`                                                  | broker's 柜台委托 ID                                           |
| `exec_id`                                                   | 交易所成交 ID — unique per execution; idempotency key         |
| `exec_type`                                                 | `15` = `Trade` (fill chunk), `19` = `CancelRejected`   |
| `symbol`, `side`, `position_effect`, `order_business` | from the original order                                        |
| `price`                                                     | execution price (fills only)                                   |
| `volume`                                                    | shares in this chunk (fills only)                              |
| `amount`                                                    | yuan in this chunk =`price * volume * lot_size` (fills only) |
| `cost`                                                      | futures-only, ignored for A-shares                             |
| `ord_rej_reason`                                            | numeric `OrderRejectReason` (cancel-rejection only)          |
| `ord_rej_reason_detail`                                     | human-readable description (cancel-rejection only)             |
| `created_at`                                                | broker-side event timestamp (`datetime.datetime`)            |

Commission is **not** on `ExecRpt` — only the cumulative `filled_commission` on `Order` carries that. If you need per-fill commission, diff successive `status` lines.

### `AccountStatus` (passed to `on_account_status`)

`account.status` is **not** an int — it's a `DictLikeConnectionStatus` (a `dict` subclass with attribute access). Reading `int(account.status)` raises `TypeError`. Drill in via `.state`:

| field                  | meaning                                                                          |
| ---------------------- | -------------------------------------------------------------------------------- |
| `account.account_id` | account id                                                                       |
| `account.status.state` | int connection-state code (see `ConnectionStatus.State` enum below)          |
| `account.status.error` | `DictLikeError` (also a `dict` subclass) — populated only when `state == 6` |
| `account.status.error.code` | int error code                                                               |
| `account.status.error.type` | error type string                                                            |
| `account.status.error.info` | human-readable description                                                   |

Connection-state constants are exported from `gm.api` (canonical defs in `gm/enum.py`). Always import the named constants — never hard-code the integer:

| constant                | value | meaning                                          |
| ----------------------- | ----- | ------------------------------------------------ |
| `State_CONNECTING`    | `1` | TCP handshake in progress                        |
| `State_CONNECTED`     | `2` | socket up, login not yet acknowledged            |
| `State_LOGGEDIN`      | `3` | **ready to trade** — toggles `trade_channel_up` on |
| `State_DISCONNECTING` | `4` | graceful teardown                                |
| `State_DISCONNECTED`  | `5` | socket down                                      |
| `State_ERROR`         | `6` | error state — `error.{code,type,info}` populated |

We `set()` `trade_channel_up` on `State_LOGGEDIN`; `State_DISCONNECTING / State_DISCONNECTED / State_ERROR` clear it. `State_CONNECTING` and `State_CONNECTED` are intermediate — neither sets nor clears.

**Rule:** no callback ever calls `run_cycle` or any business logic. Their **only** side effect is one append to the appropriate `order_record`. See [FLOW.md → Log writing](./FLOW.md#log-writing) for the safety contract.

## Modes

| mode              | timer available? | what we use                            |
| ----------------- | ---------------- | -------------------------------------- |
| `MODE_LIVE`     | yes              | **gm-executor always runs here** |
| `MODE_BACKTEST` | no               | unused —`timer()` would fail        |

## Pitfalls / things to remember

* `timer` `period` is **milliseconds**, not seconds.
* `timer` returns a dict; `timer_id` is the field, not the dict itself.
* The same `context` instance is passed to every callback — don't recreate it.
* `set_token` must precede any other API call.
* Backtest mode silently disables `timer`; do not let `mode` be configurable.
