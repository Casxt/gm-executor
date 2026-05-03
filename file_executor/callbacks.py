"""SDK callback handlers. Each one is tiny, never raises, never blocks.

Two rules from FLOW.md:
1. Append-only side effect: a callback either appends one log line or toggles a flag.
2. Never raise: an uncaught exception inside a callback can take down the SDK's
   dispatch loop. Every body is wrapped in `try / except Exception: log.exception(...)`.

`on_order_status` and `on_execution_report` use a producer/consumer split: the SDK
dispatch thread snapshots the event into a `queue.SimpleQueue` and returns immediately.
A daemon thread (`callback-processor`) drains the queue and does the real work — index
lookup + log append — under `batch_state_lock`. This keeps the SDK dispatch thread off
our locks entirely, and lets events delivered during a cycle wait safely in the queue
until the cycle releases.
"""

import logging
import queue
import threading
import time

from gm.api import (
    ExecType_CancelRejected,
    ExecType_Trade,
    OrderRejectReason_AccountDisabled,
    OrderRejectReason_AccountDisconnected,
    OrderRejectReason_AccountLoggedout,
    OrderRejectReason_IllegalAccountId,
    OrderRejectReason_IllegalPrice,
    OrderRejectReason_IllegalStrategyId,
    OrderRejectReason_IllegalSymbol,
    OrderRejectReason_IllegalVolume,
    OrderRejectReason_Internal,
    OrderRejectReason_NoEnoughCash,
    OrderRejectReason_NoEnoughPosition,
    OrderRejectReason_NotInTradingSession,
    OrderRejectReason_OrderTypeNotSupported,
    OrderRejectReason_RiskRuleCheckFailed,
    OrderRejectReason_SymbolSusppended,
    OrderRejectReason_Throttle,
    OrderRejectReason_Unknown,
    OrderSide_Buy,
    OrderStatus_Canceled,
    OrderStatus_Expired,
    OrderStatus_Filled,
    OrderStatus_New,
    OrderStatus_PartiallyFilled,
    OrderStatus_PendingNew,
    OrderStatus_Rejected,
    State_CONNECTED,
    State_CONNECTING,
    State_DISCONNECTED,
    State_DISCONNECTING,
    State_ERROR,
    State_LOGGEDIN,
)

from . import order_log, state
from .cycle import unix_now_ms

log = logging.getLogger(__name__)


# ── timer ─────────────────────────────────────────────────────────────

def on_timer(context) -> None:
    state.cycle_pending.set()                                # one signal, no work


# ── trading events ────────────────────────────────────────────────────

_event_queue: "queue.SimpleQueue[dict | None]" = queue.SimpleQueue()
"""SDK → drain pipe. SDK dispatch thread puts; `_drain_loop` thread takes. Sentinel `None` ⇒ stop."""


def on_order_status(context, order) -> None:
    try:
        # Snapshot every field we'll need: the SDK may reuse / free `order` once we return.
        cl_ord_id = order.cl_ord_id
        symbol    = getattr(order, "symbol", "") or ""
        status    = int(order.status)
        log.debug("recv status: cl_ord_id=%s symbol=%s status=%d (%s)",
                  cl_ord_id, symbol, status, _STATUS_TEXTS.get(status, f"Status{status}"))
        _event_queue.put({
            "kind":                  "status",
            "ts_ms":                 unix_now_ms(),
            "cl_ord_id":             cl_ord_id,
            "symbol":                symbol,
            "status":                status,
            "filled_volume":         int(order.filled_volume or 0),
            "ord_rej_reason":        int(getattr(order, "ord_rej_reason", 0) or 0),
            "ord_rej_reason_detail": getattr(order, "ord_rej_reason_detail", "") or "",
        })
    except Exception:
        log.exception("on_order_status enqueue failed")


def on_execution_report(context, execrpt) -> None:
    try:
        cl_ord_id = execrpt.cl_ord_id
        symbol    = getattr(execrpt, "symbol", "") or ""
        exec_type = int(execrpt.exec_type)
        log.debug("recv execrpt: cl_ord_id=%s symbol=%s exec_type=%d (%s)",
                  cl_ord_id, symbol, exec_type, _exec_type_text(exec_type))
        snap: dict = {
            "kind":            "execrpt",
            "ts_ms":           unix_now_ms(),
            "cl_ord_id":       cl_ord_id,
            "symbol":          symbol,
            "exec_type":       exec_type,
            "broker_order_id": getattr(execrpt, "order_id", "") or "",
            "exec_id":         getattr(execrpt, "exec_id", "") or "",
            "broker_ts_ms":    _broker_ts_ms(execrpt),
        }
        if exec_type == ExecType_Trade:
            snap["side"]   = int(execrpt.side)
            snap["volume"] = int(execrpt.volume)
            snap["price"]  = float(execrpt.price)
            snap["amount"] = float(execrpt.amount)
        elif exec_type == ExecType_CancelRejected:
            snap["ord_rej_reason"]        = int(getattr(execrpt, "ord_rej_reason", 0) or 0)
            snap["ord_rej_reason_detail"] = getattr(execrpt, "ord_rej_reason_detail", "") or ""
        _event_queue.put(snap)
    except Exception:
        log.exception("on_execution_report enqueue failed")


# ── drain worker ──────────────────────────────────────────────────────
#
# Each loop iteration: block on the queue for one event, then non-blocking-drain
# everything else queued, then process that whole batch in one pass under one
# `batch_state_lock` acquisition. Events are grouped by batch_id and written
# through one `drain_session` per batch — one open + one close per batch
# regardless of how many lines.
#
# Why coalesce: on the throttled cloud disk, every kernel metadata op pays a
# ~1010ms refill-bucket wait (2026-05-03 measurement). Per-event open+close
# cost ~2 buckets each; the daily reconnect replay (~50–100 events) used to
# spend minutes draining. Coalescing collapses N events for one batch into 2
# bucket waits. Typical drain pass has 1 batch.


def _drain_loop() -> None:
    stop = False
    while not stop:
        try:
            evt = _event_queue.get()                             # blocks for at least one event
        except Exception:
            log.exception("event queue read failed")
            continue
        if evt is None:                                          # sentinel from stop()
            return

        events = [evt]
        # Pull anything else already queued without waiting. Caps the burst
        # cost at "however much arrived during the previous pass".
        while True:
            try:
                e = _event_queue.get_nowait()
            except queue.Empty:
                break
            if e is None:                                        # process this pass, then exit
                stop = True
                break
            events.append(e)

        try:
            _process_drained(events)
        except Exception:
            log.exception("drain pass failed: n=%d", len(events))


def _process_drained(events: list[dict]) -> None:
    """Group N events by batch_id (resolved via clord_index), write each group
    through one `drain_session`. Foreign / orphaned events are warned + dropped.
    """
    with state.batch_state_lock:
        by_batch: dict[str, list[dict]] = {}
        for evt in events:
            try:
                pending_ms = unix_now_ms() - evt["ts_ms"]
                log.info("process %s: cl_ord_id=%s symbol=%s pending=%dms",
                         evt["kind"], evt["cl_ord_id"], evt.get("symbol", ""), pending_ms)
                resolved = _build_line(evt)
                if resolved is None:
                    continue
                batch_id, payload = resolved
                by_batch.setdefault(batch_id, []).append(payload)
            except Exception:
                log.exception("build event failed: %r", evt)

        for batch_id, lines in by_batch.items():
            try:
                with order_log.drain_session(batch_id) as session:
                    if session is None:
                        for ln in lines:
                            log.warning("no record for batch_id=%s; dropping event=%s",
                                        batch_id, ln.get("event"))
                        continue
                    for ln in lines:
                        session.append(ln)
            except Exception:
                log.exception("drain write failed: batch=%s n=%d", batch_id, len(lines))


def _build_line(evt: dict) -> "tuple[str, dict] | None":
    """Resolve cl_ord_id → batch_id and shape the on-disk JSONL payload. Caller
    holds `batch_state_lock`. Returns None for foreign cl_ord_ids (warned)."""
    if evt["kind"] == "status":
        return _build_status_line(evt)
    return _build_execrpt_line(evt)


def _build_status_line(evt: dict) -> "tuple[str, dict] | None":
    cl_ord_id   = evt["cl_ord_id"]
    status      = evt["status"]
    status_text = _STATUS_TEXTS.get(status, f"Status{status}")
    symbol      = evt["symbol"]

    rej_fields: dict = {}
    if status == OrderStatus_Rejected:
        reason = evt["ord_rej_reason"]
        rej_fields["ord_rej_reason"]        = reason
        rej_fields["ord_rej_reason_text"]   = _REJ_REASON_TEXTS.get(reason, f"Reason{reason}")
        rej_fields["ord_rej_reason_detail"] = evt["ord_rej_reason_detail"]

    batch_id = order_log.clord_index.get(cl_ord_id)
    if batch_id is None:
        extra = (f" reason={rej_fields['ord_rej_reason']} ({rej_fields['ord_rej_reason_text']}) "
                 f"detail={rej_fields['ord_rej_reason_detail']!r}") if rej_fields else ""
        log.warning("status for foreign cl_ord_id=%s symbol=%s status=%d (%s)%s; dropping",
                    cl_ord_id, symbol, status, status_text, extra)
        return None

    return batch_id, {
        "ts_ms":         evt["ts_ms"],
        "event":         "status",
        "cl_ord_id":     cl_ord_id,
        "symbol":        symbol,
        "status":        status,
        "status_text":   status_text,
        "filled_volume": evt["filled_volume"],
        **rej_fields,
    }


def _build_execrpt_line(evt: dict) -> "tuple[str, dict] | None":
    cl_ord_id = evt["cl_ord_id"]
    exec_type = evt["exec_type"]
    symbol    = evt["symbol"]

    batch_id = order_log.clord_index.get(cl_ord_id)
    if batch_id is None:
        log.warning("execrpt for foreign cl_ord_id=%s symbol=%s exec_type=%d (%s); dropping",
                    cl_ord_id, symbol, exec_type, _exec_type_text(exec_type))
        return None

    ev: dict = {
        "ts_ms":           evt["ts_ms"],
        "event":           "trade",
        "cl_ord_id":       cl_ord_id,
        "broker_order_id": evt["broker_order_id"],
        "exec_id":         evt["exec_id"],
        "exec_type":       exec_type,
        "exec_type_text":  _exec_type_text(exec_type),
        "symbol":          symbol,
        "broker_ts_ms":    evt["broker_ts_ms"],
    }
    if exec_type == ExecType_Trade:
        side = evt["side"]
        ev["side"]      = side
        ev["side_text"] = "buy" if side == OrderSide_Buy else "sell"
        ev["volume"]    = evt["volume"]
        ev["price"]     = evt["price"]
        ev["amount"]    = evt["amount"]
    elif exec_type == ExecType_CancelRejected:
        reason = evt["ord_rej_reason"]
        ev["ord_rej_reason"]        = reason
        ev["ord_rej_reason_text"]   = _REJ_REASON_TEXTS.get(reason, f"Reason{reason}")
        ev["ord_rej_reason_detail"] = evt["ord_rej_reason_detail"]
    return batch_id, ev


def start() -> threading.Thread:
    t = threading.Thread(target=_drain_loop, name="callback-processor", daemon=True)
    t.start()
    state.worker_threads.append(t)
    return t


def stop() -> None:
    """Wake the drain loop with a sentinel so it can exit cleanly."""
    _event_queue.put(None)


# ── connection events ─────────────────────────────────────────────────

def on_trade_data_connected(context) -> None:
    try:
        log.info("trade channel up")
        state.trade_channel_up.set()
        state.trade_channel_up_at = time.time()                  # arms reconnect grace
    except Exception:
        log.exception("on_trade_data_connected failed")


def on_trade_data_disconnected(context) -> None:
    try:
        log.warning("trade channel down")
        state.trade_channel_up.clear()
    except Exception:
        log.exception("on_trade_data_disconnected failed")


def on_market_data_connected(context) -> None:
    log.info("market channel up (unused)")


def on_market_data_disconnected(context) -> None:
    log.info("market channel down (unused)")


def on_account_status(context, account) -> None:
    try:
        # account.status is a DictLikeConnectionStatus (dict subclass) — the int
        # connection-state code lives on its `.state` attribute. See GM_SDK.md.
        conn = getattr(account, "status", None)
        state_code = int(getattr(conn, "state", 0) or 0)
        err = getattr(conn, "error", None) if conn else None
        err_code = int(getattr(err, "code", 0) or 0) if err else 0

        state_text = _CONN_STATE_TEXTS.get(state_code, f"State{state_code}")
        if err_code:
            log.warning("account status: id=%s state=%d (%s) err_code=%d info=%s",
                        getattr(account, "account_id", "?"), state_code, state_text,
                        err_code, getattr(err, "info", "") or "")
        else:
            log.info("account status: id=%s state=%d (%s)",
                     getattr(account, "account_id", "?"), state_code, state_text)

        # State_LOGGEDIN is the "ready to trade" signal; anything past it is
        # teardown or failure. Constants from gm.api (see gm/enum.py).
        if state_code in (State_DISCONNECTING, State_DISCONNECTED, State_ERROR):
            state.trade_channel_up.clear()
        elif state_code == State_LOGGEDIN:
            state.trade_channel_up.set()
    except Exception:
        log.exception("on_account_status failed")


def on_error(context, code, info) -> None:
    try:
        log.error("gm SDK error: code=%s info=%s", code, info)
    except Exception:
        log.exception("on_error failed")


# ── lookups ───────────────────────────────────────────────────────────

_CONN_STATE_TEXTS: dict[int, str] = {
    State_CONNECTING:    "Connecting",
    State_CONNECTED:     "Connected",
    State_LOGGEDIN:      "LoggedIn",
    State_DISCONNECTING: "Disconnecting",
    State_DISCONNECTED:  "Disconnected",
    State_ERROR:         "Error",
}


_STATUS_TEXTS: dict[int, str] = {
    OrderStatus_New:             "New",
    OrderStatus_PartiallyFilled: "PartiallyFilled",
    OrderStatus_Filled:          "Filled",
    OrderStatus_Canceled:        "Canceled",
    OrderStatus_Rejected:        "Rejected",
    OrderStatus_PendingNew:      "PendingNew",
    OrderStatus_Expired:         "Expired",
}


_REJ_REASON_TEXTS: dict[int, str] = {
    OrderRejectReason_Unknown:               "Unknown",
    OrderRejectReason_RiskRuleCheckFailed:   "RiskRuleCheckFailed",
    OrderRejectReason_NoEnoughCash:          "NoEnoughCash",
    OrderRejectReason_NoEnoughPosition:      "NoEnoughPosition",
    OrderRejectReason_IllegalAccountId:      "IllegalAccountId",
    OrderRejectReason_IllegalStrategyId:     "IllegalStrategyId",
    OrderRejectReason_IllegalSymbol:         "IllegalSymbol",
    OrderRejectReason_IllegalVolume:         "IllegalVolume",
    OrderRejectReason_IllegalPrice:          "IllegalPrice",
    OrderRejectReason_AccountDisabled:       "AccountDisabled",
    OrderRejectReason_AccountDisconnected:   "AccountDisconnected",
    OrderRejectReason_AccountLoggedout:      "AccountLoggedout",
    OrderRejectReason_NotInTradingSession:   "NotInTradingSession",
    OrderRejectReason_OrderTypeNotSupported: "OrderTypeNotSupported",
    OrderRejectReason_Throttle:              "Throttle",
    OrderRejectReason_SymbolSusppended:      "SymbolSuspended",
    OrderRejectReason_Internal:              "Internal",
}


def _exec_type_text(exec_type: int) -> str:
    if exec_type == ExecType_Trade:
        return "Trade"
    if exec_type == ExecType_CancelRejected:
        return "CancelRejected"
    return f"Exec{exec_type}"


def _broker_ts_ms(execrpt) -> int:
    created = getattr(execrpt, "created_at", None)
    if created is None:
        return 0
    try:
        return int(created.timestamp() * 1000)
    except Exception:
        return 0
