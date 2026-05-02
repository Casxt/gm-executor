"""SDK callback handlers. Each one is tiny, never raises, never blocks.

Two rules from FLOW.md:
1. Append-only side effect: a callback either appends one log line or toggles a flag.
2. Never raise: an uncaught exception inside a callback can take down the SDK's
   dispatch loop. Every body is wrapped in `try / except Exception: log.exception(...)`.
"""

import logging

from . import order_log, state
from .cycle import unix_now_ms

log = logging.getLogger(__name__)


# ── timer ─────────────────────────────────────────────────────────────

def on_timer(context) -> None:
    state.cycle_pending.set()                                # one signal, no work


# ── trading events ────────────────────────────────────────────────────

def on_order_status(context, order) -> None:
    try:
        cl_ord_id = order.cl_ord_id
        batch_id  = order_log.clord_index.get(cl_ord_id)
        if batch_id is None:
            log.warning("status for unknown cl_ord_id=%s; dropping", cl_ord_id)
            return
        status = int(order.status)
        order_log.append(batch_id, {
            "ts_ms":          unix_now_ms(),
            "event":          "status",
            "cl_ord_id":      cl_ord_id,
            "status":         status,
            "status_text":    _STATUS_TEXTS.get(status, f"Status{status}"),
            "filled_volume":  int(order.filled_volume or 0),
            "ord_rej_reason": int(getattr(order, "ord_rej_reason", 0) or 0),
        })
    except Exception:
        log.exception("on_order_status failed")


def on_execution_report(context, execrpt) -> None:
    try:
        cl_ord_id = execrpt.cl_ord_id
        batch_id  = order_log.clord_index.get(cl_ord_id)
        if batch_id is None:
            log.warning("execrpt for unknown cl_ord_id=%s; dropping", cl_ord_id)
            return

        exec_type = int(execrpt.exec_type)
        ev: dict = {
            "ts_ms":           unix_now_ms(),
            "event":           "trade",
            "cl_ord_id":       cl_ord_id,
            "broker_order_id": getattr(execrpt, "order_id", "") or "",
            "exec_id":         getattr(execrpt, "exec_id", "") or "",
            "exec_type":       exec_type,
            "exec_type_text":  _exec_type_text(exec_type),
            "symbol":          execrpt.symbol,
            "broker_ts_ms":    _broker_ts_ms(execrpt),
        }

        if exec_type == 15:                                  # fill chunk
            side = int(execrpt.side)
            ev["side"]      = side
            ev["side_text"] = "buy" if side == 1 else "sell"
            ev["volume"]    = int(execrpt.volume)
            ev["price"]     = float(execrpt.price)
            ev["amount"]    = float(execrpt.amount)
        elif exec_type == 19:                                # cancel-rejected
            ev["ord_rej_reason"]        = int(getattr(execrpt, "ord_rej_reason", 0) or 0)
            ev["ord_rej_reason_detail"] = getattr(execrpt, "ord_rej_reason_detail", "") or ""

        order_log.append(batch_id, ev)
    except Exception:
        log.exception("on_execution_report failed")


# ── connection events ─────────────────────────────────────────────────

def on_trade_data_connected(context) -> None:
    try:
        log.info("trade channel up")
        state.trade_channel_up.set()
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
        status = int(getattr(account, "status", 0) or 0)
        log.info("account status: id=%s status=%d",
                 getattr(account, "account_id", "?"), status)
        # Per myquant docs: 1=connected, 2=logged-in, 3=disconnected, 4=error.
        if status in (3, 4):
            state.trade_channel_up.clear()
        elif status == 2:
            state.trade_channel_up.set()
    except Exception:
        log.exception("on_account_status failed")


def on_error(context, code, info) -> None:
    try:
        log.error("gm SDK error: code=%s info=%s", code, info)
    except Exception:
        log.exception("on_error failed")


# ── lookups ───────────────────────────────────────────────────────────

_STATUS_TEXTS: dict[int, str] = {
    1:  "New",
    2:  "PartiallyFilled",
    3:  "Filled",
    5:  "Canceled",
    8:  "Rejected",
    10: "PendingNew",
    12: "Expired",
    15: "PendingTrigger",
    16: "Triggered",
}


def _exec_type_text(exec_type: int) -> str:
    if exec_type == 15:
        return "Trade"
    if exec_type == 19:
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
