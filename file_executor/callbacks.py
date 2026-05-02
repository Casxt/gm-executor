"""SDK callback handlers. Each one is tiny, never raises, never blocks.

Two rules from FLOW.md:
1. Append-only side effect: a callback either appends one log line or toggles a flag.
2. Never raise: an uncaught exception inside a callback can take down the SDK's
   dispatch loop. Every body is wrapped in `try / except Exception: log.exception(...)`.
"""

import logging

from gm.api import (
    ExecType_CancelRejected,
    ExecType_Trade,
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

        if exec_type == ExecType_Trade:                      # fill chunk
            side = int(execrpt.side)
            ev["side"]      = side
            ev["side_text"] = "buy" if side == OrderSide_Buy else "sell"
            ev["volume"]    = int(execrpt.volume)
            ev["price"]     = float(execrpt.price)
            ev["amount"]    = float(execrpt.amount)
        elif exec_type == ExecType_CancelRejected:
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
