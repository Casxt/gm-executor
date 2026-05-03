"""run_cycle and helpers — pass-1 cleanup, pass-2 reconcile, matched check.

Stateless across runs: every cycle reads inputs fresh from broker + filesystem,
computes a diff, acts, returns. An exception inside is just lost work; the next
cycle reconciles whatever is now true.
"""

import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from gm.api import (
    OrderSide_Buy,
    OrderSide_Sell,
    OrderType_Limit,
    OrderType_Market,
    PositionEffect_Close,
    PositionEffect_Open,
    PositionSide_Long,
    get_position,
    get_unfinished_orders,
    order_cancel,
    order_volume,
)

from . import config, order_log, state
from .models import BatchDoc
from .schema import parse_and_validate

log = logging.getLogger(__name__)


# ── small helpers ─────────────────────────────────────────────────────

def unix_now() -> int:
    return int(time.time())


def unix_now_ms() -> int:
    return int(time.time() * 1000)


@contextmanager
def timed(name: str) -> Iterator[dict[str, Any]]:
    """Log `gm_api: <name> took=<ms> [field=value ...]` on exit.

    Mutate the yielded dict to attach per-call fields (e.g. result count).
    """
    fields: dict[str, Any] = {}
    t0 = time.perf_counter()
    try:
        yield fields
    finally:
        ms = (time.perf_counter() - t0) * 1000
        extras = " " + " ".join(f"{k}={v}" for k, v in fields.items()) if fields else ""
        log.info("gm_api: %s took=%.1fms%s", name, ms, extras)


# ── cycle entry point ─────────────────────────────────────────────────

def run_cycle() -> None:
    if not state.trade_channel_up.is_set():
        log.warning("skipping cycle: trade channel down")
        return

    # Skip while within CYCLE_GRACE_SECONDS of last reconnect (let replay settle)
    # or last cycle end (let drain/connector breathe). 0.0 ⇒ no event yet.
    if state.trade_channel_up_at > 0:
        elapsed = time.time() - state.trade_channel_up_at
        if elapsed < config.CYCLE_GRACE_SECONDS:
            log.info("skipping cycle: reconnect grace (%.1fs of %ds)",
                     elapsed, config.CYCLE_GRACE_SECONDS)
            return
    if state.last_cycle_end_at > 0:
        elapsed = time.time() - state.last_cycle_end_at
        if elapsed < config.CYCLE_GRACE_SECONDS:
            log.info("skipping cycle: min gap (%.1fs of %ds)",
                     elapsed, config.CYCLE_GRACE_SECONDS)
            return

    # Held for the whole cycle so connector mirrors and callback-driven path
    # lookups can't interleave with reconcile. Reentrant — order_log helpers
    # re-acquire freely.
    with timed("acquire_lock"):
        state.batch_state_lock.acquire()
    try:
        now = unix_now()
        positions, unfinished = _broker_snapshot()

        with timed("pass_one") as f:
            seen, active = _pass_one(now, unfinished)
            f["seen"] = len(seen); f["active"] = len(active)
        if _has_overlap(seen):
            return                                      # invariant broken; operator must intervene
        if not active:
            log.info("idle: no active batch")
            return

        doc = active[0]
        with timed("reconcile") as f:
            f["batch"] = doc.batch_id; f["orders"] = len(doc.orders)
            _reconcile(doc, positions, unfinished)
        with timed("matched_check") as f:
            f["batch"] = doc.batch_id
            done = _matched(doc, positions, unfinished)
        if done:
            log.info("matched: %s -> finished/", doc.batch_id)
            order_log.move_pair(doc.batch_id, config.FINISHED_DIR)
    finally:
        state.batch_state_lock.release()
        state.last_cycle_end_at = time.time()


# ── pass 1: clean pending/ ────────────────────────────────────────────

def _pass_one(now: int, unfinished: dict[str, list]) -> tuple[list[BatchDoc], list[BatchDoc]]:
    seen:   list[BatchDoc] = []
    active: list[BatchDoc] = []
    with timed("pass_one_glob") as f:
        paths = sorted(config.PENDING_DIR.glob("*.json"))
        f["n"] = len(paths)
    for path in paths:
        try:
            doc = parse_and_validate(path)
        except Exception:
            log.exception("invalid batch %s -> failed/", path.name)
            try:
                order_log.move_invalid(path)
            except Exception:
                log.exception("failed to move invalid batch %s", path.name)
            continue

        if now > doc.expires_at:
            log.info("expired: %s -> expired/", doc.batch_id)
            _cancel_alive(doc.batch_id, unfinished)
            order_log.move_pair(doc.batch_id, config.EXPIRED_DIR)
            continue

        seen.append(doc)
        if now >= doc.valid_at:
            active.append(doc)
        else:
            log.info("scheduled: %s valid_at=%d now=%d", doc.batch_id, doc.valid_at, now)

    return seen, active


def _has_overlap(seen: list[BatchDoc]) -> bool:
    """True iff any two pending batches' [valid_at, expires_at] windows overlap.

    After sorting by valid_at, adjacent-pair check suffices (if a doesn't overlap
    its successor, it can't overlap any later one).
    """
    seen.sort(key=lambda d: d.valid_at)
    for a, b in zip(seen, seen[1:]):
        if a.expires_at >= b.valid_at:
            log.error("overlap: %s <-> %s; operator must intervene",
                      a.batch_id, b.batch_id)
            return True
    return False


# ── pass 2: reconcile ─────────────────────────────────────────────────

def _reconcile(doc: BatchDoc, positions: dict, unfinished: dict[str, list]) -> None:
    with timed("own_cl_ord_ids") as f:
        own_cl_ord_ids = _own_cl_ord_ids(doc.batch_id)
        f["batch"] = doc.batch_id; f["n"] = len(own_cl_ord_ids)
    long_side = int(PositionSide_Long)

    # One open fd for all submit-line appends in this cycle. Lazy: opens on
    # first write, so a no-op cycle pays zero file-IO. Saves ~1010ms × N
    # bucket waits on the throttled cloud disk vs. open/close-per-append.
    with order_log.cycle_session(doc.batch_id) as session:
        for order in doc.orders:
            try:
                with timed("reconcile_order") as f:
                    f["symbol"] = order.symbol
                    sym_unfinished = unfinished.get(order.symbol, [])
                    if sym_unfinished:
                        _handle_unfinished(order.symbol, sym_unfinished, own_cl_ord_ids)
                        continue

                    held = positions.get((order.symbol, long_side), 0)
                    diff = order.target - held
                    if diff == 0:
                        continue

                    _submit(session, doc.batch_id, order.id, order.symbol, diff,
                            order.order_type, order.price)
            except Exception:
                log.exception("reconcile failed for %s/%s; will retry next cycle",
                              doc.batch_id, order.id)


def _handle_unfinished(symbol: str, sym_unfinished: list, own_cl_ord_ids: set[str]) -> None:
    foreign = {o.cl_ord_id for o in sym_unfinished} - own_cl_ord_ids
    if foreign:
        log.error("foreign order on %s; skipping (cl_ord_ids=%s)", symbol, sorted(foreign))
        return

    log.info("waiting on own orders for %s (%d unfinished)", symbol, len(sym_unfinished))
    now = unix_now()
    for o in sym_unfinished:
        created_at = getattr(o, "created_at", None)
        if created_at is None:
            continue
        try:
            age = now - int(created_at.timestamp())
        except Exception:
            continue
        if age > config.STUCK_ORDER_SECONDS:
            log.error("stuck order %s on %s for %ds", o.cl_ord_id, symbol, age)


def _submit(session: "order_log._Session",
            batch_id: str, order_id: str, symbol: str, diff: int,
            order_type_str: str, price: float | None) -> None:
    buy             = diff > 0
    side            = OrderSide_Buy        if buy else OrderSide_Sell
    side_text       = "buy"                if buy else "sell"
    pos_effect      = PositionEffect_Open  if buy else PositionEffect_Close   # A-shares: sell-with-Open is interpreted as shorting and rejected ("A股不允许做空")
    pos_effect_text = "open"               if buy else "close"
    order_type = OrderType_Limit if order_type_str == "limit" else OrderType_Market
    submit_price = float(price) if order_type_str == "limit" and price is not None else 0.0
    volume = abs(diff)

    log.info("submit: batch=%s symbol=%s vol=%d side=%s type=%s price=%s",
             batch_id, symbol, volume, side_text, order_type_str, submit_price)

    with timed("order_volume") as f:
        results = order_volume(
            symbol=symbol, volume=volume,
            side=side, order_type=order_type,
            position_effect=pos_effect, price=submit_price,
        ) or []
        f["returned"] = len(results)
        f["batch"]    = batch_id
        f["order"]    = order_id

    if not results:
        log.error("order_volume returned no orders for batch=%s order=%s symbol=%s — "
                  "broker may still place the order, but we cannot track it",
                  batch_id, order_id, symbol)
        return

    for r in results:
        cl_ord_id = getattr(r, "cl_ord_id", None)
        if not cl_ord_id:
            log.error("order_volume returned a result without cl_ord_id: %r", r)
            continue
        order_log.clord_index[cl_ord_id] = batch_id
        log.info("  registering cl_ord_id=%s for batch=%s order=%s",
                 cl_ord_id, batch_id, order_id)
        session.append({
            "ts_ms":           unix_now_ms(),
            "event":           "submit",
            "order_id":        order_id,
            "symbol":          symbol,
            "side":            side_text,
            "position_effect": pos_effect_text,
            "volume":          volume,
            "order_type":      order_type_str,
            "price":           submit_price if order_type_str == "limit" else 0,
            "cl_ord_id":       cl_ord_id,
        })


# ── matched ───────────────────────────────────────────────────────────

def _matched(doc: BatchDoc, positions: dict, unfinished: dict[str, list]) -> bool:
    long_side = int(PositionSide_Long)
    own_cl_ord_ids = _own_cl_ord_ids(doc.batch_id)
    for order in doc.orders:
        if positions.get((order.symbol, long_side), 0) != order.target:
            return False
        for o in unfinished.get(order.symbol, []):
            if o.cl_ord_id in own_cl_ord_ids:
                return False
    return True


# ── cancel-on-expiry ──────────────────────────────────────────────────

def _cancel_alive(batch_id: str, unfinished: dict[str, list]) -> None:
    """Cancel still-open cl_ord_ids of `batch_id` and log one `cancel` event each."""
    ours = [
        o for orders in unfinished.values() for o in orders
        if order_log.clord_index.get(o.cl_ord_id) == batch_id
    ]
    if not ours:
        return

    with timed("order_cancel") as f:
        f["n"]     = len(ours)
        f["batch"] = batch_id
        try:
            order_cancel(ours)
        except Exception:
            log.exception("order_cancel failed for batch %s", batch_id)

    # One fd, one open + one close, regardless of how many cancel lines.
    with order_log.cycle_session(batch_id) as session:
        for o in ours:
            session.append({
                "ts_ms":     unix_now_ms(),
                "event":     "cancel",
                "cl_ord_id": o.cl_ord_id,
            })


# ── shared bits ───────────────────────────────────────────────────────

def _broker_snapshot() -> tuple[dict[tuple[str, int], int], dict[str, list]]:
    with timed("get_position") as f:
        raw_positions = get_position() or []
        f["n"] = len(raw_positions)

    positions: dict[tuple[str, int], int] = {}
    for p in raw_positions:
        positions[(p.symbol, int(p.side))] = int(p.volume)

    with timed("get_unfinished_orders") as f:
        raw_unfinished = get_unfinished_orders() or []
        f["n"] = len(raw_unfinished)

    unfinished: dict[str, list] = defaultdict(list)
    for o in raw_unfinished:
        unfinished[o.symbol].append(o)
    return positions, unfinished


def _own_cl_ord_ids(batch_id: str) -> set[str]:
    return {
        ev["cl_ord_id"] for ev in order_log.replay_record(batch_id)
        if ev.get("event") == "submit" and isinstance(ev.get("cl_ord_id"), str)
    }
