"""Batch close report — built when a batch reaches a terminal cycle (matched/expired).

Pure, fast, and side-effect-free except for the final `notify()` call:

* It runs inside the cycle worker while `batch_state_lock` is held, so it must NOT
  issue a fresh `get_position()` — that would be slow on the throttled cloud disk and,
  worse, could observe positions already moved by a later batch. Instead it reuses the
  `positions` / `unfinished` snapshot the cycle already took at its start. That snapshot
  IS the batch's terminal truth: the overlap invariant + single cycle worker guarantee
  only one active batch per cycle, so consecutive batches never share a snapshot.
* `emit_and_notify` swallows every exception — a report failure must never disturb the
  trading loop.

The report highlights **unaligned** positions (held ≠ target) and gives each a
`reason_hint` so an operator can triage (capped/under-sellable, never-filled/suspended,
foreign order, plain residual).
"""

import logging
from dataclasses import dataclass

from gm.api import PositionSide_Long

from . import order_log
from .models import BatchDoc, PositionView

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderReportRow:
    order_id: str
    symbol: str
    target: int
    held: int
    available: int
    diff: int                 # target - held; 0 ⇒ aligned
    live_own: int             # this batch's own orders still on the book
    foreign: int              # non-ours unfinished orders on this symbol
    capped: bool              # a sell was clamped to `available` at some point
    reason_hint: str | None   # set iff not aligned

    @property
    def aligned(self) -> bool:
        return self.diff == 0


@dataclass(frozen=True)
class BatchReport:
    batch_id: str
    outcome: str                       # "matched" | "expired"
    rows: list[OrderReportRow]

    @property
    def unaligned(self) -> list[OrderReportRow]:
        return [r for r in self.rows if not r.aligned]

    @property
    def all_aligned(self) -> bool:
        return all(r.aligned for r in self.rows)

    def render(self) -> str:
        lines = [f"batch={self.batch_id} outcome={self.outcome} "
                 f"orders={len(self.rows)} unaligned={len(self.unaligned)}"]
        for r in self.unaligned:
            lines.append(
                f"  [UNALIGNED] {r.symbol} order={r.order_id} "
                f"target={r.target} held={r.held} available={r.available} diff={r.diff} "
                f"live_own={r.live_own} foreign={r.foreign} capped={r.capped} "
                f"-> {r.reason_hint}")
        return "\n".join(lines)


def build(doc: BatchDoc, positions: dict[tuple[str, int], PositionView],
          unfinished: dict[str, list], outcome: str) -> BatchReport:
    # One replay of the record gives both this batch's cl_ord_ids and the symbols
    # whose sells were capped this run.
    own_cl_ord_ids: set[str] = set()
    capped_orders: set[str] = set()
    for ev in order_log.replay_record(doc.batch_id):
        kind = ev.get("event")
        if kind == "submit" and isinstance(ev.get("cl_ord_id"), str):
            own_cl_ord_ids.add(ev["cl_ord_id"])
        elif kind == "cap" and isinstance(ev.get("order_id"), str):
            capped_orders.add(ev["order_id"])

    rows: list[OrderReportRow] = []
    for order in doc.orders:
        pv = positions.get((order.symbol, int(PositionSide_Long)), PositionView(0, 0))
        diff = order.target - pv.volume
        sym_unfinished = unfinished.get(order.symbol, [])
        live_own = sum(1 for o in sym_unfinished if o.cl_ord_id in own_cl_ord_ids)
        foreign = len(sym_unfinished) - live_own
        capped = order.id in capped_orders
        rows.append(OrderReportRow(
            order_id=order.id, symbol=order.symbol,
            target=order.target, held=pv.volume, available=pv.available, diff=diff,
            live_own=live_own, foreign=foreign, capped=capped,
            reason_hint=_reason(diff, pv, live_own, foreign, capped),
        ))
    return BatchReport(batch_id=doc.batch_id, outcome=outcome, rows=rows)


def _reason(diff: int, pv: PositionView, live_own: int, foreign: int, capped: bool) -> str | None:
    if diff == 0:
        return None
    if diff < 0:                                    # held > target: a sell that didn't complete
        shortfall = -diff
        if capped or pv.available < shortfall:
            return "under-sellable: 未到账/T+1/冻结，可卖不足"
    if foreign > 0:
        return "foreign-order: 非本系统委托占用，需人工"
    if live_own > 0:
        return "never-filled: 委托挂着未成交，疑似停牌/流动性不足"
    return "residual-mismatch: 持仓与目标不符，需复查"


def emit_and_notify(doc: BatchDoc, positions: dict[tuple[str, int], PositionView],
                    unfinished: dict[str, list], outcome: str) -> None:
    """Build the report from the in-cycle snapshot and hand it to the notifier.

    Never raises — called on the terminal path of `run_cycle` under `batch_state_lock`.
    """
    try:
        from . import notify                        # late import: notify has no cycle deps
        notify.notify(build(doc, positions, unfinished, outcome))
    except Exception:
        log.exception("report failed for %s; non-fatal", doc.batch_id)
