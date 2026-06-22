"""Domain dataclasses. No behaviour, no validation — see schema.py for that."""

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderSpec:
    id: str
    symbol: str
    target: int
    order_type: str           # "market" | "limit"
    price: float | None       # required iff order_type == "limit"


@dataclass(frozen=True)
class PositionView:
    """One symbol's long-side position as seen in a cycle's broker snapshot.

    `volume` is the broker's total holding (settled + unsettled); `available`
    is the broker's currently sellable / closeable figure — already net of A-share
    T+1 lock, unsettled corporate-action shares, and open-order freezes. Sells are
    capped to `available`; `volume` still drives the target/held diff.
    """
    volume: int
    available: int


@dataclass(frozen=True)
class BatchDoc:
    schema_version: str
    batch_id: str
    valid_at: int
    expires_at: int
    account_id: str | None
    orders: list[OrderSpec]
