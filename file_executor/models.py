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
class BatchDoc:
    schema_version: str
    batch_id: str
    valid_at: int
    expires_at: int
    account_id: str | None
    orders: list[OrderSpec]
