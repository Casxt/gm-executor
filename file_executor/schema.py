"""Parse a batch JSON file and validate it against SCHEMA.md.

A `SchemaError` means the file violates the contract — caller should move it
to `failed/`. Everything else (file missing, JSON syntax) propagates as the
underlying exception type.
"""

import json
import re
from pathlib import Path

from .models import BatchDoc, OrderSpec

_BATCH_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
_SYMBOL_RE   = re.compile(r"^(SHSE|SZSE|BJSE)\.[A-Za-z0-9]+$")

_VALID_TOPLEVEL_KEYS: frozenset[str] = frozenset({
    "schema_version", "batch_id", "valid_at", "expires_at", "account_id", "orders",
})
_VALID_ORDER_KEYS: frozenset[str]    = frozenset({"id", "symbol", "target", "order_type", "price"})
_REQUIRED_ORDER_KEYS: frozenset[str] = frozenset({"id", "symbol", "target", "order_type"})


class SchemaError(ValueError):
    """The JSON parsed but does not conform to the batch schema."""


def parse_and_validate(path: Path) -> BatchDoc:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SchemaError(f"top-level must be object, got {type(raw).__name__}")

    extra = set(raw) - _VALID_TOPLEVEL_KEYS
    if extra:
        raise SchemaError(f"unknown top-level keys: {sorted(extra)}")

    if raw.get("schema_version") != "1":
        raise SchemaError(f"schema_version must be '1', got {raw.get('schema_version')!r}")

    batch_id = raw.get("batch_id")
    if not isinstance(batch_id, str) or not _BATCH_ID_RE.match(batch_id):
        raise SchemaError(f"batch_id invalid: {batch_id!r}")
    if path.stem != batch_id:
        raise SchemaError(f"filename {path.stem!r} does not match batch_id {batch_id!r}")

    valid_at   = _require_unix_seconds(raw, "valid_at")
    expires_at = _require_unix_seconds(raw, "expires_at")
    if valid_at >= expires_at:
        raise SchemaError(f"valid_at ({valid_at}) must be < expires_at ({expires_at})")

    account_id = raw.get("account_id")
    if account_id is not None and not isinstance(account_id, str):
        raise SchemaError(f"account_id must be string or absent, got {type(account_id).__name__}")

    orders_raw = raw.get("orders")
    if not isinstance(orders_raw, list) or not orders_raw:
        raise SchemaError("orders must be a non-empty list")

    seen_ids:     set[str] = set()
    seen_symbols: set[str] = set()
    orders: list[OrderSpec] = []
    for i, o in enumerate(orders_raw):
        orders.append(_parse_order(i, o, seen_ids, seen_symbols))

    return BatchDoc(
        schema_version="1",
        batch_id=batch_id,
        valid_at=valid_at,
        expires_at=expires_at,
        account_id=account_id,
        orders=orders,
    )


def _require_unix_seconds(raw: dict, key: str) -> int:
    v = raw.get(key)
    if not isinstance(v, int) or isinstance(v, bool) or v < 0:
        raise SchemaError(f"{key} must be non-negative int, got {v!r}")
    return v


def _parse_order(i: int, o: object, seen_ids: set[str], seen_symbols: set[str]) -> OrderSpec:
    if not isinstance(o, dict):
        raise SchemaError(f"orders[{i}] must be object")

    extra = set(o) - _VALID_ORDER_KEYS
    if extra:
        raise SchemaError(f"orders[{i}] has unknown keys: {sorted(extra)}")
    missing = _REQUIRED_ORDER_KEYS - set(o)
    if missing:
        raise SchemaError(f"orders[{i}] missing keys: {sorted(missing)}")

    oid = o["id"]
    if not isinstance(oid, str) or not oid:
        raise SchemaError(f"orders[{i}].id must be non-empty string")
    if oid in seen_ids:
        raise SchemaError(f"orders[{i}].id duplicate: {oid!r}")
    seen_ids.add(oid)

    symbol = o["symbol"]
    if not isinstance(symbol, str) or not _SYMBOL_RE.match(symbol):
        raise SchemaError(f"orders[{i}].symbol invalid: {symbol!r}")
    if symbol in seen_symbols:
        raise SchemaError(f"orders[{i}].symbol duplicate: {symbol!r}")
    seen_symbols.add(symbol)

    target = o["target"]
    if not isinstance(target, int) or isinstance(target, bool) or target < 0:
        raise SchemaError(f"orders[{i}].target must be non-negative int, got {target!r}")

    order_type = o["order_type"]
    if order_type not in ("market", "limit"):
        raise SchemaError(f"orders[{i}].order_type must be 'market' or 'limit', got {order_type!r}")

    price = o.get("price")
    if order_type == "limit":
        if not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0:
            raise SchemaError(f"orders[{i}].price must be > 0 for limit, got {price!r}")
        price = float(price)
    else:
        if "price" in o:
            raise SchemaError(f"orders[{i}].price forbidden for market order")
        price = None

    return OrderSpec(id=oid, symbol=symbol, target=target,
                     order_type=order_type, price=price)
