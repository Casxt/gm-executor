"""Per-batch JSONL log: append, replay, locate, move, plus the cl_ord_id index.

Concurrency rules from FLOW.md:

* Writers (`append`) hold `batch_state_lock` for the whole locate→open→write→close,
  then briefly `log_lock` for write-exclusion. Lock order is `batch_state_lock` then
  `log_lock`; never the reverse.
* Readers (`replay_record`) acquire both locks too, briefly, so they always observe a
  consistent file (no path-changes mid-read, no half-written line).
* `clord_index` is written only by the cycle worker (under `batch_state_lock`) and read only by
  the `callback-processor` daemon (also under `batch_state_lock`). Events delivered during a cycle
  sit in the SDK→drain queue until the cycle releases the lock, so the drain always observes a
  fully-committed `clord_index`. No race, no dropped events for our own orders.
* No `os.fsync`: broker is source of truth, log is audit. Windows + AV makes fsync
  multi-second and serialises the cycle. Crash loses ≤1 line; power-cut a few.
* `batch_state_lock` is reentrant: the cycle worker holds it across the entire
  `run_cycle`, then re-enters here via `append` / `replay_record` / `move_pair`.
"""

import json
import logging
import shutil
from pathlib import Path
from typing import Any

from . import config, state

log = logging.getLogger(__name__)

clord_index: dict[str, str] = {}
"""cl_ord_id → batch_id. Reads (callbacks) and writes (cycle) both happen under `batch_state_lock`."""


# ── filename helpers ──────────────────────────────────────────────────

def _record_path(batch_dir: Path, batch_id: str) -> Path:
    return batch_dir / f"{batch_id}.order_record.jsonl"


def _batch_path(batch_dir: Path, batch_id: str) -> Path:
    return batch_dir / f"{batch_id}.json"


# ── locate ────────────────────────────────────────────────────────────

def locate_record(batch_id: str) -> Path | None:
    """First matching record file across pending/finished/expired/.

    Caller MUST hold `batch_state_lock`.
    """
    for d in config.LIVE_DIRS:
        p = _record_path(d, batch_id)
        if p.exists():
            return p
    return None


# ── append ────────────────────────────────────────────────────────────

def append(batch_id: str, event: dict[str, Any]) -> None:
    """Append one JSONL line to a batch's record.

    The first line is always a `submit` written by the cycle thread; that call also
    creates the record file. For non-`submit` events arriving before any submit
    landed (callbacks for foreign orders, or a clord_index miss), we drop and warn.
    """
    line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"

    with state.batch_state_lock:
        path = locate_record(batch_id)
        if path is None:
            if event.get("event") != "submit":
                log.warning("no record for batch_id=%s; dropping event=%s",
                            batch_id, event.get("event"))
                return
            path = _record_path(config.PENDING_DIR, batch_id)
            path.parent.mkdir(parents=True, exist_ok=True)

        with state.log_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()                                        # no fsync — see ORDER_RECORD.md


# ── replay ────────────────────────────────────────────────────────────

def replay_record(batch_id: str) -> list[dict[str, Any]]:
    """All events for `batch_id`, in file order. Empty list if no record exists yet."""
    with state.batch_state_lock:
        path = locate_record(batch_id)
        if path is None:
            return []
        with state.log_lock:
            try:
                data = path.read_text(encoding="utf-8")
            except OSError:
                log.exception("failed to read %s", path)
                return []

    out: list[dict[str, Any]] = []
    for line in data.splitlines():
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            log.warning("corrupt line in %s: %r", path, line)
    return out


# ── move ──────────────────────────────────────────────────────────────

def move_pair(batch_id: str, dst: Path) -> None:
    """Move both <id>.json and <id>.order_record.jsonl to `dst`, if they exist."""
    dst.mkdir(parents=True, exist_ok=True)
    with state.batch_state_lock:
        for src_dir in config.LIVE_DIRS:
            if src_dir == dst:
                continue
            for src in (_batch_path(src_dir, batch_id), _record_path(src_dir, batch_id)):
                if src.exists():
                    shutil.move(str(src), str(dst / src.name))


def move_invalid(path: Path) -> None:
    """Move a batch that failed parse/validate to failed/. No record exists yet."""
    config.FAILED_DIR.mkdir(parents=True, exist_ok=True)
    with state.batch_state_lock:
        shutil.move(str(path), str(config.FAILED_DIR / path.name))


# ── clord_index rebuild ───────────────────────────────────────────────

def rebuild_clord_index() -> None:
    """Scan every *.order_record.jsonl under live dirs for `submit` lines."""
    with state.log_lock:
        clord_index.clear()
        for d in config.LIVE_DIRS:
            if not d.exists():
                continue
            for record_path in d.glob("*.order_record.jsonl"):
                _scan_submits(record_path)
    log.info("clord_index rebuilt: %d entries", len(clord_index))


def _scan_submits(record_path: Path) -> None:
    batch_id = record_path.name.removesuffix(".order_record.jsonl")
    try:
        with open(record_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if ev.get("event") == "submit":
                    cl_ord_id = ev.get("cl_ord_id")
                    if isinstance(cl_ord_id, str):
                        clord_index[cl_ord_id] = batch_id
    except OSError:
        log.exception("failed reading %s during clord_index rebuild", record_path)
