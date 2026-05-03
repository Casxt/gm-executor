"""Per-batch JSONL log: append, replay, locate, move, plus the cl_ord_id index.

Two write paths:

* `cycle_session(batch_id)` — cycle worker. Opens the active batch's record fd
  *once* per cycle and reuses it for every append in that cycle. Active batches
  always live in `pending/`, so the path is hardcoded (no locate). This is the
  hot path: a 5-order cycle appends 5+ lines through one fd.

* `append(batch_id, event)` — callback-processor drain. Sparse, opens/writes/
  closes per event. Uses `locate_record` because the batch may have moved to
  `finished/` or `expired/` between submit and a late callback.

Why two paths — observed 2026-05-03 on cloud VM with metadata-IOPS throttling:
every kernel metadata op (open, close, `path.exists`) gets quantized to ~1010ms
refill buckets. With per-append open+close, a 5-order cycle paid ~8s of bucket
waits inside `append`. The session amortises this to one open + one close per
cycle, regardless of N. Callback path stays simple — its appends are sparse
and time-spread, so the bucket cost is rarely felt.

Concurrency:

* The cycle worker holds `batch_state_lock` for the entire `run_cycle`, so it
  is the *only* writer for that duration. The session writes without
  `log_lock` — no contention is possible.
* Callbacks acquire `batch_state_lock` then briefly `log_lock` for
  open→write→close. Because batch_state_lock is held, log_lock is currently
  redundant; kept as a defensive write-exclusion gate for any future code.
* `clord_index` is written only by the cycle worker (under `batch_state_lock`)
  and read only by the drain (also under `batch_state_lock`). Events delivered
  mid-cycle sit in the SDK→drain queue until the cycle releases.
* `batch_state_lock` is reentrant: the cycle worker holds it across the entire
  `run_cycle`, then re-enters here via `cycle_session` / `replay_record` /
  `move_pair`.
* No `os.fsync` and no explicit `flush`: broker is source of truth, log is
  audit. `close()` flushes Python's buffer to the OS, so a crash never loses
  an already-written line. Power-cut may lose a few.
"""

import json
import logging
import shutil
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

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

    Caller MUST hold `batch_state_lock`. Each `path.exists()` is a kernel
    metadata op — on a throttled cloud disk it can take ~1010ms. The cycle
    avoids this entirely via `cycle_session`; only the (sparse) callback
    path uses it.
    """
    for d in config.LIVE_DIRS:
        p = _record_path(d, batch_id)
        if p.exists():
            return p
    return None


# ── cycle session: one open fd reused across all appends in one cycle ─

class _Session:
    """Lazy-open fd for the cycle's appends to a single batch.

    The fd opens on the *first* append (so a no-op cycle pays zero file-IO).
    Subsequent appends are pure userspace writes. `close()` happens at the
    `with` block's exit and is the only other metadata op.
    """

    def __init__(self, batch_id: str, path: Path):
        self._batch_id = batch_id
        self._path = path
        self._fd = None
        self._writes = 0
        self._open_ms = 0.0

    def append(self, event: dict[str, Any]) -> None:
        line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"
        if self._fd is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            t0 = time.perf_counter()
            self._fd = open(self._path, "a", encoding="utf-8")
            self._open_ms = (time.perf_counter() - t0) * 1000
        self._fd.write(line)
        self._writes += 1

    def _close(self) -> None:
        if self._fd is None:
            return
        t0 = time.perf_counter()
        self._fd.close()
        close_ms = (time.perf_counter() - t0) * 1000
        log.info("session: batch=%s writes=%d open=%.1fms close=%.1fms",
                 self._batch_id, self._writes, self._open_ms, close_ms)


@contextmanager
def cycle_session(batch_id: str) -> Iterator[_Session]:
    """One open fd reused across all appends in one cycle. Active batches
    always live in `pending/`, so the path is hardcoded (no locate).

    Caller MUST hold `batch_state_lock` (typically the cycle worker, which
    holds it for the whole `run_cycle`). Lazy-open: a session that gets no
    appends does no file-IO at all.
    """
    s = _Session(batch_id, _record_path(config.PENDING_DIR, batch_id))
    try:
        yield s
    finally:
        s._close()


# ── append (callback path) ────────────────────────────────────────────

_APPEND_SLOW_MS = 50.0
"""Per-step breakdown emitted when total append exceeds this. Bench shows ~0.4ms;
throttled cloud disk shows 1–8s. The threshold filters noise."""


def append(batch_id: str, event: dict[str, Any]) -> None:
    """Append one JSONL line to a batch's record. Callback-drain path only.

    The cycle uses `cycle_session` for batched appends; callbacks use this
    because a late callback may need to write to a record now in `finished/`
    or `expired/`, so we still need to `locate_record`.
    """
    line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"

    t0 = time.perf_counter()
    with state.batch_state_lock:
        path = locate_record(batch_id)
        if path is None:
            if event.get("event") != "submit":
                log.warning("no record for batch_id=%s; dropping event=%s",
                            batch_id, event.get("event"))
                return
            path = _record_path(config.PENDING_DIR, batch_id)
            path.parent.mkdir(parents=True, exist_ok=True)
        t_setup = time.perf_counter()

        with state.log_lock:
            with open(path, "a", encoding="utf-8") as f:
                t_open = time.perf_counter()
                f.write(line)
            t_close = time.perf_counter()
    t_done = time.perf_counter()

    total_ms = (t_done - t0) * 1000
    if total_ms > _APPEND_SLOW_MS:
        log.info("append slow: total=%.1fms batch=%s | setup=%.1f open=%.1f close=%.1f",
                 total_ms, batch_id,
                 (t_setup - t0)      * 1000,
                 (t_open  - t_setup) * 1000,
                 (t_close - t_open)  * 1000)


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
