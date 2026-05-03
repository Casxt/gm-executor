"""Process-wide configuration. Resolved once at module import.

Every value here comes from an env var. No file I/O at import time except
`ensure_dirs()`, which is called explicitly during `init()`.
"""

import logging
import os
import time
from pathlib import Path

# ── orders directory ──────────────────────────────────────────────────
ORDERS_DIR: Path   = Path(os.environ.get("GMX_ORDERS_DIR", "./orders")).resolve()
PENDING_DIR: Path  = ORDERS_DIR / "pending"
FINISHED_DIR: Path = ORDERS_DIR / "finished"
EXPIRED_DIR: Path  = ORDERS_DIR / "expired"
FAILED_DIR: Path   = ORDERS_DIR / "failed"

ALL_BATCH_DIRS: tuple[Path, ...] = (PENDING_DIR, FINISHED_DIR, EXPIRED_DIR, FAILED_DIR)
LIVE_DIRS: tuple[Path, ...]      = (PENDING_DIR, FINISHED_DIR, EXPIRED_DIR)
"""Dirs that may contain *.order_record.jsonl. failed/ never gets a record."""

# ── gm SDK auth & flow ────────────────────────────────────────────────
GM_TOKEN: str        = os.environ.get("GM_TOKEN", "")
GM_STRATEGY_ID: str  = os.environ.get("GM_STRATEGY_ID", "")
GM_ACCOUNT_ID: str   = os.environ.get("GM_ACCOUNT_ID", "")
GM_SERV_ADDR: str    = os.environ.get("GM_SERV_ADDR", "")

POLL_SECONDS: int     = int(os.environ.get("GMX_POLL_SECONDS", "30"))
POLL_INTERVAL_MS: int = POLL_SECONDS * 1000

STUCK_ORDER_SECONDS: int = 600
"""Per FLOW.md: log error when an own unfinished order has been on the book this long."""

RECONNECT_GRACE_SECONDS: int = int(os.environ.get("GMX_RECONNECT_GRACE_SECONDS", "30"))
"""Cycle skips for this long after every `on_trade_data_connected` so the broker's
status-replay storm settles before we trust position/unfinished snapshots."""

# ── connector ─────────────────────────────────────────────────────────
GIT_REPO_URL: str    = os.environ.get("GMX_GIT_REPO_URL", "")
GIT_LOCAL_DIR: Path  = Path(os.environ.get("GMX_GIT_LOCAL_DIR", "./git_orders")).resolve()
GIT_BRANCH: str      = os.environ.get("GMX_GIT_BRANCH", "main")
GIT_PULL_SECONDS: int = int(os.environ.get("GMX_GIT_PULL_SECONDS", "30"))

# ── remote logging ────────────────────────────────────────────────────
FEISHU_WEBHOOKS: list[str] = [
    u.strip() for u in os.environ.get("GMX_FEISHU_WEBHOOKS", "").split(",") if u.strip()
]
REMOTE_LOG_INTERVAL: int   = int(os.environ.get("GMX_REMOTE_LOG_INTERVAL", "30"))
REMOTE_LOG_QUEUE_MAX: int  = int(os.environ.get("GMX_REMOTE_LOG_QUEUE_MAX", "10000"))


def ensure_dirs() -> None:
    for d in ALL_BATCH_DIRS:
        d.mkdir(parents=True, exist_ok=True)


def bench_disk() -> None:
    """One-shot disk smoke test on PENDING_DIR. Logged at INFO so AV/path issues
    show up at startup instead of surprising us mid-cycle.

    Each measurement is the median of N tries — single samples on Windows + AV
    are too noisy (first hit often 100x the steady-state cost).
    """
    log = logging.getLogger("file_executor.bench")
    probe = PENDING_DIR / ".bench_probe"
    payload = ("x" * 200 + "\n") * 5                    # ~1 KB, 5 lines

    def _med(fn, n: int = 5) -> float:
        samples = []
        for _ in range(n):
            t0 = time.perf_counter()
            fn()
            samples.append((time.perf_counter() - t0) * 1000)
        samples.sort()
        return samples[len(samples) // 2]

    try:
        glob_ms = _med(lambda: list(PENDING_DIR.glob("*.json")))
        def _write():
            with open(probe, "w", encoding="utf-8") as f: f.write(payload); f.flush()
        write_ms = _med(_write)
        def _read():
            with open(probe, "r", encoding="utf-8") as f: f.read()
        read_ms = _med(_read)
        log.info("disk_bench: pending/ glob=%.1fms write=%.1fms read=%.1fms (median of 5; "
                 "expect <5ms each — sustained >100ms ⇒ AV/cloud-sync hooking the path)",
                 glob_ms, write_ms, read_ms)
    except Exception:
        log.exception("disk_bench failed")
    finally:
        try: probe.unlink(missing_ok=True)
        except Exception: pass
