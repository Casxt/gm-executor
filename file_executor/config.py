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
    """One-shot disk smoke test, mimicking the cycle's real workload: fresh files
    (cold AV scan path), not overwrites (warm cache).

    Reports median AND max — AV bursts on cold writes, so a fast median with a
    multi-second max is the actual signature of the slowness we hit at runtime.
    """
    log = logging.getLogger("file_executor.bench")
    payload = ("x" * 200 + "\n") * 5                    # ~1 KB, 5 lines
    n = 8

    def _stats(samples: list[float]) -> tuple[float, float]:
        s = sorted(samples)
        return s[len(s) // 2], s[-1]                    # median, max

    probes = [PENDING_DIR / f".bench_probe_{i}" for i in range(n)]
    glob_samples: list[float] = []
    create_samples: list[float] = []                    # open("w") + write + flush + close — first-touch
    append_samples: list[float] = []                    # open("a") + write + flush + close — second touch
    read_samples: list[float] = []                      # open("r") + read

    try:
        for p in probes:
            t0 = time.perf_counter()
            list(PENDING_DIR.glob("*.json"))
            glob_samples.append((time.perf_counter() - t0) * 1000)

            t0 = time.perf_counter()
            with open(p, "w", encoding="utf-8") as f: f.write(payload); f.flush()
            create_samples.append((time.perf_counter() - t0) * 1000)

            t0 = time.perf_counter()
            with open(p, "a", encoding="utf-8") as f: f.write(payload); f.flush()
            append_samples.append((time.perf_counter() - t0) * 1000)

            t0 = time.perf_counter()
            with open(p, "r", encoding="utf-8") as f: f.read()
            read_samples.append((time.perf_counter() - t0) * 1000)

        gm, gmax = _stats(glob_samples)
        cm, cmax = _stats(create_samples)
        am, amax = _stats(append_samples)
        rm, rmax = _stats(read_samples)
        log.info("disk_bench: pending/ (n=%d fresh files) "
                 "glob med=%.1fms max=%.1fms | create med=%.1fms max=%.1fms | "
                 "append med=%.1fms max=%.1fms | read med=%.1fms max=%.1fms "
                 "(max >100ms ⇒ AV/cloud-sync hooking the path)",
                 n, gm, gmax, cm, cmax, am, amax, rm, rmax)
    except Exception:
        log.exception("disk_bench failed")
    finally:
        for p in probes:
            try: p.unlink(missing_ok=True)
            except Exception: pass
