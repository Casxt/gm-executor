"""Process-wide configuration. Resolved once at module import.

Every value here comes from an env var. No file I/O at import time except
`ensure_dirs()`, which is called explicitly during `init()`.
"""

import os
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
