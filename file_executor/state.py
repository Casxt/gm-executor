"""Process-wide runtime state. Single source of truth for locks, events, threads.

Everything here is a module-level singleton. The only mutable container is
`worker_threads`; everything else is a primitive that's safe to share.

Lock order (never reverse): `log_move_lock` then `log_lock`.
"""

import threading

# ── flow control ──────────────────────────────────────────────────────
stop_event:       threading.Event = threading.Event()
"""Set during shutdown. Workers check it on every loop boundary."""

cycle_pending:    threading.Event = threading.Event()
"""Set by `on_timer` (and any caller that wants a refresh). Consumed by `cycle_worker`."""

trade_channel_up: threading.Event = threading.Event()
"""Optimistic at startup; first heartbeat callback confirms or clears."""
trade_channel_up.set()

# ── locks ─────────────────────────────────────────────────────────────
log_lock:      threading.Lock = threading.Lock()
"""Serialises *writes* to any order_record.jsonl. Brief — open→write→fsync→close."""

log_move_lock: threading.Lock = threading.Lock()
"""Serialises *path changes* (move_pair) and path lookups (locate_record)."""

# ── runtime ───────────────────────────────────────────────────────────
worker_threads: list[threading.Thread] = []
"""Daemon threads we own. Joined on shutdown."""
