"""Process-wide runtime state. Single source of truth for locks, events, threads.

Everything here is a module-level singleton. The only mutable container is
`worker_threads`; everything else is a primitive that's safe to share.

Lock order (never reverse): `batch_state_lock` then `log_lock`.
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

trade_channel_up_at: float = 0.0
"""`time.time()` of the most recent `on_trade_data_connected` callback. The cycle
skips while `now - trade_channel_up_at < RECONNECT_GRACE_SECONDS` to let the
reconnect replay storm settle (FLOW.md "Reconnect replay"). 0.0 ⇒ no callback yet,
optimistic flag governs."""

# ── locks ─────────────────────────────────────────────────────────────
log_lock:      threading.Lock = threading.Lock()
"""Serialises *writes* to any order_record.jsonl. Brief — open→write→fsync→close."""

batch_state_lock: threading.RLock = threading.RLock()
"""Serialises everything that observes or mutates **which directory each batch lives in**.

Held by:
- the cycle worker for the entire `run_cycle` (so its view of pending/ is stable —
  no connector edit, no callback-driven move can interleave);
- the connector around `is_terminal(batch_id)` + `atomic_copy` (so its mirror cannot
  resurrect a freshly-finalized batch);
- the `callback-processor` daemon for each event drained from the SDK queue (so it
  observes `clord_index` only after the in-flight cycle commits — no race-drop of
  events for orders we just submitted). The SDK callback functions themselves only
  snapshot + enqueue; they never touch this lock;
- `order_log.move_pair` / `move_invalid` (cross-directory moves);
- `order_log.append` / `replay_record` / `locate_record` (path lookups must agree
  with whichever directory currently owns the batch).

Reentrant so the cycle can call into `order_log` helpers while already holding it.
"""

# ── runtime ───────────────────────────────────────────────────────────
worker_threads: list[threading.Thread] = []
"""Daemon threads we own. Joined on shutdown."""
