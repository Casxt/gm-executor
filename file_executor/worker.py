"""cycle_worker daemon: runs run_cycle whenever cycle_pending is signalled.

Single-thread serialisation of the cycle. Catch-and-continue on per-cycle
exceptions, exponential backoff capped at 60 s on repeated failures.
"""

import logging
import threading

from . import state
from .cycle import run_cycle

log = logging.getLogger(__name__)

_BACKOFF_CAP_SECONDS = 60


def cycle_worker() -> None:
    consecutive_failures = 0
    while not state.stop_event.is_set():
        state.cycle_pending.wait()
        if state.stop_event.is_set():
            return

        try:
            run_cycle()
            consecutive_failures = 0
        except Exception:
            consecutive_failures += 1
            backoff = min(_BACKOFF_CAP_SECONDS, 2 ** consecutive_failures)
            log.exception("cycle failed (#%d); backing off %ds",
                          consecutive_failures, backoff)
            state.stop_event.wait(backoff)
        finally:
            state.cycle_pending.clear()                     # drop signals raised during the run


def start() -> threading.Thread:
    t = threading.Thread(target=cycle_worker, name="cycle-worker", daemon=True)
    t.start()
    state.worker_threads.append(t)
    return t
