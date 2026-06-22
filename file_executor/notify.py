"""Notification sink abstraction.

A `Notifier` takes a finished/expired batch's `BatchReport` and ships it somewhere
operator-visible. The contract is deliberately narrow so a real channel (Discord,
etc.) can drop in later without touching `cycle.py` / `report.py`:

* `notify(report)` MUST be non-blocking and MUST NOT raise — the cycle worker calls
  it while holding `batch_state_lock`. Do the formatting here, but push any network
  I/O onto a queue / subprocess (see `remote_log.py` for the pattern) and return.

The default `LoggingNotifier` logs at WARNING when anything is unaligned (so it also
rides the existing Feishu relay) and at INFO when the batch matched cleanly. Swap the
active notifier via `set_notifier(...)`; a future `DiscordNotifier` only implements
this same interface.
"""

import logging
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from .report import BatchReport

log = logging.getLogger(__name__)


class Notifier(Protocol):
    """Anything that can deliver a batch close report."""

    def notify(self, report: "BatchReport") -> None:
        ...


class LoggingNotifier:
    """Default sink: render the report to the local logger.

    Unaligned (or expired) → WARNING, which the remote-log handler forwards to
    Feishu. Fully aligned + matched → a single INFO summary line (no noise).
    """

    def notify(self, report: "BatchReport") -> None:
        if report.all_aligned and report.outcome == "matched":
            log.info("batch report: %s matched, %d orders all aligned",
                     report.batch_id, len(report.rows))
            return
        log.warning("batch report: %s outcome=%s — %d/%d unaligned\n%s",
                    report.batch_id, report.outcome,
                    len(report.unaligned), len(report.rows), report.render())


_notifier: Notifier = LoggingNotifier()


def set_notifier(notifier: Notifier) -> None:
    """Replace the active notifier (e.g. install a DiscordNotifier at startup)."""
    global _notifier
    _notifier = notifier


def notify(report: "BatchReport") -> None:
    """Deliver `report` via the active notifier. Never blocks the cycle, never raises."""
    try:
        _notifier.notify(report)
    except Exception:
        log.exception("notify failed for %s; non-fatal", report.batch_id)
