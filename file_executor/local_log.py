"""Local logging setup: console + rotating file behind a QueueListener.

`QueueHandler` makes `log.info(...)` non-blocking on every producer thread —
the cycle, drain, connector, and SDK callbacks all enqueue and return in
microseconds. A single listener thread drains the queue to stderr and
`logs/gm-executor.log`. Rationale: stderr writes to the PowerShell console
are multi-second on Windows, and without the queue every producer would
serialise behind the StreamHandler lock.
"""

import logging
import sys
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from queue import SimpleQueue

from . import config

_listener: QueueListener | None = None
_handlers: list[logging.Handler] = []


def start() -> None:
    global _listener, _handlers

    stop()

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    formatter = logging.Formatter(fmt)

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        config.LOG_DIR / config.LOG_FILE,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    q: SimpleQueue = SimpleQueue()
    queue_handler = QueueHandler(q)
    queue_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    for old in root.handlers[:]:
        root.removeHandler(old)
    root.setLevel(logging.INFO)
    root.addHandler(queue_handler)

    _listener = QueueListener(q, console, file_handler, respect_handler_level=True)
    _handlers = [console, file_handler]
    _listener.start()


def stop() -> None:
    global _listener, _handlers
    if _listener is None:
        return
    _listener.stop()
    _listener = None
    for h in _handlers:
        h.close()
    _handlers = []
