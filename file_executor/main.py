"""gm-executor entry point.

The gm SDK re-imports this file by path inside `gm.api.run(filename=__file__)`
and discovers callbacks by *module-level* name. Every `on_*` callback is
re-exported at module top below so the SDK can find them.

Run with:

    python -m file_executor.main

Required env: `GM_TOKEN`. Optional: `GMX_ORDERS_DIR`, `GMX_POLL_SECONDS`,
`GMX_GIT_REPO_URL`, `GMX_GIT_LOCAL_DIR`, `GMX_GIT_BRANCH`, `GMX_LOG_DIR`,
`GMX_LOG_FILE`, `GMX_LOG_MAX_BYTES`, `GMX_LOG_BACKUP_COUNT`,
`GMX_FEISHU_WEBHOOKS`. See coding-style.md and the docs under
`file_executor/docs/` for the full surface.
"""

import logging
import os
import sys
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
from queue import SimpleQueue

from gm.api import MODE_LIVE, run, set_token, stop, timer

from file_executor import callbacks as _cb
from file_executor import bench, config, connector, order_log, remote_log, state, worker

# ── SDK callback bindings (module-level so the SDK's importer sees them) ─
on_timer                    = _cb.on_timer
on_order_status             = _cb.on_order_status
on_execution_report         = _cb.on_execution_report
on_trade_data_connected     = _cb.on_trade_data_connected
on_trade_data_disconnected  = _cb.on_trade_data_disconnected
on_market_data_connected    = _cb.on_market_data_connected
on_market_data_disconnected = _cb.on_market_data_disconnected
on_account_status           = _cb.on_account_status
on_error                    = _cb.on_error


log = logging.getLogger("file_executor")
_local_log_listener: QueueListener | None = None
_local_log_handlers: list[logging.Handler] = []


def _setup_local_logging() -> None:
    global _local_log_handlers, _local_log_listener

    _stop_local_logging()

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

    q = SimpleQueue()
    queue_handler = QueueHandler(q)
    queue_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    for old_handler in root.handlers[:]:
        root.removeHandler(old_handler)
    root.setLevel(logging.INFO)
    root.addHandler(queue_handler)

    _local_log_listener = QueueListener(
        q, console, file_handler, respect_handler_level=True,
    )
    _local_log_handlers = [console, file_handler]
    _local_log_listener.start()


def _stop_local_logging() -> None:
    global _local_log_handlers, _local_log_listener
    if _local_log_listener is None:
        return
    _local_log_listener.stop()
    _local_log_listener = None
    for handler in _local_log_handlers:
        handler.close()
    _local_log_handlers = []


def init(context) -> None:
    """SDK-invoked once after `run()` connects. Wire everything here."""
    log.info("file_executor init: orders_dir=%s poll=%ds",
             config.ORDERS_DIR, config.POLL_SECONDS)

    config.ensure_dirs()
    bench.bench_disk()
    order_log.rebuild_clord_index()

    # Drain callbacks AFTER clord_index is rebuilt: any queued events from before
    # init() (e.g. broker replaying in-flight statuses on reconnect) will then see
    # a populated index instead of being warned as foreign.
    _cb.start()

    # if config.GM_TOKEN:
    #     set_token(config.GM_TOKEN)

    worker.start()
    connector.start()
    # remote_log was started in main() so warnings during run()/connect ship out

    timer(timer_func=on_timer,
          period=config.POLL_INTERVAL_MS,
          start_delay=0)


def main() -> None:
    _setup_local_logging()
    remote_log.start()

    if not config.GM_TOKEN:
        log.error("GM_TOKEN env var not set; aborting")
        sys.exit(1)

    # gm.api.run() derives a module name by str-prefix-matching `filename` against
    # sys.path entries (which it forces to forward slashes), then converts separators
    # to dots. On Windows the filename keeps backslashes, so commonprefix collapses to
    # "C:" and the result becomes a relative import like ".Users.kaizh...main". Pass
    # forward slashes so the SDK's prefix match finds the real sys.path entry.
    strategy_file = os.path.abspath(__file__).replace(os.sep, "/")

    try:
        run(strategy_id=config.GM_STRATEGY_ID,
            filename=strategy_file,
            mode=MODE_LIVE,
            token=config.GM_TOKEN,
            serv_addr=config.GM_SERV_ADDR or "")
    except KeyboardInterrupt:
        log.info("SIGINT received; shutting down")
    except Exception:
        log.exception("gm.run() crashed; shutting down")
    finally:
        _shutdown()


def _shutdown() -> None:
    state.stop_event.set()
    state.cycle_pending.set()                                # wake cycle_worker
    _cb.stop()                                                # wake callback-processor
    for t in state.worker_threads:
        t.join(timeout=10)
    try:
        remote_log.stop()                                     # close relay stdin → drain → exit
    except Exception:
        log.exception("remote_log.stop() failed")
    try:
        stop()
    except Exception:
        log.exception("gm.stop() failed")
    finally:
        _stop_local_logging()


if __name__ == "__main__":
    main()
