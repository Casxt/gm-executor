"""gm-executor entry point.

The gm SDK re-imports this file by path inside `gm.api.run(filename=__file__)`
and discovers callbacks by *module-level* name. Every `on_*` callback is
re-exported at module top below so the SDK can find them.

Run with:

    python -m file_executor.main

Required env: `GM_TOKEN`. Optional: `GMX_ORDERS_DIR`, `GMX_POLL_SECONDS`,
`GMX_GIT_REPO_URL`, `GMX_GIT_LOCAL_DIR`, `GMX_GIT_BRANCH`, `GMX_FEISHU_WEBHOOKS`. See coding-style.md
and the docs under `file_executor/docs/` for the full surface.
"""

import logging
import os
import sys

from gm.api import MODE_LIVE, run, set_token, stop, timer

from file_executor import callbacks as _cb
from file_executor import config, connector, order_log, remote_log, state, worker

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


def _setup_local_logging() -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(fmt))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def init(context) -> None:
    """SDK-invoked once after `run()` connects. Wire everything here."""
    log.info("file_executor init: orders_dir=%s poll=%ds",
             config.ORDERS_DIR, config.POLL_SECONDS)

    config.ensure_dirs()
    order_log.rebuild_clord_index()

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

    try:
        run(strategy_id=config.GM_STRATEGY_ID,
            filename=os.path.abspath(__file__),
            mode=MODE_LIVE,
            token=config.GM_TOKEN,
            serv_addr=config.GM_SERV_ADDR or "")
    except KeyboardInterrupt:
        log.info("SIGINT received; shutting down")
    finally:
        _shutdown()


def _shutdown() -> None:
    state.stop_event.set()
    state.cycle_pending.set()                                # wake cycle_worker
    for t in state.worker_threads:
        t.join(timeout=10)
    try:
        stop()
    except Exception:
        log.exception("gm.stop() failed")


if __name__ == "__main__":
    main()
