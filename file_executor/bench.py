"""Disk benchmark for the orders dir, run once at init.

The original bench used fresh probe files and was too optimistic. Production
slowness is in the order-record path: locate the live JSONL across pending /
finished / expired, open the same long-lived file, append one tiny line, flush,
and close. This benchmark keeps the quick baseline, then measures that exact
shape with per-step timings comparable to `order_log.append slow`.
"""

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

from . import config, state

log = logging.getLogger("file_executor.bench")

_BASIC_N = int(os.environ.get("GMX_BENCH_BASIC_N", "32"))
_ORDER_N = int(os.environ.get("GMX_BENCH_ORDER_N", "256"))
_PAYLOAD = ("x" * 200 + "\n") * 5
_BENCH_BATCH_ID = ".bench_order_record"


def _stats(samples: list[float]) -> tuple[float, float, float]:
    if not samples:
        return 0.0, 0.0, 0.0
    s = sorted(samples)
    return s[len(s) // 2], s[min(len(s) - 1, int(len(s) * 0.95))], s[-1]


def _fmt_stats(samples: list[float]) -> str:
    med, p95, max_v = _stats(samples)
    return f"med={med:.1f}ms p95={p95:.1f}ms max={max_v:.1f}ms"


def _ms(start: float, end: float) -> float:
    return (end - start) * 1000


def _record_path(batch_dir: Path, batch_id: str) -> Path:
    return batch_dir / f"{batch_id}.order_record.jsonl"


def _batch_path(batch_dir: Path, batch_id: str) -> Path:
    return batch_dir / f"{batch_id}.json"


def _locate_record(batch_id: str) -> Path | None:
    for d in config.LIVE_DIRS:
        p = _record_path(d, batch_id)
        if p.exists():
            return p
    return None


def _basic_measure(probes: list[Path], payload: str) -> dict[str, list[float]]:
    parent = probes[0].parent
    out: dict[str, list[float]] = {
        "glob": [],
        "create": [],
        "append": [],
        "read": [],
    }

    for p in probes:
        t0 = time.perf_counter()
        list(parent.glob("*.json"))
        out["glob"].append(_ms(t0, time.perf_counter()))

        t0 = time.perf_counter()
        with open(p, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
        out["create"].append(_ms(t0, time.perf_counter()))

        t0 = time.perf_counter()
        with open(p, "a", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
        out["append"].append(_ms(t0, time.perf_counter()))

        t0 = time.perf_counter()
        with open(p, "r", encoding="utf-8") as f:
            f.read()
        out["read"].append(_ms(t0, time.perf_counter()))

    return out


def _run_basic_phase(label: str, n: int, payload: str) -> None:
    probes = [config.PENDING_DIR / f".bench_probe_{label}_{i}" for i in range(n)]
    try:
        s = _basic_measure(probes, payload)
        log.info("disk_bench[%s_basic]: n=%d | glob %s | create %s | append %s | read %s",
                 label, n, _fmt_stats(s["glob"]), _fmt_stats(s["create"]),
                 _fmt_stats(s["append"]), _fmt_stats(s["read"]))
    except Exception:
        log.exception("disk_bench basic phase=%s failed", label)
    finally:
        for p in probes:
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


def _prepare_order_record(batch_id: str) -> tuple[Path, Path]:
    config.PENDING_DIR.mkdir(parents=True, exist_ok=True)
    batch_path = _batch_path(config.PENDING_DIR, batch_id)
    record_path = _record_path(config.PENDING_DIR, batch_id)
    batch_path.write_text(
        json.dumps({"batch_id": batch_id, "bench": True}, separators=(",", ":")),
        encoding="utf-8",
    )
    record_path.write_text("", encoding="utf-8")
    return batch_path, record_path


def _cleanup_order_record(batch_id: str) -> None:
    for d in config.LIVE_DIRS:
        for p in (_batch_path(d, batch_id), _record_path(d, batch_id)):
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass


def _append_like_production(batch_id: str, event: dict[str, Any]) -> dict[str, float]:
    line = json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n"

    t0 = time.perf_counter()
    with state.batch_state_lock:
        t_bsl = time.perf_counter()
        path = _locate_record(batch_id)
        t_locate = time.perf_counter()
        if path is None:
            path = _record_path(config.PENDING_DIR, batch_id)
            path.parent.mkdir(parents=True, exist_ok=True)
        t_setup = time.perf_counter()

        with state.log_lock:
            t_loglock = time.perf_counter()
            with open(path, "a", encoding="utf-8") as f:
                t_open = time.perf_counter()
                f.write(line)
                t_write = time.perf_counter()
                f.flush()
                t_flush = time.perf_counter()
            t_close = time.perf_counter()
    t_done = time.perf_counter()

    return {
        "total": _ms(t0, t_done),
        "bsl": _ms(t0, t_bsl),
        "locate": _ms(t_bsl, t_locate),
        "setup": _ms(t_locate, t_setup),
        "loglock": _ms(t_setup, t_loglock),
        "open": _ms(t_loglock, t_open),
        "write": _ms(t_open, t_write),
        "flush": _ms(t_write, t_flush),
        "close": _ms(t_flush, t_close),
        "release": _ms(t_close, t_done),
    }


def _replay_like_production(batch_id: str) -> int:
    with state.batch_state_lock:
        path = _locate_record(batch_id)
        if path is None:
            return 0
        with state.log_lock:
            data = path.read_text(encoding="utf-8")
    return len(data)


def _run_order_phase(label: str, n: int, replay_contended: bool) -> None:
    batch_id = f"{_BENCH_BATCH_ID}_{label}"
    samples: dict[str, list[float]] = {
        "total": [],
        "bsl": [],
        "locate": [],
        "setup": [],
        "loglock": [],
        "open": [],
        "write": [],
        "flush": [],
        "close": [],
        "release": [],
    }
    replayer: _Replayer | None = None

    try:
        _prepare_order_record(batch_id)
        if replay_contended:
            replayer = _Replayer(batch_id)
            replayer.start()

        t0 = time.perf_counter()
        for i in range(n):
            step = _append_like_production(batch_id, {
                "ts_ms": int(time.time() * 1000),
                "event": "bench",
                "seq": i,
                "cl_ord_id": f"bench-{i}",
                "payload": "x" * 160,
            })
            for k, v in step.items():
                samples[k].append(v)
        elapsed_ms = _ms(t0, time.perf_counter())
    except Exception:
        log.exception("disk_bench order_record phase=%s failed", label)
        return
    finally:
        replay_count = 0
        if replayer is not None:
            replay_count = replayer.stop_and_join()
        _cleanup_order_record(batch_id)

    log.info("disk_bench[%s_order_record]: n=%d elapsed=%.0fms replay_reads=%d | "
             "total %s | locate %s | open %s | flush %s | close %s | "
             "bsl %s | loglock %s | setup %s | write %s | release %s",
             label, n, elapsed_ms, replay_count,
             _fmt_stats(samples["total"]),
             _fmt_stats(samples["locate"]),
             _fmt_stats(samples["open"]),
             _fmt_stats(samples["flush"]),
             _fmt_stats(samples["close"]),
             _fmt_stats(samples["bsl"]),
             _fmt_stats(samples["loglock"]),
             _fmt_stats(samples["setup"]),
             _fmt_stats(samples["write"]),
             _fmt_stats(samples["release"]))


class _Replayer:
    """Background read_text loop shaped like `order_log.replay_record`."""

    def __init__(self, batch_id: str) -> None:
        self.batch_id = batch_id
        self.stop = threading.Event()
        self.iters = 0
        self.thread = threading.Thread(target=self._loop, name="bench-replayer", daemon=True)

    def _loop(self) -> None:
        while not self.stop.is_set():
            try:
                _replay_like_production(self.batch_id)
                self.iters += 1
            except Exception:
                log.exception("bench replayer iteration failed")
                return

    def start(self) -> None:
        self.thread.start()
        while self.iters == 0 and self.thread.is_alive():
            time.sleep(0.005)

    def stop_and_join(self) -> int:
        self.stop.set()
        self.thread.join(timeout=2)
        return self.iters


def bench_disk() -> None:
    _run_basic_phase("isolated", _BASIC_N, _PAYLOAD)
    _run_order_phase("isolated", _ORDER_N, replay_contended=False)
    _run_order_phase("replay_contended", _ORDER_N, replay_contended=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    config.ensure_dirs()
    bench_disk()
