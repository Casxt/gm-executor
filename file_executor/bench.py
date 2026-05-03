"""Disk benchmark for the orders dir, run once at init.

Two phases against `pending/` with fresh files (cold AV scan path, not
warm-cache overwrites):

1. **isolated** — no concurrent activity. Establishes the floor.
2. **contended** — a background thread appends to `.bench_writer.jsonl` in
   the same dir on a tight loop. Models the real cycle: while the cycle
   globs/reads/appends, the callback drain is appending to
   `<batch_id>.order_record.jsonl` for every status event. We've observed
   `glob("*.json") n=1` taking 4-6 seconds during reconcile on a 100GB-free
   NVMe with no AV — same op runs in 0.1ms isolated.

Reports median + max per op. Max >> median means bursty contention; the
contended row >> isolated row means concurrent writes in the dir are the
bottleneck (filesystem metadata lock, AV, or both).
"""

import logging
import threading
import time
from pathlib import Path

from . import config

log = logging.getLogger("file_executor.bench")


def _stats(samples: list[float]) -> tuple[float, float]:
    s = sorted(samples)
    return s[len(s) // 2], s[-1]


def _measure(probes: list[Path], payload: str) -> tuple[tuple[float, float], ...]:
    parent = probes[0].parent
    glob_s: list[float] = []
    create_s: list[float] = []
    append_s: list[float] = []
    read_s: list[float] = []

    for p in probes:
        t0 = time.perf_counter()
        list(parent.glob("*.json"))
        glob_s.append((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        with open(p, "w", encoding="utf-8") as f: f.write(payload); f.flush()
        create_s.append((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        with open(p, "a", encoding="utf-8") as f: f.write(payload); f.flush()
        append_s.append((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        with open(p, "r", encoding="utf-8") as f: f.read()
        read_s.append((time.perf_counter() - t0) * 1000)

    return _stats(glob_s), _stats(create_s), _stats(append_s), _stats(read_s)


def _run_phase(label: str, n: int, payload: str) -> None:
    probes = [config.PENDING_DIR / f".bench_probe_{label}_{i}" for i in range(n)]
    try:
        g, c, a, r = _measure(probes, payload)
        log.info("disk_bench[%s]: n=%d | glob med=%.1fms max=%.1fms | "
                 "create med=%.1fms max=%.1fms | append med=%.1fms max=%.1fms | "
                 "read med=%.1fms max=%.1fms",
                 label, n, g[0], g[1], c[0], c[1], a[0], a[1], r[0], r[1])
    except Exception:
        log.exception("disk_bench phase=%s failed", label)
    finally:
        for p in probes:
            try: p.unlink(missing_ok=True)
            except Exception: pass


class _Writer:
    """Background thread that does open(a)+write+flush+close in a loop —
    same shape as `order_log.append`. No sleep: the drain bursts statuses
    with no spacing during reconcile. We track iter count so the bench can
    report how busy the writer actually was during measurement."""

    def __init__(self, target: Path, payload: str) -> None:
        self.target = target
        self.payload = payload
        self.stop = threading.Event()
        self.iters = 0
        self.thread = threading.Thread(target=self._loop, name="bench-writer", daemon=True)

    def _loop(self) -> None:
        while not self.stop.is_set():
            try:
                with open(self.target, "a", encoding="utf-8") as f:
                    f.write(self.payload); f.flush()
                self.iters += 1
            except Exception:
                log.exception("bench writer iteration failed")
                return

    def start(self) -> None:
        self.thread.start()
        # Yield until the writer has cleared its first iteration, so the
        # measurement window starts with the writer already steady-state.
        while self.iters == 0 and self.thread.is_alive():
            time.sleep(0.005)

    def stop_and_join(self) -> int:
        self.stop.set()
        self.thread.join(timeout=2)
        return self.iters


def bench_disk() -> None:
    payload = ("x" * 200 + "\n") * 5
    n = 64                                              # ~50-100ms foreground; gives the writer a real window

    _run_phase("isolated", n, payload)

    writer = _Writer(config.PENDING_DIR / ".bench_writer.jsonl", payload)
    writer.start()
    try:
        t0 = time.perf_counter()
        _run_phase("contended", n, payload)
        elapsed_ms = (time.perf_counter() - t0) * 1000
    finally:
        iters = writer.stop_and_join()
        try: writer.target.unlink(missing_ok=True)
        except Exception: pass

    log.info("disk_bench: writer did %d appends during %.0fms contended phase "
             "(%.0f writes/s) — if contended >> isolated, same-dir writes "
             "are the cycle bottleneck", iters, elapsed_ms,
             iters * 1000 / elapsed_ms if elapsed_ms else 0)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    config.ensure_dirs()
    bench_disk()
