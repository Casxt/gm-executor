"""Git connector daemon: pulls a remote repo and copies new batches into pending/.

Read-only on the repo: pulls (clone / fetch + reset --hard); never adds, modifies,
deletes, or commits anything. The repo is the publisher's territory.

Atomic copy: write `<id>.json.tmp`, then `os.replace`. On NTFS within one volume
this is atomic, so the cycle's `glob("*.json")` never sees a partial file.

`known_batch_ids()` enumerates `pending/` first then the terminal dirs. The cycle
only ever moves files rightward (out of `pending/`), and `os.rename` is atomic, so
this order guarantees a moving file is caught in either its source or its destination.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

from . import config, state

log = logging.getLogger(__name__)

_ACTIVE_ORDER_DIRNAME = "active_order"
_PAT_RE = re.compile(r"https?://[^@\s]+@")


def _redact(text: str) -> str:
    """Strip `user:pat@` from any URL — git stderr can echo our PAT."""
    return _PAT_RE.sub("https://***@", text)


# ── git ───────────────────────────────────────────────────────────────

def _run_git(*args: str) -> None:
    proc = subprocess.run(
        ["git", *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, ["git", *args],
            output=_redact(proc.stdout or ""), stderr=_redact(proc.stderr or ""),
        )


def _sync_repo() -> None:
    """Clone on first run; otherwise fetch + reset --hard origin/<branch>."""
    branch = config.GIT_BRANCH
    if not (config.GIT_LOCAL_DIR / ".git").exists():
        config.GIT_LOCAL_DIR.parent.mkdir(parents=True, exist_ok=True)
        if config.GIT_LOCAL_DIR.exists():
            shutil.rmtree(config.GIT_LOCAL_DIR)
        _run_git("clone", "-b", branch, config.GIT_REPO_URL, str(config.GIT_LOCAL_DIR))
        return

    cwd = str(config.GIT_LOCAL_DIR)
    _run_git("-C", cwd, "fetch", "--prune", "origin")
    _run_git("-C", cwd, "reset", "--hard", f"origin/{branch}")


# ── snapshot ──────────────────────────────────────────────────────────

def _known_batch_ids() -> set[str]:
    """Basenames (minus .json) under pending/, finished/, expired/, failed/.

    Order matters: pending first, then terminal dirs (see CONNECTOR.md).
    """
    seen: set[str] = set()
    for d in (config.PENDING_DIR, config.FINISHED_DIR, config.EXPIRED_DIR, config.FAILED_DIR):
        if not d.exists():
            continue
        for path in d.glob("*.json"):
            seen.add(path.stem)
    return seen


# ── copy ──────────────────────────────────────────────────────────────

def _atomic_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def _peek_batch(path: Path) -> tuple[str, int]:
    """Cheap read of just `batch_id` and `expires_at`. Schema validation happens later in the cycle."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    return str(raw["batch_id"]), int(raw["expires_at"])


# ── loop ──────────────────────────────────────────────────────────────

def connector_loop() -> None:
    if not config.GIT_REPO_URL:
        log.warning("GMX_GIT_REPO_URL not set; connector exits")
        return
    if shutil.which("git") is None:
        log.error("git not on PATH; connector exits")
        return

    while not state.stop_event.is_set():
        try:
            _sync_repo()
        except subprocess.CalledProcessError as e:
            log.error("git sync rc=%d: %s", e.returncode, (e.stderr or "").strip()[:200])
        except Exception:
            log.exception("git sync failed")

        try:
            _import_pass()
        except Exception:
            log.exception("connector import pass failed")

        state.stop_event.wait(config.GIT_PULL_SECONDS)


def _import_pass() -> None:
    seen = _known_batch_ids()
    now  = int(time.time())
    src_dir = config.GIT_LOCAL_DIR / _ACTIVE_ORDER_DIRNAME
    if not src_dir.exists():
        return

    for path in src_dir.glob("*.json"):
        try:
            batch_id, expires_at = _peek_batch(path)
        except Exception:
            log.exception("connector: bad batch in repo: %s", path.name)
            continue

        if batch_id in seen:
            continue
        if now >= expires_at:
            continue                                          # already expired

        dst = config.PENDING_DIR / f"{batch_id}.json"
        try:
            _atomic_copy(path, dst)
            log.info("connector: imported %s", batch_id)
        except Exception:
            log.exception("connector: copy failed for %s", batch_id)


# ── lifecycle ─────────────────────────────────────────────────────────

def start() -> threading.Thread | None:
    if not config.GIT_REPO_URL:
        log.warning("GMX_GIT_REPO_URL not set; connector exits")
        return None
    t = threading.Thread(target=connector_loop, name="connector", daemon=True)
    t.start()
    state.worker_threads.append(t)
    return t
