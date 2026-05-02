# Connector

A daemon thread inside the trading process. Pulls a git repo and copies new batches into `$GMX_ORDERS_DIR/pending/`. Started from `init()` alongside the cycle timer ([FLOW.md](./FLOW.md)).

```
algo  ──push──▶  git repo  ──pull──▶  connector  ──copy──▶  pending/
```

Two invariants:

* The connector is **read-only on the repo**. It pulls. It never adds, modifies, deletes, or commits anything. The repo's contents are entirely the publisher's responsibility.
* The connector is the **only** writer into `pending/`; the cycle is the only mover out of it. Disjoint roles, no shared lock needed.

## Layout

```
$GMX_GIT_LOCAL_DIR/                # local clone, default ./git_orders
  active_order/<batch_id>.json     # publisher-owned: currently or about-to-be active

$GMX_ORDERS_DIR/
  pending/<batch_id>.json          # connector's only output
  finished/, expired/, failed/     # cycle moves files here
```

Publisher contract (entirely owned by the algo side, not the connector):

* writes batches **only** under `active_order/`,
* removes a file from `active_order/` when the publisher considers it expired (housekeeping; the connector also filters by `expires_at` defensively),
* never mutates `orders` (or any field that contributes to `batch_id`) after the first commit. **`batch_id` is the immutability boundary, not the file** — a new idea always gets a new id.
* `expires_at` is the one mutable field, and **may only be reduced** (shrink TTL or kill outright by setting `expires_at ≤ now`). Extending a batch's life is forbidden — publish a new id instead.

The connector enforces one piece of this contract — it refuses to mirror bytes with `valid_at >= expires_at` (logged + skipped). Everything else is the publisher's responsibility.

## Loop

```Python
GIT_REPO   = os.environ.get("GMX_GIT_REPO_URL")
GIT_DIR    = os.environ.get("GMX_GIT_LOCAL_DIR", "./git_orders")
GIT_BRANCH = os.environ.get("GMX_GIT_BRANCH", "main")
PULL_SEC   = int(os.environ.get("GMX_GIT_PULL_SECONDS", "30"))
ORDERS     = os.environ.get("GMX_ORDERS_DIR", "./orders")

def connector_loop(stop_event):
    if not GIT_REPO:
        log.warning("GMX_GIT_REPO_URL not set; connector exits"); return
    if shutil.which("git") is None:
        log.error("git not on PATH; connector exits"); return

    sync_repo()                                          # clone or fetch+reset

    while not stop_event.is_set():
        try:    sync_repo()
        except Exception: log.exception("git sync failed")  # keep looping

        for path in glob(os.path.join(GIT_DIR, "active_order", "*.json")):
            try:    doc = parse_minimal(path)            # batch_id, valid_at, expires_at
            except Exception:
                log.exception("bad batch in repo: %s", path); continue

            if doc.valid_at >= doc.expires_at:     continue   # invalid; refuse + warn
            if unix_now() >= doc.expires_at:       continue   # already expired

            dst = os.path.join(ORDERS, "pending", f"{doc.batch_id}.json")
            with batch_state_lock:
                if is_terminal(doc.batch_id):      continue   # finalized; never resurrect
                if file_bytes_equal(path, dst):    continue   # already mirrored
                atomic_copy(path, dst)                        # creates or overwrites in place

        stop_event.wait(PULL_SEC)
```

`atomic_copy(src, dst)` writes `dst + ".tmp"` then `os.replace(tmp, dst)`. On NTFS within the same volume the rename is atomic — the cycle's `glob("*.json")` never sees a partial file, and a torn read of an in-flight overwrite is impossible.

**Mirror, not import-once.** Each pass re-syncs publisher content into `pending/`. Three skip rules: (1) `valid_at >= expires_at` ⇒ refuse + warn; (2) `batch_id` already in a terminal dir (`finished/`, `expired/`, `failed/`) ⇒ skip; (3) `pending/<batch_id>.json` already has identical bytes ⇒ no-op. Otherwise atomic-overwrite. This is what makes `expires_at` edits land: same `batch_id`, same filename, same identity — only the bytes differ, and the next cycle reads the new value through the normal `parse_and_validate` path.

**Race vs. cycle, closed by the lock.** The cycle holds `batch_state_lock` for the *entire* `run_cycle` (reentrant — `append` / `move_pair` re-acquire freely). The connector takes the same lock per file around `is_terminal` + bytes-compare + `atomic_copy`. So:

* if a cycle is running, the connector waits — no edit can land mid-reconcile, the cycle's view of `pending/` is frozen end-to-end;
* if the connector is mirroring, the cycle's next tick waits — the connector finishes, releases, the cycle then sees the new bytes from a clean start.

There is no "phantom resurrection" window and no mid-cycle byte swap. The cycle's critical section is microseconds-to-milliseconds (broker calls already returned by the time the lock is held — `_broker_snapshot` runs inside the lock, so freshness is bounded by lock-acquire time, not by lock duration on the connector's side).

**Why "shrink-only" matters.** Because `new ≤ old`, any edit on a "just expired" batch (`old ≤ now`, cycle hasn't moved yet) also satisfies `new ≤ now` — both interleavings (cycle moves first, or connector mirrors first then cycle moves) terminate in `expired/`. The outcome is unambiguous regardless of timing.

## Git

* **First run**: `git clone -b <GIT_BRANCH> <GIT_REPO> <GIT_DIR>`.
* **Each tick**: `git -C <GIT_DIR> fetch --prune origin` then `git -C <GIT_DIR> reset --hard origin/<GIT_BRANCH>`. The local clone is a read-only mirror; we never commit, so any divergence is corruption and gets discarded.
* Branch is pinned by `GMX_GIT_BRANCH` (default `main`). We never resolve `origin/HEAD`, so a remote default-branch change can't silently shift what we track.
* The PAT is embedded in the URL (`https://<token>@github.com/owner/repo.git`). The connector treats `GMX_GIT_REPO_URL` as a secret: never log it, never include it in errors. Log host + path only.

## Concurrency

* Connector and cycle share `$GMX_ORDERS_DIR`. Connector **creates or overwrites** under `pending/`; cycle **moves out** of `pending/` into a terminal dir.
* `state.batch_state_lock` (reentrant) serialises the two. The cycle holds it for the whole `run_cycle`; the connector holds it per file around `is_terminal(batch_id)` + `atomic_copy(...)`; `order_log.move_pair` / `move_invalid` re-enter it from the cycle thread. Result: a batch is, at any instant, observably in exactly one of `pending/` or a terminal dir, and no connector edit can change `pending/<id>.json` while a cycle is reconciling against it.
* In-place overwrite of `pending/<id>.json` is safe even without the lock thanks to `os.replace` atomicity — the cycle's mid-tick read sees either the old or the new bytes, never a torn mix. The lock exists for the cross-directory transition, not for byte-level coherence.
* The connector never touches `*.order_record.jsonl`. Those are created by the cycle on first append, and a re-mirror of the json side does not touch the record side — the order_record's binding is by `batch_id`, not by file content.
* Lock order convention: `batch_state_lock` then `log_lock`, never the reverse. The connector only takes `batch_state_lock`, so it cannot deadlock against `append`/`replay_record`.

## Config (env)

| var                     | required | default        | meaning                                                                       |
| ----------------------- | -------- | -------------- | ----------------------------------------------------------------------------- |
| `GMX_GIT_REPO_URL`      | **yes**  | —              | HTTPS git URL with PAT embedded. Missing ⇒ warn + connector exits (process keeps running). |
| `GMX_GIT_LOCAL_DIR`     | no       | `./git_orders` | local clone path. Must NOT live inside `$GMX_ORDERS_DIR`.                     |
| `GMX_GIT_BRANCH`        | no       | `main`         | branch tracked by clone + fetch/reset. Pinned — `origin/HEAD` is never used.  |
| `GMX_GIT_PULL_SECONDS`  | no       | `30`           | seconds between pulls.                                                        |

## Failure handling

| event                              | action                                                                            |
| ---------------------------------- | --------------------------------------------------------------------------------- |
| `GMX_GIT_REPO_URL` missing         | log warn, connector returns. Trading process continues without new batches.       |
| `git` not on PATH                  | log error, connector returns.                                                     |
| network / auth / pull error        | log, sleep `PULL_SEC`, retry. Never crash the trading process.                    |
| corrupt JSON under `active_order/` | log per file, skip that file, continue with the rest.                             |
| no `active_order/` dir in repo     | treat as empty (no copies this tick).                                             |
| copy fails (disk full, perms)      | log per file, leave the `.tmp` for next tick to overwrite, continue.              |

## Pitfalls

* **Windows paths** — always build with `os.path.join`; never f-string a path that mixes backslashes from `os.getcwd()` and forward slashes from a constant.
* **PAT on disk** — `git clone` writes the URL (with PAT) into `<GIT_DIR>/.git/config`. ACL the directory to the service account only.
* **Don't nest dirs** — `$GMX_GIT_LOCAL_DIR` must be outside `$GMX_ORDERS_DIR`. Nesting would put `.git/` and `active_order/` under the cycle's scan root.
* **One executor per orders dir** — two trading processes pointing at the same `$GMX_ORDERS_DIR` would race on copies. There is exactly one executor per orders dir; enforce by deployment.
* **Clock skew** — `expires_at` is a unix timestamp from the publisher. Both hosts must run NTP. Skew > a few seconds will quietly let expired batches through (or hide live ones).
