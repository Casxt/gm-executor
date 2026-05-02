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
* never mutates a `<batch_id>.json` after the first commit. **`batch_id` is immutable** — a new idea always gets a new id.

The connector trusts this contract and never enforces or repairs it.

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

        seen = known_batch_ids()                         # snapshot of all 4 subdirs
        for path in glob(os.path.join(GIT_DIR, "active_order", "*.json")):
            try:    doc = parse_minimal(path)            # batch_id, expires_at
            except Exception:
                log.exception("bad batch in repo: %s", path); continue

            if doc.batch_id in seen:               continue
            if unix_now() >= doc.expires_at:       continue   # already expired
            atomic_copy(path, os.path.join(ORDERS, "pending", f"{doc.batch_id}.json"))

        stop_event.wait(PULL_SEC)
```

`atomic_copy(src, dst)` writes `dst + ".tmp"` then `os.replace(tmp, dst)`. On NTFS within the same volume the rename is atomic — the cycle's `glob("*.json")` never sees a partial file.

`known_batch_ids()` returns the set of basenames (minus `.json`) under the four subdirs. Anything in the set is skipped — already known to the executor in some state.

**Enumeration order matters: `pending/` first, then `finished/`, `expired/`, `failed/`.** The cycle only moves files **rightward** (out of `pending/` into a terminal dir, never the reverse), and `os.rename` on NTFS is atomic — at any instant the file is in exactly one directory. Given those two facts, this order makes the snapshot race-free:

* if the iterator sees `X.json` in `pending/`, done;
* if it doesn't, `X` was already moved out before `pending/` was scanned, so it's sitting in a terminal dir that we haven't scanned yet, and a later iteration will pick it up;
* if the move happens **during** the snapshot (between the `pending/` and `finished/` iterations), the `pending/` scan still saw `X` because the rename hadn't happened when it ran.

Reverse the order (terminal dirs first, `pending/` last) and the snapshot can miss a file: the cycle moves `X` from `pending/` to `finished/` after `finished/` has been scanned but before `pending/` is, and `X` ends up in neither set.

## Git

* **First run**: `git clone -b <GIT_BRANCH> <GIT_REPO> <GIT_DIR>`.
* **Each tick**: `git -C <GIT_DIR> fetch --prune origin` then `git -C <GIT_DIR> reset --hard origin/<GIT_BRANCH>`. The local clone is a read-only mirror; we never commit, so any divergence is corruption and gets discarded.
* Branch is pinned by `GMX_GIT_BRANCH` (default `main`). We never resolve `origin/HEAD`, so a remote default-branch change can't silently shift what we track.
* The PAT is embedded in the URL (`https://<token>@github.com/owner/repo.git`). The connector treats `GMX_GIT_REPO_URL` as a secret: never log it, never include it in errors. Log host + path only.

## Concurrency

* Connector and cycle share `$GMX_ORDERS_DIR` but never the same operation: connector **creates** under `pending/`, cycle **moves out** of `pending/`. Both rely on `os.replace` / `os.rename` atomicity on NTFS. No lock between them is needed.
* Snapshot correctness depends entirely on the enumeration order in `known_batch_ids()` (see above). The cycle only moves rightward, the connector enumerates rightward — so any concurrent move during the snapshot is guaranteed to be caught in either the source dir (if scanned before the move) or the destination dir (if scanned after).
* Even if a stale `seen` snapshot somehow let a known `batch_id` re-enter `pending/`, the cycle would re-reconcile against the broker, find positions already satisfied, and move it back to `finished/` — wasteful but not unsafe. The order rule above closes this gap in practice; this is the fallback.
* The connector never touches `*.order_record.jsonl`. Those are created by the cycle on first append.

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
