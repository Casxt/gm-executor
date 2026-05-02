# Connector

A daemon thread inside the trading process. Pulls a git repo and copies new batches into `$GMX_ORDERS_DIR/pending/`. Started from `init()` alongside the cycle timer ([FLOW.md](./FLOW.md)).

```
algo  ──push──▶  git repo  ──pull──▶  connector  ──copy──▶  pending/
```

Two invariants:

* Read-only on the repo. Pulls only — never commits, modifies, or deletes.
* Only the connector writes into `pending/`; only the cycle moves out of it.

## Layout

```
$GMX_GIT_LOCAL_DIR/                # local clone, default ./git_orders
  active_order/<batch_id>.json     # publisher-owned: currently or about-to-be active

$GMX_ORDERS_DIR/
  pending/<batch_id>.json          # connector's only output
  finished/, expired/, failed/     # cycle moves files here
```

Publisher contract:

* writes batches only under `active_order/`,
* removes a file from `active_order/` once its `expires_at` has passed (housekeeping),
* never mutates `orders` (or anything else in `batch_id`'s hash) after the first commit. New idea ⇒ new id.
* `expires_at` is the one mutable field, and may only be **reduced** (shrink TTL, or kill by setting it `≤ now`). Extending is forbidden.

Connector-enforced piece: refuses bytes with `valid_at >= expires_at`.

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

            if doc.valid_at >= doc.expires_at:     continue   # schema-invalid; refuse + warn

            dst = os.path.join(ORDERS, "pending", f"{doc.batch_id}.json")
            with batch_state_lock:
                if is_terminal(doc.batch_id):      continue   # finalized; never resurrect
                existed = os.path.exists(dst)
                if not existed and unix_now() >= doc.expires_at:
                    continue                                  # first import + already expired ⇒ skip
                if existed and file_bytes_equal(path, dst): continue   # already mirrored
                atomic_copy(path, dst)                        # creates or overwrites in place

        stop_event.wait(PULL_SEC)
```

`atomic_copy(src, dst)` writes `dst + ".tmp"` then `os.replace(tmp, dst)`. NTFS-atomic — the cycle never sees a partial or torn file.

**Mirror, not import-once.** Skip rules:

1. `valid_at >= expires_at` ⇒ refuse + warn.
2. `batch_id` in a terminal dir ⇒ skip (finalized; never resurrect).
3. `dst` doesn't exist **and** `now >= expires_at` ⇒ skip (don't import a stale batch).
4. `dst` exists with identical bytes ⇒ no-op.

Otherwise atomic-overwrite. Rule (3) gates **first imports only** — once a batch is in `pending/`, every edit lands. A past-`expires_at` edit is the kill signal: the next cycle tick hits the expired branch, cancels open orders, moves to `expired/`.

**Lock.** `batch_state_lock` (reentrant) is held by the cycle for the whole `run_cycle` and by the connector per file around `is_terminal` + bytes-compare + `atomic_copy`. Cycle running ⇒ connector waits; connector mirroring ⇒ cycle's next tick waits. No mid-cycle byte swap, no phantom resurrection.

## Git

* **First run**: `git clone -b <GIT_BRANCH> <GIT_REPO> <GIT_DIR>`.
* **Each tick**: `git -C <GIT_DIR> fetch --prune origin` then `git -C <GIT_DIR> reset --hard origin/<GIT_BRANCH>`. The local clone is a read-only mirror; we never commit, so any divergence is corruption and gets discarded.
* Branch is pinned by `GMX_GIT_BRANCH` (default `main`). We never resolve `origin/HEAD`, so a remote default-branch change can't silently shift what we track.
* The PAT is embedded in the URL (`https://<token>@github.com/owner/repo.git`). The connector treats `GMX_GIT_REPO_URL` as a secret: never log it, never include it in errors. Log host + path only.

## Concurrency

* `state.batch_state_lock` (reentrant) serialises connector mirrors against cycle moves and against `order_log` path lookups. At any instant, a batch is observably in exactly one of `pending/` or a terminal dir.
* Lock order: `batch_state_lock` then `log_lock`. Never the reverse. Connector takes only `batch_state_lock`, so it can't deadlock against `append` / `replay_record`.
* The connector never touches `*.order_record.jsonl`. The record's binding is by `batch_id`, not by file content, so a re-mirror of the json side leaves the record untouched.

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
