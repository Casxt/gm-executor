# Coding Style

Short rules. Follow them. Don't add more.

> **Python version: 3.13.13.** Pinned. Don't write code that would also need to run on older interpreters — use `match`, PEP 695 generics, `t-string`-style f-strings, builtin generic syntax (`list[int]`, `dict[str, X]`), `typing.Self`, etc. freely. CI installs **exactly** this version; if a feature was added later, it doesn't exist here yet.

## Dependencies

* **Standard library first.** `os`, `pathlib`, `json`, `glob`, `threading`, `subprocess`, `logging`, `time`, `datetime`, `dataclasses`, `typing`, `shutil` — these solve almost every problem this project has. Reach for them before anything else.
* **Allowed third-party**: `gm` (the SDK we depend on by definition) and `requests`. Anything else needs a clear justification in the PR.
* **No frameworks.** No `pydantic`, no `attrs`, no `click`, no `loguru`, no async runtimes. Plain functions, plain classes, plain `argparse` if a CLI is ever needed.
* **No "nice-to-have" deps.** If a builtin gets you 95% of the way there, ship the 95%.

## Type hints

* Every function signature has type hints — parameters and return.
* Module-level constants and dataclass fields are annotated.
* Use builtin generics directly (`list[X]`, `dict[K, V]`, `tuple[X, ...]`). Use `X | None` instead of `Optional[X]`, and `X | Y` instead of `Union[X, Y]`. PEP 695 type-parameter syntax (`def f[T](x: T) -> T:`) is preferred over `TypeVar` boilerplate.
* Don't annotate the obvious local: `x: int = 0` inside a function is noise. Annotate boundaries, not bodies.
* Accept that `gm.api` returns untyped objects. When passing them across our own boundaries, wrap in a small dataclass or `TypedDict` so downstream code is typed.

## Simplicity

* **Functions over classes.** Reach for a class only when state genuinely lives together. A function that takes 4 args is fine; a class with one method is not.
* **No premature abstraction.** Three similar lines beat a clever helper. Wait until the third caller before extracting.
* **Flat over nested.** Early `return` and `continue` instead of deep `if/else` trees.
* **Short functions.** If a function doesn't fit on a screen, split it — but only at a natural seam, not because of line count.
* **No silent except.** Catch a specific exception, log it, and decide what to do. `except Exception:` is allowed at the **top** of a daemon loop and nowhere else.
* **No comments that restate code.** If the *why* isn't obvious, write it; otherwise, the code is the doc.

## Naming

* `snake_case` for functions and variables. `PascalCase` for classes and dataclasses. `UPPER_SNAKE` for module-level constants.
* Names describe role, not type. `orders`, not `order_list`. `pending_dir`, not `pending_str`.
* Don't abbreviate domain words. `cl_ord_id` stays `cl_ord_id` — it's the SDK's name and matches our logs.

## Windows paths

* Build paths with `os.path.join` or `pathlib.Path`. Never f-string a path.
* Use raw strings or forward slashes for literal paths. `pathlib` accepts both and normalises.
* Filenames the SDK or git produces may have any case — never compare paths case-sensitively.

## GM SDK usage

Before calling **any** function from `gm.api`:

1. Check [`GM_SDK.md`](./GM_SDK.md). If the function and the parameters you're about to use are documented there, use the doc.
2. If the function is **not** in `GM_SDK.md`, or you're using parameters / return fields not covered there, **read the upstream docs first**: <https://www.myquant.cn/docs2/sdk/python/%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B.html> (start page) and the relevant section under *API介绍*. Then update `GM_SDK.md` with what you needed in the same PR.
3. Never guess a parameter default, a return-field name, or an enum value from memory. The SDK has surprises (e.g. `order_volume` has 11 params, not 6 — see the doc).

Treat `GM_SDK.md` as the project's curated subset of the SDK. If it's not there, it's not used; if it gets used, it goes there.

## Logging

* Use the stdlib `logging` module. One module-level `log = logging.getLogger(__name__)` per file.
* Log levels: `debug` for verbose tracing, `info` for normal-flow milestones, `warning` for recoverable surprises, `error` for things an operator should look at, `exception` only inside an `except` block.
* Never log the GM token, the git PAT, or the full git URL. Log host + path only.

## Concurrency

* `threading` is fine. No `asyncio` (the SDK is sync).
* Every shared-state mutation that crosses thread boundaries acquires a lock. Document the lock order at the top of the file (see [`FLOW.md`](./file_executor/docs/FLOW.md) for the existing one).
* **Worker threads live and die with the main thread.** Always set `thread.daemon = True`; that's the rule, not a safety net. The interpreter exits when main exits — workers go with it.
* Daemon threads get killed **abruptly**, so pair every worker with a `threading.Event` for cooperative shutdown:
  * the loop checks `stop_event.is_set()` at every iteration boundary, and uses `stop_event.wait(period)` instead of `time.sleep(period)` so a shutdown wakes it immediately;
  * on exit, main sets the event, then `thread.join(timeout=...)` to let the worker finish its current iteration cleanly. `daemon=True` is the fallback if join times out.
* Don't rely on `KeyboardInterrupt` reaching the worker — it doesn't. Signals are delivered to the main thread only.

## Errors

* Validate at boundaries (env vars, file parsing, SDK return values). Trust internal callers.
* Raise `ValueError` for bad input, `RuntimeError` for invariant breaks, `OSError`/`subprocess.CalledProcessError` for IO. Don't invent custom exceptions unless callers actually need to distinguish them.
* `assert` is for invariants we never expect to fire — not for input validation. Asserts can be stripped with `-O`.

## Tests

* Stdlib `unittest`. No `pytest`, no fixtures library.
* Test the boundary, not the internals. A test that mocks five things tests the mocks.
* If a thing is hard to test, it's probably the wrong shape — refactor before adding mocks.
