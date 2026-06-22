# Design — 减仓可用量校验 + 订单收尾报表与通知

Date: 2026-06-22
Scope: `file_executor/` (cycle.py, schema unchanged), 2 new modules, docs.

---

## 1. 背景与事故复盘（2026-06-22 线上 log）

两个症状，都源于「broker 真实可交易量 ≠ 我们 diff 出来的量」，而当前 cycle 没有
任何反馈通道，错误被埋进 INFO 日志并无限重试。

### 1.1 603836 —— 10 转 4 送股，未到账股导致 sell 被拒并死循环

- 公司行为：10 股转 4 股。`get_position()` 返回 `volume = 980`
  （700 已到账 + 280 送股未到账）。
- 当前 `_reconcile` 只读 `p.volume`，`diff = target(0) − held(980) = −980`，
  提交 sell 980。
- broker 实际可卖只有 700（未到账的 280 不可卖），整单被拒。被拒后持仓仍是 980，
  下一 cycle 再算出 −980，再提交 980 …… 无限重拒循环，直到 batch 过期。

  log 证据（同一个 980 反复提交）：
  ```
  8524  09:31:26  submit ... symbol=SHSE.603836 vol=980 side=sell
  8669  09:32:36  submit ... symbol=SHSE.603836 vol=980 side=sell
  8709  09:34:13  submit ... symbol=SHSE.603836 vol=980 side=sell
  8762  09:36:01  submit ... symbol=SHSE.603836 vol=980 side=sell
  8846 / 8893 / 8938 / 8983 / 9027 ...  全是 vol=980
  ```

- 正确行为：此刻应按 **700** 提交（当日可卖），剩下 280 等次日到账（届时
  batch 多半已过期，由报表标记「未对齐」）。

### 1.2 603137 —— 停牌，订单永远挂着，最终变「foreign」

- 停牌股可卖量可能仍是全量（可用量不为 0），所以 **available 上限并不能修复
  603137**。委托提交后既不成交也不被拒，一直挂在 `get_unfinished_orders()`：
  ```
  8661  09:32:36  waiting on own orders for SHSE.603137 (1 unfinished)
  8754  09:36:01  foreign order on SHSE.603137; skipping   ← batch 切换后变 foreign
  ```
- 这类「永不成交」只能靠 **收尾报表** 暴露，不能靠下单逻辑修复。

### 结论 / 任务拆分
- **Part 1（下单侧）**：减仓时用 broker 的「可用量」给 sell 封顶 → 修复 603836 这类
  「数量超出可卖」的拒单死循环。
- **Part 2（观测侧）**：每个 order(batch) 执行结束（matched / expired）时，基于
  **本 cycle 已有的持仓快照** 生成报表，重点高亮「未对齐」的仓位，经一个抽象的
  `notify()` 发出（先 stub，之后接 Discord）。两个事故都会被 Part 2 暴露。

---

## 2. SDK 事实核对（可用量字段）

`cycle.py` 用的 `get_position`（`gm.api.query.get_position`，无参版本）返回的是
**`list[DictLikeObject]`**（`gm/utils.py:284`，`dict` 子类，支持属性访问，这就是现在
`p.symbol / p.side / p.volume` 能работать的原因），由
`protobuf_to_dict(..., including_default_value_fields=True)` 构造 ——
**所有字段恒存在**，缺省为 0，因此读 `p.available` 不会 `AttributeError`。

`Position` proto 字段（`gm/pb/account_pb2.py`，与官方
[数据结构 · Position 持仓对象] 一致）：

| 字段 | proto # | 含义 |
| --- | --- | --- |
| `volume` | 7 | 总持仓（昨仓 + 今仓） |
| `volume_today` | 8 | 今仓 |
| `order_frozen` | 14 | 挂单冻结量 |
| `order_frozen_today` | 15 | 今仓挂单冻结 |
| **`available`** | 16 | **可平/可用量**：已考虑 A 股 T+1 锁定与挂单冻结 → 当前真正可卖 |
| `available_today` | 17 | 今仓可用 |
| `available_now` | 28 | 实时可平（部分券商/品种用） |

**选型**：减仓封顶用 **`available`**。理由：它是 broker 口径的「可卖」，对 T+1 与
未到账送股天然正确（603836 场景下应 = 700）。

> 不确定性兜底：线上首次部署后，Part 2 报表会同时打印
> `volume / available / available_today / available_now`，用一两天真实数据确认
> `available == 700` 即可；若发现某券商用的是 `available_now`，改一行常量即可，不动结构。

下单本身仍用 `order_volume`（`GM_SDK.md §Trading`），不改签名。

---

## 3. Part 1 — 减仓按可用量封顶

### 3.1 快照结构升级
当前：
```python
positions[(p.symbol, int(p.side))] = int(p.volume)        # 只有 volume
```
改为同时携带 available。引入轻量值对象（`models.py`）：
```python
@dataclass(frozen=True)
class PositionView:
    volume: int
    available: int
```
`_broker_snapshot` 返回 `dict[(symbol, side), PositionView]`。
取值统一走 helper，避免到处写 `.get(..., 0)`：
```python
def _pos(positions, symbol, side) -> PositionView:
    return positions.get((symbol, int(side)), PositionView(0, 0))
```

### 3.2 reconcile 封顶逻辑
`_reconcile` 中：
```python
pv   = _pos(positions, order.symbol, PositionSide_Long)
diff = order.target - pv.volume
if diff == 0:
    continue

if diff < 0:                                   # 减仓
    want = -diff
    submit_vol = min(want, pv.available)       # ← 封顶
    if submit_vol <= 0:
        log.warning("sell capped to 0: %s want=%d available=%d held=%d "
                    "(suspended / unsettled / all-frozen); skipping",
                    order.symbol, want, pv.available, pv.volume)
        continue
    if submit_vol < want:
        log.warning("sell capped: %s want=%d -> %d (available=%d held=%d)",
                    order.symbol, want, submit_vol, pv.available, pv.volume)
    _submit(session, ..., diff=-submit_vol, ...)
else:                                           # 加仓：不动（受现金约束，不在本次范围）
    _submit(session, ..., diff=diff, ...)
```
要点：
- 封顶只作用于 **sell**。buy 受可用现金限制，与本任务无关，保持原样。
- `available == 0`（停牌全锁 / 未到账 / 全部挂单冻结）→ 跳过本单，打 WARNING
  （WARNING 会经现有 Feishu relay 外发，`remote_log.py`）。
- `submit` 的 `submit` 日志行里 `volume` 自然变成封顶后的值，order_record 一致。

### 3.3 对 603836 死循环的修复效果
- cycle N：held 980 / available 700 → 提交 sell **700**（不再 980 整单被拒）。
- 700 成交后 held 变 280 / available 0 → 下一 cycle want=280、封顶为 0 → 跳过，
  **不再重复下单**。
- batch 因 target(0) ≠ held(280) 永不 matched → 到期走 expired，由 Part 2 报表标记
  「未对齐：280 未到账」。

### 3.4 对 matched 的影响（不改 matched 语义）
`_matched` 仍用 broker 真值 `volume == target`，**不改**。封顶导致的「卖不到位」
会让 batch 卡到过期 —— 这是预期行为，最终由报表兜住。不要为「不可卖差额」自动
判定完成（用户只要求报表，不要求自动收尾）。

---

## 4. Part 2 — 订单收尾报表 + 通知抽象

### 4.1 触发点（order/batch「执行结束」=两条且仅两条路径）
```
run_cycle:
  matched  → order_log.move_pair(doc.batch_id, FINISHED_DIR)   # cycle.py:110-111
  expired  → order_log.move_pair(doc.batch_id, EXPIRED_DIR)    # _pass_one, cycle.py:138-139
```
在这两处 **move 之前** 各调用一次 `report.emit_and_notify(doc, positions, unfinished, outcome)`。

### 4.2 「确认仓位状态必须很快」——核心约束如何满足
用户担心：多个 order 连续执行时，确认仓位必须快，否则被下一个订单影响。设计上
**用本 cycle 已经取好的快照，绝不在收尾时再发一次 `get_position()`**：

1. `run_cycle` 一进来就 `positions, unfinished = _broker_snapshot()`（cycle.py:91），
   全程持有 `batch_state_lock`。matched / expired 判定都发生在同一个 cycle 内、
   同一份快照之上。报表直接复用这份 `positions`、`unfinished` + 该 batch 的
   `order_record`（replay）。**纯内存计算，零额外 broker 往返，微秒级**。
2. 不会被「下一个订单」影响：
   - `_has_overlap` 不变式保证同一时刻只有一个 active batch；
   - 单 `cycle-worker` 串行执行，连续 batch 分属不同 cycle，各自带自己的快照；
   - 因此「这个 batch 结束瞬间的仓位」就是本 cycle 快照，天然隔离。
3. 通知本身可能慢（网络）→ `notify()` **只入队/异步、立即返回、绝不抛异常**
   （复刻 `remote_log.py` 的非阻塞子进程/队列范式），cycle 不被网络阻塞。

> 关键决定：matched 这条路径上，`_matched` 用的是 **cycle 起始快照**（提交之前的
> 持仓），所以只有在「上一轮提交已成交、本轮快照已反映」时才会判定 matched —— 即
> 宣布 matched 的那一刻，快照里每个 order 都已 held==target，报表显示「全对齐」。
> expired 路径用到期那一 cycle 的快照，反映最终真实状态。两条路径都无需补查。

### 4.3 需要把 `positions` 传进 `_pass_one`
当前 `_pass_one(now, unfinished)` 没拿到 positions（它在 `_broker_snapshot` 之后调用，
positions 已可用）。改签名为 `_pass_one(now, positions, unfinished)`，让 expired
分支能生成报表。改动局部、无副作用。

### 4.4 报表内容（重点突出「未对齐」）
按 batch 汇总，每个 order 一行，数据来源 = `doc.orders` × `positions` 快照 ×
`unfinished` × `order_record` replay：

| 列 | 来源 | 说明 |
| --- | --- | --- |
| symbol / order_id | doc | |
| target | doc | 目标仓 |
| held | positions.volume | 最终持仓 |
| available | positions.available | 当前可卖（诊断字段选型用） |
| diff | target − held | 0 = 对齐 |
| aligned | diff == 0 | |
| live_own | unfinished 里属于本 batch 的单数 | 仍挂在场内 |
| capped | 本 cycle 是否发生过 sell 封顶 | 卖不到位 |
| outcome | "matched" / "expired" | 终态 |
| reason_hint | 见下 | 仅未对齐时给 |

`reason_hint` 推断（给运维一眼定位）：
- `held > target` 且 `available < held−target` → `"under-sellable: 未到账/T+1/冻结"`（603836）
- 有本 batch 的 `live_own` 未成交且 outcome=expired → `"never-filled: 疑似停牌/流动性"`（603137）
- `unfinished` 里有 foreign → `"foreign-order: 需人工"`
- 其余 → `"residual-mismatch"`

报表结构（dataclass，便于之后序列化给 Discord）：
```python
@dataclass(frozen=True)
class OrderReportRow:
    order_id: str; symbol: str
    target: int; held: int; available: int; diff: int
    aligned: bool; live_own: int; capped: bool; reason_hint: str | None

@dataclass(frozen=True)
class BatchReport:
    batch_id: str; outcome: str            # "matched" | "expired"
    rows: list[OrderReportRow]
    @property
    def unaligned(self) -> list[OrderReportRow]: ...
    @property
    def all_aligned(self) -> bool: ...
```

发送策略：
- **matched 且全对齐** → INFO 级一行摘要（不打扰）。
- **任意未对齐 / expired** → WARNING 级，正文重点列出 unaligned 行
  （symbol / target / held / available / reason_hint）。WARNING 也会经现有 Feishu
  relay 外发，等于双通道。

### 4.5 通知抽象（`notify.py`，先 stub）
```python
# file_executor/notify.py
def notify(report: "BatchReport") -> None:
    """非阻塞、永不抛异常。当前实现：格式化后按等级写 logger
    （未对齐 -> WARNING，经 Feishu relay 外发；全对齐 -> INFO）。
    之后接 Discord：复刻 remote_log 的子进程/队列范式，POST webhook。"""
```
- 现在：纯 logging sink（零新依赖，立刻可用，且借道 Feishu）。
- 之后：加 `GMX_DISCORD_WEBHOOKS` env（`config.py`），起一个
  `discord_relay` 子进程（照抄 `feishu_relay.py` + `remote_log.py`），`notify()`
  入队即返回。**接口不变，只换 sink**。

### 4.6 `report.py` 入口
```python
def build(doc, positions, unfinished, outcome, batch_id) -> BatchReport: ...
def emit_and_notify(doc, positions, unfinished, outcome) -> None:
    try:
        rep = build(...)
        notify(rep)
    except Exception:
        log.exception("report failed for %s; non-fatal", doc.batch_id)
```
`emit_and_notify` 在 `batch_state_lock` 内被调用（cycle 持锁），但只做内存计算 +
入队，无锁竞争、无 IO 阻塞。失败完全不影响下单主流程。

---

## 5. 改动清单

| 文件 | 改动 |
| --- | --- |
| `models.py` | + `PositionView`（volume, available） |
| `cycle.py` | `_broker_snapshot` 带 available；`_pos` helper；`_reconcile` sell 封顶 + WARNING；`_pass_one(now, positions, unfinished)`；matched/expired 两处调用 `report.emit_and_notify` |
| `report.py`（新） | `BatchReport` / `OrderReportRow` / `build` / `emit_and_notify` |
| `notify.py`（新） | `notify()` 抽象，先 logging sink |
| `config.py` | （预留）`GMX_DISCORD_WEBHOOKS` 占位，本期不接 |
| `docs/FLOW.md`,`docs/ORDER_RECORD.md` | 补充封顶规则与收尾报表触发点 |

不动：`schema.py`/批次格式（target 仍是「目标仓位」语义，封顶是执行细节）、
`callbacks.py`、锁模型、session 写盘机制。

---

## 6. 风险与边界

1. **available 字段口径**：选 `available`；若线上数据表明应为 `available_now`，
   改 `_broker_snapshot` 一行。报表打印四个量做佐证，首日即可证伪。
2. **封顶后永不 matched**：预期；靠 expired + 报表收口，不自动完成。
3. **停牌（603137）封顶无效**：Part 1 不负责，Part 2 报表负责暴露
   （`reason_hint=never-filled`）。
4. **报表必须在 move 之前算**：move 之后 `positions` 快照不变（内存），其实先后皆可，
   但放 move 之前更直观；务必在 `batch_state_lock` 内、用本 cycle 快照。
5. **notify 绝不阻塞/抛异常**：任何通知失败都吞掉并本地 log，下单主流程零影响。
6. **buy 不封顶**：现金不足由 broker 拒单 + 报表体现，超出本任务范围。

---

## 7. 验收

- 构造 held=980/available=700/target=0：单 cycle 提交 sell **700**（非 980），
  700 成交后不再重复下单；batch 过期时报表出现一行
  `603836 target=0 held=280 available=0 reason=under-sellable`。
- 构造停牌 symbol（available 不为 0 但不成交）：batch 过期，报表
  `reason=never-filled`，并经 WARNING/Feishu 外发。
- 全部对齐的正常 batch：matched 时仅 INFO 摘要，不产生 WARNING 噪音。
- 收尾路径无新增 `get_position()` 调用（grep 确认），cycle 耗时不受通知影响。
