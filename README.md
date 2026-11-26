# 多租户消息投递器：并发份额、空闲借用与两种计费模型

## 要解决的问题

不同租户的消息形态差异很大：

- 租户 B：消息量大，但单条处理只要十几毫秒；
- 租户 F：单条处理近百毫秒，还经常失败、退避、重试。

如果**按消息条数把并发平分**给各租户，F 会用少量慢消息长期占住分到的槽位，
B 的大量快消息反而排队——平分恰好惩罚了「消息多但处理快」的租户。

本实现用一个**全局许可池 + 租户配额（floor/weight/cap）+ 账单驱动的排队**
解决这个问题，并对比两种计费哲学。

## 模型

系统只有一个大小为 `C` 的许可池，一个许可 = 一个并发处理槽位。
每个租户有三个静态参数（`model.TenantSpec`）：

| 参数 | 含义 |
| --- | --- |
| `floor` | 保底底线。租户有消息且 `inflight < floor` 时，比所有拿满底线的租户**绝对优先**拿许可，保证延迟敏感流量不被洪泛挤死 |
| `weight` | 名义份额。系统满载时按 `C * weight / Σweight` 切并发；实际名义份额 `entitled = max(floor, share)` |
| `cap` | 硬上限。即使全局有空闲，单租户 inflight 也不得超过 cap |

### 借用是单一许可池上的涌现行为

不单独维护「谁借给谁」的账本：租户 inflight 超过自己的 `entitled` 时，
超出的槽位就是从别人的空闲份额里**借**的（`Dispatcher.borrowed()`）。

- **能借**：全局还有物理许可、租户未触 cap、且没有别的租户在 floor 以下；
- **会还**：被借用方重新有消息且 inflight 低于 floor 时，floor 优先级让它
  在下一个释放的许可上立刻拿回底线，借用人的 inflight 回落到名义份额内，
  `borrowed` 归零即完成归还。

### 调度选择（`Dispatcher._select_locked`）

1. 过滤出有排队消息且未触 cap 的租户；
2. 第一优先组：inflight 还低于 floor 的租户（延迟底线）；
3. 其余按调度计费器的 `debt / weight` 排序，账花得少的先跑，平局用队首 FIFO。

## 两种计费器（`meters.py`）

计费器实现同一套钩子，调度器用 `debt/weight` 当排队键，所以**怎么计费直接
决定谁先被调度**。每次运行可同时挂多个计费器：一个参与调度，其余被动记账，
因此同一次运行能拿到两种口径的账单。

| | ShareMeter（并发份额） | ConsumptionMeter（处理消耗） |
| --- | --- | --- |
| 计费对象 | 持有许可的每个槽位秒（授予→释放） | 真实处理尝试的处理时长 |
| 退避等待 | 保留许可模式下**计费**（占着坑位） | **不计费**（没在干活） |
| 失败重试 | 占用多久算多久 | 每次尝试重复计费 |
| 本质 | 资源预留/占用成本 | 实际消耗的算力 |

两种哲学的分界线是**退避时是否占坑**，由 `release_during_backoff` 控制：

- `True`（工作保守，默认）：失败退避前释放许可，槽位立刻给别人用，
  重试消息重新排队。两种账单几乎相等。
- `False`（保留许可）：退避期间仍占槽位。份额账持续累加，消耗账不动，
  差额就是退避等待——演示第 3 部分有逐消息对照和实测数字。

## 故障消费者的借用与归还

`workload.build_workload()` 构造的 F 是本地故障消费者：高失败率、随机退避、
含一条永远失败的毒消息（耗尽重试进死信）。演示第 4 部分打印 F 的完整事件时间线：

- `grant source=borrow`：inflight 超过名义份额 1，槽位借自 B/L 的空闲份额；
- `release_for_backoff`：工作保守模式下故障退避立即还许可，`borrowed_after` 同步下降；
- L 的突发到达且 inflight < floor=2 时，下一个许可 `source=floor` 优先给 L，
  F 的借用被收回；F 恢复后 `borrowed` 归零，完成归还。

## 朴素基线

`naive.NaiveEqualSplitDispatcher` 复用全部机制，只替换选择逻辑：
按当前非空队列的租户头数平分许可、纯 FIFO，不设 floor、不看账单。
饱和 + 保留许可场景下，它对延迟敏感租户 L 的 p95 延迟比账单调度高一个数量级
（见演示第 1 部分），直观展示「按条数平分」的危害。

## 运行

```bash
python -m multitenant_delivery.demo      # 端到端演示，约 5 秒
python -m unittest tests.test_dispatcher -v   # 12 个单元测试
```

仅用标准库（asyncio），无第三方依赖。负载用固定随机种子在提交前生成
（处理时长、失败序列、到达时刻全部写死），两种策略重放的是逐字节相同的负载，
策略间差异只能来自调度与计费本身。

## 代码地图

```
multitenant_delivery/
  model.py       消息/租户/事件/尝试记录的数据模型
  meters.py      ShareMeter 与 ConsumptionMeter
  consumers.py   脚本化消费者（快速/慢速/故障，运行期无随机）
  dispatcher.py  许可池、floor 优先、借用、两种退避模式、死信
  naive.py       按条数平分的朴素基线
  workload.py    可复现工作负载（常规故障场景 + 持续饱和场景）
  report.py      统计、账单、makespan、时间线表格
  demo.py        端到端演示
tests/
  test_dispatcher.py
```
