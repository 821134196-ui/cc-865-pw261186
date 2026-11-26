"""端到端演示。

运行：  python -m multitenant_delivery.demo

四个部分：
1. 持续饱和 + 保留许可：朴素条数平分 vs 份额计费 vs 消耗计费——
   慢租户占住坑位时，按条数平分如何拖死消息多但单条快的租户；
2. 工作保守模式下同一负载的三种调度（故障退避即释放许可）；
3. 故障租户 F 的份额账与消耗账逐消息对比，以及保留模式的实测差额；
4. F 的借用→被 floor 抢占→归还时间线。
"""

from __future__ import annotations

import asyncio

from .dispatcher import Dispatcher
from .meters import ConsumptionMeter, ShareMeter
from .model import Message, MessageStatus
from .naive import NaiveEqualSplitDispatcher
from .report import (
    comparison_report,
    fmt_table,
    grant_preemption_trace,
    tenant_stats,
    tenant_timeline,
)
from .workload import (
    TENANT_SPECS,
    build_consumers,
    build_saturated_workload,
    build_workload,
)

TOTAL_PERMITS = 8


async def _drive(d: Dispatcher, timed_messages: list[tuple[float, Message]]) -> Dispatcher:
    runner = asyncio.create_task(d.run())
    base = None
    loop = asyncio.get_event_loop()
    for arrival, msg in timed_messages:
        if base is None:
            base = loop.time()
        delay = base + arrival - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        await d.submit(msg)
    await d.join()
    await d.aclose()
    runner.cancel()
    return d


def _make_dispatcher(scheduler_meter, *, release_during_backoff: bool) -> Dispatcher:
    consumers = build_consumers(asyncio.sleep)
    return Dispatcher(
        TENANT_SPECS,
        consumers,
        total_permits=TOTAL_PERMITS,
        scheduler_meter=scheduler_meter,
        extra_meters=(
            ConsumptionMeter() if scheduler_meter.name == "share" else ShareMeter(),
        ),
        release_during_backoff=release_during_backoff,
    )


async def run_once(timed_messages, scheduler_meter, *, release_during_backoff=True) -> Dispatcher:
    return await _drive(
        _make_dispatcher(scheduler_meter, release_during_backoff=release_during_backoff),
        timed_messages,
    )


async def run_naive(timed_messages, *, release_during_backoff=False) -> Dispatcher:
    consumers = build_consumers(asyncio.sleep)
    d = NaiveEqualSplitDispatcher(
        TENANT_SPECS,
        consumers,
        total_permits=TOTAL_PERMITS,
        release_during_backoff=release_during_backoff,
    )
    return await _drive(d, timed_messages)


def header() -> None:
    print("=" * 86)
    print("多租户投递器演示  总许可 C =", TOTAL_PERMITS)
    print(
        "租户规格： L 保底%s/权重%s/上限%s（延迟敏感）  B 保底%s/权重%s/上限%s  F 保底%s/权重%s/上限%s（故障）"
        % tuple(x for s in TENANT_SPECS for x in (s.floor, s.weight, s.cap))
    )
    wsum = sum(s.weight for s in TENANT_SPECS)
    print(
        "名义份额 C*w/Σw： L=%d  B=%d  F=%d（inflight 超出名义份额的槽位即借用）"
        % tuple(TOTAL_PERMITS * s.weight // wsum for s in TENANT_SPECS)
    )
    print("=" * 86)


async def main() -> None:
    header()

    # ==================== 1. 饱和 + 保留许可：策略差异最大的场景
    print("\n【1】持续饱和场景，保留许可退避（慢租户失败时坑位不释放）\n")
    saturated = build_saturated_workload()
    runs_retained = {
        "朴素:条数平分": await run_naive(saturated, release_during_backoff=False),
        "份额计费调度": await run_once(
            build_saturated_workload(), ShareMeter(), release_during_backoff=False
        ),
        "消耗计费调度": await run_once(
            build_saturated_workload(), ConsumptionMeter(), release_during_backoff=False
        ),
    }
    print(comparison_report(runs_retained))
    n = tenant_stats(runs_retained["朴素:条数平分"])
    s = tenant_stats(runs_retained["份额计费调度"])
    c = tenant_stats(runs_retained["消耗计费调度"])
    print("\n解读：")
    print(
        "  - 总完工时长由最慢的 F 主导，三种调度接近；真正拉开差距的是延迟底线："
        f"L 的 p95 朴素 {n['L']['p95_latency']*1000:.0f}ms vs 份额 {s['L']['p95_latency']*1000:.0f}ms"
        f" / 消耗 {c['L']['p95_latency']*1000:.0f}ms。朴素平分没有 floor 概念，"
        "F 占着 3 个坑位退避时，L 的消息只能跟 B 抢剩下的槽位。"
    )
    print(
        "  - 朴素方案按活跃租户头数把 8 槽位平分（≈3/3/2），F 的慢消息+退避把分到的"
        "槽位长期占死，却仍持续占用与 B 等量的并发——这就是「按消息条数平分」的根本"
        "问题：它按「谁有消息」分资源，而不是按「谁真正在消耗」分资源。"
    )
    print(
        "  - 账单调度按 debt/weight 排队，且 floor 绝对优先：L 的 inflight 一旦低于 2"
        " 立刻插队拿回底线；B 单条账单低、完成快，在 F 占坑的间隙能持续赢回槽位。"
    )

    # ==================== 2. 工作保守模式
    print("\n" + "=" * 86)
    print("【2】同一饱和负载，工作保守退避（故障退避立即释放许可，槽位可被借用）")
    print("=" * 86)
    runs_wc = {
        "朴素:条数平分": await run_naive(build_saturated_workload(), release_during_backoff=True),
        "份额计费调度": await run_once(build_saturated_workload(), ShareMeter()),
        "消耗计费调度": await run_once(build_saturated_workload(), ConsumptionMeter()),
    }
    print(comparison_report(runs_wc))
    print(
        "\n  工作保守模式下 F 退避就还槽位，三种调度差距明显收窄——这正说明"
        "「退避是否占坑」比排队规则更先决定隔离效果，份额/消耗之争主要发生在占坑期间。"
    )

    # ==================== 3. 两种账单逐消息对比
    print("\n" + "=" * 86)
    print("【3】故障租户 F：份额账 vs 消耗账（工作保守负载，份额调度那次运行）")
    print("=" * 86)
    share_d = runs_wc["份额计费调度"]
    headers = ["消息", "尝试", "处理秒", "退避秒", "结果", "份额账(保留口径)", "消耗账"]
    rows = []
    share_total = cons_total = 0.0
    for m in share_d.messages:
        if m.tenant != "F":
            continue
        proc = m.processing_time
        backoff = sum(r.backoff for r in m.history)
        result = "ACK" if m.status == MessageStatus.ACKED else "DLQ"
        share_total += proc + backoff
        cons_total += proc
        rows.append(
            [m.msg_id, m.attempts, f"{proc:.4f}", f"{backoff:.4f}", result,
             f"{proc + backoff:.4f}", f"{proc:.4f}"]
        )
    print(fmt_table(rows, headers))
    print(
        f"\n  保留口径合计：份额账 {share_total:.4f}s vs 消耗账 {cons_total:.4f}s，"
        f"差额 {share_total - cons_total:.4f}s 全是退避等待。"
    )
    retained = await run_once(
        build_saturated_workload(), ShareMeter(), release_during_backoff=False
    )
    bills = {m.name: m.summary()["F"]["bill"] for m in retained.meters}
    print(
        f"  保留模式实测：份额账 {bills['share']:.4f}s，消耗账 {bills['consumption']:.4f}s，"
        f"退避被份额账计入 {bills['share'] - bills['consumption']:.4f}s。"
    )

    # ==================== 4. 借用与归还时间线
    print("\n" + "=" * 86)
    print("【4】故障租户 F 的借用 → 被 floor 抢占 → 归还（工作保守，原始故障负载）")
    print("=" * 86)
    flaky_d = await run_once(build_workload(), ShareMeter())

    print("\n-- 4a. floor 抢占与借用归还的关键时刻（source=floor / 借用变化）--")
    trace_lines = grant_preemption_trace(flaky_d).splitlines()
    table_header = trace_lines[:2]
    # 只保留 floor 授予行，以及 L/F 涉及借用变化的行，避免 B 洪泛刷屏
    body = [
        line
        for line in trace_lines[2:]
        if ("floor" in line and line.split()[1] in ("L", "F"))
        or ("退避还许可" in line and line.split()[1] == "F")
    ]
    print("\n".join(table_header + body[:24]))

    print("\n-- 4b. F 自身的完整事件时间线 --")
    print(tenant_timeline(flaky_d, "F"))
    print(
        "\n读法："
        "\n  grant source=borrow：F 的 inflight 超过名义份额 1，槽位借自 B/L 的空闲份额；"
        "\n  release_for_backoff：F 故障退避立即还许可（borrowed 同步下降）；"
        "\n  L 到达且 inflight<floor=2 时，下一个许可 source=floor 优先给 L，F 的借用被收回；"
        "\n  F 恢复后 inflight 回落到名义份额以内、borrowed 归零，即完成归还。"
    )


if __name__ == "__main__":
    asyncio.run(main())
