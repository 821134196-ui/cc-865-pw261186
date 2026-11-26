"""可复现的工作负载：消息的处理剧本与到达时刻在提交前就固定下来。

固定随机种子后，两种计费策略跑的是逐字节相同的负载，
策略间差异只能来自调度与计费。

场景设计（总许可 C=8）：
  t=0       B 的 60 条快消息一次性洪泛，迅速占满许可池；
  t≈0.05s   F 的故障消息到达，此时 B 的槽位随快消息完成而滚动释放，
            F 借走空闲份额（inflight 可超过自己的名义份额 1）；
  周期性    L 每 0.09s 来 2 条延迟敏感消息，inflight 低于 floor=2 时
            无条件优先拿下一个释放的许可，把 B/F 借用的槽位收回；
  含 1 条   毒消息（永远失败）演示重试耗尽进入死信。
"""

from __future__ import annotations

import random

from .consumers import ScriptedConsumer, fast_plan, flaky_or_slow_plan
from .model import Message, TenantSpec

# 三类租户：
# L —— 延迟敏感，消息量小但要求低延迟（floor=2 的底线）
# B —— 消息多、单条很快的批量租户（会在别人空闲时借份额，cap 放到最大）
# F —— 本地故障消费者，单条慢且高失败率、频繁退避重试
TENANT_SPECS = [
    TenantSpec(name="L", floor=2, weight=2, cap=3, latency_sensitive=True, max_attempts=3),
    TenantSpec(name="B", floor=1, weight=4, cap=8, max_attempts=2),
    TenantSpec(name="F", floor=1, weight=1, cap=4, max_attempts=4),
]


def build_workload(
    *,
    n_fast: int = 60,
    n_flaky: int = 10,
    n_latency_bursts: int = 7,
    seed: int = 2611,
) -> list[tuple[float, Message]]:
    """返回 (相对到达时刻, 消息) 列表，按到达时刻排序。"""
    rng = random.Random(seed)
    timed: list[tuple[float, Message]] = []
    seq = 0

    def new_msg(tenant: str, payload: dict) -> Message:
        nonlocal seq
        seq += 1
        return Message(msg_id=f"{tenant}-{seq:03d}", tenant=tenant, enqueue_seq=seq, payload=payload)

    # B：t=0 洪泛，单条 ~12ms
    for _ in range(n_fast):
        timed.append((rng.random() * 0.02, new_msg("B", {"jitter": rng.random()})))

    # F：t≈0.05s 起陆续到达，失败序列在生成时写死
    for i in range(n_flaky):
        n_fail = rng.choices([0, 1, 2, 3], weights=[2, 4, 3, 1])[0]
        outcomes = {}
        for attempt in range(1, n_fail + 1):
            outcomes[attempt] = {
                "ok": False,
                "duration": 0.09 * (0.8 + 0.4 * rng.random()),
                "backoff": rng.choice([0.10, 0.15, 0.20]),
            }
        arrival = 0.05 + i * 0.18 + rng.random() * 0.04
        timed.append((arrival, new_msg("F", {"jitter": rng.random(), "outcomes": outcomes})))

    # F 的毒消息：没有成功剧本，组合消费者里 outcomes 缺省即成功，
    # 所以毒消息用显式 fail_forever 标记，由剧本解释
    timed.append(
        (
            0.30,
            new_msg(
                "F",
                {
                    "jitter": rng.random(),
                    "outcomes": {
                        a: {"ok": False, "duration": 0.06, "backoff": 0.08}
                        for a in range(1, 6)
                    },
                    "poison": True,
                },
            ),
        )
    )

    # L：每 0.09s 一个 2 条的小突发，制造 floor 抢占
    for burst in range(n_latency_bursts):
        for k in range(2):
            arrival = 0.08 + burst * 0.09 + k * 0.004
            timed.append((arrival, new_msg("L", {"jitter": rng.random()})))

    timed.sort(key=lambda x: x[0])
    return timed


def build_consumers(sleep) -> dict[str, ScriptedConsumer]:
    return {
        "L": ScriptedConsumer("L", fast_plan(mean=0.010), sleep),
        "B": ScriptedConsumer("B", fast_plan(mean=0.012), sleep),
        "F": ScriptedConsumer("F", flaky_or_slow_plan(mean=0.09), sleep),
    }


__all__ = ["TENANT_SPECS", "build_workload", "build_saturated_workload", "build_consumers"]


def build_saturated_workload(
    *,
    n_fast: int = 80,
    n_slow: int = 10,
    n_latency: int = 12,
    seed: int = 865,
) -> list[tuple[float, Message]]:
    """持续饱和场景：B 洪泛不断、F 慢消息持续占坑、L 周期突发。

    与 build_workload 的区别：F 消息在 t=0 附近集中到达且处理慢，
    保留许可模式下会长期占住槽位，最能暴露「按条数平分」的危害。
    """
    rng = random.Random(seed)
    timed: list[tuple[float, Message]] = []
    seq = 0

    def new_msg(tenant: str, payload: dict) -> Message:
        nonlocal seq
        seq += 1
        return Message(msg_id=f"{tenant}-{seq:03d}", tenant=tenant, enqueue_seq=seq, payload=payload)

    # B：80 条快消息，t=0 全部到达，队列长时间不空
    for _ in range(n_fast):
        timed.append((rng.random() * 0.01, new_msg("B", {"jitter": rng.random()})))

    # F：10 条慢消息集中到达，多数带 1~2 次失败重试
    for i in range(n_slow):
        n_fail = rng.choices([1, 2, 3], weights=[3, 4, 3])[0]
        outcomes = {
            a: {
                "ok": False,
                "duration": 0.10 * (0.9 + 0.2 * rng.random()),
                "backoff": rng.choice([0.12, 0.18]),
            }
            for a in range(1, n_fail + 1)
        }
        timed.append((0.02 + rng.random() * 0.05, new_msg("F", {"jitter": rng.random(), "outcomes": outcomes})))

    # L：每 0.1s 两条
    for burst in range(n_latency // 2):
        for k in range(2):
            timed.append((0.06 + burst * 0.10 + k * 0.004, new_msg("L", {"jitter": rng.random()})))

    timed.sort(key=lambda x: x[0])
    return timed
