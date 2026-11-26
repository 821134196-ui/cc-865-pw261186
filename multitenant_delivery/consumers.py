"""脚本化消费者：行为由消息负载中的「剧本」决定，运行期无随机数。

同一个负载在不同计费策略下重放时，每条消息的处理时长、失败次数、
退避时长完全一致，因此策略之间的差异只能来自调度与计费本身。
"""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

from .model import ConsumerResult, Message

PlanFn = Callable[[Message, int], dict]


def fast_plan(mean: float = 0.012) -> PlanFn:
    """消息多但单条很快的租户。"""

    def plan(msg: Message, attempt: int) -> dict:
        jitter = msg.payload["jitter"]
        return {"ok": True, "duration": mean * (0.8 + 0.4 * jitter), "backoff": 0.0}

    return plan


def slow_plan(mean: float = 0.16) -> PlanFn:
    """单条处理很慢、还会失败重试的租户。"""

    def plan(msg: Message, attempt: int) -> dict:
        jitter = msg.payload["jitter"]
        fail_on = msg.payload.get("fail_on", ())
        ok = attempt not in fail_on
        duration = mean * (0.75 + 0.5 * jitter) * (0.8 if ok else 1.2)
        backoff = 0.0 if ok else msg.payload.get("backoff", 0.12)
        return {"ok": ok, "duration": duration, "backoff": backoff}

    return plan


def flaky_plan(mean: float = 0.09, fail_rate: float = 0.6) -> PlanFn:
    """本地故障消费者：高失败率 + 重试退避，用于演示借用与归还。

    成败在消息生成阶段就写进 payload（deterministic 随机种子），
    消费者只是照着剧本执行。
    """

    def plan(msg: Message, attempt: int) -> dict:
        outcomes: dict[int, dict] = msg.payload["outcomes"]
        if attempt in outcomes:
            return outcomes[attempt]
        # 剧本用完即成功（恢复）
        return {"ok": True, "duration": mean * 0.8, "backoff": 0.0}

    return plan


def poison_plan(mean: float = 0.05) -> PlanFn:
    """毒消息：永远失败，直到耗尽重试次数进入死信。"""

    def plan(msg: Message, attempt: int) -> dict:
        return {"ok": False, "duration": mean, "backoff": msg.payload.get("backoff", 0.08)}

    return plan


class ScriptedConsumer:
    def __init__(self, name: str, plan: PlanFn, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep):
        self.name = name
        self._plan = plan
        self._sleep = sleep

    async def process(self, msg: Message) -> ConsumerResult:
        script = self._plan(msg, msg.attempts)
        await self._sleep(script["duration"])
        return ConsumerResult(
            ok=script["ok"],
            backoff=script.get("backoff", 0.0),
            detail=f"{self.name}:attempt={msg.attempts}",
        )


def flaky_or_slow_plan(mean: float = 0.09, fail_rate: float = 0.6) -> PlanFn:
    """故障租户统一剧本：消息自带 outcomes 时走 flaky，带 fail_on 时走慢速重试。"""

    flaky = flaky_plan(mean=mean, fail_rate=fail_rate)
    slow = slow_plan(mean=mean * 1.8)

    def plan(msg: Message, attempt: int) -> dict:
        if "outcomes" in msg.payload:
            return flaky(msg, attempt)
        if "fail_on" in msg.payload:
            return slow(msg, attempt)
        return {"ok": True, "duration": mean, "backoff": 0.0}

    return plan
