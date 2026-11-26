"""计费策略：按并发份额 vs 按实际处理消耗。

两种计费器实现同一套钩子，调度器用 debt(tenant)/weight 作为排队优先级键，
因此「怎么计费」直接决定「谁先被调度」：

* ShareMeter       —— 按并发份额计费：持有许可的每一个槽位秒都记账，
                      无论槽位上是在处理消息还是在退避等待。
                      计费本质是「资源预留/占用」。
* ConsumptionMeter —— 按实际处理消耗计费：只对真正的处理尝试累计处理时长，
                      退避等待（持有许可但没在干活）不计费。
                      计费本质是「实际消耗的算力」，失败重试会重复计费。
"""

from __future__ import annotations

from typing import Protocol


class Meter(Protocol):
    name: str

    def grant(self, tenant: str, at: float) -> None: ...
    def release_acquired(self, tenant: str, duration: float, ok: bool, at: float) -> None: ...
    def release_retained(self, tenant: str, duration: float, ok: bool, at: float) -> None: ...
    def reacquire(self, tenant: str, at: float) -> None: ...
    def debt(self, tenant: str, at: float) -> float: ...
    def register(self, tenants: list[str]) -> None: ...
    def summary(self) -> dict[str, dict]: ...


class ShareMeter:
    """并发份额计费：许可占用秒 = 每个槽位从授予到释放的时长之和。

    保留模式下槽位不释放，退避等待自然被计入占用秒。
    """

    name = "share"

    def __init__(self) -> None:
        # 每个当前持有槽位的授予时间戳（栈，释放时弹出）
        self._granted_at: dict[str, list[float]] = {}
        # 已结算的占用秒（许可释放时结算）
        self._bill: dict[str, float] = {}

    def register(self, tenants: list[str]) -> None:
        for t in tenants:
            self._granted_at[t] = []
            self._bill[t] = 0.0

    def grant(self, tenant: str, at: float) -> None:
        self._granted_at[tenant].append(at)

    def release_acquired(self, tenant: str, duration: float, ok: bool, at: float) -> None:
        grant_ts = self._granted_at[tenant].pop()
        self._bill[tenant] += at - grant_ts

    def release_retained(self, tenant: str, duration: float, ok: bool, at: float) -> None:
        # 许可保留在租户手里，槽位不弹出、占用连续计费
        return

    def reacquire(self, tenant: str, at: float) -> None:
        # 许可从未离开，占用计时不中断
        return

    def debt(self, tenant: str, at: float) -> float:
        open_slots = sum(at - ts for ts in self._granted_at[tenant])
        return self._bill[tenant] + open_slots

    def summary(self) -> dict[str, dict]:
        return {
            t: {"bill": self._bill[t], "held_slots": len(self._granted_at[t])}
            for t in self._bill
        }


class ConsumptionMeter:
    """处理消耗计费：只累计真实处理尝试的时长，退避等待免费。"""

    name = "consumption"

    def __init__(self) -> None:
        self._bill: dict[str, float] = {}
        self._attempts: dict[str, int] = {}
        self._failed_attempts: dict[str, int] = {}

    def register(self, tenants: list[str]) -> None:
        for t in tenants:
            self._bill[t] = 0.0
            self._attempts[t] = 0
            self._failed_attempts[t] = 0

    def grant(self, tenant: str, at: float) -> None:
        return

    def release_acquired(self, tenant: str, duration: float, ok: bool, at: float) -> None:
        self._bill[tenant] += duration
        self._attempts[tenant] += 1
        if not ok:
            self._failed_attempts[tenant] += 1

    def release_retained(self, tenant: str, duration: float, ok: bool, at: float) -> None:
        # 保留模式下一次尝试结束：处理时长照样计费，之后的退避不计费
        self._bill[tenant] += duration
        self._attempts[tenant] += 1
        if not ok:
            self._failed_attempts[tenant] += 1

    def reacquire(self, tenant: str, at: float) -> None:
        return

    def debt(self, tenant: str, at: float) -> float:
        return self._bill[tenant]

    def summary(self) -> dict[str, dict]:
        return {
            t: {
                "bill": self._bill[t],
                "attempts": self._attempts[t],
                "failed_attempts": self._failed_attempts[t],
            }
            for t in self._bill
        }
