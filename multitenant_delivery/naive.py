"""朴素基线：按消息条数把并发平分给活跃租户。

这是需求里被否定的方案——只看队列里有多少条消息，不看单条处理代价。
慢而爱重试的租户与快租户拿到一样多的槽位，快租户的大量短消息被拖死。

实现上复用 Dispatcher 的全部机制，只替换 _select_locked：
把总许可按当前非空队列的租户数量均分（向上取整、受 cap 约束），
不设 floor 优先级，也不挂计费器参与排队。
"""

from __future__ import annotations

from .dispatcher import Dispatcher
from .meters import ConsumptionMeter, ShareMeter


class NaiveEqualSplitDispatcher(Dispatcher):
    def __init__(self, specs, consumers, total_permits, **kwargs) -> None:
        # 朴素方案不按账单排队，挂一个不参与决策的份额计费器；
        # 另挂消耗计费器仅作被动记账，保证三个策略的账单口径一致。
        super().__init__(
            specs,
            consumers,
            total_permits=total_permits,
            scheduler_meter=ShareMeter(),
            extra_meters=(ConsumptionMeter(),),
            release_during_backoff=kwargs.get("release_during_backoff", True),
        )

    def _select_locked(self) -> str | None:
        if self.available <= 0:
            return None
        active = [
            t
            for t, q in self.queues.items()
            if q and self.inflight[t] < self.specs[t].cap
        ]
        if not active:
            return None
        # 每个活跃租户的「公平份额」= 总许可 / 活跃租户数（向上取整）
        fair = -(-self.C // len(active))
        room = [t for t in active if self.inflight[t] < max(1, fair)]
        pool = room or active  # 都拿满后允许继续分配直到触 cap
        # 纯 FIFO，谁的队首消息早谁先
        return min(pool, key=lambda t: self.queues[t][0].enqueue_seq)
