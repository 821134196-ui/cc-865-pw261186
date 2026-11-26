"""多租户投递调度器。

模型
----
系统只有一个大小为 ``total_permits`` 的许可池，许可 = 一个并发处理槽位。
每个租户有三个静态参数（见 TenantSpec）：

* floor  —— 保底底线：只要该租户有消息且 inflight < floor，就比所有
            已经拿满底线的租户优先拿许可，保证延迟敏感流量不被洪泛挤死；
* weight —— 名义份额：系统满载时按 ``C * weight / Σweight`` 切并发；
* cap    —— 硬上限：即使全局有空闲，单租户 inflight 也不得超过 cap。

「借用」是单一许可池上的涌现行为：租户 inflight 超过自己的名义份额
entitled = max(floor, share) 时，超出的槽位就是从别人的空闲份额里借的；
当被借用方重新有消息、且其 inflight 低于 floor 时，floor 优先级会让它
在下一个释放的许可上立刻拿回底线——借用人随即归还。

退避模式
--------
* release_during_backoff=True （工作保守，默认）：失败退避时释放许可，
  槽位立刻给别人用，重试消息重新排队；
* release_during_backoff=False （保留许可）：退避期间仍占用槽位。
"""

from __future__ import annotations

import asyncio
from collections import deque

from .consumers import ScriptedConsumer
from .meters import Meter
from .model import AttemptRecord, ConsumerResult, Event, Message, MessageStatus, TenantSpec, now


class Dispatcher:
    def __init__(
        self,
        specs: list[TenantSpec],
        consumers: dict[str, ScriptedConsumer],
        total_permits: int,
        scheduler_meter: Meter,
        extra_meters: tuple[Meter, ...] = (),
        *,
        release_during_backoff: bool = True,
    ) -> None:
        assert total_permits > 0
        assert sum(s.floor for s in specs) <= total_permits, "保底总和不能超过总许可"
        self.specs = {s.name: s for s in specs}
        self.consumers = consumers
        self.C = total_permits
        self.meters: list[Meter] = [scheduler_meter, *extra_meters]
        self.scheduler = scheduler_meter
        self.release_during_backoff = release_during_backoff

        self.queues: dict[str, deque[Message]] = {s.name: deque() for s in specs}
        self.inflight = {s.name: 0 for s in specs}
        self.available = total_permits

        self._lock = asyncio.Lock()
        self._cond = asyncio.Condition(self._lock)
        self._tasks: set[asyncio.Task] = set()
        self._stop = False

        self.events: list[Event] = []
        self.messages: list[Message] = []
        self.submitted = 0
        self.acked = 0
        self.dead_lettered = 0
        self.retries = 0
        self._seq = 0

        total_weight = sum(s.weight for s in specs)
        self._share = {
            s.name: total_permits * s.weight // total_weight for s in specs
        }
        for m in self.meters:
            m.register([s.name for s in specs])

    # ------------------------------------------------------------------ 份额
    def entitled(self, tenant: str) -> int:
        """名义份额：保底与权重份额取大值。"""
        spec = self.specs[tenant]
        return max(spec.floor, self._share[tenant])

    def borrowed(self, tenant: str) -> int:
        """当前从其他租户空闲份额中借用的槽位数。"""
        return max(0, self.inflight[tenant] - self.entitled(tenant))

    def idle_pool(self) -> int:
        """当前没有被任何租户名义份额实际占用的槽位（物理空闲 + 已借出）。"""
        used = sum(min(self.inflight[t], self.entitled(t)) for t in self.specs)
        return self.C - used

    def snapshot(self) -> dict:
        return {
            t: {
                "inflight": self.inflight[t],
                "entitled": self.entitled(t),
                "borrowed": self.borrowed(t),
                "queued": len(self.queues[t]),
                "floor": self.specs[t].floor,
                "cap": self.specs[t].cap,
            }
            for t in self.specs
        }

    # ------------------------------------------------------------------ 入口
    async def submit(self, msg: Message) -> None:
        async with self._cond:
            self._seq += 1
            msg.enqueue_seq = self._seq
            msg.first_enqueue_at = now()
            self.queues[msg.tenant].append(msg)
            self.messages.append(msg)
            self.submitted += 1
            self._log_locked("arrive", msg, {"borrowed_before": self.borrowed(msg.tenant)})
            self._cond.notify_all()

    async def join(self) -> None:
        """等待所有已提交消息走到 ack 或死信。"""
        async with self._cond:
            await self._cond.wait_for(
                lambda: self.acked + self.dead_lettered == self.submitted
            )

    async def aclose(self) -> None:
        async with self._cond:
            self._stop = True
            self._cond.notify_all()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    # ------------------------------------------------------------------ 主循环
    async def run(self) -> None:
        while True:
            async with self._cond:
                # 谓词必须覆盖「有队但暂时无法调度」（等物理许可或等 cap 腾出），
                # 否则 wait_for 谓词恒真会一直占着锁，释放通知永远进不来——死锁。
                await self._cond.wait_for(
                    lambda: self._stop or self._select_locked() is not None
                )
                if self._stop and not any(self.queues.values()) and sum(self.inflight.values()) == 0:
                    return
                tenant = self._select_locked()
                if tenant is None:
                    continue

                msg = self.queues[tenant].popleft()
                pre_inflight = self.inflight[tenant]
                self.available -= 1
                self.inflight[tenant] += 1
                msg.attempts += 1
                msg.status = MessageStatus.INFLIGHT
                at = now()
                for m in self.meters:
                    m.grant(tenant, at)

                if pre_inflight < self.specs[tenant].floor:
                    source = "floor"
                elif pre_inflight < self.entitled(tenant):
                    source = "share"
                else:
                    source = "borrow"
                self._log_locked(
                    "grant",
                    msg,
                    {"source": source, "permit_held": self.inflight[tenant]},
                )
                self._cond.notify_all()

            task = asyncio.create_task(self._process(tenant, msg))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def _select_locked(self) -> str | None:
        """选下一个该拿许可的租户。

        第一优先：inflight 还低于 floor 的租户（延迟底线）；
        其余租户按 scheduler 计费器的 debt/weight 排队，账花得少的先跑，
        平局用队首消息的入队序号（FIFO）。
        """
        if self.available <= 0:
            return None
        at = now()
        eligible = [
            t
            for t, q in self.queues.items()
            if q and self.inflight[t] < self.specs[t].cap
        ]
        if not eligible:
            return None

        below_floor = [t for t in eligible if self.inflight[t] < self.specs[t].floor]
        candidates = below_floor or eligible

        def key(t: str) -> tuple[float, int]:
            debt_ratio = self.scheduler.debt(t, at) / self.specs[t].weight
            return debt_ratio, self.queues[t][0].enqueue_seq

        return min(candidates, key=key)

    # ------------------------------------------------------------- 消息处理
    async def _process(self, tenant: str, msg: Message) -> None:
        """许可已在本任务名下。两种退避模式共用一个处理/重试循环。

        工作保守模式：每次失败都把许可释放回池子，重试走重新排队；
        保留模式：许可一直留在本任务手里，退避后原地继续尝试，
        直到成功或耗尽次数，最后统一释放。
        """
        spec = self.specs[tenant]
        while True:
            started = now()
            try:
                result = await self.consumers[tenant].process(msg)
            except Exception as exc:  # 消费者异常按一次失败处理
                result = ConsumerResult(ok=False, backoff=0.1, detail=repr(exc))
            finished = now()
            msg.history.append(
                AttemptRecord(
                    attempt=msg.attempts,
                    started_at=started,
                    finished_at=finished,
                    duration=finished - started,
                    ok=result.ok,
                    backoff=result.backoff,
                )
            )

            if result.ok:
                await self._finish(tenant, msg, finished - started, ok=True)
                return
            if msg.attempts >= spec.max_attempts:
                await self._finish(tenant, msg, finished - started, ok=False)
                return

            self.retries += 1
            if self.release_during_backoff:
                await self._backoff_releasing(tenant, msg, result.backoff, finished - started)
                return  # 重试消息重新入队，由主循环重新授予许可
            await self._backoff_retaining(tenant, msg, result.backoff, finished - started)
            msg.attempts += 1  # 保留模式原地重试，许可不释放

    async def _backoff_releasing(
        self, tenant: str, msg: Message, backoff: float, duration: float
    ) -> None:
        """工作保守：退避前释放许可，槽位立刻可被别人借用。"""
        async with self._cond:
            self._release_permit_locked(tenant, duration, ok=False)
            msg.status = MessageStatus.RETRY_WAIT
            self._log_locked(
                "release_for_backoff",
                msg,
                {"backoff": backoff, "borrowed_after": self.borrowed(tenant)},
            )
            self._cond.notify_all()

        if backoff:
            await asyncio.sleep(backoff)

        async with self._cond:
            msg.status = MessageStatus.QUEUED
            # 尝试编号由主循环重新授予许可时自增，这里只排队
            self.queues[tenant].append(msg)
            self._log_locked("requeue", msg, {"attempt_next": msg.attempts + 1})
            self._cond.notify_all()

    async def _backoff_retaining(
        self, tenant: str, msg: Message, backoff: float, duration: float
    ) -> None:
        """保留许可：退避期间槽位仍占用，份额计费器持续记账。"""
        at = now()
        async with self._cond:
            for m in self.meters:
                m.release_retained(tenant, duration, False, at)
            msg.status = MessageStatus.RETRY_WAIT
            self._log_locked("retain_for_backoff", msg, {"backoff": backoff})
            self._cond.notify_all()

        if backoff:
            await asyncio.sleep(backoff)

        async with self._cond:
            for m in self.meters:
                m.reacquire(tenant, now())
            msg.status = MessageStatus.INFLIGHT
            self._log_locked("reacquire", msg, {"permit_held": self.inflight[tenant]})
            self._cond.notify_all()

    async def _finish(self, tenant: str, msg: Message, duration: float, *, ok: bool) -> None:
        async with self._cond:
            self._release_permit_locked(tenant, duration, ok=ok)
            if ok:
                msg.status = MessageStatus.ACKED
                msg.acked_at = now()
                self.acked += 1
                kind = "ack"
            else:
                msg.status = MessageStatus.DEAD_LETTER
                self.dead_lettered += 1
                kind = "dead_letter"
            self._log_locked(
                kind,
                msg,
                {
                    "attempts": msg.attempts,
                    "borrowed_after": self.borrowed(tenant),
                    "processing_time": msg.processing_time,
                },
            )
            self._cond.notify_all()

    def _release_permit_locked(self, tenant: str, duration: float, *, ok: bool) -> None:
        at = now()
        self.available += 1
        self.inflight[tenant] -= 1
        for m in self.meters:
            m.release_acquired(tenant, duration, ok, at)

    # ------------------------------------------------------------------ 事件
    def _log_locked(self, kind: str, msg: Message, detail: dict) -> None:
        self.events.append(
            Event(
                ts=now(),
                kind=kind,
                tenant=msg.tenant,
                msg_id=msg.msg_id,
                detail=dict(detail),
                snapshot=self.snapshot(),
            )
        )
