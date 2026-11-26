"""调度器与计费器的单元测试。

运行：  python -m unittest -v tests.test_dispatcher
"""

from __future__ import annotations

import asyncio
import unittest

from multitenant_delivery.dispatcher import Dispatcher
from multitenant_delivery.meters import ConsumptionMeter, ShareMeter
from multitenant_delivery.model import ConsumerResult, Message, TenantSpec


def make_specs() -> list[TenantSpec]:
    return [
        TenantSpec("L", floor=2, weight=2, cap=3, latency_sensitive=True, max_attempts=3),
        TenantSpec("B", floor=1, weight=4, cap=8, max_attempts=2),
        TenantSpec("F", floor=1, weight=1, cap=4, max_attempts=4),
    ]


class InstantConsumer:
    """零耗时消费者，剧本直接写在消息 payload 里。"""

    def __init__(self, script_key: str = "script"):
        self.name = "instant"
        self._key = script_key

    async def process(self, msg: Message) -> ConsumerResult:
        sc = msg.payload[self._key]
        if callable(sc):
            return sc(msg)
        return ConsumerResult(ok=sc[0], backoff=sc[1] if len(sc) > 1 else 0.0)


async def run_to_completion(d: Dispatcher, messages: list[Message]) -> Dispatcher:
    runner = asyncio.create_task(d.run())
    for m in messages:
        await d.submit(m)
    await d.join()
    await d.aclose()
    runner.cancel()
    return d


def msg(tenant: str, n: int, **payload) -> Message:
    return Message(msg_id=f"{tenant}-{n}", tenant=tenant, enqueue_seq=n, payload=payload)


class PermitInvariantTests(unittest.TestCase):
    def test_permits_never_overcommitted(self):
        async def scenario():
            specs = make_specs()
            consumers = {s.name: InstantConsumer() for s in specs}
            d = Dispatcher(specs, consumers, total_permits=8, scheduler_meter=ShareMeter())
            msgs = [msg("B", i, script=(True,)) for i in range(30)]
            msgs += [msg("F", 100 + i, script=(False, 0.01)) for i in range(10)]
            await run_to_completion(d, msgs)
            # 任意事件时刻：inflight 之和 = 已授予未释放，且不超过 C
            for e in d.events:
                held = sum(s["inflight"] for s in e.snapshot.values())
                self.assertLessEqual(held, 8)
            self.assertEqual(d.available, 8)
            self.assertEqual(sum(d.inflight.values()), 0)
            self.assertEqual(d.acked + d.dead_lettered, d.submitted)

        asyncio.run(scenario())

    def test_cap_never_exceeded(self):
        async def scenario():
            specs = [TenantSpec("X", floor=1, weight=1, cap=2, max_attempts=2)]
            d = Dispatcher(specs, {"X": InstantConsumer()}, 5, ShareMeter())
            await run_to_completion(d, [msg("X", i, script=(True,)) for i in range(10)])
            self.assertLessEqual(max(e.snapshot["X"]["inflight"] for e in d.events), 2)

        asyncio.run(scenario())


class GatedConsumer:
    """处理前在事件闸门上等待，便于精确控制槽位占用。"""

    def __init__(self, gate: asyncio.Event):
        self.name = "gated"
        self._gate = gate

    async def process(self, msg: Message) -> ConsumerResult:
        await self._gate.wait()
        return ConsumerResult(ok=True)


class FloorTests(unittest.TestCase):
    def test_floor_preempts_borrower(self):
        """B 借满池后，L 到达且 inflight<floor 时必须先拿许可。"""

        async def scenario():
            specs = make_specs()
            gate = asyncio.Event()
            consumers = {
                "B": GatedConsumer(gate),
                "L": InstantConsumer(),
                "F": InstantConsumer(),
            }
            d = Dispatcher(specs, consumers, 8, ShareMeter())
            runner = asyncio.create_task(d.run())
            for i in range(8):
                await d.submit(msg("B", i, script=(True,)))
            # 等 B 占满
            for _ in range(100):
                await asyncio.sleep(0.005)
                if d.inflight["B"] == 8:
                    break
            self.assertEqual(d.inflight["B"], 8)

            await d.submit(msg("L", 90, script=(True,)))
            # 不释放 B 时 L 无许可可拿
            await asyncio.sleep(0.02)
            self.assertEqual(d.inflight["L"], 0)

            # 放一个 B 槽位，下一个许可必须给 L（floor 优先）
            gate.set()
            for _ in range(100):
                await asyncio.sleep(0.005)
                grants_l = [e for e in d.events if e.kind == "grant" and e.tenant == "L"]
                if grants_l:
                    break
            grants_l = [e for e in d.events if e.kind == "grant" and e.tenant == "L"]
            self.assertTrue(grants_l)
            self.assertEqual(grants_l[0].detail["source"], "floor")
            await d.join()
            await d.aclose()
            runner.cancel()

        asyncio.run(scenario())

    def test_floor_sum_validation(self):
        with self.assertRaises(AssertionError):
            Dispatcher(
                [TenantSpec("A", 5, 1, 5), TenantSpec("B", 5, 1, 5)],
                {"A": InstantConsumer(), "B": InstantConsumer()},
                total_permits=8,
                scheduler_meter=ShareMeter(),
            )


class BorrowReturnTests(unittest.TestCase):
    def test_borrow_then_return_on_backoff(self):
        async def scenario():
            specs = make_specs()
            # F 名义份额=1；让它先失败退避（工作保守释放），验证借用计数起落
            d = Dispatcher(
                specs,
                {s.name: InstantConsumer() for s in specs},
                8,
                ShareMeter(),
                release_during_backoff=True,
            )
            runner = asyncio.create_task(d.run())
            # F 一次失败后成功
            f_msgs = [
                msg("F", 1, script=lambda m: ConsumerResult(
                    ok=(m.attempts >= 2), backoff=0.01 if m.attempts < 2 else 0.0)),
                msg("F", 2, script=lambda m: ConsumerResult(
                    ok=(m.attempts >= 2), backoff=0.01 if m.attempts < 2 else 0.0)),
                msg("F", 3, script=lambda m: ConsumerResult(
                    ok=(m.attempts >= 2), backoff=0.01 if m.attempts < 2 else 0.0)),
            ]
            for m in f_msgs:
                await d.submit(m)
            await d.join()
            peak_borrow = max(e.snapshot["F"]["borrowed"] for e in d.events)
            self.assertGreaterEqual(peak_borrow, 1, "F 应至少借到 1 个空闲槽位")
            # 结束后所有借用归零、许可全部归还
            self.assertEqual(d.borrowed("F"), 0)
            self.assertEqual(d.available, 8)
            self.assertEqual(sum(d.inflight.values()), 0)
            await d.aclose()
            runner.cancel()

        asyncio.run(scenario())


class MeterTests(unittest.TestCase):
    def test_share_meter_charges_backoff_when_retained(self):
        async def scenario():
            specs = [TenantSpec("X", floor=1, weight=1, cap=2, max_attempts=3)]
            d = Dispatcher(
                specs,
                {"X": InstantConsumer()},
                2,
                ShareMeter(),
                extra_meters=(ConsumptionMeter(),),
                release_during_backoff=False,
            )
            m = msg("X", 1, script=lambda mm: ConsumerResult(
                ok=(mm.attempts >= 2), backoff=0.05 if mm.attempts < 2 else 0.0))
            await run_to_completion(d, [m])
            share_bill = d.meters[0].summary()["X"]["bill"]
            cons_bill = d.meters[1].summary()["X"]["bill"]
            # 保留模式：份额账包含退避，必然大于纯处理账
            self.assertGreater(share_bill, cons_bill)
            self.assertGreaterEqual(share_bill - cons_bill, 0.04)

        asyncio.run(scenario())

    def test_consumption_meter_counts_attempts(self):
        async def scenario():
            specs = [TenantSpec("X", floor=1, weight=1, cap=2, max_attempts=3)]
            d = Dispatcher(
                specs,
                {"X": InstantConsumer()},
                2,
                ConsumptionMeter(),
                release_during_backoff=True,
            )
            m = msg("X", 1, script=lambda mm: ConsumerResult(
                ok=(mm.attempts >= 3), backoff=0.005 if mm.attempts < 3 else 0.0))
            await run_to_completion(d, [m])
            summary = d.scheduler.summary()["X"]
            self.assertEqual(summary["attempts"], 3)
            self.assertEqual(summary["failed_attempts"], 2)
            self.assertGreater(summary["bill"], 0.0)

        asyncio.run(scenario())

    def test_work_conserving_bills_roughly_equal(self):
        async def scenario():
            specs = [TenantSpec("X", floor=1, weight=1, cap=2, max_attempts=3)]
            d = Dispatcher(
                specs,
                {"X": InstantConsumer()},
                2,
                ShareMeter(),
                extra_meters=(ConsumptionMeter(),),
                release_during_backoff=True,
            )
            m = msg("X", 1, script=lambda mm: ConsumerResult(
                ok=(mm.attempts >= 2), backoff=0.02 if mm.attempts < 2 else 0.0))
            await run_to_completion(d, [m])
            share_bill = d.meters[0].summary()["X"]["bill"]
            cons_bill = d.meters[1].summary()["X"]["bill"]
            # 退避释放许可，份额账只覆盖处理时长，两者应几乎相等
            self.assertAlmostEqual(share_bill, cons_bill, delta=0.02)

        asyncio.run(scenario())


class DeadLetterTests(unittest.TestCase):
    def test_poison_message_dead_letters(self):
        async def scenario():
            specs = [TenantSpec("X", floor=1, weight=1, cap=2, max_attempts=3)]
            d = Dispatcher(
                specs, {"X": InstantConsumer()}, 2, ShareMeter(), release_during_backoff=True
            )
            m = msg("X", 1, script=(False, 0.005))
            await run_to_completion(d, [m])
            self.assertEqual(d.dead_lettered, 1)
            self.assertEqual(d.acked, 0)
            self.assertEqual(m.attempts, 3)
            self.assertEqual(d.available, 2)

        asyncio.run(scenario())

    def test_retain_mode_dead_letter_releases_permit(self):
        async def scenario():
            specs = [TenantSpec("X", floor=1, weight=1, cap=2, max_attempts=2)]
            d = Dispatcher(
                specs, {"X": InstantConsumer()}, 2, ShareMeter(), release_during_backoff=False
            )
            m = msg("X", 1, script=(False, 0.005))
            await run_to_completion(d, [m])
            self.assertEqual(d.dead_lettered, 1)
            self.assertEqual(d.available, 2)
            self.assertEqual(d.inflight["X"], 0)

        asyncio.run(scenario())


class EntitlementTests(unittest.TestCase):
    def test_entitled_is_max_of_floor_and_share(self):
        specs = make_specs()  # C=8, weights 2,4,1
        d = Dispatcher(specs, {s.name: InstantConsumer() for s in specs}, 8, ShareMeter())
        # 权重份额： 8*2//7=2, 8*4//7=4, 8*1//7=1；与 floor 取大
        self.assertEqual(d.entitled("L"), 2)
        self.assertEqual(d.entitled("B"), 4)
        self.assertEqual(d.entitled("F"), 1)

    def test_idle_pool_tracks_unused_shares(self):
        async def scenario():
            specs = make_specs()
            d = Dispatcher(specs, {s.name: InstantConsumer() for s in specs}, 8, ShareMeter())
            runner = asyncio.create_task(d.run())
            await d.submit(msg("F", 1, script=(True,)))
            for _ in range(50):
                await asyncio.sleep(0.005)
                if d.acked == 1:
                    break
            # F 名义 1、此刻已释放；idle_pool 应回到 C
            self.assertEqual(d.idle_pool(), 8)
            await d.join()
            await d.aclose()
            runner.cancel()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
