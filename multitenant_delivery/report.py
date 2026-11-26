"""统计与报告：把一次模拟运行的事件和消息账本汇总成可读表格。"""

from __future__ import annotations

from .dispatcher import Dispatcher
from .model import Message, MessageStatus


def tenant_stats(d: Dispatcher) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for tenant, spec in d.specs.items():
        msgs = [m for m in d.messages if m.tenant == tenant]
        acked = [m for m in msgs if m.status == MessageStatus.ACKED]
        dlq = [m for m in msgs if m.status == MessageStatus.DEAD_LETTER]
        lat = [m.total_latency for m in acked]
        attempts = [m.attempts for m in msgs]
        proc = [m.processing_time for m in msgs]
        done = [m.acked_at for m in acked] + [m.acked_at for m in dlq]
        out[tenant] = {
            "submitted": len(msgs),
            "acked": len(acked),
            "dead_letter": len(dlq),
            "attempts": sum(attempts),
            "retries": sum(attempts) - len(msgs),
            "avg_latency": sum(lat) / len(lat) if lat else 0.0,
            "p95_latency": _percentile(lat, 95),
            "max_latency": max(lat) if lat else 0.0,
            "processing_time": sum(proc),
            "last_done": max(done) if done else 0.0,
            "floor": spec.floor,
            "entitled": d.entitled(tenant),
            "cap": spec.cap,
        }
    return out


def makespan(d: Dispatcher) -> float:
    """从首条消息入队到最后一条消息落定的墙钟时长。"""
    if not d.messages:
        return 0.0
    start = min(m.first_enqueue_at for m in d.messages)
    end = max(
        m.acked_at for m in d.messages if m.status in (MessageStatus.ACKED, MessageStatus.DEAD_LETTER)
    )
    return end - start


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))
    return s[idx]


def meter_bills(d: Dispatcher) -> dict[str, dict[str, dict]]:
    return {m.name: m.summary() for m in d.meters}


def peak_inflight(d: Dispatcher, tenant: str) -> int:
    return max((e.snapshot[tenant]["inflight"] for e in d.events), default=0)


def max_borrow(d: Dispatcher, tenant: str) -> int:
    return max((e.snapshot[tenant]["borrowed"] for e in d.events), default=0)


def fmt_table(rows: list[list[str]], headers: list[str]) -> str:
    widths = [
        max(len(str(headers[i])), *(len(str(r[i])) for r in rows)) for i in range(len(headers))
    ]
    line = "  ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    sep = "  ".join("-" * w for w in widths)
    body = "\n".join(
        "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(r)) for r in rows
    )
    return "\n".join([line, sep, body])


def comparison_report(runs: dict[str, Dispatcher]) -> str:
    """多个策略运行结果并列对比。"""
    lines: list[str] = []
    tenants = list(next(iter(runs.values())).specs)

    # 交付与延迟
    headers = ["策略", "租户", "提交", "成功", "死信", "重试", "均延迟ms", "p95ms", "峰值并发", "峰值借用"]
    rows: list[list[str]] = []
    for label, d in runs.items():
        st = tenant_stats(d)
        for t in tenants:
            rows.append(
                [
                    label,
                    t,
                    st[t]["submitted"],
                    st[t]["acked"],
                    st[t]["dead_letter"],
                    st[t]["retries"],
                    f"{st[t]['avg_latency'] * 1000:.1f}",
                    f"{st[t]['p95_latency'] * 1000:.1f}",
                    peak_inflight(d, t),
                    max_borrow(d, t),
                ]
            )
    lines.append("== 交付与延迟 ==")
    lines.append(fmt_table(rows, headers))

    # 两种计费
    lines.append("")
    lines.append("== 计费对比（每个策略同时挂两个计费器，账单不受调度器选择影响） ==")
    headers = ["策略", "租户", "份额账(占用秒)", "消耗账(处理秒)", "尝试次数", "失败尝试"]
    rows = []
    for label, d in runs.items():
        bills = meter_bills(d)
        for t in tenants:
            share = bills.get("share", {}).get(t, {}).get("bill")
            cons = bills.get("consumption", {}).get(t, {})
            rows.append(
                [
                    label,
                    t,
                    "-" if share is None else f"{share:.4f}",
                    "-" if not cons else f"{cons['bill']:.4f}",
                    cons.get("attempts", "-"),
                    cons.get("failed_attempts", "-"),
                ]
            )
            lines.append(fmt_table(rows, headers))

    # 完工时长
    lines.append("")
    lines.append("== 完工时长 makespan（首条入队 → 末条落定的墙钟秒） ==")
    headers = ["策略", "总完工秒", "L 末条秒", "B 末条秒", "F 末条秒"]
    rows = []
    for label, d in runs.items():
        st = tenant_stats(d)
        rows.append(
            [
                label,
                f"{makespan(d):.3f}",
                f"{st['L']['last_done'] - d.messages[0].first_enqueue_at:.3f}",
                f"{st['B']['last_done'] - d.messages[0].first_enqueue_at:.3f}",
                f"{st['F']['last_done'] - d.messages[0].first_enqueue_at:.3f}",
            ]
        )
    lines.append(fmt_table(rows, headers))
    return "\n".join(lines)


def tenant_timeline(d: Dispatcher, tenant: str, kinds: set[str] | None = None) -> str:
    t0 = d.events[0].ts if d.events else 0.0
    headers = ["t(ms)", "事件", "消息", "inflight", "名义", "借用", "排队", "细节"]
    rows = []
    for e in d.events:
        if e.tenant != tenant:
            continue
        if kinds and e.kind not in kinds:
            continue
        s = e.snapshot[tenant]
        detail = ",".join(f"{k}={v}" for k, v in e.detail.items())
        rows.append(
            [
                f"{(e.ts - t0) * 1000:8.1f}",
                e.kind,
                e.msg_id,
                s["inflight"],
                s["entitled"],
                s["borrowed"],
                s["queued"],
                detail,
            ]
        )
    return fmt_table(rows, headers)


def grant_preemption_trace(d: Dispatcher) -> str:
    """跨租户的许可授予序列，只看 grant/退避释放，突出 floor 抢占与借用归还。"""
    t0 = d.events[0].ts if d.events else 0.0
    headers = ["t(ms)", "租户", "授予来源", "授予后inflight", "借用", "事件"]
    rows = []
    for e in d.events:
        if e.kind == "grant":
            rows.append(
                [
                    f"{(e.ts - t0) * 1000:8.1f}",
                    e.tenant,
                    e.detail.get("source", ""),
                    e.snapshot[e.tenant]["inflight"],
                    e.snapshot[e.tenant]["borrowed"],
                    f"{e.kind} {e.msg_id}",
                ]
            )
        elif e.kind == "release_for_backoff":
            rows.append(
                [
                    f"{(e.ts - t0) * 1000:8.1f}",
                    e.tenant,
                    "-",
                    e.snapshot[e.tenant]["inflight"],
                    e.snapshot[e.tenant]["borrowed"],
                    f"退避还许可 {e.msg_id}",
                ]
            )
    return fmt_table(rows, headers)
