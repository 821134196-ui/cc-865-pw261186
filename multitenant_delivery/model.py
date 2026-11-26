"""核心数据模型。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class MessageStatus(str, Enum):
    QUEUED = "queued"
    INFLIGHT = "inflight"
    RETRY_WAIT = "retry_wait"
    ACKED = "acked"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class TenantSpec:
    """租户静态规格。

    floor  : 保底并发——延迟敏感流量的底线，任何时候优先兑现
    weight : 权重——系统满载时按权重切分总并发（名义份额）
    cap    : 硬上限——即使有空闲份额可借，单租户也不能超过该并发
    """

    name: str
    floor: int
    weight: int
    cap: int
    latency_sensitive: bool = False
    max_attempts: int = 4


@dataclass
class AttemptRecord:
    """一次处理尝试的账本记录。"""

    attempt: int
    started_at: float
    finished_at: float
    duration: float
    ok: bool
    backoff: float = 0.0  # 失败后等待重试的时长；0 表示成功或进死信


@dataclass
class ConsumerResult:
    ok: bool
    backoff: float = 0.0
    detail: str = ""


@dataclass
class Message:
    msg_id: str
    tenant: str
    enqueue_seq: int
    payload: dict = field(default_factory=dict)
    attempts: int = 0
    status: MessageStatus = MessageStatus.QUEUED
    history: list[AttemptRecord] = field(default_factory=list)
    first_enqueue_at: float = 0.0
    acked_at: float = 0.0

    @property
    def processing_time(self) -> float:
        """真正花在处理上的时间（不含退避等待）。"""
        return sum(r.duration for r in self.history)

    @property
    def total_latency(self) -> float:
        if not self.acked_at:
            return 0.0
        return self.acked_at - self.first_enqueue_at


@dataclass
class Event:
    """调度事件，供演示与测试还原时间线。"""

    ts: float
    kind: str
    tenant: str
    msg_id: str
    detail: dict = field(default_factory=dict)
    snapshot: dict = field(default_factory=dict)


def now() -> float:
    return time.monotonic()
