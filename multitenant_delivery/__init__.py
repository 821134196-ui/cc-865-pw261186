"""多租户消息投递器：并发份额、保底借用与两种计费模型。"""

from .model import AttemptRecord, ConsumerResult, Event, Message, TenantSpec
from .meters import ConsumptionMeter, Meter, ShareMeter
from .consumers import ScriptedConsumer
from .dispatcher import Dispatcher
from .naive import NaiveEqualSplitDispatcher

__all__ = [
    "AttemptRecord",
    "ConsumerResult",
    "Event",
    "Message",
    "TenantSpec",
    "Meter",
    "ShareMeter",
    "ConsumptionMeter",
    "ScriptedConsumer",
    "Dispatcher",
    "NaiveEqualSplitDispatcher",
]
