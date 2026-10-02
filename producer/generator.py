from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from random import Random
from typing import Iterator

from producer.models import OrderEvent, PaymentEvent


class Scenario(StrEnum):
    NORMAL = "normal"
    DUPLICATES = "duplicates"
    LATE = "late"
    PAYMENT_FAILURES = "payment_failures"


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 1
    scenario: Scenario = Scenario.NORMAL


Event = OrderEvent | PaymentEvent
_START_TIME = datetime(2026, 10, 1, tzinfo=UTC)
_REGIONS = ("texas", "california", "illinois", "new_york")


def generate_events(config: GeneratorConfig) -> Iterator[Event]:
    random = Random(config.seed)
    sequence = 0

    while True:
        event_time = _START_TIME + timedelta(seconds=sequence)
        if config.scenario is Scenario.LATE and sequence % 2 == 1:
            event_time -= timedelta(minutes=10)

        event_id = f"evt-{sequence}"
        if config.scenario is Scenario.DUPLICATES and sequence % 2 == 1:
            event_id = f"evt-{sequence - 1}"

        amount = round(random.uniform(5, 250), 2)
        order_id = f"ord-{sequence // 2}"
        if sequence % 2 == 0:
            yield OrderEvent(
                event_id=event_id,
                order_id=order_id,
                user_id=f"usr-{random.randrange(1, 1001)}",
                product_id=f"prd-{random.randrange(1, 101)}",
                amount=amount,
                region=random.choice(_REGIONS),
                event_time=event_time,
            )
        else:
            status = "succeeded"
            if config.scenario is Scenario.PAYMENT_FAILURES:
                status = "failed"
            yield PaymentEvent(
                event_id=event_id,
                payment_id=f"pay-{sequence // 2}",
                order_id=order_id,
                status=status,
                amount=amount,
                event_time=event_time,
            )
        sequence += 1
