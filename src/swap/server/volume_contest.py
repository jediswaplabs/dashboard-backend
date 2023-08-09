from decimal import Decimal
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime, date, timezone, timedelta

import strawberry
from pymongo.database import Database
from strawberry.types import Info


STABLE_PAIRS = {
    "0x5801bdad32f343035fb242e98d1e9371ae85bc1543962fedea16c59b35bd19b",
    "0xcfd39f5244f7b617418c018204a8a9f9a7f72e71f0ef38f968eeb2a9ca302b",
    "0xf0f5b3eed258344152e1f17baf84a2e1b621cd754b625bec169e8595aea767"
}
WEEKLY_HARD_CAP = Decimal(1000)
STABLE_POOL_MULTIPLIER = Decimal(0.5)


@dataclass
class Week:
    _id: int
    start_dt: datetime
    end_dt: datetime
    volume: int = 0
    name: str = field(init=False)

    def __post_init__(self):
        self.name = f'week{self._id}'


@strawberry.type
class VolumeContest:
    user_address: str
    week_1_volume: Decimal
    week_2_volume: Decimal
    week_3_volume: Decimal
    week_4_volume: Decimal
    week_5_volume: Decimal
    week_6_volume: Decimal
    week_7_volume: Decimal
    week_8_volume: Decimal
    total_contest_volume: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            user_address=data["user_address"],
            week_1_volume=data.get("week1", Decimal(0)),
            week_2_volume=data.get("week2", Decimal(0)),
            week_3_volume=data.get("week3", Decimal(0)),
            week_4_volume=data.get("week4", Decimal(0)),
            week_5_volume=data.get("week5", Decimal(0)),
            week_6_volume=data.get("week6", Decimal(0)),
            week_7_volume=data.get("week7", Decimal(0)),
            week_8_volume=data.get("week8", Decimal(0)),
            total_contest_volume=data["total_contest_volume"],
        )


@strawberry.input
class WhereFilterForUserAddress:
    user_address: str
    start_date: date


async def get_volume_contest(
        info: Info, where: WhereFilterForUserAddress, first: Optional[int] = 100, skip: Optional[int] = 0
) -> VolumeContest:
    db: Database = info.context["db"]

    dt = datetime.combine(where.start_date, datetime.min.time())
    utc_dt = dt.replace(tzinfo=timezone.utc)

    weeks = []
    days_delta = 0
    for i in range(1, 9):
        start_dt = utc_dt + timedelta(days=days_delta)
        days_delta += 7
        end_dt = utc_dt + timedelta(days=days_delta) - timedelta(milliseconds=1)
        weeks.append(Week(i, start_dt, end_dt))

    data = {
        'user_address': where.user_address,
    }
    total_volume = 0
    for week in weeks:
        query = {
            "to": where.user_address,
            "timestamp": {"$gte": week.start_dt, "$lte": week.end_dt},
        }
        cursor = db["swaps"].find(query, skip=skip, limit=first)
        for row in cursor:
            amount_usd = row['amount_usd'].to_decimal()
            if row['pair_id'] in STABLE_PAIRS:
                amount_usd *= STABLE_POOL_MULTIPLIER
            week.volume += amount_usd
            if week.volume >= WEEKLY_HARD_CAP:
                week.volume = WEEKLY_HARD_CAP
                break
        data[week.name] = week.volume
        total_volume += week.volume
    data['total_contest_volume'] = total_volume

    return VolumeContest.from_mongo(data)
