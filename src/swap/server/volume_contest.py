from decimal import Decimal
from typing import List, Optional
from dataclasses import dataclass, field
from datetime import datetime, date, timezone, timedelta

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.user import User, get_user


STABLE_PAIRS = {
    "0x5801bdad32f343035fb242e98d1e9371ae85bc1543962fedea16c59b35bd19b",    # USDC/USDT
    "0xcfd39f5244f7b617418c018204a8a9f9a7f72e71f0ef38f968eeb2a9ca302b",     # DAI/USDC
    "0xf0f5b3eed258344152e1f17baf84a2e1b621cd754b625bec169e8595aea767",     # DAI/USDT
    "0x70cda8400d7b1ee9e21f7194d320b9ad9c7a2b27e0d15a5a9967b9fefe10c76",     # wstETH/ETH
}

ELIGIBLE_PAIRS = [
    "0x4d0390b777b424e43839cd1e744799f3de6c176c7e32c1812a41dbd9c19db6a",    # ETH/USDC
    "0x45e7131d776dddc137e30bdd490b431c7144677e97bf9369f629ed8d3fb7dd6",    # ETH/USDT
    "0x7e2a13b40fc1119ec55e0bcf9428eedaa581ab3c924561ad4e955f95da63138",    # DAI/ETH
    "0x260e98362e0949fefff8b4de85367c035e44f734c9f8069b6ce2075ae86b45c",    # WBTC/ETH
    "0x2b3030c04e9c920bd66c6a8dc209717bbefa1ea5f8bc8ebabd639e5a4766502",    # LORDS/ETH
    "0x5a8054e5ca0b277b295a830e53bd71a6a6943b42d0dbb22329437522bc80c8",     # WBTC/USDC
    "0x44d13ad98a46fd2322ef2637e5e4c292ce8822f47b7cb9a1d581176a801c1a0",    # WBTC/USDT
    "0x39c183c8e5a2df130eefa6fbaa3b8aad89b29891f6272cb0c90deaa93ec6315",    # DAI/WBTC
    "0x7f409bd2e266e00486566dd3cb72bacc6996f49c0b19f04c0a8b5bd7bf991d1",    # LORDS/USDC
    "0x16220c67cdff746f2afd4178524a2dc9e49ff15567694277fa2302130576678",    # WBTC/wstETH
    "0x74855288dbb974584593acf7bd738572cce3d8f90a7076722d0a624a97d2620",    # wstETH/USDC
    "0x51184e312f09abcbf28132d6ef58259a6ebe9b5e7e32b5200427fdc96973f94",    # LORDS/USDT
    "0x56dc2aa83379f195de35ee699a270c76f1c2840b8b97385689d9137b38d9f44",    # DAI/LORDS
    "0x54a6698d6ac927713cf66c2f595948991e0a27e1b1ac04956c32026d94a8f99",    # LORDS/WBTC
    "0x33863afb8968fc40bc588a7c839faea1d47bb43d034b8ba19f0b8acb7191522",    # wstETH/USDT
    "0x781694f7f5f4dc9d7273e669ab0f9c8a0bd2d2279cc238e53522cd2e028c69c",    # LORDS/wstETH
    "0x73ffa5c873e39a2e8ea21494133081f4202b0dd583e50383a231b1f6f136a85",    # DAI/wstETH
] + list(STABLE_PAIRS)

WEEKLY_HARD_CAP = Decimal(1000)
STABLE_POOL_MULTIPLIER = Decimal(0.5)


@dataclass
class WeekData:
    id: int
    start_dt: datetime
    end_dt: datetime
    volume: Decimal = 0
    score: Decimal = 0
    name: str = field(init=False)

    def __post_init__(self):
        self.name = f'week_{self.id}'

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'start_dt': self.start_dt,
            'end_dt': self.end_dt,
            'volume': self.volume,
            'score': self.score,
        }

    @staticmethod
    def get_nft_level(total_score: Decimal):
        if total_score > 8000:
            return 1
        elif 6000 < total_score <= 7999:
            return 2
        elif 4000 < total_score <= 5999:
            return 3
        elif 2000 < total_score <= 3999:
            return 4
        elif 500 < total_score <= 1999:
            return 5
        else:
            return 0


@strawberry.type
class Week:
    id: int
    name: str
    start_dt: datetime
    end_dt: datetime
    volume: Decimal
    score: Decimal


@strawberry.type
class VolumeContest:
    user_id: strawberry.Private[str]
    weeks: List[Week]
    total_contest_score: Decimal
    total_contest_volume: Decimal
    nft_level: int

    @strawberry.field
    def user(self, info: Info) -> User:
        return get_user(info, self.user_id)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            user_id=data["user"],
            weeks=data["weeks"],
            total_contest_score=data["total_contest_score"],
            total_contest_volume=data["total_contest_volume"],
            nft_level=data["nft_level"],
        )


@strawberry.input
class WhereFilterForUserAddress:
    user: str
    start_date: date


async def get_volume_contest(
        info: Info, where: WhereFilterForUserAddress, first: Optional[int] = 100, skip: Optional[int] = 0
) -> VolumeContest:
    db: Database = info.context["db"]

    user = hex(int(where.user, 16))
    data = {
        'user': user,
        'weeks': [],
    }

    total_volume = Decimal(0)
    total_score = Decimal(0)

    dt = datetime.combine(where.start_date, datetime.min.time())
    utc_dt = dt.replace(tzinfo=timezone.utc)
    days_delta = 0
    for week_num in range(1, 9):
        # WeekData creation
        start_dt = utc_dt + timedelta(days=days_delta)
        days_delta += 7
        end_dt = utc_dt + timedelta(days=days_delta) - timedelta(milliseconds=1)
        week = WeekData(week_num, start_dt, end_dt)

        query = {
            "to": user,
            "timestamp": {"$gte": week.start_dt, "$lte": week.end_dt},
            "pair_id": {"$in": ELIGIBLE_PAIRS},
        }
        cursor = db["swaps"].find(query, skip=skip, limit=first)
        for row in cursor:
            amount_usd = row['amount_usd'].to_decimal()
            if row['pair_id'] in STABLE_PAIRS:
                amount_usd *= STABLE_POOL_MULTIPLIER
            week.volume += amount_usd
        week.score = min(week.volume, WEEKLY_HARD_CAP)
        data['weeks'].append(Week(**week.to_dict()))
        total_volume += week.volume
        total_score += week.score

    data['total_contest_score'] = total_score
    data['total_contest_volume'] = total_volume
    data['nft_level'] = WeekData.get_nft_level(total_score)
    return VolumeContest.from_mongo(data)
