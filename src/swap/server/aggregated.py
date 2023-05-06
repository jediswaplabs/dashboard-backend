from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Mapping
from bson import Decimal128
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import add_block_constraint, add_order_by_constraint


@strawberry.type
class ExchangeDayData:
    id: str
    day_id: int
    date: datetime

    total_volume_usd: Decimal = strawberry.field(name="totalVolumeUSD")
    daily_volume_usd: Decimal = strawberry.field(name="dailyVolumeUSD")
    daily_volume_eth: Decimal = strawberry.field(name="dailyVolumeETH")
    total_liquidity_usd: Decimal = strawberry.field(name="totalLiquidityUSD")
    total_liquidity_eth: Decimal = strawberry.field(name="totalLiquidityETH")

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data["address"],
            day_id=data["day_id"],
            date=data["date"],
            total_volume_usd=data.get("total_volume_usd", Decimal128("0")).to_decimal(),
            daily_volume_usd=data.get("daily_volume_usd", Decimal128("0")).to_decimal(),
            daily_volume_eth=data.get("daily_volume_eth", Decimal128("0")).to_decimal(),
            total_liquidity_usd=data["total_liquidity_usd"].to_decimal(),
            total_liquidity_eth=data["total_liquidity_eth"].to_decimal()
        )

@strawberry.input
class WhereFilterForExchangeDayData:
    date_lt: Optional[int] = None
    date_gt: Optional[int] = None

async def get_exchange_day_datas(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForExchangeDayData] = None
) -> List[ExchangeDayData]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.date_lt is not None:
            date_lt = datetime.fromtimestamp(where.date_lt)
            query["date"] = {"$lt": date_lt}
        if where.date_gt is not None:
            date_gt = datetime.fromtimestamp(where.date_gt)
            query["date"] = {**query.get("date", dict()), **{"$gt": date_gt}}

    cursor = db["exchange_day_data"].find(query, limit=first, skip=skip)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [ExchangeDayData.from_mongo(d) for d in cursor]


@strawberry.type
class PairDayData:
    pair_id: str
    day_id: int
    date: datetime

    daily_volume_token0: Decimal = strawberry.field(name="dailyVolumeToken0")
    daily_volume_token1: Decimal = strawberry.field(name="dailyVolumeToken1")
    daily_volume_usd: Decimal = strawberry.field(name="dailyVolumeUSD")
    total_supply: Decimal = strawberry.field(name="totalSupply")
    reserve_usd: Decimal = strawberry.field(name="reserveUSD")
    token0_price: Decimal = strawberry.field(name="token0Price")
    token1_price: Decimal = strawberry.field(name="token1Price")

    @classmethod
    def from_mongo(cls, data):
        return cls(
            pair_id=data["pair_id"],
            day_id=data["day_id"],
            date=data["date"],
            daily_volume_token0=data.get("daily_volume_token0", Decimal128("0")).to_decimal(),
            daily_volume_token1=data.get("daily_volume_token1", Decimal128("0")).to_decimal(),
            daily_volume_usd=data.get("daily_volume_usd", Decimal128("0")).to_decimal(),
            total_supply=data["total_supply"].to_decimal(),
            reserve_usd=data["reserve_usd"].to_decimal(),
            token0_price=data.get("token0_price", Decimal128("0")).to_decimal(),
            token1_price=data.get("token1_price", Decimal128("0")).to_decimal()
        )

@strawberry.input
class WhereFilterForPairDayData:
    pair: Optional[str] = None
    pair_in: Optional[List[str]] = field(default_factory=list)
    date_lt: Optional[int] = None
    date_gt: Optional[int] = None

async def get_pair_day_datas(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForPairDayData] = None
) -> List[PairDayData]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.pair is not None:
            pair_id = int(where.pair, 16)
            query["pair_id"] = hex(pair_id)
        if where.pair_in:
            pair_in = []
            for pair in where.pair_in:
                pair_in.append(hex(int(pair, 16)))
            query["pair_id"] = {"$in": pair_in}
        if where.date_lt is not None:
            date_lt = datetime.fromtimestamp(where.date_lt)
            query["date"] = {"$lt": date_lt}
        if where.date_gt is not None:
            date_gt = datetime.fromtimestamp(where.date_gt)
            query["date"] = {**query.get("date", dict()), **{"$gt": date_gt}}

    cursor = db["pair_day_data"].find(query, limit=first, skip=skip)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [PairDayData.from_mongo(d) for d in cursor]


@strawberry.type
class TokenDayData:
    token_id: str
    day_id: int
    date: datetime

    price_usd: Decimal = strawberry.field(name="priceUSD")
    total_liquidity_token: Decimal = strawberry.field(name="totalLiquidityToken")
    total_liquidity_eth: Decimal = strawberry.field(name="totalLiquidityETH")
    total_liquidity_usd: Decimal = strawberry.field(name="totalLiquidityUSD")
    daily_volume_token: Decimal = strawberry.field(name="dailyVolumeToken")
    daily_volume_eth: Decimal = strawberry.field(name="dailyVolumeETH")
    daily_volume_usd: Decimal = strawberry.field(name="dailyVolumeUSD")

    @classmethod
    def from_mongo(cls, data):
        return cls(
            token_id=data["token_id"],
            day_id=data["day_id"],
            date=data["date"],
            price_usd=data["price_usd"].to_decimal(),
            total_liquidity_token=data["total_liquidity_token"].to_decimal(),
            total_liquidity_eth=data["total_liquidity_eth"].to_decimal(),
            total_liquidity_usd=data["total_liquidity_usd"].to_decimal(),
            daily_volume_token=data.get("daily_volume_token", Decimal128("0")).to_decimal(),
            daily_volume_eth=data.get("daily_volume_eth", Decimal128("0")).to_decimal(),
            daily_volume_usd=data.get("daily_volume_usd", Decimal128("0")).to_decimal()
        )

@strawberry.input
class WhereFilterForTokenDayData:
    token: Optional[str] = None
    date_lt: Optional[int] = None
    date_gt: Optional[int] = None


async def get_token_day_datas(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForTokenDayData] = None
) -> List[TokenDayData]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.token is not None:
            token_id = int(where.token, 16)
            query["token_id"] = hex(token_id)
        if where.date_lt is not None:
            date_lt = datetime.fromtimestamp(where.date_lt)
            query["date"] = {"$lt": date_lt}
        if where.date_gt is not None:
            date_gt = datetime.fromtimestamp(where.date_gt)
            query["date"] = {**query.get("date", dict()), **{"$gt": date_gt}}

    cursor = db["token_day_data"].find(query, limit=first, skip=skip)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [TokenDayData.from_mongo(d) for d in cursor]
