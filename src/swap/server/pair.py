from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import FieldElement, BlockFilter, felt, add_block_constraint, add_order_by_constraint
from swap.server.token import Token, get_token


async def get_pair_token0(info: Info, root) -> Token:
    return await get_token(info, root.token0_id)


async def get_pair_token1(info: Info, root) -> Token:
    return await get_token(info, root.token1_id)


@strawberry.type
class Pair:
    id: FieldElement

    tx_count: int
    reserve0: Decimal
    reserve1: Decimal
    reserve_usd: Decimal = strawberry.field(name="reserveUSD")
    total_supply: Decimal
    tracked_reserve_eth: Decimal = strawberry.field(name="trackedReserveETH")
    reserve_eth: Decimal = strawberry.field(name="reserveETH")
    volume_token0: Decimal = strawberry.field(name="volumeToken0")
    volume_token1: Decimal = strawberry.field(name="volumeToken1")
    volume_usd: Decimal = strawberry.field(name="volumeUSD")
    untracked_volume_usd: Decimal = strawberry.field(name="untrackedVolumeUSD")
    token0_price: Decimal
    token1_price: Decimal
    created_at_timestamp: datetime

    token0_id: strawberry.Private[FieldElement]
    token0: Token = strawberry.field(resolver=get_pair_token0)

    token1_id: strawberry.Private[FieldElement]
    token1: Token = strawberry.field(resolver=get_pair_token1)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data["id"],
            tx_count=data["transaction_count"],
            reserve0=data["reserve0"],
            reserve1=data["reserve1"],
            reserve_usd=data["reserve_usd"].to_decimal(),
            total_supply=data["total_supply"].to_decimal(),
            tracked_reserve_eth=data["tracked_reserve_eth"].to_decimal(),
            reserve_eth=data["reserve_eth"].to_decimal(),
            volume_token0=data["volume_token0"].to_decimal(),
            volume_token1=data["volume_token1"].to_decimal(),
            volume_usd=data["volume_usd"].to_decimal(),
            untracked_volume_usd=data["untracked_volume_usd"].to_decimal(),
            token0_price=data["token0_price"].to_decimal(),
            token1_price=data["token1_price"].to_decimal(),
            created_at_timestamp=data["created_at_timestamp"],
            token0_id=data["token0_id"],
            token1_id=data["token1_id"],
        )

@strawberry.input
class WhereFilterForPair:
    id: Optional[str] = None
    id_in: Optional[List[str]] = field(default_factory=list)
    token0: Optional[str] = None
    token1: Optional[str] = None

async def get_pairs(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", block: Optional[BlockFilter] = None, where: Optional[WhereFilterForPair] = None
) -> List[Pair]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, block)

    if where is not None:
        if where.id is not None:
            pair_id = int(where.id, 16)
            query["id"] = felt(pair_id)
        if where.id_in:
            pair_in = []
            for pair_id in where.id_in:
                pair_in.append(felt(int(pair_id, 16)))
            query["id"] = {"$in": pair_in}
        if where.token0 is not None:
            token0_id = int(where.token0, 16)
            query["token0_id"] = felt(token0_id)
        if where.token1 is not None:
            token1_id = int(where.token1, 16)
            query["token1_id"] = felt(token1_id)

    cursor = db["pairs"].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Pair.from_mongo(d) for d in cursor]


def get_pair(info: Info, id: bytes) -> Pair:
    db: Database = info.context["db"]

    query = {"id": id}
    add_block_constraint(query, None)

    pair = db["pairs"].find_one(query)
    return Pair.from_mongo(pair)
