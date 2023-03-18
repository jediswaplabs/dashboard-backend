from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database

from swap.server.helpers import (BlockFilter, add_block_constraint)


@strawberry.input
class FactoryFilter:
    id: Optional[str]


@strawberry.type
class Factory:
    id: str

    pair_count: int

    total_volume_usd: Decimal = strawberry.field(name="totalVolumeUSD")
    total_volume_eth: Decimal = strawberry.field(name="totalVolumeETH")

    untracked_volume_usd: Decimal = strawberry.field(name="untrackedVolumeUSD")

    total_liquidity_usd: Decimal = strawberry.field(name="totalLiquidityUSD")
    total_liquidity_eth: Decimal = strawberry.field(name="totalLiquidityETH")

    tx_count: int

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data["id"],
            pair_count=data["pair_count"],
            tx_count=data["transaction_count"],
            total_volume_usd=data["total_volume_usd"].to_decimal(),
            total_volume_eth=data["total_volume_eth"].to_decimal(),
            untracked_volume_usd=data["untracked_volume_usd"].to_decimal(),
            total_liquidity_usd=data["total_liquidity_usd"].to_decimal(),
            total_liquidity_eth=data["total_liquidity_eth"].to_decimal(),
        )


async def get_factories(
    info, block: Optional[BlockFilter] = None, where: Optional[FactoryFilter] = None
) -> List[Factory]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, block)

    if where is not None:
        if where.id is not None:
            factory_id = hex(int(where.id, 16))
            query["id"] = factory_id
    cursor = db["factories"].find(query)
    return [Factory.from_mongo(d) for d in cursor]
