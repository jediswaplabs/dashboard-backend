from decimal import Decimal
from typing import List, Optional
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import BlockFilter, add_block_constraint, add_order_by_constraint


@strawberry.type
class Token:
    id: str

    name: str
    symbol: str
    decimals: int

    derived_eth: Decimal = strawberry.field(name="derivedETH")
    trade_volume: Decimal
    trade_volume_usd: Decimal = strawberry.field(name="tradeVolumeUSD")
    untracked_volume_usd: Decimal = strawberry.field(name="untrackedVolumeUSD")
    total_liquidity: Decimal
    transaction_count: Decimal = strawberry.field(name="txCount")

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data["id"],
            name=data["name"],
            symbol=data["symbol"],
            decimals=data["decimals"],
            derived_eth=data["derived_eth"].to_decimal(),
            trade_volume=data["trade_volume"].to_decimal(),
            trade_volume_usd=data["trade_volume_usd"].to_decimal(),
            untracked_volume_usd=data["untracked_volume_usd"].to_decimal(),
            total_liquidity=data["total_liquidity"].to_decimal(),
            transaction_count=data["transaction_count"]
        )

@strawberry.input
class WhereFilterForToken:
    id: Optional[str] = None
    id_in: Optional[List[str]] = field(default_factory=list)

async def get_tokens(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", block: Optional[BlockFilter] = None, where: Optional[WhereFilterForToken] = None
) -> List[Token]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, block)

    if where is not None:
        if where.id is not None:
            token_id = hex(int(where.id, 16))
            query["id"] = token_id
        if where.id_in:
            token_in = []
            for token in where.id_in:
                token_in.append(hex(int(token, 16)))
            query["id"] = {"$in": token_in}

    cursor = db["tokens"].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Token.from_mongo(d) for d in cursor]


def get_token(db: Database, id: str) -> Token:
    # db: Database = info.context["db"]

    query = {"id": id}
    add_block_constraint(query, None)

    token = db["tokens"].find_one(query)
    return Token.from_mongo(token)
