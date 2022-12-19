from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import FieldElement, add_block_constraint


@strawberry.type
class Token:
    id: FieldElement

    name: str
    symbol: str
    decimals: int

    total_liquidity: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data["id"],
            name=data["name"],
            symbol=data["symbol"],
            decimals=data["decimals"],
            total_liquidity=data["total_liquidity"].to_decimal(),
        )


async def get_tokens(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0
) -> List[Token]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    cursor = db["tokens"].find(query, skip=skip, limit=first)

    return [Token.from_mongo(d) for d in cursor]


async def get_token(info: Info, id: bytes):
    db: Database = info.context["db"]

    query = {"id": id}
    add_block_constraint(query, None)

    token = db["tokens"].find_one(query)
    return Token.from_mongo(token)
