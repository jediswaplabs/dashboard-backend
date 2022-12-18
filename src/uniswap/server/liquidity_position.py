from datetime import datetime
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from uniswap.server.helpers import (FieldElement, add_block_constraint,
                                    serialize_hex)
from uniswap.server.pair import Pair, get_pair
from uniswap.server.token import Token, get_token


@strawberry.type
class User:
    id: FieldElement

    @strawberry.field
    def liquidity_positions(self, info: Info) -> List["LiquidityPosition"]:
        return []


@strawberry.type
class LiquidityPosition:
    user_id: strawberry.Private[FieldElement]
    pair_id: strawberry.Private[FieldElement]

    liquidity_token_balance: Decimal

    @strawberry.field
    def id(self) -> str:
        return f"{serialize_hex(self.pair_id)}-${serialize_hex(self.user_id)}"

    @classmethod
    def from_mongo(cls, data):
        return cls(
            user_id=data["user"],
            pair_id=data["pair_address"],
            liquidity_token_balance=data["liquidity_token_balance"].to_decimal(),
        )

    @strawberry.field
    def user(self) -> User:
        return User(id=self.user_id)

    @strawberry.field
    def pair(self, info: Info) -> Pair:
        return get_pair(info, self.pair_id)


def get_liquidity_positions(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0
) -> List[LiquidityPosition]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    cursor = db["liquidity_positions"].find(query, skip=skip, limit=first)

    return [LiquidityPosition.from_mongo(d) for d in cursor]
