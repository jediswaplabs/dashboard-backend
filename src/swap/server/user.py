from decimal import Decimal
from typing import List, Optional
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import FieldElement, BlockFilter, felt, add_block_constraint, add_order_by_constraint


@strawberry.type
class User:
    id: FieldElement
    
    transaction_count: Decimal = strawberry.field(name="txCount")
    mint_count: Decimal
    burn_count: Decimal
    swap_count: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data["id"],
            transaction_count=data["transaction_count"],
            mint_count=data["mint_count"],
            burn_count=data["burn_count"],
            swap_count=data["swap_count"]
        )

@strawberry.input
class WhereFilterForUser:
    id: Optional[str] = None
    id_in: Optional[List[str]] = field(default_factory=list)

async def get_users(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", block: Optional[BlockFilter] = None, where: Optional[WhereFilterForUser] = None
) -> List[User]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, block)

    if where is not None:
        if where.id is not None:
            user_id = int(where.id, 16)
            query["id"] = felt(user_id)
        if where.id_in:
            user_in = []
            for user_id in where.id_in:
                user_in.append(felt(int(user_id, 16)))
            query["id"] = {"$in": user_in}

    cursor = db["users"].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [User.from_mongo(d) for d in cursor]

def get_user(info: Info, id: bytes) -> User:
    db: Database = info.context["db"]

    query = {"id": id}
    add_block_constraint(query, None)

    pair = db["users"].find_one(query)
    return User.from_mongo(pair)
