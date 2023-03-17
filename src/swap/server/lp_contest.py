from decimal import Decimal
from typing import List, Optional
from dataclasses import field
from datetime import datetime

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import FieldElement, BlockFilter, felt, add_block_constraint, add_order_by_constraint
from swap.server.user import User, get_user

db_name_for_contest = "lp_contest_67812345"
contest_start_block = 22550
contest_end_block = 27100


@strawberry.type
class LPContest:
    
    user_id: strawberry.Private[FieldElement]
    block: int
    timestamp: datetime
    contest_value: Decimal
    total_lp_value: Decimal
    total_blocks_eligible: int
    is_eligible: bool

    @strawberry.field
    def user(self, info: Info) -> User:
        return get_user(info, self.user_id)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            user_id=data["user"],
            block=data["block"],
            timestamp=data["timestamp"],
            contest_value=data["contest_value"],
            total_lp_value=data["total_lp_value"],
            total_blocks_eligible=data["total_blocks_eligible"],
            is_eligible=data["is_eligible"]
        )

@strawberry.input
class WhereFilterForLPContest:
    user: Optional[str] = None

async def get_lp_contest(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc"
) -> List[LPContest]:
    db: Database = info.context["db"]

    query = dict()

    cursor = db[db_name_for_contest].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [LPContest.from_mongo(d) for d in cursor]

async def get_lp_contest_block(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForLPContest] = None
) -> List[LPContest]:
    db: Database = info.context["db"]

    query = dict()

    if where is not None:
        if where.user is not None:
            user = int(where.user, 16)
            query["user"] = felt(user)

    cursor = db[f"{db_name_for_contest}_block"].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [LPContest.from_mongo(d) for d in cursor]

