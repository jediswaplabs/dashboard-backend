from decimal import Decimal
from bson import Decimal128
from typing import List, Optional
from dataclasses import field
from datetime import datetime

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import add_block_constraint, add_order_by_constraint
from swap.server.user import User, get_user

db_name_for_contest = "lp_contest_main_202305221253"
contest_start_block = 41080
contest_end_block = 200000


@strawberry.type
class LPContest:
    
    user_id: strawberry.Private[str]
    block: int
    timestamp: datetime
    contest_value: Decimal
    total_lp_value: Decimal
    total_time_eligible: int
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
            contest_value=Decimal128(data["contest_value"].to_decimal() / 10000),
            total_lp_value=data["total_lp_value"],
            total_time_eligible=data["total_time_eligible"],
            is_eligible=data["is_eligible"]
        )

@strawberry.input
class WhereFilterForLPContest:
    user: str

async def get_lp_contest(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc"
) -> List[LPContest]:
    db: Database = info.context["db"]

    query = dict()

    cursor = db[db_name_for_contest].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [LPContest.from_mongo(d) for d in cursor]

async def get_lp_contest_block(
    info: Info, where: WhereFilterForLPContest, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc"
) -> List[LPContest]:
    db: Database = info.context["db"]

    query = dict()

    if where is not None:
        if where.user is not None:
            user = hex(int(where.user, 16))
            query["user"] = user

    cursor = db[f"{db_name_for_contest}_block"].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [LPContest.from_mongo(d) for d in cursor]

@strawberry.type
class LPContestRanking:
    
    rank: Optional[int]
    total_eligible: Optional[int] = strawberry.field(name="totalEligible")
    percentile_rank: Optional[int] = strawberry.field(name="percentileRank")

    @classmethod
    def create_from_dict(cls, data):
        return cls(
            rank=data["rank"],
            total_eligible=data["total_eligible"],
            percentile_rank=data["percentile_rank"]
        )

async def get_lp_contest_percentile(
    info: Info, where: WhereFilterForLPContest) -> LPContestRanking:
    db: Database = info.context["db"]

    query = dict()

    if where is not None:
        if where.user is not None:
            user = hex(int(where.user, 16))
            query["user"] = user
    
    cursor = db[f"{db_name_for_contest}"].find(query)
    cursor = add_order_by_constraint(cursor)
    user_contest_value = [d for d in cursor][0]["contest_value"]

    pipeline = [
    {"$match": {"is_eligible": True}},
    {"$group": {"_id": None, "contest_values": {"$push": "$contest_value"}}},
    {"$project": {"count": {"$size": "$contest_values"}, "contest_values": 1}},
    {"$unwind": "$contest_values"},
    {"$sort": {"contest_values": -1}},
    {"$group": {"_id": None, "contest_values": {"$push": "$contest_values"}, "count": {"$first": "$count"}}},
    {
        "$project": {
            "contest_values": 1,
            "count": 1,
            "rank": {"$indexOfArray": ["$contest_values", user_contest_value]},
            "percentileRank": {
                "$multiply": [
                    {"$divide": [100, "$count"]},
                    {"$subtract": [{"$indexOfArray": ["$contest_values", user_contest_value]}, 0.5]},
                ]
            },
        }
    },
    ]
    cursor = db[f"{db_name_for_contest}"].aggregate(pipeline)
    answer = [d for d in cursor]
    answer_dict = dict()
    if len(answer) == 1:
        answer_dict["percentile_rank"] = round(answer[0]["percentileRank"])
        answer_dict["rank"] = answer[0]["rank"]
        answer_dict["total_eligible"] = answer[0]["count"]
    else:
        answer_dict["percentile_rank"] = None
        answer_dict["rank"] = None
        answer_dict["total_eligible"] = None
    return LPContestRanking.create_from_dict(answer_dict)

