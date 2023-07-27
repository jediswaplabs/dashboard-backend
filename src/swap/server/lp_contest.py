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

db_name_for_contest = "lp_contest_1_final"
contest_start_block = 41080
contest_end_block = 125200


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
    contest_value: Optional[Decimal] = strawberry.field(name="contestValue")

    @classmethod
    def create_from_dict(cls, data):
        return cls(
            rank=data["rank"],
            total_eligible=data["total_eligible"],
            percentile_rank=data["percentile_rank"],
            contest_value=data["contest_value"]
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
        answer_dict["contest_value"] = Decimal128(user_contest_value.to_decimal() / 10000)
    else:
        answer_dict["percentile_rank"] = None
        answer_dict["rank"] = None
        answer_dict["total_eligible"] = None
        answer_dict["contest_value"] = Decimal128(user_contest_value.to_decimal() / 10000)
    return LPContestRanking.create_from_dict(answer_dict)


@strawberry.type
class LPContestNFTRanks:
    
    L1P1_start: Optional[int]
    L1P1_end: Optional[int]
    L1P2_start: Optional[int]
    L1P2_end: Optional[int]
    L1P3_start: Optional[int]
    L1P3_end: Optional[int]
    L1P4_start: Optional[int]
    L1P4_end: Optional[int]
    L1P5_start: Optional[int]
    L1P5_end: Optional[int]

    @classmethod
    def create_from_dict(cls, data):
        return cls(
            L1P1_start=data["L1P1_start"],
            L1P1_end=data["L1P1_end"],
            L1P2_start=data["L1P2_start"],
            L1P2_end=data["L1P2_end"],
            L1P3_start=data["L1P3_start"],
            L1P3_end=data["L1P3_end"],
            L1P4_start=data["L1P4_start"],
            L1P4_end=data["L1P4_end"],
            L1P5_start=data["L1P5_start"],
            L1P5_end=data["L1P5_end"]
        )

async def get_lp_contest_nft_rank(
    info: Info) -> LPContestNFTRanks:
    db: Database = info.context["db"]

    query = dict()
    query["is_eligible"] = True
    
    cursor = db[f"{db_name_for_contest}"].find(query)
    total_eligible = len(list(cursor))
    L1P1_start = 11
    L1P1_end = int((2 * total_eligible) / 100)
    L1P2_start = L1P1_end + 1
    L1P2_end = int((10 * total_eligible) / 100)
    L1P3_start = L1P2_end + 1
    L1P3_end = int((25 * total_eligible) / 100)
    L1P4_start = L1P3_end + 1
    L1P4_end = int((55 * total_eligible) / 100)
    L1P5_start = L1P4_end + 1
    L1P5_end = total_eligible
    answer_dict = dict(
        L1P1_start = L1P1_start,
        L1P1_end = L1P1_end,
        L1P2_start = L1P2_start,
        L1P2_end = L1P2_end,
        L1P3_start = L1P3_start,
        L1P3_end = L1P3_end,
        L1P4_start = L1P4_start,
        L1P4_end = L1P4_end,
        L1P5_start = L1P5_start,
        L1P5_end = L1P5_end
    )
    return LPContestNFTRanks.create_from_dict(answer_dict)

