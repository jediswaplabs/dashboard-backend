from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import BlockFilter, add_block_constraint, add_order_by_constraint


@strawberry.type
class Block:
    id: str
    
    number: int
    parent_hash: str
    timestamp: datetime

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data["hash"],
            number=data["number"],
            parent_hash=data["parent_hash"],
            timestamp=data["timestamp"]
        )

@strawberry.input
class WhereFilterForBlock:
    id: Optional[str] = None
    timestamp_lt: Optional[int] = None
    timestamp_gt: Optional[int] = None

async def get_blocks(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForBlock] = None
) -> List[Block]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.id is not None:
            block_id = int(where.id, 16)
            query["hash"] = hex(block_id)
        if where.timestamp_lt is not None:
            timestamp_lt = datetime.utcfromtimestamp(where.timestamp_lt)
            query["timestamp"] = {"$lt": timestamp_lt}
        if where.timestamp_gt is not None:
            timestamp_gt = datetime.utcfromtimestamp(where.timestamp_gt)
            query["timestamp"] = {**query.get("timestamp", dict()), **{"$gt": timestamp_gt}}

    cursor = db["blocks"].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Block.from_mongo(d) for d in cursor]
