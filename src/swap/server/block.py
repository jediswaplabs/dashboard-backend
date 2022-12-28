from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import FieldElement, BlockFilter, felt, add_block_constraint, add_order_by_constraint


@strawberry.type
class Block:
    id: FieldElement
    
    number: int
    parent_hash: FieldElement
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

async def get_blocks(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForBlock] = None
) -> List[Block]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.id is not None:
            block_id = int(where.id, 16)
            query["hash"] = felt(block_id)

    cursor = db["blocks"].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Block.from_mongo(d) for d in cursor]
