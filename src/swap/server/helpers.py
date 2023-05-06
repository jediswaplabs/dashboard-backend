from typing import NewType, Optional, TypeVar, Generic
from pymongo import ASCENDING, DESCENDING
from pymongo.cursor import CursorType

import strawberry

@strawberry.input
class BlockFilter:
    number: Optional[int]

T = TypeVar("T")

@strawberry.input
class EQFilter(Generic[T]):
    eq: Optional[T] = None

@strawberry.input
class GTLTFilter(Generic[T]):
    gt: Optional[T] = None
    lt: Optional[T] = None


def add_block_constraint(query: dict, block: Optional[BlockFilter]):
    if block is None or block.number is None:
        query["_chain.valid_to"] = None
    else:
        query["$or"] = [
            {
                "$and": [
                    {"_chain.valid_to": None},
                    {"_chain.valid_from": {"$lte": block.number}},
                ]
            },
            {
                "$and": [
                    {"_chain.valid_to": {"$gt": block.number}},
                    {"_chain.valid_from": {"$lte": block.number}},
                ]
            },
        ]

def add_order_by_constraint(cursor: CursorType, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc") -> CursorType:
    if orderBy:
        if orderByDirection == "asc":
            cursor = cursor.sort(orderBy, ASCENDING)
        else:
            cursor = cursor.sort(orderBy, DESCENDING)
    return cursor