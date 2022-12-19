from typing import NewType, Optional

import strawberry


def parse_hex(value):
    if not value.startswith("0x"):
        raise ValueError("invalid Hex value")
    return bytes.fromhex(value.replace("0x", ""))


def serialize_hex(token_id):
    return "0x" + token_id.hex()


FieldElement = strawberry.scalar(
    NewType("FieldElement", bytes), parse_value=parse_hex, serialize=serialize_hex
)


@strawberry.input
class BlockFilter:
    number: Optional[int]


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
