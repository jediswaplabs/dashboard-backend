from datetime import datetime
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import (FieldElement, add_block_constraint,
                                    serialize_hex)


@strawberry.type
class Transaction:
    id: FieldElement
    timestamp: datetime

    @strawberry.field
    def mints(self, info) -> List["Mint"]:
        return get_transaction_mints(info, self)

    @strawberry.field
    def burns(self, info) -> List["Burn"]:
        return get_transaction_burns(info, self)

    @strawberry.field
    def swaps(self, info) -> List["Swap"]:
        return get_transaction_swaps(info, self)

    @classmethod
    def from_mongo(cls, data):
        return cls(id=data["hash"], timestamp=data["block_timestamp"])


@strawberry.type
class Mint:
    transaction_hash: strawberry.Private[FieldElement]
    index: strawberry.Private[int]
    pair_id: strawberry.Private[FieldElement]

    sender: FieldElement
    to: FieldElement
    liquidity: Decimal
    amount0: Decimal
    amount1: Decimal
    amount_usd: Decimal = strawberry.field(name="amountUSD")

    @strawberry.field
    def id(self) -> str:
        return f"{serialize_hex(self.transaction_hash)}-{self.index}"

    @strawberry.field
    def transaction(self, info) -> Transaction:
        return get_transaction(info, self.transaction_hash)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            transaction_hash=data["transaction_hash"],
            index=data["index"],
            pair_id=data["pair_id"],
            sender=data["sender"],
            to=data["to"],
            liquidity=data["liquidity"].to_decimal(),
            amount0=data["amount0"].to_decimal(),
            amount1=data["amount1"].to_decimal(),
            amount_usd=data["amount_usd"].to_decimal(),
        )


@strawberry.type
class Burn:
    transaction_hash: strawberry.Private[FieldElement]
    index: strawberry.Private[int]
    pair_id: strawberry.Private[FieldElement]

    sender: FieldElement
    to: FieldElement
    liquidity: Decimal
    amount0: Decimal
    amount1: Decimal
    amount_usd: Decimal = strawberry.field(name="amountUSD")

    @strawberry.field
    def id(self) -> str:
        return f"{serialize_hex(self.transaction_hash)}-{self.index}"

    @strawberry.field
    def transaction(self, info) -> Transaction:
        return get_transaction(info, self.transaction_hash)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            transaction_hash=data["transaction_hash"],
            index=data["index"],
            pair_id=data["pair_id"],
            sender=data["sender"],
            to=data["to"],
            liquidity=data["liquidity"].to_decimal(),
            amount0=data["amount0"].to_decimal(),
            amount1=data["amount1"].to_decimal(),
            amount_usd=data["amount_usd"].to_decimal(),
        )


@strawberry.type
class Swap:
    transaction_hash: strawberry.Private[FieldElement]
    index: strawberry.Private[int]
    pair_id: strawberry.Private[FieldElement]

    sender: FieldElement
    to: FieldElement
    amount0_in: Decimal
    amount0_out: Decimal
    amount1_in: Decimal
    amount1_out: Decimal
    amount_usd: Decimal = strawberry.field(name="amountUSD")

    @strawberry.field
    def id(self) -> str:
        return f"{serialize_hex(self.transaction_hash)}-{self.index}"

    @strawberry.field
    def transaction(self, info) -> Transaction:
        return get_transaction(info, self.transaction_hash)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            transaction_hash=data["transaction_hash"],
            index=data["log_index"],
            pair_id=data["pair_id"],
            sender=data["sender"],
            to=data["to"],
            amount0_in=data["amount0_in"].to_decimal(),
            amount0_out=data["amount0_out"].to_decimal(),
            amount1_in=data["amount1_in"].to_decimal(),
            amount1_out=data["amount1_out"].to_decimal(),
            amount_usd=data["amount_usd"].to_decimal(),
        )


async def get_transactions(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0
) -> List[Transaction]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    cursor = db["transactions"].find(query, limit=first, skip=skip)
    return [Transaction.from_mongo(d) for d in cursor]


def get_transaction(info: Info, hash: FieldElement) -> Transaction:
    db: Database = info.context["db"]

    query = {"hash": hash}
    add_block_constraint(query, None)

    transaction = db["transactions"].find_one(query)
    return Transaction.from_mongo(transaction)


def get_transaction_mints(info: Info, root) -> List[Mint]:
    db: Database = info.context["db"]

    query = {"transaction_hash": root.id}
    add_block_constraint(query, None)

    cursor = db["mints"].find(query)
    return [Mint.from_mongo(d) for d in cursor]


def get_transaction_burns(info: Info, root) -> List[Burn]:
    db: Database = info.context["db"]

    query = {"transaction_hash": root.id}
    add_block_constraint(query, None)

    cursor = db["burns"].find(query)
    return [Burn.from_mongo(d) for d in cursor]


def get_transaction_swaps(info: Info, root) -> List[Swap]:
    db: Database = info.context["db"]

    query = {"transaction_hash": root.id}
    add_block_constraint(query, None)

    cursor = db["swaps"].find(query)
    return [Swap.from_mongo(d) for d in cursor]
