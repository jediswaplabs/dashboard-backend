from datetime import datetime
from decimal import Decimal
from tokenize import String
from typing import List, Optional
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from swap.server.helpers import (FieldElement, felt, add_block_constraint, add_order_by_constraint, serialize_hex)
from swap.server.pair import Pair


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
    index: strawberry.Private[int]
    pair_id: strawberry.Private[FieldElement]

    transaction_hash: FieldElement
    timestamp: datetime
    sender: FieldElement
    to: FieldElement
    liquidity: Decimal
    amount0: Decimal
    amount1: Decimal
    amount_usd: Decimal = strawberry.field(name="amountUSD")
    zap_in: bool

    @strawberry.field
    def id(self) -> str:
        return f"{serialize_hex(self.transaction_hash)}-{self.index}"

    @strawberry.field
    def pair(self, info: Info) -> Pair:
        return info.context["pair_loader"].load(self.pair_id)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            transaction_hash=data["transaction_hash"],
            timestamp=data["timestamp"],
            index=data["index"],
            pair_id=data["pair_id"],
            sender=data.get("sender", felt(0)),
            to=data["to"],
            liquidity=data["liquidity"].to_decimal(),
            amount0=data["amount0"].to_decimal(),
            amount1=data["amount1"].to_decimal(),
            amount_usd=data["amount_usd"].to_decimal(),
            zap_in=data.get("zap_in", False),
        )


@strawberry.type
class Burn:
    index: strawberry.Private[int]
    pair_id: strawberry.Private[FieldElement]

    transaction_hash: FieldElement
    timestamp: datetime
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
    def pair(self, info: Info) -> Pair:
        return info.context["pair_loader"].load(self.pair_id)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            transaction_hash=data["transaction_hash"],
            timestamp=data["timestamp"],
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
    index: strawberry.Private[int]
    pair_id: strawberry.Private[FieldElement]

    transaction_hash: FieldElement
    timestamp: datetime
    pair_id: FieldElement
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
    def pair(self, info: Info) -> Pair:
        return info.context["pair_loader"].load(self.pair_id)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            transaction_hash=data["transaction_hash"],
            timestamp=data["timestamp"],
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

@strawberry.input
class WhereFilterForTransaction:
    id: Optional[FieldElement] = None

async def get_transactions(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForTransaction] = None
) -> List[Transaction]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.id is not None:
            query["hash"] = where.id

    cursor = db["transactions"].find(query, limit=first, skip=skip)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)
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

@strawberry.input
class WhereFilterForMintandSwap:
    pair: Optional[str] = None
    pair_in: Optional[List[str]] = field(default_factory=list)
    to: Optional[str] = None

async def get_swaps(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForMintandSwap] = None
) -> List[Swap]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.pair is not None:
            pair_id = int(where.pair, 16)
            query["pair_id"] = felt(pair_id)
        if where.pair_in:
            pair_in = []
            for pair in where.pair_in:
                pair_in.append(felt(int(pair, 16)))
            query["pair_id"] = {"$in": pair_in}
        if where.to is not None:
            to = int(where.to, 16)
            query["to"] = felt(to)

    cursor = db["swaps"].find(query, limit=first, skip=skip)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [Swap.from_mongo(d) for d in cursor]

async def get_mints(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForMintandSwap] = None
) -> List[Mint]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.pair is not None:
            pair_id = int(where.pair, 16)
            query["pair_id"] = felt(pair_id)
        if where.pair_in:
            pair_in = []
            for pair in where.pair_in:
                pair_in.append(felt(int(pair, 16)))
            query["pair_id"] = {"$in": pair_in}
        if where.to is not None:
            to = int(where.to, 16)
            query["to"] = felt(to)
    
    cursor = db["mints"].find(query, limit=first, skip=skip)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [Mint.from_mongo(d) for d in cursor]

@strawberry.input
class WhereFilterForBurn:
    pair: Optional[str] = None
    pair_in: Optional[List[str]] = field(default_factory=list)
    sender: Optional[str] = None

async def get_burns(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, orderByDirection: Optional[str] = "asc", where: Optional[WhereFilterForBurn] = None
) -> List[Mint]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, None)

    if where is not None:
        if where.pair is not None:
            pair_id = int(where.pair, 16)
            query["pair_id"] = felt(pair_id)
        if where.pair_in:
            pair_in = []
            for pair in where.pair_in:
                pair_in.append(felt(int(pair, 16)))
            query["pair_id"] = {"$in": pair_in}
        if where.sender is not None:
            sender = int(where.sender, 16)
            query["sender"] = felt(sender)

    cursor = db["burns"].find(query, limit=first, skip=skip)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [Mint.from_mongo(d) for d in cursor]
