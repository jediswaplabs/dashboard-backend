from typing import List

import strawberry

from swap.server.factory import Factory, get_factories
from swap.server.liquidity_position import (LiquidityPosition,
                                               get_liquidity_positions)
from swap.server.pair import Pair, get_pairs
from swap.server.token import Token, get_tokens
from swap.server.transaction import Transaction, get_transactions


@strawberry.type
class Query:
    uniswap_factories: List[Factory] = strawberry.field(resolver=get_factories)
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
    pairs: List[Pair] = strawberry.field(resolver=get_pairs)
    transaction: List[Transaction] = strawberry.field(resolver=get_transactions)
    liquidity_positions: List[LiquidityPosition] = strawberry.field(
        resolver=get_liquidity_positions
    )
