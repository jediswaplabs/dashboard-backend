from typing import List

import strawberry

from swap.server.block import Block, get_blocks
from swap.server.factory import Factory, get_factories
from swap.server.liquidity_position import (LiquidityPosition,
                                               get_liquidity_positions)
from swap.server.pair import Pair, get_pairs
from swap.server.token import Token, get_tokens
from swap.server.transaction import Transaction, get_transactions
from swap.server.transaction import Swap, get_swaps
from swap.server.transaction import Mint, get_mints
from swap.server.transaction import Burn, get_burns
from swap.server.aggregated import ExchangeDayData, get_exchange_day_datas
from swap.server.aggregated import PairDayData, get_pair_day_datas
from swap.server.aggregated import TokenDayData, get_token_day_datas


@strawberry.type
class Query:
    blocks: List[Block] = strawberry.field(resolver=get_blocks)
    jediswap_factories: List[Factory] = strawberry.field(resolver=get_factories)
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
    pairs: List[Pair] = strawberry.field(resolver=get_pairs)
    transactions: List[Transaction] = strawberry.field(resolver=get_transactions)
    swaps: List[Swap] = strawberry.field(resolver=get_swaps)
    mints: List[Mint] = strawberry.field(resolver=get_mints)
    burns: List[Burn] = strawberry.field(resolver=get_burns)
    liquidity_positions: List[LiquidityPosition] = strawberry.field(
        resolver=get_liquidity_positions)
    exchange_day_datas: List[ExchangeDayData] = strawberry.field(resolver=get_exchange_day_datas)
    pair_day_datas: List[PairDayData] = strawberry.field(resolver=get_pair_day_datas)
    token_day_datas: List[TokenDayData] = strawberry.field(resolver=get_token_day_datas)
