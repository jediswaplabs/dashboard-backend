from typing import List

import strawberry

from uniswap.server.factory import Factory, get_factories
from uniswap.server.pair import Pair, get_pairs
from uniswap.server.token import Token, get_tokens
from uniswap.server.transaction import Transaction, get_transactions


@strawberry.type
class Query:
    uniswap_factories: List[Factory] = strawberry.field(resolver=get_factories)
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
    pairs: List[Pair] = strawberry.field(resolver=get_pairs)
    transaction: List[Transaction] = strawberry.field(resolver=get_transactions)
