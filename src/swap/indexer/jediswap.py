from decimal import Decimal
from typing import Union
from bson import Decimal128

from apibara import Info

from swap.indexer.context import IndexerContext
from swap.indexer.helpers import felt

from structlog import get_logger

logger = get_logger(__name__)

jediswap_factory = int(
    "0x00dad44c139a476c7a17fc8141e6db680e9abc9f56fe249a105094c44382c2fd", 16
)

zap_in_addresses = [int("0x029a303b928b9391ce797ec27d011d3937054bee783ca7831df792bae00c925c", 16)]

index_from_block = 10_760

_eth = int("049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7", 16)
_usdc = int("053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8", 16)

_whitelist = [
    # ETH
    _eth,
    # DAI
    int("00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3", 16),
    # USDC
    _usdc,
    # USDT
    int("068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8", 16),
    # wBTC
    int("03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac", 16),
]

# Value from starkscan
_eth_usdc_address = felt(
    2177149292491018417715774000056994188369467207221503622945886811766623165290
)

_minimum_liquidity_threshold_eth = Decimal("0")


async def get_eth_price(info: Info[IndexerContext]):
    """Returns ETH price using the price in the ETH-USDC pool."""
    pair = await info.storage.find_one("pairs", {"id": _eth_usdc_address})
    if pair is None:
        return None
    return pair["token1_price"].to_decimal()


async def find_eth_per_token(info: Info[IndexerContext], token: Union[int, bytes]):
    """Search through pools to find the price of token in eth."""
    if isinstance(token, int):
        token = felt(token)

    if token == felt(_eth): 
        return Decimal("1")

    for whitelisted in _whitelist:
        pair = await info.storage.find_one(
            "pairs", {"token0_id": token, "token1_id": felt(whitelisted)}
        )
        if pair is not None:
            if pair["reserve_eth"].to_decimal() >= _minimum_liquidity_threshold_eth:
                token1 = await info.storage.find_one(
                    "tokens", {"id": felt(whitelisted)}
                )
                token0_derived_eth = pair["token1_price"].to_decimal() * token1["derived_eth"].to_decimal()
                await info.storage.find_one_and_update(
                    "tokens", 
                    {"id": token}, 
                    {"$set": {"derived_eth": Decimal128(token0_derived_eth)}},
                    )
                return (token0_derived_eth)

        pair = await info.storage.find_one(
            "pairs", {"token1_id": token, "token0_id": felt(whitelisted)}
        )
        if pair is not None:
            if pair["reserve_eth"].to_decimal() >= _minimum_liquidity_threshold_eth:
                token0 = await info.storage.find_one(
                    "tokens", {"id": felt(whitelisted)}
                )
                token1_derived_eth = pair["token0_price"].to_decimal() * token0["derived_eth"].to_decimal()
                await info.storage.find_one_and_update(
                    "tokens", 
                    {"id": token}, 
                    {"$set": {"derived_eth": Decimal128(token1_derived_eth)}},
                    )
                return (token1_derived_eth)

    return Decimal("0")


async def get_tracked_liquidity_usd(
    info: Info[IndexerContext], token0, token0_amount, token1, token1_amount
):
    eth_usd = info.context.eth_price
    price0 = token0["derived_eth"].to_decimal() * eth_usd
    price1 = token1["derived_eth"].to_decimal() * eth_usd

    token0_whitelisted = False
    token1_whitelisted = False
    for whitelisted in _whitelist:
        if felt(whitelisted) == token0["id"]:
            token0_whitelisted = True
        if felt(whitelisted) == token1["id"]:
            token1_whitelisted = True

    # take average of the two
    if token0_whitelisted and token1_whitelisted:
        return token0_amount * price0 + token1_amount * price1

    # take twice the first
    if token0_whitelisted:
        return token0_amount * price0 * Decimal("2")

    # take twice the second
    if token1_whitelisted:
        return token1_amount * price1 * Decimal("2")

    # non-whitelisted asset
    return Decimal("0")


async def get_tracked_volume_usd(
    info: Info[IndexerContext], token0, token0_amount, token1, token1_amount, pair
):
    price0 = token0["derived_eth"].to_decimal() * info.context.eth_price
    price1 = token1["derived_eth"].to_decimal() * info.context.eth_price

    # return average price
    return (token0_amount * price0 + token1_amount * price1) / Decimal("2")
