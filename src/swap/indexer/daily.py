from datetime import datetime
from typing import Union

from apibara import Info
from bson import Decimal128

from swap.indexer.context import IndexerContext
from swap.indexer.helpers import felt

from structlog import get_logger

logger = get_logger(__name__)


async def snapshot_pair_day_data(info: Info[IndexerContext], pair_address: int):
    pair = await info.storage.find_one("pairs", {"id": felt(pair_address)})

    day_id, day_start = _day_id(info)

    pair_day_data = await info.storage.find_one("pair_day_data", {
            "pair_id": felt(pair_address),
            "day_id": day_id,
        })

    if pair_day_data:
        await info.storage.find_one_and_update(
            "pair_day_data",
            {
                "pair_id": felt(pair_address),
                "day_id": day_id,
            },
            {
                "$set": {
                    "pair_id": felt(pair_address),
                    "day_id": day_id,
                    "date": day_start,
                    "token0_id": pair["token0_id"],
                    "token1_id": pair["token1_id"],
                    "total_supply": pair["total_supply"],
                    "reserve0": pair["reserve0"],
                    "reserve1": pair["reserve1"],
                    "reserve_usd": pair["reserve_usd"],
                }
            }
        )
    else:
        await info.storage.insert_one(
            "pair_day_data",
            {
                "pair_id": felt(pair_address),
                "day_id": day_id,
                "date": day_start,
                "token0_id": pair["token0_id"],
                "token1_id": pair["token1_id"],
                "total_supply": pair["total_supply"],
                "reserve0": pair["reserve0"],
                "reserve1": pair["reserve1"],
                "reserve_usd": pair["reserve_usd"],
            }
        )


async def update_pair_day_data(info: Info[IndexerContext], pair_address: int, update):
    day_id, _day_start = _day_id(info)

    await info.storage.find_one_and_update(
        "pair_day_data",
        {
            "pair_id": felt(pair_address),
            "day_id": day_id,
        },
        update,
    )


async def snapshot_pair_hour_data(info: Info[IndexerContext], pair_address: int):
    pair = await info.storage.find_one("pairs", {"id": felt(pair_address)})

    hour_id, hour_start = _hour_id(info)

    pair_hour_data = await info.storage.find_one("pair_hour_data", {
            "pair_id": felt(pair_address),
            "hour_id": hour_id,
        })

    if pair_hour_data:
        await info.storage.find_one_and_update(
            "pair_hour_data",
            {
                "pair_id": felt(pair_address),
                "hour_id": hour_id,
            },
            {
                "$set": {
                    "pair_id": felt(pair_address),
                    "hour_id": hour_id,
                    "date": hour_start,
                    "token0_id": pair["token0_id"],
                    "token1_id": pair["token1_id"],
                    "total_supply": pair["total_supply"],
                    "reserve0": pair["reserve0"],
                    "reserve1": pair["reserve1"],
                    "reserve_usd": pair["reserve_usd"],
                }
            }
        )
    else:
        await info.storage.insert_one(
            "pair_hour_data",
            {
                "pair_id": felt(pair_address),
                "hour_id": hour_id,
                "date": hour_start,
                "token0_id": pair["token0_id"],
                "token1_id": pair["token1_id"],
                "total_supply": pair["total_supply"],
                "reserve0": pair["reserve0"],
                "reserve1": pair["reserve1"],
                "reserve_usd": pair["reserve_usd"],
            }
        )


async def update_pair_hour_data(info: Info[IndexerContext], pair_address: int, update):
    hour_id, _hour_start = _hour_id(info)

    await info.storage.find_one_and_update(
        "pair_hour_data",
        {
            "pair_id": felt(pair_address),
            "hour_id": hour_id,
        },
        update,
    )


async def snapshot_exchange_day_data(info: Info[IndexerContext], address: int):
    exchange = await info.storage.find_one("factories", {"id": felt(address)})

    day_id, day_start = _day_id(info)

    exchange_day_data = await info.storage.find_one("exchange_day_data", {
            "address": felt(address),
            "day_id": day_id,
        })

    if exchange_day_data:
        await info.storage.find_one_and_update(
            "exchange_day_data",
            {
                "address": felt(address),
                "day_id": day_id,
            },
            {
                "$set": {
                    "address": felt(address),
                    "day_id": day_id,
                    "date": day_start,
                    "total_volume_usd": exchange["total_volume_usd"],
                    "total_volume_eth": exchange["total_volume_eth"],
                    "total_liquidity_usd": exchange["total_liquidity_usd"],
                    "total_liquidity_eth": exchange["total_liquidity_eth"],
                    "transaction_count": exchange["transaction_count"],
                }
            }
        )
    else:
        await info.storage.insert_one(
            "exchange_day_data",
            {
                "address": felt(address),
                "day_id": day_id,
                "date": day_start,
                "total_volume_usd": exchange["total_volume_usd"],
                "total_volume_eth": exchange["total_volume_eth"],
                "total_liquidity_usd": exchange["total_liquidity_usd"],
                "total_liquidity_eth": exchange["total_liquidity_eth"],
                "transaction_count": exchange["transaction_count"],
            }
        )


async def update_exchange_day_data(info: Info[IndexerContext], address: int, update):
    day_id, _day_start = _day_id(info)

    await info.storage.find_one_and_update(
        "exchange_day_data",
        {
            "address": felt(address),
            "day_id": day_id,
        },
        update,
    )


async def snapshot_token_day_data(
    info: Info[IndexerContext], token_address: Union[int, bytes]
):
    if isinstance(token_address, int):
        token_address = felt(token_address)

    day_id, day_start = _day_id(info)

    token = await info.storage.find_one("tokens", {"id": token_address})

    price_usd = token["derived_eth"].to_decimal() * info.context.eth_price
    total_liquidity_token = token["total_liquidity"].to_decimal()
    total_liquidity_eth = total_liquidity_token * token["derived_eth"].to_decimal()
    total_liquidity_usd = total_liquidity_eth * info.context.eth_price

    token_day_data = await info.storage.find_one("token_day_data", {
            "token_id": token_address,
            "day_id": day_id,
        })

    if token_day_data:
        await info.storage.find_one_and_update(
            "token_day_data",
            {
                "token_id": token_address,
                "day_id": day_id,
            },
            {
                "$set": {
                    "token_id": token_address,
                    "day_id": day_id,
                    "date": day_start,
                    "price_usd": Decimal128(price_usd),
                    "total_liquidity_token": Decimal128(total_liquidity_token),
                    "total_liquidity_eth": Decimal128(total_liquidity_eth),
                    "total_liquidity_usd": Decimal128(total_liquidity_usd),
                }
            }
        )
    else:
        await info.storage.insert_one(
            "token_day_data",
            {
                "token_id": token_address,
                "day_id": day_id,
                "date": day_start,
                "price_usd": Decimal128(price_usd),
                "total_liquidity_token": Decimal128(total_liquidity_token),
                "total_liquidity_eth": Decimal128(total_liquidity_eth),
                "total_liquidity_usd": Decimal128(total_liquidity_usd),
            }
        )


async def update_token_day_data(
    info: Info[IndexerContext], token: Union[int, bytes], update
):
    if isinstance(token, int):
        token = felt(token)

    day_id, _day_start = _day_id(info)

    await info.storage.find_one_and_update(
        "token_day_data",
        {
            "token_id": token,
            "day_id": day_id,
        },
        update,
    )


def _day_id(info: Info[IndexerContext]):
    ts = int(info.context.block_timestamp.timestamp())
    day_id = ts // 86400
    day_start = datetime.fromtimestamp(day_id * 86400)
    return day_id, day_start


def _hour_id(info: Info[IndexerContext]):
    ts = int(info.context.block_timestamp.timestamp())
    hour_id = ts // 3600
    hour_start = datetime.fromtimestamp(hour_id * 3600)
    return hour_id, hour_start
