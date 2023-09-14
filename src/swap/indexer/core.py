from decimal import Decimal

from apibara.indexer import Info
from apibara.starknet import felt
from apibara.starknet.proto.starknet_pb2 import BlockHeader, Event
from bson import Decimal128
from structlog import get_logger

from swap.indexer.abi import (burn_decoder, decode_event, mint_decoder,
                                 swap_decoder, sync_decoder, transfer_decoder)
from swap.indexer.context import IndexerContext
from swap.indexer.daily import (snapshot_exchange_day_data,
                                   snapshot_pair_day_data,
                                   snapshot_pair_hour_data,
                                   snapshot_token_day_data,
                                   update_exchange_day_data,
                                   update_pair_day_data, update_pair_hour_data,
                                   update_token_day_data)
from swap.indexer.helpers import (create_liquidity_snapshot, find_or_create_user,
                                     create_transaction,
                                     fetch_token_balance, price,
                                     replace_liquidity_position, to_decimal,
                                     update_transaction_count)
from swap.indexer.jediswap import (find_eth_per_token,
                                      get_tracked_liquidity_usd,
                                      get_tracked_volume_usd, jediswap_factory, zap_in_addresses)

logger = get_logger(__name__)


async def handle_transfer(info: Info, event: Event, transaction_hash: str):
    transfer = decode_event(transfer_decoder, event.data)
    pair_address_int = felt.to_int(event.from_address)
    pair_address = hex(pair_address_int)
    logger.info("handle Transfer", **transfer._asdict())
    if transfer.from_ == 0 and transfer.to == 1 and transfer.value == 1000:
        return

    value = to_decimal(transfer.value, 18)
    await create_transaction(info, transaction_hash)

    # mints
    mints = await info.storage.find(
        "mints",
        {
            "pair_id": pair_address,
            "transaction_hash": transaction_hash,
        },
    )
    mints = list(mints)

    if transfer.from_ == 0:
        logger.info("transfer is a mint")

        # update total supply
        await info.storage.find_one_and_update(
            "pairs",
            {"id": pair_address},
            {"$inc": {"total_supply": Decimal128(value)}},
        )

        # create new mint if no mints so far or if last one is done already
        if not mints or _is_complete_mint(mints[-1]):
            mint = {
                "transaction_hash": transaction_hash,
                "index": len(mints),
                "pair_id": pair_address,
                "to": hex(transfer.to),
                "liquidity": Decimal128(value),
                "timestamp": info.context.block_timestamp,
            }
            await info.storage.insert_one("mints", mint)
    elif transfer.from_ in zap_in_addresses:
            # update latest mint
            logger.info("transfer is zapper")
            mints = await info.storage.find(
                "mints",
                {
                    "pair_id": pair_address,
                    "transaction_hash": transaction_hash,
                },
                sort={"index": 1},
            )
            mints = list(mints)
            assert mints
            await info.storage.find_one_and_update(
                "mints",
                {
                    "pair_id": pair_address,
                    "transaction_hash": transaction_hash,
                    "index": len(mints) - 1,
                },
                {
                    "$set": {
                        "to": hex(transfer.to),
                        "zap_in": True
                    }
                },
            )

    if transfer.to == pair_address_int:
        logger.info("transfer is burn (direct)")
        # send directly to pair
        burns = await info.storage.find(
            "burns",
            {
                "pair_id": pair_address,
                "transaction_hash": transaction_hash,
            },
        )
        burns = list(burns)

        burn = {
            "transaction_hash": transaction_hash,
            "index": len(burns),
            "pair_id": pair_address,
            "sender": hex(transfer.from_),
            "to": hex(transfer.to),
            "liquidity": Decimal128(value),
            "timestamp": info.context.block_timestamp,
            "needs_complete": True,
        }
        await info.storage.insert_one("burns", burn)

    # burns
    if transfer.to == 0 and transfer.from_ == pair_address_int:
        logger.info("transfer is a burn")

        # update total supply
        await info.storage.find_one_and_update(
            "pairs",
            {"id": pair_address},
            {"$inc": {"total_supply": Decimal128(-value)}},
        )

        burns = await info.storage.find(
            "burns",
            {
                "pair_id": pair_address,
                "transaction_hash": transaction_hash,
            },
        )
        burns = list(burns)

        burn = None
        if burns:
            current_burn = burns[-1]
            # continue previous burn
            if current_burn["needs_complete"]:
                burn = current_burn

        if burn is None:
            burn = {
                "transaction_hash": transaction_hash,
                "index": len(burns),
                "pair_id": pair_address,
                "sender": hex(transfer.from_),
                "to": hex(transfer.to),
                "liquidity": Decimal128(value),
                "timestamp": info.context.block_timestamp,
                "needs_complete": False,
            }

        # if this logical burn included a fee mint, account for this
        if mints and not _is_complete_mint(mints[-1]):
            mint = mints[-1]
            burn["fee_to"] = mint["to"]
            burn["fee_liquidity"] = mint["liquidity"]
            # remove logical mint
            await info.storage.delete_one(
                "mints",
                {"transaction_hash": mint["transaction_hash"], "index": mint["index"]},
            )

        del burn["_id"]
        if burn["needs_complete"]:
            # replace existing burn
            await info.storage.find_one_and_replace(
                "burns",
                {
                    "transaction_hash": burn["transaction_hash"],
                    "index": burn["index"],
                },
                burn,
            )
        else:
            await info.storage.insert_one("burns", burn)

    if transfer.from_ != 0 and transfer.from_ != pair_address:
        from_user_balance = await fetch_token_balance(
            info, pair_address, transfer.from_
        )
        from_user_balance = to_decimal(from_user_balance, 18)
        logger.debug("from user balance", balance=from_user_balance)
        await replace_liquidity_position(
            info, pair_address, transfer.from_, from_user_balance
        )
        await create_liquidity_snapshot(info, pair_address, transfer.from_)

    if transfer.to != 0 and transfer.to != pair_address:
        to_user_balance = await fetch_token_balance(info, pair_address, transfer.to)
        to_user_balance = to_decimal(to_user_balance, 18)
        logger.debug("to user balance", balance=to_user_balance)
        await replace_liquidity_position(
            info, pair_address, transfer.to, to_user_balance
        )
        await create_liquidity_snapshot(info, pair_address, transfer.to)


async def handle_sync(info: Info, event: Event, transaction_hash: str):
    sync = decode_event(sync_decoder, event.data)
    pair_address = hex(felt.to_int(event.from_address))
    logger.info("handle Sync", **sync._asdict())

    pair = await info.storage.find_one("pairs", {"id": pair_address})
    assert pair is not None

    token0 = await info.storage.find_one("tokens", {"id": pair["token0_id"]})
    assert token0 is not None

    token1 = await info.storage.find_one("tokens", {"id": pair["token1_id"]})
    assert token1 is not None

    reserve0 = to_decimal(sync.reserve0, token0["decimals"])
    reserve1 = to_decimal(sync.reserve1, token1["decimals"])

    token0_price = price(reserve0, reserve1)
    token1_price = price(reserve1, reserve0)

    logger.info(
        "new reserves and price",
        reserve0=reserve0,
        reserve1=reserve1,
        price0=token0_price,
        price1=token1_price,
    )

    old_pair = await info.storage.find_one_and_update(
        "pairs",
        {
            "id": pair_address,
        },
        {
            "$set": {
                "reserve0": Decimal128(reserve0),
                "reserve1": Decimal128(reserve1),
                "token0_price": Decimal128(token0_price),
                "token1_price": Decimal128(token1_price),
            }
        },
    )

    token0_liquidity = (
        token0["total_liquidity"].to_decimal()
        - old_pair["reserve0"].to_decimal()
        + reserve0
    )
    token1_liquidity = (
        token1["total_liquidity"].to_decimal()
        - old_pair["reserve1"].to_decimal()
        + reserve1
    )

    await info.storage.find_one_and_update(
        "tokens",
        {"id": token0["id"]},
        {"$set": {"total_liquidity": Decimal128(token0_liquidity)}},
    )

    await info.storage.find_one_and_update(
        "tokens",
        {"id": token1["id"]},
        {"$set": {"total_liquidity": Decimal128(token1_liquidity)}},
    )

    logger.info("fetch prices", token0=token0["symbol"], token1=token1["symbol"])
    token0_derived_eth = await find_eth_per_token(info, token0["id"])
    token1_derived_eth = await find_eth_per_token(info, token1["id"])

    logger.info(
        "refresh token eth price",
        token0=token0_derived_eth,
        token1=token1_derived_eth,
    )

    tracked_liquidity_usd = await get_tracked_liquidity_usd(
        info, token0, reserve0, token1, reserve1
    )
    if info.context.eth_price != Decimal("0"):
        tracked_liquidity_eth = tracked_liquidity_usd / info.context.eth_price
    else:
        tracked_liquidity_eth = Decimal("0")

    reserve_eth = (
        reserve0 * token0["derived_eth"].to_decimal()
        + reserve1 * token1["derived_eth"].to_decimal()
    )
    reserve_usd = reserve_eth * info.context.eth_price

    # update derived amounts
    await info.storage.find_one_and_update(
        "pairs",
        {
            "id": pair_address,
        },
        {
            "$set": {
                "tracked_reserve_eth": Decimal128(tracked_liquidity_eth),
                "reserve_eth": Decimal128(reserve_eth),
                "reserve_usd": Decimal128(reserve_usd),
            }
        },
    )

    factory = await info.storage.find_one("factories", {"id": hex(jediswap_factory)})

    total_liquidity_eth = factory["total_liquidity_eth"].to_decimal() - old_pair["tracked_reserve_eth"].to_decimal() + tracked_liquidity_eth
    total_liquidity_usd = total_liquidity_eth * info.context.eth_price

    await info.storage.find_one_and_update(
        "factories",
        {"id": hex(jediswap_factory)},
        {
            "$set": {
                "total_liquidity_eth": Decimal128(total_liquidity_eth),
                "total_liquidity_usd": Decimal128(total_liquidity_usd),
            }
        },
    )


async def handle_mint(info: Info, event: Event, transaction_hash: str):
    mint = decode_event(mint_decoder, event.data)
    pair_address = hex(felt.to_int(event.from_address))
    logger.info("handle Mint", **mint._asdict())

    transaction = await info.storage.find_one(
        "transactions", {"hash": transaction_hash}
    )
    assert transaction is not None

    mints = await info.storage.find(
        "mints",
        {
            "pair_id": pair_address,
            "transaction_hash": transaction_hash,
        },
        sort={"index": 1},
    )
    mints = list(mints)
    assert mints

    pair = await info.storage.find_one("pairs", {"id": pair_address})
    assert pair is not None

    token0 = await info.storage.find_one("tokens", {"id": pair["token0_id"]})
    assert token0 is not None

    token1 = await info.storage.find_one("tokens", {"id": pair["token1_id"]})
    assert token1 is not None

    await update_transaction_count(info, jediswap_factory, pair_address, token0, token1)

    token0_amount = to_decimal(mint.amount0, token0["decimals"])
    token1_amount = to_decimal(mint.amount1, token1["decimals"])

    # get new amounts of usd and eth for tracking
    amount_total_eth = (
        token1["derived_eth"].to_decimal() * token1_amount
        + token0["derived_eth"].to_decimal() * token0_amount
    )
    amount_total_usd = amount_total_eth * info.context.eth_price

    # update latest mint
    mint = await info.storage.find_one_and_update(
        "mints",
        {
            "pair_id": pair_address,
            "transaction_hash": transaction_hash,
            "index": len(mints) - 1,
        },
        {
            "$set": {
                "sender": hex(mint.sender),
                "amount0": Decimal128(token0_amount),
                "amount1": Decimal128(token1_amount),
                "amount_usd": Decimal128(amount_total_usd),
            }
        },
    )

    user = await find_or_create_user(info, mint["to"])

    # update users data
    await info.storage.find_one_and_update(
        "users",
        {"id": user["id"]},
        {
            "$inc": {
                "transaction_count": 1,
                "mint_count": 1,
            }
        },
    )

    # update lp position
    await create_liquidity_snapshot(info, pair_address, mint["to"])

    # update daily stats
    await snapshot_pair_day_data(info, pair_address)
    await update_pair_day_data(info, pair_address, {"$inc": {"transaction_count": 1}})

    await snapshot_pair_hour_data(info, pair_address)
    await update_pair_hour_data(info, pair_address, {"$inc": {"transaction_count": 1}})

    await snapshot_exchange_day_data(info, jediswap_factory)

    await snapshot_token_day_data(info, pair["token0_id"])
    await update_token_day_data(
        info, pair["token0_id"], {"$inc": {"transaction_count": 1}}
    )

    await snapshot_token_day_data(info, pair["token1_id"])
    await update_token_day_data(
        info, pair["token1_id"], {"$inc": {"transaction_count": 1}}
    )


async def handle_burn(info: Info, event: Event, transaction_hash: str):
    burn = decode_event(burn_decoder, event.data)
    pair_address = hex(felt.to_int(event.from_address))
    logger.info("handle Burn", **burn._asdict())

    transaction = await info.storage.find_one(
        "transactions", {"hash": transaction_hash}
    )
    if transaction is None:
        return

    burns = await info.storage.find(
        "burns",
        {
            "pair_id": pair_address,
            "transaction_hash": transaction_hash,
        },
        sort={"index": 1},
    )
    burns = list(burns)
    assert burns

    pair = await info.storage.find_one("pairs", {"id": pair_address})
    assert pair is not None

    token0 = await info.storage.find_one("tokens", {"id": pair["token0_id"]})
    assert token0 is not None

    token1 = await info.storage.find_one("tokens", {"id": pair["token1_id"]})
    assert token1 is not None

    await update_transaction_count(info, jediswap_factory, pair_address, token0, token1)

    token0_amount = to_decimal(burn.amount0, token0["decimals"])
    token1_amount = to_decimal(burn.amount1, token1["decimals"])

    # get new amounts of usd and eth for tracking
    amount_total_eth = (
        token1["derived_eth"].to_decimal() * token1_amount
        + token0["derived_eth"].to_decimal() * token0_amount
    )
    amount_total_usd = amount_total_eth * info.context.eth_price

    # update burn
    burn = await info.storage.find_one_and_update(
        "burns",
        {
            "pair_id": pair_address,
            "transaction_hash": transaction_hash,
            "index": len(burns) - 1,
        },
        {
            "$set": {
                # "to": hex(burn.to),
                "amount0": Decimal128(token0_amount),
                "amount1": Decimal128(token1_amount),
                "amount_usd": Decimal128(amount_total_usd),
            }
        },
    )

    user = await find_or_create_user(info, burn["sender"])

    # update users data
    await info.storage.find_one_and_update(
        "users",
        {"id": user["id"]},
        {
            "$inc": {
                "transaction_count": 1,
                "burn_count": 1,
            }
        },
    )

    # update lp position
    await create_liquidity_snapshot(info, pair_address, burn["sender"])

    # update daily stats
    await snapshot_pair_day_data(info, pair_address)
    await update_pair_day_data(info, pair_address, {"$inc": {"transaction_count": 1}})

    await snapshot_pair_hour_data(info, pair_address)
    await update_pair_hour_data(info, pair_address, {"$inc": {"transaction_count": 1}})

    await snapshot_exchange_day_data(info, jediswap_factory)

    await snapshot_token_day_data(info, pair["token0_id"])
    await update_token_day_data(
        info, pair["token0_id"], {"$inc": {"transaction_count": 1}}
    )

    await snapshot_token_day_data(info, pair["token1_id"])
    await update_token_day_data(
        info, pair["token1_id"], {"$inc": {"transaction_count": 1}}
    )


async def handle_swap(info: Info, event: Event, transaction_hash: str):
    swap = decode_event(swap_decoder, event.data)
    pair_address = hex(felt.to_int(event.from_address))
    logger.info("handle Swap", **swap._asdict())

    pair = await info.storage.find_one("pairs", {"id": pair_address})
    assert pair is not None

    token0 = await info.storage.find_one("tokens", {"id": pair["token0_id"]})
    assert token0 is not None

    token1 = await info.storage.find_one("tokens", {"id": pair["token1_id"]})
    assert token1 is not None

    amount0_in = to_decimal(swap.amount0_in, token0["decimals"])
    amount1_in = to_decimal(swap.amount1_in, token1["decimals"])
    amount0_out = to_decimal(swap.amount0_out, token0["decimals"])
    amount1_out = to_decimal(swap.amount1_out, token1["decimals"])

    # total for volume updates
    amount0_total = amount0_in + amount0_out
    amount1_total = amount1_in + amount1_out

    derived_amount_eth = (
        token1["derived_eth"].to_decimal() * amount1_total
        + token0["derived_eth"].to_decimal() * amount0_total
    ) / Decimal("2")
    derive_amount_usd = derived_amount_eth * info.context.eth_price

    tracked_amount_usd = await get_tracked_volume_usd(
        info, token0, amount0_total, token1, amount1_total, pair
    )
    tracked_amount_eth = Decimal("0")
    if info.context.eth_price != Decimal("0"):
        tracked_amount_eth = tracked_amount_usd / info.context.eth_price

    amount0_total_eth = amount0_total * token0["derived_eth"].to_decimal()
    amount0_total_usd = amount0_total_eth * info.context.eth_price
    amount1_total_eth = amount1_total * token1["derived_eth"].to_decimal()
    amount1_total_usd = amount1_total_eth * info.context.eth_price

    user = await find_or_create_user(info, swap.to)

    # update users data
    await info.storage.find_one_and_update(
        "users",
        {"id": user["id"]},
        {
            "$inc": {
                "transaction_count": 1,
                "swap_count": 1,
            }
        },
    )

    # update tokens data
    await info.storage.find_one_and_update(
        "tokens",
        {"id": pair["token0_id"]},
        {
            "$inc": {
                "trade_volume": Decimal128(amount0_total),
                "trade_volume_usd": Decimal128(tracked_amount_usd),
                "untracked_volume_usd": Decimal128(derive_amount_usd),
                "transaction_count": 1,
            }
        },
    )

    await info.storage.find_one_and_update(
        "tokens",
        {"id": pair["token1_id"]},
        {
            "$inc": {
                "trade_volume": Decimal128(amount1_total),
                "trade_volume_usd": Decimal128(tracked_amount_usd),
                "untracked_volume_usd": Decimal128(derive_amount_usd),
                "transaction_count": 1,
            }
        },
    )

    # update pair
    await info.storage.find_one_and_update(
        "pairs",
        {"id": pair_address},
        {
            "$inc": {
                "volume_usd": Decimal128(tracked_amount_usd),
                "volume_token0": Decimal128(amount0_total),
                "volume_token1": Decimal128(amount1_total),
                "untracked_volume_usd": Decimal128(derived_amount_eth),
                "transaction_count": 1,
            }
        },
    )

    # update factory
    await info.storage.find_one_and_update(
        "factories",
        {"id": hex(jediswap_factory)},
        {
            "$inc": {
                "total_volume_usd": Decimal128(tracked_amount_usd),
                "total_volume_eth": Decimal128(tracked_amount_eth),
                "untracked_volume_usd": Decimal128(derive_amount_usd),
                "transaction_count": 1,
            }
        },
    )

    await create_transaction(info, transaction_hash)

    await info.storage.insert_one(
        "swaps",
        {
            "transaction_hash": transaction_hash,
            "pair_id": pair["id"],
            "timestamp": info.context.block_timestamp,
            "amount0_in": Decimal128(amount0_in),
            "amount1_in": Decimal128(amount1_in),
            "amount0_out": Decimal128(amount0_out),
            "amount1_out": Decimal128(amount1_out),
            "sender": hex(swap.sender),
            "to": hex(swap.to),
            "from": hex(swap.sender),  # TODO: should be tx sender
            "amount_usd": Decimal128(max(tracked_amount_usd, derive_amount_usd)),
        },
    )

    # update daily stats
    await snapshot_exchange_day_data(info, jediswap_factory)
    await update_exchange_day_data(
        info,
        jediswap_factory,
        {
            "$inc": {
                "daily_volume_usd": Decimal128(tracked_amount_usd),
                "daily_volume_eth": Decimal128(tracked_amount_eth),
                "daily_volume_untracked": Decimal128(derive_amount_usd),
            }
        },
    )

    await snapshot_pair_day_data(info, pair_address)
    await update_pair_day_data(
        info,
        pair_address,
        {
            "$inc": {
                "transaction_count": 1,
                "daily_volume_token0": Decimal128(amount0_total),
                "daily_volume_token1": Decimal128(amount1_total),
                "daily_volume_usd": Decimal128(tracked_amount_usd),
            }
        },
    )

    await snapshot_pair_hour_data(info, pair_address)
    await update_pair_hour_data(
        info,
        pair_address,
        {
            "$inc": {
                "transaction_count": 1,
                "hourly_volume_token0": Decimal128(amount0_total),
                "hourly_volume_token1": Decimal128(amount1_total),
                "hourly_volume_usd": Decimal128(tracked_amount_usd),
            }
        },
    )

    await snapshot_token_day_data(info, pair["token0_id"])
    await update_token_day_data(
        info,
        pair["token0_id"],
        {
            "$inc": {
                "transaction_count": 1,
                "daily_volume_token": Decimal128(amount0_total),
                "daily_volume_eth": Decimal128(amount0_total_eth),
                "daily_volume_usd": Decimal128(amount0_total_usd),
            }
        },
    )

    await snapshot_token_day_data(info, pair["token1_id"])
    await update_token_day_data(
        info,
        pair["token1_id"],
        {
            "$inc": {
                "transaction_count": 1,
                "daily_volume_token": Decimal128(amount1_total),
                "daily_volume_eth": Decimal128(amount1_total_eth),
                "daily_volume_usd": Decimal128(amount1_total_usd),
            }
        },
    )


def _is_complete_mint(mint):
    return mint["sender"] is not None
