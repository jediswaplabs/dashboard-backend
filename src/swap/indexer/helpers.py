from decimal import Decimal
from typing import List, Union

from apibara import Info
from bson import Decimal128
from starknet_py.cairo.felt import decode_shortstring
from starknet_py.contract import ContractFunction
from starknet_py.net.client_models import Call

from swap.indexer.context import IndexerContext


def felt(n: int) -> str:
    return n.to_bytes(32, "big")


def uint256(low, high):
    return low + (high << 128)


def to_decimal(n: int, decimals: int) -> Decimal:
    num = Decimal(10) ** Decimal(decimals)
    return Decimal(n) / num


def price(a: Decimal, b: Decimal):
    if b == Decimal("0"):
        return Decimal("0")
    return a / b


async def create_token(info: Info[IndexerContext], address: int):
    token = await info.storage.find_one("tokens", {"id": felt(address)})
    if token is not None:
        return token
    name = await fetch_token_name(info, address)
    symbol = await fetch_token_symbol(info, address)
    decimals = await fetch_token_decimals(info, address)
    total_supply = await fetch_token_total_supply(info, address)

    token = {
        "id": felt(address),
        "name": name,
        "symbol": symbol,
        "decimals": decimals,
        # used for market cap
        "total_supply": felt(total_supply),
        # token specific volume
        "trade_volume": Decimal128("0"),
        "trade_volume_usd": Decimal128("0"),
        "untracked_volume_usd": Decimal128("0"),
        # transactions
        "transaction_count": 0,
        # liquidity across pairs
        "total_liquidity": Decimal128("0"),
        # derived price (in eth)
        "derived_eth": Decimal128("1"),
    }

    await info.storage.insert_one("tokens", token)
    return token


async def create_transaction(info: Info[IndexerContext], transaction_hash: bytes):
    transaction = await info.storage.find_one(
        "transactions", {"hash": transaction_hash}
    )
    if transaction is not None:
        return transaction

    transaction = {
        "hash": transaction_hash,
        "block_number": info.context.block_number,
        "block_timestamp": info.context.block_timestamp,
    }

    await info.storage.insert_one("transactions", transaction)
    return transaction

async def find_or_create_user(info: Info[IndexerContext], user_id: Union[int, bytes]):
    
    if isinstance(user_id, int):
        user_id = felt(user_id)
    
    user = await info.storage.find_one(
        "users", {"id": user_id}
    )
    if user is not None:
        return user

    user = {
        "id": user_id,
        "transaction_count": 0,
        "swap_count": 0,
        "mint_count": 0,
        "burn_count": 0,
    }

    await info.storage.insert_one("users", user)
    return user


async def replace_liquidity_position(
    info: Info[IndexerContext], pair_address: int, user: int, balance: Decimal
):
    await info.storage.find_one_and_replace(
        "liquidity_positions",
        {
            "pair_address": felt(pair_address),
            "user": felt(user),
        },
        {
            "pair_address": felt(pair_address),
            "user": felt(user),
            "liquidity_token_balance": Decimal128(balance),
        },
        upsert=True,
    )


async def create_liquidity_snapshot(
    info: Info[IndexerContext], pair_address: int, user: Union[int, bytes]
):
    if isinstance(user, int):
        user = felt(user)

    pair = await info.storage.find_one("pairs", {"id": felt(pair_address)})
    assert pair is not None
    token0 = await info.storage.find_one("tokens", {"id": pair["token0_id"]})
    assert token0 is not None
    token1 = await info.storage.find_one("tokens", {"id": pair["token1_id"]})
    assert token1 is not None
    position = await info.storage.find_one(
        "liquidity_positions", {"pair_address": felt(pair_address), "user": user}
    )
    assert position is not None

    token0_price_usd = token0["derived_eth"].to_decimal() * info.context.eth_price
    token1_price_usd = token1["derived_eth"].to_decimal() * info.context.eth_price

    await info.storage.insert_one(
        "liquidity_position_snapshots",
        {
            "pair_address": felt(pair_address),
            "user": user,
            "timestamp": info.context.block_timestamp,
            "block": info.context.block_number,
            "token0_price_usd": Decimal128(token0_price_usd),
            "token1_price_usd": Decimal128(token1_price_usd),
            "reserve0": pair["reserve0"],
            "reserve1": pair["reserve1"],
            "reserve_usd": pair["reserve_usd"],
            "liquidity_token_total_supply": pair["total_supply"],
            "liquidity_token_balance": position["liquidity_token_balance"],
        },
    )


async def update_transaction_count(
    info: Info[IndexerContext], factory: int, pair: int, token0, token1
):
    await info.storage.find_one_and_update(
        "factories", {"id": felt(factory)}, {"$inc": {"transaction_count": 1}}
    )

    await info.storage.find_one_and_update(
        "tokens", {"id": token0["id"]}, {"$inc": {"transaction_count": 1}}
    )

    await info.storage.find_one_and_update(
        "tokens", {"id": token1["id"]}, {"$inc": {"transaction_count": 1}}
    )

    await info.storage.find_one_and_update(
        "pairs", {"id": felt(pair)}, {"$inc": {"transaction_count": 1}}
    )


async def fetch_token_balance(
    info: Info[IndexerContext], token_address: int, user: int
):
    result = await simple_call(info, token_address, "balanceOf", [user])
    return uint256(result[0], result[1])


async def fetch_token_name(info: Info[IndexerContext], address: int):
    result = await simple_call(info, address, "name", [])
    return decode_shortstring(result[0]).strip("\x00")


async def fetch_token_symbol(info: Info[IndexerContext], address: int):
    result = await simple_call(info, address, "symbol", [])
    return decode_shortstring(result[0]).strip("\x00")


async def fetch_token_decimals(info: Info[IndexerContext], address: int):
    result = await simple_call(info, address, "decimals", [])
    return result[0]


async def fetch_token_total_supply(info: Info[IndexerContext], address: int):
    result = await simple_call(info, address, "totalSupply", [])
    return uint256(result[0], result[1])


async def simple_call(
    info: Info[IndexerContext], contract: int, method: str, calldata: List[int]
):
    selector = ContractFunction.get_selector(method)
    call = Call(contract, selector, calldata)
    return await info.context.rpc.call_contract(
        call, block_hash=info.context.block_hash
    )

def lp_contest_1(user: str):
    indexer_id = "jediswap-testnet"
    user = felt(int(user, 16))
    import os
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit("MONGO_URL not set")
    from pymongo import MongoClient
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]
    query = dict()
    cursor = db["blocks"].find(query, limit=1)
    from swap.server.helpers import add_order_by_constraint
    cursor = add_order_by_constraint(cursor, "number", "desc")
    from swap.server.block import Block
    returned_block = [Block.from_mongo(d) for d in cursor][0]
    latest_block_number = returned_block.number
    print(latest_block_number)
    
    contest_start_block = 17100
    query = dict()
    query["number"] = contest_start_block
    cursor = db["blocks"].find(query, limit=1)
    returned_block = [Block.from_mongo(d) for d in cursor][0]
    contest_start_timestamp = returned_block.timestamp
    ts = int(contest_start_timestamp.timestamp())
    contest_start_hour_id = ts // 3600
    print(contest_start_block, contest_start_timestamp, contest_start_hour_id)
    
    last_lp_value = 0
    last_block_number = contest_start_block

    query = dict()
    query["user"] = user
    query["block"] = {"$lt": contest_start_block}
    cursor = db["liquidity_position_snapshots"].find(query, limit=1)
    cursor = add_order_by_constraint(cursor, "block", "desc")
    from swap.server.liquidity_position import LiquidityPositionSnapshot
    liquidity_position_snapshots = [LiquidityPositionSnapshot.from_mongo(d) for d in cursor]
    print(liquidity_position_snapshots, len(liquidity_position_snapshots))
    if len(liquidity_position_snapshots) != 0:
        lps = liquidity_position_snapshots[0]
        query = dict()
        query["pair_id"] = lps.pair_id
        query["hour_id"] = contest_start_hour_id
        cursor = db["pair_hour_data"].find(query, limit=1)
        cursor = add_order_by_constraint(cursor, "reserve_usd", "desc")
        pair_hour_data = [d for d in cursor][0]
        highest_reserve_usd = pair_hour_data["reserve_usd"]
        print(highest_reserve_usd)
        cursor = db["pair_hour_data"].find(query, limit=1)
        cursor = add_order_by_constraint(cursor, "reserve_usd", "asc")
        pair_hour_data = [d for d in cursor][0]
        lowest_reserve_usd = pair_hour_data["reserve_usd"]
        print(lowest_reserve_usd)
        average_reserve_usd = (highest_reserve_usd.to_decimal() + lowest_reserve_usd.to_decimal()) / 2
        print(average_reserve_usd)
        cursor = db["pair_hour_data"].find(query, limit=1)
        cursor = add_order_by_constraint(cursor, "total_supply", "desc")
        pair_hour_data = [d for d in cursor][0]
        highest_total_supply = pair_hour_data["total_supply"]
        print(highest_total_supply)
        cursor = db["pair_hour_data"].find(query, limit=1)
        cursor = add_order_by_constraint(cursor, "total_supply", "asc")
        pair_hour_data = [d for d in cursor][0]
        lowest_total_supply = pair_hour_data["total_supply"]
        print(lowest_total_supply)
        average_total_supply = (highest_total_supply.to_decimal() + lowest_total_supply.to_decimal()) / 2
        print(average_total_supply)
        last_lp_value = (average_reserve_usd / average_total_supply) * lps.liquidity_token_balance
        print(last_lp_value)
    query = dict()
    query["user"] = user
    query["block"] = {"$lt": latest_block_number, "$gte": contest_start_block}
    cursor = db["liquidity_position_snapshots"].find(query)
    cursor = add_order_by_constraint(cursor, "block", "asc")
    liquidity_position_snapshots = [LiquidityPositionSnapshot.from_mongo(d) for d in cursor]
    total_contest_value = 0
    for (i, lps) in enumerate(liquidity_position_snapshots):
        this_block_number = lps.block
        this_pair_id = lps.pair_id
        if i < len(liquidity_position_snapshots) - 1:
            if (liquidity_position_snapshots[i+1].block == this_block_number and liquidity_position_snapshots[i+1].pair_id == this_pair_id):
                continue
        contest_value_contribution = last_lp_value * (this_block_number - last_block_number)
        total_contest_value = total_contest_value + contest_value_contribution
        this_lp_value = (lps.reserve_usd / lps.liquidity_token_total_supply) * lps.liquidity_token_balance
        last_lp_value = this_lp_value
        last_block_number = this_block_number
        print(lps, contest_value_contribution, total_contest_value, this_lp_value)
    contest_value_contribution = last_lp_value * (latest_block_number - 1 - last_block_number)
    total_contest_value = total_contest_value + contest_value_contribution
    print(total_contest_value)
    return liquidity_position_snapshots, total_contest_value
