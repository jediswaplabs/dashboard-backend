from celery import Celery
from swap.indexer.helpers import felt
from bson import Decimal128
from pymongo import MongoClient
import redis
import os
import sys

redis_url = os.environ.get('REDIS_URL', None)
if redis_url is None:
    sys.exit("REDIS_URL not set")
app = Celery('tasks', broker=redis_url)

def get_redis_connection():
    connection = redis.from_url(redis_url, 
        socket_timeout=2, 
        socket_connect_timeout=2)
    return connection

def get_from_redis(key):
    try:
        redis_conn = get_redis_connection()
        if redis_conn:
            value = redis_conn.get(key)
            try:
                value = value.decode('utf-8')
            except:
                pass
            return value
    except Exception as e:
        print("get_redis_connection : Could not get connection")
    return None

def set_in_redis(key, value, expiry=None):
    try:
        redis_conn = get_redis_connection()
        if redis_conn:
            if expiry is None:
                expiry = 60 * 60 * 24 * 30
            redis_conn.setex(key, expiry, value)
            return value
    except Exception as e:
        print("get_redis_connection : Could not get connection")
    return None

@app.task
def lp_contest_for_block(block: int):
    print(block)
    last_block_done = get_from_redis("last_block_done")
    if last_block_done:
        if block > int(last_block_done):
            pass
        else:
            return "Already done"
    indexer_id = "jediswap-testnet"
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit("MONGO_URL not set")
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]
    query = dict()
    query["block"] = {"$lte": block}
    cursor = db["liquidity_position_snapshots"].distinct("user", query)
    users = [d for d in cursor]
    print(len(users))
    from swap.server.helpers import serialize_hex
    for user in users[:4]:
        lp_contest_each_user.apply_async(args=[serialize_hex(user), block])
    set_in_redis("last_block_done", block)

@app.task
def lp_contest_each_user(user: str, latest_block_number: int):
    indexer_id = "jediswap-testnet"
    user = felt(int(user, 16))
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit("MONGO_URL not set")
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]
    
    contest_start_block = 17100
    min_lp_value = 25
    min_blocks = 100
    query = dict()
    query["number"] = contest_start_block
    cursor = db["blocks"].find(query, limit=1)
    from swap.server.block import Block
    returned_block = [Block.from_mongo(d) for d in cursor][0]
    contest_start_timestamp = returned_block.timestamp
    ts = int(contest_start_timestamp.timestamp())
    contest_start_hour_id = ts // 3600
    print(contest_start_block, contest_start_timestamp, contest_start_hour_id)
    last_block_number = contest_start_block
    query = dict()
    query["user"] = user
    query["block"] = {"$lt": contest_start_block}
    cursor = db["liquidity_position_snapshots"].distinct("pair_address", query)
    pair_addresses = [d for d in cursor]
    print(pair_addresses, len(pair_addresses))
    last_lp_values = dict()
    last_lp_value_total = 0
    from swap.server.liquidity_position import LiquidityPositionSnapshot
    from swap.server.helpers import add_order_by_constraint
    if len(pair_addresses) != 0:
        for pair_address in pair_addresses:
            query = dict()
            query["user"] = user
            query["block"] = {"$lt": contest_start_block}
            query["pair_address"] = pair_address
            cursor = db["liquidity_position_snapshots"].find(query, limit=1)
            cursor = add_order_by_constraint(cursor, "block", "desc")
            liquidity_position_snapshots = [LiquidityPositionSnapshot.from_mongo(d) for d in cursor]
            print(liquidity_position_snapshots, len(liquidity_position_snapshots))
            lps = liquidity_position_snapshots[0]
            if lps.liquidity_token_balance != 0:
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
                last_lp_values[lps.pair_id] = last_lp_value
                last_lp_value_total = last_lp_value_total + last_lp_value
    print(last_lp_values, last_lp_value_total)
    query = dict()
    query["user"] = user
    query["block"] = {"$lte": latest_block_number, "$gte": contest_start_block}
    cursor = db["liquidity_position_snapshots"].find(query)
    cursor = add_order_by_constraint(cursor, "block", "asc")
    liquidity_position_snapshots = [LiquidityPositionSnapshot.from_mongo(d) for d in cursor]
    total_contest_value = 0
    is_eligible = False
    total_blocks_eligible = 0
    for (i, lps) in enumerate(liquidity_position_snapshots):
        this_block_number = lps.block
        this_pair_id = lps.pair_id
        if i < len(liquidity_position_snapshots) - 1:
            if (liquidity_position_snapshots[i+1].block == this_block_number and liquidity_position_snapshots[i+1].pair_id == this_pair_id):
                continue
        contest_value_contribution = last_lp_value_total * (this_block_number - last_block_number)
        if last_lp_value_total > min_lp_value:
            total_blocks_eligible = total_blocks_eligible + this_block_number - last_block_number
            if total_blocks_eligible > min_blocks:
                is_eligible = True
        total_contest_value = total_contest_value + contest_value_contribution
        this_lp_value = (lps.reserve_usd / lps.liquidity_token_total_supply) * lps.liquidity_token_balance
        last_lp_values[lps.pair_id] = this_lp_value
        last_block_number = this_block_number
        last_lp_value_total = sum(last_lp_values.values())
        print(last_block_number, total_contest_value, is_eligible)
    print(last_lp_values, last_lp_value_total)
    contest_value_contribution = last_lp_value_total * (latest_block_number - last_block_number)
    if last_lp_value_total > min_lp_value:
        total_blocks_eligible = total_blocks_eligible + latest_block_number - last_block_number
        if total_blocks_eligible > min_blocks:
            is_eligible = True
    total_contest_value = total_contest_value + contest_value_contribution
    print(latest_block_number, total_contest_value, is_eligible)
    db["lp_contest_234567_block"].insert_one(
            {
                "user": user,
                "block": latest_block_number,
                "contest_value": Decimal128(total_contest_value),
                "is_eligible": is_eligible
            }
        )
    db["lp_contest_234567"].find_one_and_replace(
            {
                "user": user,
            },
            {
                "user": user,
                "block": latest_block_number,
                "contest_value": Decimal128(total_contest_value),
                "is_eligible": is_eligible
            },
            upsert=True,
        )