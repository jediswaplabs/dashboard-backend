from datetime import datetime
from decimal import Decimal
from celery import Celery
from kombu import Queue, Exchange
from bson import Decimal128
from pymongo import MongoClient
from typing import Optional
from swap.server.helpers import add_order_by_constraint
from swap.server.lp_contest import db_name_for_contest, contest_start_block, contest_end_block
from swap.server.liquidity_position import LiquidityPositionSnapshot
from swap.main import indexer_id
import redis
import os
import sys

redis_url = os.environ.get('REDIS_URL', None)
if redis_url is None:
    sys.exit("REDIS_URL not set")

app = Celery('tasks', broker=redis_url)
app.conf.task_queues = (
    Queue(f"{db_name_for_contest}_queue"),
)


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

# ELIGIBLE_PAIR_ADDRESSES = ['0x4d0390b777b424e43839cd1e744799f3de6c176c7e32c1812a41dbd9c19db6a', '0x5801bdad32f343035fb242e98d1e9371ae85bc1543962fedea16c59b35bd19b']
ELIGIBLE_PAIR_ADDRESSES = ['0x5a8054e5ca0b277b295a830e53bd71a6a6943b42d0dbb22329437522bc80c8', '0xcfd39f5244f7b617418c018204a8a9f9a7f72e71f0ef38f968eeb2a9ca302b', '0xf0f5b3eed258344152e1f17baf84a2e1b621cd754b625bec169e8595aea767', '0x260e98362e0949fefff8b4de85367c035e44f734c9f8069b6ce2075ae86b45c', '0x39c183c8e5a2df130eefa6fbaa3b8aad89b29891f6272cb0c90deaa93ec6315', '0x44d13ad98a46fd2322ef2637e5e4c292ce8822f47b7cb9a1d581176a801c1a0', '0x45e7131d776dddc137e30bdd490b431c7144677e97bf9369f629ed8d3fb7dd6', '0x4d0390b777b424e43839cd1e744799f3de6c176c7e32c1812a41dbd9c19db6a', '0x5801bdad32f343035fb242e98d1e9371ae85bc1543962fedea16c59b35bd19b', '0x7e2a13b40fc1119ec55e0bcf9428eedaa581ab3c924561ad4e955f95da63138']

@app.task
def lp_contest_for_block(latest_block_number: int, user_offset: Optional[int] = 0):
    print(latest_block_number)
    last_block_done = get_from_redis(f"{db_name_for_contest}_last_block_done")
    if last_block_done:
        if latest_block_number > int(last_block_done):
            pass
        else:
            return "Already done"
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit("MONGO_URL not set")
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]
    
    query = dict()
    query["number"] = latest_block_number
    cursor = db["blocks"].find(query, limit=1)
    from swap.server.block import Block
    returned_block = [Block.from_mongo(d) for d in cursor][0]
    latest_block_timestamp = returned_block.timestamp

    for pair_address in ELIGIBLE_PAIR_ADDRESSES:
        update_pair_cumulative_price(pair_address, latest_block_number)

    query = dict()
    query["block"] = {"$lte": latest_block_number}
    query["pair_address"] = {"$in": ELIGIBLE_PAIR_ADDRESSES}
    cursor = db["liquidity_position_snapshots"].aggregate([{"$match": query}, {"$group": {"_id": "$user"}}, { "$sort": {"_id": 1}}, {"$skip": user_offset}, {"$limit": 10000}])
    users = [d["_id"] for d in cursor]
    for user in users:
        lp_contest_each_user.apply_async(args=[user, latest_block_number, latest_block_timestamp], queue=f"{db_name_for_contest}_queue", expires=3600)
    if len(users) < 10000:
        set_in_redis(f"{db_name_for_contest}_last_block_done", latest_block_number)
    else:
        lp_contest_for_block.apply_async(args=[latest_block_number, user_offset + 10000], queue=f"{db_name_for_contest}_queue", expires=300)

def update_pair_cumulative_price(pair_address: str, latest_block_number: int):
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit("MONGO_URL not set")
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]

    pair_id = hex(int(pair_address, 16))

    query = dict()
    query["pair"] = pair_id
    cursor = db[f"{db_name_for_contest}_pair_block_cum_price"].find(query, limit=1)
    cursor = add_order_by_constraint(cursor, "block", "desc")
    pair_block_data = [d for d in cursor]
    starting_block_number = contest_start_block
    cumulative_price_usd = Decimal(0)
    time_cumulative_price_usd = Decimal(0)
    if pair_block_data:
        starting_block_number = pair_block_data[-1]["block"] + 1
        cumulative_price_usd = pair_block_data[-1]["cumulative_price_usd"].to_decimal()
        time_cumulative_price_usd = pair_block_data[-1]["time_cumulative_price_usd"].to_decimal()


    query = dict()
    query["id"] = pair_id

    for block_number in range(starting_block_number, latest_block_number+1):
        query["$or"] = [
            {
                "$and": [
                    {"_chain.valid_to": None},
                    {"_chain.valid_from": {"$lte": block_number}},
                ]
            },
            {
                "$and": [
                    {"_chain.valid_to": {"$gt": block_number}},
                    {"_chain.valid_from": {"$lte": block_number}},
                ]
            },
        ]
        cursor = db["pairs"].find(query)
        required_pair = [d for d in cursor][-1]
        price_usd = required_pair["reserve_usd"].to_decimal() / required_pair["total_supply"].to_decimal()
        if block_number == contest_start_block:
            cumulative_price_usd = price_usd
            time_cumulative_price_usd = price_usd
        else:
            cumulative_price_usd = cumulative_price_usd + price_usd
            block_query = dict()
            block_query["number"] = {"$lte": block_number}
            cursor = db["blocks"].find(block_query, limit=1)
            cursor = add_order_by_constraint(cursor, "number", "desc")
            from swap.server.block import Block
            returned_block = [Block.from_mongo(d) for d in cursor][0]
            block_timestamp = returned_block.timestamp
            block_query["number"] = {"$lte": block_number - 1}
            cursor = db["blocks"].find(block_query, limit=1)
            cursor = add_order_by_constraint(cursor, "number", "desc")
            from swap.server.block import Block
            returned_block = [Block.from_mongo(d) for d in cursor][0]
            previous_block_timestamp = returned_block.timestamp
            block_secs = block_timestamp - previous_block_timestamp
            # print(block_secs.total_seconds(), block_secs)
            time_cumulative_price_usd = time_cumulative_price_usd + (Decimal(block_secs.total_seconds()) * price_usd)

        # print(block_number, cumulative_price_usd, time_cumulative_price_usd, price_usd)
        db[f"{db_name_for_contest}_pair_block_cum_price"].insert_one(
                {
                    "pair": pair_id,
                    "block": block_number,
                    "price_usd": Decimal128(price_usd),
                    "cumulative_price_usd": Decimal128(cumulative_price_usd),
                    "time_cumulative_price_usd": Decimal128(time_cumulative_price_usd)
                }
            )

@app.task
def lp_contest_each_user(user: str, latest_block_number: int, latest_block_timestamp: datetime):
    user = hex(int(user, 16))
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit("MONGO_URL not set")
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]

    min_lp_value = 25
    min_time = 60 * 60 * 24 * 30
    total_contest_value = 0
    total_time_eligible = 0
    is_eligible = False
    last_block_number = contest_start_block
    query = dict()
    query["user"] = user
    cursor = db[db_name_for_contest].find(query)
    user_contest = [d for d in cursor]
    if len(user_contest) > 0:
        user_contest = user_contest[0]
        total_contest_value = user_contest["contest_value"].to_decimal()
        total_time_eligible = user_contest["total_time_eligible"]
        is_eligible = user_contest["is_eligible"]
        last_block_number = user_contest["block"]
        last_lp_value_total = user_contest["total_lp_value"].to_decimal()
        last_lp_token_balances = user_contest["lp_token_balances"]
        last_lp_values = user_contest["lp_values"]
        for key, value in last_lp_values.items():
            last_lp_values[key] = value.to_decimal()
    else:
        query = dict()
        query["user"] = user
        query["block"] = {"$lt": contest_start_block}
        query["pair_address"] = {"$in": ELIGIBLE_PAIR_ADDRESSES}
        cursor = db["liquidity_position_snapshots"].distinct("pair_address", query)
        pair_addresses = [d for d in cursor]
        # print(pair_addresses, len(pair_addresses))
        last_lp_values = dict()
        last_lp_token_balances = dict()
        last_lp_value_total = 0
        if len(pair_addresses) != 0:
            for pair_address in pair_addresses:
                query = dict()
                query["user"] = user
                query["block"] = {"$lt": contest_start_block}
                query["pair_address"] = pair_address
                cursor = db["liquidity_position_snapshots"].find(query, limit=1)
                cursor = add_order_by_constraint(cursor, "block", "desc")
                liquidity_position_snapshots = [LiquidityPositionSnapshot.from_mongo(d) for d in cursor]
                # print(liquidity_position_snapshots, len(liquidity_position_snapshots))
                lps = liquidity_position_snapshots[0]
                if lps.liquidity_token_balance != 0:
                    query = dict()
                    query["id"] = lps.pair_id
                    query["$or"] = [
                        {
                            "$and": [
                                {"_chain.valid_to": None},
                                {"_chain.valid_from": {"$lte": contest_start_block}},
                            ]
                        },
                        {
                            "$and": [
                                {"_chain.valid_to": {"$gt": contest_start_block}},
                                {"_chain.valid_from": {"$lte": contest_start_block}},
                            ]
                        },
                    ]
                    cursor = db["pairs"].find(query)
                    required_pair_entry = [d for d in cursor][-1]
                    last_lp_value = (required_pair_entry["reserve_usd"].to_decimal() / required_pair_entry["total_supply"].to_decimal()) * lps.liquidity_token_balance
                    # print(last_lp_value)
                    last_lp_values[lps.pair_id] = last_lp_value
                    last_lp_token_balances[lps.pair_id] = Decimal128(lps.liquidity_token_balance)
                    last_lp_value_total = last_lp_value_total + last_lp_value
    # print(last_lp_values, last_lp_value_total, last_lp_token_balances)
    query = dict()
    query["user"] = user
    query["block"] = {"$lte": latest_block_number, "$gt": last_block_number}
    query["pair_address"] = {"$in": ELIGIBLE_PAIR_ADDRESSES}
    cursor = db["liquidity_position_snapshots"].find(query)
    cursor = add_order_by_constraint(cursor, "block", "asc")
    liquidity_position_snapshots = [LiquidityPositionSnapshot.from_mongo(d) for d in cursor]
    for (i, lps) in enumerate(liquidity_position_snapshots):
        this_block_number = lps.block
        this_pair_id = lps.pair_id
        if i < len(liquidity_position_snapshots) - 1:
            if (liquidity_position_snapshots[i+1].block == this_block_number and liquidity_position_snapshots[i+1].pair_id == this_pair_id):
                continue
        contest_value_contribution = 0
        if this_block_number > last_block_number:
            for pair_id, last_lp_token_balance_for_pair in last_lp_token_balances.items():
                query = dict()
                query["pair"] = pair_id
                query["block"] = {"$in": [this_block_number, last_block_number]}
                cursor = db[f"{db_name_for_contest}_pair_block_cum_price"].find(query)
                cursor = add_order_by_constraint(cursor, "block", "desc")
                pair_block_data = [d for d in cursor]
                cumulative_price_diff_for_pair = pair_block_data[0]["time_cumulative_price_usd"].to_decimal() - pair_block_data[1]["time_cumulative_price_usd"].to_decimal()
                contest_value_contribution = contest_value_contribution + (last_lp_token_balance_for_pair.to_decimal() * cumulative_price_diff_for_pair)
            if last_lp_value_total > min_lp_value:
                block_query = dict()
                block_query["number"] = {"$lte": this_block_number}
                cursor = db["blocks"].find(block_query, limit=1)
                cursor = add_order_by_constraint(cursor, "number", "desc")
                from swap.server.block import Block
                returned_block = [Block.from_mongo(d) for d in cursor][0]
                this_block_timestamp = returned_block.timestamp
                block_query["number"] = {"$lte": last_block_number}
                cursor = db["blocks"].find(block_query, limit=1)
                cursor = add_order_by_constraint(cursor, "number", "desc")
                from swap.server.block import Block
                returned_block = [Block.from_mongo(d) for d in cursor][0]
                last_block_timestamp = returned_block.timestamp
                time_eligible = this_block_timestamp - last_block_timestamp
                total_time_eligible = total_time_eligible + int(time_eligible.total_seconds())
                if total_time_eligible > min_time:
                    is_eligible = True
        total_contest_value = total_contest_value + contest_value_contribution
        this_lp_value = (lps.reserve_usd / lps.liquidity_token_total_supply) * lps.liquidity_token_balance
        last_lp_values[lps.pair_id] = this_lp_value
        last_lp_token_balances[lps.pair_id] = Decimal128(lps.liquidity_token_balance)
        last_block_number = this_block_number
        last_lp_value_total = sum(last_lp_values.values())
        # print(last_block_number, total_contest_value, is_eligible)
    # print(last_lp_values, last_lp_value_total, last_lp_token_balances)
    contest_value_contribution = 0
    if latest_block_number > last_block_number:
        for pair_id, last_lp_token_balance_for_pair in last_lp_token_balances.items():
            query = dict()
            query["pair"] = pair_id
            query["block"] = {"$in": [latest_block_number, last_block_number]}
            cursor = db[f"{db_name_for_contest}_pair_block_cum_price"].find(query)
            cursor = add_order_by_constraint(cursor, "block", "desc")
            pair_block_data = [d for d in cursor]
            cumulative_price_diff_for_pair = pair_block_data[0]["time_cumulative_price_usd"].to_decimal() - pair_block_data[1]["time_cumulative_price_usd"].to_decimal()
            contest_value_contribution = contest_value_contribution + (last_lp_token_balance_for_pair.to_decimal() * cumulative_price_diff_for_pair)
        if last_lp_value_total > min_lp_value:
            block_query = dict()
            block_query["number"] = {"$lte": latest_block_number}
            cursor = db["blocks"].find(block_query, limit=1)
            cursor = add_order_by_constraint(cursor, "number", "desc")
            from swap.server.block import Block
            returned_block = [Block.from_mongo(d) for d in cursor][0]
            latest_block_timestamp_ = returned_block.timestamp
            block_query["number"] = {"$lte": last_block_number}
            cursor = db["blocks"].find(block_query, limit=1)
            cursor = add_order_by_constraint(cursor, "number", "desc")
            from swap.server.block import Block
            returned_block = [Block.from_mongo(d) for d in cursor][0]
            last_block_timestamp = returned_block.timestamp
            time_eligible = latest_block_timestamp_ - last_block_timestamp
            total_time_eligible = total_time_eligible + int(time_eligible.total_seconds())
            if total_time_eligible > min_time:
                is_eligible = True
    total_contest_value = total_contest_value + contest_value_contribution
    # print(latest_block_number, latest_block_timestamp, total_contest_value, is_eligible)
    if total_contest_value == 0:
        total_contest_value = Decimal(0)
    if last_lp_value_total == 0:
        last_lp_value_total = Decimal(0)
    latest_block_timestamp = datetime.fromisoformat(latest_block_timestamp)
    for key, value in last_lp_values.items():
        last_lp_values[key] = Decimal128(value)
    db[f"{db_name_for_contest}_block"].insert_one(
            {
                "user": user,
                "block": latest_block_number,
                "timestamp": latest_block_timestamp,
                "contest_value": Decimal128(total_contest_value),
                "total_lp_value": Decimal128(last_lp_value_total),
                "total_time_eligible": total_time_eligible,
                "is_eligible": is_eligible,
                "lp_token_balances": last_lp_token_balances,
                "lp_values": last_lp_values
            }
        )
    db[db_name_for_contest].find_one_and_replace(
            {
                "user": user,
            },
            {
                "user": user,
                "block": latest_block_number,
                "timestamp": latest_block_timestamp,
                "contest_value": Decimal128(total_contest_value),
                "total_lp_value": Decimal128(last_lp_value_total),
                "total_time_eligible": total_time_eligible,
                "is_eligible": is_eligible,
                "lp_token_balances": last_lp_token_balances,
                "lp_values": last_lp_values
            },
            upsert=True,
        )