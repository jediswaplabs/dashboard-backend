import os
from pymongo import MongoClient, ASCENDING, DESCENDING
from swap.main import indexer_id
from swap.server.lp_contest import db_name_for_contest

mongo_url = os.environ.get('MONGO_URL', None)
mongo = MongoClient(mongo_url)
db_name = indexer_id.replace("-", "_")
db = mongo[db_name]

db["blocks"].create_index([("number", DESCENDING)])
db["blocks"].create_index([("_chain.valid_to", ASCENDING), ("timestamp", DESCENDING)])
db["blocks"].create_index([("_chain.valid_to", ASCENDING), ("timestamp", ASCENDING)])

db["tokens"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["tokens"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING), ("_chain.valid_from", ASCENDING)])
db["tokens"].create_index([("_chain.valid_to", ASCENDING)])
db["tokens"].create_index([("_chain.valid_from", ASCENDING)])

db["pairs"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pairs"].create_index([("token0_id", ASCENDING), ("token1_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pairs"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING), ("_chain.valid_from", ASCENDING)])
db["pairs"].create_index([("_chain.valid_from", ASCENDING)])
db["pairs"].create_index([("_chain.valid_to", ASCENDING)])

db["token_day_data"].create_index([("token_id", ASCENDING), ("day_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["token_day_data"].create_index([("_chain.valid_to", ASCENDING)])
db["token_day_data"].create_index([("_chain.valid_from", ASCENDING)])

db[f"{db_name_for_contest}"].create_index([("user", ASCENDING)])
db[f"{db_name_for_contest}"].create_index([("contest_value", DESCENDING)])
db[f"{db_name_for_contest}"].create_index([("_chain.valid_from", ASCENDING)])
db[f"{db_name_for_contest}"].create_index([("_chain.valid_to", ASCENDING)])

db[f"{db_name_for_contest}_block"].create_index([("_chain.valid_from", ASCENDING)])
db[f"{db_name_for_contest}_block"].create_index([("_chain.valid_to", ASCENDING)])
db[f"{db_name_for_contest}_block"].create_index([("user", ASCENDING)])

db[f"{db_name_for_contest}_pair_block_cum_price"].create_index([("pair", ASCENDING), ("block", ASCENDING)])

db["exchange_day_data"].create_index([("address", ASCENDING), ("day_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["exchange_day_data"].create_index([("_chain.valid_to", ASCENDING)])
db["exchange_day_data"].create_index([("_chain.valid_from", ASCENDING)])

db["factories"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["factories"].create_index([("_chain.valid_to", ASCENDING), ("_chain.valid_from", ASCENDING)])
db["factories"].create_index([("_chain.valid_from", ASCENDING)])

db["pair_day_data"].create_index([("pair_id", ASCENDING), ("day_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pair_day_data"].create_index([("_chain.valid_to", ASCENDING)])
db["pair_day_data"].create_index([("_chain.valid_from", ASCENDING)])

db["pair_hour_data"].create_index([("pair_id", ASCENDING), ("hour_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pair_hour_data"].create_index([("_chain.valid_to", ASCENDING)])
db["pair_hour_data"].create_index([("_chain.valid_from", ASCENDING)])

db["users"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["users"].create_index([("_chain.valid_to", ASCENDING)])
db["users"].create_index([("_chain.valid_from", ASCENDING)])

db["liquidity_positions"].create_index([("_chain.valid_to", ASCENDING)])
db["liquidity_positions"].create_index([("_chain.valid_from", ASCENDING)])
db["liquidity_positions"].create_index([("_chain.valid_to", ASCENDING), ("pair_address", ASCENDING), ("user", ASCENDING)])
db["liquidity_positions"].create_index([("_chain.valid_to", ASCENDING), ("pair_address", ASCENDING), ("liquidity_token_balance", DESCENDING)])
db["liquidity_positions"].create_index([("_chain.valid_to", ASCENDING), ("liquidity_token_balance", DESCENDING)])

db["liquidity_position_snapshots"].create_index([("block", ASCENDING)])
db["liquidity_position_snapshots"].create_index([("user", ASCENDING), ("block", ASCENDING)])
db["liquidity_position_snapshots"].create_index([("user", ASCENDING), ("block", ASCENDING), ("pair_address", ASCENDING)])
db["liquidity_position_snapshots"].create_index([("_chain.valid_to", ASCENDING)])
db["liquidity_position_snapshots"].create_index([("_chain.valid_from", ASCENDING)])

db["transactions"].create_index([("hash", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["transactions"].create_index([("_chain.valid_to", ASCENDING)])
db["transactions"].create_index([("_chain.valid_from", ASCENDING)])
db["transactions"].create_index([("_chain.valid_to", ASCENDING), ("block_timestamp", DESCENDING)])

db["swaps"].create_index([("to", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["swaps"].create_index([("transaction_hash", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["swaps"].create_index([("pair_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["swaps"].create_index([("pair_id", ASCENDING), ("_chain.valid_to", ASCENDING), ("timestamp", DESCENDING)])
db["swaps"].create_index([("pair_id", ASCENDING), ("transaction_hash", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["swaps"].create_index([("_chain.valid_to", ASCENDING)])
db["swaps"].create_index([("_chain.valid_from", ASCENDING)])

db["mints"].create_index([("transaction_hash", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["mints"].create_index([("pair_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["mints"].create_index([("pair_id", ASCENDING), ("transaction_hash", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["mints"].create_index([("pair_id", ASCENDING), ("to", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["mints"].create_index([("_chain.valid_to", ASCENDING)])
db["mints"].create_index([("_chain.valid_from", ASCENDING)])
db["mints"].create_index([("to", ASCENDING), ("_chain.valid_to", ASCENDING), ("block_timestamp", DESCENDING)])
db["mints"].create_index([("pair_id", ASCENDING), ("_chain.valid_to", ASCENDING), ("timestamp", DESCENDING)])

db["burns"].create_index([("transaction_hash", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["burns"].create_index([("pair_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["burns"].create_index([("_chain.valid_to", ASCENDING)])
db["burns"].create_index([("_chain.valid_from", ASCENDING)])
db["burns"].create_index([("sender", ASCENDING), ("_chain.valid_to", ASCENDING), ("timestamp", DESCENDING)])
db["burns"].create_index([("pair_id", ASCENDING), ("_chain.valid_to", ASCENDING), ("timestamp", DESCENDING)])