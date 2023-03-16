import os
from pymongo import MongoClient, ASCENDING
from swap.main import indexer_id
from swap.server.lp_contest import db_name_for_contest

mongo_url = os.environ.get('MONGO_URL', None)
mongo = MongoClient(mongo_url)
db_name = indexer_id.replace("-", "_")
db = mongo[db_name]

db["tokens"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["tokens"].create_index([("_chain.valid_to", ASCENDING)])
db["tokens"].create_index([("_chain.valid_from", ASCENDING)])

db["pairs"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pairs"].create_index([("token0_id", ASCENDING), ("token1_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pairs"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING), ("_chain.valid_from", ASCENDING)])
db["pairs"].create_index([("_chain.valid_from", ASCENDING)])
db["pairs"].create_index([("_chain.valid_to", ASCENDING)])

db["token_day_data"].create_index([("token_id", ASCENDING), ("day_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["token_day_data"].create_index([("_chain.valid_to", ASCENDING)])

db[f"{db_name_for_contest}_block"].create_index([("_chain.valid_from", ASCENDING)])
db[f"{db_name_for_contest}_block"].create_index([("_chain.valid_to", ASCENDING)])

db["exchange_day_data"].create_index([("address", ASCENDING), ("day_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["exchange_day_data"].create_index([("_chain.valid_to", ASCENDING)])

db["factories"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])

db["pair_day_data"].create_index([("pair_id", ASCENDING), ("day_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pair_day_data"].create_index([("_chain.valid_to", ASCENDING)])

db["pair_hour_data"].create_index([("pair_id", ASCENDING), ("hour_id", ASCENDING), ("_chain.valid_to", ASCENDING)])
db["pair_hour_data"].create_index([("_chain.valid_to", ASCENDING)])

db["users"].create_index([("id", ASCENDING), ("_chain.valid_to", ASCENDING)])

db["liquidity_position_snapshots"].create_index([("block", ASCENDING)])
