from decimal import Decimal


from apibara.starknet import EventFilter, felt, Filter
from apibara.indexer import Info
from apibara.starknet.proto.starknet_pb2 import BlockHeader, Event
from bson import Decimal128
from structlog import get_logger

from swap.indexer.abi import decode_event
from swap.indexer.helpers import create_token

logger = get_logger(__name__)

TRANSFER_KEY = felt.from_hex("0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9")
SWAP_KEY = felt.from_hex("0xe316f0d9d2a3affa97de1d99bb2aac0538e2666d0d8545545ead241ef0ccab")
SYNC_KEY = felt.from_hex("0xe14a408baf7f453312eec68e9b7d728ec5337fbdf671f917ee8c80f3255232")
MINT_KEY = felt.from_hex("0x34e55c1cd55f1338241b50d352f0e91c7e4ffad0e4271d64eb347589ebdfd16")
BURN_KEY = felt.from_hex("0x243e1de00e8a6bc1dfa3e950e6ade24c52e4a25de4dee7fb5affe918ad1e744")


async def handle_pair_created(indexer, info: Info, header: BlockHeader, event: Event):
    pair_created = decode_event('PairCreated', event.data)
    factory_address = hex(felt.to_int(event.from_address))
    logger.info("handle PairCreated", **pair_created._asdict())

    # Update factory
    existing_factory = await info.storage.find_one_and_update(
        "factories", {"id": factory_address}, {"$inc": {"pair_count": 1}}
    )

    if existing_factory is None:
        await info.storage.insert_one(
            "factories",
            {
                "id": factory_address,
                "pair_count": 1,
                # total volume
                "total_volume_usd": Decimal128("0"),
                "total_volume_eth": Decimal128("0"),
                # untracked volume
                "untracked_volume_usd": Decimal128("0"),
                # total liquidity
                "total_liquidity_usd": Decimal128("0"),
                "total_liquidity_eth": Decimal128("0"),
                # transactions
                "transaction_count": 0,
            },
        )

    # create or update tokens
    token0 = await create_token(info, pair_created.token0)
    token1 = await create_token(info, pair_created.token1)
    logger.info("new pool", token0=token0, token1=token1)

    # create pair
    await info.storage.insert_one(
        "pairs",
        {
            "id": hex(pair_created.pair),
            "token0_id": token0["id"],
            "token1_id": token1["id"],
            "reserve0": Decimal128("0"),
            "reserve1": Decimal128("0"),
            "total_supply": Decimal128("0"),
            # derived liquidity
            "reserve_eth": Decimal128("0"),
            "reserve_usd": Decimal128("0"),
            "tracked_reserve_eth": Decimal128("0"),
            # asset pair price
            "token0_price": Decimal128("0"),
            "token1_price": Decimal128("0"),
            # lifetime volume
            "volume_token0": Decimal128("0"),
            "volume_token1": Decimal128("0"),
            "volume_usd": Decimal128("0"),
            "untracked_volume_usd": Decimal128("0"),
            "transaction_count": 0,
            # creation stats
            "created_at_timestamp": header.timestamp.ToDatetime(),
            "created_at_block": header.block_number,
            "liquidity_provider_count": 0,
        },
    )

    # start tracking events from pair contract
    pair_address_felt = felt.from_int(pair_created.pair)
    indexer.update_filter(
            Filter()
            .with_header(weak=False)
            .add_event(
                EventFilter().with_from_address(pair_address_felt).with_keys([TRANSFER_KEY])
            )
            .add_event(
                EventFilter().with_from_address(pair_address_felt).with_keys([SWAP_KEY])
            )
            .add_event(
                EventFilter().with_from_address(pair_address_felt).with_keys([SYNC_KEY])
            )
            .add_event(
                EventFilter().with_from_address(pair_address_felt).with_keys([MINT_KEY])
            )
            .add_event(
                EventFilter().with_from_address(pair_address_felt).with_keys([BURN_KEY])
            )
        )
