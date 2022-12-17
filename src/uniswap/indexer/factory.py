from decimal import Decimal
from apibara import EventFilter, Info
from apibara.model import BlockHeader, StarkNetEvent
from bson import Decimal128
from structlog import get_logger

from uniswap.indexer.abi import decode_event, pair_created_decoder
from uniswap.indexer.helpers import create_token, felt


logger = get_logger(__name__)


async def handle_pair_created(info: Info, header: BlockHeader, event: StarkNetEvent):
    pair_created = decode_event(pair_created_decoder, event.data)
    factory_address = int.from_bytes(event.address, "big")
    logger.info("handle PairCreated", **pair_created._asdict())

    # Update factory
    existing_factory = await info.storage.find_one_and_update(
        "factories", {"id": felt(factory_address)}, {"$inc": {"pair_count": 1}}
    )

    if existing_factory is None:
        await info.storage.insert_one(
            "factories",
            {
                "id": felt(factory_address),
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
            "id": felt(pair_created.pair),
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
            "created_at_timestamp": header.timestamp,
            "created_at_block": header.number,
            "liquidity_provider_count": 0,
        },
    )

    # start tracking events from pair contract
    info.add_event_filters(
        filters=[
            EventFilter.from_event_name("Swap", address=pair_created.pair),
            EventFilter.from_event_name("Sync", address=pair_created.pair),
            EventFilter.from_event_name("Transfer", address=pair_created.pair),
            EventFilter.from_event_name("Mint", address=pair_created.pair),
            EventFilter.from_event_name("Burn", address=pair_created.pair),
        ]
    )
