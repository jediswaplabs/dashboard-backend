from datetime import datetime
from decimal import Decimal

from apibara import EventFilter, Info, NewEvents
from apibara.indexer.runner import IndexerRunner, IndexerRunnerConfiguration
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.net.models import StarknetChainId
from structlog import get_logger

from uniswap.indexer.context import IndexerContext
from uniswap.indexer.core import (handle_burn, handle_mint, handle_swap,
                                  handle_sync, handle_transfer)
from uniswap.indexer.factory import handle_pair_created
from uniswap.indexer.jediswap import get_eth_price

factory_address = "0x00dad44c139a476c7a17fc8141e6db680e9abc9f56fe249a105094c44382c2fd"


_event_to_handler = {
    "PairCreated": handle_pair_created,
    "Transfer": handle_transfer,
    "Swap": handle_swap,
    "Sync": handle_sync,
    "Mint": handle_mint,
    "Burn": handle_burn,
}

logger = get_logger(__name__)


async def handle_events(info: Info[IndexerContext], block_events: NewEvents):
    block = block_events.block
    info.context.block_hash = int.from_bytes(block.hash, "big")
    info.context.block_number = block.number
    info.context.block_timestamp = block.timestamp

    eth_price = await get_eth_price(info)
    if eth_price is None:
        eth_price = Decimal("0")
    info.context.eth_price = eth_price

    logger.info(
        "handle events", block_number=block.number, block_timestamp=block.timestamp
    )

    for event in block_events.events:
        handler = _event_to_handler.get(event.name)
        if handler is not None:
            await handler(info, block, event)
        else:
            logger.warn(f"Unhandled event {event.name}")


async def run_indexer(stream_url, mongodb_url, rpc_url, indexer_id, restart):
    runner = IndexerRunner(
        config=IndexerRunnerConfiguration(
            apibara_url=stream_url,
            storage_url=mongodb_url,
        ),
        reset_state=restart,
        indexer_id=indexer_id,
        new_events_handler=handle_events,
    )

    context = IndexerContext(
        rpc=FullNodeClient(rpc_url, net=StarknetChainId.MAINNET),
        block_hash=0,
        block_number=0,
        block_timestamp=None,
        eth_price=Decimal("0"),
    )
    runner.set_context(context)

    # Add a filter on the factory to detect when pairs are created.
    runner.add_event_filters(
        filters=[
            EventFilter.from_event_name(name="PairCreated", address=factory_address)
        ],
        index_from_block=10_760,
    )

    await runner.run()
