from decimal import Decimal

import logging

from apibara.indexer import IndexerRunner, IndexerRunnerConfiguration, Info
from apibara.indexer.indexer import IndexerConfiguration
from apibara.protocol.proto.stream_pb2 import Cursor, DataFinality
from apibara.starknet import EventFilter, Filter, StarkNetIndexer, felt
from apibara.starknet.cursor import starknet_cursor
from apibara.starknet.proto.starknet_pb2 import Block, BlockHeader
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.net.models import StarknetChainId
from structlog import get_logger

from swap.indexer.context import IndexerContext
from swap.indexer.core import (handle_burn, handle_mint, handle_swap,
                                  handle_sync, handle_transfer)
from swap.indexer.factory import handle_pair_created, TRANSFER_KEY, SWAP_KEY, SYNC_KEY, MINT_KEY, BURN_KEY
from swap.indexer.jediswap import get_eth_price, index_from_block

FACTORY_ADDRESS = felt.from_hex("0x00dad44c139a476c7a17fc8141e6db680e9abc9f56fe249a105094c44382c2fd")
PAIR_CREATED_KEY = felt.from_hex("0x19437bf1c5c394fc8509a2e38c9c72c152df0bac8be777d4fc8f959ac817189")

logger = get_logger(__name__)

class JediSwapIndexer(StarkNetIndexer):
    _indexer_id: str

    def __init__(self, indexer_id):
        self._indexer_id = indexer_id
        super().__init__()

    def indexer_id(self) -> str:
        return self._indexer_id
    
    def initial_configuration(self) -> Filter:
        # Return initial configuration of the indexer.
        return IndexerConfiguration(
            filter=Filter()
            .with_header(weak=True)
            .add_event(
                EventFilter()
                .with_from_address(FACTORY_ADDRESS)
                .with_keys([PAIR_CREATED_KEY])
            ),
            starting_cursor=starknet_cursor(index_from_block),
            finality=DataFinality.DATA_STATUS_FINALIZED,
        )

    async def handle_data(self, info: Info, data: Block):
        await handle_block(info, data.header)
        await handle_events(self, info, data)

async def handle_block(info: Info, block_header: BlockHeader):
    # Store the block information in the database.
    block = {
        "number": block_header.block_number,
        "hash": hex(felt.to_int(block_header.block_hash)),
        "parent_hash": hex(felt.to_int(block_header.parent_block_hash)),
        "timestamp": block_header.timestamp.ToDatetime(),
    }
    logger.info(
        "handle block", block = block
    )
    
    from swap.tasks import lp_contest_for_block, contest_start_block, contest_end_block, db_name_for_contest
    required_block = block["number"] - 1
    if (required_block % 100) == 0:
        if contest_start_block <= (block["number"] - 1) <= contest_end_block:
            lp_contest_for_block.apply_async(args=[block["number"] - 1], queue=f"{db_name_for_contest}_queue", expires=300)
    await info.storage.insert_one("blocks", block)


async def handle_events(indexer: JediSwapIndexer, info: Info, block: Block):
    block_header = block.header
    info.context.block_hash = hex(felt.to_int(block_header.block_hash))
    info.context.block_number = block_header.block_number
    info.context.block_timestamp = block_header.timestamp.ToDatetime()

    eth_price = await get_eth_price(info)
    if eth_price is None:
        eth_price = Decimal("0")
    info.context.eth_price = eth_price

    logger.info(
        "handle events", block_number=block_header.block_number, block_timestamp=block_header.timestamp.ToDatetime()
    )

    for event_with_tx in block.events:
        transaction_hash = hex(felt.to_int(event_with_tx.transaction.meta.hash))
        event = event_with_tx.event
        if event.keys[0] == PAIR_CREATED_KEY:
            logger.info("event name", event_name="PairCreated")
            await handle_pair_created(indexer, info, block_header, event)
        elif event.keys[0] == TRANSFER_KEY:
            logger.info("event name", event_name="Transfer")
            await handle_transfer(info, event, transaction_hash)
        elif event.keys[0] == SWAP_KEY:
            logger.info("event name", event_name="Swap")
            await handle_swap(info, event, transaction_hash)
        elif event.keys[0] == SYNC_KEY:
            logger.info("event name", event_name="Sync")
            await handle_sync(info, event, transaction_hash)
        elif event.keys[0] == MINT_KEY:
            logger.info("event name", event_name="Mint")
            await handle_mint(info, event, transaction_hash)
        elif event.keys[0] == BURN_KEY:
            logger.info("event name", event_name="Burn")
            await handle_burn(info, event, transaction_hash)
        else:
            logger.warn(f"Unhandled event {event.name}")


async def run_indexer(server_url, mongodb_url, rpc_url, indexer_id, restart):
    runner = IndexerRunner(
        config=IndexerRunnerConfiguration(
            stream_url=server_url,
            storage_url=mongodb_url,
        ),
        reset_state=restart,
    )

    context = IndexerContext(
        rpc=FullNodeClient(rpc_url, net=StarknetChainId.MAINNET),
        block_hash=0,
        block_number=0,
        block_timestamp=None,
        eth_price=Decimal("0"),
    )

    while True:
        await runner.run(JediSwapIndexer(indexer_id), ctx=context)
        logger.warn("disconnected. reconnecting.")

