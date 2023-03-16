from decimal import Decimal

import logging

from apibara.indexer import IndexerRunner, IndexerRunnerConfiguration, Info
from apibara.indexer.indexer import IndexerConfiguration
from apibara.protocol.proto.stream_pb2 import Cursor, DataFinality
from apibara.starknet import EventFilter, Filter, StarkNetIndexer, felt
from apibara.starknet.cursor import starknet_cursor
from apibara.starknet.proto.starknet_pb2 import Block, BlockHeader
# from starknet_py.net.full_node_client import FullNodeClient
# from starknet_py.net.models import StarknetChainId
from structlog import get_logger

from swap.indexer.context import IndexerContext
from swap.indexer.core import (handle_burn, handle_mint, handle_swap,
                                  handle_sync, handle_transfer)
from swap.indexer.factory import handle_pair_created
from swap.indexer.jediswap import get_eth_price, index_from_block

factory_address = felt.from_hex("0x00dad44c139a476c7a17fc8141e6db680e9abc9f56fe249a105094c44382c2fd")
pair_created_key = felt.from_hex("0x19437bf1c5c394fc8509a2e38c9c72c152df0bac8be777d4fc8f959ac817189")
transfer_key = felt.from_hex("0x99cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9")
swap_key = felt.from_hex("0xe316f0d9d2a3affa97de1d99bb2aac0538e2666d0d8545545ead241ef0ccab")
sync_key = felt.from_hex("0xe14a408baf7f453312eec68e9b7d728ec5337fbdf671f917ee8c80f3255232")
mint_key = felt.from_hex("0x34e55c1cd55f1338241b50d352f0e91c7e4ffad0e4271d64eb347589ebdfd16")
burn_key = felt.from_hex("0x243e1de00e8a6bc1dfa3e950e6ade24c52e4a25de4dee7fb5affe918ad1e744")


_event_to_handler = {
    "PairCreated": handle_pair_created,
    "Transfer": handle_transfer,
    "Swap": handle_swap,
    "Sync": handle_sync,
    "Mint": handle_mint,
    "Burn": handle_burn,
}

# _key_to_event = {
#     pair_created_key: 1,
#     "Transfer": 2,
#     "Swap": handle_swap,
#     "Sync": handle_sync,
#     "Mint": handle_mint,
#     "Burn": handle_burn,
# }

logger = get_logger(__name__)

async def handle_block(info: Info, block_header: BlockHeader):
    # Store the block information in the database.
    block = {
        "number": block_header.block_number,
        "hash": block_header.block_hash,
        "parent_hash": block_header.parent_block_hash,
        "timestamp": block_header.timestamp,
    }
    logger.info(
        "handle block", block_number=block["number"], block_timestamp=block["timestamp"]
    )
    
    from swap.tasks import lp_contest_for_block, contest_start_block, contest_end_block, db_name_for_contest
    required_block = block["number"] - 1
    if (required_block % 100) == 0:
        if contest_start_block <= (block["number"] - 1) <= contest_end_block:
            lp_contest_for_block.apply_async(args=[block["number"] - 1], queue=f"{db_name_for_contest}_queue", expires=300)
    await info.storage.insert_one("blocks", block)


async def handle_events(info: Info[IndexerContext], data: Block):
    block_header = data.header
    info.context.block_hash = int.from_bytes(block_header.block_hash, "big")
    info.context.block_number = block_header.block_number
    info.context.block_timestamp = block_header.timestamp

    eth_price = await get_eth_price(info)
    if eth_price is None:
        eth_price = Decimal("0")
    info.context.eth_price = eth_price

    logger.info(
        "handle events", block_number=block_header.block_number, block_timestamp=block_header.timestamp
    )

    for event in data.events:
        logger.info(
            "event key", event_key=event.keys
        )
        handler = _event_to_handler.get(event.name)
        if handler is not None:
            await handler(info, block_header, event)
        else:
            logger.warn(f"Unhandled event {event.name}")

class JediSwapIndexer(StarkNetIndexer):
    def indexer_id(self) -> str:
        return "jediswap-testnet"
    
    def initial_configuration(self) -> Filter:
        # Return initial configuration of the indexer.
        return IndexerConfiguration(
            filter=Filter().add_event(
                EventFilter().with_from_address(factory_address).with_keys([pair_created_key])
            ),
            starting_cursor=starknet_cursor(index_from_block),
            finality=DataFinality.DATA_STATUS_FINALIZED,
        )

    async def handle_data(self, info: Info, data: Block):
        await handle_block(info, data.header)
        await handle_events(info, data)


async def run_indexer(server_url, mongodb_url, rpc_url, indexer_id, restart):
    runner = IndexerRunner(
        config=IndexerRunnerConfiguration(
            stream_url=server_url,
            storage_url=mongodb_url,
        ),
        reset_state=restart,
        # indexer_id=indexer_id,
        # new_events_handler=handle_events,
    )

    context = IndexerContext(
        # rpc=FullNodeClient(rpc_url, net=StarknetChainId.MAINNET),
        block_hash=0,
        block_number=0,
        block_timestamp=None,
        eth_price=Decimal("0"),
    )
    # runner.set_context(context)

    # runner.add_block_handler(handle_block)

    # Add a filter on the factory to detect when pairs are created.
    # runner.add_event_filters(
    #     filters=[
    #         EventFilter.from_event_name(name="PairCreated", address=factory_address)
    #     ],
    #     index_from_block=index_from_block,
    # )

    while True:
        await runner.run(JediSwapIndexer(), ctx=context)
        logger.warn("disconnected. reconnecting.")

