from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from starknet_py.net.full_node_client import FullNodeClient


@dataclass
class IndexerContext:
    block_hash: int
    block_number: int
    block_timestamp: datetime
    eth_price: Decimal
    rpc: FullNodeClient
