from typing import List

from apibara.starknet import felt
from apibara.starknet.proto.types_pb2 import FieldElement

from collections import namedtuple

# from starknet_py.contract import serializer_for_function

def from_uint256(low: FieldElement, high: FieldElement) -> int:
    return felt.to_int(low) + (felt.to_int(high) << 128)

# uint256_abi = {
#     "name": "Uint256",
#     "type": "struct",
#     "size": 2,
#     "members": [
#         {"name": "low", "offset": 0, "type": "felt"},
#         {"name": "high", "offset": 1, "type": "felt"},
#     ],
# }

# pair_created_abi = {
#     "name": "PairCreated",
#     "type": "event",
#     "keys": [],
#     "outputs": [
#         {"name": "token0", "type": "felt"},
#         {"name": "token1", "type": "felt"},
#         {"name": "pair", "type": "felt"},
#         {"name": "total_pairs", "type": "felt"},
#     ],
# }

# sync_abi = {
#     "name": "Sync",
#     "type": "event",
#     "keys": [],
#     "outputs": [
#         {"name": "reserve0", "type": "Uint256"},
#         {"name": "reserve1", "type": "Uint256"},
#     ],
# }

# swap_abi = {
#     "name": "Swap",
#     "type": "event",
#     "keys": [],
#     "outputs": [
#         {"name": "sender", "type": "felt"},
#         {"name": "amount0_in", "type": "Uint256"},
#         {"name": "amount1_in", "type": "Uint256"},
#         {"name": "amount0_out", "type": "Uint256"},
#         {"name": "amount1_out", "type": "Uint256"},
#         {"name": "to", "type": "felt"},
#     ],
# }

# transfer_abi = {
#     "name": "Transfer",
#     "type": "event",
#     "keys": [],
#     "outputs": [
#         {"name": "from_", "type": "felt"},
#         {"name": "to", "type": "felt"},
#         {"name": "value", "type": "Uint256"},
#     ],
# }

# mint_abi = {
#     "name": "Mint",
#     "type": "event",
#     "keys": [],
#     "outputs": [
#         {"name": "sender", "type": "felt"},
#         {"name": "amount0", "type": "Uint256"},
#         {"name": "amount1", "type": "Uint256"},
#     ],
# }

# burn_abi = {
#     "name": "Burn",
#     "type": "event",
#     "keys": [],
#     "outputs": [
#         {"name": "sender", "type": "felt"},
#         {"name": "amount0", "type": "Uint256"},
#         {"name": "amount1", "type": "Uint256"},
#         {"name": "to", "type": "felt"},
#     ],
# }


# def _event_decoder(abi):
#     return serializer_for_function(abi=abi)


# pair_created_decoder = _event_decoder(pair_created_abi)
# sync_decoder = _event_decoder(sync_abi)
# swap_decoder = _event_decoder(swap_abi)
# transfer_decoder = _event_decoder(transfer_abi)
# mint_decoder = _event_decoder(mint_abi)
# burn_decoder = _event_decoder(burn_abi)


def decode_event(event_name: str, data: List[bytes]):
    # starknet.py requires data to be int, not bytes
    if event_name == 'PairCreated':
        token0 = felt.to_int(data[0])
        token1 = felt.to_int(data[1])
        pair = felt.to_int(data[2])
        total_pairs = felt.to_int(data[3])
        pair_created = namedtuple('pair_created', ['token0', 'token1', 'pair', 'total_pairs'])
        return pair_created(token0, token1, pair, total_pairs)
    if event_name == 'Sync':
        reserve0 = from_uint256(data[0], data[1])
        reserve1 = from_uint256(data[2], data[3])
        sync = namedtuple('sync', ['reserve0', 'reserve1'])
        return sync(reserve0, reserve1)
    if event_name == 'Swap':
        sender = felt.to_int(data[0])
        amount0_in = from_uint256(data[1], data[2])
        amount1_in = from_uint256(data[3], data[4])
        amount0_out = from_uint256(data[5], data[6])
        amount1_out = from_uint256(data[7], data[8])
        to = felt.to_int(data[9])
        swap = namedtuple('swap', ['sender', 'amount0_in', 'amount1_in', 'amount0_out', 'amount1_out', 'to'])
        return swap(sender, amount0_in, amount1_in, amount0_out, amount1_out, to)
    if event_name == 'Transfer':
        from_ = felt.to_int(data[0])
        to = felt.to_int(data[1])
        value = from_uint256(data[2], data[3])
        transfer = namedtuple('transfer', ['from_', 'to', 'value'])
        return transfer(from_, to, value)
    if event_name == 'Mint':
        sender = felt.to_int(data[0])
        amount0 = from_uint256(data[1], data[2])
        amount1 = from_uint256(data[3], data[4])
        mint = namedtuple('mint', ['sender', 'amount0', 'amount1'])
        return mint(sender, amount0, amount1)
    if event_name == 'Burn':
        sender = felt.to_int(data[0])
        amount0 = from_uint256(data[1], data[2])
        amount1 = from_uint256(data[3], data[4])
        to = felt.to_int(data[5])
        burn = namedtuple('burn', ['sender', 'amount0', 'amount1', 'to'])
        return burn(sender, amount0, amount1, to)
