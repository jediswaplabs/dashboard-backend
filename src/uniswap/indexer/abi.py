from typing import List

from starknet_py.contract import (FunctionCallSerializer,
                                  identifier_manager_from_abi)

uint256_abi = {
    "name": "Uint256",
    "type": "struct",
    "size": 2,
    "members": [
        {"name": "low", "offset": 0, "type": "felt"},
        {"name": "high", "offset": 1, "type": "felt"},
    ],
}

pair_created_abi = {
    "name": "PairCreated",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "token0", "type": "felt"},
        {"name": "token1", "type": "felt"},
        {"name": "pair", "type": "felt"},
        {"name": "total_pairs", "type": "felt"},
    ],
}

sync_abi = {
    "name": "Sync",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "reserve0", "type": "Uint256"},
        {"name": "reserve1", "type": "Uint256"},
    ],
}

swap_abi = {
    "name": "Swap",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "sender", "type": "felt"},
        {"name": "amount0_in", "type": "Uint256"},
        {"name": "amount1_in", "type": "Uint256"},
        {"name": "amount0_out", "type": "Uint256"},
        {"name": "amount1_out", "type": "Uint256"},
        {"name": "to", "type": "felt"},
    ],
}

transfer_abi = {
    "name": "Transfer",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "from_", "type": "felt"},
        {"name": "to", "type": "felt"},
        {"name": "value", "type": "Uint256"},
    ],
}

mint_abi = {
    "name": "Mint",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "sender", "type": "felt"},
        {"name": "amount0", "type": "Uint256"},
        {"name": "amount1", "type": "Uint256"},
    ],
}

burn_abi = {
    "name": "Burn",
    "type": "event",
    "keys": [],
    "outputs": [
        {"name": "sender", "type": "felt"},
        {"name": "amount0", "type": "Uint256"},
        {"name": "amount1", "type": "Uint256"},
        {"name": "to", "type": "felt"},
    ],
}


def _event_decoder(abi):
    return FunctionCallSerializer(
        abi=abi, identifier_manager=identifier_manager_from_abi([uint256_abi, abi])
    )


pair_created_decoder = _event_decoder(pair_created_abi)
sync_decoder = _event_decoder(sync_abi)
swap_decoder = _event_decoder(swap_abi)
transfer_decoder = _event_decoder(transfer_abi)
mint_decoder = _event_decoder(mint_abi)
burn_decoder = _event_decoder(burn_abi)


def decode_event(decoder: FunctionCallSerializer, data: List[bytes]):
    # starknet.py requires data to be int, not bytes
    return decoder.to_python([int.from_bytes(d, "big") for d in data])
