import asyncio
from functools import wraps

import click
from structlog import get_logger

from uniswap.indexer import run_indexer

logger = get_logger(__name__)


def async_command(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@click.group()
def cli():
    pass


@cli.command()
@click.option("--stream-url", default=None, help="stream url.")
@click.option("--mongo-url", default=None, help="MongoDB url.")
@click.option("--rpc-url", default=None, help="StarkNet RPC url.")
@click.option("--restart", is_flag=True, help="Restart indexing from the beginning.")
@async_command
async def indexer(stream_url, mongo_url, rpc_url, restart):
    logger.info(
        "starting indexer",
        stream_url=stream_url,
        # skip mongo_url: contains password
        rpc_url=rpc_url,
        restart=restart,
    )
    await run_indexer(stream_url, mongo_url, rpc_url, restart)


@cli.command()
@click.option("--mongo-url", default=None, help="MongoDB url.")
@async_command
async def server(mongo_url):
    logger.info("starting server")
