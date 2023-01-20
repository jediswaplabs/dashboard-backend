import asyncio
from datetime import datetime
from typing import List, NewType, Optional

import aiohttp_cors
import strawberry
from strawberry.dataloader import DataLoader
from aiohttp import web
from pymongo import MongoClient
from strawberry.aiohttp.views import GraphQLView

from swap.server.query import Query
from swap.server.pair import Pair, get_pair
from swap.server.token import Token, get_token

async def load_tokens(db, keys) -> List[Token]:
    return [get_token(db, key) for key in keys]

async def load_pairs(db, keys) -> List[Pair]:
    return [get_pair(db, key) for key in keys]

class IndexerGraphQLView(GraphQLView):
    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        self._db = db

    async def get_context(self, _request, _response):
        return {"db": self._db, 
                "token_loader": DataLoader(load_fn=lambda ids: load_tokens(self._db, ids)),
                "pair_loader": DataLoader(load_fn=lambda ids: load_pairs(self._db, ids))}


async def run_graphql_server(mongo_url, indexer_id):
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]

    schema = strawberry.Schema(query=Query)
    view = IndexerGraphQLView(db, schema=schema)

    app = web.Application()
    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True, allow_headers="*", allow_methods="*"
            )
        },
    )

    resource = cors.add(app.router.add_resource("/graphql"))
    cors.add(resource.add_route("*", view))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", "8000")
    await site.start()

    print(f"GraphQL server started on port 8000")

    while True:
        await asyncio.sleep(5_000)
