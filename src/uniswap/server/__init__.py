import asyncio
from datetime import datetime
from typing import List, NewType, Optional

import strawberry
from aiohttp import web
from pymongo import MongoClient
from strawberry.aiohttp.views import GraphQLView

from uniswap.server.query import Query


class IndexerGraphQLView(GraphQLView):
    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        self._db = db

    async def get_context(self, _request, _response):
        return {"db": self._db}


async def run_graphql_server(mongo_url, indexer_id):
    mongo = MongoClient(mongo_url)
    db_name = indexer_id.replace("-", "_")
    db = mongo[db_name]

    schema = strawberry.Schema(query=Query)
    view = IndexerGraphQLView(db, schema=schema)

    app = web.Application()
    app.router.add_route("*", "/graphql", view)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", "8080")
    await site.start()

    print(f"GraphQL server started on port 8080")

    while True:
        await asyncio.sleep(5_000)
