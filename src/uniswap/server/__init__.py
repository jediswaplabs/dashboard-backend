import asyncio
from datetime import datetime
from typing import List, NewType, Optional

import aiohttp_cors
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
