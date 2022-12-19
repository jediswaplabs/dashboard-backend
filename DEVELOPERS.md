# Developer's guide

This project is a starting point to build a GraphQL API for your DEX. As such, you may need to make several changes to adapt it to your use case.

## Indexer

The indexer's code is in the `src/swap/indexer` folder:

```ml
src/swap/indexer/
├── abi.py: "decode starknet events into python objects"
├── context.py: "define shared context between handlers"
├── core.py: "handle pool's events"
├── daily.py: "create and update daily price snapshots"
├── factory.py: "handle factory's events"
├── helpers.py: "utilities to create/update entities"
├── __init__.py: "configure and run the indexer"
└── jediswap.py: "dex configuration"
```

## GraphQL API

The API code is in the `src/swap/server` folder:

```ml
src/swap/server/
├── factory.py: "factory entity and resolvers"
├── helpers.py: "graphql scalars and query helpers"
├── __init__.py: "configure and run graphql server"
├── liquidity_position.py: "liquidity position entities and resolvers"
├── pair.py: "pair entities and resolvers"
├── query.py: "root Query entity"
├── token.py: "token entities and resolvers"
└── transaction.py: "transactions entities and resolvers"
```

### Customizing the API

The server use Strawberry GraphQL and so it can be customized to fit your needs.

In this section, we show how to achieve common tasks:


**Adding new entities**

Create new entities by defining new [object types](https://strawberry.rocks/docs/types/object-types).
Notice that Strawberry relies on Python's type annotations to derive its schema.

Then add a `from_mongo(cls, data)` class method to the new entity, this method is responsible for converting the mongodb object into the entity.

```py
@strawberry.type
class LiquidityPosition:
    # fields here...

    @classmethod
    def from_mongo(cls, data):
        return cls(
            user_id=data["user"],
            pair_id=data["pair_address"],
            liquidity_token_balance=data["liquidity_token_balance"].to_decimal(),
        )
```

If your entity has a child entity, you should only store the child's id and then add a custom field to resolve it. Use the `strawberry.Private[...]` type to mark a field as private (it won't be returned to the user) and then define a `strawberry.field` to lookup the child on demand.

```py
@strawberry.type
class LiquidityPosition:
    pair_id: strawberry.Private[FieldElement]

    @strawberry.field
    def pair(self, info: Info) -> Pair:
        return get_pair(info, self.pair_id)
```

**Adding new filters**

Add new filters to queries by adding more parameters to resolvers. Strawberry uses the type annotations to derive the type of the filter.
You can combine [input types](https://strawberry.rocks/docs/types/input-types) with [enums](https://strawberry.rocks/docs/types/enums) to match any GraphQL API you have in mind.

For example, the `uniswapFactories` root query delegates fetching the DEX factories to the `swap.server.factory.get_factories` method.

```py
async def get_factories(
    info,
    block: Optional[BlockFilter] = None,
    where: Optional[FactoryFilter] = None
) -> List[Factory]:
    # code here...
```

The `BlockFilter` input is used to return data at a specific block number. This filter is defined as:

```py
@strawberry.input
class BlockFilter:
    number: Optional[int]
```

The `FactoryFilter` is used to lookup a specific factory by its id.

```py
@strawberry.input
class FactoryFilter:
    id: Optional[FieldElement]
```


**Implementing resolvers**

Resolvers are small functions that, given a query, return zero or more entities.
All resolvers receive a special `info` parameter that contains the server context. This context contains a shared reference to the current database.
The `db` object is a pymongo's database object.

```py
from pymongo.database import Database


async def get_foo(info) -> List[Foo]:
    db: Database = info.context["db"]
```

Notice that the indexer stores all versions of an entity, including the past version of entities that have been updated.
You should use the `add_block_constraint` helper function to select entities that exist at a specific block (or `None` for the most recent version).

```py
from swap.server.helpers import add_block_constraint


def get_foo(info) -> List[Foo]:
    db: Database = info.context["db"]

    query = dict()
    add_block_constraint(query, block)

    # update query here...

    cursor = db["foos"].find(query)

    # return values here...
```

You can learn more about the database by connecting to it with a tool like MongoDB Compass and looking at the entities.