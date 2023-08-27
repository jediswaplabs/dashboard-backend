# DEX Indexer and GraphQL API with Apibara

This repository shows how to use Apibara to index a decentralized exchange (DEX) and build a GraphQL API.

This project separates the indexer from the API server so that it becomes easier to deploy and scale in a production environment.

You should always run a single instance of the indexer but can run multiple API servers side by side to serve more users.

The project uses:

* the [Apibara Python SDK](https://www.apibara.com/docs/python-sdk) for receiving StarkNet data.
* MongoDB for storage.
* [Strawberry GraphQL](https://strawberry.rocks/) for the GraphQL API server.

## Setting up

Create a new virtual environment for this project. While this step is not required, it is _highly recommended_ to avoid conflicts between different installed packages.

    python3.9 -m venv .venv

Then activate the virtual environment.

    source .venv/bin/activate

Then install `poetry` and use it to install the package dependencies.

    python3 -m pip install poetry
    poetry install

Start MongoDB using the provided `docker-compose` file:

    docker-compose up

Notice that you can use any managed MongoDB like MongoDB Atlas.

## Getting started

This example shows how to index JediSwap on StarkNet. You can change it to index any Uniswap-V2-like DEX by changing the configuration in `src/swap/indexer/jediswap.py`.

Once setup, start indexing by running the following command:

```sh
poetry run swap-indexer indexer
```

where you need to set following environment variables:

* `SERVER_URL`: the Apibara server, should be `mainnet.starknet.a5a.ch`.
* `MONGO_URL`: mongodb connection url. If you use the provided docker compose file use `mongodb://apibara:apibara@localhost:27017`.
* `RPC_URL`: the StarkNet RPC url. You can use Infura for this.
* `APIBARA_AUTH_TOKEN`: _your-apibara-auth-token_

The indexer will then start indexing your DEX.

Start the GraphQL API server with:

```sh
poetry run swap-indexer server
```

where environment variable `MONGO_URL` is the same connection string you used above.
The GraphQL API is served at `http://localhost:8000/graphql` and
it includes a GrapihQL page for testing the API.

## Production deployment & scaling

The repository contains a `docker-compose.prod.yml` file with a good starting point for a production deployment.

Here are some changes you should be considering:

* Use a managed MongoDB, for example MongoDB Atlas.
* Run the indexer and the API server on separate machines.

As the number of users grows, you may need to scale your API server:

* Deploy multiple instances of the API server behind a load balancer.
* Replicate the database and connect the API servers to the replicas.
* Cache entities with e.g. redis.

## License

Copyright 2022 GNC Labs Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
