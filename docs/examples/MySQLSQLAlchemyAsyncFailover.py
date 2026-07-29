#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Async SQLAlchemy + AWS Advanced Python Wrapper: failover on Aurora MySQL.

Uses aiomysql as the async MySQL driver. The dialect
`mysql+aws_wrapper_aiomysql` routes create_async_engine through the
wrapper's async plugin pipeline.

Wrapper plugins are configured via the `wrapper_plugins` URL alias
(SA reserves the `plugins` query-string key for its own plugin loader).
"""

import asyncio

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from aws_advanced_python_wrapper.aio import release_resources_async

CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "mysql"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_async_engine(
        f"mysql+aws_wrapper_aiomysql://{USER}:{PASSWORD}@"
        f"{CLUSTER_ENDPOINT}:3306/{DB_NAME}"
        "?wrapper_dialect=aurora-mysql&wrapper_plugins=failover",
    )


async def run_workload(engine, iterations: int = 20) -> None:
    for i in range(iterations):
        try:
            async with engine.connect() as conn:
                row = await conn.execute(text("SELECT @@aurora_server_id"))
                instance_id = row.scalar_one()
                print(f"iter {i}: connected to instance {instance_id}")
        except OperationalError as exc:
            print(
                f"iter {i}: operational error "
                f"({type(exc.orig).__name__}); retrying"
            )


async def main() -> None:
    engine = build_engine()
    try:
        await run_workload(engine)
    finally:
        await engine.dispose()
        await release_resources_async()


if __name__ == "__main__":
    asyncio.run(main())
