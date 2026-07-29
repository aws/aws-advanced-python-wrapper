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

"""Async SQLAlchemy + AWS Advanced Python Wrapper: failover on Aurora PostgreSQL.

URL-based async engine usage (SP-9). The `postgresql+aws_wrapper_psycopg` URL is
shared with the sync engine -- `create_async_engine` selects the async dialect via
the sync dialect's `get_async_dialect_cls` hook -- routing through
`AsyncAwsWrapperConnection`, so all wrapper plugins (failover, host_monitoring_v2,
etc.) are available in async apps.

The wrapper's `plugins` connection property is spelled `wrapper_plugins` in the
URL query string because SA reserves `plugins=` for its own engine-plugin loader.
The dialect renames the alias before the DBAPI call.
"""

import asyncio

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from aws_advanced_python_wrapper import release_resources

CLUSTER_ENDPOINT = "database.cluster-xyz.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
USER = "john"
PASSWORD = "pwd"


def build_engine():
    return create_async_engine(
        f"postgresql+aws_wrapper_psycopg://{USER}:{PASSWORD}@"
        f"{CLUSTER_ENDPOINT}:5432/{DB_NAME}"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,host_monitoring_v2",
    )


async def run_workload(engine, iterations: int = 20) -> None:
    for i in range(iterations):
        try:
            async with engine.connect() as conn:
                row = await conn.execute(
                    text("SELECT pg_catalog.aurora_db_instance_identifier()")
                )
                instance_id = row.scalar_one()
                print(f"iter {i}: connected to instance {instance_id}")
        except OperationalError as exc:
            # FailoverSuccessError is classified as OperationalError by the wrapper;
            # SA wraps it here. Retrying the transaction reconnects through the
            # new writer.
            print(f"iter {i}: operational error ({type(exc.orig).__name__}); retrying")


async def main() -> None:
    engine = build_engine()
    try:
        await run_workload(engine)
    finally:
        await engine.dispose()
        release_resources()


if __name__ == "__main__":
    asyncio.run(main())
