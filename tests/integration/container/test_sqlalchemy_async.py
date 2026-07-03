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

"""Async SQLAlchemy integration tests for Aurora PG and Aurora MySQL.

Async twin of test_sqlalchemy.py.  Uses the wrapper's registered URL schemes
(postgresql+aws_wrapper_psycopg, mysql+aws_wrapper_aiomysql) via
``create_async_engine_for_driver`` rather than the sync creator-callable
pattern.

Proves:
- AsyncEngine / AsyncConnection lifecycle against real Aurora clusters.
- Aurora failover surfaces as sqlalchemy.exc.OperationalError and a retry recovers.
- Read/Write Splitting's read_only flip routes to a reader and returns to a writer.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from aws_advanced_python_wrapper.utils.log import Logger
from tests.integration.container.utils.async_connection_helpers import (
    cleanup_async, create_async_engine_for_driver)
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_num_instances)
from .utils.database_engine import DatabaseEngine
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_driver import TestDriver
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from .utils.connection_utils import ConnectionUtils

logger = Logger(__name__)


def _is_mysql_async(test_driver: TestDriver) -> bool:
    return test_driver == TestDriver.MYSQL_ASYNC


def _wrapper_dialect_async(test_driver: TestDriver) -> str:
    return "aurora-mysql" if _is_mysql_async(test_driver) else "aurora-pg"


def _instance_id_sql_async(test_driver: TestDriver) -> str:
    if _is_mysql_async(test_driver):
        return "SELECT @@aurora_server_id"
    return "SELECT pg_catalog.aurora_db_instance_identifier()"


def _readonly_option_async(test_driver: TestDriver) -> dict:
    return {"mysql_readonly": True} if _is_mysql_async(test_driver) else {"postgresql_readonly": True}


@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestSqlAlchemyAsync:

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        logger.info(f"Starting test: {request.node.name}")
        yield
        logger.info(f"Ending test: {request.node.name}")

    @pytest.fixture(scope="class")
    def aurora_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    def test_sqlalchemy_creator_survives_aurora_failover_async(
            self, test_driver: TestDriver, conn_utils: ConnectionUtils, aurora_utility):
        """AsyncEngine recovers from Aurora failover via the OperationalError retry path."""
        engine_kind = TestEnvironment.get_current().get_engine()
        if engine_kind not in (DatabaseEngine.PG, DatabaseEngine.MYSQL):
            pytest.skip(f"Unsupported engine: {engine_kind}")

        async def inner() -> None:
            initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

            engine = create_async_engine_for_driver(
                test_driver,
                user=conn_utils.user,
                password=conn_utils.password,
                host=conn_utils.writer_host,
                port=conn_utils.port,
                dbname=conn_utils.dbname,
                wrapper_dialect=_wrapper_dialect_async(test_driver),
                # Modernize to ``failover_v2`` (the async registry aliases
                # both names to the same factory; ``failover_v2`` matches
                # main's DEFAULT_PLUGINS and the sync test conventions in
                # this file). host_monitoring_v2 works on both engines here
                # because the async wrapper uses asyncio.Task-based
                # cooperative cancellation rather than thread-based abort
                # (aws_advanced_python_wrapper/aio/host_monitoring_plugin.py).
                wrapper_plugins="failover_v2,host_monitoring_v2",
            )
            try:
                async with engine.connect() as conn:
                    pre_failover_id = (await conn.execute(text(_instance_id_sql_async(test_driver)))).scalar_one()
                    assert pre_failover_id == initial_writer_id

                # Trigger failover against the cluster.
                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                # First query after failover should raise sqlalchemy.exc.OperationalError
                # because FailoverSuccessError is reclassified as OperationalError.
                recovered = False
                attempts_remaining = 10
                # Verify the new connection lands on the actual writer via
                # the data plane (``pg_is_in_recovery()`` / ``@@innodb_read_only``)
                # rather than the control-plane ``DescribeDBClusters.IsClusterWriter``
                # which can lag by tens of seconds to minutes post-failover.
                engine_type = TestEnvironment.get_current().get_engine()
                is_reader_sql = (
                    "SELECT pg_catalog.pg_is_in_recovery()"
                    if engine_type == DatabaseEngine.PG
                    else "SELECT @@innodb_read_only"
                )
                while attempts_remaining > 0 and not recovered:
                    try:
                        async with engine.connect() as conn:
                            new_writer_id = (await conn.execute(
                                text(_instance_id_sql_async(test_driver))
                            )).scalar_one()
                            assert new_writer_id != initial_writer_id
                            is_reader = (await conn.execute(
                                text(is_reader_sql)
                            )).scalar_one()
                            assert is_reader in (0, False)
                            recovered = True
                    except OperationalError:
                        attempts_remaining -= 1

                assert recovered, "SA did not recover from failover within 10 attempts"
            finally:
                await engine.dispose()
                await cleanup_async()

        asyncio.run(inner())

    def test_sqlalchemy_creator_read_write_splitting_async(
            self, test_driver: TestDriver, conn_utils: ConnectionUtils, aurora_utility):
        """R/W splitting routes read_only connections to a reader and returns to writer."""
        engine_kind = TestEnvironment.get_current().get_engine()
        if engine_kind not in (DatabaseEngine.PG, DatabaseEngine.MYSQL):
            pytest.skip(f"Unsupported engine: {engine_kind}")

        async def inner() -> None:
            engine = create_async_engine_for_driver(
                test_driver,
                user=conn_utils.user,
                password=conn_utils.password,
                host=conn_utils.writer_host,
                port=conn_utils.port,
                dbname=conn_utils.dbname,
                wrapper_dialect=_wrapper_dialect_async(test_driver),
                # Pair ``read_write_splitting`` with ``failover_v2`` so the
                # cluster topology monitor starts on initial connect; without
                # it, the host list stays at {writer-only} and the read-only
                # flip falls back to the writer (see the sync test of the
                # same name for the detailed root-cause analysis). Async
                # MySQL can include ``host_monitoring_v2`` because the async
                # plugin uses asyncio.Task cancellation, not thread abort.
                wrapper_plugins="read_write_splitting,failover_v2,host_monitoring_v2",
            )
            try:
                async with engine.connect() as conn:
                    writer_id = (await conn.execute(text(_instance_id_sql_async(test_driver)))).scalar_one()
                    await conn.commit()

                async with engine.connect() as conn:
                    conn = await conn.execution_options(**_readonly_option_async(test_driver))
                    reader_id = (await conn.execute(text(_instance_id_sql_async(test_driver)))).scalar_one()
                    await conn.commit()

                assert reader_id != writer_id, (
                    f"read-only connection should route to a reader, got {reader_id}"
                )

                # Next non-read-only connection should return to the writer.
                async with engine.connect() as conn:
                    back_to_writer = (await conn.execute(text(_instance_id_sql_async(test_driver)))).scalar_one()
                    await conn.commit()
                assert back_to_writer == writer_id
            finally:
                await engine.dispose()
                await cleanup_async()

        asyncio.run(inner())
