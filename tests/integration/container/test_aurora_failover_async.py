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

from __future__ import annotations

import asyncio
import gc
from time import sleep
from typing import TYPE_CHECKING, List

import pytest

from aws_advanced_python_wrapper.errors import (
    FailoverSuccessError, TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.async_connection_helpers import (
    cleanup_async, connect_async, query_host_role_async,
    query_instance_id_async)
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_features, enable_on_num_instances)
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.proxy_helper import ProxyHelper

if TYPE_CHECKING:
    from .utils.test_instance_info import TestInstanceInfo
    from .utils.test_driver import TestDriver
    from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection

from aws_advanced_python_wrapper.utils.log import Logger
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures
from .utils.test_timings import FAILOVER_WITHIN_TRANSACTION_PYTEST_TIMEOUT_SEC

logger = Logger(__name__)


@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestAuroraFailoverAsync:
    IDLE_CONNECTIONS_NUM: int = 5
    logger = Logger(__name__)

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.logger.info(f"Starting test: {request.node.name}")
        yield
        asyncio.run(cleanup_async())
        self.logger.info(f"Ending test: {request.node.name}")
        asyncio.run(cleanup_async())
        gc.collect()

    @pytest.fixture(scope='class')
    def aurora_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        p: Properties = Properties({
            "socket_timeout": 10,
            "connect_timeout": 10,
            "monitoring-connect_timeout": 5,
            "monitoring-socket_timeout": 5,
            "autocommit": True,
            "cluster_id": "cluster1"
        })

        # Parity with the sync test_aurora_failover.py props fixture: gdb_failover
        # needs a home region, and these tests assert reconnection to the new
        # WRITER, so pin gdb to strict-writer. Without this, gdb_failover defaults
        # to HOME_READER_OR_WRITER on the instance endpoint and routes to a reader,
        # failing the role==WRITER assertion. (failover_v2 ignores these props.)
        WrapperProperties.FAILOVER_HOME_REGION.set(p, TestEnvironment.get_current().get_aurora_region())
        WrapperProperties.ACTIVE_HOME_FAILOVER_MODE.set(p, "strict-writer")
        WrapperProperties.INACTIVE_HOME_FAILOVER_MODE.set(p, "strict-writer")

        features = TestEnvironment.get_current().get_features()
        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in features \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in features:
            WrapperProperties.ENABLE_TELEMETRY.set(p, True)
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, True)
        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in features:
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")
        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in features:
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    @pytest.fixture(scope='class')
    def proxied_props(self, props, conn_utils):
        props_copy = props.copy()
        endpoint_suffix = TestEnvironment.get_current().get_proxy_database_info().get_instance_endpoint_suffix()
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(props_copy, f"?.{endpoint_suffix}:{conn_utils.proxy_port}")
        return props_copy

    @pytest.mark.parametrize("plugins", ["failover_v2", "gdb_failover"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_to_new_writer_fail_on_connection_invocation_async(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        async def inner() -> None:
            initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

            props_copy = dict(props)
            props_copy["plugins"] = plugins
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **props_copy)
            try:
                # crash instance1 and nominate a new writer
                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                # failure occurs on Connection invocation
                with pytest.raises(FailoverSuccessError):
                    await query_instance_id_async(conn, aurora_utility)

                # assert that we are connected to the new writer after failover happens.
                current_connection_id = await query_instance_id_async(conn, aurora_utility)
                # Verify writer-status via the data plane (the connection's
                # own ``pg_is_in_recovery()`` / ``@@innodb_read_only``) rather
                # than the control-plane ``DescribeDBClusters.IsClusterWriter``,
                # which can lag by tens of seconds to minutes post-failover.
                assert await query_host_role_async(conn) == HostRole.WRITER
                assert current_connection_id != initial_writer_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover_v2", "gdb_failover"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_to_new_writer_fail_on_connection_bound_object_invocation_async(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        async def inner() -> None:
            initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

            props_copy = dict(props)
            props_copy["plugins"] = plugins
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **props_copy)
            try:
                # crash instance1 and nominate a new writer
                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                # failure occurs on Cursor invocation
                with pytest.raises(FailoverSuccessError):
                    await query_instance_id_async(conn, aurora_utility)

                # assert that we are connected to the new writer after failover happens and we can reuse the cursor
                current_connection_id = await query_instance_id_async(conn, aurora_utility)
                # Verify writer-status via the data plane (the connection's
                # own ``pg_is_in_recovery()`` / ``@@innodb_read_only``) rather
                # than the control-plane ``DescribeDBClusters.IsClusterWriter``,
                # which can lag by tens of seconds to minutes post-failover.
                assert await query_host_role_async(conn) == HostRole.WRITER
                assert current_connection_id != initial_writer_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    # Async ships a single failover plugin and a single host-monitoring
    # plugin, both with v2 semantics; the ``failover``/``host_monitoring``
    # registry codes are aliases for those same implementations (see
    # aio/plugin_factory.py PLUGIN_FACTORIES). The async test matrix therefore
    # triggers tests with the v2 codes only -- the v1 aliases would re-run
    # identical plugins. (Sync keeps distinct v1/v2 implementations and
    # parametrizes both, except where v1 was dropped for a libpq teardown
    # race -- commit e713459.) ``gdb_failover`` is a *distinct* (region-aware)
    # plugin, not an alias, so it is exercised alongside ``failover_v2`` --
    # mirroring sync ``test_aurora_failover.py`` (#1246).
    @pytest.mark.parametrize("plugins", ["failover_v2,host_monitoring_v2",
                                         "gdb_failover,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    def test_fail_from_reader_to_writer_async(
            self,
            test_environment: TestEnvironment,
            test_driver: TestDriver,
            conn_utils,
            proxied_props,
            aurora_utility,
            plugins):
        async def inner() -> None:
            reader: TestInstanceInfo = test_environment.get_proxy_instances()[1]
            writer_id: str = test_environment.get_proxy_writer().get_instance_id()

            proxied_props_copy = dict(proxied_props)
            proxied_props_copy["plugins"] = plugins
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_proxy_connect_params(reader.get_host()),
                **proxied_props_copy)
            try:
                ProxyHelper.disable_connectivity(reader.get_instance_id())
                with pytest.raises(FailoverSuccessError):
                    await query_instance_id_async(conn, aurora_utility)
                current_connection_id = await query_instance_id_async(conn, aurora_utility)

                assert writer_id == current_connection_id
                # Verify writer-status via the data plane (the connection's
                # own ``pg_is_in_recovery()`` / ``@@innodb_read_only``) rather
                # than the control-plane ``DescribeDBClusters.IsClusterWriter``,
                # which can lag by tens of seconds to minutes post-failover.
                assert await query_host_role_async(conn) == HostRole.WRITER
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover_v2", "gdb_failover"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_with_session_states_autocommit_async(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        async def inner() -> None:
            initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

            props_copy = dict(props)
            props_copy["plugins"] = plugins
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **props_copy)
            try:
                await conn.set_autocommit(False)

                async with conn.cursor() as cursor_1:
                    await cursor_1.execute("DROP TABLE IF EXISTS session_states")
                    await cursor_1.execute(
                        "CREATE TABLE session_states "
                        "(id int not null primary key, session_states_field varchar(255) not null)")
                    await conn.commit()

                async with conn.cursor() as cursor_2:
                    await cursor_2.execute("INSERT INTO session_states VALUES (1, 'test field string 1')")

                    aurora_utility.failover_cluster_and_wait_until_writer_changed()

                    with pytest.raises(TransactionResolutionUnknownError):
                        await cursor_2.execute("INSERT INTO session_states VALUES (2, 'test field string 2')")

                # Attempt to query the instance id.
                current_connection_id = await query_instance_id_async(conn, aurora_utility)
                # Assert that we are connected to the new writer after failover happens.
                # Verify writer-status via the data plane (the connection's
                # own ``pg_is_in_recovery()`` / ``@@innodb_read_only``) rather
                # than the control-plane ``DescribeDBClusters.IsClusterWriter``,
                # which can lag by tens of seconds to minutes post-failover.
                assert await query_host_role_async(conn) == HostRole.WRITER
                next_cluster_writer_id = aurora_utility.get_cluster_writer_instance_id()
                assert current_connection_id == next_cluster_writer_id
                assert current_connection_id != initial_writer_id

                async with conn.cursor() as cursor_3:
                    await cursor_3.execute("SELECT count(*) from session_states")
                    result = await cursor_3.fetchone()
                    assert 0 == int(result[0])
                    await cursor_3.execute("DROP TABLE IF EXISTS session_states")
                    await conn.commit()
                    # Assert autocommit is still False after failover.
                    assert conn.autocommit is False
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover_v2", "gdb_failover"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_with_session_states_readonly_async(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        async def inner() -> None:
            initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

            props_copy = dict(props)
            props_copy["plugins"] = plugins
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **props_copy)
            try:
                assert conn.read_only is False
                await conn.set_read_only(True)
                assert conn.read_only is True

                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                with pytest.raises(FailoverSuccessError):
                    await query_instance_id_async(conn, aurora_utility)

                # Attempt to query the instance id.
                current_connection_id = await query_instance_id_async(conn, aurora_utility)
                # Assert that we are connected to the new writer after failover happens.
                # Verify writer-status via the data plane (the connection's
                # own ``pg_is_in_recovery()`` / ``@@innodb_read_only``) rather
                # than the control-plane ``DescribeDBClusters.IsClusterWriter``,
                # which can lag by tens of seconds to minutes post-failover.
                assert await query_host_role_async(conn) == HostRole.WRITER
                next_cluster_writer_id = aurora_utility.get_cluster_writer_instance_id()
                assert current_connection_id == next_cluster_writer_id
                assert current_connection_id != initial_writer_id

                # Assert readonly is still True after failover.
                assert conn.read_only is True
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover_v2", "gdb_failover"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_writer_fail_within_transaction_set_autocommit_false_async(
            self, test_driver: TestDriver, test_environment: TestEnvironment, props, conn_utils, aurora_utility,
            plugins):
        async def inner() -> None:
            initial_writer_id = test_environment.get_writer().get_instance_id()

            props_copy = dict(props)
            props_copy["plugins"] = plugins
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **props_copy)
            try:
                async with conn.cursor() as cursor_1:
                    await cursor_1.execute("DROP TABLE IF EXISTS test3_2")
                    await cursor_1.execute(
                        "CREATE TABLE test3_2 (id int not null primary key, test3_2_field varchar(255) not null)")
                    await conn.commit()

                await conn.set_autocommit(False)

                async with conn.cursor() as cursor_2:
                    await cursor_2.execute("INSERT INTO test3_2 VALUES (1, 'test field string 1')")

                    aurora_utility.failover_cluster_and_wait_until_writer_changed()

                    with pytest.raises(TransactionResolutionUnknownError):
                        await cursor_2.execute("INSERT INTO test3_2 VALUES (2, 'test field string 2')")

                # attempt to query the instance id
                current_connection_id: str = await query_instance_id_async(conn, aurora_utility)

                # assert that we are connected to the new writer after failover happens
                # Data-plane writer check; see earlier sites for rationale.
                assert await query_host_role_async(conn) == HostRole.WRITER
                next_cluster_writer_id: str = aurora_utility.get_cluster_writer_instance_id()

                assert current_connection_id == next_cluster_writer_id
                assert initial_writer_id != next_cluster_writer_id

                # cursor_2 can not be used anymore since it's invalid

                async with conn.cursor() as cursor_3:
                    await cursor_3.execute("SELECT count(*) from test3_2")
                    result = await cursor_3.fetchone()
                    assert 0 == int(result[0])
                    await cursor_3.execute("DROP TABLE IF EXISTS test3_2")
                    await conn.commit()
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover_v2", "gdb_failover"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    @pytest.mark.timeout(FAILOVER_WITHIN_TRANSACTION_PYTEST_TIMEOUT_SEC)
    def test_writer_fail_within_transaction_start_transaction_async(
            self, test_driver: TestDriver, test_environment: TestEnvironment, props, conn_utils, aurora_utility,
            plugins):
        async def inner() -> None:
            initial_writer_id = test_environment.get_writer().get_instance_id()

            props_copy = dict(props)
            props_copy["plugins"] = plugins
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **props_copy)
            try:
                async with conn.cursor() as cursor_1:
                    await cursor_1.execute("DROP TABLE IF EXISTS test3_3")
                    await cursor_1.execute(
                        "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)")
                    await conn.commit()

                    await cursor_1.execute("START TRANSACTION")

                async with conn.cursor() as cursor_2:
                    await cursor_2.execute("INSERT INTO test3_3 VALUES (1, 'test field string 1')")

                    aurora_utility.failover_cluster_and_wait_until_writer_changed()

                    with pytest.raises(TransactionResolutionUnknownError):
                        await cursor_2.execute("INSERT INTO test3_3 VALUES (2, 'test field string 2')")

                # attempt to query the instance id
                current_connection_id: str = await query_instance_id_async(conn, aurora_utility)

                # assert that we are connected to the new writer after failover happens
                # Data-plane writer check; see earlier sites for rationale.
                assert await query_host_role_async(conn) == HostRole.WRITER
                next_cluster_writer_id: str = aurora_utility.get_cluster_writer_instance_id()

                assert current_connection_id == next_cluster_writer_id
                assert initial_writer_id != next_cluster_writer_id

                # cursor_2 can not be used anymore since it's invalid

                async with conn.cursor() as cursor_3:
                    await cursor_3.execute("SELECT count(*) from test3_3")
                    result = await cursor_3.fetchone()
                    assert 0 == int(result[0])
                    await cursor_3.execute("DROP TABLE IF EXISTS test3_3")
                    await conn.commit()
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["aurora_connection_tracker,failover_v2",
                                         "aurora_connection_tracker,gdb_failover"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    @pytest.mark.repeat(5)  # Run this test case a few more times since it is a flakey test
    def test_writer_failover_in_idle_connections_async(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        # idle_connections is populated inside inner() and read after asyncio.run();
        # use a mutable container so the outer scope can access it.
        idle_connections: List[AsyncAwsWrapperConnection] = []

        async def inner() -> None:
            current_writer_id = aurora_utility.get_cluster_writer_instance_id()

            props_copy = dict(props)
            props_copy["plugins"] = plugins

            for _i in range(self.IDLE_CONNECTIONS_NUM):
                idle_connections.append(
                    await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_utils.get_connect_params(),
                        **props_copy))

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(),
                **props_copy)
            try:
                instance_id = await query_instance_id_async(conn, aurora_utility)
                assert current_writer_id == instance_id

                # ensure that all idle connections are still opened
                for idle_connection in idle_connections:
                    assert idle_connection.is_closed is False

                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                with pytest.raises(FailoverSuccessError):
                    await query_instance_id_async(conn, aurora_utility)
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

        sleep(10)

        # Ensure that all idle connections are closed.
        # is_closed is proxied through __getattr__ to the underlying driver connection.
        for idle_connection in idle_connections:
            assert idle_connection.is_closed is True
