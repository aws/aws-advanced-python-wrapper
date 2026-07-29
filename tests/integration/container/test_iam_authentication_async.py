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

"""Async twin of test_iam_authentication.py.

Exercises the iam_authentication plugin on AsyncAwsWrapperConnection:
token generation, IAM-authenticated connection open, and failure paths
with wrong username / missing username / invalid iam_host.

Translation notes vs the sync file:
- ``AwsWrapperConnection.connect(...)`` → ``await connect_async(test_driver=..., connect_params=..., **props)``
- ``validate_connection`` (sync context manager) → ``_validate_connection_async`` (async coroutine)
- ``failover_with_iam`` uses ``await _query_instance_id_async`` for cursor calls
- ``aurora_utility.assert_first_query_throws`` (sync) is replaced by an inline
  ``_assert_first_query_throws_async`` that awaits cursor operations
- ``params.pop("use_pure", None)`` is carried over verbatim — aiomysql does not
  use ``use_pure``; the pop is a harmless no-op for MySQL async and a safety net
  if the dict ever gains that key from ``get_connect_params``.
"""

from __future__ import annotations

import asyncio
from socket import gethostbyname
from typing import TYPE_CHECKING, Any, Dict

import pytest

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                FailoverSuccessError)
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.async_connection_helpers import (
    assert_first_query_throws_async, cleanup_async, connect_async,
    query_host_role_async, query_instance_id_async)
from tests.integration.container.utils.conditions import (
    disable_on_features, enable_on_deployments, enable_on_features,
    enable_on_num_instances)
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection
    from tests.integration.container.utils.test_driver import TestDriver
    from tests.integration.container.utils.test_instance_info import \
        TestInstanceInfo


# ---------------------------------------------------------------------------
# Module-level async helpers
# ---------------------------------------------------------------------------

async def _validate_connection_async(conn: AsyncAwsWrapperConnection) -> None:
    """Run a trivial query to confirm the connection is live."""
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT now()")
        records = await cursor.fetchall()
        assert len(records) == 1


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

@enable_on_features([TestEnvironmentFeatures.IAM])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestAwsIamAuthenticationAsync:

    @pytest.fixture(scope='class')
    def props(self):
        p: Properties = Properties()

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    def test_iam_wrong_database_username_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        async def inner() -> None:
            try:
                user = f"WRONG_{conn_utils.iam_user}_USER"
                params: Dict[str, Any] = conn_utils.get_connect_params(user=user)
                params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver

                with pytest.raises(AwsWrapperError):
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=params,
                        plugins="iam",
                        **dict(props))
                    await conn.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    def test_iam_no_database_username_async(self, test_driver: TestDriver, conn_utils, props):
        async def inner() -> None:
            try:
                params: Dict[str, Any] = conn_utils.get_connect_params()
                params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
                params.pop("user", None)

                with pytest.raises(AwsWrapperError):
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=params,
                        plugins="iam",
                        **dict(props))
                    await conn.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    def test_iam_invalid_host_async(self, test_driver: TestDriver, conn_utils, props):
        async def inner() -> None:
            try:
                params: Dict[str, Any] = conn_utils.get_connect_params()
                params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
                params["iam_host"] = "<>"

                with pytest.raises(AwsWrapperError):
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=params,
                        plugins="iam",
                        **dict(props))
                    await conn.close()
            finally:
                await cleanup_async()

        asyncio.run(inner())

    def test_iam_using_ip_address_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        async def inner() -> None:
            instance: TestInstanceInfo = test_environment.get_writer()
            ip_address = gethostbyname(instance.get_host())

            params: Dict[str, Any] = conn_utils.get_connect_params(
                host=ip_address, user=conn_utils.iam_user, password="<anything>")
            params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
            params["iam_host"] = instance.get_host()

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=params,
                plugins="iam",
                **dict(props))
            try:
                await _validate_connection_async(conn)
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_iam_valid_connection_properties_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        async def inner() -> None:
            params: Dict[str, Any] = conn_utils.get_connect_params(
                user=conn_utils.iam_user, password="<anything>")
            params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=params,
                plugins="iam",
                **dict(props))
            try:
                await _validate_connection_async(conn)
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_iam_valid_connection_properties_no_password_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        async def inner() -> None:
            params: Dict[str, Any] = conn_utils.get_connect_params(user=conn_utils.iam_user)
            params.pop("use_pure", None)  # AWS tokens are truncated when using the pure Python MySQL driver
            params.pop("password", None)

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=params,
                plugins="iam",
                **dict(props))
            try:
                await _validate_connection_async(conn)
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover_v2,iam"])
    @enable_on_num_instances(min_instances=2)
    @enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
    @disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                          TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                          TestEnvironmentFeatures.PERFORMANCE])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED, TestEnvironmentFeatures.IAM])
    def test_failover_with_iam_async(
            self, test_driver: TestDriver, props, conn_utils, plugins):
        async def inner() -> None:
            region = TestEnvironment.get_current().get_info().get_region()
            aurora_utility = RdsTestUtility(region)
            initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

            props_copy = dict(props)
            props_copy.update({
                "plugins": plugins,
                "socket_timeout": 10,
                "connect_timeout": 10,
                "monitoring-connect_timeout": 5,
                "monitoring-socket_timeout": 5,
                "topology_refresh_ms": 10,
                "autocommit": True,
            })

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(user=conn_utils.iam_user),
                **props_copy)
            try:
                # crash instance1 and nominate a new writer
                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                # failure occurs on Cursor invocation
                await assert_first_query_throws_async(conn, aurora_utility, FailoverSuccessError)

                # assert that we are connected to the new writer after failover happens
                current_connection_id = await query_instance_id_async(conn, aurora_utility)
                # Verify via the connection's data plane (pg_is_in_recovery /
                # @@innodb_read_only); the control-plane RDS API can lag.
                assert await query_host_role_async(conn) == HostRole.WRITER
                assert current_connection_id != initial_writer_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())
