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
from typing import TYPE_CHECKING

import pytest

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.async_connection_helpers import (
    cleanup_async, connect_async)
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_features, enable_on_num_instances)
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.proxy_helper import ProxyHelper
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from .utils.connection_utils import ConnectionUtils
    from .utils.test_driver import TestDriver
    from .utils.test_instance_info import TestInstanceInfo


@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE,
                      TestEnvironmentFeatures.SKIP_ASYNC_DRIVER_TESTS])
class TestBasicConnectivityAsync:

    @pytest.fixture(scope='class')
    def props(self):
        # By default, don't load the host_monitoring plugin so that the test doesn't require abort connection support
        p: Properties = Properties({
            WrapperProperties.PLUGINS.name: "aurora_connection_tracker,failover_v2",
            "connect_timeout": 3,
            "autocommit": True,
            "cluster_id": "cluster1"})

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    def test_direct_connection_async(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils: ConnectionUtils):
        async def inner() -> None:
            conn = await connect_async(test_driver=test_driver, connect_params=conn_utils.get_connect_params())
            try:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    result = await cur.fetchone()
                    assert 1 == result[0]
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_wrapper_connection_async(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils: ConnectionUtils, props):
        async def inner() -> None:
            conn = await connect_async(test_driver=test_driver, connect_params=conn_utils.get_connect_params(), **dict(props))
            try:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    result = await cur.fetchone()
                    assert 1 == result[0]
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_direct_connection_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils: ConnectionUtils):
        async def inner() -> None:
            conn = await connect_async(test_driver=test_driver, connect_params=conn_utils.get_proxy_connect_params())
            try:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    result = await cur.fetchone()
                    assert 1 == result[0]
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils: ConnectionUtils, props):
        async def inner() -> None:
            conn = await connect_async(test_driver=test_driver, connect_params=conn_utils.get_proxy_connect_params(), **dict(props))
            try:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    result = await cur.fetchone()
                    assert 1 == result[0]
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection_failed_async(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils: ConnectionUtils, props):
        async def inner() -> None:
            instance: TestInstanceInfo = test_environment.get_proxy_writer()

            ProxyHelper.disable_connectivity(instance.get_instance_id())

            try:
                await connect_async(
                    test_driver=test_driver,
                    connect_params=conn_utils.get_proxy_connect_params(),
                    **dict(props))

                # Should not be here since proxy is blocking db connectivity
                assert False

            except Exception:
                # That is expected exception. Test pass.
                assert True

            finally:
                await cleanup_async()

        asyncio.run(inner())

    @pytest.mark.parametrize("plugins", ["failover_v2,host_monitoring_v2"])
    @enable_on_num_instances(min_instances=2)
    @enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
    @enable_on_features([TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    def test_wrapper_connection_reader_cluster_with_efm_enabled_async(self, test_driver: TestDriver, conn_utils: ConnectionUtils, plugins):
        async def inner() -> None:
            local_props: Properties = Properties({
                WrapperProperties.PLUGINS.name: plugins,
                "socket_timeout": 5,
                "connect_timeout": 5,
                "monitoring-connect_timeout": 3,
                "monitoring-socket_timeout": 3,
                "autocommit": True})
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=conn_utils.get_connect_params(conn_utils.reader_cluster_host),
                **dict(local_props))
            try:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    result = await cur.fetchone()
                    assert 1 == result[0]
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())
