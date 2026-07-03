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
from datetime import datetime, timedelta
from time import sleep
from typing import TYPE_CHECKING, List

import pytest

from tests.integration.container.utils.async_connection_helpers import (
    assert_first_query_throws_async, cleanup_async, connect_async,
    query_instance_id_async)

if TYPE_CHECKING:
    from tests.integration.container.utils.connection_utils import ConnectionUtils
    from tests.integration.container.utils.test_driver import TestDriver
    from tests.integration.container.utils.test_instance_info import TestInstanceInfo

from aws_advanced_python_wrapper.aio.connection_provider import \
    AsyncConnectionProviderManager
from aws_advanced_python_wrapper.aio.pooled_connection_provider import \
    AsyncPooledConnectionProvider
from aws_advanced_python_wrapper.errors import FailoverSuccessError
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.conditions import (
    enable_on_features, enable_on_num_instances)
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

@enable_on_num_instances(min_instances=5)
@enable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY])
class TestAutoScalingAsync:
    @pytest.fixture
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture
    def props(self):
        p: Properties = Properties({"plugins": "read_write_splitting", "connect_timeout": 10, "autocommit": True})

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    @pytest.fixture
    def failover_props(self):
        p = {"plugins": "read_write_splitting,failover_v2", "connect_timeout": 10, "autocommit": True, "cluster_id": "cluster1"}

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    @staticmethod
    def is_url_in_pool(desired_url: str, pool: List[str]) -> bool:
        for url in pool:
            if url == desired_url:
                return True

        return False

    def test_pooled_connection_auto_scaling__set_read_only_on_old_connection_async(
            self, test_driver: TestDriver, props, conn_utils: ConnectionUtils, rds_utils):
        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        instances: List[TestInstanceInfo] = TestEnvironment.get_current().get_info().get_database_info().get_instances()
        original_cluster_size = len(instances)

        provider = AsyncPooledConnectionProvider(
            lambda _, __: {"pool_size": original_cluster_size},
            None,
            None,
            120000000000,  # 2 minutes
            180000000000)  # 3 minutes
        AsyncConnectionProviderManager.set_connection_provider(provider)

        async def inner():
            connections: List[AsyncAwsWrapperConnection] = []
            try:
                for i in range(0, original_cluster_size):
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_utils.get_connect_params(instances[i].get_host()),
                        **dict(props))
                    connections.append(conn)

                new_instance: TestInstanceInfo = rds_utils.create_db_instance("auto-scaling-instance")
                new_instance_conn: AsyncAwsWrapperConnection
                try:
                    new_instance_conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_utils.get_connect_params(),
                        **dict(props))
                    connections.append(new_instance_conn)

                    await asyncio.sleep(5)

                    writer_id = await query_instance_id_async(new_instance_conn, rds_utils)

                    await new_instance_conn.set_read_only(True)
                    reader_id = await query_instance_id_async(new_instance_conn, rds_utils)

                    assert new_instance.get_instance_id() == reader_id
                    assert writer_id != reader_id
                    assert TestAutoScalingAsync.is_url_in_pool(new_instance.get_url(), provider.pool_urls)

                    await new_instance_conn.set_read_only(False)
                finally:
                    rds_utils.delete_db_instance(new_instance.get_instance_id())

                stop_time = datetime.now() + timedelta(minutes=5)
                while datetime.now() <= stop_time and len(rds_utils.get_instance_ids()) != original_cluster_size:
                    sleep(5)

                if len(rds_utils.get_instance_ids()) != original_cluster_size:
                    pytest.fail("The deleted instance is still in the cluster topology")

                await new_instance_conn.set_read_only(True)

                instance_id = await query_instance_id_async(new_instance_conn, rds_utils)
                assert writer_id != instance_id
                assert new_instance.get_instance_id() != instance_id

                assert not TestAutoScalingAsync.is_url_in_pool(new_instance.get_url(), provider.pool_urls)
                assert len(instances) == len(provider.pool_urls)
            finally:
                for conn in connections:
                    await conn.close()

                await AsyncConnectionProviderManager.release_resources()
                AsyncConnectionProviderManager.reset_provider()
                await cleanup_async()

        asyncio.run(inner())

    def test_pooled_connection_auto_scaling__failover_from_deleted_reader_async(
            self, test_driver: TestDriver, failover_props, conn_utils: ConnectionUtils, rds_utils):
        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(failover_props, "least_connections")

        instances: List[TestInstanceInfo] = TestEnvironment.get_current().get_info().get_database_info().get_instances()

        provider = AsyncPooledConnectionProvider(
            lambda _, __: {"pool_size": len(instances) * 5},
            None,
            None,
            120000000000,  # 2 minutes
            180000000000)  # 3 minutes
        AsyncConnectionProviderManager.set_connection_provider(provider)

        async def inner():
            connections: List[AsyncAwsWrapperConnection] = []
            try:
                for i in range(1, len(instances)):
                    conn_str = conn_utils.get_connect_params(instances[i].get_host())

                    # Create 2 connections per instance
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_str,
                        **dict(failover_props))
                    connections.append(conn)
                    conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_str,
                        **dict(failover_props))
                    connections.append(conn)

                new_instance: TestInstanceInfo = rds_utils.create_db_instance("auto-scaling-instance")
                new_instance_conn: AsyncAwsWrapperConnection
                try:
                    new_instance_conn = await connect_async(
                        test_driver=test_driver,
                        connect_params=conn_utils.get_connect_params(instances[0].get_host()),
                        **dict(failover_props))
                    connections.append(new_instance_conn)

                    await new_instance_conn.set_read_only(True)
                    reader_id = await query_instance_id_async(new_instance_conn, rds_utils)

                    assert new_instance.get_instance_id() == reader_id
                    assert TestAutoScalingAsync.is_url_in_pool(new_instance.get_url(), provider.pool_urls)
                finally:
                    rds_utils.delete_db_instance(new_instance.get_instance_id())

                await assert_first_query_throws_async(new_instance_conn, rds_utils, FailoverSuccessError)

                new_reader_id = await query_instance_id_async(new_instance_conn, rds_utils)
                assert new_instance.get_instance_id() != new_reader_id
            finally:
                for conn in connections:
                    await conn.close()

                await AsyncConnectionProviderManager.release_resources()
                AsyncConnectionProviderManager.reset_provider()
                await cleanup_async()

        asyncio.run(inner())
