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

from datetime import datetime, timedelta
from time import sleep
from typing import TYPE_CHECKING, List

import pytest

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver
    from tests.integration.container.utils.test_instance_info import TestInstanceInfo

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.errors import FailoverSuccessError
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from tests.integration.container.utils.aurora_test_utility import \
    AuroraTestUtility
from tests.integration.container.utils.conditions import (
    enable_on_features, enable_on_num_instances)
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@enable_on_num_instances(min_instances=5)
@enable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY])
class TestAutoScaling:
    @pytest.fixture
    def aurora_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_aurora_region()
        return AuroraTestUtility(region)

    @pytest.fixture
    def props(self):
        return {"plugins": "read_write_splitting", "connect_timeout": 10, "autocommit": True}

    @pytest.fixture
    def failover_props(self):
        return {"plugins": "read_write_splitting,failover", "connect_timeout": 10, "autocommit": True}

    @staticmethod
    def is_url_in_pool(desired_url: str, pool: List[str]) -> bool:
        for url in pool:
            if url == desired_url:
                return True

        return False

    def test_pooled_connection_auto_scaling__set_read_only_on_old_connection(
            self, test_driver: TestDriver, props, conn_utils, aurora_utils):
        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        instances: List[TestInstanceInfo] = TestEnvironment.get_current().get_info().get_database_info().get_instances()
        original_cluster_size = len(instances)

        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": original_cluster_size},
            None,
            120000000000,  # 2 minutes
            180000000000)  # 3 minutes
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)

        connections: List[AwsWrapperConnection] = []
        try:
            for i in range(0, original_cluster_size):
                conn_str = conn_utils.get_conn_string(instances[i].get_host())
                conn = AwsWrapperConnection.connect(target_driver_connect, conn_str, **props)
                connections.append(conn)

            new_instance_conn: AwsWrapperConnection
            new_instance: TestInstanceInfo = aurora_utils.create_db_instance("auto-scaling-instance")
            try:
                new_instance_conn = AwsWrapperConnection.connect(
                    target_driver_connect, conn_utils.get_conn_string(), **props)
                connections.append(new_instance_conn)

                sleep(5)

                writer_id = aurora_utils.query_instance_id(new_instance_conn)

                new_instance_conn.read_only = True
                reader_id = aurora_utils.query_instance_id(new_instance_conn)

                assert new_instance.get_instance_id() == reader_id
                assert writer_id != reader_id
                assert self.is_url_in_pool(new_instance.get_url(), provider.pool_urls)

                new_instance_conn.read_only = False
            finally:
                aurora_utils.delete_db_instance(new_instance.get_instance_id())

            stop_time = datetime.now() + timedelta(minutes=5)
            while datetime.now() <= stop_time and len(aurora_utils.get_aurora_instance_ids()) != original_cluster_size:
                sleep(5)

            if len(aurora_utils.get_aurora_instance_ids()) != original_cluster_size:
                pytest.fail("The deleted instance is still in the cluster topology")

            new_instance_conn.read_only = True

            instance_id = aurora_utils.query_instance_id(new_instance_conn)
            assert writer_id != instance_id
            assert new_instance.get_instance_id() != instance_id

            assert not self.is_url_in_pool(new_instance.get_url(), provider.pool_urls)
            assert len(instances) == len(provider.pool_urls)
        finally:
            for conn in connections:
                conn.close()

            ConnectionProviderManager.release_resources()
            ConnectionProviderManager.reset_provider()

    def test_pooled_connection_auto_scaling__failover_from_deleted_reader(
            self, test_driver: TestDriver, failover_props, conn_utils, aurora_utils):
        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(failover_props, "least_connections")

        instances: List[TestInstanceInfo] = TestEnvironment.get_current().get_info().get_database_info().get_instances()

        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": len(instances) * 5},
            None,
            120000000000,  # 2 minutes
            180000000000)  # 3 minutes
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)

        connections: List[AwsWrapperConnection] = []
        try:
            for i in range(1, len(instances)):
                conn_str = conn_utils.get_conn_string(instances[i].get_host())

                # Create 2 connections per instance
                conn = AwsWrapperConnection.connect(target_driver_connect, conn_str, **failover_props)
                connections.append(conn)
                conn = AwsWrapperConnection.connect(target_driver_connect, conn_str, **failover_props)
                connections.append(conn)

            new_instance_conn: AwsWrapperConnection
            new_instance: TestInstanceInfo = aurora_utils.create_db_instance("auto-scaling-instance")
            try:
                new_instance_conn = AwsWrapperConnection.connect(
                    target_driver_connect,
                    conn_utils.get_conn_string(instances[0].get_host()),
                    **failover_props)
                connections.append(new_instance_conn)

                new_instance_conn.read_only = True
                reader_id = aurora_utils.query_instance_id(new_instance_conn)

                assert new_instance.get_instance_id() == reader_id
                assert self.is_url_in_pool(new_instance.get_url(), provider.pool_urls)
            finally:
                aurora_utils.delete_db_instance(new_instance.get_instance_id())

            aurora_utils.assert_first_query_throws(new_instance_conn, FailoverSuccessError)

            new_reader_id = aurora_utils.query_instance_id(new_instance_conn)
            assert new_instance.get_instance_id() != new_reader_id
        finally:
            for conn in connections:
                conn.close()

            ConnectionProviderManager.release_resources()
            ConnectionProviderManager.reset_provider()
