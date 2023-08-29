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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver

import pytest

from aws_wrapper import AwsWrapperConnection
from aws_wrapper.errors import AwsWrapperError, FailoverSuccessError
from aws_wrapper.utils.properties import WrapperProperties
from tests.integration.container.utils.aurora_test_utility import \
    AuroraTestUtility
from tests.integration.container.utils.conditions import (
    enable_on_deployment, enable_on_features, enable_on_num_instances)
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.proxy_helper import ProxyHelper
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@enable_on_num_instances(min_instances=2)
@enable_on_deployment(DatabaseEngineDeployment.AURORA)
class TestReadWriteSplitting:
    @pytest.fixture(scope='class')
    def aurora_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_aurora_region()
        return AuroraTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        return {"plugins": "aurora_host_list, read_write_splitting", "connect_timeout": 10, "autocommit": True}

    @pytest.fixture(scope='class')
    def props_with_failover(self):
        return {
            "plugins": "aurora_host_list, read_write_splitting, failover",
            "connect_timeout": 10, "autocommit": True, "timeout": 5000
        }

    @pytest.fixture(scope='class')
    def proxied_props(self, props):
        props_copy = props.copy()
        endpoint_suffix = TestEnvironment.get_current().get_proxy_database_info().get_instance_endpoint_suffix()
        props_copy.update({WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name: f"?.{endpoint_suffix}"})
        return props_copy

    @pytest.fixture(scope='class')
    def proxied_props_with_failover(self, props_with_failover):
        props_copy = props_with_failover.copy()
        endpoint_suffix = TestEnvironment.get_current().get_proxy_database_info().get_instance_endpoint_suffix()
        props_copy.update({WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name: f"?.{endpoint_suffix}"})
        return props_copy

    def test_connect_to_writer__switch_read_only(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_conn_string(), target_driver_connect, **props)
        writer_id = aurora_utils.query_instance_id(conn)

        conn.read_only = True
        reader_id = aurora_utils.query_instance_id(conn)
        assert writer_id != reader_id

        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert reader_id == current_id

        conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

        conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert reader_id == current_id

    def test_connect_to_reader__switch_read_only(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        reader_instance = test_environment.get_instances()[1]
        conn = AwsWrapperConnection.connect(
            conn_utils.get_conn_string(reader_instance.get_host()), target_driver_connect, **props)
        reader_id = aurora_utils.query_instance_id(conn)

        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert reader_id == current_id

        conn.read_only = False
        writer_id = aurora_utils.query_instance_id(conn)
        assert reader_id != writer_id

    def test_connect_to_reader_cluster__switch_read_only(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(
            conn_utils.get_conn_string(conn_utils.reader_cluster_host), target_driver_connect, **props)
        reader_id = aurora_utils.query_instance_id(conn)

        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert reader_id == current_id

        conn.read_only = False
        writer_id = aurora_utils.query_instance_id(conn)
        assert reader_id != writer_id

    def test_set_read_only_false__read_only_transaction(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_conn_string(), target_driver_connect, **props)
        writer_id = aurora_utils.query_instance_id(conn)

        conn.read_only = True
        reader_id = aurora_utils.query_instance_id(conn)
        assert writer_id != reader_id

        cursor = conn.cursor()
        cursor.execute("START TRANSACTION READ ONLY")
        cursor.execute("SELECT 1")

        with pytest.raises(AwsWrapperError):
            conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert reader_id == current_id

        cursor.execute("COMMIT")
        conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

    def test_set_read_only_false_in_transaction(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_conn_string(), target_driver_connect, **props)
        writer_id = aurora_utils.query_instance_id(conn)

        conn.read_only = True
        reader_id = aurora_utils.query_instance_id(conn)
        assert writer_id != reader_id

        cursor = conn.cursor()
        conn.autocommit = False
        cursor.execute("SELECT 1")

        with pytest.raises(AwsWrapperError):
            conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert reader_id == current_id

        cursor.execute("COMMIT")
        conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

    def test_set_read_only_true_in_transaction(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_conn_string(), target_driver_connect, **props)
        writer_id = aurora_utils.query_instance_id(conn)

        cursor = conn.cursor()
        conn.autocommit = False
        cursor.execute("SELECT 1")

        with pytest.raises(Exception):
            conn.read_only = True
        assert conn.read_only is False

        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    @enable_on_num_instances(min_instances=3)
    def test_set_read_only_true__all_readers_down(
            self, test_environment: TestEnvironment, test_driver: TestDriver, proxied_props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_props)
        writer_id = aurora_utils.query_instance_id(conn)

        instance_ids = [instance.get_instance_id() for instance in test_environment.get_instances()]
        for i in range(1, len(instance_ids)):
            ProxyHelper.disable_connectivity(instance_ids[i])

        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

        conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

        ProxyHelper.enable_all_connectivity()
        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id != current_id

    def test_set_read_only_true__closed_connection(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_conn_string(), target_driver_connect, **props)
        conn.close()

        with pytest.raises(AwsWrapperError):
            conn.read_only = True

    # TODO: Enable test when we resolve the query timeout issue for topology query
    # @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    # def test_set_read_only_false__all_instances_down(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver, proxied_props, conn_utils, aurora_utils):
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     conn = AwsWrapperConnection.connect(conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_props)
    #     writer_id = aurora_utils.query_instance_id(conn)
    #
    #     conn.read_only = True
    #     reader_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id != reader_id
    #
    #     ProxyHelper.disable_all_connectivity()
    #     with pytest.raises(AwsWrapperError):
    #         conn.read_only = False

    # TODO: Enable test when we implement old connection exception handling
    # def test_execute__old_connection(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     conn = AwsWrapperConnection.connect(conn_utils.get_conn_string(), target_driver_connect, **props)
    #     writer_id = aurora_utils.query_instance_id(conn)
    #
    #     old_cursor = conn.cursor()
    #     old_cursor.execute("SELECT 1")
    #     conn.read_only = True  # Switch connection internally
    #     conn.autocommit = False
    #
    #     with pytest.raises(AwsWrapperError):
    #         old_cursor.execute("SELECT 1")
    #
    #     reader_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id != reader_id
    #
    #     old_cursor.close()
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert reader_id == current_id

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED, TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    @enable_on_num_instances(min_instances=3)
    def test_failover_to_new_writer__switch_read_only(
            self, test_environment: TestEnvironment, test_driver: TestDriver,
            proxied_props_with_failover, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(
            conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_props_with_failover)
        original_writer_id = aurora_utils.query_instance_id(conn)

        instance_ids = [instance.get_instance_id() for instance in test_environment.get_instances()]
        for i in range(1, len(instance_ids)):
            ProxyHelper.disable_connectivity(instance_ids[i])

        # Force internal reader connection to the writer instance
        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert original_writer_id == current_id
        conn.read_only = False

        ProxyHelper.enable_all_connectivity()
        aurora_utils.failover_cluster_and_wait_until_writer_changed(original_writer_id)
        aurora_utils.assert_first_query_throws(conn, FailoverSuccessError)

        new_writer_id = aurora_utils.query_instance_id(conn)
        assert original_writer_id != new_writer_id
        assert aurora_utils.is_db_instance_writer(new_writer_id)

        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert new_writer_id != current_id

        conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert new_writer_id == current_id

    # TODO: Enable test when we resolve the query timeout issue for topology query
    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    @enable_on_num_instances(min_instances=3)
    def test_failover_to_new_reader__switch_read_only(
            self, test_environment: TestEnvironment, test_driver: TestDriver,
            proxied_props_with_failover, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        props = proxied_props_with_failover.copy()
        props.update({WrapperProperties.FAILOVER_MODE.name: "reader-or-writer"})
        conn = AwsWrapperConnection.connect(conn_utils.get_proxy_conn_string(), target_driver_connect, **props)
        writer_id = aurora_utils.query_instance_id(conn)

        conn.read_only = True
        reader_id = aurora_utils.query_instance_id(conn)
        assert writer_id != reader_id

        instances = test_environment.get_instances()
        other_reader_id = next((instance_id for instance_id in instances[1:] if id != reader_id), None)
        if other_reader_id is None:
            pytest.fail("Could not acquire alternate reader ID")

        # Kill all instances except for one other reader
        for instance in instances:
            instance_id = instance.get_instance_id()
            if instance_id != other_reader_id:
                ProxyHelper.disable_connectivity(instance_id)

        aurora_utils.assert_first_query_throws(conn, FailoverSuccessError)
        assert not conn.is_closed
        current_id = aurora_utils.query_instance_id(conn)
        assert other_reader_id == current_id
        assert reader_id != current_id

        ProxyHelper.enable_all_connectivity()
        conn.read_only = False
        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

        conn.read_only = True
        current_id = aurora_utils.query_instance_id(conn)
        assert other_reader_id == current_id

    # TODO: Enable test when we resolve the psycopg query timeout issue
    # @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    # @enable_on_num_instances(min_instances=3)
    # def test_failover_reader_to_writer__switch_read_only(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver,
    #         proxied_props_with_failover, conn_utils, aurora_utils):
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     conn = AwsWrapperConnection.connect(
    #         conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_props_with_failover)
    #     writer_id = aurora_utils.query_instance_id(conn)
    #
    #     conn.read_only = True
    #     reader_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id != reader_id
    #
    #     # Kill all instances except the writer
    #     for instance in test_environment.get_instances():
    #         instance_id = instance.get_instance_id()
    #         if instance_id != writer_id:
    #             ProxyHelper.disable_connectivity(instance_id)
    #
    #     aurora_utils.assert_first_query_throws(conn, FailoverSuccessError)
    #     assert not conn.is_closed
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id == current_id
    #
    #     ProxyHelper.enable_all_connectivity()
    #     conn.read_only = True
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id != current_id
    #
    #     conn.read_only = False
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id == current_id
