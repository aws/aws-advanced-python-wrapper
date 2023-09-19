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

from aws_wrapper.host_list_provider import AuroraHostListProvider

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver

import pytest
from sqlalchemy import PoolProxiedConnection

from aws_wrapper import AwsWrapperConnection
from aws_wrapper.connection_provider import (
    ConnectionProviderManager, SqlAlchemyPooledConnectionProvider)
from aws_wrapper.errors import (AwsWrapperError, FailoverSuccessError,
                                TransactionResolutionUnknownError)
from aws_wrapper.utils.properties import WrapperProperties
from tests.integration.container.utils.aurora_test_utility import \
    AuroraTestUtility
from tests.integration.container.utils.conditions import (
    enable_on_deployment, enable_on_features, enable_on_num_instances)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
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

    @pytest.fixture(autouse=True)
    def clear_caches(self):
        AuroraHostListProvider._topology_cache.clear()
        AuroraHostListProvider._is_primary_cluster_id_cache.clear()
        AuroraHostListProvider._cluster_ids_to_update.clear()

    @pytest.fixture(scope='class')
    def props(self):
        return {"plugins": "aurora_host_list,read_write_splitting", "connect_timeout": 10, "autocommit": True}

    @pytest.fixture(scope='class')
    def failover_props(self):
        return {
            "plugins": "read_write_splitting, failover, host_monitoring", "connect_timeout": 10, "autocommit": True}

    @pytest.fixture(scope='class')
    def proxied_props(self, props):
        props_copy = props.copy()
        endpoint_suffix = TestEnvironment.get_current().get_proxy_database_info().get_instance_endpoint_suffix()
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(props_copy, f"?.{endpoint_suffix}")
        return props_copy

    @pytest.fixture(scope='class')
    def proxied_failover_props(self, failover_props):
        props_copy = failover_props.copy()
        endpoint_suffix = TestEnvironment.get_current().get_proxy_database_info().get_instance_endpoint_suffix()
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(props_copy, f"?.{endpoint_suffix}")
        return props_copy

    @pytest.fixture(autouse=True)
    def cleanup_connection_provider(self):
        yield
        ConnectionProviderManager.release_resources()
        ConnectionProviderManager.reset_provider()

    def test_connect_to_writer__switch_read_only(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
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
        conn = AwsWrapperConnection.connect(target_driver_connect,
                                            conn_utils.get_conn_string(reader_instance.get_host()), **props)
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
        conn = AwsWrapperConnection.connect(target_driver_connect,
                                            conn_utils.get_conn_string(conn_utils.reader_cluster_host), **props)
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
        conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
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
        conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
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
        conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
        writer_id = aurora_utils.query_instance_id(conn)

        cursor = conn.cursor()
        conn.autocommit = False
        cursor.execute("SELECT 1")

        with pytest.raises(Exception):
            conn.read_only = True
        assert conn.read_only is False

        current_id = aurora_utils.query_instance_id(conn)
        assert writer_id == current_id

    # @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    # @enable_on_num_instances(min_instances=3)
    # def test_set_read_only_true__all_readers_down(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver, proxied_props, conn_utils, aurora_utils):
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     conn = AwsWrapperConnection.connect(conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_props)
    #     writer_id = aurora_utils.query_instance_id(conn)
    #
    #     instance_ids = [instance.get_instance_id() for instance in test_environment.get_instances()]
    #     for i in range(1, len(instance_ids)):
    #         ProxyHelper.disable_connectivity(instance_ids[i])
    #
    #     conn.read_only = True
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id == current_id
    #
    #     conn.read_only = False
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id == current_id
    #
    #     ProxyHelper.enable_all_connectivity()
    #     conn.read_only = True
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id != current_id

    def test_set_read_only_true__closed_connection(
            self, test_environment: TestEnvironment, test_driver: TestDriver, props, conn_utils, aurora_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
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

    # TODO: Enable test when we resolve the query timeout issue for topology query
    # @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED, TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    # @enable_on_num_instances(min_instances=3)
    # def test_failover_to_new_writer__switch_read_only(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver,
    #         proxied_failover_props, conn_utils, aurora_utils):
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     conn = AwsWrapperConnection.connect(
    #         conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_failover_props)
    #     original_writer_id = aurora_utils.query_instance_id(conn)
    #
    #     instance_ids = [instance.get_instance_id() for instance in test_environment.get_instances()]
    #     for i in range(1, len(instance_ids)):
    #         ProxyHelper.disable_connectivity(instance_ids[i])
    #
    #     # Force internal reader connection to the writer instance
    #     conn.read_only = True
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert original_writer_id == current_id
    #     conn.read_only = False
    #
    #     ProxyHelper.enable_all_connectivity()
    #     aurora_utils.failover_cluster_and_wait_until_writer_changed(original_writer_id)
    #     aurora_utils.assert_first_query_throws(conn, FailoverSuccessError)
    #
    #     new_writer_id = aurora_utils.query_instance_id(conn)
    #     assert original_writer_id != new_writer_id
    #     assert aurora_utils.is_db_instance_writer(new_writer_id)
    #
    #     conn.read_only = True
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert new_writer_id != current_id
    #
    #     conn.read_only = False
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert new_writer_id == current_id

    # TODO: Enable test when we resolve the query timeout issue for topology query
    # @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    # @enable_on_num_instances(min_instances=3)
    # def test_failover_to_new_reader__switch_read_only(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver,
    #         proxied_failover_props, conn_utils, aurora_utils):
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     props = proxied_failover_props.copy()
    #     props.update({WrapperProperties.FAILOVER_MODE.name: "reader-or-writer"})
    #     conn = AwsWrapperConnection.connect(conn_utils.get_proxy_conn_string(), target_driver_connect, **props)
    #     writer_id = aurora_utils.query_instance_id(conn)
    #
    #     conn.read_only = True
    #     reader_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id != reader_id
    #
    #     instances = test_environment.get_instances()
    #     other_reader_id = next((
    #         instance.get_instance_id() for instance in instances[1:] if instance.get_instance_id() != reader_id), None)
    #     if other_reader_id is None:
    #         pytest.fail("Could not acquire alternate reader ID")
    #
    #     # Kill all instances except for one other reader
    #     for instance in instances:
    #         instance_id = instance.get_instance_id()
    #         if instance_id != other_reader_id:
    #             ProxyHelper.disable_connectivity(instance_id)
    #
    #     aurora_utils.assert_first_query_throws(conn, FailoverSuccessError)
    #     assert not conn.is_closed
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert other_reader_id == current_id
    #     assert reader_id != current_id
    #
    #     ProxyHelper.enable_all_connectivity()
    #     conn.read_only = False
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id == current_id
    #
    #     conn.read_only = True
    #     current_id = aurora_utils.query_instance_id(conn)
    #     assert other_reader_id == current_id

    # TODO: Enable test when we resolve the psycopg query timeout issue
    # @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    # @enable_on_num_instances(min_instances=3)
    # def test_failover_reader_to_writer__switch_read_only(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver,
    #         proxied_failover_props, conn_utils, aurora_utils):
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     conn = AwsWrapperConnection.connect(
    #         conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_failover_props)
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

    def test_pooled_connection__reuses_cached_connection(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn1 = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
        assert isinstance(conn1.target_connection, PoolProxiedConnection)
        driver_conn1 = conn1.target_connection.driver_connection
        conn1.close()

        conn2 = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
        assert isinstance(conn2.target_connection, PoolProxiedConnection)
        driver_conn2 = conn2.target_connection.driver_connection
        conn2.close()

        assert conn1 is not conn2
        assert driver_conn1 is driver_conn2

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__failover(
            self, test_environment: TestEnvironment, test_driver: TestDriver, aurora_utils, conn_utils, failover_props):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_url = conn_utils.get_conn_string()
        conn = AwsWrapperConnection.connect(target_driver_connect, initial_writer_url, **failover_props)
        assert isinstance(conn.target_connection, PoolProxiedConnection)
        initial_driver_conn = conn.target_connection.driver_connection
        initial_writer_id = aurora_utils.query_instance_id(conn)

        aurora_utils.failover_cluster_and_wait_until_writer_changed()
        with pytest.raises(FailoverSuccessError):
            aurora_utils.query_instance_id(conn)

        new_writer_id = aurora_utils.query_instance_id(conn)
        assert initial_writer_id != new_writer_id

        assert not isinstance(conn.target_connection, PoolProxiedConnection)
        new_driver_conn = conn.target_connection
        assert initial_driver_conn is not new_driver_conn

        # New connection to the original writer (now a reader)
        conn = AwsWrapperConnection.connect(target_driver_connect, initial_writer_url, **failover_props)
        current_id = aurora_utils.query_instance_id(conn)
        assert initial_writer_id == current_id

        assert isinstance(conn.target_connection, PoolProxiedConnection)
        current_driver_conn = conn.target_connection.driver_connection
        # The initial connection should have been evicted from the pool when failover occurred,
        # so this should be a new connection even though it is connected to the same instance.
        assert initial_driver_conn is not current_driver_conn

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__cluster_url_failover(
            self, test_environment: TestEnvironment, test_driver: TestDriver, aurora_utils, conn_utils, failover_props):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(target_driver_connect,
                                            conn_utils.get_conn_string(conn_utils.writer_cluster_host),
                                            **failover_props)
        # The internal connection pool should not be used if the connection is established via a cluster URL.
        assert 0 == len(SqlAlchemyPooledConnectionProvider._database_pools)

        initial_writer_id = aurora_utils.query_instance_id(conn)
        assert not isinstance(conn.target_connection, PoolProxiedConnection)
        initial_driver_conn = conn.target_connection

        aurora_utils.failover_cluster_and_wait_until_writer_changed()
        with pytest.raises(FailoverSuccessError):
            aurora_utils.query_instance_id(conn)

        new_writer_id = aurora_utils.query_instance_id(conn)
        assert initial_writer_id != new_writer_id
        assert 0 == len(SqlAlchemyPooledConnectionProvider._database_pools)

        assert not isinstance(conn.target_connection, PoolProxiedConnection)
        new_driver_conn = conn.target_connection
        assert initial_driver_conn is not new_driver_conn

    # TODO: Enable test when we resolve the query timeout issue for topology query
    # @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED, TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    # def test_pooled_connection__failover_failed(
    #         self, test_environment: TestEnvironment, test_driver: TestDriver,
    #         aurora_utils, conn_utils, proxied_failover_props):
    #     provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
    #     ConnectionProviderManager.set_connection_provider(provider)
    #
    #     WrapperProperties.FAILOVER_TIMEOUT_SEC.set(proxied_failover_props, "1")
    #     WrapperProperties.FAILURE_DETECTION_TIME_MS.set(proxied_failover_props, "1000")
    #     WrapperProperties.FAILURE_DETECTION_COUNT.set(proxied_failover_props, "1")
    #
    #     target_driver_connect = DriverHelper.get_connect_func(test_driver)
    #     conn = AwsWrapperConnection.connect(
    #         conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_failover_props)
    #     assert isinstance(conn.target_connection, PoolProxiedConnection)
    #     initial_driver_conn = conn.target_connection.driver_connection
    #     writer_id = aurora_utils.query_instance_id(conn)
    #
    #     ProxyHelper.disable_all_connectivity()
    #     with pytest.raises(FailoverFailedError):
    #         aurora_utils.query_instance_id(conn)
    #
    #     ProxyHelper.enable_all_connectivity()
    #     conn = AwsWrapperConnection.connect(
    #         conn_utils.get_proxy_conn_string(), target_driver_connect, **proxied_failover_props)
    #
    #     current_writer_id = aurora_utils.query_instance_id(conn)
    #     assert writer_id == current_writer_id
    #
    #     assert isinstance(conn.target_connection, PoolProxiedConnection)
    #     current_driver_conn = conn.target_connection.driver_connection
    #     # The initial connection should have been evicted from the pool when failover occurred,
    #     # so this should be a new connection even though it is connected to the same instance.
    #     assert initial_driver_conn is not current_driver_conn

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__failover_in_transaction(
            self, test_environment: TestEnvironment, test_driver: TestDriver, aurora_utils, conn_utils, failover_props):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_url = conn_utils.get_conn_string()
        conn = AwsWrapperConnection.connect(target_driver_connect, initial_writer_url, **failover_props)
        assert isinstance(conn.target_connection, PoolProxiedConnection)
        initial_driver_conn = conn.target_connection.driver_connection

        conn.autocommit = False
        initial_writer_id = aurora_utils.query_instance_id(conn)

        aurora_utils.failover_cluster_and_wait_until_writer_changed()
        with pytest.raises(TransactionResolutionUnknownError):
            aurora_utils.query_instance_id(conn)

        new_writer_id = aurora_utils.query_instance_id(conn)
        assert initial_writer_id != new_writer_id

        assert not isinstance(conn.target_connection, PoolProxiedConnection)
        new_driver_conn = conn.target_connection
        assert initial_driver_conn is not new_driver_conn

        conn = AwsWrapperConnection.connect(target_driver_connect, initial_writer_url, **failover_props)
        current_id = aurora_utils.query_instance_id(conn)
        assert initial_writer_id == current_id

        assert isinstance(conn.target_connection, PoolProxiedConnection)
        current_driver_conn = conn.target_connection.driver_connection
        # The initial connection should have been evicted from the pool when failover occurred,
        # so this should be a new connection even though it is connected to the same instance.
        assert initial_driver_conn is not current_driver_conn

    def test_pooled_connection__different_users(
            self, test_environment: TestEnvironment, test_driver: TestDriver, aurora_utils, conn_utils, props):
        privileged_user_props = props.copy()

        limited_user_props = props.copy()
        limited_user_name = "limited_user"
        limited_user_new_db = "limited_user_db"
        limited_user_password = "limited_user_password"
        WrapperProperties.USER.set(limited_user_props, limited_user_name)
        WrapperProperties.PASSWORD.set(limited_user_props, limited_user_password)

        wrong_user_right_password_props = props.copy()
        WrapperProperties.USER.set(wrong_user_right_password_props, "wrong_user")

        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        try:
            conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(),
                                                **privileged_user_props)
            assert isinstance(conn.target_connection, PoolProxiedConnection)
            privileged_driver_conn = conn.target_connection.driver_connection

            cursor = conn.cursor()
            cursor.execute(f"DROP USER IF EXISTS {limited_user_name}")
            aurora_utils.create_user(conn, limited_user_name, limited_user_password)
            engine = test_environment.get_engine()
            if engine == DatabaseEngine.MYSQL:
                db = test_environment.get_database_info().get_default_db_name()
                # MySQL needs this extra command to allow the limited user to connect to the default database
                cursor.execute(f"GRANT ALL PRIVILEGES ON {db}.* TO {limited_user_name}")

            # Validate that the privileged connection established above is not reused and that the new connection is
            # correctly established under the limited user
            conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(),
                                                **limited_user_props)
            assert isinstance(conn.target_connection, PoolProxiedConnection)
            limited_driver_conn = conn.target_connection.driver_connection
            assert privileged_driver_conn is not limited_driver_conn

            cursor = conn.cursor()
            with pytest.raises(Exception):
                # The limited user does not have create permissions on the default database, so this should fail
                cursor.execute(f"CREATE DATABASE {limited_user_new_db}")

            with pytest.raises(Exception):
                AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(),
                                             **wrong_user_right_password_props)
        finally:
            conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(),
                                                **privileged_user_props)
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {limited_user_new_db}")
            cursor.execute(f"DROP USER IF EXISTS {limited_user_name}")

    @enable_on_num_instances(min_instances=5)
    def test_pooled_connection__least_connections(
            self, test_environment: TestEnvironment, test_driver: TestDriver, aurora_utils, conn_utils, props):
        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        instances = test_environment.get_instances()
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": len(instances)})
        ConnectionProviderManager.set_connection_provider(provider)

        connections = []
        connected_reader_ids = []
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        try:
            # Assume one writer and [size - 1] readers. Create an internal connection pool for each reader.
            for _ in range(len(instances) - 1):
                conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
                connections.append(conn)

                conn.read_only = True
                reader_id = aurora_utils.query_instance_id(conn)
                assert reader_id not in connected_reader_ids
                connected_reader_ids.append(reader_id)
        finally:
            for conn in connections:
                conn.close()

    """Tests custom pool mapping together with internal connection pools and the leastConnections
    host selection strategy. This test overloads one reader with connections and then verifies
    that new connections are sent to the other readers until their connection count equals that of
    the overloaded reader.
    """
    @enable_on_num_instances(min_instances=5)
    def test_pooled_connection__least_connections__pool_mapping(
            self, test_environment: TestEnvironment, test_driver: TestDriver, aurora_utils, conn_utils, props):
        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        # We will be testing all instances excluding the writer and overloaded reader. Each instance
        # should be tested numOverloadedReaderConnections times to increase the pool connection count
        # until it equals the connection count of the overloaded reader.
        instances = test_environment.get_instances()
        overloaded_reader_connection_count = 3
        num_test_connections = (len(instances) - 2) * overloaded_reader_connection_count
        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": num_test_connections},
            # Create a new pool for each instance-arbitrary_prop combination
            lambda host_info, conn_props: f"{host_info.url}-{len(SqlAlchemyPooledConnectionProvider._database_pools)}"
        )
        ConnectionProviderManager.set_connection_provider(provider)

        connections = []
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        try:
            reader_to_overload = instances[1]
            for i in range(overloaded_reader_connection_count):
                # This should result in numOverloadedReaderConnections pools to the same reader instance,
                # with each pool consisting of just one connection. The total connection count for the
                # instance should be numOverloadedReaderConnections despite being spread across multiple
                # pools.
                conn = AwsWrapperConnection.connect(
                    target_driver_connect, conn_utils.get_conn_string(reader_to_overload.get_host()), **props)
                connections.append(conn)
            assert overloaded_reader_connection_count == len(SqlAlchemyPooledConnectionProvider._database_pools)

            for _ in range(num_test_connections):
                conn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string(), **props)
                connections.append(conn)

                conn.read_only = True
                current_id = aurora_utils.query_instance_id(conn)
                assert reader_to_overload.get_instance_id() != current_id
        finally:
            for conn in connections:
                conn.close()
