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

import gc

import pytest
from sqlalchemy import PoolProxiedConnection

from aws_advanced_python_wrapper import AwsWrapperConnection, release_resources
from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.errors import (
    AwsWrapperError, FailoverFailedError, FailoverSuccessError,
    ReadWriteSplittingError, TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.host_list_provider import RdsHostListProvider
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import \
    SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.storage.storage_service import \
    StorageService
from tests.integration.container.utils.conditions import (
    disable_on_engines, disable_on_features, enable_on_deployments,
    enable_on_features, enable_on_num_instances)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.proxy_helper import ProxyHelper
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_driver import TestDriver
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA,
                        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER,
                        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestReadWriteSplitting:

    logger = Logger(__name__)

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.logger.info(f"Starting test: {request.node.name}")
        yield
        self.logger.info(f"Ending test: {request.node.name}")

        release_resources()
        gc.collect()

    # Plugin configurations
    @pytest.fixture(
        params=[("read_write_splitting", "read_write_splitting"), ("srw", "srw")]
    )
    def plugin_config(self, request):
        return request.param

    @pytest.fixture(scope="class")
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(autouse=True)
    def clear_caches(self):
        StorageService.clear_all()
        RdsHostListProvider._is_primary_cluster_id_cache.clear()
        RdsHostListProvider._cluster_ids_to_update.clear()
        yield
        ConnectionProviderManager.release_resources()
        ConnectionProviderManager.reset_provider()
        gc.collect()
        ProxyHelper.enable_all_connectivity()

    @pytest.fixture
    def props(self, plugin_config, conn_utils):
        plugin_name, plugin_value = plugin_config
        p: Properties = Properties(
            {
                "plugins": plugin_value,
                "socket_timeout": 10,
                "connect_timeout": 10,
                "autocommit": True,
            }
        )

        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(p, conn_utils.writer_cluster_host)
            WrapperProperties.SRW_READ_ENDPOINT.set(p, conn_utils.reader_cluster_host)
            WrapperProperties.SRW_CONNECT_RETRY_TIMEOUT_MS.set(p, "30000")
            WrapperProperties.SRW_CONNECT_RETRY_INTERVAL_MS.set(p, "1000")

        if (
            TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED
            in TestEnvironment.get_current().get_features()
            or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED
            in TestEnvironment.get_current().get_features()
        ):
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if (
            TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED
            in TestEnvironment.get_current().get_features()
        ):
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if (
            TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED
            in TestEnvironment.get_current().get_features()
        ):
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    @pytest.fixture
    def failover_props(self, plugin_config, conn_utils):
        plugin_name, plugin_value = plugin_config
        props = {
            "plugins": f"{plugin_value},failover",
            "socket_timeout": 10,
            "connect_timeout": 10,
            "autocommit": True,
            "cluster_id": "cluster1"
        }
        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(
                props, conn_utils.writer_cluster_host
            )
            WrapperProperties.SRW_READ_ENDPOINT.set(
                props, conn_utils.reader_cluster_host
            )

        return props

    @pytest.fixture
    def proxied_props(self, props, plugin_config, conn_utils):
        plugin_name, _ = plugin_config
        props_copy = props.copy()

        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_writer_cluster_host}:{conn_utils.proxy_port}",
            )
            WrapperProperties.SRW_READ_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_reader_cluster_host}:{conn_utils.proxy_port}",
            )

        endpoint_suffix = (
            TestEnvironment.get_current()
            .get_proxy_database_info()
            .get_instance_endpoint_suffix()
        )
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(
            props_copy, f"?.{endpoint_suffix}:{conn_utils.proxy_port}"
        )
        return props_copy

    @pytest.fixture
    def proxied_failover_props(self, failover_props, plugin_config, conn_utils):
        plugin_name, _ = plugin_config
        props_copy = failover_props.copy()

        # Add simple plugin specific configuration
        if plugin_name == "srw":
            WrapperProperties.SRW_WRITE_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_writer_cluster_host}:{conn_utils.proxy_port}",
            )
            WrapperProperties.SRW_READ_ENDPOINT.set(
                props_copy,
                f"{conn_utils.proxy_reader_cluster_host}:{conn_utils.proxy_port}",
            )

        endpoint_suffix = (
            TestEnvironment.get_current()
            .get_proxy_database_info()
            .get_instance_endpoint_suffix()
        )
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(
            props_copy, f"?.{endpoint_suffix}:{conn_utils.proxy_port}"
        )
        return props_copy

    def test_connect_to_writer__switch_read_only(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            conn.read_only = True
            reader_id = rds_utils.query_instance_id(conn)
            assert writer_id != reader_id

            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert reader_id == current_id

            conn.read_only = False
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

            conn.read_only = False
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert reader_id == current_id

    def test_connect_to_reader__switch_read_only(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        props,
        conn_utils,
        rds_utils,
        plugin_config,
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            pytest.skip(
                "Test only applies to read_write_splitting plugin: srw does not connect to instances"
            )
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        reader_instance = test_environment.get_instances()[1]
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_connect_params(reader_instance.get_host()),
            **props,
        ) as conn:
            reader_id = rds_utils.query_instance_id(conn)

            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert reader_id == current_id

            conn.read_only = False
            writer_id = rds_utils.query_instance_id(conn)
            assert reader_id != writer_id

    def test_connect_to_reader_cluster__switch_read_only(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_connect_params(conn_utils.reader_cluster_host),
            **props,
        ) as conn:
            reader_id = rds_utils.query_instance_id(conn)

            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert reader_id == current_id

            conn.read_only = False
            writer_id = rds_utils.query_instance_id(conn)
            assert reader_id != writer_id

    def test_set_read_only_false__read_only_transaction(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            conn.read_only = True
            reader_id = rds_utils.query_instance_id(conn)
            assert writer_id != reader_id

            with conn.cursor() as cursor:
                cursor.execute("START TRANSACTION READ ONLY")
                cursor.execute("SELECT 1")
                cursor.fetchone()

                with pytest.raises(ReadWriteSplittingError):
                    conn.read_only = False

                current_id = rds_utils.query_instance_id(conn)
                assert reader_id == current_id

                cursor.execute("COMMIT")

                conn.read_only = False
                current_id = rds_utils.query_instance_id(conn)
                assert writer_id == current_id

    def test_set_read_only_false_in_transaction(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            conn.read_only = True
            reader_id = rds_utils.query_instance_id(conn)
            assert writer_id != reader_id

            with conn.cursor() as cursor:
                conn.autocommit = False
                cursor.execute("START TRANSACTION")

                with pytest.raises(ReadWriteSplittingError):
                    conn.read_only = False

                current_id = rds_utils.query_instance_id(conn)
                assert reader_id == current_id

                cursor.execute("COMMIT")
                conn.read_only = False
                current_id = rds_utils.query_instance_id(conn)
                assert writer_id == current_id

    def test_set_read_only_true_in_transaction(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            cursor = conn.cursor()
            conn.autocommit = False
            cursor.execute("START TRANSACTION")

            # MySQL allows users to change the read_only value during a transaction, Psycopg does not
            if test_driver == TestDriver.MYSQL:
                conn.read_only = True
            elif test_driver == TestDriver.PG:
                with pytest.raises(Exception):
                    conn.read_only = True
                assert conn.read_only is False

            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    @enable_on_num_instances(min_instances=3)
    def test_set_read_only_true__all_readers_down(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_props,
        conn_utils,
        rds_utils,
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params = conn_utils.get_proxy_connect_params()

        with AwsWrapperConnection.connect(
            target_driver_connect, **connect_params, **proxied_props
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            # Disable all reader instance ids and reader cluster endpoint.
            instance_ids = [
                instance.get_instance_id()
                for instance in test_environment.get_instances()
            ]
            for i in range(1, len(instance_ids)):
                ProxyHelper.disable_connectivity(instance_ids[i])
            ProxyHelper.disable_connectivity(
                test_environment.get_proxy_database_info().get_cluster_read_only_endpoint()
            )

            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

            conn.read_only = False
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

            ProxyHelper.enable_all_connectivity()
            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id != current_id

    def test_set_read_only_true__closed_connection(
        self, test_driver: TestDriver, props, conn_utils, rds_utils
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        )
        conn.close()

        with pytest.raises(AwsWrapperError):
            conn.read_only = True

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    @pytest.mark.skip
    def test_set_read_only_false__all_instances_down(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_props,
        conn_utils,
        rds_utils,
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        reader = test_environment.get_proxy_instances()[1]
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_proxy_connect_params(reader.get_host()),
            **proxied_props,
        ) as conn:
            ProxyHelper.disable_all_connectivity()
            with pytest.raises(AwsWrapperError):
                conn.read_only = False

    def test_execute__old_connection(
        self,
        test_driver: TestDriver,
        props: Properties,
        conn_utils,
        rds_utils,
        plugin_config,
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.set(props, "False")
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_connect_params(conn_utils.writer_cluster_host),
            **props,
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            old_cursor = conn.cursor()
            old_cursor.execute("SELECT 1")
            old_cursor.fetchone()
            conn.read_only = True  # Switch connection internally
            conn.autocommit = False

            with pytest.raises(AwsWrapperError):
                old_cursor.execute("SELECT 1")

            reader_id = rds_utils.query_instance_id(conn)
            assert writer_id != reader_id

            old_cursor.close()
            current_id = rds_utils.query_instance_id(conn)
            assert reader_id == current_id

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    @enable_on_num_instances(min_instances=3)
    def test_failover_to_new_writer__switch_read_only(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_failover_props,
        conn_utils,
        rds_utils,
    ):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params = conn_utils.get_proxy_connect_params()

        with AwsWrapperConnection.connect(
            target_driver_connect, **connect_params, **proxied_failover_props
        ) as conn:
            original_writer_id = rds_utils.query_instance_id(conn)

            # Disable all reader instance ids and reader cluster endpoint.
            instance_ids = [
                instance.get_instance_id()
                for instance in test_environment.get_instances()
            ]
            for i in range(1, len(instance_ids)):
                ProxyHelper.disable_connectivity(instance_ids[i])
            ProxyHelper.disable_connectivity(
                test_environment.get_proxy_database_info().get_cluster_read_only_endpoint()
            )

            # Force internal reader connection to the writer instance
            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert original_writer_id == current_id
            conn.read_only = False

            ProxyHelper.enable_all_connectivity()
            rds_utils.failover_cluster_and_wait_until_writer_changed(original_writer_id)
            rds_utils.assert_first_query_throws(conn, FailoverSuccessError)

            new_writer_id = rds_utils.query_instance_id(conn)
            assert original_writer_id != new_writer_id
            assert rds_utils.is_db_instance_writer(new_writer_id)

            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert new_writer_id != current_id

            conn.read_only = False
            current_id = rds_utils.query_instance_id(conn)
            assert new_writer_id == current_id

    @pytest.mark.parametrize("plugins", ["read_write_splitting,failover,host_monitoring", "read_write_splitting,failover,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    @enable_on_num_instances(min_instances=3)
    @disable_on_engines([DatabaseEngine.MYSQL])
    def test_failover_to_new_reader__switch_read_only(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_failover_props,
        conn_utils,
        rds_utils,
        plugin_config,
        plugins
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            # Disabling the reader connection in srw, the srwReadEndpoint, results in defaulting to the writer not connecting to another reader.
            pytest.skip(
                "Test only applies to read_write_splitting plugin: reader connection failover"
            )

        WrapperProperties.FAILOVER_MODE.set(proxied_failover_props, "reader-or-writer")

        WrapperProperties.PLUGINS.set(proxied_failover_props, plugins)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_proxy_connect_params(),
            **proxied_failover_props,
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            conn.read_only = True
            reader_id = rds_utils.query_instance_id(conn)
            assert writer_id != reader_id

            instances = test_environment.get_instances()
            other_reader_id = next(
                (
                    instance.get_instance_id()
                    for instance in instances[1:]
                    if instance.get_instance_id() != reader_id
                ),
                None,
            )
            if other_reader_id is None:
                pytest.fail("Could not acquire alternate reader ID")

            # Kill all instances except for one other reader
            for instance in instances:
                instance_id = instance.get_instance_id()
                if instance_id != other_reader_id:
                    ProxyHelper.disable_connectivity(instance_id)

            rds_utils.assert_first_query_throws(conn, FailoverSuccessError)
            assert not conn.is_closed
            current_id = rds_utils.query_instance_id(conn)
            assert other_reader_id == current_id
            assert reader_id != current_id

            ProxyHelper.enable_all_connectivity()
            conn.read_only = False
            assert not conn.is_closed
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

            conn.read_only = True
            assert not conn.is_closed
            current_id = rds_utils.query_instance_id(conn)
            assert other_reader_id == current_id

    @pytest.mark.parametrize("plugins", ["failover,host_monitoring", "failover,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    @enable_on_num_instances(min_instances=3)
    @disable_on_engines([DatabaseEngine.MYSQL])
    def test_failover_reader_to_writer__switch_read_only(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        proxied_failover_props,
        conn_utils,
        rds_utils,
        plugin_config,
        plugins
    ):
        plugin_name, _ = plugin_config
        WrapperProperties.PLUGINS.set(proxied_failover_props, plugin_name + "," + plugins)
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_proxy_connect_params(),
            **proxied_failover_props,
        ) as conn:
            writer_id = rds_utils.query_instance_id(conn)

            conn.read_only = True
            reader_id = rds_utils.query_instance_id(conn)
            assert writer_id != reader_id

            # Kill all instances except the writer
            for instance in test_environment.get_instances():
                instance_id = instance.get_instance_id()
                if instance_id != writer_id:
                    ProxyHelper.disable_connectivity(instance_id)
            ProxyHelper.disable_connectivity(
                test_environment.get_proxy_database_info().get_cluster_read_only_endpoint()
            )

            rds_utils.assert_first_query_throws(conn, FailoverSuccessError)
            assert not conn.is_closed
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

            ProxyHelper.enable_all_connectivity()
            conn.read_only = True
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id != current_id

            conn.read_only = False
            current_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_id

    def test_incorrect_reader_endpoint(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        conn_utils,
        rds_utils,
        plugin_config,
    ):
        plugin_name, plugin_value = plugin_config
        if plugin_name != "srw":
            pytest.skip(
                "Test only applies to simple_read_write_splitting plugin: uses srwReadEndpoint property"
            )

        props = Properties(
            {"plugins": plugin_value, "connect_timeout": 30, "autocommit": True}
        )
        port = (
            test_environment.get_info().get_database_info().get_cluster_endpoint_port()
        )
        writer_endpoint = conn_utils.writer_cluster_host

        # Set both endpoints to writer (incorrect reader endpoint)
        WrapperProperties.SRW_WRITE_ENDPOINT.set(props, f"{writer_endpoint}:{port}")
        WrapperProperties.SRW_READ_ENDPOINT.set(props, f"{writer_endpoint}:{port}")

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_connect_params(conn_utils.writer_cluster_host),
            **props,
        ) as conn:
            writer_connection_id = rds_utils.query_instance_id(conn)

            # Switch to reader successfully
            conn.read_only = True
            reader_connection_id = rds_utils.query_instance_id(conn)
            # Should stay on writer as fallback since reader endpoint points to a writer
            assert writer_connection_id == reader_connection_id

            # Going to the write endpoint will be the same connection again
            conn.read_only = False
            final_connection_id = rds_utils.query_instance_id(conn)
            assert writer_connection_id == final_connection_id

    def test_autocommit_state_preserved_across_connection_switches(
        self, test_driver: TestDriver, props, conn_utils, rds_utils, plugin_config
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "srw":
            pytest.skip(
                "Test only applies to simple_read_write_splitting plugin: autocommit impacts srw verification"
            )

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        WrapperProperties.SRW_VERIFY_NEW_CONNECTIONS.set(props, "False")
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        ) as conn:
            # Set autocommit to False on writer
            conn.autocommit = False
            assert conn.autocommit is False
            writer_connection_id = rds_utils.query_instance_id(conn)
            conn.commit()

            # Switch to reader - autocommit should remain False
            conn.read_only = True
            assert conn.autocommit is False
            reader_connection_id = rds_utils.query_instance_id(conn)
            assert writer_connection_id != reader_connection_id
            conn.commit()

            # Change autocommit on reader
            conn.autocommit = True
            assert conn.autocommit is True

            # Switch back to writer - autocommit should be True
            conn.read_only = False
            assert conn.autocommit is True
            final_writer_connection_id = rds_utils.query_instance_id(conn)
            assert writer_connection_id == final_writer_connection_id

    def test_pooled_connection__reuses_cached_connection(
        self, test_driver: TestDriver, conn_utils, props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn1 = AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        )
        assert isinstance(conn1.target_connection, PoolProxiedConnection)
        driver_conn1 = conn1.target_connection.driver_connection
        conn1.close()

        conn2 = AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **props
        )
        assert isinstance(conn2.target_connection, PoolProxiedConnection)
        driver_conn2 = conn2.target_connection.driver_connection
        conn2.close()

        assert conn1 is not conn2
        assert driver_conn1 is driver_conn2

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__failover(
        self, test_driver: TestDriver, rds_utils, conn_utils, failover_props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **failover_props
        ) as conn:
            assert isinstance(conn.target_connection, PoolProxiedConnection)
            initial_driver_conn = conn.target_connection.driver_connection
            initial_writer_id = rds_utils.query_instance_id(conn)

            rds_utils.failover_cluster_and_wait_until_writer_changed()
            with pytest.raises(FailoverSuccessError):
                rds_utils.query_instance_id(conn)

            new_writer_id = rds_utils.query_instance_id(conn)
            assert initial_writer_id != new_writer_id

            assert not isinstance(conn.target_connection, PoolProxiedConnection)
            new_driver_conn = conn.target_connection
            assert initial_driver_conn is not new_driver_conn

        # New connection to the original writer (now a reader)
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **failover_props
        ) as conn:
            current_id = rds_utils.query_instance_id(conn)
            assert initial_writer_id == current_id

            assert isinstance(conn.target_connection, PoolProxiedConnection)
            current_driver_conn = conn.target_connection.driver_connection
            # The initial connection should have been evicted from the pool when failover occurred,
            # so this should be a new connection even though it is connected to the same instance.
            assert initial_driver_conn is not current_driver_conn

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__cluster_url_failover(
        self, test_driver: TestDriver, rds_utils, conn_utils, failover_props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_connect_params(conn_utils.writer_cluster_host),
            **failover_props,
        ) as conn:
            # The internal connection pool should not be used if the connection is established via a cluster URL.
            assert 0 == len(SqlAlchemyPooledConnectionProvider._database_pools)

            initial_writer_id = rds_utils.query_instance_id(conn)
            assert not isinstance(conn.target_connection, PoolProxiedConnection)
            initial_driver_conn = conn.target_connection

            rds_utils.failover_cluster_and_wait_until_writer_changed()
            with pytest.raises(FailoverSuccessError):
                rds_utils.query_instance_id(conn)

            new_writer_id = rds_utils.query_instance_id(conn)
            assert initial_writer_id != new_writer_id
            assert 0 == len(SqlAlchemyPooledConnectionProvider._database_pools)

            assert not isinstance(conn.target_connection, PoolProxiedConnection)
            new_driver_conn = conn.target_connection
            assert initial_driver_conn is not new_driver_conn

    @pytest.mark.parametrize("plugins", ["failover,host_monitoring", "failover,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED,
                         TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    @disable_on_engines([DatabaseEngine.MYSQL])
    def test_pooled_connection__failover_failed(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        proxied_failover_props,
        plugin_config,
        plugins
    ):
        plugin_name, _ = plugin_config
        writer_host = test_environment.get_writer().get_host()
        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": 1},
            None,
            lambda host_info, props: writer_host in host_info.host,
        )
        ConnectionProviderManager.set_connection_provider(provider)

        WrapperProperties.FAILOVER_TIMEOUT_SEC.set(proxied_failover_props, "1")
        WrapperProperties.FAILURE_DETECTION_TIME_MS.set(proxied_failover_props, "1000")
        WrapperProperties.FAILURE_DETECTION_COUNT.set(proxied_failover_props, "1")
        WrapperProperties.PLUGINS.set(proxied_failover_props, plugin_name + "," + plugins)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_proxy_connect_params(),
            **proxied_failover_props,
        ) as conn:
            assert isinstance(conn.target_connection, PoolProxiedConnection)
            initial_driver_conn = conn.target_connection.driver_connection
            writer_id = rds_utils.query_instance_id(conn)

            ProxyHelper.disable_all_connectivity()
            with pytest.raises(FailoverFailedError):
                rds_utils.query_instance_id(conn)

            ProxyHelper.enable_all_connectivity()
            conn = AwsWrapperConnection.connect(
                target_driver_connect,
                **conn_utils.get_proxy_connect_params(),
                **proxied_failover_props,
            )

            current_writer_id = rds_utils.query_instance_id(conn)
            assert writer_id == current_writer_id

            assert isinstance(conn.target_connection, PoolProxiedConnection)
            current_driver_conn = conn.target_connection.driver_connection
            # The initial connection should have been evicted from the pool when failover occurred,
            # so this should be a new connection even though it is connected to the same instance.
            assert initial_driver_conn is not current_driver_conn

    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_pooled_connection__failover_in_transaction(
        self, test_driver: TestDriver, rds_utils, conn_utils, failover_props
    ):
        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **failover_props
        ) as conn:
            assert isinstance(conn.target_connection, PoolProxiedConnection)
            initial_driver_conn = conn.target_connection.driver_connection
            initial_writer_id = rds_utils.query_instance_id(conn)

            conn.autocommit = False
            cursor = conn.cursor()
            cursor.execute("START TRANSACTION")

            rds_utils.failover_cluster_and_wait_until_writer_changed()
            with pytest.raises(TransactionResolutionUnknownError):
                rds_utils.query_instance_id(conn)

            new_writer_id = rds_utils.query_instance_id(conn)
            assert initial_writer_id != new_writer_id

            assert not isinstance(conn.target_connection, PoolProxiedConnection)
            new_driver_conn = conn.target_connection
            assert initial_driver_conn is not new_driver_conn

        rds_utils.wait_until_cluster_has_desired_status(TestEnvironment.get_current().get_info().get_db_name(), "available")

        with AwsWrapperConnection.connect(
            target_driver_connect, **conn_utils.get_connect_params(), **failover_props
        ) as conn:
            current_id = rds_utils.query_instance_id(conn)
            assert initial_writer_id == current_id

            assert isinstance(conn.target_connection, PoolProxiedConnection)
            current_driver_conn = conn.target_connection.driver_connection
            # The initial connection should have been evicted from the pool when failover occurred,
            # so this should be a new connection even though it is connected to the same instance.
            assert initial_driver_conn is not current_driver_conn

    def test_pooled_connection__different_users(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        props,
    ):
        privileged_user_props = conn_utils.get_connect_params().copy()
        limited_user_props = conn_utils.get_connect_params().copy()
        limited_user_name = "limited_user"
        limited_user_new_db = "limited_user_db"
        limited_user_password = "limited_user_password"
        WrapperProperties.USER.set(limited_user_props, limited_user_name)
        WrapperProperties.PASSWORD.set(limited_user_props, limited_user_password)

        wrong_user_right_password_props = conn_utils.get_connect_params().copy()
        WrapperProperties.USER.set(wrong_user_right_password_props, "wrong_user")

        provider = SqlAlchemyPooledConnectionProvider(lambda _, __: {"pool_size": 1})
        ConnectionProviderManager.set_connection_provider(provider)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        try:
            with AwsWrapperConnection.connect(
                target_driver_connect, **privileged_user_props, **props
            ) as conn:
                assert isinstance(conn.target_connection, PoolProxiedConnection)
                privileged_driver_conn = conn.target_connection.driver_connection

                with conn.cursor() as cursor:
                    cursor.execute(f"DROP USER IF EXISTS {limited_user_name}")
                    rds_utils.create_user(
                        conn, limited_user_name, limited_user_password
                    )
                    engine = test_environment.get_engine()
                    if engine == DatabaseEngine.MYSQL:
                        db = test_environment.get_database_info().get_default_db_name()
                        # MySQL needs this extra command to allow the limited user to connect to the default database
                        cursor.execute(
                            f"GRANT ALL PRIVILEGES ON {db}.* TO {limited_user_name}"
                        )

                    # Validate that the privileged connection established above is not reused and that the new connection is
                    # correctly established under the limited user
                    with AwsWrapperConnection.connect(
                        target_driver_connect, **limited_user_props, **props
                    ) as conn2:
                        assert isinstance(
                            conn2.target_connection, PoolProxiedConnection
                        )
                        limited_driver_conn = conn2.target_connection.driver_connection
                        assert privileged_driver_conn is not limited_driver_conn

                        with conn2.cursor() as cursor2:
                            with pytest.raises(Exception):
                                # The limited user does not have create permissions on the default database, so this should fail
                                cursor2.execute(
                                    f"CREATE DATABASE {limited_user_new_db}"
                                )

                            with pytest.raises(Exception):
                                AwsWrapperConnection.connect(
                                    target_driver_connect,
                                    **wrong_user_right_password_props,
                                    **props,
                                )
        finally:
            conn = AwsWrapperConnection.connect(
                target_driver_connect, **privileged_user_props, **props
            )
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {limited_user_new_db}")
            cursor.execute(f"DROP USER IF EXISTS {limited_user_name}")

    @enable_on_num_instances(min_instances=5)
    def test_pooled_connection__least_connections(
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        props,
        plugin_config,
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            pytest.skip(
                "Test only applies to read_write_splitting plugin: reader host selector strategy"
            )

        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        instances = test_environment.get_instances()
        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": len(instances)}
        )
        ConnectionProviderManager.set_connection_provider(provider)

        connections = []
        connected_reader_ids = []
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        try:
            # Assume one writer and [size - 1] readers. Create an internal connection pool for each reader.
            for _ in range(len(instances) - 1):
                conn = AwsWrapperConnection.connect(
                    target_driver_connect, **conn_utils.get_connect_params(), **props
                )
                connections.append(conn)

                conn.read_only = True
                reader_id = rds_utils.query_instance_id(conn)
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
        self,
        test_environment: TestEnvironment,
        test_driver: TestDriver,
        rds_utils,
        conn_utils,
        props,
        plugin_config,
    ):
        plugin_name, _ = plugin_config
        if plugin_name != "read_write_splitting":
            pytest.skip(
                "Test only applies to read_write_splitting plugin: reader host selector strategy"
            )

        WrapperProperties.READER_HOST_SELECTOR_STRATEGY.set(props, "least_connections")

        # We will be testing all instances excluding the writer and overloaded reader. Each instance
        # should be tested overloaded_reader_connection_count times to increase the pool connection count
        # until it equals the connection count of the overloaded reader.
        instances = test_environment.get_instances()
        overloaded_reader_connection_count = 3
        num_test_connections = (len(instances) - 2) * overloaded_reader_connection_count
        provider = SqlAlchemyPooledConnectionProvider(
            lambda _, __: {"pool_size": num_test_connections},
            # Create a new pool for each instance-arbitrary_prop combination
            lambda host_info, conn_props: f"{host_info.url}-{len(SqlAlchemyPooledConnectionProvider._database_pools)}",
        )
        ConnectionProviderManager.set_connection_provider(provider)

        connections = []
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        try:
            reader_to_overload = instances[1]
            for i in range(overloaded_reader_connection_count):
                # This should result in overloaded_reader_connection_count pools to the same reader instance,
                # with each pool consisting of just one connection. The total connection count for the
                # instance should be overloaded_reader_connection_count despite being spread across multiple
                # pools.
                conn = AwsWrapperConnection.connect(
                    target_driver_connect,
                    **conn_utils.get_connect_params(reader_to_overload.get_host()),
                    **props,
                )
                connections.append(conn)
            assert overloaded_reader_connection_count == len(
                SqlAlchemyPooledConnectionProvider._database_pools
            )

            for _ in range(num_test_connections):
                conn = AwsWrapperConnection.connect(
                    target_driver_connect, **conn_utils.get_connect_params(), **props
                )
                connections.append(conn)

                conn.read_only = True
                current_id = rds_utils.query_instance_id(conn)
                assert reader_to_overload.get_instance_id() != current_id
        finally:
            for conn in connections:
                conn.close()
