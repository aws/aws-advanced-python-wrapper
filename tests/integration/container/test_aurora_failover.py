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

import gc
from time import sleep
from typing import TYPE_CHECKING, List

import pytest  # type: ignore

from aws_advanced_python_wrapper.errors import (
    FailoverSuccessError, TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_features, enable_on_num_instances)
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.proxy_helper import ProxyHelper

if TYPE_CHECKING:
    from .utils.test_instance_info import TestInstanceInfo
    from .utils.test_driver import TestDriver

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection
from .utils.driver_helper import DriverHelper
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

logger = Logger(__name__)


@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestAuroraFailover:
    IDLE_CONNECTIONS_NUM: int = 5
    logger = Logger(__name__)

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        self.logger.info(f"Starting test: {request.node.name}")
        yield
        # Clean up global resources created by wrapper
        release_resources()
        self.logger.info(f"Ending test: {request.node.name}")
        release_resources()
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
            "autocommit": True
        })

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

    @pytest.mark.parametrize("plugins", ["failover", "failover_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_to_new_writer_fail_on_connection_invocation(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        props["plugins"] = plugins
        with AwsWrapperConnection.connect(
                target_driver_connect, **conn_utils.get_connect_params(), **props) as aws_conn:
            # crash instance1 and nominate a new writer
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            # failure occurs on Connection invocation
            aurora_utility.assert_first_query_throws(aws_conn, FailoverSuccessError)

            # assert that we are connected to the new writer after failover happens.
            current_connection_id = aurora_utility.query_instance_id(aws_conn)
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True
            assert current_connection_id != initial_writer_id

    @pytest.mark.parametrize("plugins", ["failover", "failover_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_to_new_writer_fail_on_connection_bound_object_invocation(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        props["plugins"] = plugins
        with AwsWrapperConnection.connect(
                target_driver_connect, **conn_utils.get_connect_params(), **props) as aws_conn:
            # crash instance1 and nominate a new writer
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            # failure occurs on Cursor invocation
            aurora_utility.assert_first_query_throws(aws_conn, FailoverSuccessError)

            # assert that we are connected to the new writer after failover happens and we can reuse the cursor
            current_connection_id = aurora_utility.query_instance_id(aws_conn)
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True
            assert current_connection_id != initial_writer_id

    @pytest.mark.parametrize("plugins", ["failover,host_monitoring", "failover,host_monitoring_v2",
                                         "failover_v2,host_monitoring", "failover_v2,host_monitoring_v2"])
    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                         TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    def test_fail_from_reader_to_writer(
            self,
            test_environment: TestEnvironment,
            test_driver: TestDriver,
            conn_utils,
            proxied_props,
            aurora_utility,
            plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        reader: TestInstanceInfo = test_environment.get_proxy_instances()[1]
        writer_id: str = test_environment.get_proxy_writer().get_instance_id()

        proxied_props["plugins"] = plugins
        with AwsWrapperConnection.connect(
                target_driver_connect,
                **conn_utils.get_proxy_connect_params(reader.get_host()),
                **proxied_props) as aws_conn:
            ProxyHelper.disable_connectivity(reader.get_instance_id())
            aurora_utility.assert_first_query_throws(aws_conn, FailoverSuccessError)
            current_connection_id = aurora_utility.query_instance_id(aws_conn)

            assert writer_id == current_connection_id
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True

    @pytest.mark.parametrize("plugins", ["failover", "failover_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_with_session_states_autocommit(self, test_driver: TestDriver, props, conn_utils, aurora_utility,
                                                             plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        props["plugins"] = plugins
        with AwsWrapperConnection.connect(target_driver_connect, **conn_utils.get_connect_params(), **props) as conn:
            conn.autocommit = False

            with conn.cursor() as cursor_1:
                cursor_1.execute("DROP TABLE IF EXISTS session_states")
                cursor_1.execute("CREATE TABLE session_states (id int not null primary key, session_states_field varchar(255) not null)")
                conn.commit()

            with conn.cursor() as cursor_2:
                cursor_2.execute("INSERT INTO session_states VALUES (1, 'test field string 1')")

                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                with pytest.raises(TransactionResolutionUnknownError):
                    cursor_2.execute("INSERT INTO session_states VALUES (2, 'test field string 2')")

            # Attempt to query the instance id.
            current_connection_id = aurora_utility.query_instance_id(conn)
            # Assert that we are connected to the new writer after failover happens.
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True
            next_cluster_writer_id = aurora_utility.get_cluster_writer_instance_id()
            assert current_connection_id == next_cluster_writer_id
            assert current_connection_id != initial_writer_id

            with conn.cursor() as cursor_3:
                cursor_3.execute("SELECT count(*) from session_states")
                result = cursor_3.fetchone()
                assert 0 == int(result[0])
                cursor_3.execute("DROP TABLE IF EXISTS session_states")
                conn.commit()
                # Assert autocommit is still False after failover.
                assert conn.autocommit is False

    @pytest.mark.parametrize("plugins", ["failover", "failover_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_fail_from_writer_with_session_states_readonly(self, test_driver: TestDriver, props, conn_utils, aurora_utility,
                                                           plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        props["plugins"] = plugins
        with AwsWrapperConnection.connect(target_driver_connect, **conn_utils.get_connect_params(), **props) as conn:
            assert conn.read_only is False
            conn.read_only = True
            assert conn.read_only is True

            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            aurora_utility.assert_first_query_throws(conn, FailoverSuccessError)

            # Attempt to query the instance id.
            current_connection_id = aurora_utility.query_instance_id(conn)
            # Assert that we are connected to the new writer after failover happens.
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True
            next_cluster_writer_id = aurora_utility.get_cluster_writer_instance_id()
            assert current_connection_id == next_cluster_writer_id
            assert current_connection_id != initial_writer_id

            # Assert readonly is still True after failover.
            assert conn.read_only is True

    @pytest.mark.parametrize("plugins", ["failover", "failover_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_writer_fail_within_transaction_set_autocommit_false(
            self, test_driver: TestDriver, test_environment: TestEnvironment, props, conn_utils, aurora_utility, plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_id = test_environment.get_writer().get_instance_id()

        props["plugins"] = plugins
        with AwsWrapperConnection.connect(
                target_driver_connect, **conn_utils.get_connect_params(), **props) as conn, conn.cursor() as cursor_1:
            cursor_1.execute("DROP TABLE IF EXISTS test3_2")
            cursor_1.execute("CREATE TABLE test3_2 (id int not null primary key, test3_2_field varchar(255) not null)")
            conn.commit()

            conn.autocommit = False

            with conn.cursor() as cursor_2:
                cursor_2.execute("INSERT INTO test3_2 VALUES (1, 'test field string 1')")

                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                with pytest.raises(TransactionResolutionUnknownError):
                    cursor_2.execute("INSERT INTO test3_2 VALUES (2, 'test field string 2')")

            # attempt to query the instance id
            current_connection_id: str = aurora_utility.query_instance_id(conn)

            # assert that we are connected to the new writer after failover happens
            assert aurora_utility.is_db_instance_writer(current_connection_id)
            next_cluster_writer_id: str = aurora_utility.get_cluster_writer_instance_id()

            assert current_connection_id == next_cluster_writer_id
            assert initial_writer_id != next_cluster_writer_id

            # cursor_2 can not be used anymore since it's invalid

            with conn.cursor() as cursor_3:
                cursor_3.execute("SELECT count(*) from test3_2")
                result = cursor_3.fetchone()
                assert 0 == int(result[0])
                cursor_3.execute("DROP TABLE IF EXISTS test3_2")
                conn.commit()

    @pytest.mark.parametrize("plugins", ["failover", "failover_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_writer_fail_within_transaction_start_transaction(
            self, test_driver: TestDriver, test_environment: TestEnvironment, props, conn_utils, aurora_utility,
            plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        initial_writer_id = test_environment.get_writer().get_instance_id()

        props["plugins"] = plugins
        with AwsWrapperConnection.connect(
                target_driver_connect, **conn_utils.get_connect_params(), **props) as conn:
            with conn.cursor() as cursor_1:
                cursor_1.execute("DROP TABLE IF EXISTS test3_3")
                cursor_1.execute(
                    "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)")
                conn.commit()

                cursor_1.execute("START TRANSACTION")

            with conn.cursor() as cursor_2:
                cursor_2.execute("INSERT INTO test3_3 VALUES (1, 'test field string 1')")

                aurora_utility.failover_cluster_and_wait_until_writer_changed()

                with pytest.raises(TransactionResolutionUnknownError):
                    cursor_2.execute("INSERT INTO test3_3 VALUES (2, 'test field string 2')")

            # attempt to query the instance id
            current_connection_id: str = aurora_utility.query_instance_id(conn)

            # assert that we are connected to the new writer after failover happens
            assert aurora_utility.is_db_instance_writer(current_connection_id)
            next_cluster_writer_id: str = aurora_utility.get_cluster_writer_instance_id()

            assert current_connection_id == next_cluster_writer_id
            assert initial_writer_id != next_cluster_writer_id

            # cursor_2 can not be used anymore since it's invalid

            with conn.cursor() as cursor_3:
                cursor_3.execute("SELECT count(*) from test3_3")
                result = cursor_3.fetchone()
                assert 0 == int(result[0])
                cursor_3.execute("DROP TABLE IF EXISTS test3_3")
                conn.commit()

    @pytest.mark.parametrize("plugins", ["aurora_connection_tracker,failover", "aurora_connection_tracker,failover_v2"])
    @enable_on_features([TestEnvironmentFeatures.FAILOVER_SUPPORTED])
    def test_writer_failover_in_idle_connections(
            self, test_driver: TestDriver, props, conn_utils, aurora_utility, plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        current_writer_id = aurora_utility.get_cluster_writer_instance_id()

        idle_connections: List[AwsWrapperConnection] = []
        props["plugins"] = plugins

        for i in range(self.IDLE_CONNECTIONS_NUM):
            idle_connections.append(
                AwsWrapperConnection.connect(
                    target_driver_connect, **conn_utils.get_connect_params(), **props))

        with AwsWrapperConnection.connect(
                target_driver_connect, **conn_utils.get_connect_params(), **props) as conn:
            instance_id = aurora_utility.query_instance_id(conn)
            assert current_writer_id == instance_id

            # ensure that all idle connections are still opened
            for idle_connection in idle_connections:
                assert idle_connection.is_closed is False

            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            with pytest.raises(FailoverSuccessError):
                aurora_utility.query_instance_id(conn)

        sleep(10)

        # Ensure that all idle connections are closed.
        for idle_connection in idle_connections:
            assert idle_connection.is_closed is True
