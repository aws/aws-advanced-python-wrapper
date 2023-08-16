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

from time import sleep
from typing import TYPE_CHECKING, List

import pytest

from aws_wrapper.errors import (FailoverSuccessError,
                                TransactionResolutionUnknownError)
from .utils.proxy_helper import ProxyHelper

if TYPE_CHECKING:
    from .utils.test_environment import TestEnvironment
    from .utils.test_instance_info import TestInstanceInfo
    from .utils.test_database_info import TestDatabaseInfo
    from .utils.test_proxy_database_info import TestProxyDatabaseInfo
    from .utils.test_driver import TestDriver

from logging import getLogger

from aws_wrapper.wrapper import AwsWrapperConnection
from .utils.aurora_test_utility import AuroraTestUtility
from .utils.conditions import failover_support_required
from .utils.driver_helper import DriverHelper


class TestAuroraFailover:
    IDLE_CONNECTIONS_NUM: int = 5
    logger = getLogger(__name__)

    @failover_support_required
    def test_fail_from_writer_to_new_writer_fail_on_connection_invocation(self, test_environment: TestEnvironment,
                                                                          test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params: str = self._init_default_props(test_environment)
        plugin: str = "failover"
        region: str = test_environment.get_info().get_aurora_region()
        aurora_utility = AuroraTestUtility(region)
        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        with AwsWrapperConnection.connect(connect_params, target_driver_connect, plugins=plugin) as aws_conn:
            # Enable autocommit, otherwise each select statement will start a valid transaction.
            aws_conn.autocommit = True

            # crash instance1 and nominate a new writer
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            # failure occurs on Connection invocation
            aurora_utility.assert_first_query_throws(aws_conn, FailoverSuccessError)

            # assert that we are connected to the new writer after failover happens.
            current_connection_id = aurora_utility.query_instance_id(aws_conn)
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True
            assert current_connection_id != initial_writer_id

    @failover_support_required
    def test_fail_from_writer_to_new_writer_fail_on_connection_bound_object_invocation(self,
                                                                                       test_environment: TestEnvironment,
                                                                                       test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params: str = self._init_default_props(test_environment)
        plugin: str = "failover"

        region: str = test_environment.get_info().get_aurora_region()
        aurora_utility = AuroraTestUtility(region)
        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        with AwsWrapperConnection.connect(connect_params, target_driver_connect, plugins=plugin) as aws_conn:
            # Enable autocommit, otherwise each select statement will start a valid transaction.
            aws_conn.autocommit = True

            cursor = aws_conn.cursor()
            assert cursor is not None

            # crash instance1 and nominate a new writer
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            # failure occurs on Connection invocation
            aurora_utility.assert_first_query_throws(aws_conn, FailoverSuccessError)

            # assert that we are connected to the new writer after failover happens and we can reuse the cursor
            current_connection_id = aurora_utility.query_instance_id(aws_conn)
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True
            assert current_connection_id != initial_writer_id

    @failover_support_required
    def test_fail_from_reader_to_writer(self, test_environment: TestEnvironment,
                                        test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        database_info: TestProxyDatabaseInfo = test_environment.get_info().get_proxy_database_info()
        instance: TestInstanceInfo = database_info.get_instances()[1]
        writer_id: str = database_info.get_instances()[0].get_instance_id()
        db_name: str = database_info.get_default_db_name()
        user: str = database_info.get_username()
        password: str = database_info.get_password()
        plugin: str = "failover"
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4} connect_timeout=10".format(
            instance.get_host(), instance.get_port(), db_name, user, password)

        region: str = test_environment.get_info().get_aurora_region()
        aurora_utility = AuroraTestUtility(region)

        with AwsWrapperConnection.connect(
                connect_params,
                target_driver_connect,
                plugins=plugin,
                cluster_instance_host_pattern=f"?.{database_info.get_instance_endpoint_suffix()}") as aws_conn:
            # Enable autocommit, otherwise each select statement will start a valid transaction.
            aws_conn.autocommit = True

            ProxyHelper.disable_connectivity(instance.get_instance_id())

            aurora_utility.assert_first_query_throws(aws_conn, FailoverSuccessError, None)
            TestAuroraFailover.logger.debug(aurora_utility.get_aurora_instance_ids())

            current_connection_id = aurora_utility.query_instance_id(aws_conn)

            assert writer_id == current_connection_id
            assert aurora_utility.is_db_instance_writer(current_connection_id) is True

    @failover_support_required
    def test_writer_fail_within_transaction_set_autocommit_false(self, test_environment: TestEnvironment,
                                                                 test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params: str = self._init_default_props(test_environment)
        region: str = test_environment.get_info().get_aurora_region()
        aurora_utility = AuroraTestUtility(region)
        initial_writer_id = test_environment.get_info().get_database_info().get_instances()[0].get_instance_id()

        with AwsWrapperConnection.connect(connect_params, target_driver_connect, plugins="failover") as conn, \
                conn.cursor() as cursor_1:
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

    @failover_support_required
    def test_writer_fail_within_transaction_start_transaction(self, test_environment: TestEnvironment,
                                                              test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params: str = self._init_default_props(test_environment)
        region: str = test_environment.get_info().get_aurora_region()
        aurora_utility = AuroraTestUtility(region)
        initial_writer_id = test_environment.get_info().get_database_info().get_instances()[0].get_instance_id()

        with AwsWrapperConnection.connect(connect_params, target_driver_connect, plugins="failover") as conn:
            # Enable autocommit, otherwise each select statement will start a valid transaction.
            conn.autocommit = True

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

    @failover_support_required
    def test_writer_failover_in_idle_connections(self, test_environment: TestEnvironment,
                                                 test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params: str = self._init_default_props(test_environment)
        region: str = test_environment.get_info().get_aurora_region()
        aurora_utility = AuroraTestUtility(region)
        current_writer_id = aurora_utility.get_cluster_writer_instance_id()

        idle_connections: List[AwsWrapperConnection] = []

        for i in range(self.IDLE_CONNECTIONS_NUM):
            idle_connections.append(AwsWrapperConnection.connect(connect_params, target_driver_connect,
                                                                 plugins="aurora_connection_tracker,failover"))

        with AwsWrapperConnection.connect(connect_params, target_driver_connect,
                                          plugins="aurora_connection_tracker,failover") as conn:

            # Enable autocommit, otherwise each select statement will start a valid transaction.
            conn.autocommit = True

            instance_id = aurora_utility.query_instance_id(conn)
            assert current_writer_id == instance_id

            # ensure that all idle connections are still opened
            for idle_connection in idle_connections:
                assert idle_connection is not None

            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            with pytest.raises(FailoverSuccessError):
                aurora_utility.query_instance_id(conn)

        sleep(10)

        # Ensure that all idle connections are closed.
        for idle_connection in idle_connections:
            assert idle_connection.is_closed is True

    @failover_support_required
    def test_basic_failover_with_efm(self, test_environment: TestEnvironment,
                                     test_driver: TestDriver):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        connect_params: str = self._init_default_props(test_environment)
        region: str = test_environment.get_info().get_aurora_region()
        aurora_utility = AuroraTestUtility(region)
        initial_writer_instance_info = test_environment.get_info().get_database_info().get_instances()[0]
        nominated_writer_instance_info = test_environment.get_info().get_database_info().get_instances()[1]

        nominated_writer_id = nominated_writer_instance_info.get_instance_id()

        with AwsWrapperConnection.connect(connect_params, target_driver_connect,
                                          plugins="failover,aurora_host_list") as conn:
            # Enable autocommit, otherwise each select statement will start a valid transaction.
            conn.autocommit = True

            aurora_utility.failover_cluster_and_wait_until_writer_changed(nominated_writer_id)
            aurora_utility.assert_first_query_throws(conn, FailoverSuccessError)

            current_connection_id = aurora_utility.query_instance_id(conn)

            instance_ids = aurora_utility.get_aurora_instance_ids()

            assert len(instance_ids) > 0

            next_writer_id = instance_ids[0]

            assert initial_writer_instance_info.get_instance_id() != current_connection_id
            assert next_writer_id == current_connection_id

    def _init_default_props(self, test_environment: TestEnvironment, ) -> str:
        database_info: TestDatabaseInfo = test_environment.get_info().get_database_info()
        instance: TestInstanceInfo = database_info.get_instances()[0]
        db_name: str = database_info.get_default_db_name()
        user: str = database_info.get_username()
        password: str = database_info.get_password()
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4} connect_timeout=10".format(
            instance.get_host(), instance.get_port(), db_name, user, password)

        return connect_params
