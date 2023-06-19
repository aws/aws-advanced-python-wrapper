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

from aws_wrapper.wrapper import AwsWrapperConnection
from .framework.conditions import network_outages_enabled_required
from .framework.driver_helper import DriverHelper
from .framework.proxy_helper import ProxyHelper
from .framework.test_driver import TestDriver
from .framework.test_environment import TestEnvironment
from .framework.test_instance_info import TestInstanceInfo


class TestBasicConnectivity:

    def test_direct_connection(self, test_environment: TestEnvironment,
                               test_driver: TestDriver):

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_info().get_database_info().get_instances()[0]
        db_name: str = test_environment.get_info().get_database_info().get_default_db_name()
        user: str = test_environment.get_info().get_database_info().get_username()
        password: str = test_environment.get_info().get_database_info().get_password()
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4}".format(
            instance.get_host(), instance.get_port(), db_name, user, password)

        conn = target_driver_connect(connect_params)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        records = cursor.fetchall()
        assert len(records) == 1

        conn.close()

    def test_wrapper_connection(self, test_environment: TestEnvironment,
                                test_driver: TestDriver):

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_info().get_database_info().get_instances()[0]
        db_name: str = test_environment.get_info().get_database_info().get_default_db_name()
        user: str = test_environment.get_info().get_database_info().get_username()
        password: str = test_environment.get_info().get_database_info().get_password()
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4}".format(
            instance.get_host(), instance.get_port(), db_name, user, password)

        awsconn = AwsWrapperConnection.connect(connect_params,
                                               target_driver_connect)
        awscursor = awsconn.cursor()
        awscursor.execute("SELECT 1")
        records = awscursor.fetchall()
        assert len(records) == 1

        awsconn.close()

    @network_outages_enabled_required
    def test_proxied_direct_connection(self, test_environment: TestEnvironment,
                                       test_driver: TestDriver):

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_info().get_proxy_database_info().get_instances()[0]
        db_name: str = test_environment.get_info().get_proxy_database_info().get_default_db_name()
        user: str = test_environment.get_info().get_proxy_database_info().get_username()
        password: str = test_environment.get_info().get_proxy_database_info().get_password()
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4}".format(
            instance.get_host(), instance.get_port(), db_name, user, password)

        conn = target_driver_connect(connect_params)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        records = cursor.fetchall()
        assert len(records) == 1

        conn.close()

    @network_outages_enabled_required
    def test_proxied_wrapper_connection(self, test_environment: TestEnvironment,
                                        test_driver: TestDriver):

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_info().get_proxy_database_info().get_instances()[0]
        db_name: str = test_environment.get_info().get_proxy_database_info().get_default_db_name()
        user: str = test_environment.get_info().get_proxy_database_info().get_username()
        password: str = test_environment.get_info().get_proxy_database_info().get_password()
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4}".format(
            instance.get_host(), instance.get_port(), db_name, user, password)

        awsconn = AwsWrapperConnection.connect(connect_params,
                                               target_driver_connect)
        awscursor = awsconn.cursor()
        awscursor.execute("SELECT 1")
        records = awscursor.fetchall()
        assert len(records) == 1

        awsconn.close()

    @network_outages_enabled_required
    def test_proxied_wrapper_connection_failed(
            self, test_environment: TestEnvironment, test_driver: TestDriver):

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_info().get_proxy_database_info().get_instances()[0]
        db_name: str = test_environment.get_info().get_proxy_database_info().get_default_db_name()
        user: str = test_environment.get_info().get_proxy_database_info().get_username()
        password: str = test_environment.get_info().get_proxy_database_info().get_password()
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4} connect_timeout=3".format(
            instance.get_host(), instance.get_port(), db_name, user, password)

        ProxyHelper.disable_connectivity(instance.get_instance_id())

        try:
            AwsWrapperConnection.connect(connect_params, target_driver_connect)

            # Should not be here since proxy is blocking db connectivity
            assert False

        except Exception:
            # That is expected exception. Test pass.
            assert True
