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

from .utils.test_environment_features import TestEnvironmentFeatures

if TYPE_CHECKING:
    from .utils.test_driver import TestDriver
    from .utils.test_environment import TestEnvironment
    from .utils.test_instance_info import TestInstanceInfo

from aws_wrapper.wrapper import AwsWrapperConnection
from .utils.conditions import enable_on_features
from .utils.driver_helper import DriverHelper
from .utils.proxy_helper import ProxyHelper


class TestBasicConnectivity:

    def test_direct_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = target_driver_connect(conn_utils.get_conn_string())
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        records = cursor.fetchall()
        assert len(records) == 1

        conn.close()

    def test_wrapper_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        awsconn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_conn_string())
        awscursor = awsconn.cursor()
        awscursor.execute("SELECT 1")
        records = awscursor.fetchall()
        assert len(records) == 1

        awsconn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_direct_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = target_driver_connect(conn_utils.get_proxy_conn_string())
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        records = cursor.fetchall()
        assert len(records) == 1

        conn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        awsconn = AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_proxy_conn_string())
        awscursor = awsconn.cursor()
        awscursor.execute("SELECT 1")
        records = awscursor.fetchall()
        assert len(records) == 1

        awsconn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection_failed(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_proxy_writer()

        ProxyHelper.disable_connectivity(instance.get_instance_id())

        try:
            AwsWrapperConnection.connect(target_driver_connect, conn_utils.get_proxy_conn_string())

            # Should not be here since proxy is blocking db connectivity
            assert False

        except Exception:
            # That is expected exception. Test pass.
            assert True
