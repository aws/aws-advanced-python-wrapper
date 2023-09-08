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

import pytest

from tests.integration.container.utils.aurora_test_utility import \
    AuroraTestUtility
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment

if TYPE_CHECKING:
    from .utils.test_driver import TestDriver
    from .utils.test_instance_info import TestInstanceInfo

from aws_wrapper.wrapper import AwsWrapperConnection
from .utils.conditions import (disable_on_features, enable_on_deployment,
                               enable_on_features, enable_on_num_instances)
from .utils.driver_helper import DriverHelper
from .utils.proxy_helper import ProxyHelper
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures


class TestBasicConnectivity:

    @pytest.fixture(scope='class')
    def aurora_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_aurora_region()
        return AuroraTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        return {"plugins": "host_monitoring", "connect_timeout": 10}

    def test_direct_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = target_driver_connect(conn_utils.get_conn_string())
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    def test_wrapper_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_conn_string(), target_driver_connect)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_direct_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = target_driver_connect(conn_utils.get_proxy_conn_string())
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(conn_utils.get_proxy_conn_string(), target_driver_connect)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection_failed(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_proxy_writer()

        ProxyHelper.disable_connectivity(instance.get_instance_id())

        try:
            AwsWrapperConnection.connect(conn_utils.get_proxy_conn_string(), target_driver_connect)

            # Should not be here since proxy is blocking db connectivity
            assert False

        except Exception:
            # That is expected exception. Test pass.
            assert True

    @enable_on_num_instances(min_instances=2)
    @enable_on_deployment(DatabaseEngineDeployment.AURORA)
    @disable_on_features([TestEnvironmentFeatures.PERFORMANCE])
    def test_wrapper_connection_reader_cluster_with_efm_enabled(
            self, test_driver: TestDriver, props, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(
            conn_utils.get_conn_string(conn_utils.reader_cluster_host), target_driver_connect, **props)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()
