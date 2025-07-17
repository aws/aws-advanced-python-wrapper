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
    from .utils.test_instance_info import TestInstanceInfo
    from .utils.test_driver import TestDriver

import pytest

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_features, enable_on_num_instances)
from .utils.driver_helper import DriverHelper
from .utils.proxy_helper import ProxyHelper
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures


@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestBasicConnectivity:

    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        # By default, don't load the host_monitoring plugin so that the test doesn't require abort connection support
        p: Properties = Properties({WrapperProperties.PLUGINS.name: "aurora_connection_tracker,failover", "connect_timeout": 3})

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    def test_direct_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = target_driver_connect(**conn_utils.get_connect_params())
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    def test_wrapper_connection(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(target_driver_connect, **conn_utils.get_connect_params(), **props)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_direct_connection(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = target_driver_connect(**conn_utils.get_proxy_connect_params())
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(target_driver_connect, **conn_utils.get_proxy_connect_params(), **props)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()

    @enable_on_features([TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED])
    def test_proxied_wrapper_connection_failed(
            self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils, props):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        instance: TestInstanceInfo = test_environment.get_proxy_writer()

        ProxyHelper.disable_connectivity(instance.get_instance_id())

        try:
            AwsWrapperConnection.connect(target_driver_connect, **conn_utils.get_proxy_connect_params(), **props)

            # Should not be here since proxy is blocking db connectivity
            assert False

        except Exception:
            # That is expected exception. Test pass.
            assert True

    @pytest.mark.parametrize("plugins", ["host_monitoring", "host_monitoring_v2"])
    @enable_on_num_instances(min_instances=2)
    @enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
    @enable_on_features([TestEnvironmentFeatures.ABORT_CONNECTION_SUPPORTED])
    def test_wrapper_connection_reader_cluster_with_efm_enabled(self, test_driver: TestDriver, conn_utils, plugins):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        conn = AwsWrapperConnection.connect(
            target_driver_connect,
            **conn_utils.get_connect_params(conn_utils.reader_cluster_host),
            plugins=plugins, connect_timeout=10)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert 1 == result[0]

        conn.close()
