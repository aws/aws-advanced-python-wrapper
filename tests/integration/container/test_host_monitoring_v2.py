#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from logging import getLogger
from threading import Thread
from time import perf_counter_ns
from typing import TYPE_CHECKING

import pytest

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.conditions import (
    disable_on_features, enable_on_deployments)
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver

logger = getLogger(__name__)


@enable_on_deployments([DatabaseEngineDeployment.AURORA,
                        DatabaseEngineDeployment.RDS,
                        DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER,
                        DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE])
@disable_on_features([TestEnvironmentFeatures.PERFORMANCE,
                      TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT])
class TestHostMonitoringV2:
    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        p: Properties = Properties({"plugins": "host_monitoring_v2",
                                    "socket_timeout": 30,
                                    "connect_timeout": 10,
                                    "failure_detection_time_ms": 5_000,
                                    "failure_detection_interval_ms": 5_000,
                                    "failure_detection_count": 1,
                                    "autocommit": True})

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(p, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    @pytest.fixture(scope='class')
    def proxied_props(self, props, conn_utils):
        props_copy = props.copy()
        endpoint_suffix = TestEnvironment.get_current().get_proxy_database_info().get_instance_endpoint_suffix()
        WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.set(props_copy, f"?.{endpoint_suffix}:{conn_utils.proxy_port}")
        return props_copy

    def test_host_monitoring_v2_network_failure_detection(self,
                                                          test_environment: TestEnvironment,
                                                          test_driver: TestDriver,
                                                          proxied_props: Properties,
                                                          conn_utils,
                                                          rds_utils):
        failure_delay_ms: int = 10_000
        max_duration_ms: int = 30_000
        start_ns: float = 0.0
        try:
            target_driver_connect = DriverHelper.get_connect_func(test_driver)
            instance = test_environment.get_proxy_instances()[0]
            conn = AwsWrapperConnection.connect(target_driver_connect,
                                                **conn_utils.get_proxy_connect_params(instance.get_host()),
                                                **proxied_props)
            simulate_temporary_failure_thread: Thread = rds_utils.simulate_temporary_failure(instance.get_instance_id(),
                                                                                             failure_delay_ms,
                                                                                             max_duration_ms)
            with conn.cursor() as cursor:
                simulate_temporary_failure_thread.start()
                start_ns = float(perf_counter_ns())
                cursor.execute(rds_utils.get_sleep_sql(30))
                pytest.fail("Sleep query should have failed")
        except Exception:
            end_ns = float(perf_counter_ns())
            duration_ns: float = end_ns - start_ns
            assert duration_ns > (failure_delay_ms * 1_000_000)
            assert duration_ns < (max_duration_ms * 1_000_000)
