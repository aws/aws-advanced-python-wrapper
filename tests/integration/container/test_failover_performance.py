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

import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging import getLogger
from time import perf_counter_ns, sleep
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pytest

from tests.integration.container.utils.driver_helper import DriverHelper

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection
    from tests.integration.container.utils.test_driver import TestDriver

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.conditions import enable_on_features
from tests.integration.container.utils.performance_utility import (
    PerformanceUtil, PerfStatBase)
from tests.integration.container.utils.proxy_helper import ProxyHelper
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

logger = getLogger(__name__)


class PerfStatMonitoring(PerfStatBase):
    param_detection_time: int = 0
    param_detection_interval: int = 0
    param_detection_count: int = 0
    param_network_outage_delay_millis: int = 0
    min_failure_detection_time_millis: int = 0
    max_failure_detection_time_millis: int = 0
    avg_failure_detection_time_millis: int = 0

    def write_data(self, writer):
        writer.writerow([self.param_detection_time,
                         self.param_detection_interval,
                         self.param_detection_count,
                         self.param_network_outage_delay_millis,
                         self.min_failure_detection_time_millis,
                         self.max_failure_detection_time_millis,
                         self.avg_failure_detection_time_millis])


@enable_on_features([TestEnvironmentFeatures.PERFORMANCE,
                     TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED,
                     TestEnvironmentFeatures.FAILOVER_SUPPORTED])
class TestPerformance:
    REPEAT_TIMES: int = 5
    TIMEOUT_SEC: int = 1
    CONNECT_TIMEOUT_SEC: int = 10
    PERF_FAILOVER_TIMEOUT_SEC: int = 120

    PERF_STAT_MONITORING_HEADER = [
        "FailureDetectionGraceTime",
        "FailureDetectionInterval",
        "FailureDetectionCount",
        "NetworkOutageDelayMillis",
        "MinFailureDetectionTimeMillis",
        "MaxFailureDetectionTimeMillis",
        "AvgFailureDetectionTimeMillis"
    ]

    logger = getLogger(__name__)

    failure_detection_time_params = [
        (30000, 5000, 3, 5),
        (30000, 5000, 3, 10),
        (30000, 5000, 3, 15),
        (30000, 5000, 3, 20),
        (30000, 5000, 3, 25),
        (30000, 5000, 3, 30),
        (30000, 5000, 3, 35),
        (30000, 5000, 3, 40),
        (30000, 5000, 3, 50),
        (30000, 5000, 3, 60),

        (6000, 1000, 1, 1),
        (6000, 1000, 1, 2),
        (6000, 1000, 1, 3),
        (6000, 1000, 1, 4),
        (6000, 1000, 1, 5),
        (6000, 1000, 1, 6),
        (6000, 1000, 1, 7),
        (6000, 1000, 1, 8),
        (6000, 1000, 1, 9),
        (6000, 1000, 1, 10),
    ]

    @pytest.fixture(scope='class')
    def props(self):
        endpoint_suffix = TestEnvironment.get_current().get_proxy_database_info().get_instance_endpoint_suffix()
        props: Properties = Properties({
            "monitoring-connect_timeout": TestPerformance.TIMEOUT_SEC,
            "monitoring-socket_timeout": TestPerformance.TIMEOUT_SEC,
            "connect_timeout": TestPerformance.CONNECT_TIMEOUT_SEC,
            "autocommit": "True",
            WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.name: f"?.{endpoint_suffix}"
        })

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.ENABLE_TELEMETRY.set(props, "True")
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(props, "True")

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(props, "XRAY")

        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in TestEnvironment.get_current().get_features():
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(props, "OTLP")

        return props

    @pytest.mark.parametrize("plugins", ["host_monitoring", "host_monitoring_v2"])
    def test_failure_detection_time_efm(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils,
                                        props: Properties, plugins):
        enhanced_failure_monitoring_perf_data_list: List[PerfStatBase] = []
        target_driver_connect_func = DriverHelper.get_connect_func(test_driver)
        try:
            for i in range(len(TestPerformance.failure_detection_time_params)):
                param = TestPerformance.failure_detection_time_params[i]
                detection_time: int = param[0]
                detection_interval: int = param[1]
                detection_count: int = param[2]
                sleep_delay_sec: int = param[3]

                WrapperProperties.FAILURE_DETECTION_ENABLED.set(props, "True")
                WrapperProperties.FAILURE_DETECTION_TIME_MS.set(props, str(detection_time))
                WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.set(props, str(detection_interval))
                WrapperProperties.FAILURE_DETECTION_COUNT.set(props, str(detection_count))
                WrapperProperties.PLUGINS.set(props, plugins)

                data: PerfStatMonitoring = PerfStatMonitoring()
                self._measure_performance(test_environment, target_driver_connect_func, conn_utils, sleep_delay_sec, props, data)
                data.param_detection_time = detection_time
                data.param_detection_interval = detection_interval
                data.param_detection_count = detection_count
                enhanced_failure_monitoring_perf_data_list.append(data)
        finally:
            PerformanceUtil.write_perf_data_to_file(
                f"/app/tests/integration/container/reports/"
                f"DbEngine_{test_environment.get_engine()}_"
                f"Plugins_{plugins}_"
                f"FailureDetectionPerformanceResults_EnhancedMonitoringEnabled.csv",
                TestPerformance.PERF_STAT_MONITORING_HEADER, enhanced_failure_monitoring_perf_data_list)

    @pytest.mark.parametrize("plugins", ["failover,host_monitoring", "failover,host_monitoring_v2"])
    def test_failure_detection_time_failover_and_efm(self, test_environment: TestEnvironment, test_driver: TestDriver, conn_utils,
                                                     props: Properties, plugins):
        enhanced_failure_monitoring_perf_data_list: List[PerfStatBase] = []
        try:
            for i in range(len(TestPerformance.failure_detection_time_params)):
                param = TestPerformance.failure_detection_time_params[i]
                detection_time: int = param[0]
                detection_interval: int = param[1]
                detection_count: int = param[2]
                sleep_delay_sec: int = param[3]

                WrapperProperties.FAILURE_DETECTION_ENABLED.set(props, "True")
                WrapperProperties.FAILURE_DETECTION_TIME_MS.set(props, str(detection_time))
                WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.set(props, str(detection_interval))
                WrapperProperties.FAILURE_DETECTION_COUNT.set(props, str(detection_count))
                WrapperProperties.PLUGINS.set(props, plugins)
                WrapperProperties.FAILOVER_TIMEOUT_SEC.set(props, TestPerformance.PERF_FAILOVER_TIMEOUT_SEC)
                WrapperProperties.FAILOVER_MODE.set(props, "strict_reader")

                data: PerfStatMonitoring = PerfStatMonitoring()
                self._measure_performance(test_environment, test_driver, conn_utils, sleep_delay_sec, props, data)
                data.param_detection_time = detection_time
                data.param_detection_interval = detection_interval
                data.param_detection_count = detection_count
                enhanced_failure_monitoring_perf_data_list.append(data)
        finally:
            PerformanceUtil.write_perf_data_to_file(
                f"/app/tests/integration/container/reports/"
                f"DbEngine_{test_environment.get_engine()}_"
                f"Plugins_{plugins}_"
                f"FailureDetectionPerformanceResults_FailoverAndEnhancedMonitoringEnabled.csv",
                TestPerformance.PERF_STAT_MONITORING_HEADER, enhanced_failure_monitoring_perf_data_list)

    def _measure_performance(
            self,
            test_environment: TestEnvironment,
            connect_func,
            conn_utils,
            sleep_delay_sec: int,
            props: Properties,
            data: PerfStatMonitoring):
        query: str = "SELECT pg_catalog.pg_sleep(600)"
        downtime: AtomicInt = AtomicInt()
        elapsed_times: List[int] = []

        for _ in range(TestPerformance.REPEAT_TIMES):
            downtime.set(0)

            with self._open_connect_with_retry(connect_func,
                                               conn_utils.get_proxy_connect_params(
                                                   test_environment.get_proxy_writer().get_host()),
                                               props) as aws_conn, ThreadPoolExecutor() as executor:
                try:
                    futures = [
                        executor.submit(self._stop_network_thread, test_environment, sleep_delay_sec, downtime),
                        executor.submit(self._execute, aws_conn, query, downtime, elapsed_times),
                    ]

                    for future in as_completed(futures):
                        future.result()

                except Exception:
                    traceback.print_exc()
                    pytest.fail()
                finally:
                    executor.shutdown(wait=False)
                    ProxyHelper.enable_connectivity(test_environment.get_proxy_writer().get_instance_id())

        min_val: int = min(elapsed_times)
        max_val: int = max(elapsed_times)
        avg_val: int = int(sum(elapsed_times) / len(elapsed_times))

        data.param_network_outage_delay_millis = sleep_delay_sec * 1000
        data.min_failure_detection_time_millis = PerformanceUtil.to_millis(min_val)
        data.max_failure_detection_time_millis = PerformanceUtil.to_millis(max_val)
        data.avg_failure_detection_time_millis = PerformanceUtil.to_millis(avg_val)

    def _open_connect_with_retry(self, connect_func, connect_params: Dict[str, Any], props: Properties):
        connection_attempts: int = 0
        conn: Optional[Connection] = None
        while conn is None and connection_attempts < 10:
            try:
                conn = AwsWrapperConnection.connect(
                    connect_func,
                    **connect_params,
                    **props)
            except Exception as e:
                TestPerformance.logger.debug("OpenConnectionFailed", str(e))
            connection_attempts += 1

        if conn is None:
            pytest.fail(f"Unable to connect to {connect_params}")
        return conn

    def _stop_network_thread(self, test_environment: TestEnvironment, sleep_delay_seconds: int, downtime: AtomicInt):
        sleep(sleep_delay_seconds)
        ProxyHelper.disable_connectivity(test_environment.get_proxy_writer().get_instance_id())
        down = perf_counter_ns()
        downtime.set(down)

    def _execute(self, aws_conn: Connection, query: str, downtime, elapsed_times):
        aws_cursor = aws_conn.cursor()
        try:
            aws_cursor.execute(query)
            pytest.fail("Sleep query finished, should not be possible with connectivity disabled")
        except Exception:
            # Calculate and add detection time
            failure_time: int = perf_counter_ns() - downtime.get()
            elapsed_times.append(failure_time)
