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

from dataclasses import dataclass
from logging import getLogger
from time import perf_counter_ns
from typing import TYPE_CHECKING, List

import pytest

from aws_wrapper.connect_time_plugin import ConnectTimePlugin
from aws_wrapper.connection_provider import (
    ConnectionProviderManager, SqlAlchemyPooledConnectionProvider)
from aws_wrapper.execute_time_plugin import ExecuteTimePlugin
from tests.integration.container.utils.driver_helper import DriverHelper

if TYPE_CHECKING:
    from tests.integration.container.utils.test_environment import TestEnvironment
    from tests.integration.container.utils.test_driver import TestDriver

from aws_wrapper import AwsWrapperConnection
from aws_wrapper.utils.properties import Properties
from tests.integration.container.utils.conditions import enable_on_features
from tests.integration.container.utils.performance_utility import (
    PerformanceUtil, PerfStatBase)
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

logger = getLogger(__name__)


@dataclass
class Result:
    switch_to_reader_min: int
    switch_to_reader_max: int
    switch_to_reader_avg: int

    switch_to_writer_min: int
    switch_to_writer_max: int
    switch_to_writer_avg: int


@dataclass
class PerfStatSwitchConnection(PerfStatBase):
    connection_switch: str
    min_overhead_time: int
    max_overhead_time: int
    avg_overhead_time: int
    avg_overhead_percentage: float

    def write_data(self, writer):
        writer.writerow([self.connection_switch,
                         self.min_overhead_time,
                         self.max_overhead_time,
                         self.avg_overhead_time,
                         self.avg_overhead_percentage])


@enable_on_features([TestEnvironmentFeatures.PERFORMANCE])
class TestReadWriteSplittingPerformance:
    REPEAT_TIMES: int = 100
    TIMEOUT_SEC: int = 5
    CONNECT_TIMEOUT_SEC: int = 10

    PERF_SWITCH_CONNECTION_STATS_HEADER = [
        "Benchmark",
        "Min Overhead Time",
        "Max Overhead Time",
        "Average Overhead Time",
        "Percentage (%) Increase of Average Test Run Overhead from Baseline",
    ]

    @pytest.fixture(scope='class')
    def default_plugins_props(self):
        props: Properties = Properties({
            "connect_timeout": TestReadWriteSplittingPerformance.CONNECT_TIMEOUT_SEC,
            "socket_timeout": TestReadWriteSplittingPerformance.TIMEOUT_SEC,
            "plugins": "connect_time,execute_time",
            "autocommit": "True"
        })
        return props

    @pytest.fixture(scope='class')
    def read_write_plugin_props(self):
        props: Properties = Properties({
            "connect_timeout": TestReadWriteSplittingPerformance.CONNECT_TIMEOUT_SEC,
            "socket_timeout": TestReadWriteSplittingPerformance.TIMEOUT_SEC,
            "plugins": "read_write_splitting,connect_time,execute_time",
            "autocommit": "True"
        })
        return props

    def test_switch_reader_writer_connection(
            self,
            test_environment: TestEnvironment,
            test_driver: TestDriver,
            conn_utils,
            read_write_plugin_props: Properties,
            default_plugins_props: Properties):

        set_readonly_perf_data_list: List[PerfStatBase] = []
        target_driver_connect_func = DriverHelper.get_connect_func(test_driver)

        try:
            result_with_def_plugins = self._measure_performance(test_environment, target_driver_connect_func, conn_utils, default_plugins_props)
            result_with_plugins = self._measure_performance(test_environment, target_driver_connect_func, conn_utils, read_write_plugin_props)

            ConnectionProviderManager.set_connection_provider(SqlAlchemyPooledConnectionProvider())
            results_with_pools = self._measure_performance(test_environment, target_driver_connect_func, conn_utils, read_write_plugin_props)
            ConnectionProviderManager.release_resources()
            ConnectionProviderManager.reset_provider()

            set_readonly_perf_data_list.append(PerfStatSwitchConnection(
                "Switch to reader",
                result_with_plugins.switch_to_reader_min - result_with_def_plugins.switch_to_reader_min,
                result_with_plugins.switch_to_reader_max - result_with_def_plugins.switch_to_reader_max,
                result_with_plugins.switch_to_reader_avg - result_with_def_plugins.switch_to_reader_avg,
                self.get_percentage_difference(result_with_def_plugins.switch_to_reader_avg, result_with_plugins.switch_to_reader_avg)
            ))

            set_readonly_perf_data_list.append(PerfStatSwitchConnection(
                "Switch back to writer (use cached connection)",
                result_with_plugins.switch_to_writer_min - result_with_def_plugins.switch_to_writer_min,
                result_with_plugins.switch_to_writer_max - result_with_def_plugins.switch_to_writer_max,
                result_with_plugins.switch_to_writer_avg - result_with_def_plugins.switch_to_writer_avg,
                self.get_percentage_difference(result_with_def_plugins.switch_to_writer_avg, result_with_plugins.switch_to_writer_avg)
            ))

            # internal connection pool results

            set_readonly_perf_data_list.append(PerfStatSwitchConnection(
                "Connection Pool switch to reader",
                results_with_pools.switch_to_reader_min - result_with_def_plugins.switch_to_reader_min,
                results_with_pools.switch_to_reader_max - result_with_def_plugins.switch_to_reader_max,
                results_with_pools.switch_to_reader_avg - result_with_def_plugins.switch_to_reader_avg,
                self.get_percentage_difference(result_with_def_plugins.switch_to_reader_avg, results_with_pools.switch_to_reader_avg)
            ))

            set_readonly_perf_data_list.append(PerfStatSwitchConnection(
                "Connection Pool switch back to writer (use cached connection)",
                results_with_pools.switch_to_writer_min - result_with_def_plugins.switch_to_writer_min,
                results_with_pools.switch_to_writer_max - result_with_def_plugins.switch_to_writer_max,
                results_with_pools.switch_to_writer_avg - result_with_def_plugins.switch_to_writer_avg,
                self.get_percentage_difference(result_with_def_plugins.switch_to_writer_avg, results_with_pools.switch_to_writer_avg)
            ))

        finally:
            PerformanceUtil.write_perf_data_to_file(
                f"/app/tests/integration/container/reports/"
                f"DbEngine_{test_environment.get_engine()}_ReadWriteSplittingPerformanceResults_SwitchReaderWriterConnection.csv",
                TestReadWriteSplittingPerformance.PERF_SWITCH_CONNECTION_STATS_HEADER, set_readonly_perf_data_list)

    def _measure_performance(
            self,
            test_environment: TestEnvironment,
            connect_func,
            conn_utils,
            read_write_plugin_props: Properties) -> Result:

        switch_to_reader_elapsed_times: List[int] = []
        switch_to_writer_elapsed_times: List[int] = []
        conn_str = conn_utils.get_conn_string(test_environment.get_writer().get_host())

        for _ in range(TestReadWriteSplittingPerformance.REPEAT_TIMES):
            with AwsWrapperConnection.connect(connect_func, conn_str, **read_write_plugin_props) as aws_conn:
                ConnectTimePlugin.reset_connect_time()
                ExecuteTimePlugin.reset_execute_time()

                switch_to_reader_start_time = perf_counter_ns()
                aws_conn.read_only = True
                switch_to_reader_elapsed_time = perf_counter_ns() - switch_to_reader_start_time

                connect_time: int = ConnectTimePlugin.connect_time
                execute_time: int = ExecuteTimePlugin.execute_time

                switch_to_reader_elapsed_times.append(switch_to_reader_elapsed_time - connect_time - execute_time)

                ConnectTimePlugin.reset_connect_time()
                ExecuteTimePlugin.reset_execute_time()

                switch_to_writer_start_time = perf_counter_ns()
                aws_conn.read_only = False
                switch_to_writer_elapsed_time = perf_counter_ns() - switch_to_writer_start_time

                connect_time = ConnectTimePlugin.connect_time
                execute_time = ExecuteTimePlugin.execute_time

                switch_to_writer_elapsed_times.append(switch_to_writer_elapsed_time - connect_time - execute_time)

        return Result(
            PerformanceUtil.to_millis(min(switch_to_reader_elapsed_times)),
            PerformanceUtil.to_millis(max(switch_to_reader_elapsed_times)),
            PerformanceUtil.to_millis(int(sum(switch_to_reader_elapsed_times) / len(switch_to_reader_elapsed_times))),
            PerformanceUtil.to_millis(min(switch_to_writer_elapsed_times)),
            PerformanceUtil.to_millis(max(switch_to_writer_elapsed_times)),
            PerformanceUtil.to_millis(int(sum(switch_to_writer_elapsed_times) / len(switch_to_writer_elapsed_times)))
        )

    def get_percentage_difference(self, v1, v2) -> float:
        return round((v2 - v1) / v1, 2)
