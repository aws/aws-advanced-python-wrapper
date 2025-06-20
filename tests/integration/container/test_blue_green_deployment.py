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

from typing import TYPE_CHECKING, Any, Deque, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from .utils.connection_utils import ConnectionUtils
    from .utils.test_driver import TestDriver

import math
import socket
from collections import deque
from dataclasses import dataclass
from threading import Event, Thread
from time import perf_counter_ns, sleep

import boto3
import pytest
from tabulate import tabulate  # type: ignore

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.blue_green_plugin import (BlueGreenPlugin,
                                                           BlueGreenRole)
from aws_advanced_python_wrapper.database_dialect import DialectCode
from aws_advanced_python_wrapper.driver_info import DriverInfo
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.concurrent import (ConcurrentDict,
                                                          CountDownLatch)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from .utils.conditions import enable_on_deployments, enable_on_features
from .utils.database_engine import DatabaseEngine
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.driver_helper import DriverHelper
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures


@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.MULTI_AZ_INSTANCE])
@enable_on_features([TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT])
class TestBlueGreenDeployment:
    logger = Logger(__name__)

    INCLUDE_CLUSTER_ENDPOINTS = False
    INCLUDE_WRITER_AND_READER_ONLY = False
    TEST_CLUSTER_ID = "test-cluster-id"
    MYSQL_BG_STATUS_QUERY = \
        ("SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) as hostId, endpoint, port, role, status, version "
         "FROM mysql.rds_topology")
    PG_AURORA_BG_STATUS_QUERY = \
        ("SELECT id, SPLIT_PART(endpoint, '.', 1) as hostId, endpoint, port, role, status, version "
         "FROM get_blue_green_fast_switchover_metadata('aws_jdbc_driver')")
    PG_RDS_BG_STATUS_QUERY = f"SELECT * FROM rds_tools.show_topology('aws_jdbc_driver-{DriverInfo.DRIVER_VERSION}')"
    results: ConcurrentDict[str, BlueGreenResults] = ConcurrentDict()
    unhandled_exceptions: Deque[Exception] = deque()

    @pytest.fixture(scope='class')
    def test_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def rds_utils(self):
        return RdsUtils()

    def test_switchover(self, conn_utils, test_utility, rds_utils, test_environment, test_driver):
        self.results.clear()
        self.unhandled_exceptions.clear()

        iam_enabled = TestEnvironmentFeatures.IAM in test_environment.get_features()
        start_time_ns = perf_counter_ns()
        stop = Event()
        start_latch = CountDownLatch()
        finish_latch = CountDownLatch()
        thread_count = 0
        thread_finish_count = 0
        threads: List[Thread] = []

        env = TestEnvironment.get_current()
        info = env.get_info()
        db_name = info.get_db_name()
        test_instance = env.get_writer()
        topology_instances: List[str] = self.get_bg_endpoints(
            test_environment, test_utility, rds_utils, info.get_bg_deployment_id())
        topology_instances_str = '\n'.join(topology_instances)
        self.logger.debug(f"topology_instances: \n{topology_instances_str}")

        for host in topology_instances:
            host_id = host[0:host.index(".")]
            assert host_id

            self.results.put(host_id, BlueGreenResults())

            if rds_utils.is_not_green_or_old_instance(host):
                threads.append(Thread(
                    target=self.direct_topology_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.direct_blue_connectivity_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.direct_blue_idle_connectivity_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_blue_idle_connectivity_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_blue_executing_connectivity_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_blue_new_connection_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1
                # TODO: should we increment thread_finish_count too?

                threads.append(Thread(
                    target=self.blue_dns_monitor,
                    args=(host_id, host, start_latch, stop, finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

            if rds_utils.is_green_instance(host):
                threads.append(Thread(
                    target=self.direct_topology_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_green_connectivity_monitor,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.green_dns_monitor,
                    args=(host_id, host, start_latch, stop, finish_latch, self.results.get(host_id))))
                thread_count += 1
                thread_finish_count += 1

                if iam_enabled:
                    rds_client = boto3.client("rds", region_name=test_environment.get_region())

                    threads.append(Thread(
                        target=self.green_iam_connectivity_monitor,
                        args=(test_driver, conn_utils, rds_client, host_id, "BlueHostToken",
                              self.rds_utils().remove_green_instance_prefix(host), host, test_instance.get_port(),
                              db_name, start_latch, stop, finish_latch, self.results.get(host_id),
                              self.results.get(host_id).green_direct_iam_ip_with_blue_node_connect_times, False, True)))
                    thread_count += 1
                    thread_finish_count += 1

                    threads.append(Thread(
                        target=self.green_iam_connectivity_monitor,
                        args=(test_driver, conn_utils, rds_client, host_id, "GreenHostToken", host, host,
                              test_instance.get_port(), db_name, start_latch, stop, finish_latch,
                              self.results.get(host_id),
                              self.results.get(host_id).green_direct_iam_ip_with_green_node_connect_times, True, False)
                    ))
                    thread_count += 1
                    thread_finish_count += 1

        threads.append(Thread(
            target=self.bg_switchover_trigger,
            args=(test_utility, info.get_bg_deployment_id(), start_latch, finish_latch, self.results)))
        thread_count += 1
        thread_finish_count += 1

        for result in self.results.values():
            result.start_time_ns.set(start_time_ns)

        for thread in threads:
            thread.start()

        self.logger.debug("All threads started.")

        finish_latch.wait_sec(6 * 60)
        self.logger.debug("All threads completed.")

        sleep(3 * 60)

        self.logger.debug("Stopping all threads...")
        stop.set()

        for thread in threads:
            thread.join(timeout=10)
            if thread.is_alive():
                self.logger.debug("Timed out waiting for a thread to stop running...")

        self.logger.debug("Done waiting for threads to stop.")

        for host_id, result in self.results.items():
            assert result.bg_trigger_time_ns.get() > 0, \
                f"bg_trigger_time for {host_id} was {result.bg_trigger_time_ns.get()}"

        self.logger.debug("Test is over.")
        self.print_metrics(rds_utils)

        if len(self.unhandled_exceptions) > 0:
            self.log_unhandled_exceptions()
            pytest.fail("There were unhandled exceptions.")

        self.assert_test()

        self.logger.debug("Completed")

    def get_bg_endpoints(
            self,
            test_env: TestEnvironment,
            test_utility: RdsTestUtility,
            rds_utils: RdsUtils,
            bg_id: str) -> List[str]:
        bg_deployment = test_utility.get_blue_green_deployment(bg_id)
        if bg_deployment is None:
            pytest.fail(f"Blue/Green deployment with ID '{bg_id}' not found.")

        if test_env.get_deployment() == DatabaseEngineDeployment.MULTI_AZ_INSTANCE:
            blue_instance = test_utility.get_rds_instance_info_by_arn(bg_deployment["Source"])
            if blue_instance is None:
                pytest.fail("Blue instance not found.")

            green_instance = test_utility.get_rds_instance_info_by_arn(bg_deployment["Target"])
            if green_instance is None:
                pytest.fail("Green instance not found.")

            return [blue_instance["Endpoint"]["Address"], green_instance["Endpoint"]["Address"]]

        elif test_env.get_deployment() == DatabaseEngineDeployment.AURORA:
            endpoints = []
            blue_cluster = test_utility.get_cluster_by_arn(bg_deployment["Source"])
            if blue_cluster is None:
                pytest.fail("Blue cluster not found.")

            if self.INCLUDE_CLUSTER_ENDPOINTS:
                endpoints.append(test_env.get_database_info().get_cluster_endpoint())

            instances = test_env.get_instances()
            if self.INCLUDE_WRITER_AND_READER_ONLY:
                endpoints.append(instances[0].get_host())
                if len(instances) > 1:
                    endpoints.append(instances[1].get_host())
            else:
                endpoints.extend([instance_info.get_host() for instance_info in instances])

            green_cluster = test_utility.get_cluster_by_arn(bg_deployment["Target"])
            if green_cluster is None:
                pytest.fail("Green cluster not found.")

            if self.INCLUDE_CLUSTER_ENDPOINTS:
                endpoints.append(green_cluster["Endpoint"])

            instance_ids = test_utility.get_instance_ids(green_cluster["Endpoint"])
            if len(instance_ids) < 1:
                pytest.fail("Cannot find green cluster instances.")

            instance_pattern = rds_utils.get_rds_instance_host_pattern(green_cluster["Endpoint"])
            if self.INCLUDE_WRITER_AND_READER_ONLY:
                endpoints.append(instance_pattern.replace("?", instance_ids[0]))
                if len(instance_ids) > 1:
                    endpoints.append(instance_pattern.replace("?", instance_ids[1]))
            else:
                endpoints.extend([instance_pattern.replace("?", instance_id) for instance_id in instance_ids])

            return endpoints
        else:
            pytest.fail(f"Unsupported blue/green engine deployment: {test_env.get_deployment()}")

    # Monitor BG status changes
    # Can terminate for itself
    def direct_topology_monitor(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            host_id: str,
            host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        conn = None
        test_env = TestEnvironment.get_current()
        engine = test_env.get_engine()

        query = None
        if engine == DatabaseEngine.MYSQL:
            query = self.MYSQL_BG_STATUS_QUERY
        elif engine == DatabaseEngine.PG:
            db_deployment = test_env.get_deployment()
            if db_deployment == DatabaseEngineDeployment.AURORA:
                query = self.PG_AURORA_BG_STATUS_QUERY
            elif db_deployment == DatabaseEngineDeployment.MULTI_AZ_INSTANCE:
                query = self.PG_RDS_BG_STATUS_QUERY
            else:
                pytest.fail(f"Unsupported blue/green database engine deployment: {db_deployment}")
        else:
            pytest.fail(f"Unsupported database engine: {engine}")

        try:
            conn = self.get_direct_connection(
                test_driver,
                **conn_utils.get_connect_params(host=host, port=port, dbname=db),
                **self.get_telemetry_params())
            self.logger.debug(f"[DirectTopology] @ {host_id}] Connection opened.")

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(f"[DirectTopology @ {host_id}] Starting BG status monitoring.")

            end_time_ns = perf_counter_ns() + 15 * 60 * 1_000_000_000  # 15 minutes
            while not stop.is_set() and perf_counter_ns() < end_time_ns:
                if conn is None:
                    conn = self.get_direct_connection(
                        test_driver, **conn_utils.get_connect_params(host=host, port=port, dbname=db))
                    self.logger.debug(f"[DirectTopology] @ {host_id}] Connection re-opened.")

                try:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    for record in cursor:
                        role = record["role"]
                        version = record["version"]
                        status = record["status"]
                        is_green = BlueGreenRole.parse_role(role, version) == BlueGreenRole.TARGET

                        def _log_and_return_time(_) -> int:
                            self.logger.debug(f"[DirectTopology] @ {host_id}] Status changed to: {status}.")
                            return perf_counter_ns()

                        if is_green:
                            results.green_status_time.compute_if_absent(status, _log_and_return_time)
                        else:
                            results.blue_status_time.compute_if_absent(status, _log_and_return_time)

                    sleep(0.1)
                except Exception as e:
                    self.logger.debug(f"[DirectTopology] @ {host_id}] Thread exception: {e}.")
                    self.close_connection(conn)
                    conn = None
        except Exception as e:
            self.logger.debug(f"[DirectTopology] @ {host_id}] Thread unhandled exception: {e}.")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[DirectTopology] @ {host_id}] Thread is completed.")

    def get_telemetry_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        features = TestEnvironment.get_current().get_features()
        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in features \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in features:
            params[WrapperProperties.ENABLE_TELEMETRY.name] = True
            params[WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.name] = True
        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in features:
            params[WrapperProperties.TELEMETRY_TRACES_BACKEND.name] = "XRAY"
        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in features:
            params[WrapperProperties.TELEMETRY_METRICS_BACKEND.name] = "OTLP"

        return params

    def get_direct_connection(self, test_driver: TestDriver, **connect_params) -> AwsWrapperConnection:
        conn = None
        connect_count = 0
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        while conn is None and connect_count < 10:
            try:
                conn = target_driver_connect(**connect_params)
            except Exception:
                # ignore, try to connect again
                pass

            connect_count += 1

        if conn is None:
            pytest.fail(f"Cannot connect to {connect_params.get('host')}")

        return conn

    def close_connection(self, conn: Optional[AwsWrapperConnection]):
        try:
            if conn is not None and not conn.is_closed:
                conn.close()
        except Exception:
            # do nothing
            pass

    # Blue node
    # Checking: connectivity, SELECT 1
    # Can terminate for itself
    def direct_blue_connectivity_monitor(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            host_id: str,
            host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        conn = None
        try:
            conn = self.get_direct_connection(
                test_driver,
                **conn_utils.get_connect_params(host=host, port=port, dbname=db),
                **self.get_telemetry_params())
            self.logger.debug(f"[DirectBlueConnectivity @ {host_id}] Connection opened.")

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(f"[DirectBlueConnectivity @ {host_id}] Starting connectivity monitoring.")

            while not stop.is_set():
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    sleep(1)
                except Exception as e:
                    self.logger.debug(f"[DirectBlueConnectivity @ {host_id}] Thread exception: {e}")
                    results.direct_blue_lost_connection_time_ns.set(perf_counter_ns())
                    break
        except Exception as e:
            self.logger.debug(f"[DirectBlueConnectivity @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[DirectBlueConnectivity @ {host_id}] Thread is completed.")

    # Blue node
    # Checking: connectivity, is_closed
    # Can terminate for itself
    def direct_blue_idle_connectivity_monitor(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            host_id: str,
            host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        conn = None
        try:
            conn = self.get_direct_connection(
                test_driver,
                **conn_utils.get_connect_params(host=host, port=port, dbname=db),
                **self.get_telemetry_params())
            self.logger.debug(f"[DirectBlueIdleConnectivity @ {host_id}] Connection opened.")

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(f"[DirectBlueIdleConnectivity @ {host_id}] Starting connectivity monitoring.")

            while not stop.is_set():
                try:
                    if conn.is_closed:
                        results.direct_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                        break

                    sleep(1)
                except Exception as e:
                    self.logger.debug(f"[DirectBlueIdleConnectivity @ {host_id}] Thread exception: {e}")
                    results.direct_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                    break
        except Exception as e:
            self.logger.debug(f"[DirectBlueIdleConnectivity @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[DirectBlueIdleConnectivity @ {host_id}] Thread is completed.")

    # Blue node
    # Check: connectivity, is_closed
    # Can terminate for itself
    def wrapper_blue_idle_connectivity_monitor(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            host_id: str,
            host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        conn = None
        try:
            connect_params = self.get_wrapper_connect_params(conn_utils, host, port, db)
            conn = self.get_wrapper_connection(test_driver, **connect_params)
            self.logger.debug(f"[WrapperBlueIdleConnectivity @ {host_id}] Connection opened.")

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(f"[WrapperBlueIdleConnectivity @ {host_id}] Starting connectivity monitoring.")

            while not stop.is_set():
                try:
                    if conn.is_closed:
                        results.wrapper_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                        break

                    sleep(1)
                except Exception as e:
                    self.logger.debug(f"[WrapperBlueIdleConnectivity @ {host_id}] Thread exception: {e}")
                    results.direct_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                    break
        except Exception as e:
            self.logger.debug(f"[WrapperBlueIdleConnectivity @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[WrapperBlueIdleConnectivity @ {host_id}] Thread is completed.")

    def get_wrapper_connect_params(self, conn_utils: ConnectionUtils, host: str, port: int, db: str) -> Dict[str, Any]:
        params = conn_utils.get_connect_params(host=host, port=port, dbname=db)
        params = {**params, **self.get_telemetry_params()}
        params[WrapperProperties.CLUSTER_ID.name] = self.TEST_CLUSTER_ID
        test_env = TestEnvironment.get_current()
        engine = test_env.get_engine()
        db_deployment = test_env.get_deployment()

        if db_deployment == DatabaseEngineDeployment.AURORA:
            if engine == DatabaseEngine.MYSQL:
                params[WrapperProperties.DIALECT.name] = DialectCode.AURORA_MYSQL
            elif engine == DatabaseEngine.PG:
                params[WrapperProperties.DIALECT.name] = DialectCode.AURORA_PG
        elif db_deployment == DatabaseEngineDeployment.MULTI_AZ_INSTANCE:
            if engine == DatabaseEngine.MYSQL:
                params[WrapperProperties.DIALECT.name] = DialectCode.RDS_MYSQL
            elif engine == DatabaseEngine.PG:
                params[WrapperProperties.DIALECT.name] = DialectCode.RDS_PG

        if TestEnvironmentFeatures.IAM in test_env.get_features():
            params[WrapperProperties.PLUGINS.name] = "bg,iam"
            params[WrapperProperties.USER.name] = test_env.get_info().get_iam_user_name()
            params[WrapperProperties.IAM_REGION.name] = test_env.get_info().get_region()
        else:
            params[WrapperProperties.PLUGINS.name] = "bg"

        return params

    def get_wrapper_connection(self, test_driver: TestDriver, **connect_params) -> AwsWrapperConnection:
        conn = None
        connect_count = 0
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        while conn is None and connect_count < 10:
            try:
                conn = AwsWrapperConnection.connect(target_driver_connect, **connect_params)
            except Exception:
                # ignore, try to connect again
                pass

            connect_count += 1

        if conn is None:
            pytest.fail(f"Cannot connect to {connect_params.get('host')}")

        return conn

    # Blue node
    # Check: connectivity, SELECT sleep(5)
    # Expect: long execution time (longer than 5s) during active phase of switchover
    # Can terminate for itself
    def wrapper_blue_executing_connectivity_monitor(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            host_id: str,
            host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        conn = None
        query = None
        test_env = TestEnvironment.get_current()
        engine = test_env.get_engine()
        if engine == DatabaseEngine.MYSQL:
            query = "SELECT sleep(5)"
        elif engine == DatabaseEngine.PG:
            query = "SELECT pg_sleep(5)"
        else:
            pytest.fail(f"Unsupported database engine: {engine}")

        try:
            connect_params = self.get_wrapper_connect_params(conn_utils, host, port, db)
            conn = self.get_wrapper_connection(test_driver, **connect_params)
            bg_plugin: Optional[BlueGreenPlugin] = conn._unwrap(BlueGreenPlugin)
            assert bg_plugin is not None, f"Unable to find blue/green plugin in wrapper connection for {host}."
            self.logger.debug(f"[WrapperBlueExecute @ {host_id}] Connection opened.")

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(f"[WrapperBlueExecute @ {host_id}] Starting connectivity monitoring.")

            while not stop.is_set():
                start_time_ns = perf_counter_ns()
                try:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    end_time_ns = perf_counter_ns()
                    results.blue_wrapper_execute_times.append(
                        TimeHolder(start_time_ns, end_time_ns, bg_plugin.get_hold_time_ns()))
                except Exception as e:
                    results.blue_wrapper_execute_times.append(
                        TimeHolder(start_time_ns, perf_counter_ns(), bg_plugin.get_hold_time_ns(), str(e)))
                    if conn.is_closed:
                        break

                sleep(1)
        except Exception as e:
            self.logger.debug(f"[WrapperBlueExecute @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[WrapperBlueExecute @ {host_id}] Thread is completed.")

    # Blue node
    # Check: connectivity, opening a new connection
    # Expect: longer opening connection time during active phase of switchover
    # Need a stop signal to terminate
    def wrapper_blue_new_connection_monitor(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            host_id: str,
            host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        conn = None
        try:
            connect_params = self.get_wrapper_connect_params(conn_utils, host, port, db)

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(f"[WrapperBlueNewConnection @ {host_id}] Starting connectivity monitoring.")

            while not stop.is_set():
                start_time_ns = perf_counter_ns()

                try:
                    conn = self.get_wrapper_connection(test_driver, **connect_params)
                    end_time_ns = perf_counter_ns()
                    bg_plugin: Optional[BlueGreenPlugin] = conn._unwrap(BlueGreenPlugin)
                    assert bg_plugin is not None, f"Unable to find blue/green plugin in wrapper connection for {host}."

                    results.blue_wrapper_connect_times.append(
                        TimeHolder(start_time_ns, end_time_ns, bg_plugin.get_hold_time_ns()))
                except Exception as e:
                    self.logger.debug(f"[WrapperBlueNewConnection @ {host_id}] Thread exception: {e}")
                    end_time_ns = perf_counter_ns()
                    if conn is not None:
                        bg_plugin = conn._unwrap(BlueGreenPlugin)
                        assert bg_plugin is not None, f"Unable to find blue/green plugin in wrapper connection for {host}."
                        results.blue_wrapper_connect_times.append(
                            TimeHolder(start_time_ns, end_time_ns, bg_plugin.get_hold_time_ns(), str(e)))
                    else:
                        results.blue_wrapper_connect_times.append(
                            TimeHolder(start_time_ns, end_time_ns, error=str(e)))

                self.close_connection(conn)
                conn = None
                sleep(1)

        except Exception as e:
            self.logger.debug(f"[WrapperBlueNewConnection @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[WrapperBlueNewConnection @ {host_id}] Thread is completed.")

    # Blue DNS
    # Check time of IP address change
    # Can terminate for itself
    def blue_dns_monitor(
            self,
            host_id: str,
            host: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        try:
            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)

            original_ip = socket.gethostbyname(host)
            self.logger.debug(f"[BlueDNS @ {host_id}] {host} -> {original_ip}")

            while not stop.is_set():
                sleep(1)

                try:
                    current_ip = socket.gethostbyname(host)
                    if current_ip != original_ip:
                        results.dns_blue_changed_time_ns.set(perf_counter_ns())
                        self.logger.debug(f"[BlueDNS @ {host_id}] {host} -> {current_ip}")
                        break
                except socket.gaierror as e:
                    self.logger.debug(f"[BlueDNS @ {host_id}] Error: {e}")
                    results.dns_blue_error = str(e)
                    results.dns_blue_changed_time_ns.set(perf_counter_ns())
                    break

        except Exception as e:
            self.logger.debug(f"[BlueDNS @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[BlueDNS @ {host_id}] Thread is completed.")

    # Green node
    # Check: connectivity, SELECT 1
    # Expect: no interruption, execute takes longer time during BG switchover
    # Can terminate for itself
    def wrapper_green_connectivity_monitor(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            host_id: str,
            host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        conn = None
        try:
            connect_params = self.get_wrapper_connect_params(conn_utils, host, port, db)
            conn = self.get_wrapper_connection(test_driver, **connect_params)
            self.logger.debug(f"[WrapperGreenConnectivity @ {host_id}] Connection opened.")

            bg_plugin: Optional[BlueGreenPlugin] = conn._unwrap(BlueGreenPlugin)
            assert bg_plugin is not None, f"Unable to find blue/green plugin in wrapper connection for {host}."

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(f"[WrapperGreenConnectivity @ {host_id}] Starting connectivity monitoring.")

            start_time_ns = perf_counter_ns()
            while not stop.is_set():
                try:
                    cursor = conn.cursor()
                    start_time_ns = perf_counter_ns()
                    cursor.execute("SELECT 1")
                    end_time_ns = perf_counter_ns()
                    results.green_wrapper_execute_times.append(
                        TimeHolder(start_time_ns, end_time_ns, bg_plugin.get_hold_time_ns()))
                    sleep(1)
                except Exception as e:
                    # TODO: do we need to handle the query timeout scenario like JDBC does for sqlTimeoutException?
                    self.logger.debug(f"[WrapperGreenConnectivity @ {host_id}] Thread exception: {e}")
                    results.wrapper_green_lost_connection_time_ns.set(perf_counter_ns())
                    break
        except Exception as e:
            self.logger.debug(f"[WrapperGreenConnectivity @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[WrapperGreenConnectivity @ {host_id}] Thread is completed.")

    # Green node
    # Check: DNS record presence
    # Expect: DNS record is deleted during/after switchover
    # Can terminate by itself
    def green_dns_monitor(
            self,
            host_id: str,
            host: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults):
        try:
            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)

            ip = socket.gethostbyname(host)
            self.logger.debug(f"[GreenDNS @ {host_id}] {host} -> {ip}")

            while not stop.is_set():
                sleep(1)

                try:
                    socket.gethostbyname(host)
                except socket.gaierror:
                    results.dns_green_removed_time_ns.set(perf_counter_ns())
                    break

        except Exception as e:
            self.logger.debug(f"[GreenDNS @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[GreenDNS @ {host_id}] Thread is completed.")

    # Green node
    # Check: connectivity (opening a new connection) with IAM when using node IP address
    # Expect: lose connectivity after green node changes its name (green prefix to no prefix)
    # Can terminate for itself
    def green_iam_connectivity_monitor(
            self,
            test_driver,
            conn_utils: ConnectionUtils,
            rds_client,
            host_id: str,
            thread_prefix: str,
            iam_token_host: str,
            connect_host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResults,
            result_queue: Deque[TimeHolder],
            notify_on_first_error: bool,
            exit_on_first_success: bool):
        conn = None
        try:
            test_env = TestEnvironment.get_current()
            iam_user = test_env.get_info().get_iam_user_name()
            green_ip = socket.gethostbyname(connect_host)
            connect_params = conn_utils.get_connect_params(host=green_ip, port=port, user=iam_user, dbname=db)
            connect_params[WrapperProperties.CONNECT_TIMEOUT_SEC.name] = 10
            connect_params[WrapperProperties.SOCKET_TIMEOUT_SEC.name] = 10

            sleep(1)

            # Notify that this thread is ready for work
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)
            self.logger.debug(
                f"[DirectGreenIamIp{thread_prefix} @ {host_id}] Starting connectivity monitoring {iam_token_host}")

            while not stop.is_set():
                token = rds_client.generate_db_auth_token(DBHostname=iam_token_host, port=port, DBUsername=iam_user)
                connect_params[WrapperProperties.PASSWORD.name] = token

                start_ns = perf_counter_ns()
                try:
                    target_driver_conn = DriverHelper.get_connect_func(test_driver)
                    conn = target_driver_conn(**connect_params)
                    end_ns = perf_counter_ns()
                    result_queue.append(TimeHolder(start_ns, end_ns))

                    if exit_on_first_success:
                        results.green_node_changed_name_time_ns.compare_and_set(0, perf_counter_ns())
                        self.logger.debug(
                            f"[DirectGreenIamIp{thread_prefix} @ {host_id}] Successfully connected. Exiting thread...")
                        return
                # TODO: do we need to handle the query timeout scenario like JDBC does for sqlTimeoutException?
                except Exception as e:
                    self.logger.debug(f"[DirectGreenIamIp{thread_prefix} @ {host_id}] Thread exception: {e}")
                    end_ns = perf_counter_ns()
                    result_queue.append(TimeHolder(start_ns, end_ns, error=str(e)))
                    # TODO: is 'Access Denied' the error message in Python as well as JDBC?
                    if notify_on_first_error and "access denied" in str(e).lower():
                        results.green_node_changed_name_time_ns.compare_and_set(0, perf_counter_ns())
                        self.logger.debug(
                            f"[DirectGreenIamIp{thread_prefix} @ {host_id}] "
                            f"Encountered first 'Access denied' exception. Exiting thread...")
                        return

                self.close_connection(conn)
                conn = None
                sleep(1)

        except Exception as e:
            self.logger.debug(f"[DirectGreenIamIp{thread_prefix} @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            self.close_connection(conn)
            finish_latch.count_down()
            self.logger.debug(f"[DirectGreenIamIp{thread_prefix} @ {host_id}] Thread is completed.")

    # Trigger BG switchover using RDS API
    # Can terminate for itself
    def bg_switchover_trigger(
            self,
            test_utility: RdsTestUtility,
            bg_id: str,
            start_latch: CountDownLatch,
            finish_latch: CountDownLatch,
            results: Dict[str, BlueGreenResults]):
        try:
            start_latch.count_down()

            # Wait until other threads are ready to start the test
            start_latch.wait_sec(5 * 60)

            sync_time_ns = perf_counter_ns()
            for result in results.values():
                result.threads_sync_time.set(sync_time_ns)

            sleep(30)
            test_utility.switchover_blue_green_deployment(bg_id)

            bg_trigger_time_ns = perf_counter_ns()
            for result in results.values():
                result.bg_trigger_time_ns.set(bg_trigger_time_ns)
        except Exception as e:
            self.logger.debug(f"[Switchover] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            finish_latch.count_down()
            self.logger.debug("[Switchover] Thread is completed.")

    def print_metrics(self, rds_utils: RdsUtils):
        bg_trigger_time_ns = next((result.bg_trigger_time_ns.get() for result in self.results.values()), None)
        assert bg_trigger_time_ns is not None, "Cannot get bg_trigger_time"

        table = []
        headers = [
            "Instance/endpoint",
            "Start time",
            "Threads sync",
            "direct Blue conn dropped (idle)",
            "direct Blue conn dropped (SELECT 1)",
            "wrapper Blue conn dropped (idle)",
            "wrapper Green conn dropped (SELECT 1)",
            "Blue DNS updated",
            "Green DNS removed",
            "Green node certificate change"
        ]

        def entry_green_comparator(result_entry: Tuple[str, BlueGreenResults]):
            return 1 if rds_utils.is_green_instance(result_entry[0] + ".") else 0

        def entry_name_comparator(result_entry: Tuple[str, BlueGreenResults]):
            rds_utils.remove_green_instance_prefix(result_entry[0]).lower()

        sorted_entries: List[Tuple[str, BlueGreenResults]] = sorted(
            self.results.items(),
            key=lambda result_entry: (
                entry_green_comparator(result_entry),
                entry_name_comparator(result_entry)
            )
        )

        if not sorted_entries:
            table.append(["No entries"])

        for entry in sorted_entries:
            results = entry[1]
            start_time_ms = (results.start_time_ns.get() - bg_trigger_time_ns) // 1_000_000
            threads_sync_time_ms = (results.threads_sync_time.get() - bg_trigger_time_ns) // 1_000_000
            direct_blue_idle_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(results.direct_blue_idle_lost_connection_time_ns, bg_trigger_time_ns))
            direct_blue_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(results.direct_blue_lost_connection_time_ns, bg_trigger_time_ns))
            wrapper_blue_idle_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(results.wrapper_blue_idle_lost_connection_time_ns, bg_trigger_time_ns))
            wrapper_green_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(results.wrapper_green_lost_connection_time_ns, bg_trigger_time_ns))
            dns_blue_changed_time_ms = (
                self.get_formatted_time_ns_to_ms(results.dns_blue_changed_time_ns, bg_trigger_time_ns))
            dns_green_removed_time_ms = (
                self.get_formatted_time_ns_to_ms(results.dns_green_removed_time_ns, bg_trigger_time_ns))
            green_node_changed_name_time_ms = (
                self.get_formatted_time_ns_to_ms(results.green_node_changed_name_time_ns, bg_trigger_time_ns))

            table.append([
                entry[0],
                start_time_ms,
                threads_sync_time_ms,
                direct_blue_idle_lost_connection_time_ms,
                direct_blue_lost_connection_time_ms,
                wrapper_blue_idle_lost_connection_time_ms,
                wrapper_green_lost_connection_time_ms,
                dns_blue_changed_time_ms,
                dns_green_removed_time_ms,
                green_node_changed_name_time_ms])

        self.logger.debug(f"\n{tabulate(table, headers=headers)}")

        for entry in sorted_entries:
            if not entry[1].blue_status_time and not entry[1].green_status_time:
                continue
            self.print_node_status_times(entry[0], entry[1], bg_trigger_time_ns)

        for entry in sorted_entries:
            if not entry[1].blue_wrapper_connect_times:
                continue
            self.print_duration_times(
                entry[0], "Wrapper connection time (ms) to Blue",
                entry[1].blue_wrapper_connect_times, bg_trigger_time_ns)

        for entry in sorted_entries:
            if not entry[1].green_direct_iam_ip_with_green_node_connect_times:
                continue
            self.print_duration_times(
                entry[0], "Wrapper IAM (green token) connection time (ms) to Green",
                entry[1].green_direct_iam_ip_with_green_node_connect_times, bg_trigger_time_ns)

        for entry in sorted_entries:
            if not entry[1].blue_wrapper_execute_times:
                continue
            self.print_duration_times(
                entry[0], "Wrapper execution time (ms) to Blue",
                entry[1].blue_wrapper_execute_times, bg_trigger_time_ns)

        for entry in sorted_entries:
            if not entry[1].green_wrapper_execute_times:
                continue
            self.print_duration_times(
                entry[0], "Wrapper execution time (ms) to Green",
                entry[1].green_wrapper_execute_times, bg_trigger_time_ns)

    def get_formatted_time_ns_to_ms(self, atomic_end_time_ns: AtomicInt, time_zero_ns: int) -> str:
        return "-" if atomic_end_time_ns.get() == 0 else f"{(atomic_end_time_ns.get() - time_zero_ns) // 1_000_000} ms"

    def print_node_status_times(self, node: str, results: BlueGreenResults, time_zero_ns: int):
        status_map: ConcurrentDict[str, int] = results.blue_status_time
        status_map.put_all(results.green_status_time)
        table = []
        headers = ["Status", "SOURCE", "TARGET"]
        sorted_status_names = [k for k, v in sorted(status_map.items(), key=lambda x: x[1])]
        for status in sorted_status_names:
            blue_status_time_ns = results.blue_status_time.get(status)
            if blue_status_time_ns:
                source_time_ms_str = f"{(blue_status_time_ns - time_zero_ns) // 1_000_000} ms"
            else:
                source_time_ms_str = ""

            green_status_time_ns = results.green_status_time.get(status)
            if green_status_time_ns:
                target_time_ms_str = f"{(green_status_time_ns - time_zero_ns) // 1_000_000} ms"
            else:
                target_time_ms_str = ""

            table.append([status, source_time_ms_str, target_time_ms_str])

        self.logger.debug(f"\n{node}:\n{tabulate(table, headers=headers)}")

    def print_duration_times(self, node: str, title: str, times: Deque[TimeHolder], time_zero_ns: int):
        table = []
        headers = ["Connect at (ms)", "Connect time/duration (ms)", "Error"]
        p99_ns = self.get_percentile([time.end_time_ns - time.start_time_ns for time in times], 99.0)
        p99_ms = p99_ns // 1_000_000
        table.append(["p99", p99_ms, ""])
        first_connect = times[0]
        table.append([
            (first_connect.start_time_ns - time_zero_ns) // 1_000_000,
            (first_connect.end_time_ns - first_connect.start_time_ns) // 1_000_000,
            self.get_formatted_error(first_connect.error)
        ])

        for time_holder in times:
            duration_ms = (time_holder.end_time_ns - time_holder.start_time_ns) // 1_000_000
            if duration_ms > p99_ms:
                table.append([
                    (time_holder.start_time_ns - time_zero_ns) // 1_000_000,
                    (time_holder.end_time_ns - time_holder.start_time_ns) // 1_000_000,
                    self.get_formatted_error(time_holder.error)
                ])

        last_connect = times[-1]
        table.append([
            (last_connect.start_time_ns - time_zero_ns) // 1_000_000,
            (last_connect.end_time_ns - last_connect.start_time_ns) // 1_000_000,
            self.get_formatted_error(last_connect.error)
        ])

        self.logger.debug(f"\n{node}: {title}\n{tabulate(table, headers=headers)}")

    def get_formatted_error(self, error: Optional[str]) -> str:
        return "" if error is None else error[0:min(len(error), 100)].replace("\n", " ") + "..."

    def get_percentile(self, input_data: List[int], percentile: float):
        if not input_data:
            return 0

        sorted_list = sorted(input_data)
        rank = 1 if percentile == 0 else math.ceil(percentile / 100.0 * len(input_data))
        return sorted_list[rank - 1]

    def log_unhandled_exceptions(self):
        for exception in self.unhandled_exceptions:
            self.logger.debug(f"Unhandled exception: {exception}")

    def assert_test(self):
        bg_trigger_time_ns = next((result.bg_trigger_time_ns.get() for result in self.results.values()), None)
        assert bg_trigger_time_ns is not None, "Cannot get bg_trigger_time"

        max_green_node_change_time_ms = max(
            (0 if result.green_node_change_name_time.get() == 0
             else (result.green_node_change_name_time.get() - bg_trigger_time_ns) // 1_000_000
             for result in self.results.values()),
            default=0
        )
        self.logger.debug(f"max_green_node_change_time: {max_green_node_change_time_ms} ms")

        switchover_complete_time_ms = max(
            (0 if x == 0
             else (x - bg_trigger_time_ns) // 1_000_000
             for result in self.results.values()
             if result.green_status_time
             for x in [result.green_status_time.get("SWITCHOVER_COMPLETED", 0)]),
            default=0
        )
        self.logger.debug(f"switchoverCompleteTime: {switchover_complete_time_ms} ms")

        # Assertions
        assert switchover_complete_time_ms != 0, "BG switchover hasn't completed."
        assert switchover_complete_time_ms >= max_green_node_change_time_ms, "Green node changed name after SWITCHOVER_COMPLETED."


@dataclass
class TimeHolder:
    start_time_ns: int
    end_time_ns: int
    hold_ns: int = 0
    error: Optional[str] = None


@dataclass
class BlueGreenResults:
    start_time_ns: AtomicInt = AtomicInt()
    threads_sync_time: AtomicInt = AtomicInt()
    bg_trigger_time_ns: AtomicInt = AtomicInt()
    direct_blue_lost_connection_time_ns: AtomicInt = AtomicInt()
    direct_blue_idle_lost_connection_time_ns: AtomicInt = AtomicInt()
    wrapper_blue_idle_lost_connection_time_ns: AtomicInt = AtomicInt()
    wrapper_green_lost_connection_time_ns: AtomicInt = AtomicInt()
    dns_blue_changed_time_ns: AtomicInt = AtomicInt()
    dns_blue_error: Optional[str] = None
    dns_green_removed_time_ns: AtomicInt = AtomicInt()
    green_node_changed_name_time_ns: AtomicInt = AtomicInt()
    blue_status_time: ConcurrentDict[str, int] = ConcurrentDict()
    green_status_time: ConcurrentDict[str, int] = ConcurrentDict()
    blue_wrapper_connect_times: Deque[TimeHolder] = deque()
    blue_wrapper_execute_times: Deque[TimeHolder] = deque()
    green_wrapper_execute_times: Deque[TimeHolder] = deque()
    green_direct_iam_ip_with_blue_node_connect_times: Deque[TimeHolder] = deque()
    green_direct_iam_ip_with_green_node_connect_times: Deque[TimeHolder] = deque()
