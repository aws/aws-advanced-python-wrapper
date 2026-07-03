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

"""Async twin of test_blue_green_deployment.py.

Exercises the bluegreen plugin on AsyncAwsWrapperConnection — connection
behaviour during the four BG phases (CREATED / PREPARATION / IN_PROGRESS /
POST_SWITCHOVER).

Translation notes vs the sync file:
- Each thread monitor owns ONE persistent event loop for its entire
  lifetime; all async work on that thread's connection (open, cursor
  operations, close, cleanup) is driven via ``loop.run_until_complete``.
  Driver connection state (psycopg AsyncConnection wait futures, aiomysql
  stream locks) binds to the loop the connection was opened on — repeated
  ``asyncio.run()`` would create a fresh loop each call and trigger
  ``RuntimeError: got Future attached to a different loop`` or silent
  hangs. See ``_run_worker_with_loop``.
- ``AwsWrapperConnection.connect(...)`` → ``AsyncAwsWrapperConnection.connect(...)``
  (awaited inside each thread's persistent loop).
- ``conn.cursor()`` sync context manager → ``conn.cursor()`` async context
  manager (``async with``); ``execute`` / ``fetchall`` / ``fetchone`` are
  awaited.
- ``conn.close()`` → ``await conn.close()``.
- ``is_closed`` for psycopg AsyncConnection uses the sync ``.closed``
  property; for aiomysql uses ``not .open``. See ``_is_conn_closed_async``.
- ``conn._unwrap(BlueGreenPlugin)`` → ``_unwrap_plugin_async(conn, BlueGreenPlugin)``
  (searches ``conn._plugin_manager._plugins`` directly because
  ``AsyncAwsWrapperConnection`` does not expose ``_unwrap``).
- ``DriverHelper.get_connect_func`` returns the async connect callable for
  PG_ASYNC / MYSQL_ASYNC drivers.
- boto3 RDS Blue/Green switchover/wait calls stay synchronous.
- ``sleep()`` calls inside threads remain synchronous (OS threads; no event
  loop blocked).
- Every thread teardown invokes ``cleanup_async()`` on its own loop so
  wrapper background tasks are released cleanly.
"""

from __future__ import annotations

import asyncio
import math
import socket
from collections import deque
from dataclasses import dataclass, field
from threading import Event, Thread
from time import perf_counter_ns, sleep
from typing import (TYPE_CHECKING, Any, Callable, Deque, Dict, List, Optional,
                    Tuple, Type)

import psycopg
import pytest
from tabulate import tabulate  # type: ignore[import-untyped]

from aws_advanced_python_wrapper.aio.wrapper import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.blue_green_plugin import (BlueGreenPlugin,
                                                           BlueGreenRole)
from aws_advanced_python_wrapper.database_dialect import DialectCode
from aws_advanced_python_wrapper.driver_info import DriverInfo
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.concurrent import (ConcurrentDict,
                                                          CountDownLatch)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from tests.integration.container.utils.async_connection_helpers import \
    cleanup_async
from tests.integration.container.utils.conditions import (
    enable_on_deployments, enable_on_features)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from tests.integration.container.utils.connection_utils import \
        ConnectionUtils
    from tests.integration.container.utils.test_driver import TestDriver


# ---------------------------------------------------------------------------
# Module-level async helpers
# ---------------------------------------------------------------------------

def _unwrap_plugin_async(
        conn: AsyncAwsWrapperConnection,
        plugin_class: Type[Any]) -> Optional[Any]:
    """Replicate ``AwsWrapperConnection._unwrap`` for async connections.

    ``AsyncAwsWrapperConnection`` does not expose ``_unwrap``; we walk
    ``_plugin_manager._plugins`` directly (same logic as the sync
    ``PluginService._unwrap``).
    """
    for plugin in conn._plugin_manager._plugins:
        if isinstance(plugin, plugin_class):
            return plugin
    return None


def _is_conn_closed_async(conn: Any) -> bool:
    """Return True if the connection (raw or wrapped) appears closed.

    For psycopg's async connection the ``.closed`` property is sync.
    For aiomysql the ``.open`` property is sync (closed = not open).
    For ``AsyncAwsWrapperConnection`` the ``__getattr__`` proxy forwards
    attribute access to the underlying driver connection, so the same
    checks work on the wrapper object.
    """
    if isinstance(conn, AsyncAwsWrapperConnection):
        target = conn._target_conn
    else:
        target = conn

    if isinstance(target, psycopg.AsyncConnection):
        return bool(target.closed)
    # aiomysql.Connection
    if hasattr(target, "open"):
        return not bool(target.open)
    # Fallback: treat as closed if we cannot determine
    return True


# ---------------------------------------------------------------------------
# Per-thread async helpers  (driven via loop.run_until_complete in worker
# threads that own a persistent event loop; see ``_run_worker_with_loop``).
# ---------------------------------------------------------------------------

async def _open_direct_conn_async(
        test_driver: TestDriver,
        connect_params: Dict[str, Any]) -> Any:
    """Open a raw async driver connection (no wrapper)."""
    connect_func = DriverHelper.get_connect_func(test_driver)
    return await connect_func(**connect_params)


async def _open_wrapper_conn_async(
        test_driver: TestDriver,
        connect_params: Dict[str, Any]) -> AsyncAwsWrapperConnection:
    """Open an AsyncAwsWrapperConnection."""
    connect_func = DriverHelper.get_connect_func(test_driver)
    return await AsyncAwsWrapperConnection.connect(connect_func, **connect_params)


async def _close_conn_async(conn: Any) -> None:
    """Close an async connection (raw or wrapped)."""
    try:
        if conn is not None and not _is_conn_closed_async(conn):
            await conn.close()
    except Exception:
        pass


def _run_worker_with_loop(
        body: Callable[[asyncio.AbstractEventLoop], None]) -> None:
    """Drive ``body`` on a persistent event loop owned by the current thread.

    Async driver connections (psycopg AsyncConnection, aiomysql.Connection)
    bind internal state (wait futures, stream locks) to the event loop they
    were created on. Repeatedly calling ``asyncio.run()`` inside a worker
    thread creates a fresh loop per call and breaks that binding, producing
    ``RuntimeError: got Future attached to a different loop`` or silent
    hangs.

    This helper encapsulates the correct shape: one loop per thread for
    the thread's entire lifetime, with ``cleanup_async()`` always invoked
    on that same loop in ``finally`` so wrapper background tasks are
    released even if the body raises.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        try:
            body(loop)
        finally:
            try:
                loop.run_until_complete(cleanup_async())
            except Exception:
                pass
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Data classes (shared with sync; no change needed)
# ---------------------------------------------------------------------------

@dataclass
class TimeHolder:
    start_time_ns: int
    end_time_ns: int
    hold_ns: int = 0
    error: Optional[str] = None


@dataclass
class BlueGreenResultsAsync:
    start_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    threads_sync_time: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    bg_trigger_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    direct_blue_lost_connection_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    direct_blue_idle_lost_connection_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    wrapper_blue_idle_lost_connection_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    wrapper_green_lost_connection_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    dns_blue_changed_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    dns_blue_error: Optional[str] = None
    dns_green_removed_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    green_node_changed_name_time_ns: AtomicInt = field(default_factory=lambda: AtomicInt(0))
    blue_status_time: ConcurrentDict = field(default_factory=ConcurrentDict)
    green_status_time: ConcurrentDict = field(default_factory=ConcurrentDict)
    blue_wrapper_connect_times: Deque[TimeHolder] = field(default_factory=deque)
    blue_wrapper_execute_times: Deque[TimeHolder] = field(default_factory=deque)
    green_wrapper_execute_times: Deque[TimeHolder] = field(default_factory=deque)
    green_direct_iam_ip_with_blue_node_connect_times: Deque[TimeHolder] = field(default_factory=deque)
    green_direct_iam_ip_with_green_node_connect_times: Deque[TimeHolder] = field(default_factory=deque)


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE])
@enable_on_features([TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT])
class TestBlueGreenDeploymentAsync:
    logger = Logger(__name__)

    INCLUDE_CLUSTER_ENDPOINTS = False
    INCLUDE_WRITER_AND_READER_ONLY = False
    TEST_CLUSTER_ID = "test-cluster-id-async"
    MYSQL_BG_STATUS_QUERY = \
        ("SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) as hostId, endpoint, port, role, status, version "
         "FROM mysql.rds_topology")
    PG_AURORA_BG_STATUS_QUERY = \
        ("SELECT id, SPLIT_PART(endpoint, '.', 1) as hostId, endpoint, port, role, status, version "
         "FROM pg_catalog.get_blue_green_fast_switchover_metadata('aws_advanced_python_wrapper')")
    PG_RDS_BG_STATUS_QUERY = \
        (f"SELECT id, SPLIT_PART(endpoint, '.', 1) as hostId, endpoint, port, role, status, version "
         f"FROM rds_tools.show_topology('aws_advanced_python_wrapper-{DriverInfo.DRIVER_VERSION}')")
    results: ConcurrentDict = ConcurrentDict()
    unhandled_exceptions: Deque[Exception] = deque()

    @pytest.fixture(scope='class')
    def test_utility(self):
        return RdsTestUtility.get_utility()

    @pytest.fixture(scope='class')
    def rds_utils(self):
        return RdsUtils()

    def test_switchover_async(
            self, conn_utils: ConnectionUtils, test_utility: RdsTestUtility,
            rds_utils: RdsUtils, test_environment: TestEnvironment,
            test_driver: TestDriver):
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
        db_name = conn_utils.dbname
        test_instance = env.get_writer()
        topology_instances: List[str] = self.get_bg_endpoints(
            test_environment, test_utility, rds_utils, info.get_bg_deployment_id())
        topology_instances_str = '\n'.join(topology_instances)
        self.logger.debug(f"topology_instances: \n{topology_instances_str}")

        for host in topology_instances:
            host_id = host[0:host.index(".")]
            assert host_id

            bg_results = BlueGreenResultsAsync()
            self.results.put(host_id, bg_results)

            if rds_utils.is_not_green_or_old_instance(host):
                threads.append(Thread(
                    target=self.direct_topology_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.direct_blue_connectivity_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.direct_blue_idle_connectivity_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_blue_idle_connectivity_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_blue_executing_connectivity_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_blue_new_connection_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.blue_dns_monitor,
                    args=(host_id, host, start_latch, stop, finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

            if rds_utils.is_green_instance(host):
                threads.append(Thread(
                    target=self.direct_topology_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.wrapper_green_connectivity_monitor_async,
                    args=(test_driver, conn_utils, host_id, host, test_instance.get_port(), db_name, start_latch, stop,
                          finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                threads.append(Thread(
                    target=self.green_dns_monitor,
                    args=(host_id, host, start_latch, stop, finish_latch, bg_results)))
                thread_count += 1
                thread_finish_count += 1

                if iam_enabled:
                    rds_client = test_utility.get_rds_client()

                    threads.append(Thread(
                        target=self.green_iam_connectivity_monitor_async,
                        args=(test_driver, conn_utils, rds_client, host_id, "BlueHostToken",
                              rds_utils.remove_green_instance_prefix(host), host, test_instance.get_port(),
                              db_name, start_latch, stop, finish_latch, bg_results,
                              bg_results.green_direct_iam_ip_with_blue_node_connect_times, False, True)))
                    thread_count += 1
                    thread_finish_count += 1

                    threads.append(Thread(
                        target=self.green_iam_connectivity_monitor_async,
                        args=(test_driver, conn_utils, rds_client, host_id, "GreenHostToken", host, host,
                              test_instance.get_port(), db_name, start_latch, stop, finish_latch,
                              bg_results, bg_results.green_direct_iam_ip_with_green_node_connect_times, True, False)
                    ))
                    thread_count += 1
                    thread_finish_count += 1

        threads.append(Thread(
            target=self.bg_switchover_trigger,
            args=(test_utility, info.get_bg_deployment_id(), start_latch, finish_latch, self.results)))
        thread_count += 1
        thread_finish_count += 1

        start_latch.set_count(thread_count)
        finish_latch.set_count(thread_finish_count)

        for result in self.results.values():
            result.start_time_ns.set(start_time_ns)

        for thread in threads:
            thread.start()

        self.logger.debug("All threads started.")

        finish_latch.wait_sec(6 * 60)
        self.logger.debug("All threads completed.")

        sleep(12 * 60)

        self.logger.debug("Stopping all threads...")
        stop.set()

        for thread in threads:
            thread.join(timeout=30)
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

        if test_env.get_deployment() == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE:
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

    def get_direct_connection_with_retry_async(
            self, test_driver: TestDriver, **connect_params: Any) -> Any:
        """Open a raw async driver connection with up to 10 retries.

        Runs its own event loop (called from an OS thread, not a coroutine).
        """
        conn = None
        connect_count = 0
        while conn is None and connect_count < 10:
            try:
                conn = asyncio.run(_open_direct_conn_async(test_driver, connect_params))
            except Exception:
                pass
            connect_count += 1

        if conn is None:
            pytest.fail(f"Cannot connect to {connect_params.get('host')}")

        return conn

    def get_wrapper_connection_with_retry_async(
            self, test_driver: TestDriver, **connect_params: Any) -> AsyncAwsWrapperConnection:
        """Open an AsyncAwsWrapperConnection with up to 10 retries.

        Runs its own event loop (called from an OS thread, not a coroutine).
        """
        conn = None
        connect_count = 0
        while conn is None and connect_count < 10:
            try:
                conn = asyncio.run(_open_wrapper_conn_async(test_driver, connect_params))
            except Exception:
                pass
            connect_count += 1

        if conn is None:
            pytest.fail(f"Cannot connect to {connect_params.get('host')}")

        return conn

    def close_connection_async(
            self,
            conn: Any,
            loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """Close an async connection from a thread context.

        ``loop`` must be the same event loop the connection was opened on
        (the thread's persistent loop). Falls back to ``asyncio.run`` only
        when no loop is supplied (e.g., the retry helpers that run entirely
        on their own transient loop); callers inside a monitor thread
        body MUST pass their persistent loop.
        """
        try:
            if conn is not None and not _is_conn_closed_async(conn):
                if loop is not None:
                    loop.run_until_complete(_close_conn_async(conn))
                else:
                    asyncio.run(_close_conn_async(conn))
        except Exception:
            pass

    def get_wrapper_connect_params(
            self, conn_utils: ConnectionUtils, host: str, port: int, db: str) -> Dict[str, Any]:
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
        elif db_deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE:
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

    def is_timeout_exception(self, exception: Exception) -> bool:
        error_message = str(exception).lower()
        timeout_keywords = [
            "timeout", "timed out", "statement timeout",
            "query execution was interrupted", "canceling statement due to",
            "connection timed out", "lost connection", "terminated"
        ]

        if any(keyword in error_message for keyword in timeout_keywords):
            return True

        if isinstance(exception, psycopg.Error):
            if "canceling statement due to statement timeout" in error_message:
                return True

        # aiomysql / mysql error codes via errno attribute
        if hasattr(exception, 'errno') and exception.errno in (1205, 2013, 2006):
            return True

        return False

    # ------------------------------------------------------------------
    # Thread monitors (async variants)
    # ------------------------------------------------------------------

    def direct_topology_monitor_async(
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
            results: BlueGreenResultsAsync) -> None:
        """Monitor BG status changes; can terminate for itself."""
        test_env = TestEnvironment.get_current()
        engine = test_env.get_engine()

        query = None
        if engine == DatabaseEngine.MYSQL:
            query = self.MYSQL_BG_STATUS_QUERY
        elif engine == DatabaseEngine.PG:
            db_deployment = test_env.get_deployment()
            if db_deployment == DatabaseEngineDeployment.AURORA:
                query = self.PG_AURORA_BG_STATUS_QUERY
            elif db_deployment == DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE:
                query = self.PG_RDS_BG_STATUS_QUERY
            else:
                pytest.fail(f"Unsupported blue/green database engine deployment: {db_deployment}")
        else:
            pytest.fail(f"Unsupported database engine: {engine}")

        async def _execute_topology(c: Any) -> None:
            async with c.cursor() as cursor:
                await cursor.execute(query)
                async for record in cursor:
                    role = record[4]
                    status = record[5]
                    version = record[6]
                    is_green = BlueGreenRole.parse_role(role, version) == BlueGreenRole.TARGET

                    def _log_and_return_time(_: Any) -> int:
                        self.logger.debug(f"[AsyncDirectTopology @ {host_id}] Status changed to: {status}.")
                        return perf_counter_ns()

                    if is_green:
                        results.green_status_time.compute_if_absent(status, _log_and_return_time)
                    else:
                        results.blue_status_time.compute_if_absent(status, _log_and_return_time)

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Any = None
            try:
                conn = loop.run_until_complete(_open_direct_conn_async(
                    test_driver, conn_utils.get_connect_params(host=host, port=port, dbname=db)))
                self.logger.debug(f"[AsyncDirectTopology @ {host_id}] Connection opened.")
                sleep(1)

                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(f"[AsyncDirectTopology @ {host_id}] Starting BG status monitoring.")

                end_time_ns = perf_counter_ns() + 15 * 60 * 1_000_000_000
                while not stop.is_set() and perf_counter_ns() < end_time_ns:
                    if conn is None:
                        try:
                            conn = loop.run_until_complete(_open_direct_conn_async(
                                test_driver, conn_utils.get_connect_params(host=host, port=port, dbname=db)))
                            self.logger.debug(f"[AsyncDirectTopology @ {host_id}] Connection re-opened.")
                        except Exception:
                            sleep(0.1)
                            continue

                    try:
                        loop.run_until_complete(_execute_topology(conn))
                        sleep(0.1)
                    except Exception as e:
                        self.logger.debug(f"[AsyncDirectTopology @ {host_id}] Thread exception: {e}.")
                        self.close_connection_async(conn, loop)
                        conn = None
            except Exception as e:
                self.logger.debug(f"[AsyncDirectTopology @ {host_id}] Thread unhandled exception: {e}.")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncDirectTopology @ {host_id}] Thread is completed.")

    def direct_blue_connectivity_monitor_async(
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
            results: BlueGreenResultsAsync) -> None:
        """Blue node: check connectivity with SELECT 1; can terminate for itself."""

        async def _open() -> Any:
            return await _open_direct_conn_async(
                test_driver, conn_utils.get_connect_params(host=host, port=port, dbname=db))

        async def _select1(c: Any) -> None:
            async with c.cursor() as cursor:
                await cursor.execute("SELECT 1")
                await cursor.fetchall()

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Any = None
            try:
                conn = loop.run_until_complete(_open())
                self.logger.debug(f"[AsyncDirectBlueConnectivity @ {host_id}] Connection opened.")

                sleep(1)
                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(
                    f"[AsyncDirectBlueConnectivity @ {host_id}] Starting connectivity monitoring.")

                while not stop.is_set():
                    try:
                        loop.run_until_complete(_select1(conn))
                        sleep(1)
                    except Exception as e:
                        self.logger.debug(
                            f"[AsyncDirectBlueConnectivity @ {host_id}] Thread exception: {e}")
                        results.direct_blue_lost_connection_time_ns.set(perf_counter_ns())
                        break
            except Exception as e:
                self.logger.debug(
                    f"[AsyncDirectBlueConnectivity @ {host_id}] Thread unhandled exception: {e}")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncDirectBlueConnectivity @ {host_id}] Thread is completed.")

    def direct_blue_idle_connectivity_monitor_async(
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
            results: BlueGreenResultsAsync) -> None:
        """Blue node: check connectivity via is_closed; can terminate for itself."""

        async def _open() -> Any:
            return await _open_direct_conn_async(
                test_driver, conn_utils.get_connect_params(host=host, port=port, dbname=db))

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Any = None
            try:
                conn = loop.run_until_complete(_open())
                self.logger.debug(f"[AsyncDirectBlueIdleConnectivity @ {host_id}] Connection opened.")

                sleep(1)
                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(
                    f"[AsyncDirectBlueIdleConnectivity @ {host_id}] Starting connectivity monitoring.")

                while not stop.is_set():
                    try:
                        if _is_conn_closed_async(conn):
                            results.direct_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                            break
                        sleep(1)
                    except Exception as e:
                        self.logger.debug(
                            f"[AsyncDirectBlueIdleConnectivity @ {host_id}] Thread exception: {e}")
                        results.direct_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                        break
            except Exception as e:
                self.logger.debug(
                    f"[AsyncDirectBlueIdleConnectivity @ {host_id}] Thread unhandled exception: {e}")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncDirectBlueIdleConnectivity @ {host_id}] Thread is completed.")

    def wrapper_blue_idle_connectivity_monitor_async(
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
            results: BlueGreenResultsAsync) -> None:
        """Blue node (wrapper): check connectivity via is_closed; can terminate for itself."""

        async def _open() -> AsyncAwsWrapperConnection:
            return await _open_wrapper_conn_async(
                test_driver, self.get_wrapper_connect_params(conn_utils, host, port, db))

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Optional[AsyncAwsWrapperConnection] = None
            try:
                conn = loop.run_until_complete(_open())
                self.logger.debug(f"[AsyncWrapperBlueIdleConnectivity @ {host_id}] Connection opened.")

                sleep(1)
                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(
                    f"[AsyncWrapperBlueIdleConnectivity @ {host_id}] Starting connectivity monitoring.")

                while not stop.is_set():
                    try:
                        if _is_conn_closed_async(conn):
                            results.wrapper_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                            break
                        sleep(1)
                    except Exception as e:
                        self.logger.debug(
                            f"[AsyncWrapperBlueIdleConnectivity @ {host_id}] Thread exception: {e}")
                        results.direct_blue_idle_lost_connection_time_ns.set(perf_counter_ns())
                        break
            except Exception as e:
                self.logger.debug(
                    f"[AsyncWrapperBlueIdleConnectivity @ {host_id}] Thread unhandled exception: {e}")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncWrapperBlueIdleConnectivity @ {host_id}] Thread is completed.")

    def wrapper_blue_executing_connectivity_monitor_async(
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
            results: BlueGreenResultsAsync) -> None:
        """Blue node (wrapper): check connectivity with sleep query; can terminate for itself."""
        query = None
        test_env = TestEnvironment.get_current()
        engine = test_env.get_engine()
        if engine == DatabaseEngine.MYSQL:
            query = "SELECT sleep(5)"
        elif engine == DatabaseEngine.PG:
            query = "SELECT pg_catalog.pg_sleep(5)"
        else:
            pytest.fail(f"Unsupported database engine: {engine}")

        connect_params = self.get_wrapper_connect_params(conn_utils, host, port, db)

        async def _open() -> AsyncAwsWrapperConnection:
            return await _open_wrapper_conn_async(test_driver, connect_params)

        async def _execute_sleep(c: AsyncAwsWrapperConnection) -> None:
            async with c.cursor() as cursor:
                await cursor.execute(query)
                await cursor.fetchall()

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Optional[AsyncAwsWrapperConnection] = None
            try:
                conn = loop.run_until_complete(_open())
                bg_plugin: Optional[BlueGreenPlugin] = _unwrap_plugin_async(conn, BlueGreenPlugin)
                assert bg_plugin is not None, \
                    f"Unable to find blue/green plugin in wrapper connection for {host}."
                self.logger.debug(f"[AsyncWrapperBlueExecute @ {host_id}] Connection opened.")

                sleep(1)
                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(
                    f"[AsyncWrapperBlueExecute @ {host_id}] Starting connectivity monitoring.")

                while not stop.is_set():
                    start_time_ns = perf_counter_ns()
                    try:
                        loop.run_until_complete(_execute_sleep(conn))
                        end_time_ns = perf_counter_ns()
                        results.blue_wrapper_execute_times.append(
                            TimeHolder(start_time_ns, end_time_ns, bg_plugin.get_hold_time_ns()))
                    except Exception as e:
                        results.blue_wrapper_execute_times.append(
                            TimeHolder(start_time_ns, perf_counter_ns(),
                                       bg_plugin.get_hold_time_ns(), str(e)))
                        if _is_conn_closed_async(conn):
                            break
                    sleep(1)
            except Exception as e:
                self.logger.debug(f"[AsyncWrapperBlueExecute @ {host_id}] Thread unhandled exception: {e}")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncWrapperBlueExecute @ {host_id}] Thread is completed.")

    def wrapper_blue_new_connection_monitor_async(
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
            results: BlueGreenResultsAsync) -> None:
        """Blue node (wrapper): check new connection open time; needs stop signal."""
        connect_params = self.get_wrapper_connect_params(conn_utils, host, port, db)

        async def _open() -> AsyncAwsWrapperConnection:
            return await _open_wrapper_conn_async(test_driver, connect_params)

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Optional[AsyncAwsWrapperConnection] = None
            try:
                sleep(1)
                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(
                    f"[AsyncWrapperBlueNewConnection @ {host_id}] Starting connectivity monitoring.")

                while not stop.is_set():
                    start_time_ns = perf_counter_ns()

                    try:
                        conn = loop.run_until_complete(_open())
                        end_time_ns = perf_counter_ns()
                        bg_plugin: Optional[BlueGreenPlugin] = _unwrap_plugin_async(conn, BlueGreenPlugin)
                        assert bg_plugin is not None, \
                            f"Unable to find blue/green plugin in wrapper connection for {host}."

                        results.blue_wrapper_connect_times.append(
                            TimeHolder(start_time_ns, end_time_ns, bg_plugin.get_hold_time_ns()))
                    except Exception as e:
                        if self.is_timeout_exception(e):
                            self.logger.debug(
                                f"[AsyncWrapperBlueNewConnection @ {host_id}] Thread timeout exception: {e}")
                        else:
                            self.logger.debug(
                                f"[AsyncWrapperBlueNewConnection @ {host_id}] Thread exception: {e}")

                        end_time_ns = perf_counter_ns()
                        if conn is not None:
                            bg_plugin = _unwrap_plugin_async(conn, BlueGreenPlugin)
                            assert bg_plugin is not None, \
                                f"Unable to find blue/green plugin in wrapper connection for {host}."
                            results.blue_wrapper_connect_times.append(
                                TimeHolder(start_time_ns, end_time_ns,
                                           bg_plugin.get_hold_time_ns(), str(e)))
                        else:
                            results.blue_wrapper_connect_times.append(
                                TimeHolder(start_time_ns, end_time_ns, error=str(e)))

                    self.close_connection_async(conn, loop)
                    conn = None
                    sleep(1)

            except Exception as e:
                self.logger.debug(
                    f"[AsyncWrapperBlueNewConnection @ {host_id}] Thread unhandled exception: {e}")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncWrapperBlueNewConnection @ {host_id}] Thread is completed.")

    # Blue / Green DNS monitors are purely sync (socket calls); reuse verbatim.

    def blue_dns_monitor(
            self,
            host_id: str,
            host: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResultsAsync) -> None:
        try:
            start_latch.count_down()
            start_latch.wait_sec(5 * 60)

            original_ip = socket.gethostbyname(host)
            self.logger.debug(f"[AsyncBlueDNS @ {host_id}] {host} -> {original_ip}")

            while not stop.is_set():
                sleep(1)
                try:
                    current_ip = socket.gethostbyname(host)
                    if current_ip != original_ip:
                        results.dns_blue_changed_time_ns.set(perf_counter_ns())
                        self.logger.debug(f"[AsyncBlueDNS @ {host_id}] {host} -> {current_ip}")
                        break
                except socket.gaierror as e:
                    self.logger.debug(f"[AsyncBlueDNS @ {host_id}] Error: {e}")
                    results.dns_blue_error = str(e)
                    results.dns_blue_changed_time_ns.set(perf_counter_ns())
                    break

        except Exception as e:
            self.logger.debug(f"[AsyncBlueDNS @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncBlueDNS @ {host_id}] Thread is completed.")

    def wrapper_green_connectivity_monitor_async(
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
            results: BlueGreenResultsAsync) -> None:
        """Green node (wrapper): check connectivity with SELECT 1; can terminate for itself."""

        async def _open() -> AsyncAwsWrapperConnection:
            return await _open_wrapper_conn_async(
                test_driver, self.get_wrapper_connect_params(conn_utils, host, port, db))

        async def _select1(c: AsyncAwsWrapperConnection) -> None:
            async with c.cursor() as cursor:
                await cursor.execute("SELECT 1")
                await cursor.fetchall()

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Optional[AsyncAwsWrapperConnection] = None
            try:
                conn = loop.run_until_complete(_open())
                self.logger.debug(f"[AsyncWrapperGreenConnectivity @ {host_id}] Connection opened.")

                bg_plugin: Optional[BlueGreenPlugin] = _unwrap_plugin_async(conn, BlueGreenPlugin)
                assert bg_plugin is not None, \
                    f"Unable to find blue/green plugin in wrapper connection for {host}."

                sleep(1)
                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(
                    f"[AsyncWrapperGreenConnectivity @ {host_id}] Starting connectivity monitoring.")

                start_time_ns = perf_counter_ns()
                while not stop.is_set():
                    try:
                        start_time_ns = perf_counter_ns()
                        loop.run_until_complete(_select1(conn))
                        end_time_ns = perf_counter_ns()
                        results.green_wrapper_execute_times.append(
                            TimeHolder(start_time_ns, end_time_ns, bg_plugin.get_hold_time_ns()))
                        sleep(1)
                    except Exception as e:
                        if self.is_timeout_exception(e):
                            self.logger.debug(
                                f"[AsyncWrapperGreenConnectivity @ {host_id}] Thread timeout exception: {e}")
                            results.green_wrapper_execute_times.append(
                                TimeHolder(start_time_ns, perf_counter_ns(),
                                           bg_plugin.get_hold_time_ns(), str(e)))
                            if _is_conn_closed_async(conn):
                                results.wrapper_green_lost_connection_time_ns.set(perf_counter_ns())
                                break
                        else:
                            self.logger.debug(
                                f"[AsyncWrapperGreenConnectivity @ {host_id}] Thread exception: {e}")
                            results.wrapper_green_lost_connection_time_ns.set(perf_counter_ns())
                            break
            except Exception as e:
                self.logger.debug(
                    f"[AsyncWrapperGreenConnectivity @ {host_id}] Thread unhandled exception: {e}")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncWrapperGreenConnectivity @ {host_id}] Thread is completed.")

    def green_dns_monitor(
            self,
            host_id: str,
            host: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResultsAsync) -> None:
        try:
            start_latch.count_down()
            start_latch.wait_sec(5 * 60)

            ip = socket.gethostbyname(host)
            self.logger.debug(f"[AsyncGreenDNS @ {host_id}] {host} -> {ip}")

            while not stop.is_set():
                sleep(1)
                try:
                    socket.gethostbyname(host)
                except socket.gaierror:
                    results.dns_green_removed_time_ns.set(perf_counter_ns())
                    break

        except Exception as e:
            self.logger.debug(f"[AsyncGreenDNS @ {host_id}] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            finish_latch.count_down()
            self.logger.debug(f"[AsyncGreenDNS @ {host_id}] Thread is completed.")

    def green_iam_connectivity_monitor_async(
            self,
            test_driver: TestDriver,
            conn_utils: ConnectionUtils,
            rds_client: Any,
            host_id: str,
            thread_prefix: str,
            iam_token_host: str,
            connect_host: str,
            port: int,
            db: str,
            start_latch: CountDownLatch,
            stop: Event,
            finish_latch: CountDownLatch,
            results: BlueGreenResultsAsync,
            result_queue: Deque[TimeHolder],
            notify_on_first_error: bool,
            exit_on_first_success: bool) -> None:
        """Green node: IAM connectivity with IP address; can terminate for itself."""

        async def _open_direct(params: Dict[str, Any]) -> Any:
            return await _open_direct_conn_async(test_driver, params)

        def _body(loop: asyncio.AbstractEventLoop) -> None:
            conn: Any = None
            try:
                test_env = TestEnvironment.get_current()
                iam_user = test_env.get_info().get_iam_user_name()
                green_ip = socket.gethostbyname(connect_host)
                connect_params = conn_utils.get_connect_params(
                    host=green_ip, port=port, user=iam_user, dbname=db)
                connect_params[WrapperProperties.CONNECT_TIMEOUT_SEC.name] = 10
                if test_env.get_engine() == DatabaseEngine.MYSQL:
                    connect_params["auth_plugin"] = "mysql_clear_password"

                sleep(1)
                start_latch.count_down()
                start_latch.wait_sec(5 * 60)
                self.logger.debug(
                    f"[AsyncDirectGreenIamIp{thread_prefix} @ {host_id}] "
                    f"Starting connectivity monitoring {iam_token_host}")

                while not stop.is_set():
                    token = rds_client.generate_db_auth_token(
                        DBHostname=iam_token_host, Port=port, DBUsername=iam_user)
                    connect_params[WrapperProperties.PASSWORD.name] = token

                    start_ns = perf_counter_ns()
                    try:
                        conn = loop.run_until_complete(_open_direct(connect_params))
                        end_ns = perf_counter_ns()
                        result_queue.append(TimeHolder(start_ns, end_ns))

                        if exit_on_first_success:
                            results.green_node_changed_name_time_ns.compare_and_set(
                                0, perf_counter_ns())
                            self.logger.debug(
                                f"[AsyncDirectGreenIamIp{thread_prefix} @ {host_id}] "
                                f"Successfully connected. Exiting thread...")
                            return
                    except Exception as e:
                        if self.is_timeout_exception(e):
                            self.logger.debug(
                                f"[AsyncDirectGreenIamIp{thread_prefix} @ {host_id}] "
                                f"Thread exception: {e}")
                            result_queue.append(TimeHolder(start_ns, perf_counter_ns(), error=str(e)))
                        else:
                            self.logger.debug(
                                f"[AsyncDirectGreenIamIp{thread_prefix} @ {host_id}] "
                                f"Thread exception: {e}")
                            result_queue.append(TimeHolder(start_ns, perf_counter_ns(), error=str(e)))
                            if notify_on_first_error and "access denied" in str(e).lower():
                                results.green_node_changed_name_time_ns.compare_and_set(
                                    0, perf_counter_ns())
                                self.logger.debug(
                                    f"[AsyncDirectGreenIamIp{thread_prefix} @ {host_id}] "
                                    f"Encountered first 'Access denied' exception. Exiting thread...")
                                return

                    self.close_connection_async(conn, loop)
                    conn = None
                    sleep(1)

            except Exception as e:
                self.logger.debug(
                    f"[AsyncDirectGreenIamIp{thread_prefix} @ {host_id}] "
                    f"Thread unhandled exception: {e}")
                self.unhandled_exceptions.append(e)
            finally:
                self.close_connection_async(conn, loop)

        try:
            _run_worker_with_loop(_body)
        finally:
            finish_latch.count_down()
            self.logger.debug(
                f"[AsyncDirectGreenIamIp{thread_prefix} @ {host_id}] Thread is completed.")

    def bg_switchover_trigger(
            self,
            test_utility: RdsTestUtility,
            bg_id: str,
            start_latch: CountDownLatch,
            finish_latch: CountDownLatch,
            results: Any) -> None:
        """Trigger BG switchover using RDS API (synchronous boto3 calls)."""
        try:
            start_latch.count_down()
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
            self.logger.debug(f"[AsyncSwitchover] Thread unhandled exception: {e}")
            self.unhandled_exceptions.append(e)
        finally:
            finish_latch.count_down()
            self.logger.debug("[AsyncSwitchover] Thread is completed.")

    # ------------------------------------------------------------------
    # Metrics / assertions
    # ------------------------------------------------------------------

    def print_metrics(self, rds_utils: RdsUtils) -> None:
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

        def entry_green_comparator(result_entry: Tuple[str, BlueGreenResultsAsync]) -> int:
            return 1 if rds_utils.is_green_instance(result_entry[0] + ".") else 0

        def entry_name_comparator(result_entry: Tuple[str, BlueGreenResultsAsync]) -> Optional[str]:
            return rds_utils.remove_green_instance_prefix(result_entry[0]).lower()

        sorted_entries: List[Tuple[str, BlueGreenResultsAsync]] = sorted(
            self.results.items(),
            key=lambda result_entry: (
                entry_green_comparator(result_entry),
                entry_name_comparator(result_entry)
            )
        )

        if not sorted_entries:
            table.append(["No entries"])

        for entry in sorted_entries:
            result = entry[1]
            start_time_ms = (result.start_time_ns.get() - bg_trigger_time_ns) // 1_000_000
            threads_sync_time_ms = (result.threads_sync_time.get() - bg_trigger_time_ns) // 1_000_000
            direct_blue_idle_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(result.direct_blue_idle_lost_connection_time_ns, bg_trigger_time_ns))
            direct_blue_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(result.direct_blue_lost_connection_time_ns, bg_trigger_time_ns))
            wrapper_blue_idle_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(result.wrapper_blue_idle_lost_connection_time_ns, bg_trigger_time_ns))
            wrapper_green_lost_connection_time_ms = (
                self.get_formatted_time_ns_to_ms(result.wrapper_green_lost_connection_time_ns, bg_trigger_time_ns))
            dns_blue_changed_time_ms = (
                self.get_formatted_time_ns_to_ms(result.dns_blue_changed_time_ns, bg_trigger_time_ns))
            dns_green_removed_time_ms = (
                self.get_formatted_time_ns_to_ms(result.dns_green_removed_time_ns, bg_trigger_time_ns))
            green_node_changed_name_time_ms = (
                self.get_formatted_time_ns_to_ms(result.green_node_changed_name_time_ns, bg_trigger_time_ns))

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

    def print_node_status_times(
            self, node: str, results: BlueGreenResultsAsync, time_zero_ns: int) -> None:
        status_map: ConcurrentDict = results.blue_status_time
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

    def print_duration_times(
            self, node: str, title: str, times: Deque[TimeHolder], time_zero_ns: int) -> None:
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

    def get_percentile(self, input_data: List[int], percentile: float) -> int:
        if not input_data:
            return 0
        sorted_list = sorted(input_data)
        rank = 1 if percentile == 0 else math.ceil(percentile / 100.0 * len(input_data))
        return sorted_list[rank - 1]

    def log_unhandled_exceptions(self) -> None:
        for exception in self.unhandled_exceptions:
            self.logger.debug(f"Unhandled exception: {exception}")

    def assert_test(self) -> None:
        bg_trigger_time_ns = next((result.bg_trigger_time_ns.get() for result in self.results.values()), None)
        assert bg_trigger_time_ns is not None, "Cannot get bg_trigger_time"

        max_green_node_changed_name_time_ms = max(
            (0 if result.green_node_changed_name_time_ns.get() == 0
             else (result.green_node_changed_name_time_ns.get() - bg_trigger_time_ns) // 1_000_000
             for result in self.results.values()),
            default=0
        )
        self.logger.debug(f"max_green_node_changed_name_time: {max_green_node_changed_name_time_ms} ms")

        switchover_complete_time_ms = max(
            (0 if x == 0
             else (x - bg_trigger_time_ns) // 1_000_000
             for result in self.results.values()
             if result.green_status_time
             for x in [result.green_status_time.get("SWITCHOVER_COMPLETED", 0)]),
            default=0
        )
        self.logger.debug(f"switchover_complete_time: {switchover_complete_time_ms} ms")

        assert switchover_complete_time_ms != 0, "BG switchover hasn't completed."
        assert switchover_complete_time_ms >= max_green_node_changed_name_time_ms, \
            "Green node changed name after SWITCHOVER_COMPLETED."
