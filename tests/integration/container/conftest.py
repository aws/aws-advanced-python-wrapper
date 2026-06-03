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

import atexit
import threading
from typing import TYPE_CHECKING, Optional

from aws_xray_sdk.core import xray_recorder  # type: ignore

from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.custom_endpoint_plugin import \
    CustomEndpointMonitor
from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
from aws_advanced_python_wrapper.plugin_service import PluginServiceImpl
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from .utils.test_driver import TestDriver
    from aws_xray_sdk.core.models.segment import Segment  # type: ignore

import socket
import timeit
from time import sleep
from typing import List

import pytest  # type: ignore

from .utils.connection_utils import ConnectionUtils
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.proxy_helper import ProxyHelper
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

logger = Logger(__name__)


_DRAINABLE_POOL_NAMES = (
    "TopologyUtils",
    "AuroraTopologyUtils",
    "MultiAzTopologyUtils",
    "PluginServiceImplExecutor",
    "DriverDialectExecutor",
    "DatabaseDialectManagerExecutor",
    "MySQLDriverDialectExecutor",
)

_DRAIN_TIMEOUT_SEC = 30.0


def _drain_inflight_workers(timeout_sec: float = _DRAIN_TIMEOUT_SEC) -> None:
    """
    Wait for in-flight worker threads to finish before connections are torn down.
    """
    deadline = timeit.default_timer() + timeout_sec

    # 1. Drain the shared pools. shutdown(wait=True) joins worker threads, which
    #    only exit once their current work item (the stuck C call) returns.
    for pool_name in _DRAINABLE_POOL_NAMES:
        remaining = deadline - timeit.default_timer()
        if remaining <= 0:
            logger.warning("[drain] Timed out before draining pool: " + pool_name)
            continue

        drained = _drain_pool_with_timeout(pool_name, remaining)
        if not drained:
            logger.warning(
                "[drain] Pool '%s' did not drain within timeout; a worker may still "
                "be blocked on a disabled host." % pool_name)

    # 2. Wait for any remaining wrapper helper threads (monitors, host monitors)
    #    to wind down now that the network is back.
    _join_wrapper_helper_threads(deadline)


def _drain_pool_with_timeout(pool_name: str, timeout_sec: float) -> bool:
    done = threading.Event()

    def _shutdown():
        try:
            services_container.release_thread_pool(pool_name, wait=True)
        finally:
            done.set()

    watcher = threading.Thread(
        target=_shutdown, name="drain-%s" % pool_name, daemon=True)
    watcher.start()
    return done.wait(timeout=timeout_sec)


def _join_wrapper_helper_threads(deadline: float) -> None:
    current = threading.current_thread()
    for thread in list(threading.enumerate()):
        if thread is current or thread is threading.main_thread():
            continue
        if not thread.is_alive() or not thread.daemon:
            continue

        name = thread.name or ""
        if not _is_wrapper_helper_thread(name):
            continue

        remaining = deadline - timeit.default_timer()
        if remaining <= 0:
            logger.warning("[drain] Timed out before joining thread: " + name)
            return

        thread.join(timeout=remaining)
        if thread.is_alive():
            logger.warning("[drain] Helper thread still alive after join: " + name)


def _is_wrapper_helper_thread(name: str) -> bool:
    helper_markers = (
        "_monitor",          # ClusterTopologyMonitor._monitor thread
        "HostMonitor",       # host monitoring threads
        "MonitorService",    # monitor service cleanup
    )
    if any(marker in name for marker in helper_markers):
        return True
    return any(name.startswith(pool) for pool in _DRAINABLE_POOL_NAMES)


@pytest.fixture(scope='module')
def conn_utils():
    return ConnectionUtils()


def pytest_runtest_setup(item):
    test_name: Optional[str] = None
    if hasattr(item, "callspec"):
        current_driver = item.callspec.params.get("test_driver")
        TestEnvironment.get_current().set_current_driver(current_driver)
        test_name = item.callspec.id
    else:
        TestEnvironment.get_current().set_current_driver(None)

    logger.info("Starting test preparation for: " + test_name)

    segment: Optional[Segment] = None
    if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features():
        segment = xray_recorder.begin_segment("test: setup")
        segment.put_annotation("engine", TestEnvironment.get_current().get_engine().name)
        segment.put_annotation("deployment", TestEnvironment.get_current().get_deployment().name)
        segment.put_annotation("python_version", TestEnvironment.get_current()
                               .get_info().get_request().get_target_python_version().name)
        if test_name is not None:
            segment.put_annotation("test_name", test_name)

    info = TestEnvironment.get_current().get_info()
    request = info.get_request()

    if TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED in request.get_features():
        ProxyHelper.enable_all_connectivity()
        # Prototype (option 1): the previous test may have left worker threads
        # blocked against a host it disabled via Toxiproxy. Now that
        # connectivity is restored, wait for them to unwind before this test's
        # setup tears down / recreates connection-holding services below. This
        # prevents the mid-suite use-after-free crash (observed as
        # "none_dealloc: ... refcount error in a C extension").
        _drain_inflight_workers()

    deployment = request.get_database_engine_deployment()
    if DatabaseEngineDeployment.AURORA == deployment or DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER == deployment:
        rds_utility = RdsTestUtility(info.get_region(), info.get_rds_endpoint())
        rds_utility.wait_until_cluster_has_desired_status(info.get_db_name(), "available")

        # Need to ensure that cluster details through API matches topology fetched through SQL
        # Wait up to 5min
        instances: List[str] = list()
        start_time = timeit.default_timer()
        while (len(instances) < request.get_num_of_instances()
               or len(instances) == 0
               or not rds_utility.is_db_instance_writer(instances[0])) and (
                timeit.default_timer() - start_time) < 300:  # 5 min

            try:
                instances = rds_utility.get_instance_ids()
            except Exception as ex:
                logger.warning("conftest.ExceptionWhileObtainingInstanceIDs", ex)
                instances = list()

            # Only sleep if condition is still not met
            if (len(instances) < request.get_num_of_instances()
                or len(instances) == 0
                or not rds_utility.is_db_instance_writer(instances[0])) and (
                    timeit.default_timer() - start_time) < 300:
                sleep(5)

        assert len(instances) > 0
        current_writer = instances[0]
        assert rds_utility.is_db_instance_writer(current_writer)

        rds_utility.make_sure_instances_up(instances)

        info.get_database_info().move_instance_first(current_writer)
        info.get_proxy_database_info().move_instance_first(current_writer)

        # Wait for cluster URL to resolve to the writer
        start_time = timeit.default_timer()
        cluster_endpoint = info.get_database_info().get_cluster_endpoint()
        writer_endpoint = info.get_database_info().get_instances()[0].get_host()
        cluster_ip = socket.gethostbyname(cluster_endpoint)
        writer_ip = socket.gethostbyname(writer_endpoint)
        while cluster_ip != writer_ip and (timeit.default_timer() - start_time) < 300:  # 5 min
            sleep(5)
            cluster_ip = socket.gethostbyname(cluster_endpoint)
            writer_ip = socket.gethostbyname(writer_endpoint)

        assert cluster_ip == writer_ip

        RdsUtils.clear_cache()
        services_container.release_resources()
        PluginServiceImpl._host_availability_expiring_cache.clear()
        DatabaseDialectManager._known_endpoint_dialects.clear()
        CustomEndpointMonitor._custom_endpoint_info_cache.clear()

        ConnectionProviderManager.release_resources()
        ConnectionProviderManager.reset_provider()
        DatabaseDialectManager.reset_custom_dialect()
        DriverDialectManager.reset_custom_dialect()
        ExceptionManager.reset_custom_handler()

        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in TestEnvironment.get_current().get_features() \
                and segment is not None:
            xray_recorder.end_segment()


def pytest_generate_tests(metafunc):
    if "test_environment" in metafunc.fixturenames:
        environment = TestEnvironment.get_current()
        metafunc.parametrize("test_environment", [environment], ids=[repr(environment)])
    if "test_driver" in metafunc.fixturenames:
        allowed_drivers: List[TestDriver] = TestEnvironment.get_current().get_allowed_test_drivers()  # type: ignore
        metafunc.parametrize("test_driver", allowed_drivers)


def pytest_sessionstart(session):
    TestEnvironment.get_current()


def pytest_sessionfinish(session, exitstatus):
    # Enable all connectivity in case any helper threads are still trying to execute against a disabled host
    ProxyHelper.enable_all_connectivity()

    # Prototype (option 1): now that connectivity is restored, wait for any
    # orphaned worker threads (stuck mid-query against the previously-disabled
    # host) to actually finish before the interpreter exits and tears down the
    # psycopg connection objects they hold. This closes the use-after-free
    # window that produces the exit-time segfault.
    _drain_inflight_workers()


def log_exit():
    print("Python program is exiting...")


atexit.register(log_exit)
