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
from typing import TYPE_CHECKING, Optional

from aws_xray_sdk.core import xray_recorder

from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.custom_endpoint_plugin import (
    CustomEndpointMonitor, CustomEndpointPlugin)
from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
from aws_advanced_python_wrapper.host_list_provider import (
    MonitoringRdsHostListProvider, RdsHostListProvider)
from aws_advanced_python_wrapper.host_monitoring_plugin import \
    MonitoringThreadContainer
from aws_advanced_python_wrapper.plugin_service import PluginServiceImpl
from aws_advanced_python_wrapper.thread_pool_container import \
    ThreadPoolContainer
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.sliding_expiration_cache_container import \
    SlidingExpirationCacheContainer

if TYPE_CHECKING:
    from .utils.test_driver import TestDriver
    from aws_xray_sdk.core.models.segment import Segment

import socket
import timeit
from time import sleep
from typing import List

import pytest

from .utils.connection_utils import ConnectionUtils
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.proxy_helper import ProxyHelper
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures
logger = Logger(__name__)


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
        RdsHostListProvider._topology_cache.clear()
        RdsHostListProvider._is_primary_cluster_id_cache.clear()
        RdsHostListProvider._cluster_ids_to_update.clear()
        PluginServiceImpl._host_availability_expiring_cache.clear()
        DatabaseDialectManager._known_endpoint_dialects.clear()
        CustomEndpointMonitor._custom_endpoint_info_cache.clear()
        MonitoringThreadContainer.clean_up()
        ThreadPoolContainer.release_resources(wait=True)
        SlidingExpirationCacheContainer.release_resources()

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


def log_exit():
    print("Python program is exiting...")


atexit.register(log_exit)
