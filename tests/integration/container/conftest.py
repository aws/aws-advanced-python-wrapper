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

from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
from aws_advanced_python_wrapper.host_list_provider import RdsHostListProvider
from aws_advanced_python_wrapper.plugin_service import PluginServiceImpl
from aws_advanced_python_wrapper.utils.log import Logger

if TYPE_CHECKING:
    from .utils.test_driver import TestDriver

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
    if hasattr(item, "callspec"):
        current_driver = item.callspec.params.get("test_driver")
        TestEnvironment.get_current().set_current_driver(current_driver)
    else:
        TestEnvironment.get_current().set_current_driver(None)

    info = TestEnvironment.get_current().get_info()
    request = info.get_request()

    if TestEnvironmentFeatures.NETWORK_OUTAGES_ENABLED in request.get_features():
        ProxyHelper.enable_all_connectivity()

    deployment = request.get_database_engine_deployment()
    if DatabaseEngineDeployment.AURORA == deployment or DatabaseEngineDeployment.MULTI_AZ == deployment:
        rds_utility = RdsTestUtility(info.get_region())
        rds_utility.wait_until_cluster_has_desired_status(info.get_cluster_name(), "available")
        try:
            rds_utility.get_instance_ids()
        except Exception as e:
            print(f"asdfasdf exception while validating extension (python): {e}")

        # Need to ensure that cluster details through API matches topology fetched through SQL
        # Wait up to 5min
        instances: List[str] = list()
        start_time = timeit.default_timer()
        while (len(instances) != request.get_num_of_instances()
               or len(instances) == 0
               or not rds_utility.is_db_instance_writer(instances[0])) and (
                timeit.default_timer() - start_time) < 300:  # 5 min

            try:
                instances = rds_utility.get_instance_ids()
            except Exception as ex:
                logger.warning("conftest.ExceptionWhileObtainingInstanceIDs", ex)
                instances = list()

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

        RdsHostListProvider._topology_cache.clear()
        RdsHostListProvider._is_primary_cluster_id_cache.clear()
        RdsHostListProvider._cluster_ids_to_update.clear()
        PluginServiceImpl._host_availability_expiring_cache.clear()
        DatabaseDialectManager._known_endpoint_dialects.clear()

        ConnectionProviderManager.reset_provider()
        DatabaseDialectManager.reset_custom_dialect()
        DriverDialectManager.reset_custom_dialect()
        ExceptionManager.reset_custom_handler()


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
