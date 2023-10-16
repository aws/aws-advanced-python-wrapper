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

from aws_advanced_python_wrapper.connection_provider import \
    ConnectionProviderManager
from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
from aws_advanced_python_wrapper.host_list_provider import RdsHostListProvider
from aws_advanced_python_wrapper.plugin_service import PluginServiceImpl


def pytest_runtest_setup(item):
    RdsHostListProvider._topology_cache.clear()
    RdsHostListProvider._is_primary_cluster_id_cache.clear()
    RdsHostListProvider._cluster_ids_to_update.clear()
    PluginServiceImpl._host_availability_expiring_cache.clear()
    DatabaseDialectManager._known_endpoint_dialects.clear()

    ConnectionProviderManager.reset_provider()
    DatabaseDialectManager.reset_custom_dialect()
    DriverDialectManager.reset_custom_dialect()
    ExceptionManager.reset_custom_handler()
