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

from typing import Set, Optional, ClassVar, Callable

from boto3 import Session

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249 import Connection
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.plugin_service import PluginService
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperties
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import SlidingExpirationCacheWithCleanupThread
from aws_advanced_python_wrapper.utils.telemetry.telemetry import TelemetryFactory, TelemetryCounter

logger = Logger(__name__)


class CustomEndpointMonitor:
    def __init__(self):
        ...


class CustomEndpointPlugin(Plugin):
    """
    A plugin that analyzes custom endpoints for custom endpoint information and custom endpoint changes, such as adding
    or removing an instance in the custom endpoint.
    """
    _SUBSCRIBED_METHODS: Set[str] = {"connect"}
    _CACHE_CLEANUP_RATE_NS: int = 6 * 10 ^ 10  # 1 minute
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()
    _monitors: ClassVar[SlidingExpirationCacheWithCleanupThread[str, CustomEndpointMonitor]] = \
        SlidingExpirationCacheWithCleanupThread(_CACHE_CLEANUP_RATE_NS,
                                                should_dispose_func=lambda monitor: True,
                                                item_disposal_func=lambda monitor: monitor.close())

    def __init__(self, plugin_service: PluginService, props: Properties, session: Optional[Session] = None):
        self._plugin_service = plugin_service
        self._props = props
        self._session = session

        self._should_wait_for_info: bool = WrapperProperties.WAIT_FOR_CUSTOM_ENDPOINT_INFO.get_bool(self._props)
        self._wait_for_info_timeout_ms: int = WrapperProperties.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS.get_int(self._props)
        self._idle_monitor_expiration_ms: int = \
            WrapperProperties.CUSTOM_ENDPOINT_IDLE_MONITOR_EXPIRATION_MS.get_int(self._props)

        telemetry_factory: TelemetryFactory = self._plugin_service.get_telemetry_factory()
        self._wait_for_info_counter: TelemetryCounter = telemetry_factory.create_counter("customEndpoint.waitForInfo.counter")

        self._custom_endpoint_host_info: Optional[HostInfo] = None
        self._custom_endpoint_host_id: Optional[str] = None

        CustomEndpointPlugin._SUBSCRIBED_METHODS.update(self._plugin_service.network_bound_methods)

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        ...


class CustomEndpointPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return CustomEndpointPlugin(plugin_service, props)
