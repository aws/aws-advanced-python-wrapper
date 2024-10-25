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

from threading import Event, Thread
from time import perf_counter_ns, sleep
from typing import TYPE_CHECKING, Set, Optional, ClassVar, Callable, Dict, Union, List

from aws_advanced_python_wrapper.allowed_and_blocked_hosts import AllowedAndBlockedHosts
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

from boto3 import Session
from enum import Enum

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249 import Connection
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperties
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import SlidingExpirationCacheWithCleanupThread
from aws_advanced_python_wrapper.utils.telemetry.telemetry import TelemetryFactory, TelemetryCounter

logger = Logger(__name__)


class CustomEndpointRoleType(Enum):
    """
    Enum representing the possible roles of instances specified by a custom endpoint. Note that, currently, it is not
    possible to create a WRITER custom endpoint.
    """
    ANY = "ANY"
    READER = "READER"

    @classmethod
    def from_string(cls, value):
        return CustomEndpointRoleType(value)


class CustomEndpointInfo:
    def __init__(self,
                 endpoint_id: str,
                 cluster_id: str,
                 endpoint: str,
                 role_type: CustomEndpointRoleType,
                 static_members: Optional[Set[str]],
                 excluded_members: Optional[Set[str]]):
        self.endpoint_id = endpoint_id
        self.cluster_id = cluster_id
        self.endpoint = endpoint
        self.role_type = role_type
        self.static_members = None if not static_members else static_members
        self.excluded_members = None if not excluded_members else excluded_members

    @classmethod
    def from_db_cluster_endpoint(cls, endpoint_response_info: Dict[str, Union[str, List[str]]]):
        return CustomEndpointInfo(
            endpoint_response_info.get("DBClusterEndpointIdentifier"),
            endpoint_response_info.get("DBClusterIdentifier"),
            endpoint_response_info.get("Endpoint"),
            CustomEndpointRoleType.from_string(endpoint_response_info.get("EndpointType")),
            endpoint_response_info.get("StaticMembers"),
            endpoint_response_info.get("ExcludedMembers")
        )

    def __eq__(self, other: object):
        if self is object:
            return True
        if not isinstance(other, CustomEndpointInfo):
            return False

        return self.endpoint_id == other.endpoint_id \
            and self.cluster_id == other.cluster_id \
            and self.endpoint == other.endpoint \
            and self.role_type == other.role_type \
            and self.static_members == other.static_members \
            and self.excluded_members == other.excluded_members

    def __hash__(self):
        return hash((self.endpoint_id, self.cluster_id, self.endpoint, self.role_type))

    def __str__(self):
        return (f"CustomEndpointInfo[endpoint={self.endpoint}, cluster_id={self.cluster_id}, "
                f"role_type={self.role_type}, endpoint_id={self.endpoint_id}, static_members={self.static_members}, "
                f"excluded_members={self.excluded_members}]")


class CustomEndpointMonitor:
    _CUSTOM_ENDPOINT_INFO_EXPIRATION_NS: ClassVar[int] = 5 * 60_000_000_000  # 5 minutes
    # Keys are custom endpoint URLs, values are information objects for the associated custom endpoint.
    _custom_endpoint_info_cache: ClassVar[CacheMap[str, CustomEndpointInfo]] = CacheMap()

    def __init__(self,
                 plugin_service: PluginService,
                 custom_endpoint_host_info: HostInfo,
                 endpoint_id: str,
                 region: str,
                 refresh_rate_ns: int,
                 session: Optional[Session] = None
                 ):
        self._plugin_service = plugin_service
        self._custom_endpoint_host_info = custom_endpoint_host_info
        self._endpoint_id = endpoint_id
        self._region = region
        self._refresh_rate_ns = refresh_rate_ns
        self._client = session.client('rds', region_name=region)

        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._info_changed_counter = telemetry_factory.create_counter("customEndpoint.infoChanged.counter")
        self._stop_event = Event()

        self._thread = Thread(daemon=True, name="CustomEndpointMonitorThread", target=self._run)
        self._thread.start()

    def _run(self):
        logger.debug("CustomEndpointMonitorImpl.StartingMonitor", self._custom_endpoint_host_info.host)

        try:
            while not self._stop_event.is_set():
                try:
                    start = perf_counter_ns()

                    response = self._client.describe_db_cluster_endpoints(
                        DBClusterEndpointIdentifier=self._endpoint_id,
                        Filters=[
                            {
                                "Name": "db-cluster-endpoint-type",
                                "Values": ["custom"]
                            }
                        ]
                    )

                    endpoints = response["DBClusterEndpoints"]
                    if len(endpoints) != 1:
                        endpoint_hostnames = [endpoint["Endpoint"] for endpoint in endpoints]
                        logger.warning(
                            "CustomEndpointMonitorImpl.UnexpectedNumberOfEndpoints",
                            self._endpoint_id,
                            self._region,
                            len(endpoints),
                            endpoint_hostnames)

                        sleep(self._refresh_rate_ns / 1_000_000_000)
                        continue

                    endpoint_info = CustomEndpointInfo.from_db_cluster_endpoint(endpoints[0])
                    cached_info = self._custom_endpoint_info_cache.get(self._custom_endpoint_host_info.host)
                    if cached_info is not None and cached_info == endpoint_info:
                        elapsed_time = perf_counter_ns() - start
                        sleep_duration = min(0, self._refresh_rate_ns - elapsed_time)
                        sleep(sleep_duration / 1_000_000_000)
                        continue

                    logger.debug(
                        "CustomEndpointMonitorImpl.DetectedChangeInCustomEndpointInfo",
                        self._custom_endpoint_host_info.host, endpoint_info)

                    # The custom endpoint info has changed, so we need to update the set of allowed/blocked hosts.
                    hosts = AllowedAndBlockedHosts(endpoint_info.static_members, endpoint_info.excluded_members)
                    self._plugin_service.allowed_and_blocked_hosts = hosts
                    self._custom_endpoint_info_cache.put(
                        self._custom_endpoint_host_info.host, endpoint_info, self._CUSTOM_ENDPOINT_INFO_EXPIRATION_NS)
                    self._info_changed_counter.inc()

                    elapsed_time = perf_counter_ns() - start
                    sleep_duration = min(0, self._refresh_rate_ns - elapsed_time)
                    sleep(sleep_duration / 1_000_000_000)
                    continue
                except InterruptedError as e:
                    raise e
                except Exception as e:
                    # If the exception is not an InterruptedException, log it and continue monitoring.
                    logger.error("CustomEndpointMonitorImpl.Exception", self._custom_endpoint_host_info.host, e)
        except InterruptedError:
            logger.info("CustomEndpointMonitorImpl.Interrupted", self._custom_endpoint_host_info.host)
        finally:
            self._custom_endpoint_info_cache.remove(self._custom_endpoint_host_info.host)
            self._client.close()
            logger.debug("CustomEndpointMonitorImpl.StoppedMonitor", self._custom_endpoint_host_info.host)

    def has_custom_endpoint_info(self):
        return self._custom_endpoint_info_cache.get(self._custom_endpoint_host_info.host) is not None

    def close(self):
        logger.debug("CustomEndpointMonitorImpl.StoppingMonitor", self._custom_endpoint_host_info.host)
        self._stop_event.set()
        self._custom_endpoint_info_cache.remove(self._custom_endpoint_host_info.host)

    def clear_cache(self):
        logger.info("CustomEndpointMonitorImpl.ClearCache")
        self._custom_endpoint_info_cache.clear()


class CustomEndpointPlugin(Plugin):
    """
    A plugin that analyzes custom endpoints for custom endpoint information and custom endpoint changes, such as adding
    or removing an instance in the custom endpoint.
    """
    _SUBSCRIBED_METHODS: Set[str] = {"connect"}
    _CACHE_CLEANUP_RATE_NS: int = 6 * 10 ^ 10  # 1 minute
    _monitors: ClassVar[SlidingExpirationCacheWithCleanupThread[str, CustomEndpointMonitor]] = \
        SlidingExpirationCacheWithCleanupThread(_CACHE_CLEANUP_RATE_NS,
                                                should_dispose_func=lambda monitor: True,
                                                item_disposal_func=lambda monitor: monitor.close())

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._props = props

        self._should_wait_for_info: bool = WrapperProperties.WAIT_FOR_CUSTOM_ENDPOINT_INFO.get_bool(self._props)
        self._wait_for_info_timeout_ms: int = WrapperProperties.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS.get_int(self._props)
        self._idle_monitor_expiration_ms: int = \
            WrapperProperties.CUSTOM_ENDPOINT_IDLE_MONITOR_EXPIRATION_MS.get_int(self._props)

        telemetry_factory: TelemetryFactory = self._plugin_service.get_telemetry_factory()
        self._wait_for_info_counter: TelemetryCounter = telemetry_factory.create_counter("customEndpoint.waitForInfo.counter")
        self._custom_endpoint_host_info: Optional[HostInfo] = None
        self._custom_endpoint_id: Optional[str] = None
        self._rds_utils = RdsUtils()

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
        if not self._rds_utils.is_rds_custom_cluster_dns(host_info.host):
            return connect_func()

        self._custom_endpoint_host_info = host_info
        logger.debug("CustomEndpointPlugin.connectionRequestToCustomEndpoint", host_info.host)

        self._custom_endpoint_id = self._rds_utils.get_cluster_id(host_info.host)
        if not self._custom_endpoint_id:
            raise AwsWrapperError(
                Messages.get_formatted(
                    "CustomEndpointPlugin.ErrorParsingEndpointIdentifier", self._custom_endpoint_host_info.host))




class CustomEndpointPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return CustomEndpointPlugin(plugin_service, props)
