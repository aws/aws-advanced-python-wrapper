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

from copy import copy
from dataclasses import dataclass
from datetime import datetime
from threading import Event, Lock, Thread
from time import sleep
from typing import (TYPE_CHECKING, Callable, ClassVar, Dict, List, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import RandomHostSelector
from aws_advanced_python_wrapper.plugin import Plugin
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import \
    SlidingExpirationCacheWithCleanupThread
from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryContext, TelemetryFactory, TelemetryGauge, TelemetryTraceLevel)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.utils.notifications import HostEvent

logger = Logger(__name__)

MAX_VALUE = 2147483647


class FastestResponseStrategyPlugin(Plugin):
    _FASTEST_RESPONSE_STRATEGY_NAME = "fastest_response"
    _SUBSCRIBED_METHODS: Set[str] = {"accepts_strategy",
                                     "connect",
                                     "force_connect",
                                     "get_host_info_by_strategy",
                                     "notify_host_list_changed"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props
        self._host_response_time_service: HostResponseTimeService = \
            HostResponseTimeService(plugin_service, props, WrapperProperties.RESPONSE_MEASUREMENT_INTERVAL_MILLIS.get_int(props))
        self._cache_expiration_nanos = WrapperProperties.RESPONSE_MEASUREMENT_INTERVAL_MILLIS.get_int(props) * 10 ^ 6
        self._random_host_selector = RandomHostSelector()
        self._cached_fastest_response_host_by_role: CacheMap[str, HostInfo] = CacheMap()
        self._hosts: Tuple[HostInfo, ...] = ()

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
        return self._connect(host_info, props, is_initial_connection, connect_func)

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect(host_info, props, is_initial_connection, force_connect_func)

    def _connect(
            self,
            host: HostInfo,
            properties: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        conn = connect_func()

        if is_initial_connection:
            self._plugin_service.refresh_host_list(conn)

        return conn

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy == FastestResponseStrategyPlugin._FASTEST_RESPONSE_STRATEGY_NAME

    def get_host_info_by_strategy(self, role: HostRole, strategy: str) -> HostInfo:
        if not self.accepts_strategy(role, strategy):
            logger.error("FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy", strategy)
            raise AwsWrapperError(Messages.get_formatted("FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy", strategy))

        fastest_response_host: Optional[HostInfo] = self._cached_fastest_response_host_by_role.get(role.name)
        if fastest_response_host is not None:

            # Found a fastest host. Let's find it in the latest topology.
            for host in self._plugin_service.hosts:
                if host == fastest_response_host:
                    # found the fastest host in the topology
                    return host
                # It seems that the fastest cached host isn't in the latest topology.
                # Let's ignore cached results and find the fastest host.

        # Cached result isn't available. Need to find the fastest response time host.
        eligible_hosts: List[FastestResponseStrategyPlugin.ResponseTimeTuple] = []
        for host in self._plugin_service.hosts:
            if role == host.role:
                response_time_tuple = FastestResponseStrategyPlugin.ResponseTimeTuple(host,
                                                                                      self._host_response_time_service.get_response_time(host))
                eligible_hosts.append(response_time_tuple)

        # Sort by response time then retrieve the first host
        sorted_eligible_hosts: List[FastestResponseStrategyPlugin.ResponseTimeTuple] = \
            sorted(eligible_hosts, key=lambda x: x.response_time)

        calculated_fastest_response_host = sorted_eligible_hosts[0].host_info
        if calculated_fastest_response_host is None or \
                self._host_response_time_service.get_response_time(calculated_fastest_response_host) == MAX_VALUE:
            logger.debug("FastestResponseStrategyPlugin.RandomHostSelected")
            return self._random_host_selector.get_host(self._plugin_service.hosts, role, self._properties)

        self._cached_fastest_response_host_by_role.put(role.name,
                                                       calculated_fastest_response_host,
                                                       self._cache_expiration_nanos)

        return calculated_fastest_response_host

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        self._hosts = self._plugin_service.hosts
        if self._host_response_time_service is not None:
            self._host_response_time_service.set_hosts(self._hosts)

    @dataclass
    class ResponseTimeTuple:
        host_info: HostInfo
        response_time: int


class FastestResponseStrategyPluginFactory:

    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return FastestResponseStrategyPlugin(plugin_service, props)


class HostResponseTimeMonitor:

    _MONITORING_PROPERTY_PREFIX: str = "frt-"
    _NUM_OF_MEASURES: int = 5
    _DEFAULT_CONNECT_TIMEOUT_SEC = 10

    def __init__(self, plugin_service: PluginService, host_info: HostInfo, props: Properties, interval_ms: int):
        self._plugin_service = plugin_service
        self._host_info = host_info
        self._properties = props
        self._interval_ms = interval_ms

        self._telemetry_factory: TelemetryFactory = self._plugin_service.get_telemetry_factory()
        self._response_time: int = MAX_VALUE
        self._lock: Lock = Lock()
        self._monitoring_conn: Optional[Connection] = None
        self._is_stopped: Event = Event()

        self._host_id: Optional[str] = self._host_info.host_id
        if self._host_id is None or self._host_id == "":
            self._host_id = self._host_info.host

        self._daemon_thread: Thread = Thread(daemon=True, target=self.run)

        # Report current response time (in milliseconds) to telemetry engine.
        # Report -1 if response time couldn't be measured.
        self._response_time_gauge: TelemetryGauge = \
            self._telemetry_factory.create_gauge("frt.response.time." + self._host_id,
                                                 lambda: self._response_time if self._response_time != MAX_VALUE else -1)
        self._daemon_thread.start()

    @property
    def response_time(self):
        return self._response_time

    @response_time.setter
    def response_time(self, response_time: int):
        self._response_time = response_time

    @property
    def host_info(self):
        return self._host_info

    @property
    def is_stopped(self):
        return self._is_stopped.is_set()

    def close(self):
        self._is_stopped.set()
        self._daemon_thread.join(5)
        logger.debug("HostResponseTimeMonitor.Stopped", self._host_info.host)

    def _get_current_time(self):
        return datetime.now().microsecond / 1000  # milliseconds

    def run(self):
        context: TelemetryContext = self._telemetry_factory.open_telemetry_context(
            "host response time thread", TelemetryTraceLevel.TOP_LEVEL)
        context.set_attribute("url", self._host_info.url)
        try:
            while not self.is_stopped:
                self._open_connection()

                if self._monitoring_conn is not None:

                    response_time_sum = 0
                    count = 0
                    for i in range(self._NUM_OF_MEASURES):
                        if self.is_stopped:
                            break
                        start_time = self._get_current_time()
                        if self._plugin_service.driver_dialect.ping(self._monitoring_conn):
                            calculated_response_time = self._get_current_time() - start_time
                            response_time_sum = response_time_sum + calculated_response_time
                            count = count + 1

                        if count > 0:
                            self.response_time = response_time_sum / count
                        else:
                            self.response_time = MAX_VALUE
                        logger.debug("HostResponseTimeMonitor.ResponseTime", self._host_info.host, self._response_time)

                sleep(self._interval_ms / 1000)

        except InterruptedError:
            # exit thread
            logger.debug("HostResponseTimeMonitor.InterruptedExceptionDuringMonitoring", self._host_info.host)
        except Exception as e:
            # this should not be reached; log and exit thread
            logger.debug("HostResponseTimeMonitor.ExceptionDuringMonitoringStop",
                         self._host_info.host,
                         e)  # print full trace stack of the exception.
        finally:
            self._is_stopped.set()
            if self._monitoring_conn is not None:
                try:
                    self._monitoring_conn.close()
                except Exception:
                    # Do nothing
                    pass

            if context is not None:
                context.close_context()

    def _open_connection(self):
        try:
            driver_dialect = self._plugin_service.driver_dialect
            if self._monitoring_conn is None or driver_dialect.is_closed(self._monitoring_conn):
                monitoring_conn_properties: Properties = copy(self._properties)
                for key, value in self._properties.items():
                    if key.startswith(self._MONITORING_PROPERTY_PREFIX):
                        monitoring_conn_properties[key[len(self._MONITORING_PROPERTY_PREFIX):len(key)]] = value
                        monitoring_conn_properties.pop(key, None)

                # Set a default connect timeout if the user hasn't configured one
                if monitoring_conn_properties.get(WrapperProperties.CONNECT_TIMEOUT_SEC.name, None) is None:
                    monitoring_conn_properties[WrapperProperties.CONNECT_TIMEOUT_SEC.name] = HostResponseTimeMonitor._DEFAULT_CONNECT_TIMEOUT_SEC

                logger.debug("HostResponseTimeMonitor.OpeningConnection", self._host_info.url)
                self._monitoring_conn = self._plugin_service.force_connect(self._host_info, monitoring_conn_properties, None)
                logger.debug("HostResponseTimeMonitor.OpenedConnection", self._host_info.url)

        except Exception:
            if self._monitoring_conn is not None:
                try:
                    self._monitoring_conn.close()
                except Exception:
                    pass  # ignore

                self._monitoring_conn = None


class HostResponseTimeService:
    _CACHE_EXPIRATION_NS: int = 6 * 10 ^ 11  # 10 minutes
    _CACHE_CLEANUP_NS: int = 6 * 10 ^ 10  # 1 minute
    _lock: Lock = Lock()
    _monitoring_hosts: ClassVar[SlidingExpirationCacheWithCleanupThread[str, HostResponseTimeMonitor]] = \
        SlidingExpirationCacheWithCleanupThread(_CACHE_CLEANUP_NS,
                                                should_dispose_func=lambda monitor: True,
                                                item_disposal_func=lambda monitor: HostResponseTimeService._monitor_close(monitor))

    def __init__(self, plugin_service: PluginService, props: Properties, interval_ms: int):
        self._plugin_service = plugin_service
        self._properties = props
        self._interval_ms = interval_ms
        self._hosts: Tuple[HostInfo, ...] = ()
        self._telemetry_factory: TelemetryFactory = self._plugin_service.get_telemetry_factory()
        self._host_count_gauge: TelemetryGauge = self._telemetry_factory.create_gauge("frt.hosts.count", lambda: len(self._monitoring_hosts))

    @property
    def hosts(self) -> Tuple[HostInfo, ...]:
        return self._hosts

    @hosts.setter
    def hosts(self, new_hosts: Tuple[HostInfo, ...]):
        self._hosts = new_hosts

    @staticmethod
    def _monitor_close(monitor: HostResponseTimeMonitor):
        try:
            monitor.close()
        except Exception:
            pass

    def get_response_time(self, host_info: HostInfo) -> int:
        monitor: Optional[HostResponseTimeMonitor] = HostResponseTimeService._monitoring_hosts.get(host_info.url)
        if monitor is None:
            return MAX_VALUE
        return monitor.response_time

    def set_hosts(self, new_hosts: Tuple[HostInfo, ...]) -> None:
        old_hosts_dict = {x.url: x for x in self.hosts}
        self.hosts = new_hosts

        for host in self.hosts:
            if host.url not in old_hosts_dict:
                with self._lock:
                    self._monitoring_hosts.compute_if_absent(host.url,
                                                             lambda _: HostResponseTimeMonitor(
                                                                 self._plugin_service,
                                                                 host,
                                                                 self._properties,
                                                                 self._interval_ms), self._CACHE_EXPIRATION_NS)
