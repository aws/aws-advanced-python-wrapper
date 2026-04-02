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

import time
from copy import copy
from dataclasses import dataclass
from datetime import timedelta
from threading import Event, Lock, Thread
from time import perf_counter_ns, sleep
from typing import (TYPE_CHECKING, Callable, ClassVar, Dict, List, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import RandomHostSelector
from aws_advanced_python_wrapper.plugin import Plugin
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.events import (EventBase,
                                                      MonitorResetEvent)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.storage.cache_map import CacheMap
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


class ResponseTimeHolder:
    """Wrapper type for response time data, used as StorageService type key."""

    def __init__(self, url: str, response_time: int):
        self.url = url
        self.response_time = response_time


class FastestResponseStrategyPlugin(Plugin):
    _FASTEST_RESPONSE_STRATEGY_NAME = "fastest_response"
    _SUBSCRIBED_METHODS: Set[str] = {"accepts_strategy",
                                     "connect",
                                     "get_host_info_by_strategy",
                                     "notify_host_list_changed"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props
        self._host_response_time_service: HostResponseTimeService = \
            HostResponseTimeService(plugin_service, props, WrapperProperties.RESPONSE_MEASUREMENT_INTERVAL_MS.get_int(props))
        self._cache_expiration_nanos = WrapperProperties.RESPONSE_MEASUREMENT_INTERVAL_MS.get_int(props) * 1_000_000
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
        conn = connect_func()

        if is_initial_connection:
            self._plugin_service.refresh_host_list(conn)

        return conn

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy == FastestResponseStrategyPlugin._FASTEST_RESPONSE_STRATEGY_NAME

    def get_host_info_by_strategy(self, role: HostRole, strategy: str, host_list: Optional[List[HostInfo]] = None) -> HostInfo:
        if not self.accepts_strategy(role, strategy):
            logger.error("FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy", strategy)
            raise AwsWrapperError(Messages.get_formatted("FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy", strategy))

        fastest_response_host: Optional[HostInfo] = self._cached_fastest_response_host_by_role.get(role.name)
        if fastest_response_host is not None:

            # Found a fastest host. Let's find it in the latest topology.
            for host in self._plugin_service.hosts:
                if host.get_host_and_port() == fastest_response_host.get_host_and_port():
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
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
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
        self._storage_service = services_container.get_storage_service()

        self._telemetry_factory: TelemetryFactory = self._plugin_service.get_telemetry_factory()
        self._response_time: int = MAX_VALUE
        self._lock: Lock = Lock()
        self._monitoring_conn: Optional[Connection] = None
        self._is_stopped: Event = Event()
        self._last_activity_ns: int = perf_counter_ns()

        self._host_id: Optional[str] = self._host_info.host_id
        if self._host_id is None or self._host_id == "":
            self._host_id = self._host_info.host

        self._daemon_thread: Thread = Thread(daemon=True, target=self.run)

        # Report current response time (in milliseconds) to telemetry engine.
        # Report -1 if response time couldn't be measured.
        self._response_time_gauge: TelemetryGauge | None = \
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

    def stop(self) -> None:
        self._is_stopped.set()
        self.close()

    @property
    def can_dispose(self) -> bool:
        return self._is_stopped.is_set()

    @property
    def last_activity_ns(self) -> int:
        return self._last_activity_ns

    def process_event(self, event: EventBase) -> None:
        if isinstance(event, MonitorResetEvent) and self._host_info.host in event.endpoints:
            logger.debug("HostResponseTimeMonitor.ResetEventReceived", self._host_info.host)
            self._monitoring_conn = None
            self._response_time = MAX_VALUE
            self._storage_service.remove(ResponseTimeHolder, self._host_info.url)

    def close(self):
        self._daemon_thread.join(5)
        logger.debug("HostResponseTimeMonitor.Stopped", self._host_info.host)

    def _get_current_time(self):
        return time.perf_counter() * 1000  # milliseconds

    def run(self):
        context: TelemetryContext = self._telemetry_factory.open_telemetry_context(
            "host response time thread", TelemetryTraceLevel.TOP_LEVEL)

        if context is not None:
            context.set_attribute("url", self._host_info.url)
        try:
            while not self.is_stopped:
                self._last_activity_ns = perf_counter_ns()
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
                            self._storage_service.put(ResponseTimeHolder, self._host_info.url,
                                                      ResponseTimeHolder(self._host_info.url, self._response_time))
                        else:
                            self.response_time = MAX_VALUE
                            self._storage_service.remove(ResponseTimeHolder, self._host_info.url)
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
    _CACHE_EXPIRATION_NS: ClassVar[int] = 10 * 60_000_000_000  # 10 minutes
    _lock: ClassVar[Lock] = Lock()

    def __init__(self, plugin_service: PluginService, props: Properties, interval_ms: int):
        self._plugin_service = plugin_service
        self._properties = props
        self._interval_ms = interval_ms
        self._hosts: Tuple[HostInfo, ...] = ()
        self._telemetry_factory: TelemetryFactory = self._plugin_service.get_telemetry_factory()
        self._storage_service = services_container.get_storage_service()

        self._storage_service.register(
            ResponseTimeHolder, item_expiration_time=timedelta(minutes=10))

        self._monitor_service = services_container.get_monitor_service()
        self._monitor_service.register_monitor_type(
            HostResponseTimeMonitor,
            expiration_timeout_ns=HostResponseTimeService._CACHE_EXPIRATION_NS,
            produced_data_type=ResponseTimeHolder)

        self._host_count_gauge: TelemetryGauge | None = self._telemetry_factory.create_gauge(
            "frt.hosts.count",
            lambda: self._monitor_service.count(HostResponseTimeMonitor)
        )

    @property
    def hosts(self) -> Tuple[HostInfo, ...]:
        return self._hosts

    @hosts.setter
    def hosts(self, new_hosts: Tuple[HostInfo, ...]):
        self._hosts = new_hosts

    def get_response_time(self, host_info: HostInfo) -> int:
        holder = self._storage_service.get(ResponseTimeHolder, host_info.url)
        if holder is not None:
            return holder.response_time
        return MAX_VALUE

    def set_hosts(self, new_hosts: Tuple[HostInfo, ...]) -> None:
        old_hosts_dict = {x.url: x for x in self.hosts}
        self.hosts = new_hosts

        for host in self.hosts:
            if host.url not in old_hosts_dict:
                def _create_monitor(h: HostInfo = host) -> HostResponseTimeMonitor:
                    return HostResponseTimeMonitor(self._plugin_service, h, self._properties, self._interval_ms)

                with self._lock:
                    self._monitor_service.run_if_absent(
                        HostResponseTimeMonitor,
                        host.url,
                        _create_monitor)
