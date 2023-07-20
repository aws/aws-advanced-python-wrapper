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

if TYPE_CHECKING:
    from aws_wrapper.dialect import Dialect
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService

from logging import getLogger
from threading import Lock
from typing import Any, Callable, Dict, FrozenSet, Optional, Set

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostinfo import HostAvailability, HostInfo
from aws_wrapper.plugin import CanReleaseResources, Plugin, PluginFactory
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.notifications import HostEvent
from aws_wrapper.utils.properties import Properties, WrapperProperties
from aws_wrapper.utils.rdsutils import RdsUtils
from aws_wrapper.utils.subscribed_method_utils import SubscribedMethodUtils

logger = getLogger(__name__)


class HostMonitoringPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return HostMonitoringPlugin(plugin_service, props)


class HostMonitoringPlugin(Plugin, CanReleaseResources):
    _SUBSCRIBED_METHODS: Set[str] = {"*"}

    def __init__(self, plugin_service, props):
        self._props: Properties = props
        self._plugin_service: PluginService = plugin_service
        self._monitoring_host_info: Optional[HostInfo] = None
        self._rds_utils: RdsUtils = RdsUtils()
        self._monitor_service: MonitorService = MonitorService(plugin_service)
        self._lock: Lock = Lock()

    @property
    def subscribed_methods(self) -> Set[str]:
        return HostMonitoringPlugin._SUBSCRIBED_METHODS

    def connect(self, host_info: HostInfo, props: Properties,
                initial: bool, connect_func: Callable) -> Connection:
        return self._connect(host_info, connect_func)

    def force_connect(self, host_info: HostInfo, props: Properties,
                      initial: bool, force_connect_func: Callable) -> Connection:
        return self._connect(host_info, force_connect_func)

    def _connect(self, host_info: HostInfo, connect_func: Callable) -> Connection:
        conn = connect_func()
        if conn:
            rds_type = self._rds_utils.identify_rds_type(host_info.host)
            if rds_type.is_rds_cluster:
                host_info.reset_aliases()
                self._plugin_service.fill_aliases(conn, host_info)
        return conn

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any) -> Any:
        connection = self._plugin_service.current_connection
        if connection is None:
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringPlugin.NullConnection", method_name))

        host_info = self._plugin_service.current_host_info
        if host_info is None:
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringPlugin.NullHostInfo", method_name))

        is_enabled = WrapperProperties.FAILURE_DETECTION_ENABLED.get_bool(self._props)
        if not is_enabled or method_name not in SubscribedMethodUtils.NETWORK_BOUND_METHODS:
            return execute_func()

        failure_detection_time_ms = WrapperProperties.FAILURE_DETECTION_TIME_MS.get_int(self._props)
        failure_detection_interval = WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.get_int(self._props)
        failure_detection_count = WrapperProperties.FAILURE_DETECTION_COUNT.get_int(self._props)

        monitor_context = None
        result = None

        try:
            logger.debug(Messages.get_formatted("HostMonitoringPlugin.ActivatedMonitoring", method_name))
            monitor_context = self._monitor_service.start_monitoring(
                connection,
                self._get_monitoring_host_info().all_aliases,
                self._get_monitoring_host_info(),
                self._props,
                failure_detection_time_ms,
                failure_detection_interval,
                failure_detection_count
            )
            result = execute_func()
        finally:
            if monitor_context:
                with self._lock:
                    self._monitor_service.stop_monitoring(monitor_context)
                    if monitor_context.is_host_unavailable():
                        self._plugin_service.set_availability(
                            self._get_monitoring_host_info().all_aliases, HostAvailability.NOT_AVAILABLE)
                        dialect = self._plugin_service.dialect
                        if dialect is not None and not dialect.is_closed(connection):
                            try:
                                connection.close()
                            except Exception:
                                pass
                            raise AwsWrapperError(
                                Messages.get_formatted("HostMonitoringPlugin.UnavailableHost", host_info.as_alias()))
                logger.debug(Messages.get_formatted("HostMonitoringPlugin.MonitoringDeactivated", method_name))

        return result

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        if HostEvent.WENT_DOWN in changes or HostEvent.HOST_DELETED in changes:
            monitoring_aliases = self._get_monitoring_host_info().all_aliases
            if monitoring_aliases:
                self._monitor_service.stop_monitoring_host_connections(monitoring_aliases)

        self._monitoring_host_info = None

    def _get_monitoring_host_info(self):
        if self._monitoring_host_info is None:
            self._monitoring_host_info = self._plugin_service.current_host_info
            rds_type = self._rds_utils.identify_rds_type(self._monitoring_host_info.url)

            try:
                if rds_type.is_rds_cluster:
                    logger.debug(Messages.get("HostMonitoringPlugin.ClusterEndpointHostInfo"))
                    self._monitoring_host_info = self._plugin_service.identify_connection()
                    if self._monitoring_host_info is None:
                        raise AwsWrapperError(
                            Messages.get_formatted(
                                "HostMonitoringPlugin.UnableToIdentifyConnection",
                                self._plugin_service.current_host_info.host,
                                self._plugin_service.host_list_provider))
                    self._plugin_service.fill_aliases(host_info=self._monitoring_host_info)
            except Exception as e:
                message = Messages.get_formatted("HostMonitoringPlugin.ErrorIdentifyingConnection", e)
                logger.debug(message)
                raise AwsWrapperError(message) from e
        return self._monitoring_host_info

    def release_resources(self):
        if self._monitor_service is not None:
            self._monitor_service.release_resources()

        self._monitor_service = None


class MonitoringContext:
    def __init__(
            self,
            monitor: Monitor,
            connection: Connection,
            dialect: Dialect,
            failure_detection_time_ms: int,
            failure_detection_interval_ms: int,
            failure_detection_count: int):
        self._monitor: Monitor = monitor
        self._connection: Connection = connection
        self._dialect: Dialect = dialect
        self._failure_detection_time_ms: int = failure_detection_time_ms
        self._failure_detection_interval_ms: int = failure_detection_interval_ms
        self._failure_detection_count: int = failure_detection_count

        self._monitor_start_time_ns: int = 0
        self._expected_active_monitoring_start_time_ns: int = 0
        self._unavailable_host_start_time_ns: int = 0
        self._current_failure_count: int = 0
        self._is_host_unavailable: bool = False
        self._is_active_context: bool = True

    @property
    def failure_detection_interval_ms(self) -> int:
        return self._failure_detection_interval_ms

    @property
    def failure_detection_count(self) -> int:
        return self._failure_detection_count

    @property
    def expected_active_monitoring_start_time_ns(self) -> int:
        return self._expected_active_monitoring_start_time_ns

    @property
    def monitor(self) -> Monitor:
        return self._monitor

    @property
    def is_active_context(self) -> bool:
        return self._is_active_context

    @is_active_context.setter
    def is_active_context(self, is_active_context: bool):
        self.is_active_context = is_active_context

    def is_host_unavailable(self) -> bool:
        return self._is_host_unavailable

    def _set_monitor_start_time_ns(self, start_time_ns: int):
        self._monitor_start_time_ns = start_time_ns
        self._expected_active_monitoring_start_time_ns = start_time_ns + self._failure_detection_time_ms * 1_000_000

    def _abort_connection(self):
        if self._connection is None or not self._is_active_context:
            return
        try:
            self._dialect.abort_connection(self._connection)
        except Exception as e:
            # log and ignore
            logger.debug(Messages.get_formatted("MonitorContext.ExceptionAbortingConnection", e))

    def update_connection_status(
            self, url: str, status_check_start_time_ns: int, status_check_end_time_ns: int, is_valid: bool):
        if not self._is_active_context:
            return
        total_elapsed_time_ns = status_check_end_time_ns - self._monitor_start_time_ns

        if total_elapsed_time_ns > (self._failure_detection_time_ms * 1_000_000):
            self._set_connection_valid(url, is_valid, status_check_start_time_ns, status_check_end_time_ns)

    def _set_connection_valid(
            self, url: str, is_valid: bool, status_check_start_time_ns: int, status_check_end_time_ns: int):
        if is_valid:
            self._current_failure_count = 0
            self._unavailable_host_start_time_ns = 0
            self._is_host_unavailable = False
            logger.debug("MonitorContext.HostAvailable")
            return

        self._current_failure_count += 1
        if self._unavailable_host_start_time_ns <= 0:
            self._unavailable_host_start_time_ns = status_check_start_time_ns
        unavailable_host_duration_ns = status_check_end_time_ns - self._unavailable_host_start_time_ns
        max_unavailable_host_duration_ms = \
            self._failure_detection_interval_ms * max(0, self._failure_detection_count)

        if unavailable_host_duration_ns > (max_unavailable_host_duration_ms * 1_000_000):
            logger.debug(Messages.get_formatted("MonitorContext.HostUnavailable", url))
            self._is_host_unavailable = True
            self._abort_connection()
            return

        logger.debug(Messages.get_formatted("MonitorContext.HostNotResponding", url, self._current_failure_count))
        return


class Monitor:
    def __init__(
            self,
            plugin_service: PluginService,
            host_info: HostInfo,
            props: Properties,
            monitor_service: MonitorService):
        self._plugin_service: PluginService = plugin_service
        self._host_info: HostInfo = host_info
        self._props: Properties = props
        self._monitor_disposal_time_ms = WrapperProperties.MONITOR_DISPOSAL_TIME_MS.get_int(props)
        self._monitor_service: MonitorService = monitor_service

    def start_monitoring(self, context: MonitoringContext):
        ...

    def stop_monitoring(self, context: MonitoringContext):
        ...

    def clear_contexts(self):
        ...


class MonitorThreadContainer:
    @staticmethod
    def release_instance():
        ...

    def get_or_create_monitor(  # type: ignore
            self, host_aliases: FrozenSet[str], monitor_supplier: Callable) -> Monitor:
        ...

    def get_monitor(self, alias: str) -> Optional[Monitor]:  # type: ignore
        ...

    def reset_resource(self, monitor: Monitor):
        ...

    def release_resource(self, monitor: Monitor):
        ...


class MonitorService:
    def __init__(self, plugin_service: PluginService):
        self._plugin_service: PluginService = plugin_service
        self._thread_container: MonitorThreadContainer = MonitorThreadContainer()
        self._cached_monitor_aliases: Optional[FrozenSet[str]] = None
        self._cached_monitor: Optional[Monitor] = None

    def start_monitoring(self,
                         conn: Connection,
                         host_aliases: FrozenSet[str],
                         host_info: HostInfo,
                         props: Properties,
                         failure_detection_time_ms: int,
                         failure_detection_interval_ms: int,
                         failure_detection_count: int) -> MonitoringContext:
        if not host_aliases:
            raise AwsWrapperError(Messages.get_formatted("MonitorService.EmptyAliasSet", host_info))

        if self._cached_monitor is None \
                or self._cached_monitor_aliases is None \
                or self._cached_monitor_aliases != host_aliases:
            monitor = self._thread_container.get_or_create_monitor(
                host_aliases, lambda: self._create_monitor(host_info, props))
            self._cached_monitor = monitor
            self._cached_monitor_aliases = host_aliases
        else:
            monitor = self._cached_monitor

        dialect = self._plugin_service.dialect
        if dialect is None:
            self._plugin_service.update_dialect()
            dialect = self._plugin_service.dialect
            if dialect is None:
                raise AwsWrapperError(Messages.get("MonitorService.NullDialect"))

        context = MonitoringContext(
            monitor, conn, dialect, failure_detection_time_ms, failure_detection_interval_ms, failure_detection_count)
        monitor.start_monitoring(context)
        return context

    def _create_monitor(self, host_info: HostInfo, props: Properties):
        return Monitor(self._plugin_service, host_info, props, self)

    def stop_monitoring(self, context: MonitoringContext):
        monitor = context.monitor
        monitor.stop_monitoring(context)

    def stop_monitoring_host_connections(self, host_aliases: FrozenSet):
        for alias in host_aliases:
            monitor = self._thread_container.get_monitor(alias)
            if monitor is not None:
                monitor.clear_contexts()
                self._thread_container.reset_resource(monitor)
                return

    def release_resources(self):
        self._thread_container = None
        MonitorThreadContainer.release_instance()

    def notify_unused(self, monitor: Monitor):
        self._thread_container.release_resource(monitor)
