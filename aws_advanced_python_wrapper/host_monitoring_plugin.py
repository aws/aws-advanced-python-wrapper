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
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

from dataclasses import dataclass
from queue import Queue
from threading import Event, Lock, Thread
from time import perf_counter_ns
from typing import Any, Callable, Dict, FrozenSet, Optional, Set

from _weakref import ReferenceType, ref

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.plugin import (CanReleaseResources, Plugin,
                                                PluginFactory)
from aws_advanced_python_wrapper.utils import core_services
from aws_advanced_python_wrapper.utils.events import (EventBase,
                                                      MonitorResetEvent)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryCounter, TelemetryTraceLevel)
from aws_advanced_python_wrapper.utils.utils import QueueUtils

logger = Logger(__name__)


class HostMonitoringPluginFactory(PluginFactory):
    @staticmethod
    def get_instance(plugin_service: PluginService, props: Properties) -> Plugin:
        return HostMonitoringPlugin(plugin_service, props)


class HostMonitoringPlugin(Plugin, CanReleaseResources):
    _SUBSCRIBED_METHODS: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.NOTIFY_HOST_LIST_CHANGED.method_name,
        DbApiMethod.NOTIFY_CONNECTION_CHANGED.method_name
    }

    def __init__(self, plugin_service, props):
        dialect: DriverDialect = plugin_service.driver_dialect
        if not dialect.supports_abort_connection():
            raise AwsWrapperError(Messages.get_formatted(
                "HostMonitoringPlugin.ConfigurationNotSupported", type(dialect).__name__))

        self._props: Properties = props
        self._plugin_service: PluginService = plugin_service
        self._is_connection_initialized = False
        self._monitoring_host_info: Optional[HostInfo] = None
        self._rds_utils: RdsUtils = RdsUtils()
        self._monitor_service: HostMonitorService = HostMonitorService(plugin_service)
        self._lock: Lock = Lock()
        self._is_enabled = WrapperProperties.FAILURE_DETECTION_ENABLED.get_bool(self._props)
        self._failure_detection_time_ms = WrapperProperties.FAILURE_DETECTION_TIME_MS.get_int(self._props)
        self._failure_detection_interval = WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.get_int(self._props)
        self._failure_detection_count = WrapperProperties.FAILURE_DETECTION_COUNT.get_int(self._props)
        HostMonitoringPlugin._SUBSCRIBED_METHODS.update(self._plugin_service.network_bound_methods)

    @property
    def subscribed_methods(self) -> Set[str]:
        return HostMonitoringPlugin._SUBSCRIBED_METHODS

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        conn = connect_func()
        if conn:
            rds_type = self._rds_utils.identify_rds_type(host_info.host)
            if rds_type.is_rds_cluster:
                host_info.reset_aliases()
                self._plugin_service.fill_aliases(conn, host_info)
        return conn

    def execute(self, target: object, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        connection = self._plugin_service.current_connection
        if connection is None:
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringPlugin.ConnectionNone", method_name))

        host_info = self._plugin_service.current_host_info
        if host_info is None:
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringPlugin.HostInfoNoneForMethod", method_name))

        if not self._is_enabled or not self._plugin_service.is_network_bound_method(method_name):
            return execute_func()

        monitor_context = None
        result = None

        try:
            logger.debug("HostMonitoringPlugin.ActivatedMonitoring", method_name)
            monitor_context = self._monitor_service.start_monitoring(
                connection,
                self._get_monitoring_host_info().all_aliases,
                self._get_monitoring_host_info(),
                self._props,
                self._failure_detection_time_ms,
                self._failure_detection_interval,
                self._failure_detection_count
            )
            result = execute_func()
        finally:
            if monitor_context:
                with self._lock:
                    self._monitor_service.stop_monitoring(monitor_context)
                    if monitor_context.is_host_unavailable():
                        self._plugin_service.set_availability(
                            self._get_monitoring_host_info().all_aliases, HostAvailability.UNAVAILABLE)

                        driver_dialect = self._plugin_service.driver_dialect
                        if driver_dialect is not None and not driver_dialect.is_closed(connection):
                            try:
                                connection.close()
                            except Exception:
                                pass
                            raise AwsWrapperError(
                                Messages.get_formatted("HostMonitoringPlugin.UnavailableHost", host_info.as_alias()))
                logger.debug("HostMonitoringPlugin.MonitoringDeactivated", method_name)

        return result

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        self._monitoring_host_info = None
        if ConnectionEvent.INITIAL_CONNECTION in changes:
            self._is_connection_initialized = True

        return OldConnectionSuggestedAction.NO_OPINION

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]):
        if not self._is_connection_initialized:
            return

        for host, events in changes.items():
            if HostEvent.WENT_DOWN not in events and HostEvent.HOST_DELETED not in events:
                return

            monitoring_aliases = self._get_monitoring_host_info().all_aliases
            if len(monitoring_aliases) == 0:
                return

            if host in monitoring_aliases:
                self._monitor_service.stop_monitoring_host(monitoring_aliases)

    def _get_monitoring_host_info(self) -> HostInfo:
        if self._monitoring_host_info is None:
            current_host_info = self._plugin_service.current_host_info
            if current_host_info is None:
                raise AwsWrapperError("HostMonitoringPlugin.HostInfoNone")
            self._monitoring_host_info = current_host_info
            rds_type = self._rds_utils.identify_rds_type(self._monitoring_host_info.host)

            try:
                if rds_type.is_rds_cluster:
                    logger.debug("HostMonitoringPlugin.ClusterEndpointHostInfo")
                    self._monitoring_host_info = self._plugin_service.identify_connection()
                    if self._monitoring_host_info is None:
                        raise AwsWrapperError(
                            Messages.get_formatted(
                                "HostMonitoringPlugin.UnableToIdentifyConnection",
                                current_host_info.host,
                                self._plugin_service.host_list_provider))
                    self._plugin_service.fill_aliases(host_info=self._monitoring_host_info)
            except Exception as e:
                message = "HostMonitoringPlugin.ErrorIdentifyingConnection"
                logger.debug(message, e)
                raise AwsWrapperError(Messages.get_formatted(message, e)) from e
        return self._monitoring_host_info

    def release_resources(self):
        self._monitor_service = None


class MonitoringContext:
    """
    Monitoring context for each connection.
    This contains each connection's criteria for whether a server should be considered unhealthy.
    The context is shared between the main thread and the monitor thread.
    """
    def __init__(
            self,
            monitor: Monitor,
            connection: Connection,
            target_dialect: DriverDialect,
            failure_detection_time_ms: int,
            failure_detection_interval_ms: int,
            failure_detection_count: int,
            aborted_connections_counter: TelemetryCounter | None):
        self._monitor: Monitor = monitor
        self._connection: Connection = connection
        self._target_dialect: DriverDialect = target_dialect
        self._failure_detection_time_ms: int = failure_detection_time_ms
        self._failure_detection_interval_ms: int = failure_detection_interval_ms
        self._failure_detection_count: int = failure_detection_count
        self._aborted_connections_counter = aborted_connections_counter

        self._monitor_start_time_ns: int = 0  # Time of monitor context submission
        self._active_monitoring_start_time_ns: int = 0  # Time when the monitor should start checking the connection
        self._unavailable_host_start_time_ns: int = 0
        self._current_failure_count: int = 0
        self._is_host_unavailable: bool = False
        self._is_active: bool = True

    @property
    def failure_detection_interval_ms(self) -> int:
        return self._failure_detection_interval_ms

    @property
    def failure_detection_count(self) -> int:
        return self._failure_detection_count

    @property
    def active_monitoring_start_time_ns(self) -> int:
        return self._active_monitoring_start_time_ns

    @property
    def monitor(self) -> Monitor:
        return self._monitor

    @property
    def is_active(self) -> bool:
        return self._is_active

    @is_active.setter
    def is_active(self, is_active: bool):
        self._is_active = is_active

    def is_host_unavailable(self) -> bool:
        return self._is_host_unavailable

    def set_monitor_start_time_ns(self, start_time_ns: int):
        self._monitor_start_time_ns = start_time_ns
        self._active_monitoring_start_time_ns = start_time_ns + self._failure_detection_time_ms * 1_000_000

    def _abort_connection(self):
        if self._connection is None or not self._is_active:
            return
        try:
            if self._target_dialect.is_closed(self._connection):
                return
            self._target_dialect.abort_connection(self._connection)
        except Exception as e:
            # log and ignore
            logger.debug("MonitorContext.ExceptionAbortingConnection", e)

    def update_host_status(
            self, host: str, status_check_start_time_ns: int, status_check_end_time_ns: int, is_available: bool):
        """
        Update whether the connection is still valid if the total elapsed time has passed the grace period.

        :param host: the host for logging purposes.
        :param status_check_start_time_ns: the time when connection status check started in nanoseconds.
        :param status_check_end_time_ns: the time when connection status check ended in nanoseconds.
        :param is_available: whether the connection is valid.
        :return:
        """
        if not self._is_active:
            return
        total_elapsed_time_ns = status_check_end_time_ns - self._monitor_start_time_ns
        if total_elapsed_time_ns > (self._failure_detection_time_ms * 1_000_000):
            self._set_host_availability(host, is_available, status_check_start_time_ns, status_check_end_time_ns)

    def _set_host_availability(
            self, host: str, is_available: bool, status_check_start_time_ns: int, status_check_end_time_ns: int):
        """
        Set whether the connection to the server is still available based on the monitoring settings set in connection
        properties. The monitoring settings include:
        - failure_detection_time_ms
        - failure_detection_interval_ms
        - failure_detection_count

        :param host: the host for logging purposes.
        :param is_available: whether the connection is still available.
        :param status_check_start_time_ns: the time when connection status check started in nanoseconds.
        :param status_check_end_time_ns: the time when connection status check ended in nanoseconds.
        """
        if is_available:
            self._current_failure_count = 0
            self._unavailable_host_start_time_ns = 0
            self._is_host_unavailable = False
            return

        self._current_failure_count += 1
        if self._unavailable_host_start_time_ns <= 0:
            self._unavailable_host_start_time_ns = status_check_start_time_ns
        unavailable_host_duration_ns = status_check_end_time_ns - self._unavailable_host_start_time_ns
        max_unavailable_host_duration_ms = \
            self._failure_detection_interval_ms * max(0, self._failure_detection_count)

        if unavailable_host_duration_ns > (max_unavailable_host_duration_ms * 1_000_000):
            logger.debug("MonitorContext.HostUnavailable", host)
            self._is_host_unavailable = True
            self._abort_connection()
            if self._aborted_connections_counter is not None:
                self._aborted_connections_counter.inc()
            return

        logger.debug("MonitorContext.HostNotResponding", host, self._current_failure_count)
        return


class Monitor:
    """
     This class uses a background thread to monitor a particular server with one or more active :py:class:`Connection`
     objects.
    """
    _DEFAULT_CONNECT_TIMEOUT_SEC = 10
    _INACTIVE_SLEEP_MS = 100
    _MIN_HOST_CHECK_TIMEOUT_MS = 3000
    _QUERY = "SELECT 1"

    def __init__(
            self,
            plugin_service: PluginService,
            host_info: HostInfo,
            props: Properties):
        self._plugin_service: PluginService = plugin_service
        self._host_info: HostInfo = host_info
        self._props: Properties = props
        self._telemetry_factory = self._plugin_service.get_telemetry_factory()

        self._lock: Lock = Lock()
        self._active_contexts: Queue[MonitoringContext] = Queue()
        self._new_contexts: Queue[MonitoringContext] = Queue()
        self._monitoring_conn: Optional[Connection] = None
        self._is_stopped: Event = Event()
        self._monitor_disposal_time_ms: int = WrapperProperties.MONITOR_DISPOSAL_TIME_MS.get_int(props)
        self._context_last_used_ns: int = perf_counter_ns()
        self._host_check_timeout_ms: int = Monitor._MIN_HOST_CHECK_TIMEOUT_MS

        host_id = self._host_info.host_id if self._host_info.host_id is not None else self._host_info.host
        self._host_invalid_counter = self._telemetry_factory.create_counter(
            f"host_monitoring.host_unhealthy.count.{host_id}")

        self._thread = Thread(daemon=True, target=self.run, name="EFMv1Monitor")
        self._thread.start()

    @dataclass
    class HostStatus:
        is_available: bool
        elapsed_time_ns: int

    @property
    def is_stopped(self):
        return self._is_stopped.is_set()

    def stop(self):
        self._is_stopped.set()
        self.close()

    def close(self) -> None:
        if self._thread.is_alive():
            self._thread.join(timeout=5.0)
        if self._monitoring_conn is not None:
            try:
                self._monitoring_conn.close()
            except Exception:
                pass

    @property
    def can_dispose(self) -> bool:
        return self._active_contexts.empty() and self._new_contexts.empty()

    @property
    def last_activity_ns(self) -> int:
        return self._context_last_used_ns

    def process_event(self, event: EventBase) -> None:
        if isinstance(event, MonitorResetEvent) and self._host_info.host in event.endpoints:
            logger.debug("Monitor.ResetEventReceived", self._host_info.host)
            self._monitoring_conn = None

    def start_monitoring(self, context: MonitoringContext):
        current_time_ns = perf_counter_ns()
        context.set_monitor_start_time_ns(current_time_ns)
        self._context_last_used_ns = current_time_ns
        self._new_contexts.put(context)

    def stop_monitoring(self, context: MonitoringContext):
        if context is None:
            logger.warning("Monitor.ContextNone")
            return

        context.is_active = False
        self._context_last_used_ns = perf_counter_ns()

    def clear_contexts(self):
        QueueUtils.clear(self._new_contexts)
        QueueUtils.clear(self._active_contexts)

    def run(self):
        try:
            self._is_stopped.clear()

            while not self.is_stopped:
                try:
                    current_time_ns = perf_counter_ns()
                    first_added_new_context = None

                    # Process new contexts
                    while (new_monitor_context := QueueUtils.get(self._new_contexts)) is not None:
                        if first_added_new_context == new_monitor_context:
                            # This context has already been processed.
                            # Add it back to the queue and process it in the next round.
                            self._new_contexts.put(new_monitor_context)
                            break

                        if not new_monitor_context.is_active:
                            # Discard inactive contexts
                            continue

                        if current_time_ns >= new_monitor_context.active_monitoring_start_time_ns:
                            # Submit the context for active monitoring
                            self._active_contexts.put(new_monitor_context)
                            continue

                        # The active monitoring start time has not been hit yet.
                        # Add the context back to the queue and check it later.
                        self._new_contexts.put(new_monitor_context)
                        if first_added_new_context is None:
                            first_added_new_context = new_monitor_context

                    if self._active_contexts.empty():
                        if (perf_counter_ns() - self._context_last_used_ns) >= self._monitor_disposal_time_ms * 1_000_000:
                            # No active contexts and idle too long — stop self
                            break

                        self.sleep(Monitor._INACTIVE_SLEEP_MS / 1000)
                        continue

                    status_check_start_time_ns = perf_counter_ns()
                    self._context_last_used_ns = status_check_start_time_ns
                    status = self._check_host_status(self._host_check_timeout_ms)
                    delay_ms = -1
                    first_added_new_context = None

                    monitor_context: MonitoringContext
                    while (monitor_context := QueueUtils.get(self._active_contexts)) is not None:
                        with self._lock:
                            if not monitor_context.is_active:
                                # Discard inactive contexts
                                continue

                            if first_added_new_context == monitor_context:
                                # This context has already been processed by this loop.
                                # Add it back to the queue and exit the loop.
                                self._active_contexts.put(monitor_context)
                                break

                            # Process the context
                            monitor_context.update_host_status(
                                self._host_info.url,
                                status_check_start_time_ns,
                                status_check_start_time_ns + status.elapsed_time_ns,
                                status.is_available)

                            if not monitor_context.is_active or monitor_context.is_host_unavailable():
                                continue

                            # The context is still active and the host is still available. Continue monitoring the context.
                            self._active_contexts.put(monitor_context)
                            if first_added_new_context is None:
                                first_added_new_context = monitor_context

                            if delay_ms == -1 or delay_ms > monitor_context.failure_detection_interval_ms:
                                delay_ms = monitor_context.failure_detection_interval_ms

                    if delay_ms == -1:
                        delay_ms = Monitor._INACTIVE_SLEEP_MS
                    else:
                        # Subtract the time taken for the status check from the delay
                        delay_ms -= (status.elapsed_time_ns / 1_000_000)
                        if delay_ms <= 0:
                            delay_ms = Monitor._MIN_HOST_CHECK_TIMEOUT_MS
                        # Use this delay for all active contexts
                        self._host_check_timeout_ms = delay_ms

                    self.sleep(delay_ms / 1000)
                except InterruptedError as e:
                    raise e
                except Exception as e:
                    logger.debug("Monitor.ExceptionInMonitorLoop", self._host_info.host)
                    logger.debug(e, exc_info=True)
        except InterruptedError:
            logger.warning("Monitor.InterruptedException", self._host_info.host)
        except Exception as e:
            logger.debug("Monitor.StoppingMonitorUnhandledException", self._host_info.host)
            logger.debug(e, exc_info=True)
        finally:
            self._is_stopped.set()
            core_services.get_monitor_service().detach(Monitor, self)
            if self._monitoring_conn is not None:
                try:
                    self._monitoring_conn.close()
                except Exception:
                    pass

    def _check_host_status(self, host_check_timeout_ms: int) -> HostStatus:
        context = self._telemetry_factory.open_telemetry_context(
            "connection status check", TelemetryTraceLevel.FORCE_TOP_LEVEL)

        if context is not None:
            context.set_attribute("url", self._host_info.url)

        start_ns = perf_counter_ns()
        try:
            driver_dialect = self._plugin_service.driver_dialect
            if self._monitoring_conn is None or driver_dialect.is_closed(self._monitoring_conn):
                monitoring_properties: Properties = PropertiesUtils.create_monitoring_properties(self._props)

                # Set a default connect timeout if the user hasn't configured one
                if monitoring_properties.get(WrapperProperties.CONNECT_TIMEOUT_SEC.name, None) is None:
                    monitoring_properties[WrapperProperties.CONNECT_TIMEOUT_SEC.name] = Monitor._DEFAULT_CONNECT_TIMEOUT_SEC

                logger.debug("Monitor.OpeningMonitorConnection", self._host_info.url)
                start_ns = perf_counter_ns()
                self._monitoring_conn = self._plugin_service.force_connect(self._host_info, monitoring_properties, None)
                logger.debug("Monitor.OpenedMonitorConnection", self._host_info.url)
                return Monitor.HostStatus(True, perf_counter_ns() - start_ns)

            start_ns = perf_counter_ns()
            is_available = self._is_host_available(self._monitoring_conn, host_check_timeout_ms / 1000)
            if not is_available:
                if self._host_invalid_counter is not None:
                    self._host_invalid_counter.inc()
            return Monitor.HostStatus(is_available, perf_counter_ns() - start_ns)
        except Exception:
            if self._host_invalid_counter is not None:
                self._host_invalid_counter.inc()
            return Monitor.HostStatus(False, perf_counter_ns() - start_ns)
        finally:
            if context is not None:
                context.close_context()

    def _is_host_available(self, conn: Connection, timeout_sec: float) -> bool:
        try:
            self._execute_conn_check(conn, timeout_sec)
            return True
        except TimeoutError:
            return False

    def _execute_conn_check(self, conn: Connection, timeout_sec: float):
        driver_dialect = self._plugin_service.driver_dialect
        with conn.cursor() as cursor:
            query = Monitor._QUERY
            driver_dialect.execute(DbApiMethod.CURSOR_EXECUTE.method_name, lambda: cursor.execute(query), query, exec_timeout=timeout_sec)
            cursor.fetchone()

    def sleep(self, duration: int):
        self._is_stopped.wait(duration)


class HostMonitorService:
    def __init__(self, plugin_service: PluginService):
        self._plugin_service: PluginService = plugin_service
        self._cached_monitor_aliases: Optional[FrozenSet[str]] = None
        self._cached_monitor: Optional[ReferenceType[Monitor]] = None

        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._aborted_connections_counter = telemetry_factory.create_counter("host_monitoring.connections.aborted")

        self._monitor_service = core_services.get_monitor_service()
        self._monitor_service.register_monitor_type(
            Monitor,
            expiration_timeout_ns=WrapperProperties.MONITOR_DISPOSAL_TIME_MS.get_int(
                self._plugin_service.props) * 1_000_000)

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

        monitor: Optional[Monitor] = None if self._cached_monitor is None else self._cached_monitor()
        if monitor is None \
                or monitor.is_stopped \
                or self._cached_monitor_aliases is None \
                or self._cached_monitor_aliases != host_aliases:
            monitor = self._monitor_service.run_if_absent_with_aliases(
                Monitor,
                host_aliases,
                lambda: Monitor(self._plugin_service, host_info, props))
            self._cached_monitor = ref(monitor)
            self._cached_monitor_aliases = host_aliases

        context = MonitoringContext(
            monitor, conn, self._plugin_service.driver_dialect, failure_detection_time_ms,
            failure_detection_interval_ms, failure_detection_count, self._aborted_connections_counter)
        monitor.start_monitoring(context)
        return context

    @staticmethod
    def stop_monitoring(context: MonitoringContext):
        monitor = context.monitor
        monitor.stop_monitoring(context)

    def stop_monitoring_host(self, host_aliases: FrozenSet):
        for alias in host_aliases:
            monitor = self._monitor_service.get(Monitor, alias)
            if monitor is not None:
                monitor.clear_contexts()
                return
