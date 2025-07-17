#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import copy
import weakref
from queue import Queue
from threading import Thread
from time import perf_counter_ns, sleep
from typing import (TYPE_CHECKING, Set, Optional, Callable, Any, Dict, ClassVar)

import psycopg

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.plugin import Plugin, CanReleaseResources, PluginFactory
from aws_advanced_python_wrapper.utils.atomic import AtomicReference, AtomicBoolean
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import ConnectionEvent, OldConnectionSuggestedAction, HostEvent
from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperties
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import SlidingExpirationCacheWithCleanupThread
from aws_advanced_python_wrapper.utils.telemetry.telemetry import TelemetryCounter, TelemetryTraceLevel

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

logger = Logger(__name__)


class HostMonitoringV2Plugin(Plugin, CanReleaseResources):
    _SUBSCRIBED_METHODS: Set[str] = {"*"}

    def __init__(self, plugin_service, props):
        dialect: DriverDialect = plugin_service.driver_dialect
        if not dialect.supports_abort_connection():
            raise AwsWrapperError(Messages.get_formatted(
                "HostMonitoringV2Plugin.ConfigurationNotSupported", type(dialect).__name__))

        self._props: Properties = props
        self._plugin_service: PluginService = plugin_service
        self._is_connection_initialized = False
        self._monitoring_host_info: Optional[HostInfo] = None
        self._rds_utils: RdsUtils = RdsUtils()
        self._monitor_service: MonitorServiceV2 = MonitorServiceV2(plugin_service)

    @property
    def subscribed_methods(self) -> Set[str]:
        return HostMonitoringV2Plugin._SUBSCRIBED_METHODS

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
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringV2Plugin.ConnectionNone", method_name))

        host_info = self._plugin_service.current_host_info
        if host_info is None:
            raise AwsWrapperError(Messages.get_formatted("HostMonitoringV2Plugin.HostInfoNoneForMethod", method_name))

        is_enabled = WrapperProperties.FAILURE_DETECTION_ENABLED.get_bool(self._props)
        if not is_enabled or not self._plugin_service.is_network_bound_method(method_name):
            return execute_func()

        failure_detection_time_ms = WrapperProperties.FAILURE_DETECTION_TIME_MS.get_int(self._props)
        failure_detection_interval = WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.get_int(self._props)
        failure_detection_count = WrapperProperties.FAILURE_DETECTION_COUNT.get_int(self._props)

        monitor_context = None
        result = None

        try:
            logger.debug("HostMonitoringV2Plugin.ActivatedMonitoring", method_name)
            monitor_context = self._monitor_service.start_monitoring(
                connection,
                self._get_monitoring_host_info(),
                self._props,
                failure_detection_time_ms,
                failure_detection_interval,
                failure_detection_count
            )
            result = execute_func()
        finally:
            if monitor_context:
                self._monitor_service.stop_monitoring(monitor_context, connection)
            logger.debug("HostMonitoringV2Plugin.MonitoringDeactivated", method_name)

        return result

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        # TODO: fix condition
        if ConnectionEvent.INITIAL_CONNECTION in changes:
            self._is_connection_initialized = True
            self._monitoring_host_info = None

        return OldConnectionSuggestedAction.NO_OPINION

    def _get_monitoring_host_info(self) -> HostInfo:
        if self._monitoring_host_info is None:
            current_host_info = self._plugin_service.current_host_info
            if current_host_info is None:
                raise AwsWrapperError("HostMonitoringV2Plugin.HostInfoNone")
            self._monitoring_host_info = current_host_info
            rds_type = self._rds_utils.identify_rds_type(self._monitoring_host_info.url)

            try:
                if rds_type.is_rds_cluster:
                    logger.debug("HostMonitoringV2Plugin.ClusterEndpointHostInfo")
                    self._monitoring_host_info = self._plugin_service.identify_connection()
                    if self._monitoring_host_info is None:
                        raise AwsWrapperError(
                            Messages.get_formatted(
                                "HostMonitoringV2Plugin.UnableToIdentifyConnection",
                                current_host_info.host,
                                self._plugin_service.host_list_provider))
                    self._plugin_service.fill_aliases(host_info=self._monitoring_host_info)
            except Exception as e:
                message = "HostMonitoringV2Plugin.ErrorIdentifyingConnection"
                logger.debug(message, e)
                raise AwsWrapperError(Messages.get_formatted(message, e)) from e
        return self._monitoring_host_info

    def release_resources(self):
        if self._monitor_service is not None:
            self._monitor_service.release_resources()

        self._monitor_service = None


class HostMonitoringV2PluginFactory(PluginFactory):

    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return HostMonitoringV2Plugin(plugin_service, props)


class MonitoringContext:
    """
    Monitoring context for each connection.
    This contains each connection's criteria for whether a server should be considered unhealthy.
    The context is shared between the main thread and the monitor thread.
    """
    def __init__(self, connection: Connection):
        self._connection_to_abort: AtomicReference = AtomicReference(weakref.ref(connection))
        self._node_unhealthy: AtomicBoolean = AtomicBoolean(False)

    def set_node_unhealthy(self) -> None:
        self._node_unhealthy.set(True)

    def should_abort(self) -> bool:
        connection_weak_ref = self._connection_to_abort.get()
        return self._node_unhealthy.get() and connection_weak_ref is not None and connection_weak_ref() is not None

    def set_inactive(self) -> None:
        self._connection_to_abort.set(None)

    def get_connection(self) -> Connection | None:
        connection_weak_ref = self._connection_to_abort.get()
        if connection_weak_ref is not None:
            return connection_weak_ref()
        else:
            return None

    def is_active(self) -> bool:
        connection_weak_ref = self._connection_to_abort.get()
        return connection_weak_ref is not None and connection_weak_ref() is not None


class HostMonitorV2:
    """
     This class uses a background thread to monitor a particular server with one or more active :py:class:`Connection`
     objects.
    """
    _THREAD_SLEEP_NANO = 100_000_000
    _MONITORING_PROPERTY_PREFIX = "monitoring-"
    _QUERY = "SELECT 1"

    def __init__(
            self,
            plugin_service: PluginService,
            host_info: HostInfo,
            props: Properties,
            failure_detection_time_ms: int,
            failure_detection_interval_ms: int,
            failure_detection_count: int,
            aborted_connection_counter: TelemetryCounter):
        self._plugin_service: PluginService = plugin_service
        self._host_info: HostInfo = host_info
        self._props: Properties = props
        self._telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._failure_detection_time_ns = failure_detection_time_ms
        self._failure_detection_interval_ns = failure_detection_interval_ms
        self._failure_detection_count = failure_detection_count
        self._aborted_connection_counter = aborted_connection_counter

        self._active_contexts = Queue()
        self._new_contexts: ConcurrentDict[float, Queue] = ConcurrentDict()
        self._is_stopped = AtomicBoolean(False)
        self._is_unhealthy = False
        self._failure_count = 0
        self._invalid_node_start_time_ns = 0
        self._monitoring_connection: Optional[Connection] = None
        self._driver_dialect = self._plugin_service.driver_dialect

        self._monitor_run_thread = Thread(daemon=True, name="HostMonitoringThreadRun", target=self.run)
        self._monitor_run_thread.start()
        self._monitor_new_context_thread = Thread(daemon=True, name="HostMonitoringThreadNewContextRun", target=self._new_context_run)
        self._monitor_new_context_thread.start()

    def can_dispose(self) -> bool:
        return self._active_contexts.empty() and self._new_contexts.items() == 0

    @property
    def is_stopped(self):
        return self._is_stopped.get()

    def stop(self):
        self._is_stopped.set(True)

    def start_monitoring(self, context: MonitoringContext):
        if self.is_stopped:
            logger.warning("HostMonitorV2.MonitorIsStopped", self._host_info.host)

        current_time_ns = perf_counter_ns()
        start_monitoring_time_ns = current_time_ns + self._failure_detection_time_ns
        queue = self._new_contexts.compute_if_absent(start_monitoring_time_ns, lambda k: Queue())
        queue.put(weakref.ref(context))

    def get_current_time(self) -> float:
        return int(perf_counter_ns())

    def _new_context_run(self) -> None:
        logger.debug("HostMonitorV2.StartMonitoringThreadNewContext", self._host_info.host)

        try:
            while not self.is_stopped:
                current_time_ns = self.get_current_time()

                processed_keys = []
                keys = list(self._new_contexts.keys())
                for key in keys:
                    queue: Queue = self._new_contexts.get(key)
                    if queue is not None:
                        if key < current_time_ns:
                            processed_keys.append(key)
                            while not queue.empty():
                                context = queue.get_nowait()()
                                if context is not None and context.is_active():
                                    self._active_contexts.put(weakref.ref(context))

                for key in processed_keys:
                    self._new_contexts.remove(key)

                sleep(1)
        except InterruptedError as ex:
            pass
        except Exception as ex:
            logger.debug("HostMonitorV2.ExceptionDuringMonitoringStop", self._host_info.host)

        logger.debug("HostMonitorV2.StopMonitoringThreadNewContext", self._host_info.host)

    def run(self) -> None:
        logger.debug("HostMonitorV2.StartMonitoringThread", self._host_info.host)

        try:
            while not self.is_stopped:
                if self._active_contexts.empty() and not self._is_unhealthy:
                    sleep(self._THREAD_SLEEP_NANO // 1_000_000_000)
                    continue

                status_check_start_time_ns = self.get_current_time()
                is_valid = self.check_connection_status()
                status_check_end_time_ns = self.get_current_time()

                self._update_node_health_status(is_valid, status_check_start_time_ns, status_check_end_time_ns)

                temp_active_contexts = []
                while not self._active_contexts.empty():
                    monitor_context_weak_ref = self._active_contexts.get_nowait()
                    if self.is_stopped:
                        break

                    if monitor_context_weak_ref is None:
                        continue

                    monitor_context = monitor_context_weak_ref()

                    if self._is_unhealthy:
                        # Kill Connection
                        monitor_context.set_node_unhealthy()
                        connection_to_abort = monitor_context.get_connection()
                        monitor_context.set_inactive()
                        if connection_to_abort is not None:
                            self.abort_connection(connection_to_abort)
                            if self._aborted_connection_counter is not None:
                                self._aborted_connection_counter.inc()
                    elif monitor_context.is_active():
                      temp_active_contexts.append(monitor_context)

                for active_context in temp_active_contexts:
                    self._active_contexts.put(weakref.ref(active_context))

                delay_ns = self._failure_detection_interval_ns - (status_check_end_time_ns - status_check_start_time_ns)
                if delay_ns < self._THREAD_SLEEP_NANO:
                    delay_ns = self._THREAD_SLEEP_NANO

                sleep(delay_ns // 1_000_000_000)

        except InterruptedError as ex:
            pass
        except Exception as ex:
            logger.debug("HostMonitorV2.ExceptionDuringMonitoringStop", self._host_info.host)
            raise ex
        finally:
            self.stop()
            if self._monitoring_connection is not None:
                try:
                    self._monitoring_connection.close()
                except AwsWrapperError as ex:
                    pass

    def check_connection_status(self) -> bool:
        connect_telemetry_context = self._telemetry_factory.open_telemetry_context("connection status check", TelemetryTraceLevel.FORCE_TOP_LEVEL)

        if connect_telemetry_context is not None:
            connect_telemetry_context.set_attribute("url", self._host_info.url)

        try:
            if self._monitoring_connection is None or self._driver_dialect.is_closed(self._monitoring_connection):
                monitoring_properties = copy.deepcopy(self._props)
                for property_key in monitoring_properties.keys():
                    if property_key.startswith(self._MONITORING_PROPERTY_PREFIX):
                        monitoring_properties[property_key[len(self._MONITORING_PROPERTY_PREFIX):]] = monitoring_properties[property_key]
                        monitoring_properties.pop(property_key, None)

                logger.debug("HostMonitorV2.OpeningMonitoringConnection", self._host_info.url)
                self._monitoring_connection = self._plugin_service.force_connect(self._host_info, monitoring_properties)
                logger.debug("HostMonitorV2.OpenedMonitoringConnection", self._host_info.url)
                return True

            valid_timeout = ((self._failure_detection_interval_ns - self._THREAD_SLEEP_NANO) // 2) * 1_000_000
            return self._is_host_available(self._monitoring_connection, valid_timeout)
        except AwsWrapperError:
            return False
        except psycopg.errors.ConnectionTimeout:
            return False
        except psycopg.errors.OperationalError:
            return False
        finally:
            if connect_telemetry_context is not None:
                connect_telemetry_context.close_context()

    def _is_host_available(self, conn: Connection, timeout_sec: float) -> bool:
        try:
            self._execute_conn_check(conn, timeout_sec)
            return True
        except TimeoutError:
            return False
        except psycopg.OperationalError:
            return False

    def _execute_conn_check(self, conn: Connection, timeout_sec: float):
        driver_dialect = self._plugin_service.driver_dialect
        with conn.cursor() as cursor:
            query = HostMonitorV2._QUERY
            driver_dialect.execute("Cursor.execute", lambda: cursor.execute(query), query, exec_timeout=timeout_sec)
            cursor.fetchone()

    def _update_node_health_status(
            self,
            connection_valid: bool,
            status_check_start_ns: float,
            status_check_end_ns: float) -> None:
        if not connection_valid:
            self._failure_count += 1

            if self._invalid_node_start_time_ns == 0:
                self._invalid_node_start_time_ns = status_check_start_ns

            invalid_node_duration_ns = status_check_end_ns - self._invalid_node_start_time_ns
            max_invalid_node_duration_ns = self._failure_detection_interval_ns * max(0, self._failure_detection_count - 1)

            if invalid_node_duration_ns >= max_invalid_node_duration_ns:
                logger.debug("HostMonitorV2.HostDead", self._host_info.host)
                self._is_unhealthy = True
                return

            logger.debug("HostMonitorV2.HostNotResponding", self._host_info.host, self._failure_count)

        if self._failure_count > 0:
            # Node is back alive
            logger.debug("HostMonitorV2.HostAlive", self._host_info.host)

        self._failure_count = 0
        self._invalid_node_start_time_ns = 0
        self.is_unhealthy = False

    def abort_connection(self, connection: Connection) -> None:
        try:
            connection.close()
        except AwsWrapperError as ex:
            logger.debug("HostMonitorV2.ExceptionAbortingConnection", ex)


class MonitorServiceV2:
    # 1 Minute to Nanoseconds
    _CACHE_CLEANUP_NANO = 1 * 60 * 1_000_000_000

    _monitors: ClassVar[SlidingExpirationCacheWithCleanupThread[str, HostMonitorV2]] = \
        SlidingExpirationCacheWithCleanupThread(_CACHE_CLEANUP_NANO,
                                                should_dispose_func=lambda monitor: monitor.can_dispose(),
                                                item_disposal_func=lambda monitor: monitor.close())

    def __init__(self, plugin_service: PluginService):
        self._plugin_service: PluginService = plugin_service

        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._aborted_connections_counter = telemetry_factory.create_counter("efm2.connections.aborted")

    def start_monitoring(self,
                         conn: Connection,
                         host_info: HostInfo,
                         props: Properties,
                         failure_detection_time_ms: int,
                         failure_detection_interval_ms: int,
                         failure_detection_count: int) -> MonitoringContext:
        monitor = self.get_monitor(conn, host_info, props, failure_detection_time_ms, failure_detection_interval_ms, failure_detection_count)
        context = MonitoringContext(conn)
        monitor.start_monitoring(context)
        return context

    def stop_monitoring(self, context: MonitoringContext, connection_to_abort: Connection):
        if context.should_abort():
            context.set_inactive()
            try:
                connection_to_abort.close()
                if self._aborted_connections_counter is not None:
                    self._aborted_connections_counter.inc()
            except AwsWrapperError as ex:
                logger.debug("MonitorServiceV2.ExceptionAbortingConnection", ex)
        else:
            context.set_inactive()

    def release_resources(self):
        pass

    def get_monitor(self,
                         conn: Connection,
                         host_info: HostInfo,
                         props: Properties,
                         failure_detection_time_ms: int,
                         failure_detection_interval_ms: int,
                         failure_detection_count: int) -> HostMonitorV2:
        monitor_key = "{}:{}:{}:{}".format(
            failure_detection_time_ms,
            failure_detection_interval_ms,
            failure_detection_count,
            host_info.host
        )

        cache_expiration_ns = WrapperProperties.MONITOR_DISPOSAL_TIME_MS.get_float(props) * 1_000_000
        return self._monitors.compute_if_absent(monitor_key,
                                                lambda k: HostMonitorV2(self._plugin_service,
                                                                        host_info,
                                                                        props,
                                                                        failure_detection_time_ms,
                                                                        failure_detection_interval_ms,
                                                                        failure_detection_count,
                                                                        self._aborted_connections_counter),
                                                cache_expiration_ns)



