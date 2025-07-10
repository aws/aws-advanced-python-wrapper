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
import time
from contextlib import closing
from threading import Event, RLock, Thread
from time import sleep
from typing import (TYPE_CHECKING, Any, Callable, ClassVar, List, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.database_dialect import (
    AuroraLimitlessDialect, DatabaseDialect)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin import Plugin
from aws_advanced_python_wrapper.utils.concurrent import ConcurrentDict
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import \
    SlidingExpirationCacheWithCleanupThread
from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
    TelemetryContext, TelemetryFactory, TelemetryTraceLevel)
from aws_advanced_python_wrapper.utils.utils import LogUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

logger = Logger(__name__)


class LimitlessPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"connect"}

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props
        self._limitless_router_service = LimitlessRouterService(
            self._plugin_service,
            LimitlessQueryHelper(self._plugin_service)
        )

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

        connection: Optional[Connection] = None

        dialect: DatabaseDialect = self._plugin_service.database_dialect
        if not isinstance(dialect, AuroraLimitlessDialect):
            refreshed_dialect = self._plugin_service.database_dialect
            if not isinstance(refreshed_dialect, AuroraLimitlessDialect):
                raise UnsupportedOperationError(
                    Messages.get_formatted("LimitlessPlugin.UnsupportedDialectOrDatabase",
                                           type(refreshed_dialect).__name__))

        if is_initial_connection:
            self._limitless_router_service.start_monitoring(host_info, props)

        self._context: LimitlessContext = LimitlessContext(
            host_info,
            props,
            connection,
            connect_func,
            [],
            self
        )
        self._limitless_router_service.establish_connection(self._context)
        connection = self._context.get_connection()
        if connection is not None and not self._plugin_service.driver_dialect.is_closed(connection):
            return connection

        raise AwsWrapperError(Messages.get_formatted("LimitlessPlugin.FailedToConnectToHost", host_info.host))


class LimitlessPluginFactory:

    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return LimitlessPlugin(plugin_service, props)


class LimitlessRouterMonitor:
    _MONITORING_PROPERTY_PREFIX: str = "limitless-router-monitor-"

    def __init__(self,
                 plugin_service: PluginService,
                 host_info: HostInfo,
                 limitless_router_cache: SlidingExpirationCacheWithCleanupThread,
                 limitless_router_cache_key: str,
                 props: Properties,
                 interval_ms: int):
        self._plugin_service = plugin_service
        self._host_info = host_info
        self._limitless_router_cache = limitless_router_cache
        self._limitless_router_cache_key = limitless_router_cache_key

        self._properties = copy.deepcopy(props)
        for property_key in self._properties.keys():
            if property_key.startswith(self._MONITORING_PROPERTY_PREFIX):
                self._properties[property_key[len(self._MONITORING_PROPERTY_PREFIX):]] = self._properties[property_key]
                self._properties.pop(property_key)

        WrapperProperties.WAIT_FOR_ROUTER_INFO.set(self._properties, False)

        self._interval_ms = interval_ms
        self._query_helper = LimitlessQueryHelper(plugin_service)
        self._telemetry_factory: TelemetryFactory = self._plugin_service.get_telemetry_factory()
        self._monitoring_conn: Optional[Connection] = None
        self._is_stopped: Event = Event()

        self._daemon_thread: Thread = Thread(daemon=True, target=self.run)
        self._daemon_thread.start()

    @property
    def host_info(self):
        return self._host_info

    @property
    def is_stopped(self):
        return self._is_stopped.is_set()

    def close(self):
        self._is_stopped.set()
        if self._monitoring_conn:
            self._monitoring_conn.close()
        self._daemon_thread.join(5)
        logger.debug("LimitlessRouterMonitor.Stopped", self.host_info.host)

    def run(self):
        logger.debug("LimitlessRouterMonitor.Running", self.host_info.host)

        telemetry_context: TelemetryContext = self._telemetry_factory.open_telemetry_context(
            "limitless router monitor thread", TelemetryTraceLevel.TOP_LEVEL)
        if telemetry_context is not None:
            telemetry_context.set_attribute("url", self._host_info.url)

        try:
            while not self.is_stopped:
                self._open_connection()

                if self._monitoring_conn is not None:

                    new_limitless_routers = self._query_helper.query_for_limitless_routers(self._monitoring_conn, self._host_info.port)
                    self._limitless_router_cache.compute_if_absent(self._limitless_router_cache_key,
                                                                   lambda _: new_limitless_routers,
                                                                   WrapperProperties.LIMITLESS_MONITOR_DISPOSAL_TIME_MS.get(
                                                                       self._properties) * 1_000_000)
                    logger.debug(LogUtils.log_topology(tuple(new_limitless_routers), "[limitlessRouterMonitor] Topology:"))

                sleep(self._interval_ms / 1000)

        except InterruptedError as e:
            logger.debug("LimitlessRouterMonitor.InterruptedExceptionDuringMonitoring", self.host_info.host)
            if telemetry_context:
                telemetry_context.set_exception(e)
                telemetry_context.set_success(False)
        except Exception as e:
            # this should not be reached; log and exit thread
            logger.debug("LimitlessRouterMonitor.ExceptionDuringMonitoringStop", self.host_info.host, e)
            if telemetry_context:
                telemetry_context.set_exception(e)
                telemetry_context.set_success(False)

        finally:
            self._is_stopped.set()
            if self._monitoring_conn is not None:
                try:
                    self._monitoring_conn.close()
                except Exception:
                    # Ignore
                    pass

            if telemetry_context is not None:
                telemetry_context.close_context()

    def _open_connection(self):
        try:
            driver_dialect = self._plugin_service.driver_dialect
            if self._monitoring_conn is None or driver_dialect.is_closed(self._monitoring_conn):
                logger.debug("LimitlessRouterMonitor.OpeningConnection", self.host_info.url)
                self._monitoring_conn = self._plugin_service.force_connect(self._host_info, self._properties, None)
                logger.debug("LimitlessRouterMonitor.OpenedConnection", self._monitoring_conn)

        except Exception as e:
            if self._monitoring_conn is not None:
                try:
                    self._monitoring_conn.close()
                except Exception:
                    pass  # ignore

                    self._monitoring_conn = None
            raise e


class LimitlessQueryHelper:
    _DEFAULT_QUERY_TIMEOUT_SEC: int = 5

    def __init__(self, plugin_service: PluginService):
        self._plugin_service = plugin_service

    def query_for_limitless_routers(self, connection: Connection, host_port_to_map: int) -> List[HostInfo]:

        database_dialect = self._plugin_service.database_dialect
        if not isinstance(database_dialect, AuroraLimitlessDialect):
            raise UnsupportedOperationError(Messages.get("LimitlessQueryHelper.UnsupportedDialectOrDatabase"))
        aurora_limitless_dialect: AuroraLimitlessDialect = database_dialect
        query = aurora_limitless_dialect.limitless_router_endpoint_query

        with closing(connection.cursor()) as cursor:
            self._plugin_service.driver_dialect.execute("Cursor.execute",
                                                        lambda: cursor.execute(query),
                                                        query,
                                                        exec_timeout=LimitlessQueryHelper._DEFAULT_QUERY_TIMEOUT_SEC)
            return self._map_result_set_to_host_info_list(cursor.fetchall(), host_port_to_map)

    def _map_result_set_to_host_info_list(self, result_set: List[Tuple[Any, Any]], host_port_to_map: int) -> List[HostInfo]:
        list_of_host_infos: List[HostInfo] = []
        for result in result_set:
            list_of_host_infos.append(self._create_host_info(result, host_port_to_map))
        return list_of_host_infos

    def _create_host_info(self, result: Tuple[Any, Any], host_port_to_map: int) -> HostInfo:
        host_name: str = result[0]
        cpu: float = float(result[1])

        weight: int = round(10 - (cpu * 10))
        if weight < 1 or weight > 10:
            weight = 1
            logger.debug("LimitlessRouterMonitor.InvalidRouterLoad", host_name, cpu)

        return HostInfo(host_name, host_port_to_map, weight=weight, host_id=host_name)


class LimitlessContext:

    def __init__(self,
                 host_info: HostInfo,
                 props: Properties,
                 connection: Optional[Connection],
                 connect_func: Callable,
                 limitless_routers: List[HostInfo],
                 connection_plugin: LimitlessPlugin) -> None:
        self._host_info = host_info
        self._props = props
        self._connection = connection
        self._connect_func = connect_func
        self._limitless_routers = limitless_routers
        self._connection_plugin = connection_plugin

    def get_host_info(self):
        return self._host_info

    def get_props(self):
        return self._props

    def get_connection(self):
        return self._connection

    def set_connection(self, connection: Connection):
        if self._connection is not None and self._connection != connection:
            try:
                self._connection.close()
            except Exception:
                pass

        self._connection = connection

    def get_connect_func(self) -> Callable:
        return self._connect_func

    def get_limitless_routers(self):
        return self._limitless_routers

    def set_limitless_routers(self, limitless_routers: List[HostInfo]):
        self._limitless_routers = limitless_routers

    def get_connection_plugin(self):
        return self._connection_plugin

    def is_any_router_available(self):
        for router in self.get_limitless_routers():
            if router.get_availability() == HostAvailability.AVAILABLE:
                return True
        return False


class LimitlessRouterService:
    _CACHE_CLEANUP_NS: int = 6 * 10 ^ 10  # 1 minute
    _limitless_router_cache: ClassVar[SlidingExpirationCacheWithCleanupThread[str, List[HostInfo]]] = \
        SlidingExpirationCacheWithCleanupThread(_CACHE_CLEANUP_NS)

    _limitless_router_monitor: ClassVar[SlidingExpirationCacheWithCleanupThread[str, LimitlessRouterMonitor]] = \
        SlidingExpirationCacheWithCleanupThread(_CACHE_CLEANUP_NS,
                                                should_dispose_func=lambda monitor: True,
                                                item_disposal_func=lambda monitor: monitor.close())

    _force_get_limitless_routers_lock_map: ClassVar[ConcurrentDict[str, RLock]] = ConcurrentDict()

    def __init__(self, plugin_service: PluginService, query_helper: LimitlessQueryHelper):
        self._plugin_service = plugin_service
        self._query_helper = query_helper

    def establish_connection(self, context: LimitlessContext) -> None:
        context.set_limitless_routers(self._get_limitless_routers(
            self._plugin_service.host_list_provider.get_cluster_id(), context.get_props()))

        if context.get_limitless_routers() is None or len(context.get_limitless_routers()) == 0:
            logger.debug("LimitlessRouterService.LimitlessRouterCacheEmpty")

            wait_for_router_info = WrapperProperties.WAIT_FOR_ROUTER_INFO.get(context.get_props())
            if wait_for_router_info:
                self._synchronously_get_limitless_routers_with_retry(context)
            else:
                logger.debug("LimitlessRouterService.UsingProvidedConnectUrl")
                if context.get_connection() is None or self._plugin_service.driver_dialect.is_closed(context.get_connection()):
                    context.set_connection(context.get_connect_func()())
                    return

        if context.get_host_info() in context.get_limitless_routers():
            logger.debug(Messages.get_formatted("LimitlessRouterService.ConnectWithHost", context.get_host_info().host))
            if context.get_connection() is None:
                try:
                    context.set_connection(context.get_connect_func()())
                except Exception as e:
                    if self._is_login_exception(e):
                        raise e

                    self._retry_connection_with_least_loaded_routers(context)
            return

        try:
            selected_host_info = self._plugin_service.get_host_info_by_strategy(
                HostRole.WRITER, "weighted_random", context.get_limitless_routers())
            logger.debug("LimitlessRouterService.SelectedHost", "None" if selected_host_info is None else selected_host_info.host)
        except Exception as e:
            if self._is_login_exception(e) or isinstance(e, UnsupportedOperationError):
                raise e

            self._retry_connection_with_least_loaded_routers(context)
            return

        if selected_host_info is None:
            self._retry_connection_with_least_loaded_routers(context)
            return

        try:
            context.set_connection(self._plugin_service.connect(selected_host_info, context.get_props(), context.get_connection_plugin()))
        except Exception as e:
            if self._is_login_exception(e):
                raise e

            if selected_host_info is not None:
                logger.debug("LimitlessRouterService.FailedToConnectToHost", selected_host_info.host)
                selected_host_info.set_availability(HostAvailability.UNAVAILABLE)

            self._retry_connection_with_least_loaded_routers(context)

    def _get_limitless_routers(self, cluster_id: str, props: Properties) -> List[HostInfo]:
        # Convert milliseconds to nanoseconds
        cache_expiration_nano: int = WrapperProperties.LIMITLESS_MONITOR_DISPOSAL_TIME_MS.get_int(props) * 1_000_000
        LimitlessRouterService._limitless_router_cache.set_cleanup_interval_ns(cache_expiration_nano)
        routers = LimitlessRouterService._limitless_router_cache.get(cluster_id)
        if routers is None:
            return []
        return routers

    def _retry_connection_with_least_loaded_routers(self, context: LimitlessContext) -> None:
        retry_count = 0
        max_retries = WrapperProperties.MAX_RETRIES_MS.get_int(context.get_props())
        while retry_count < max_retries:
            retry_count += 1
            if context.get_limitless_routers() is None or len(context.get_limitless_routers()) == 0 or not context.is_any_router_available():
                self._synchronously_get_limitless_routers_with_retry(context)

                if (context.get_limitless_routers() is None
                        or len(context.get_limitless_routers()) == 0
                        or not context.is_any_router_available()):
                    logger.debug("LimitlessRouterService.NoRoutersAvailableForRetry")

                    if context.get_connection() is not None and not self._plugin_service.driver_dialect.is_closed(context.get_connection()):
                        return
                    else:
                        try:
                            context.set_connection(context.get_connect_func()())
                            return
                        except Exception as e:
                            if self._is_login_exception(e):
                                raise e

                            raise AwsWrapperError(Messages.get_formatted("LimitlessRouterService.UnableToConnectNoRoutersAvailable"),
                                                  context.get_host_info().host) from e

            try:
                selected_host_info = self._plugin_service.get_host_info_by_strategy(
                    HostRole.WRITER, "highest_weight", context.get_limitless_routers())
                logger.debug("LimitlessRouterService.SelectedHostForRetry",
                             "None" if selected_host_info is None else selected_host_info.host)
                if selected_host_info is None:
                    continue

            except UnsupportedOperationError as e:
                logger.error("LimitlessRouterService.IncorrectConfiguration")
                raise e
            except AwsWrapperError:
                continue

            try:
                context.set_connection(self._plugin_service.connect(selected_host_info, context.get_props(), context.get_connection_plugin()))
                if context.get_connection() is not None:
                    return

            except Exception as e:
                if self._is_login_exception(e):
                    raise e
                selected_host_info.set_availability(HostAvailability.UNAVAILABLE)
                logger.debug("LimitlessRouterService.FailedToConnectToHost", selected_host_info.host)

        raise AwsWrapperError(Messages.get("LimitlessRouterService.MaxRetriesExceeded"))

    def _synchronously_get_limitless_routers_with_retry(self, context: LimitlessContext) -> None:
        logger.debug("LimitlessRouterService.SynchronouslyGetLimitlessRouters")
        retry_count = -1
        max_retries = WrapperProperties.GET_ROUTER_MAX_RETRIES.get_int(context.get_props())
        retry_interval_ms = WrapperProperties.GET_ROUTER_RETRY_INTERVAL_MS.get_float(context.get_props())
        first_iteration = True
        while first_iteration or retry_count < max_retries:
            # Emulate do while loop
            first_iteration = False
            try:
                self._synchronously_get_limitless_routers(context)
                if context.get_limitless_routers() is not None or len(context.get_limitless_routers()) > 0:
                    return

                time.sleep(retry_interval_ms)
            except InterruptedError as e:
                raise AwsWrapperError(Messages.get("LimitlessRouterService.InterruptedSynchronousGetRouter"), e)
            except Exception as e:
                if self._is_login_exception(e):
                    raise e
            finally:
                retry_count += 1

        raise AwsWrapperError(Messages.get("LimitlessRouterService.NoRoutersAvailable"))

    def _synchronously_get_limitless_routers(self, context: LimitlessContext) -> None:
        cache_expiration_nano: int = WrapperProperties.LIMITLESS_MONITOR_DISPOSAL_TIME_MS.get_int(context.get_props()) * 1_000_000

        lock = LimitlessRouterService._force_get_limitless_routers_lock_map.compute_if_absent(
            self._plugin_service.host_list_provider.get_cluster_id(),
            lambda _: RLock()
        )
        if lock is None:
            raise AwsWrapperError(Messages.get("LimitlessRouterService.LockFailedToAcquire"))

        lock.acquire()
        try:
            limitless_routers = LimitlessRouterService._limitless_router_cache.get(
                self._plugin_service.host_list_provider.get_cluster_id())
            if limitless_routers is not None and len(limitless_routers) != 0:
                context.set_limitless_routers(limitless_routers)
                return
            connection = context.get_connection()
            if connection is None or self._plugin_service.driver_dialect.is_closed(connection):
                context.set_connection(context.get_connect_func()())

            new_limitless_routers: List[HostInfo] = self._query_helper.query_for_limitless_routers(
                connection, context.get_host_info().port)

            if new_limitless_routers is not None and len(new_limitless_routers) != 0:
                context.set_limitless_routers(new_limitless_routers)
                LimitlessRouterService._limitless_router_cache.compute_if_absent(
                    self._plugin_service.host_list_provider.get_cluster_id(),
                    lambda _: new_limitless_routers,
                    cache_expiration_nano
                )
            else:
                raise AwsWrapperError(Messages.get("LimitlessRouterService.FetchedEmptyRouterList"))

        finally:
            lock.release()

    def _is_login_exception(self, error: Optional[Exception] = None):
        self._plugin_service.is_login_exception(error)

    def start_monitoring(self, host_info: HostInfo,
                         props: Properties) -> None:
        try:
            limitless_router_monitor_key: str = self._plugin_service.host_list_provider.get_cluster_id()
            cache_expiration_nano: int = WrapperProperties.LIMITLESS_MONITOR_DISPOSAL_TIME_MS.get_int(props) * 1_000_000
            intervals_ms: int = WrapperProperties.LIMITLESS_INTERVAL_MILLIS.get_int(props)

            LimitlessRouterService._limitless_router_monitor.compute_if_absent(
                limitless_router_monitor_key,
                lambda _: LimitlessRouterMonitor(self._plugin_service,
                                                 host_info,
                                                 LimitlessRouterService._limitless_router_cache,
                                                 limitless_router_monitor_key,
                                                 props,
                                                 intervals_ms), cache_expiration_nano)
        except Exception as e:
            logger.debug("LimitlessRouterService.ErrorStartingMonitor", e)
            raise e

    def clear_cache(self) -> None:
        LimitlessRouterService._force_get_limitless_routers_lock_map.clear()
        LimitlessRouterService._limitless_router_cache.clear()
