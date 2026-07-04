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

"""Async Aurora Limitless plugin -- full port of the sync retry state machine.

Port of :mod:`aws_advanced_python_wrapper.limitless_plugin` (that module is
the source of truth). Behaviour is kept identical -- same properties and
defaults, same error types and message keys, same retry semantics -- while
the I/O paths are async (``await``, ``asyncio.Task``, ``asyncio.sleep``).

Structure mirrors sync:
  * :class:`AsyncLimitlessPlugin` -- ``connect`` gate + FailedToConnectToHost.
  * :class:`AsyncLimitlessRouterService` -- ``establish_connection`` state
    machine (weighted_random primary selection, highest_weight retry with
    least-loaded fallback), plus the class-level standing-monitor registry.
  * :class:`AsyncLimitlessRouterMonitor` -- one background task per cluster
    that refreshes the router list on ``limitless_intervals_ms`` cadence via a
    ``force_connect`` probe (WAIT_FOR_ROUTER_INFO forced False to avoid
    re-triggering discovery).
  * :class:`AsyncLimitlessRouterCache` -- per-cluster router cache with a TTL
    of ``LIMITLESS_MONITOR_DISPOSAL_TIME_MS``.
  * :class:`AsyncLimitlessQueryHelper` -- dialect-gated router query bounded to
    5s via :func:`asyncio.wait_for`.

Monitor teardown hooks into
:func:`aws_advanced_python_wrapper.aio.cleanup.register_shutdown_hook`, so
``release_resources_async()`` drains the standing tasks.

Login failures short-circuit out of the router retry loop (raised
immediately), matching sync after the parity-review fix to
``_is_login_exception`` (it previously discarded the classification verdict
on both sides).
"""

from __future__ import annotations

import asyncio
import copy
import math
import time
from threading import Lock
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    List, Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.cleanup import (cancel_task_threadsafe,
                                                     register_shutdown_hook)
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.database_dialect import AuroraLimitlessDialect
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel
from aws_advanced_python_wrapper.utils.utils import LogUtils, Utils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
        TelemetryContext, TelemetryFactory)


logger = Logger(__name__)

# Sync selects with ``weighted_random`` on the primary path and
# ``highest_weight`` on the least-loaded retry path (limitless_plugin.py:381,
# 440). We match those strings so identical config produces identical routing.
_STRATEGY_INITIAL: str = "weighted_random"
_STRATEGY_RETRY: str = "highest_weight"


class AsyncLimitlessQueryHelper:
    """Async port of ``LimitlessQueryHelper``.

    Runs the dialect's pre-baked router-discovery query, bounded to
    :data:`_DEFAULT_QUERY_TIMEOUT_SEC` via :func:`asyncio.wait_for` (sync bounds
    it via the driver's ``exec_timeout``; async has no ``execute`` on the driver
    dialect, so the timeout lives here -- see AsyncDriverDialect docstring).
    """

    _DEFAULT_QUERY_TIMEOUT_SEC: int = 5

    def __init__(self, plugin_service: AsyncPluginService) -> None:
        self._plugin_service = plugin_service

    async def query_for_limitless_routers(
            self, connection: Any, host_port_to_map: int) -> List[HostInfo]:
        database_dialect = self._plugin_service.database_dialect
        if not isinstance(database_dialect, AuroraLimitlessDialect):
            raise UnsupportedOperationError(
                Messages.get("LimitlessQueryHelper.UnsupportedDialectOrDatabase"))
        query = database_dialect.limitless_router_endpoint_query

        rows = await asyncio.wait_for(
            self._run_query(connection, query),
            timeout=AsyncLimitlessQueryHelper._DEFAULT_QUERY_TIMEOUT_SEC)
        return self._map_result_set_to_host_info_list(rows, host_port_to_map)

    @staticmethod
    async def _run_query(connection: Any, query: str) -> List[Tuple[Any, ...]]:
        cursor = connection.cursor()
        async with cursor:
            await cursor.execute(query)
            return list(await cursor.fetchall())

    def _map_result_set_to_host_info_list(
            self,
            result_set: List[Tuple[Any, ...]],
            host_port_to_map: int) -> List[HostInfo]:
        return [self._create_host_info(result, host_port_to_map)
                for result in result_set]

    def _create_host_info(
            self, result: Tuple[Any, ...], host_port_to_map: int) -> HostInfo:
        host_name: str = result[0]
        cpu: float = float(result[1])

        weight: int = 10 - math.floor(cpu * 10)
        if weight < 1 or weight > 10:
            weight = 1
            logger.debug("LimitlessRouterMonitor.InvalidRouterLoad", host_name, cpu)

        return HostInfo(host_name, host_port_to_map, weight=weight, host_id=host_name)


class AsyncLimitlessContext:
    """Async port of ``LimitlessContext`` -- the mutable bag threaded through
    the state machine. :meth:`set_connection` is async so a replaced connection
    can be aborted via the driver dialect."""

    def __init__(
            self,
            host_info: HostInfo,
            props: Properties,
            connection: Optional[Any],
            connect_func: Callable[..., Awaitable[Any]],
            limitless_routers: List[HostInfo],
            connection_plugin: Optional[AsyncPlugin],
            plugin_service: AsyncPluginService) -> None:
        self._host_info = host_info
        self._props = props
        self._connection = connection
        self._connect_func = connect_func
        self._limitless_routers = limitless_routers
        self._connection_plugin = connection_plugin
        self._plugin_service = plugin_service

    def get_host_info(self) -> HostInfo:
        return self._host_info

    def get_props(self) -> Properties:
        return self._props

    def get_connection(self) -> Optional[Any]:
        return self._connection

    async def set_connection(self, connection: Optional[Any]) -> None:
        if self._connection is not None and self._connection is not connection:
            try:
                await self._plugin_service.driver_dialect.abort_connection(self._connection)
            except Exception:  # noqa: BLE001 - best-effort close of replaced conn
                pass
        self._connection = connection

    def get_connect_func(self) -> Callable[..., Awaitable[Any]]:
        return self._connect_func

    def get_limitless_routers(self) -> List[HostInfo]:
        return self._limitless_routers

    def set_limitless_routers(self, limitless_routers: List[HostInfo]) -> None:
        self._limitless_routers = limitless_routers

    def get_connection_plugin(self) -> Optional[AsyncPlugin]:
        return self._connection_plugin

    def is_any_router_available(self) -> bool:
        for router in self._limitless_routers:
            if router.get_availability() == HostAvailability.AVAILABLE:
                return True
        return False


class AsyncLimitlessRouterCache:
    """Class-level per-cluster router cache with a TTL.

    Sync stores routers in the StorageService with an item-expiration of
    ``LIMITLESS_MONITOR_DISPOSAL_TIME_MS`` (limitless_plugin.py:341-344). We
    replicate that TTL here rather than depend on the shared StorageService.
    Uses :class:`threading.Lock` because the cache can be read by event loops
    on different threads (Django + async-wrapper consumers).
    """

    _lock: ClassVar[Lock] = Lock()
    # cluster_id -> (routers, expiry_ns). expiry_ns is a monotonic deadline.
    _by_cluster: ClassVar[Dict[str, Tuple[List[HostInfo], int]]] = {}

    @classmethod
    def _default_ttl_ns(cls) -> int:
        return WrapperProperties.LIMITLESS_MONITOR_DISPOSAL_TIME_MS.get_int(Properties()) * 1_000_000

    @classmethod
    def put(
            cls,
            cluster_id: str,
            routers: List[HostInfo],
            ttl_ns: Optional[int] = None) -> None:
        if ttl_ns is None:
            ttl_ns = cls._default_ttl_ns()
        expiry_ns = time.perf_counter_ns() + ttl_ns
        with cls._lock:
            cls._by_cluster[cluster_id] = (list(routers), expiry_ns)

    @classmethod
    def get(cls, cluster_id: str) -> List[HostInfo]:
        now_ns = time.perf_counter_ns()
        with cls._lock:
            entry = cls._by_cluster.get(cluster_id)
            if entry is None:
                return []
            routers, expiry_ns = entry
            if now_ns >= expiry_ns:
                cls._by_cluster.pop(cluster_id, None)
                return []
            return list(routers)

    @classmethod
    def clear(cls) -> None:
        with cls._lock:
            cls._by_cluster.clear()


class AsyncLimitlessRouterMonitor:
    """Background task that refreshes the router list for one cluster.

    Async port of ``LimitlessRouterMonitor``. The probe connection is opened via
    :meth:`AsyncPluginService.force_connect` using a deep-copied property set
    with the ``limitless-router-monitor-`` prefix stripped and
    ``WAIT_FOR_ROUTER_INFO`` forced False -- so the probe never re-triggers
    router discovery (limitless_plugin.py:130-136, 225).
    """

    _MONITORING_PROPERTY_PREFIX: str = "limitless-router-monitor-"

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_info: HostInfo,
            props: Properties,
            interval_ms: int,
            cluster_id: str) -> None:
        self._plugin_service = plugin_service
        self._host_info = host_info
        self._cluster_id = cluster_id
        self._interval_sec = max(0.001, interval_ms / 1000.0)
        self._disposal_time_ns = int(
            WrapperProperties.LIMITLESS_MONITOR_DISPOSAL_TIME_MS.get_int(props)) * 1_000_000

        self._properties = copy.deepcopy(props)
        for property_key in list(self._properties.keys()):
            if property_key.startswith(self._MONITORING_PROPERTY_PREFIX):
                self._properties[property_key[len(self._MONITORING_PROPERTY_PREFIX):]] = \
                    self._properties[property_key]
                self._properties.pop(property_key)
        WrapperProperties.WAIT_FOR_ROUTER_INFO.set(self._properties, False)

        self._query_helper = AsyncLimitlessQueryHelper(plugin_service)
        self._telemetry_factory: TelemetryFactory = plugin_service.get_telemetry_factory()
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._probe_conn: Optional[Any] = None
        self._last_activity_ns: int = time.perf_counter_ns()

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    @property
    def host_info(self) -> HostInfo:
        return self._host_info

    @property
    def last_activity_ns(self) -> int:
        return self._last_activity_ns

    @property
    def can_dispose(self) -> bool:
        # Mirrors sync limitless_plugin.py:161-162 -- disposable once stopped.
        return self._stop_event.is_set()

    def is_stale(self, now_ns: int) -> bool:
        """Idle beyond the disposal window (LIMITLESS_MONITOR_DISPOSAL_TIME_MS).

        A healthy monitor refreshes ``_last_activity_ns`` every interval, so it
        never goes stale; a stuck/idle one does and gets swept by
        :meth:`AsyncLimitlessRouterService.dispose_stale_monitors`.
        """
        return (now_ns - self._last_activity_ns) > self._disposal_time_ns

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event.clear()
        # Owner loop for thread-safe cancellation from other loops/threads
        # (module-level monitor registry).
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        logger.debug("LimitlessRouterMonitor.Running", self._host_info.host)
        telemetry_context: Optional[TelemetryContext] = None
        if self._telemetry_factory is not None:
            telemetry_context = self._telemetry_factory.open_telemetry_context(
                "limitless router monitor task", TelemetryTraceLevel.TOP_LEVEL)
            if telemetry_context is not None:
                telemetry_context.set_attribute("url", self._host_info.url)

        try:
            while not self._stop_event.is_set():
                try:
                    await self._refresh_once()
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001 - keep the loop alive
                    logger.debug(
                        "LimitlessRouterMonitor.errorDuringMonitoringStop",
                        self._host_info.host, e)
                    if telemetry_context is not None:
                        telemetry_context.set_exception(e)
                        telemetry_context.set_success(False)
                await asyncio.sleep(self._interval_sec)
        except asyncio.CancelledError:
            pass
        finally:
            self._stop_event.set()
            await self._close_probe()
            if telemetry_context is not None:
                telemetry_context.close_context()

    async def _refresh_once(self) -> None:
        self._last_activity_ns = time.perf_counter_ns()
        conn = await self._open_connection()
        if conn is None:
            return
        routers = await self._query_helper.query_for_limitless_routers(
            conn, self._host_info.port)
        ttl_ns = self._disposal_time_ns if self._disposal_time_ns > 0 else None
        AsyncLimitlessRouterCache.put(self._cluster_id, routers, ttl_ns=ttl_ns)
        logger.debug(LogUtils.log_topology(
            tuple(routers), "[limitlessRouterMonitor] Topology:"))

    async def _open_connection(self) -> Optional[Any]:
        try:
            driver_dialect = self._plugin_service.driver_dialect
            if self._probe_conn is None or await driver_dialect.is_closed(self._probe_conn):
                logger.debug("LimitlessRouterMonitor.OpeningConnection", self._host_info.url)
                self._probe_conn = await self._plugin_service.force_connect(
                    self._host_info, self._properties, None)
                logger.debug("LimitlessRouterMonitor.OpenedConnection", self._probe_conn)
            return self._probe_conn
        except Exception:  # noqa: BLE001 - probe open best-effort; next cycle retries
            await self._close_probe()
            return None

    async def _close_probe(self) -> None:
        if self._probe_conn is not None:
            try:
                await self._plugin_service.driver_dialect.abort_connection(self._probe_conn)
            except Exception:  # noqa: BLE001 - teardown best-effort
                pass
            self._probe_conn = None

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._task
        if task is not None and not task.done():
            owner_loop = getattr(self, "_loop", None)
            cancel_task_threadsafe(task, owner_loop)
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if owner_loop is None or running is owner_loop:
                # Awaiting a foreign-loop task is invalid; drain only when the
                # task belongs to the current loop.
                try:
                    await task
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        self._task = None
        await self._close_probe()
        logger.debug("LimitlessRouterMonitor.Stopped", self._host_info.host)


class AsyncLimitlessRouterService:
    """``establish_connection`` state machine + standing-monitor registry.

    Instance role mirrors sync ``LimitlessRouterService`` (owned by the plugin,
    holds plugin_service + query_helper, runs the retry machine). Class-level
    state -- the per-cluster monitor registry and the per-cluster fetch locks --
    is shared across plugin instances, mirroring sync's ClassVar lock map.
    """

    _registry_lock: ClassVar[Lock] = Lock()
    _monitors: ClassVar[Dict[str, AsyncLimitlessRouterMonitor]] = {}
    _force_get_routers_locks: ClassVar[Dict[str, asyncio.Lock]] = {}

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            query_helper: AsyncLimitlessQueryHelper) -> None:
        self._plugin_service = plugin_service
        self._query_helper = query_helper

    # ---- connect state machine ---------------------------------------

    async def establish_connection(self, context: AsyncLimitlessContext) -> None:
        cluster_id = self._cluster_id(context.get_host_info())
        context.set_limitless_routers(self._get_limitless_routers(cluster_id))

        routers = context.get_limitless_routers()
        if routers is None or len(routers) == 0:
            logger.debug("LimitlessRouterService.LimitlessRouterCacheEmpty")

            wait_for_router_info = WrapperProperties.WAIT_FOR_ROUTER_INFO.get(context.get_props())
            if wait_for_router_info:
                await self._synchronously_get_limitless_routers_with_retry(context)
            else:
                logger.debug("LimitlessRouterService.UsingProvidedConnectUrl")
                conn = context.get_connection()
                if conn is None or await self._plugin_service.driver_dialect.is_closed(conn):
                    await context.set_connection(await context.get_connect_func()())
                    return

        routers = context.get_limitless_routers()
        if Utils.contains_host_and_port(tuple(routers), context.get_host_info().get_host_and_port()):
            logger.debug(Messages.get_formatted(
                "LimitlessRouterService.ConnectWithHost", context.get_host_info().host))
            if context.get_connection() is None:
                try:
                    await context.set_connection(await context.get_connect_func()())
                except Exception as e:  # noqa: BLE001
                    if self._is_login_exception(e):
                        raise e
                    await self._retry_connection_with_least_loaded_routers(context)
            return

        try:
            selected_host_info = self._plugin_service.get_host_info_by_strategy(
                HostRole.WRITER, _STRATEGY_INITIAL, context.get_limitless_routers())
            logger.debug("LimitlessRouterService.SelectedHost",
                         "None" if selected_host_info is None else selected_host_info.host)
        except Exception as e:  # noqa: BLE001
            if self._is_login_exception(e) or isinstance(e, UnsupportedOperationError):
                raise e
            await self._retry_connection_with_least_loaded_routers(context)
            return

        if selected_host_info is None:
            await self._retry_connection_with_least_loaded_routers(context)
            return

        try:
            await context.set_connection(await self._plugin_service.connect(
                selected_host_info, context.get_props(),
                plugin_to_skip=context.get_connection_plugin()))
        except Exception as e:  # noqa: BLE001
            if self._is_login_exception(e):
                raise e

            logger.debug("LimitlessRouterService.FailedToConnectToHost", selected_host_info.host)
            selected_host_info.set_availability(HostAvailability.UNAVAILABLE)

            await self._retry_connection_with_least_loaded_routers(context)

    async def _retry_connection_with_least_loaded_routers(
            self, context: AsyncLimitlessContext) -> None:
        retry_count = 0
        max_retries = WrapperProperties.MAX_RETRIES_MS.get_int(context.get_props())
        while retry_count < max_retries:
            retry_count += 1
            routers = context.get_limitless_routers()
            if routers is None or len(routers) == 0 or not context.is_any_router_available():
                await self._synchronously_get_limitless_routers_with_retry(context)

                routers = context.get_limitless_routers()
                if routers is None or len(routers) == 0 or not context.is_any_router_available():
                    logger.debug("LimitlessRouterService.NoRoutersAvailableForRetry")

                    conn = context.get_connection()
                    if conn is not None and not await self._plugin_service.driver_dialect.is_closed(conn):
                        return
                    else:
                        try:
                            await context.set_connection(await context.get_connect_func()())
                            return
                        except Exception as e:  # noqa: BLE001
                            if self._is_login_exception(e):
                                raise e

                            raise AwsWrapperError(Messages.get_formatted(
                                "LimitlessRouterService.UnableToConnectNoRoutersAvailable",
                                context.get_host_info().host), e) from e

            try:
                selected_host_info = self._plugin_service.get_host_info_by_strategy(
                    HostRole.WRITER, _STRATEGY_RETRY, context.get_limitless_routers())
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
                await context.set_connection(await self._plugin_service.connect(
                    selected_host_info, context.get_props(),
                    plugin_to_skip=context.get_connection_plugin()))
                if context.get_connection() is not None:
                    return
            except Exception as e:  # noqa: BLE001
                if self._is_login_exception(e):
                    raise e
                selected_host_info.set_availability(HostAvailability.UNAVAILABLE)
                logger.debug("LimitlessRouterService.FailedToConnectToHost", selected_host_info.host)

        raise AwsWrapperError(Messages.get("LimitlessRouterService.MaxRetriesExceeded"))

    async def _synchronously_get_limitless_routers_with_retry(
            self, context: AsyncLimitlessContext) -> None:
        logger.debug("LimitlessRouterService.SynchronouslyGetLimitlessRouters")
        retry_count = -1
        max_retries = WrapperProperties.GET_ROUTER_MAX_RETRIES.get_int(context.get_props())
        retry_interval_ms = WrapperProperties.GET_ROUTER_RETRY_INTERVAL_MS.get_float(context.get_props())
        first_iteration = True
        while first_iteration or retry_count < max_retries:
            # Emulate a do-while loop.
            first_iteration = False
            try:
                await self._synchronously_get_limitless_routers(context)
                routers = context.get_limitless_routers()
                if routers is not None or len(routers) > 0:
                    return

                await asyncio.sleep(retry_interval_ms)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                if self._is_login_exception(e):
                    raise e
            finally:
                retry_count += 1

        raise AwsWrapperError(Messages.get("LimitlessRouterService.NoRoutersAvailable"))

    async def _synchronously_get_limitless_routers(
            self, context: AsyncLimitlessContext) -> None:
        cluster_id = self._cluster_id(context.get_host_info())
        lock = self._get_cluster_lock(cluster_id)
        if lock is None:
            raise AwsWrapperError(Messages.get("LimitlessRouterService.LockFailedToAcquire"))

        async with lock:
            limitless_routers = self._get_limitless_routers(cluster_id)
            if limitless_routers is not None and len(limitless_routers) != 0:
                context.set_limitless_routers(limitless_routers)
                return

            connection = context.get_connection()
            if connection is None or await self._plugin_service.driver_dialect.is_closed(connection):
                await context.set_connection(await context.get_connect_func()())

            # Sync (limitless_plugin.py:509) queries the STALE ``connection``
            # local captured before the reconnect above -- an obvious defect
            # masked in its tests by a mock query helper. Async re-reads the
            # fresh connection so discovery runs against the just-opened conn.
            new_limitless_routers = await self._query_helper.query_for_limitless_routers(
                context.get_connection(), context.get_host_info().port)

            if new_limitless_routers is not None and len(new_limitless_routers) != 0:
                context.set_limitless_routers(new_limitless_routers)
                self._put_routers(cluster_id, new_limitless_routers, context.get_props())
            else:
                raise AwsWrapperError(Messages.get("LimitlessRouterService.FetchedEmptyRouterList"))

    def _is_login_exception(self, error: Optional[Exception] = None) -> bool:
        # Login failures short-circuit out of the retry loop (raised by the
        # callers) instead of burning the router retry budget. Matches sync
        # after the parity-review fix (previously both sides discarded this
        # verdict).
        return self._plugin_service.is_login_exception(error)

    def _get_limitless_routers(self, cluster_id: str) -> List[HostInfo]:
        return AsyncLimitlessRouterCache.get(cluster_id)

    def _put_routers(
            self, cluster_id: str, routers: List[HostInfo], props: Properties) -> None:
        ttl_ns = int(WrapperProperties.LIMITLESS_MONITOR_DISPOSAL_TIME_MS.get_int(props)) * 1_000_000
        AsyncLimitlessRouterCache.put(
            cluster_id, routers, ttl_ns=ttl_ns if ttl_ns > 0 else None)

    def _cluster_id(self, host_info: HostInfo) -> str:
        """Derive the cache key from the host-list provider like sync
        (limitless_plugin.py:353), falling back to the host's hostname so
        single-host / provider-less setups still route."""
        hlp = self._plugin_service.host_list_provider
        if hlp is not None:
            get_cid = getattr(hlp, "get_cluster_id", None)
            if get_cid is not None:
                try:
                    cid = get_cid()
                    if cid:
                        return str(cid)
                except Exception:  # noqa: BLE001 - best-effort
                    pass
        return host_info.host

    # ---- standing-monitor registry -----------------------------------

    def start_monitoring(self, host_info: HostInfo, props: Properties) -> None:
        try:
            cluster_id = self._cluster_id(host_info)
            interval_ms = WrapperProperties.LIMITLESS_INTERVAL_MILLIS.get_int(props)
            AsyncLimitlessRouterService.ensure_monitor(
                self._plugin_service, host_info, props, interval_ms, cluster_id)
        except Exception as e:
            logger.debug("LimitlessRouterService.ErrorStartingMonitor", e)
            raise e

    @classmethod
    def _get_cluster_lock(cls, cluster_id: str) -> asyncio.Lock:
        with cls._registry_lock:
            lock = cls._force_get_routers_locks.get(cluster_id)
            if lock is None:
                lock = asyncio.Lock()
                cls._force_get_routers_locks[cluster_id] = lock
            return lock

    @classmethod
    def ensure_monitor(
            cls,
            plugin_service: AsyncPluginService,
            host_info: HostInfo,
            props: Properties,
            interval_ms: int,
            cluster_id: str) -> AsyncLimitlessRouterMonitor:
        with cls._registry_lock:
            existing = cls._monitors.get(cluster_id)
            if existing is not None and existing.is_running():
                return existing
            monitor = AsyncLimitlessRouterMonitor(
                plugin_service, host_info, props, interval_ms, cluster_id)
            cls._monitors[cluster_id] = monitor
        monitor.start()
        register_shutdown_hook(monitor.stop)
        return monitor

    @classmethod
    async def dispose_stale_monitors(cls, now_ns: Optional[int] = None) -> None:
        """Stop and drop monitors idle beyond LIMITLESS_MONITOR_DISPOSAL_TIME_MS.

        Async stand-in for sync's periodic MonitorService cleanup: triggered on
        new connection activity rather than by a background thread.
        """
        now = now_ns if now_ns is not None else time.perf_counter_ns()
        to_stop: List[AsyncLimitlessRouterMonitor] = []
        with cls._registry_lock:
            for cid, monitor in list(cls._monitors.items()):
                if monitor.is_stale(now):
                    to_stop.append(monitor)
                    cls._monitors.pop(cid, None)
        for monitor in to_stop:
            try:
                await monitor.stop()
            except Exception as e:  # noqa: BLE001
                logger.debug("LimitlessRouterService.ErrorClosingMonitor", e)

    @classmethod
    async def stop_all(cls) -> None:
        """Stop every registered monitor. Intended for shutdown / test cleanup."""
        with cls._registry_lock:
            monitors = list(cls._monitors.values())
            cls._monitors.clear()
        for m in monitors:
            await m.stop()

    @classmethod
    def _reset_for_tests(cls) -> None:
        with cls._registry_lock:
            cls._monitors.clear()
            cls._force_get_routers_locks.clear()


class AsyncLimitlessPlugin(AsyncPlugin):
    """Async counterpart of :class:`LimitlessPlugin`."""

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._limitless_router_service = AsyncLimitlessRouterService(
            plugin_service, AsyncLimitlessQueryHelper(plugin_service))
        self._context: Optional[AsyncLimitlessContext] = None

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        dialect = self._plugin_service.database_dialect
        if not isinstance(dialect, AuroraLimitlessDialect):
            refreshed_dialect = self._plugin_service.database_dialect
            if not isinstance(refreshed_dialect, AuroraLimitlessDialect):
                raise UnsupportedOperationError(Messages.get_formatted(
                    "LimitlessPlugin.UnsupportedDialectOrDatabase",
                    type(refreshed_dialect).__name__))

        if is_initial_connection:
            await self._limitless_router_service.dispose_stale_monitors()
            self._limitless_router_service.start_monitoring(host_info, props)

        context = AsyncLimitlessContext(
            host_info, props, None, connect_func, [], self, self._plugin_service)
        self._context = context

        await self._limitless_router_service.establish_connection(context)
        connection = context.get_connection()
        if connection is not None and not await self._plugin_service.driver_dialect.is_closed(connection):
            return connection

        raise AwsWrapperError(Messages.get_formatted(
            "LimitlessPlugin.FailedToConnectToHost", host_info.host))


__all__ = [
    "AsyncLimitlessPlugin",
    "AsyncLimitlessRouterService",
    "AsyncLimitlessRouterMonitor",
    "AsyncLimitlessRouterCache",
    "AsyncLimitlessQueryHelper",
    "AsyncLimitlessContext",
]
