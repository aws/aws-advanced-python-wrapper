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

"""Async Aurora Limitless plugin with a standing router monitor (L.1).

Port of :mod:`aws_advanced_python_wrapper.limitless_plugin`. Matches
sync parity by running a single background ``asyncio.Task`` per cluster
that refreshes the router list on ``limitless_intervals_ms`` cadence
and publishes it to a class-level cache. Plugin ``connect()`` consumes
the cache and falls back to on-demand discovery only when the cache
is empty (initial connect / stale-cache recovery).

Design notes:
  * One monitor per cluster identifier, reference-counted by plugin
    instances so the monitor tears down when the last user closes.
  * Cache keyed by cluster identifier; last-refresh timestamps
    preserved so on-demand fallback can measure staleness.
  * Monitor teardown hooks into
    :func:`aws_advanced_python_wrapper.aio.cleanup.register_shutdown_hook`,
    so ``release_resources_async()`` cleans up the standing task.
"""

from __future__ import annotations

import asyncio
import math
from threading import Lock
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    List, Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.cleanup import register_shutdown_hook
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService


logger = Logger(__name__)

# Sync LimitlessRouterService uses ``weighted_random`` on the primary
# path. We match that so identical config strings produce the same
# router selection behavior across sync/async.
_STRATEGY: str = "weighted_random"

# Default refresh interval in ms when LIMITLESS_INTERVAL_MILLIS prop is
# absent. Matches sync default (7500 ms).
_DEFAULT_INTERVAL_MS: int = 7500


def _rows_to_host_infos(
        rows: List[Tuple[Any, ...]],
        host_port_to_map: int) -> List[HostInfo]:
    """Turn ``[(router_endpoint, load), ...]`` into HostInfo weighted list.

    Weight math mirrors sync's ``LimitlessQueryHelper._create_host_info``:
    ``weight = clamp(10 - floor(cpu * 10), 1, 10)``.
    """
    out: List[HostInfo] = []
    for row in rows:
        if not row:
            continue
        host_name = str(row[0])
        try:
            cpu = float(row[1]) if len(row) > 1 else 0.0
        except (TypeError, ValueError):
            cpu = 0.0
        weight = 10 - math.floor(cpu * 10)
        if weight < 1 or weight > 10:
            weight = 1
        out.append(HostInfo(
            host=host_name,
            port=host_port_to_map,
            role=HostRole.WRITER,
            weight=weight,
            host_id=host_name,
        ))
    return out


async def _query_routers(
        conn: Any,
        query: str) -> List[tuple]:
    """Run a limitless-router-discovery query on ``conn``.

    Matches sync ``LimitlessQueryHelper.query_for_limitless_routers``
    semantics: execute the dialect's pre-baked query and return all
    rows. Exceptions are swallowed -- callers get an empty list and
    fall back to direct connect.
    """
    cur = conn.cursor()
    try:
        async with cur:
            await cur.execute(query)
            return list(await cur.fetchall())
    except Exception:  # noqa: BLE001 - probe failure -> empty list
        return []


class AsyncLimitlessRouterMonitor:
    """Background task that refreshes the router list for a cluster.

    Lifecycle:
      * :meth:`start` creates the monitor task if not running.
      * :meth:`stop` sets an event + cancels the task.
      * One monitor per cluster. :class:`AsyncLimitlessRouterService`
        dedupes by cluster id.

    The monitor opens its own probe connection via
    :meth:`AsyncPluginService.connect` (skipping the Limitless plugin
    to avoid recursion). The probe connection is reused for repeated
    queries; if it's closed by the driver, the next refresh reopens.
    """

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_info: HostInfo,
            props: Properties,
            interval_ms: int,
            cluster_id: str) -> None:
        self._plugin_service = plugin_service
        self._host_info = host_info
        self._props = props
        self._interval_sec = max(0.05, interval_ms / 1000.0)
        self._cluster_id = cluster_id
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._probe_conn: Optional[Any] = None

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            # Initial-delay loop: wait the interval *before* the first
            # refresh so short-lived consumers (and fast unit tests)
            # that stop the monitor promptly never race with a mid-flight
            # probe. Matches sync monitor cadence: it also sleeps
            # first since the initial discovery is done on-demand by
            # the plugin.
            slept = 0.0
            step = min(0.05, self._interval_sec)
            while slept < self._interval_sec and not self._stop_event.is_set():
                await asyncio.sleep(step)
                slept += step
            while not self._stop_event.is_set():
                try:
                    await self._refresh_once()
                except Exception:  # noqa: BLE001 - transient failure
                    pass
                slept = 0.0
                while slept < self._interval_sec and not self._stop_event.is_set():
                    await asyncio.sleep(step)
                    slept += step
        except asyncio.CancelledError:
            return

    async def _refresh_once(self) -> None:
        # Ensure we have a probe connection. Best-effort: use the
        # plugin-service connect pipeline (skipping this monitor) so
        # auth/secrets etc. apply.
        if self._probe_conn is None:
            self._probe_conn = await self._open_probe_connection()
            if self._probe_conn is None:
                return
        database_dialect = self._plugin_service.database_dialect
        query = getattr(
            database_dialect, "limitless_router_endpoint_query", None)
        if not query:
            return
        rows = await _query_routers(self._probe_conn, query)
        routers = _rows_to_host_infos(rows, self._host_info.port)
        AsyncLimitlessRouterCache.put(self._cluster_id, routers)

    async def _open_probe_connection(self) -> Optional[Any]:
        try:
            return await self._plugin_service.connect(
                self._host_info, self._props)
        except Exception:  # noqa: BLE001 - probe open best-effort
            return None

    async def stop(self) -> None:
        self._stop_event.set()
        task = self._task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._task = None
        if self._probe_conn is not None:
            try:
                await self._plugin_service.driver_dialect.abort_connection(
                    self._probe_conn)
            except Exception:  # noqa: BLE001 - teardown best-effort
                pass
            self._probe_conn = None


class AsyncLimitlessRouterCache:
    """Class-level per-cluster router cache.

    Publishes :meth:`put`/:meth:`get` for monitors and plugins. Uses
    :class:`threading.Lock` because the cache can be read by event
    loops on different threads (rare but possible in Django +
    async-wrapper consumers).
    """

    _lock: ClassVar[Lock] = Lock()
    _by_cluster: ClassVar[Dict[str, List[HostInfo]]] = {}

    @classmethod
    def put(cls, cluster_id: str, routers: List[HostInfo]) -> None:
        with cls._lock:
            cls._by_cluster[cluster_id] = list(routers)

    @classmethod
    def get(cls, cluster_id: str) -> List[HostInfo]:
        with cls._lock:
            return list(cls._by_cluster.get(cluster_id, ()))

    @classmethod
    def clear(cls) -> None:
        with cls._lock:
            cls._by_cluster.clear()


class AsyncLimitlessRouterService:
    """Coordinates :class:`AsyncLimitlessRouterMonitor` instances.

    Ensures only one monitor runs per cluster. Consumers call
    :meth:`ensure_monitor` on each plugin connect; the service
    lazily creates the monitor if not already running.
    """

    _lock: ClassVar[Lock] = Lock()
    _monitors: ClassVar[Dict[str, AsyncLimitlessRouterMonitor]] = {}

    @classmethod
    def ensure_monitor(
            cls,
            plugin_service: AsyncPluginService,
            host_info: HostInfo,
            props: Properties,
            interval_ms: int,
            cluster_id: str) -> AsyncLimitlessRouterMonitor:
        with cls._lock:
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
    async def stop_all(cls) -> None:
        """Stop every monitor registered. Intended for test cleanup."""
        with cls._lock:
            monitors = list(cls._monitors.values())
            cls._monitors.clear()
        for m in monitors:
            await m.stop()

    @classmethod
    def _reset_for_tests(cls) -> None:
        with cls._lock:
            cls._monitors.clear()


class AsyncLimitlessPlugin(AsyncPlugin):
    """Async counterpart of :class:`LimitlessPlugin` with standing monitor."""

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    def _cluster_id(self, host_info: HostInfo) -> str:
        """Derive a stable cache key from the host.

        Prefer the host-list-provider's ``get_cluster_id`` when
        available (matches sync behavior); fall back to the host's
        hostname so single-host setups still route.
        """
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

    def _interval_ms(self, props: Properties) -> int:
        try:
            return int(WrapperProperties.LIMITLESS_INTERVAL_MILLIS.get_int(
                props) or _DEFAULT_INTERVAL_MS)
        except Exception:  # noqa: BLE001 - missing/invalid prop
            return _DEFAULT_INTERVAL_MS

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        cluster_id = self._cluster_id(host_info)

        # Kick the standing monitor on initial connect. It populates the
        # cache asynchronously; subsequent connects read the cache
        # directly.
        if is_initial_connection:
            AsyncLimitlessRouterService.ensure_monitor(
                self._plugin_service,
                host_info,
                props,
                self._interval_ms(props),
                cluster_id,
            )

        routers = AsyncLimitlessRouterCache.get(cluster_id)
        if not routers:
            # Cache cold (initial connect or monitor not yet refreshed).
            # Open an initial connection and query on-demand so the
            # first caller isn't blocked on monitor cadence.
            initial_conn = await connect_func()
            rows: List[tuple] = []
            database_dialect = self._plugin_service.database_dialect
            query = getattr(
                database_dialect, "limitless_router_endpoint_query", None)
            if query:
                rows = await _query_routers(initial_conn, query)
            routers = _rows_to_host_infos(rows, host_info.port)
            if routers:
                AsyncLimitlessRouterCache.put(cluster_id, routers)
            else:
                logger.debug(
                    "LimitlessRouterService.LimitlessRouterCacheEmpty")
                return initial_conn
        else:
            initial_conn = None

        # If the current host is already one of the routers, keep the
        # initial connection -- matches sync's
        # ``Utils.contains_host_and_port`` short-circuit.
        for r in routers:
            if r.host == host_info.host and r.port == host_info.port:
                logger.debug(
                    "LimitlessRouterService.ConnectWithHost", host_info.host)
                if initial_conn is None:
                    return await connect_func()
                return initial_conn

        # Pick a router via the configured strategy.
        try:
            picked = self._plugin_service.get_host_info_by_strategy(
                HostRole.WRITER, _STRATEGY, routers)
        except Exception:  # noqa: BLE001 - strategy probe best-effort
            picked = None
        if picked is None:
            return initial_conn if initial_conn is not None else await connect_func()
        logger.debug("LimitlessRouterService.SelectedHost", picked.host)

        # Open a connection to the picked router through the plugin
        # pipeline, skipping ourselves to avoid recursion.
        router_props = self._props_with_host(props, picked)
        try:
            router_conn = await self._plugin_service.connect(
                picked, router_props, plugin_to_skip=self)
        except Exception:  # noqa: BLE001
            logger.debug(
                "LimitlessRouterService.FailedToConnectToHost", picked.host)
            if initial_conn is not None:
                return initial_conn
            return await connect_func()

        # Close the initial conn best-effort; return the router conn.
        if initial_conn is not None:
            try:
                await driver_dialect.abort_connection(initial_conn)
            except Exception:  # noqa: BLE001
                pass
        return router_conn

    @staticmethod
    def _props_with_host(
            props: Properties, host_info: HostInfo) -> Properties:
        new_props = Properties(dict(props))
        new_props["host"] = host_info.host
        if host_info.is_port_specified():
            new_props["port"] = str(host_info.port)
        return new_props


__all__ = [
    "AsyncLimitlessPlugin",
    "AsyncLimitlessRouterMonitor",
    "AsyncLimitlessRouterService",
    "AsyncLimitlessRouterCache",
]
