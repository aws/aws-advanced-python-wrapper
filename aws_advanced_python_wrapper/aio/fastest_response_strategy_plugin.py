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

"""Async Fastest-Response-Strategy plugin with standing monitors (L.2).

Full port of :mod:`aws_advanced_python_wrapper.fastest_response_strategy_plugin`.
Ports:

* :class:`ResponseTimeHolder` -- shared shape with sync.
* :class:`AsyncHostResponseTimeMonitor` -- per-host background
  ``asyncio.Task`` that averages N ping round-trips on an interval.
* :class:`AsyncHostResponseTimeService` -- topology-driven monitor
  lifecycle (spawns monitors for new hosts, tears them down for
  removed hosts).
* :class:`AsyncFastestResponseStrategyPlugin` -- now consults the
  service for measured response times and falls back to Random only
  when no host has ever been measured.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from threading import Lock
from typing import (TYPE_CHECKING, Any, Callable, ClassVar, Dict, List,
                    Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.cleanup import register_shutdown_hook
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import RandomHostSelector
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.utils.notifications import HostEvent
    from aws_advanced_python_wrapper.utils.properties import Properties


logger = Logger(__name__)

_STRATEGY_NAME = "fastest_response"
# Sentinel used when a probe fails / times out. Mirrors sync's MAX_VALUE.
_MAX_RESPONSE_NS = 10 ** 18
_DEFAULT_PROBE_TIMEOUT_SEC = 2.0
_DEFAULT_CACHE_TTL_SEC = 30.0
_DEFAULT_MEASURE_INTERVAL_MS = 30_000
_NUM_OF_MEASURES = 5


@dataclass
class ResponseTimeHolder:
    """Per-host response time snapshot."""
    url: str
    response_time_ns: int


class AsyncHostResponseTimeCache:
    """Class-level per-host-URL response-time cache.

    Mirrors sync's storage-service-backed ``ResponseTimeHolder`` cache
    without the full services-container machinery. Keyed by
    ``HostInfo.url`` so sync+async plugins reading the same topology
    converge on the same identifier.
    """

    _lock: ClassVar[Lock] = Lock()
    _by_url: ClassVar[Dict[str, ResponseTimeHolder]] = {}

    @classmethod
    def put(cls, url: str, response_time_ns: int) -> None:
        with cls._lock:
            cls._by_url[url] = ResponseTimeHolder(url, response_time_ns)

    @classmethod
    def get(cls, url: str) -> int:
        """Return the last recorded response-time ns, or ``_MAX_RESPONSE_NS``
        if the URL has no measurement yet."""
        with cls._lock:
            holder = cls._by_url.get(url)
        return holder.response_time_ns if holder is not None else _MAX_RESPONSE_NS

    @classmethod
    def remove(cls, url: str) -> None:
        with cls._lock:
            cls._by_url.pop(url, None)

    @classmethod
    def clear(cls) -> None:
        with cls._lock:
            cls._by_url.clear()


class AsyncHostResponseTimeMonitor:
    """Per-host background task that measures response time.

    Lifecycle:
      * :meth:`start` creates the task if not already running.
      * :meth:`stop` signals stop + cancels the task.
      * Monitor owns its probe connection; reopens if closed.

    Measurement cadence:
      * ``interval_ms`` between measurement rounds.
      * Each round runs ``_NUM_OF_MEASURES`` pings and averages them.
      * Writes the average (in ns) to :class:`AsyncHostResponseTimeCache`.
    """

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_info: HostInfo,
            props: Properties,
            interval_ms: int) -> None:
        self._plugin_service = plugin_service
        self._host_info = host_info
        self._props = props
        self._interval_sec = max(0.05, interval_ms / 1000.0)
        self._probe_timeout_sec = _DEFAULT_PROBE_TIMEOUT_SEC
        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._probe_conn: Optional[Any] = None

    @property
    def host_info(self) -> HostInfo:
        return self._host_info

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            # Initial-delay loop -- short-lived consumers / fast tests
            # never race with a mid-flight probe.
            await self._sleep_or_stop()
            while not self._stop_event.is_set():
                try:
                    await self._measure_once()
                except Exception:  # noqa: BLE001 - transient
                    pass
                await self._sleep_or_stop()
        except asyncio.CancelledError:
            return

    async def _sleep_or_stop(self) -> None:
        slept = 0.0
        step = min(0.05, self._interval_sec)
        while slept < self._interval_sec and not self._stop_event.is_set():
            await asyncio.sleep(step)
            slept += step

    async def _measure_once(self) -> None:
        if self._probe_conn is None:
            self._probe_conn = await self._open_probe_connection()
            if self._probe_conn is None:
                # Mark host as unreachable so selectors avoid it until
                # the next successful probe.
                AsyncHostResponseTimeCache.put(
                    self._host_info.url, _MAX_RESPONSE_NS)
                return

        driver_dialect = self._plugin_service.driver_dialect
        total_ns = 0
        count = 0
        for _ in range(_NUM_OF_MEASURES):
            if self._stop_event.is_set():
                return
            start_ns = time.perf_counter_ns()
            try:
                ok = await asyncio.wait_for(
                    driver_dialect.ping(self._probe_conn),
                    timeout=self._probe_timeout_sec,
                )
            except Exception:  # noqa: BLE001 - treat as miss
                ok = False
            if ok:
                total_ns += time.perf_counter_ns() - start_ns
                count += 1
            else:
                # Probe conn likely broken -- reset for next measurement.
                await self._close_probe_conn()
                break

        if count > 0:
            avg_ns = total_ns // count
            AsyncHostResponseTimeCache.put(self._host_info.url, avg_ns)
        else:
            AsyncHostResponseTimeCache.put(
                self._host_info.url, _MAX_RESPONSE_NS)

    async def _open_probe_connection(self) -> Optional[Any]:
        try:
            return await self._plugin_service.connect(
                self._host_info, self._props)
        except Exception:  # noqa: BLE001 - probe open best-effort
            return None

    async def _close_probe_conn(self) -> None:
        if self._probe_conn is None:
            return
        try:
            await self._plugin_service.driver_dialect.abort_connection(
                self._probe_conn)
        except Exception:  # noqa: BLE001
            pass
        self._probe_conn = None

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
        await self._close_probe_conn()


class AsyncHostResponseTimeService:
    """Topology-aware monitor-lifecycle coordinator.

    Given a set of hosts, creates one :class:`AsyncHostResponseTimeMonitor`
    per URL. On topology change, tears down monitors for removed hosts
    and spawns monitors for added hosts. Idempotent on ``set_hosts``.
    """

    _lock: ClassVar[Lock] = Lock()
    _monitors: ClassVar[Dict[str, AsyncHostResponseTimeMonitor]] = {}
    # Holds strong references to in-flight stop tasks spawned from
    # ``set_hosts`` so the GC doesn't collect them mid-cancel (which
    # triggers "Task was destroyed but it is pending" warnings under
    # -Werror). Entries self-remove via done_callback.
    _stop_tasks: ClassVar[set] = set()

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties,
            interval_ms: int) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._interval_ms = interval_ms
        self._known_urls: Set[str] = set()

    def set_hosts(self, hosts: Tuple[HostInfo, ...]) -> None:
        # Starting/stopping monitors requires a running event loop
        # (``asyncio.Event`` and ``create_task`` both need one). If we
        # were called outside a loop (sync-style test path, notify fired
        # from a non-asyncio caller), just track the new URL set and
        # let the next in-loop call reconcile.
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self._known_urls = {h.url for h in hosts}
            return

        new_urls = {h.url for h in hosts}
        new_hosts_by_url = {h.url: h for h in hosts}

        to_add = new_urls - self._known_urls
        to_remove = self._known_urls - new_urls

        # Spawn monitors for newly-seen hosts.
        for url in to_add:
            with AsyncHostResponseTimeService._lock:
                existing = AsyncHostResponseTimeService._monitors.get(url)
                if existing is not None and existing.is_running():
                    continue
                monitor = AsyncHostResponseTimeMonitor(
                    self._plugin_service,
                    new_hosts_by_url[url],
                    self._props,
                    self._interval_ms,
                )
                AsyncHostResponseTimeService._monitors[url] = monitor
            monitor.start()
            register_shutdown_hook(monitor.stop)

        # Tear down monitors for removed hosts. Keep a strong ref on
        # each in-flight stop task until it completes; otherwise the
        # event loop may collect the task mid-cancel and surface a
        # "Task was destroyed but it is pending" warning -- fatal
        # under pytest's -Werror. done_callback removes the ref once
        # the stop finishes.
        for url in to_remove:
            with AsyncHostResponseTimeService._lock:
                stopped = AsyncHostResponseTimeService._monitors.get(url)
                if stopped is not None:
                    del AsyncHostResponseTimeService._monitors[url]
            if stopped is not None:
                task = asyncio.create_task(stopped.stop())
                AsyncHostResponseTimeService._stop_tasks.add(task)
                task.add_done_callback(
                    AsyncHostResponseTimeService._stop_tasks.discard)

        self._known_urls = new_urls

    @staticmethod
    def get_response_time_ns(host_info: HostInfo) -> int:
        return AsyncHostResponseTimeCache.get(host_info.url)

    @classmethod
    async def stop_all(cls) -> None:
        with cls._lock:
            monitors = list(cls._monitors.values())
            cls._monitors.clear()
        for m in monitors:
            await m.stop()

    @classmethod
    def _reset_for_tests(cls) -> None:
        with cls._lock:
            cls._monitors.clear()


class AsyncFastestResponseStrategyPlugin(AsyncPlugin):
    """Async counterpart of :class:`FastestResponseStrategyPlugin`.

    Uses measured response times from :class:`AsyncHostResponseTimeService`;
    falls back to :class:`RandomHostSelector` only when no host has ever
    been measured successfully (cold start).
    """

    _SUBSCRIBED: Set[str] = {
        "connect",
        "accepts_strategy",
        "get_host_info_by_strategy",
        "notify_host_list_changed",
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props

        interval_ms = WrapperProperties.RESPONSE_MEASUREMENT_INTERVAL_MS.get_int(
            props)
        if not interval_ms or interval_ms <= 0:
            interval_ms = _DEFAULT_MEASURE_INTERVAL_MS
        self._interval_ms = interval_ms
        self._cache_ttl_sec = interval_ms / 1000.0

        self._response_time_service = AsyncHostResponseTimeService(
            plugin_service, props, interval_ms)

        self._random_selector = RandomHostSelector()

        # Cache keyed by HostRole.name -> (winner, expires_at).
        self._cached_fastest: Dict[str, Tuple[HostInfo, float]] = {}

        self._target_driver_func: Optional[Callable] = None

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    # ---- connect: refresh + seed monitors -----------------------------

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Any]) -> Any:
        self._target_driver_func = target_driver_func
        conn = await connect_func()

        if is_initial_connection:
            try:
                await self._plugin_service.refresh_host_list(conn)
            except Exception:  # noqa: BLE001 - best-effort
                pass
            # Seed monitors from the newly-refreshed topology.
            self._response_time_service.set_hosts(
                self._plugin_service.all_hosts)
        return conn

    # ---- strategy API ------------------------------------------------

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy == _STRATEGY_NAME

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        if strategy != _STRATEGY_NAME:
            logger.error(
                "FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy",
                strategy)
            raise AwsWrapperError(
                Messages.get_formatted(
                    "FastestResponseStrategyPlugin.UnsupportedHostSelectorStrategy",
                    strategy))

        candidates_src = (
            host_list if host_list else list(self._plugin_service.all_hosts))
        candidates = [h for h in candidates_src if h.role == role]
        if not candidates:
            return None

        # Cache hit?
        cached = self._cached_fastest.get(role.name)
        if cached is not None:
            winner, expires_at = cached
            if self._loop_time() < expires_at:
                for h in candidates:
                    if (h.host == winner.host and h.port == winner.port):
                        return h
                self._cached_fastest.pop(role.name, None)

        # Consult the measured-response-time cache.
        best: Optional[HostInfo] = None
        best_ns: int = _MAX_RESPONSE_NS
        for h in candidates:
            rt = AsyncHostResponseTimeService.get_response_time_ns(h)
            if rt < best_ns:
                best = h
                best_ns = rt

        if best is None or best_ns >= _MAX_RESPONSE_NS:
            # Cold start -- no measurement yet. Fall back to random.
            logger.debug("FastestResponseStrategyPlugin.RandomHostSelected")
            return self._random_selector.get_host(
                tuple(candidates), role, self._props)

        self._cached_fastest[role.name] = (
            best, self._loop_time() + self._cache_ttl_sec)
        return best

    # ---- async probe (kept for direct callers / legacy) ---------------

    async def measure_and_cache(
            self, role: HostRole) -> Optional[HostInfo]:
        """Probe every host of ``role`` in parallel, record response times.

        Preserved for direct async callers that want a synchronous
        (awaitable) warmup before querying the strategy.
        """
        candidates = [
            h for h in self._plugin_service.all_hosts if h.role == role]
        if not candidates:
            return None

        driver_dialect = self._plugin_service.driver_dialect
        results = await asyncio.gather(
            *(self._measure_one(h, driver_dialect) for h in candidates),
            return_exceptions=True,
        )

        best: Optional[HostInfo] = None
        best_ns: int = _MAX_RESPONSE_NS
        for host, r in zip(candidates, results):
            if isinstance(r, BaseException):
                continue
            if not isinstance(r, int) or r >= _MAX_RESPONSE_NS:
                continue
            # Publish each probe's result to the shared cache.
            AsyncHostResponseTimeCache.put(host.url, r)
            if r < best_ns:
                best = host
                best_ns = r

        if best is not None:
            self._cached_fastest[role.name] = (
                best, self._loop_time() + self._cache_ttl_sec)
        return best

    async def _measure_one(
            self,
            host_info: HostInfo,
            driver_dialect: AsyncDriverDialect) -> int:
        if self._target_driver_func is None:
            return _MAX_RESPONSE_NS

        conn: Any = None
        start_ns = time.perf_counter_ns()
        try:
            conn = await asyncio.wait_for(
                driver_dialect.connect(
                    host_info, self._props, self._target_driver_func),
                timeout=_DEFAULT_PROBE_TIMEOUT_SEC,
            )
            ok = await asyncio.wait_for(
                driver_dialect.ping(conn),
                timeout=_DEFAULT_PROBE_TIMEOUT_SEC,
            )
            if not ok:
                return _MAX_RESPONSE_NS
            return time.perf_counter_ns() - start_ns
        except Exception as e:  # noqa: BLE001
            logger.debug(
                "HostResponseTimeMonitor.ExceptionDuringMonitoringStop",
                host_info.host, e)
            return _MAX_RESPONSE_NS
        finally:
            if conn is not None:
                try:
                    await driver_dialect.abort_connection(conn)
                except Exception:  # noqa: BLE001
                    pass

    # ---- notifications ------------------------------------------------

    def notify_host_list_changed(
            self, changes: Dict[str, Set[HostEvent]]) -> None:
        self._cached_fastest.clear()
        # Refresh the monitor set to match the new topology.
        self._response_time_service.set_hosts(
            self._plugin_service.all_hosts)

    # ---- helpers ------------------------------------------------------

    @staticmethod
    def _loop_time() -> float:
        try:
            return asyncio.get_running_loop().time()
        except RuntimeError:
            return time.monotonic()


__all__ = [
    "AsyncFastestResponseStrategyPlugin",
    "AsyncHostResponseTimeMonitor",
    "AsyncHostResponseTimeService",
    "AsyncHostResponseTimeCache",
    "ResponseTimeHolder",
]
