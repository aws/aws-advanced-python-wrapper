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

"""Async Enhanced Failure Monitoring (EFM) plugin.

Wraps each network-bound call with a watchdog that spawns an
:class:`asyncio.Task` probing the connection over a short timeout. If
the probe fails, the watchdog aborts the connection so the surrounding
call returns the underlying ``OperationalError`` immediately instead of
waiting for kernel TCP timeouts.

Shares the sync EFM plugin's connection properties:
  * ``failure_detection_enabled``
  * ``failure_detection_time_ms`` -- grace period before probing starts
  * ``failure_detection_interval_ms`` -- probe cadence
  * ``failure_detection_count`` -- consecutive failures before declaring outage
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, FrozenSet,
                    Optional, Set)

from aws_advanced_python_wrapper.aio.cleanup import register_shutdown_hook
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.notifications import (ConnectionEvent,
                                                             HostEvent)
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncHostMonitoringPlugin(AsyncPlugin):
    """Async counterpart of the Host Monitoring Plugin v2.

    Runs a single long-lived monitor task per plugin (one plugin per
    connection in the async wrapper). The task is started LAZILY on the
    first :meth:`execute` call -- we can't start it in ``__init__`` because
    the plugin is constructed before a connection exists. Once running, it
    probes ``driver_dialect.ping(conn)`` on ``interval_ms`` cadence after
    an initial ``grace_ms`` grace period, accumulating consecutive failures
    across probes.

    Phase C.1 only establishes the standing task + failure accumulator.
    C.2 adds the UNAVAILABLE/abort behavior; C.3 adds notify hooks;
    C.4 wires teardown via ``release_resources``.
    """

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._enabled = WrapperProperties.FAILURE_DETECTION_ENABLED.get_bool(
            props
        )
        ms = WrapperProperties.FAILURE_DETECTION_TIME_MS.get(props)
        self._grace_sec = (float(ms) / 1000.0) if ms else 30.0
        ms = WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.get(props)
        self._interval_sec = (float(ms) / 1000.0) if ms else 5.0
        count = WrapperProperties.FAILURE_DETECTION_COUNT.get(props)
        self._failure_count_threshold = int(count) if count else 3

        # Standing-task state (Phase C.1). These are populated lazily on the
        # first execute() call; tracked on the instance so C.2 tests (and
        # release_resources in C.4) can observe/manipulate them.
        self._monitor_task: Optional[asyncio.Task] = None
        self._monitor_stop: asyncio.Event = asyncio.Event()
        self._consecutive_failures: int = 0
        self._monitored_aliases: FrozenSet[str] = frozenset()
        self._host_unavailable: bool = False

        # Telemetry counter -- matches sync host_monitoring_plugin.py:592
        # (emitted when the monitor aborts a connection after the failure
        # threshold is reached).
        tf = self._plugin_service.get_telemetry_factory()
        self._aborted_connections_counter = tf.create_counter(
            "efm.aborted_connections.count")

        register_shutdown_hook(self._shutdown)

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
        return await connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if not self._enabled:
            return await execute_func()

        if self._host_unavailable:
            raise AwsWrapperError(
                f"Host {sorted(self._monitored_aliases)} is unavailable after"
                f" {self._failure_count_threshold} consecutive failed health checks.")

        conn = self._plugin_service.current_connection
        host_info = self._plugin_service.current_host_info
        if conn is None or host_info is None:
            return await execute_func()

        self._ensure_monitor_started(conn, host_info)
        return await execute_func()

    def _ensure_monitor_started(self, conn: Any, host_info: HostInfo) -> None:
        """Start the standing monitor task if it's not already running."""
        if self._monitor_task is not None and not self._monitor_task.done():
            return
        self._monitored_aliases = host_info.as_aliases()
        self._monitor_stop = asyncio.Event()
        self._consecutive_failures = 0
        driver_dialect = self._plugin_service.driver_dialect
        self._monitor_task = asyncio.create_task(
            self._monitor(conn, driver_dialect, self._monitor_stop)
        )

    async def _monitor(
            self,
            conn: Any,
            driver_dialect: AsyncDriverDialect,
            stop_event: asyncio.Event) -> None:
        """Standing monitor: probe on ``interval_sec`` cadence, track failures.

        Grace period first, then probe forever until ``stop_event`` fires.
        Threshold-based abort + UNAVAILABLE marking land in C.2.
        """
        try:
            # Grace period: don't probe immediately -- long-running but
            # healthy queries shouldn't trigger the monitor.
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._grace_sec
                )
                return
            except asyncio.TimeoutError:
                pass

            while not stop_event.is_set():
                try:
                    ok = await asyncio.wait_for(
                        driver_dialect.ping(conn), timeout=self._interval_sec
                    )
                except (asyncio.TimeoutError, Exception):
                    ok = False

                if ok:
                    self._consecutive_failures = 0
                else:
                    self._consecutive_failures += 1

                if self._consecutive_failures >= self._failure_count_threshold:
                    await self._mark_host_unavailable(conn, driver_dialect)
                    return

                try:
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=self._interval_sec
                    )
                    return
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            return

    async def _mark_host_unavailable(
            self,
            conn: Any,
            driver_dialect: AsyncDriverDialect) -> None:
        """Threshold breach: mark UNAVAILABLE, abort, flag the plugin.

        Mirrors sync host_monitoring_plugin.py:139-151.
        """
        self._plugin_service.set_availability(
            self._monitored_aliases, HostAvailability.UNAVAILABLE)
        try:
            # Reach the underlying raw driver connection before aborting. The
            # monitored connection may be a ``_PooledAsyncConnectionProxy``
            # whose ``close()`` merely returns it to the pool -- the socket
            # stays open, so aborting the proxy never severs an in-flight
            # (e.g. blackholed) query and it hangs until the kernel/test
            # timeout. ``driver_connection`` unwraps the proxy to the real
            # connection; for a non-pooled connection the getattr falls back to
            # ``conn`` itself, so behavior there is unchanged.
            target = getattr(conn, "driver_connection", conn)
            await driver_dialect.abort_connection(target)
        except Exception:  # noqa: BLE001 - abort is best-effort (socket may be dead)
            pass
        self._host_unavailable = True
        if self._aborted_connections_counter is not None:
            self._aborted_connections_counter.inc()

    def notify_connection_changed(
            self, changes: Set[ConnectionEvent]) -> None:
        """Connection swap -> reset monitor state and cancel the task.

        The next execute starts a fresh monitor against the new connection.
        Mirrors sync host_monitoring_plugin.py:155-160.
        """
        self._consecutive_failures = 0
        self._host_unavailable = False
        self._monitor_stop.set()
        self._monitor_stop = asyncio.Event()
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            self._monitor_task = None

    def notify_host_list_changed(
            self, changes: Dict[str, Set[HostEvent]]) -> None:
        """Topology change -> stop monitor if our host went down.

        Mirrors sync host_monitoring_plugin.py:162-175.
        """
        if not self._monitored_aliases:
            return
        for alias, events in changes.items():
            if alias not in self._monitored_aliases:
                continue
            if HostEvent.WENT_DOWN in events or HostEvent.HOST_DELETED in events:
                self._monitor_stop.set()
                return

    async def _shutdown(self) -> None:
        """Cancel the standing monitor task cleanly.

        Registered with aio.cleanup.register_shutdown_hook in __init__ so
        users calling release_resources_async() on exit don't leak the
        monitor task. Idempotent -- safe to call multiple times.
        """
        self._monitor_stop.set()
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except (asyncio.CancelledError, Exception):
                pass
            self._monitor_task = None


# Optional alias for consistency with sync "v2" naming.
AsyncHostMonitoringV2Plugin = AsyncHostMonitoringPlugin
