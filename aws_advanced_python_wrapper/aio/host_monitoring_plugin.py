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

"""Async Enhanced Failure Monitoring (EFM) plugin -- faithful port of the sync
``host_monitoring_v2_plugin.py`` (the "v2"/EFM2 design).

Behaviourally equal to sync v2, but async-idiomatic (asyncio tasks instead of
threads). The load-bearing v2 semantics preserved here:

* **Dedicated monitoring connection.** The monitor opens its OWN connection via
  ``plugin_service.force_connect`` and probes THAT (never the in-band
  application connection). Sync ref: ``HostMonitorV2.check_connection_status``.
* **Shared monitors.** One monitor per ``"{time}:{interval}:{count}:{url}"`` key
  is shared across every connection with the same parameters, held in a
  module-level registry with sliding idle expiry. Sync ref:
  ``MonitorServiceV2.get_monitor`` + ``_CACHE_CLEANUP_NANO``.
* **Execute-window-bounded monitoring.** Each network-bound execute registers a
  :class:`_AsyncMonitoringContext` (a weakref to the app connection) on the
  monitor at execute start and deactivates it in ``finally``. The monitor only
  probes while a context is active. Sync ref: ``HostMonitoringV2Plugin.execute``.
* **Duration-based failure math.** A host is dead once
  ``invalid_duration >= interval * max(0, count - 1)`` -- NOT a consecutive
  count. Sync ref: ``HostMonitorV2._update_host_health_status``.
* **Abort on death.** When the host is declared dead the monitor aborts every
  active context's app connection through ``driver_dialect.abort_connection``.
  For psycopg that severs the socket (SHUT_RDWR); on a single event loop the
  suspended read wakes promptly with an OSError/OperationalError that the
  failover plugin classifies as a connection loss. See the abort-semantics note
  on :class:`AsyncHostMonitorV2._abort_connection`.

Leak fix (F17): the plugin does NOT register a per-instance shutdown hook (the
old design's ``register_shutdown_hook(self._shutdown)`` retained every plugin --
and its connection -- in the global hook list forever). Monitors live in the
module registry with their own lifecycle and a SINGLE module-level shutdown hook
stops them all; per-execute contexts die with their execute scope via weakrefs.
Nothing module-level retains the plugin, so a closed connection's plugin is
collectable.
"""

from __future__ import annotations

import asyncio
import time
import weakref
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, List,
                    Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.cleanup import (cancel_task_threadsafe,
                                                     register_shutdown_hook)
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import (PropertiesUtils,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

if TYPE_CHECKING:
    from weakref import ReferenceType

    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.utils.telemetry.telemetry import (
        TelemetryCounter, TelemetryFactory)

logger = Logger(__name__)

# Idle poll cadence when no context is active (sync HostMonitorV2._THREAD_SLEEP).
_THREAD_SLEEP_SEC = 0.1
# Sliding idle-expiry for a shared monitor with no contexts (sync
# MonitorServiceV2._CACHE_CLEANUP_NANO == 1 minute).
_MONITOR_EXPIRATION_SEC = 60.0
# Default monitoring-connection connect timeout when the user configured none
# (sync host_monitoring_plugin.py Monitor._DEFAULT_CONNECT_TIMEOUT_SEC). Async
# connects to an unreachable host hang without a bound, so always supply one.
_DEFAULT_MONITORING_CONNECT_TIMEOUT_SEC = 10

# Module-level shared-monitor registry, keyed "{time}:{interval}:{count}:{url}".
# Holds monitors (which reference the plugin service, NOT the plugin), so a
# closed connection's plugin stays collectable. Sliding-expiry disposal is
# opportunistic (on next monitor request) + on host-deleted notifications +
# via the single module shutdown hook.
_monitors: Dict[str, AsyncHostMonitorV2] = {}
_shutdown_hook_registered: bool = False


def _monitor_key(
        time_ms: int, interval_ms: int, count: int, host_url: str) -> str:
    return "{}:{}:{}:{}".format(time_ms, interval_ms, count, host_url)


async def _abort_target_connection(
        driver_dialect: AsyncDriverDialect, connection: Any) -> None:
    """Abort ``connection`` best-effort, unwrapping a pooled proxy first.

    A pooled connection is a proxy whose ``close()`` merely returns it to the
    pool (socket stays open); ``driver_connection`` unwraps it to the real
    connection so an in-flight blackholed query is actually severed. A non-pooled
    connection has no ``driver_connection`` so it is aborted directly.
    """
    target = getattr(connection, "driver_connection", connection)
    if target is None:
        return
    try:
        if await driver_dialect.is_closed(target):
            return
        await driver_dialect.abort_connection(target)
    except Exception as ex:  # noqa: BLE001 - abort is best-effort (socket may be dead)
        logger.debug("HostMonitorV2.ExceptionAbortingConnection", ex)


def _register_shutdown_hook_once() -> None:
    global _shutdown_hook_registered
    if not _shutdown_hook_registered:
        register_shutdown_hook(_stop_all_monitors)
        _shutdown_hook_registered = True


def _cleanup_idle_monitors() -> None:
    """Dispose monitors that are idle past the sliding-expiry window, or whose
    task/loop is no longer usable. Opportunistic: called on each monitor
    request so cleanup piggybacks on activity rather than a standing task."""
    now = time.monotonic()
    for key, monitor in list(_monitors.items()):
        if not monitor.is_usable() or (
                monitor.can_dispose and now - monitor.last_used >= _MONITOR_EXPIRATION_SEC):
            monitor.stop()
            _monitors.pop(key, None)


def _get_or_create_monitor(
        plugin_service: AsyncPluginService,
        host_info: HostInfo,
        props: Properties,
        failure_detection_time_ms: int,
        failure_detection_interval_ms: int,
        failure_detection_count: int,
        aborted_counter: Optional[TelemetryCounter]) -> AsyncHostMonitorV2:
    _cleanup_idle_monitors()
    key = _monitor_key(
        failure_detection_time_ms, failure_detection_interval_ms,
        failure_detection_count, host_info.url)
    existing = _monitors.get(key)
    if existing is not None and existing.is_usable():
        existing.touch()
        return existing
    if existing is not None:
        # Stale (task done or bound to a closed loop): replace it.
        existing.stop()
    monitor = AsyncHostMonitorV2(
        plugin_service, host_info, props, failure_detection_time_ms,
        failure_detection_interval_ms, failure_detection_count,
        aborted_counter, key)
    monitor.start()
    _monitors[key] = monitor
    _register_shutdown_hook_once()
    return monitor


def _stop_monitors_for_hosts(host_urls: Set[str]) -> None:
    """Stop + drop shared monitors whose host was deleted from the topology."""
    for key, monitor in list(_monitors.items()):
        if monitor.host_url in host_urls:
            logger.debug("HostMonitorV2.StopMonitoringThread", monitor.host)
            monitor.stop()
            _monitors.pop(key, None)


async def _stop_all_monitors() -> None:
    """Module-level shutdown hook: stop and drain every shared monitor.

    Registered ONCE (not per plugin) so the global hook list never retains a
    plugin or connection. Idempotent.
    """
    global _shutdown_hook_registered
    monitors = list(_monitors.values())
    _monitors.clear()
    _shutdown_hook_registered = False
    for monitor in monitors:
        monitor.stop()
    if monitors:
        await asyncio.gather(
            *(monitor.aclose() for monitor in monitors), return_exceptions=True)


def _reset_monitor_registry() -> None:
    """Testing helper: stop and drop all monitors without awaiting.

    Leftover tasks belong to already-closed event loops (each ``asyncio.run``
    creates a fresh loop), so cancelling and forgetting them is sufficient.
    """
    global _shutdown_hook_registered
    for monitor in list(_monitors.values()):
        monitor.stop()
    _monitors.clear()
    _shutdown_hook_registered = False


class _AsyncMonitoringContext:
    """Per-execute monitoring context: a weakref to the app connection plus the
    "should abort" flag the monitor sets when the host dies. Weakref-based so a
    finished execute's connection stays collectable. Sync ref: ``MonitoringContext``.
    """

    def __init__(self, connection: Any) -> None:
        self._connection_ref: Optional[ReferenceType] = weakref.ref(connection)
        self._host_unhealthy: bool = False

    def set_host_unhealthy(self) -> None:
        self._host_unhealthy = True

    def should_abort(self) -> bool:
        return self._host_unhealthy and self.get_connection() is not None

    def set_inactive(self) -> None:
        self._connection_ref = None

    def get_connection(self) -> Optional[Any]:
        if self._connection_ref is None:
            return None
        return self._connection_ref()

    def is_active(self) -> bool:
        return self.get_connection() is not None


class AsyncHostMonitorV2:
    """Monitors one server for one or more active connections. Runs a single
    asyncio task that probes a dedicated monitoring connection, marks the host
    UNAVAILABLE when it dies, and aborts every active context's connection.
    Async-idiomatic port of sync ``HostMonitorV2`` (the sync version splits this
    across two threads -- a probe loop and a new-context promoter -- which merge
    cleanly into one task here).
    """

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_info: HostInfo,
            props: Properties,
            failure_detection_time_ms: int,
            failure_detection_interval_ms: int,
            failure_detection_count: int,
            aborted_counter: Optional[TelemetryCounter],
            key: str) -> None:
        self._plugin_service = plugin_service
        self._host_info = host_info
        self._props = props
        self._key = key
        self._telemetry_factory: TelemetryFactory = plugin_service.get_telemetry_factory()
        self._failure_detection_time_sec = failure_detection_time_ms / 1000.0
        self._interval_sec = failure_detection_interval_ms / 1000.0
        self._failure_detection_count = failure_detection_count
        self._aborted_counter = aborted_counter
        self._driver_dialect: AsyncDriverDialect = plugin_service.driver_dialect

        # (activation_time, weakref(context)) pairs still inside their grace
        # window, and the promoted-and-active context weakrefs.
        self._new_contexts: List[Tuple[float, ReferenceType]] = []
        self._active_contexts: List[ReferenceType] = []

        self._is_unhealthy = False
        self._failure_count = 0
        self._invalid_host_start_time = 0.0
        self._monitoring_connection: Optional[Any] = None

        self._stop_event = asyncio.Event()
        self._stopped = False
        self._last_used = time.monotonic()
        self._task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ---- lifecycle / bookkeeping --------------------------------------

    def start(self) -> None:
        # Always called from within the running loop (execute -> start_monitoring).
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.create_task(self._run())

    def touch(self) -> None:
        self._last_used = time.monotonic()

    @property
    def last_used(self) -> float:
        return self._last_used

    @property
    def host(self) -> str:
        return self._host_info.host

    @property
    def host_url(self) -> str:
        return self._host_info.url

    @property
    def can_dispose(self) -> bool:
        return not self._active_contexts and not self._new_contexts

    def is_usable(self) -> bool:
        if self._stopped or self._task is None or self._task.done():
            return False
        try:
            return self._loop is asyncio.get_running_loop()
        except RuntimeError:
            return False

    def stop(self) -> None:
        """Signal the monitor to stop (sync, fire-and-forget). The task drains
        on its own loop; :meth:`aclose` awaits it fully."""
        self._stopped = True
        self._stop_event.set()
        if self._task is not None and not self._task.done():
            cancel_task_threadsafe(self._task, self._loop)

    async def aclose(self) -> None:
        """Await the monitor task's full teardown."""
        self.stop()
        if self._task is not None:
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None

    def start_monitoring(self, context: _AsyncMonitoringContext) -> None:
        if self._stopped:
            logger.warning("HostMonitorV2.MonitorIsStopped", self._host_info.host)
        self.touch()
        activation = time.monotonic() + self._failure_detection_time_sec
        self._new_contexts.append((activation, weakref.ref(context)))

    # ---- monitor loop -------------------------------------------------

    async def _run(self) -> None:
        logger.debug("HostMonitorV2.StartMonitoringThread", self._host_info.host)
        try:
            while not self._stop_event.is_set():
                self._promote_new_contexts(time.monotonic())

                if not self._active_contexts and not self._is_unhealthy:
                    await asyncio.sleep(_THREAD_SLEEP_SEC)
                    continue

                status_check_start = time.monotonic()
                is_valid = await self._check_connection_status()
                status_check_end = time.monotonic()

                self._update_host_health_status(
                    is_valid, status_check_start, status_check_end)

                if self._is_unhealthy:
                    self._plugin_service.set_availability(
                        self._host_info.as_aliases(), HostAvailability.UNAVAILABLE)

                await self._drain_active_contexts()

                delay = self._interval_sec - (status_check_end - status_check_start)
                await asyncio.sleep(delay if delay > _THREAD_SLEEP_SEC else _THREAD_SLEEP_SEC)
        except asyncio.CancelledError:
            pass
        except Exception as ex:  # noqa: BLE001 - mirror sync: log + stop
            logger.debug(
                "HostMonitorV2.ExceptionDuringMonitoringStop", self._host_info.host, ex)
        finally:
            self._stopped = True
            self._stop_event.set()
            if self._monitoring_connection is not None:
                await _abort_target_connection(
                    self._driver_dialect, self._monitoring_connection)
                self._monitoring_connection = None
            logger.debug("HostMonitorV2.StopMonitoringThread", self._host_info.host)

    def _promote_new_contexts(self, now: float) -> None:
        """Move contexts whose grace window has elapsed into the active set."""
        if not self._new_contexts:
            return
        still_pending: List[Tuple[float, ReferenceType]] = []
        for activation, context_ref in self._new_contexts:
            if now < activation:
                still_pending.append((activation, context_ref))
                continue
            context = context_ref()
            if context is not None and context.is_active():
                self._active_contexts.append(context_ref)
        self._new_contexts = still_pending

    async def _drain_active_contexts(self) -> None:
        survivors: List[ReferenceType] = []
        for context_ref in self._active_contexts:
            if self._stop_event.is_set():
                break
            context = context_ref()
            if context is None:
                continue
            if self._is_unhealthy:
                context.set_host_unhealthy()
                connection_to_abort = context.get_connection()
                if connection_to_abort is not None:
                    await _abort_target_connection(
                        self._driver_dialect, connection_to_abort)
                    if self._aborted_counter is not None:
                        self._aborted_counter.inc()
                context.set_inactive()
            elif context.is_active():
                survivors.append(context_ref)
        self._active_contexts = survivors

    async def _check_connection_status(self) -> bool:
        telemetry_context = self._telemetry_factory.open_telemetry_context(
            "connection status check", TelemetryTraceLevel.FORCE_TOP_LEVEL)
        if telemetry_context is not None:
            telemetry_context.set_attribute("url", self._host_info.url)
        try:
            if self._monitoring_connection is None or await self._driver_dialect.is_closed(
                    self._monitoring_connection):
                monitoring_properties = PropertiesUtils.create_monitoring_properties(self._props)
                if monitoring_properties.get(WrapperProperties.CONNECT_TIMEOUT_SEC.name) is None:
                    monitoring_properties[WrapperProperties.CONNECT_TIMEOUT_SEC.name] = \
                        _DEFAULT_MONITORING_CONNECT_TIMEOUT_SEC
                logger.debug("HostMonitorV2.OpeningMonitoringConnection", self._host_info.url)
                self._monitoring_connection = await self._plugin_service.force_connect(
                    self._host_info, monitoring_properties)
                logger.debug("HostMonitorV2.OpenedMonitoringConnection", self._host_info.url)
                return True
            return await self._is_host_available(
                self._monitoring_connection, self._valid_probe_timeout_sec())
        except Exception:  # noqa: BLE001 - any probe failure counts as unavailable
            return False
        finally:
            if telemetry_context is not None:
                telemetry_context.close_context()

    def _valid_probe_timeout_sec(self) -> float:
        timeout = (self._interval_sec - _THREAD_SLEEP_SEC) / 2
        return timeout if timeout > _THREAD_SLEEP_SEC else _THREAD_SLEEP_SEC

    async def _is_host_available(self, conn: Any, timeout_sec: float) -> bool:
        try:
            return bool(await asyncio.wait_for(
                self._driver_dialect.ping(conn), timeout=timeout_sec))
        except Exception:  # noqa: BLE001 - TimeoutError or driver error => unavailable
            return False

    def _update_host_health_status(
            self,
            connection_valid: bool,
            status_check_start: float,
            status_check_end: float) -> None:
        """Duration-based health accrual (sync HostMonitorV2._update_host_health_status).

        A host is dead once it has been continuously invalid for at least
        ``interval * max(0, count - 1)`` seconds -- NOT after ``count``
        consecutive probe failures.
        """
        if not connection_valid:
            self._failure_count += 1
            if self._invalid_host_start_time == 0.0:
                self._invalid_host_start_time = status_check_start
            invalid_host_duration = status_check_end - self._invalid_host_start_time
            max_invalid_host_duration = self._interval_sec * max(
                0, self._failure_detection_count - 1)
            if invalid_host_duration >= max_invalid_host_duration:
                logger.debug("HostMonitorV2.HostDead", self._host_info.host)
                self._is_unhealthy = True
                return
            logger.debug(
                "HostMonitorV2.HostNotResponding", self._host_info.host, self._failure_count)
            return

        if self._failure_count > 0:
            logger.debug("HostMonitorV2.HostAlive", self._host_info.host)
        self._failure_count = 0
        self._invalid_host_start_time = 0.0
        self._is_unhealthy = False

    async def _abort_connection(self, connection: Any) -> None:
        """Abort an application connection.

        Async abort semantics: the monitor runs as a separate task on the SAME
        event loop as the awaiting execute. Aborting through
        ``driver_dialect.abort_connection`` severs the socket -- for psycopg via
        ``socket.shutdown(SHUT_RDWR)`` on the connection's fd -- which makes the
        loop's selector wake the suspended read promptly (even on a blackholed
        host) with an OSError/OperationalError. That error propagates out of the
        in-flight ``execute`` as a normal (``Exception``-derived) connection
        loss, which the failover plugin's ``except Exception`` classifies and
        acts on. We deliberately do NOT cancel the awaiting task: an
        ``asyncio.CancelledError`` is a ``BaseException`` and would slip past the
        failover plugin, defeating EFM+failover. Socket-sever is the async
        equivalent of sync's cross-thread ``abort_connection``.
        """
        await _abort_target_connection(self._driver_dialect, connection)


class _AsyncMonitorServiceV2:
    """Per-plugin façade over the shared module-level monitor registry.

    Owns the ``efm2.connections.aborted`` telemetry counter (shared with the
    monitors it creates). Async port of sync ``MonitorServiceV2``.
    """

    def __init__(self, plugin_service: AsyncPluginService) -> None:
        self._plugin_service = plugin_service
        telemetry_factory = plugin_service.get_telemetry_factory()
        self._aborted_counter: Optional[TelemetryCounter] = \
            telemetry_factory.create_counter("efm2.connections.aborted")

    async def start_monitoring(
            self,
            conn: Any,
            host_info: HostInfo,
            props: Properties,
            failure_detection_time_ms: int,
            failure_detection_interval_ms: int,
            failure_detection_count: int) -> _AsyncMonitoringContext:
        monitor = _get_or_create_monitor(
            self._plugin_service, host_info, props, failure_detection_time_ms,
            failure_detection_interval_ms, failure_detection_count,
            self._aborted_counter)
        context = _AsyncMonitoringContext(conn)
        monitor.start_monitoring(context)
        return context

    async def stop_monitoring(
            self, context: _AsyncMonitoringContext, connection_to_abort: Any) -> None:
        if context.should_abort():
            context.set_inactive()
            try:
                await _abort_target_connection(
                    self._plugin_service.driver_dialect, connection_to_abort)
                if self._aborted_counter is not None:
                    self._aborted_counter.inc()
            except AwsWrapperError as ex:
                logger.debug("MonitorServiceV2.ExceptionAbortingConnection", ex)
        else:
            context.set_inactive()


class AsyncHostMonitoringPlugin(AsyncPlugin):
    """Async Host Monitoring (EFM v2) plugin. One instance per connection;
    shares monitors across connections via the module registry."""

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        dialect = plugin_service.driver_dialect
        if not dialect.supports_abort_connection():
            raise AwsWrapperError(Messages.get_formatted(
                "HostMonitoringV2Plugin.ConfigurationNotSupported", type(dialect).__name__))

        self._plugin_service = plugin_service
        self._properties = props
        self._monitoring_host_info: Optional[HostInfo] = None
        self._rds_utils = RdsUtils()
        self._monitor_service: Optional[_AsyncMonitorServiceV2] = _AsyncMonitorServiceV2(plugin_service)
        self._failure_detection_time_ms = WrapperProperties.FAILURE_DETECTION_TIME_MS.get_int(props)
        self._failure_detection_interval_ms = WrapperProperties.FAILURE_DETECTION_INTERVAL_MS.get_int(props)
        self._failure_detection_count = WrapperProperties.FAILURE_DETECTION_COUNT.get_int(props)
        self._failure_detection_enabled = WrapperProperties.FAILURE_DETECTION_ENABLED.get_bool(props)

        # Subscribe to connect (for cluster-alias resolution) + every
        # network-bound method (so execute() brackets them with monitoring).
        # notify_* hooks are dispatched to all plugins regardless of this set.
        self._subscribed: Set[str] = {DbApiMethod.CONNECT.method_name}
        self._subscribed.update(plugin_service.network_bound_methods)

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._subscribed

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        connection = await connect_func()
        if connection is not None:
            rds_type = self._rds_utils.identify_rds_type(host_info.host)
            if rds_type.is_rds_cluster:
                host_info.reset_aliases()
                await self._fill_aliases(connection, host_info)
        return connection

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if self._plugin_service.current_connection is None:
            raise AwsWrapperError(Messages.get_formatted(
                "HostMonitoringV2Plugin.ConnectionNone", method_name))
        if (not self._failure_detection_enabled
                or self._monitor_service is None
                or not self._plugin_service.is_network_bound_method(method_name)):
            return await execute_func()

        monitor_context: Optional[_AsyncMonitoringContext] = None
        try:
            logger.debug("HostMonitoringV2Plugin.ActivatedMonitoring", method_name)
            monitor_context = await self._monitor_service.start_monitoring(
                self._plugin_service.current_connection,
                await self._get_monitoring_host_info(),
                self._properties,
                self._failure_detection_time_ms,
                self._failure_detection_interval_ms,
                self._failure_detection_count)
            return await execute_func()
        finally:
            if monitor_context is not None and self._monitor_service is not None:
                await self._monitor_service.stop_monitoring(
                    monitor_context, self._plugin_service.current_connection)
            logger.debug("HostMonitoringV2Plugin.MonitoringDeactivated", method_name)

    def notify_connection_changed(
            self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        if ConnectionEvent.CONNECTION_OBJECT_CHANGED in changes:
            self._monitoring_host_info = None
        return OldConnectionSuggestedAction.NO_OPINION

    def notify_host_list_changed(self, changes: Dict[str, Set[HostEvent]]) -> None:
        """Stop shared monitors for hosts removed from the topology so they do
        not keep probing (and holding the plugin service) after the host is
        gone. Now actually dispatched by the async plugin manager."""
        deleted = {
            url for url, events in changes.items() if HostEvent.HOST_DELETED in events}
        if deleted:
            _stop_monitors_for_hosts(deleted)

    async def _get_monitoring_host_info(self) -> HostInfo:
        if self._monitoring_host_info is not None:
            return self._monitoring_host_info
        current_host_info = self._plugin_service.current_host_info
        if current_host_info is None:
            raise AwsWrapperError(Messages.get("HostMonitoringV2Plugin.HostInfoNone"))
        self._monitoring_host_info = current_host_info
        rds_url_type = self._rds_utils.identify_rds_type(self._monitoring_host_info.host)

        try:
            if not rds_url_type.is_rds_cluster:
                return self._monitoring_host_info
            logger.debug("HostMonitoringV2Plugin.ClusterEndpointHostInfo")
            current_connection = self._plugin_service.current_connection
            # Async approximation of sync's identify_connection + fill_aliases:
            # identify_connection already returns the topology (instance-level)
            # HostInfo with its instance aliases, so it doubles as the monitoring
            # host and the alias source.
            identified = await self._plugin_service.identify_connection(current_connection)
            if identified is None:
                raise AwsWrapperError(Messages.get_formatted(
                    "HostMonitoringV2Plugin.UnableToIdentifyConnection",
                    current_host_info.host,
                    self._plugin_service.host_list_provider))
            self._monitoring_host_info = identified
        except Exception as e:
            if isinstance(e, AwsWrapperError):
                raise
            message = "HostMonitoringV2Plugin.ErrorIdentifyingConnection"
            logger.debug(message, e)
            raise AwsWrapperError(Messages.get_formatted(message, e)) from e
        return self._monitoring_host_info

    async def _fill_aliases(self, connection: Any, host_info: HostInfo) -> None:
        """Add the connection's resolved instance endpoint as an alias of a
        cluster-endpoint host (async approximation of sync ``fill_aliases``:
        ``identify_connection`` + ``as_aliases``). Best-effort."""
        try:
            host_info.add_alias(host_info.as_alias())
            identified = await self._plugin_service.identify_connection(connection)
            if identified is not None:
                host_info.add_alias(*identified.as_aliases())
        except Exception:  # noqa: BLE001 - alias enrichment is best-effort
            pass

    async def release_resources(self) -> None:
        """Drop this plugin's monitor-service reference. The shared monitors are
        module-level and are NOT stopped here (other connections may share them);
        they self-dispose on idle expiry and are stopped by the module shutdown
        hook. Per-execute contexts already died with their execute scope."""
        self._monitor_service = None


# Back-compat alias for the "v2" naming used by the plugin factory / sync parity.
AsyncHostMonitoringV2Plugin = AsyncHostMonitoringPlugin
