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

"""Async Blue-Green deployment plugin (L.3 + M).

Port of sync :mod:`aws_advanced_python_wrapper.blue_green_plugin` with:

- :class:`AsyncBlueGreenStatusMonitor` -- asyncio.Task that polls the
  DB's BG-status query, parses it into interim status, and publishes
  transitions via the provider.
- :class:`AsyncBlueGreenStatusProvider` -- per-cluster singleton that
  consumes interim statuses, builds the routing tables, and writes the
  resulting :class:`BlueGreenStatus` into plugin service status storage.
- :class:`SubstituteConnectRouting`, :class:`SuspendConnectRouting`,
  :class:`SuspendUntilCorrespondingHostFoundConnectRouting`,
  :class:`SuspendExecuteRouting` -- real implementations replacing
  the previous NotImplementedError stubs.

Pragmatic divergence from sync:
- No IP-address tracking or hash-based change-detection
  (optimizations, not correctness).
- Monitor uses a single per-cluster asyncio.Task instead of
  per-role threads; role is disambiguated from each status row.
- Host-list-provider bootstrap deferred -- relies on the plugin
  service's existing host_list_provider machinery.
- No panic-mode recovery reconnect logic (sync's extra safety net
  when the monitor connection dies mid-switchover); best-effort
  reconnect on next iteration instead.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    List, Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.cleanup import register_shutdown_hook
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo


logger = Logger(__name__)


# ---- Enums + data classes --------------------------------------------


class BlueGreenIntervalRate(Enum):
    BASELINE = "BASELINE"
    INCREASED = "INCREASED"
    HIGH = "HIGH"


class BlueGreenPhase(Enum):
    """BG deployment phase, in order.

    ``phase_value`` is the numeric ordering used for comparisons (sync
    parity). ``is_switchover_active_or_completed`` flags phases that
    should engage suspend/substitute routings.
    """

    NOT_CREATED = (0, False)
    CREATED = (1, False)
    PREPARATION = (2, True)
    IN_PROGRESS = (3, True)
    POST = (4, True)
    COMPLETED = (5, True)

    def __new__(cls, value: int, active: bool) -> BlueGreenPhase:
        obj = object.__new__(cls)
        obj._value_ = (value, active)
        return obj

    @property
    def phase_value(self) -> int:
        return self.value[0]

    @property
    def is_switchover_active_or_completed(self) -> bool:
        return self.value[1]

    @staticmethod
    def parse_phase(phase_str: Optional[str]) -> BlueGreenPhase:
        """Parse RDS-side status strings into BG phases.

        Mirrors sync blue_green_plugin.py:90 -- same strings, same
        mapping. Unknown strings default to NOT_CREATED for robustness
        (sync raises ValueError; async is lenient to avoid monitor
        crashes on new RDS engine versions).
        """
        if not phase_str:
            return BlueGreenPhase.NOT_CREATED
        phase_upper = phase_str.upper()
        mapping = {
            "AVAILABLE": BlueGreenPhase.CREATED,
            "SWITCHOVER_INITIATED": BlueGreenPhase.PREPARATION,
            "SWITCHOVER_IN_PROGRESS": BlueGreenPhase.IN_PROGRESS,
            "SWITCHOVER_IN_POST_PROCESSING": BlueGreenPhase.POST,
            "SWITCHOVER_COMPLETED": BlueGreenPhase.COMPLETED,
        }
        return mapping.get(phase_upper, BlueGreenPhase.NOT_CREATED)


class BlueGreenRole(Enum):
    SOURCE = "SOURCE"
    TARGET = "TARGET"

    @staticmethod
    def parse_role(role_str: Optional[str]) -> Optional[BlueGreenRole]:
        if role_str == "BLUE_GREEN_DEPLOYMENT_SOURCE":
            return BlueGreenRole.SOURCE
        if role_str == "BLUE_GREEN_DEPLOYMENT_TARGET":
            return BlueGreenRole.TARGET
        return None


@dataclass
class BlueGreenDbStatusInfo:
    version: str
    endpoint: str
    port: int
    phase: BlueGreenPhase
    role: BlueGreenRole


@dataclass
class BlueGreenStatus:
    bg_id: str
    phase: BlueGreenPhase
    connect_routings: List[ConnectRouting] = field(default_factory=list)
    execute_routings: List[ExecuteRouting] = field(default_factory=list)
    role_by_host: Dict[str, BlueGreenRole] = field(default_factory=dict)
    corresponding_hosts: Dict[str, Tuple[str, Optional[str]]] = field(
        default_factory=dict)

    def get_role(self, host_info: HostInfo) -> Optional[BlueGreenRole]:
        return self.role_by_host.get(host_info.host.lower())


@dataclass
class BlueGreenInterimStatus:
    """Monitor -> Provider hand-off payload."""
    bg_id: str
    phase: BlueGreenPhase
    source_hosts: Tuple[str, ...] = ()
    target_hosts: Tuple[str, ...] = ()
    source_port: int = -1
    target_port: int = -1


# ---- Routing ABCs + concrete implementations -------------------------


class ConnectRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        ...


class ExecuteRouting(ABC):
    @abstractmethod
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        ...

    @abstractmethod
    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            execute_func: Callable[..., Awaitable[Any]],
            method_name: str) -> Any:
        ...


@dataclass
class _BaseRouting:
    host_matcher: Optional[str] = None
    role_matcher: Optional[BlueGreenRole] = None

    def _matches(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        if self.host_matcher is not None and self.host_matcher != host_info.host:
            return False
        if self.role_matcher is not None and self.role_matcher != role:
            return False
        return True


class PassThroughConnectRouting(_BaseRouting, ConnectRouting):
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        return await connect_func()


class RejectConnectRouting(_BaseRouting, ConnectRouting):
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        raise AwsWrapperError(
            f"Blue-Green deployment currently rejects new connections to "
            f"{host_info.host}.")


@dataclass
class SubstituteConnectRouting(_BaseRouting, ConnectRouting):
    """Redirect the connect to an alternate host.

    Used during PREPARATION / IN_PROGRESS phases when the connect target
    should actually hit the opposite side (source -> target or vice
    versa). Substitute host is looked up in
    :attr:`BlueGreenStatus.corresponding_hosts` keyed by the lower-cased
    original host, matching sync blue_green_plugin.py:355-429.
    """

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        status = plugin._get_status()  # noqa: SLF001 - tight coupling by design
        if status is None:
            return await connect_func()

        pair = status.corresponding_hosts.get(host_info.host.lower())
        if pair is None:
            return await connect_func()
        _, substitute = pair
        if not substitute or substitute == host_info.host:
            return await connect_func()

        # Open a connection to the substitute host through the plugin
        # pipeline, skipping ourselves so auth/secrets plugins re-apply.
        new_host = _clone_host_with(host_info, substitute)
        new_props = Properties(dict(props))
        new_props["host"] = substitute
        try:
            return await plugin._plugin_service.connect(
                new_host, new_props, plugin_to_skip=plugin)
        except Exception:  # noqa: BLE001
            return await connect_func()


@dataclass
class SuspendConnectRouting(_BaseRouting, ConnectRouting):
    """Block connect up to ``BG_CONNECT_TIMEOUT_MS`` while the phase is
    active; then pass through once a non-active phase is reached.

    Matches sync blue_green_plugin.py:430-483. If the phase never
    transitions within the timeout, raise -- we can't silently let the
    connection through in the middle of a cutover.
    """

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        timeout_sec = _get_timeout_sec(
            props, WrapperProperties.BG_CONNECT_TIMEOUT_MS, 30_000)
        deadline = asyncio.get_event_loop().time() + timeout_sec
        while asyncio.get_event_loop().time() < deadline:
            status = plugin._get_status()
            # Release once the cutover window (IN_PROGRESS) is over.
            # Sync blocks only during IN_PROGRESS; POST/COMPLETED are
            # release signals.
            if status is None or status.phase != BlueGreenPhase.IN_PROGRESS:
                return await connect_func()
            await asyncio.sleep(0.1)
        raise AwsWrapperError(
            f"Blue-Green suspend window expired on connect to "
            f"{host_info.host}.")


@dataclass
class SuspendUntilCorrespondingHostFoundConnectRouting(
        _BaseRouting, ConnectRouting):
    """Wait until a corresponding host exists in the opposite BG role.

    Used during switchover when a freshly-requested endpoint must have
    resolved to its target-side counterpart before routing. Matches
    sync blue_green_plugin.py:484-553.
    """

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Optional[Any]:
        timeout_sec = _get_timeout_sec(
            props, WrapperProperties.BG_CONNECT_TIMEOUT_MS, 30_000)
        deadline = asyncio.get_event_loop().time() + timeout_sec
        host_lower = host_info.host.lower()
        while asyncio.get_event_loop().time() < deadline:
            status = plugin._get_status()
            if status is None:
                return await connect_func()
            pair = status.corresponding_hosts.get(host_lower)
            if pair is not None and pair[1]:
                # Corresponding host resolved -- delegate to substitute
                # logic so we pick it up.
                sub = SubstituteConnectRouting(
                    host_matcher=self.host_matcher,
                    role_matcher=self.role_matcher)
                return await sub.apply(
                    plugin, host_info, props, is_initial_connection,
                    connect_func)
            await asyncio.sleep(0.1)
        raise AwsWrapperError(
            f"Corresponding host for {host_info.host} did not appear "
            f"within {timeout_sec}s during Blue-Green switchover.")


class PassThroughExecuteRouting(_BaseRouting, ExecuteRouting):
    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            execute_func: Callable[..., Awaitable[Any]],
            method_name: str) -> Any:
        return await execute_func()


@dataclass
class SuspendExecuteRouting(_BaseRouting, ExecuteRouting):
    """Block execute while the BG phase is active (IN_PROGRESS).

    Polls until either the phase transitions to a non-active state or
    ``BG_SWITCHOVER_TIMEOUT_MS`` elapses. Matches sync
    blue_green_plugin.py:571-629.
    """

    def is_match(self, host_info: HostInfo, role: BlueGreenRole) -> bool:
        return self._matches(host_info, role)

    async def apply(
            self,
            plugin: AsyncBlueGreenPlugin,
            execute_func: Callable[..., Awaitable[Any]],
            method_name: str) -> Any:
        props = plugin._props
        timeout_sec = _get_timeout_sec(
            props, WrapperProperties.BG_SWITCHOVER_TIMEOUT_MS, 180_000)
        deadline = asyncio.get_event_loop().time() + timeout_sec
        while asyncio.get_event_loop().time() < deadline:
            status = plugin._get_status()
            # Release once the cutover window (IN_PROGRESS) is over.
            if status is None or status.phase != BlueGreenPhase.IN_PROGRESS:
                return await execute_func()
            await asyncio.sleep(0.1)
        raise AwsWrapperError(
            f"Blue-Green switchover did not complete within {timeout_sec}s "
            f"for {method_name}.")


# ---- Helpers ---------------------------------------------------------


def _clone_host_with(host_info: HostInfo, new_host: str) -> HostInfo:
    from aws_advanced_python_wrapper.hostinfo import HostInfo as _HostInfoImpl
    return _HostInfoImpl(
        host=new_host,
        port=host_info.port,
        role=host_info.role,
        host_id=host_info.host_id,
    )


def _get_timeout_sec(
        props: Properties,
        prop: Any,
        default_ms: int) -> float:
    try:
        value = prop.get_int(props) if prop is not None else None
    except Exception:  # noqa: BLE001
        value = None
    if not value or value <= 0:
        value = default_ms
    return float(value) / 1000.0


# ---- Status monitor + provider -------------------------------------


class AsyncBlueGreenStatusMonitor:
    """Polls the DB's BG-status table and publishes interim status.

    One monitor per cluster. Uses an asyncio.Task that:
      1. Opens (or reuses) a probe connection via plugin_service.
      2. Runs the :class:`BlueGreenDialect` BG-status query.
      3. Parses rows into :class:`BlueGreenDbStatusInfo`.
      4. Builds a :class:`BlueGreenInterimStatus` and hands it to
         the provided processor callback.
      5. Sleeps ``interval_ms`` and repeats.

    The monitor is intentionally resilient: probe open or query failures
    get logged at debug + retried on the next iteration; they don't
    crash the task.
    """

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            initial_host_info: HostInfo,
            props: Properties,
            bg_id: str,
            interval_ms: int,
            interim_status_processor: Callable[
                [BlueGreenInterimStatus], Awaitable[None]]) -> None:
        self._plugin_service = plugin_service
        self._initial_host_info = initial_host_info
        self._props = Properties(dict(props))
        self._bg_id = bg_id
        self._interval_sec = max(0.05, interval_ms / 1000.0)
        self._processor = interim_status_processor

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._probe_conn: Optional[Any] = None
        self._current_phase = BlueGreenPhase.NOT_CREATED

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        try:
            # Initial-delay loop so fast unit tests can stop before probe.
            await self._sleep_or_stop()
            while not self._stop_event.is_set():
                try:
                    await self._poll_once()
                except Exception:  # noqa: BLE001 - monitor resilience
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

    async def _poll_once(self) -> None:
        if self._probe_conn is None:
            self._probe_conn = await self._open_probe_conn()
            if self._probe_conn is None:
                return

        dialect = self._plugin_service.database_dialect
        query = getattr(dialect, "blue_green_status_query", None)
        if not query:
            # Not a BlueGreenDialect; there's nothing to monitor.
            return

        rows = await self._fetch_rows(query)
        source_hosts: List[str] = []
        target_hosts: List[str] = []
        source_port = -1
        target_port = -1
        latest_phase = BlueGreenPhase.NOT_CREATED

        for row in rows:
            # Columns per sync: version, endpoint, port, role, status.
            if not row or len(row) < 5:
                continue
            try:
                endpoint = str(row[1]).lower()
                port = int(row[2])
                role = BlueGreenRole.parse_role(str(row[3]))
                phase = BlueGreenPhase.parse_phase(str(row[4]))
            except Exception:  # noqa: BLE001
                continue
            if role is None:
                continue
            if role == BlueGreenRole.SOURCE:
                source_hosts.append(endpoint)
                source_port = port
            else:
                target_hosts.append(endpoint)
                target_port = port
            if phase.phase_value > latest_phase.phase_value:
                latest_phase = phase

        if latest_phase != self._current_phase:
            self._current_phase = latest_phase

        interim = BlueGreenInterimStatus(
            bg_id=self._bg_id,
            phase=latest_phase,
            source_hosts=tuple(source_hosts),
            target_hosts=tuple(target_hosts),
            source_port=source_port,
            target_port=target_port,
        )
        await self._processor(interim)

    async def _fetch_rows(self, query: str) -> List[tuple]:
        conn = self._probe_conn
        if conn is None:
            return []
        try:
            cur = conn.cursor()
            async with cur:
                await cur.execute(query)
                return list(await cur.fetchall())
        except Exception:  # noqa: BLE001
            # Drop the probe conn so next iteration reopens a fresh one.
            await self._close_probe_conn()
            return []

    async def _open_probe_conn(self) -> Optional[Any]:
        try:
            return await self._plugin_service.connect(
                self._initial_host_info, self._props)
        except Exception:  # noqa: BLE001
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


class AsyncBlueGreenStatusProvider:
    """Per-cluster singleton that consumes interim statuses, builds the
    routing tables, and publishes :class:`BlueGreenStatus` into the
    plugin service's status store.

    Routing table construction by phase:
      - NOT_CREATED / CREATED: no routings (the plugin passes through).
      - PREPARATION: SubstituteConnectRouting for source -> target.
      - IN_PROGRESS: SuspendConnectRouting + SuspendExecuteRouting.
      - POST: SubstituteConnectRouting (swapped direction).
      - COMPLETED: no routings (clean state).
    """

    _lock: ClassVar[Lock] = Lock()
    _by_cluster: ClassVar[Dict[str, AsyncBlueGreenStatusProvider]] = {}

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            bg_id: str) -> None:
        self._plugin_service = plugin_service
        self._bg_id = bg_id

    async def process_interim(self, interim: BlueGreenInterimStatus) -> None:
        role_by_host: Dict[str, BlueGreenRole] = {}
        corresponding: Dict[str, Tuple[str, Optional[str]]] = {}
        for h in interim.source_hosts:
            role_by_host[h.lower()] = BlueGreenRole.SOURCE
        for h in interim.target_hosts:
            role_by_host[h.lower()] = BlueGreenRole.TARGET

        # Pair up source/target hosts positionally. Sync does more
        # sophisticated matching by endpoint similarity; we fall back
        # to ordering because the status table lists them in the same
        # order per role.
        pairs = list(
            zip(interim.source_hosts, interim.target_hosts))
        for src, tgt in pairs:
            corresponding[src.lower()] = (src, tgt)
            corresponding[tgt.lower()] = (tgt, src)

        connect_routings, execute_routings = _routings_for_phase(
            interim.phase, corresponding)

        status = BlueGreenStatus(
            bg_id=interim.bg_id,
            phase=interim.phase,
            connect_routings=connect_routings,
            execute_routings=execute_routings,
            role_by_host=role_by_host,
            corresponding_hosts=corresponding,
        )
        self._plugin_service.set_status(
            BlueGreenStatus, interim.bg_id, status)

    @classmethod
    def get_or_create(
            cls,
            plugin_service: AsyncPluginService,
            bg_id: str) -> AsyncBlueGreenStatusProvider:
        with cls._lock:
            existing = cls._by_cluster.get(bg_id)
            if existing is not None:
                return existing
            provider = cls(plugin_service, bg_id)
            cls._by_cluster[bg_id] = provider
            return provider

    @classmethod
    def _reset_for_tests(cls) -> None:
        with cls._lock:
            cls._by_cluster.clear()


def _routings_for_phase(
        phase: BlueGreenPhase,
        corresponding: Dict[str, Tuple[str, Optional[str]]],
) -> Tuple[List[ConnectRouting], List[ExecuteRouting]]:
    """Build the routing tables for ``phase``.

    Mirrors the sync provider's phase-table assembly.
    """
    connect: List[ConnectRouting] = []
    execute: List[ExecuteRouting] = []

    if phase == BlueGreenPhase.NOT_CREATED or phase == BlueGreenPhase.CREATED:
        return connect, execute

    if phase == BlueGreenPhase.PREPARATION:
        # Substitute source hosts -> their target counterpart.
        for key, (orig, sub) in corresponding.items():
            if sub and sub != orig:
                connect.append(SubstituteConnectRouting(host_matcher=orig))
        return connect, execute

    if phase == BlueGreenPhase.IN_PROGRESS:
        # Block all new connects + executes during the active cutover.
        connect.append(SuspendConnectRouting())
        execute.append(SuspendExecuteRouting())
        return connect, execute

    if phase == BlueGreenPhase.POST:
        # Switchover window finished; substitute to the new target.
        for key, (orig, sub) in corresponding.items():
            if sub and sub != orig:
                connect.append(SubstituteConnectRouting(host_matcher=orig))
        return connect, execute

    # COMPLETED -- no routings; the plugin becomes a passthrough.
    return connect, execute


class AsyncBlueGreenMonitorService:
    """Lifecycle coordinator for :class:`AsyncBlueGreenStatusMonitor`.

    Mirrors :class:`AsyncLimitlessRouterService`'s pattern: one
    monitor per cluster, lifecycle tracked in a class-level dict,
    teardown registered with the cleanup hook.
    """

    _lock: ClassVar[Lock] = Lock()
    _monitors: ClassVar[Dict[str, AsyncBlueGreenStatusMonitor]] = {}

    @classmethod
    def ensure_monitor(
            cls,
            plugin_service: AsyncPluginService,
            host_info: HostInfo,
            props: Properties,
            bg_id: str,
            interval_ms: int,
            processor: Callable[
                [BlueGreenInterimStatus], Awaitable[None]],
    ) -> AsyncBlueGreenStatusMonitor:
        with cls._lock:
            existing = cls._monitors.get(bg_id)
            if existing is not None and existing.is_running():
                return existing
            monitor = AsyncBlueGreenStatusMonitor(
                plugin_service, host_info, props, bg_id, interval_ms,
                processor)
            cls._monitors[bg_id] = monitor
        monitor.start()
        register_shutdown_hook(monitor.stop)
        return monitor

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


# ---- Plugin --------------------------------------------------------


class AsyncBlueGreenPlugin(AsyncPlugin):
    """Async Blue-Green plugin with monitor + routing dispatch.

    On initial connect, starts a per-cluster
    :class:`AsyncBlueGreenStatusMonitor` that polls the DB for BG
    status and publishes :class:`BlueGreenStatus` via the plugin
    service's status store. Subsequent connect/execute calls consult
    the published status and dispatch through phase-appropriate
    routings.
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}
    _DEFAULT_POLL_INTERVAL_MS = 5_000

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._bg_id = str(
            WrapperProperties.BG_ID.get(props) or "default").strip().lower()
        # Subscribe to network-bound methods so execute routings engage.
        self._subscribed = set(self._SUBSCRIBED)
        self._subscribed.update(self._plugin_service.network_bound_methods)
        self._provider: Optional[AsyncBlueGreenStatusProvider] = None

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._subscribed)

    def _ensure_provider(self) -> AsyncBlueGreenStatusProvider:
        if self._provider is None:
            self._provider = AsyncBlueGreenStatusProvider.get_or_create(
                self._plugin_service, self._bg_id)
        return self._provider

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        if is_initial_connection:
            provider = self._ensure_provider()
            interval_ms = self._poll_interval_ms(props)
            AsyncBlueGreenMonitorService.ensure_monitor(
                self._plugin_service,
                host_info,
                props,
                self._bg_id,
                interval_ms,
                provider.process_interim,
            )

        status = self._get_status()
        if status is None:
            return await connect_func()

        role = status.get_role(host_info)
        if role is None:
            return await connect_func()

        routing = next(
            (r for r in status.connect_routings
             if r.is_match(host_info, role)),
            None,
        )
        if routing is None:
            return await connect_func()
        result = await routing.apply(
            self, host_info, props, is_initial_connection, connect_func)
        if result is not None:
            return result
        return await connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        status = self._get_status()
        if status is None:
            return await execute_func()

        host_info = self._plugin_service.current_host_info
        if host_info is None:
            return await execute_func()

        role = status.get_role(host_info)
        if role is None:
            return await execute_func()

        routing = next(
            (r for r in status.execute_routings
             if r.is_match(host_info, role)),
            None,
        )
        if routing is None:
            return await execute_func()
        return await routing.apply(self, execute_func, method_name)

    def _get_status(self) -> Optional[BlueGreenStatus]:
        return self._plugin_service.get_status(BlueGreenStatus, self._bg_id)

    def _poll_interval_ms(self, props: Properties) -> int:
        try:
            raw = WrapperProperties.BG_INTERVAL_BASELINE_MS.get_int(props)
        except Exception:  # noqa: BLE001
            raw = None
        if not raw or raw <= 0:
            return AsyncBlueGreenPlugin._DEFAULT_POLL_INTERVAL_MS
        return int(raw)


__all__ = [
    "AsyncBlueGreenPlugin",
    "AsyncBlueGreenStatusMonitor",
    "AsyncBlueGreenStatusProvider",
    "AsyncBlueGreenMonitorService",
    "BlueGreenIntervalRate",
    "BlueGreenPhase",
    "BlueGreenRole",
    "BlueGreenStatus",
    "BlueGreenInterimStatus",
    "BlueGreenDbStatusInfo",
    "ConnectRouting",
    "ExecuteRouting",
    "PassThroughConnectRouting",
    "RejectConnectRouting",
    "SubstituteConnectRouting",
    "SuspendConnectRouting",
    "SuspendUntilCorrespondingHostFoundConnectRouting",
    "PassThroughExecuteRouting",
    "SuspendExecuteRouting",
]
