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

"""Async cluster topology monitor.

Background :class:`asyncio.Task` that periodically awakens, calls
:meth:`AsyncAuroraHostListProvider.force_refresh`, and sleeps. Replaces
the sync :class:`ClusterTopologyMonitor`'s thread-based loop.

3.0.0 keeps the monitor minimal: one task per provider instance, fixed
interval, no suggestions feedback loop (sync EFM uses that; async EFM
in SP-5 may add its own). Cancellation is clean -- ``stop()`` cancels
the task and awaits its exit.

Phase G.1 adds a high-frequency refresh window after a writer change
is detected: once a new writer is observed, the monitor temporarily
shortens its tick interval to ``high_refresh_rate_sec`` (default 1s)
for ``HIGH_REFRESH_PERIOD_SEC`` (default 30s) before reverting to the
normal cadence. Mirrors the sync implementation at
``cluster_topology_monitor.py:86, :121, :192-210, :273-282``.

Phase G.4 adds parallel-probe panic mode: when ``connection_getter``
returns ``None`` (no monitoring connection -- e.g., post-failover) and
a ``probe_host`` callable was injected at construction, the monitor
spawns one :class:`asyncio.Task` per host in ``_last_topology``. Each
probe opens a raw connection and classifies its role; the first to
find a writer wins via :class:`asyncio.Event`, and its connection is
stashed as verified-writer state for the caller (failover) to claim.
Mirrors sync ``cluster_topology_monitor.py:230-320``.
"""

from __future__ import annotations

import asyncio
import time
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.host_list_provider import (
        AsyncAuroraHostListProvider, Topology)
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


class AsyncClusterTopologyMonitor:
    """Drive periodic topology refresh against the current connection."""

    HIGH_REFRESH_PERIOD_SEC: float = 30.0
    IGNORE_REQUEST_SEC: float = 10.0

    def __init__(
            self,
            provider: AsyncAuroraHostListProvider,
            connection_getter: Any,
            refresh_interval_sec: float = 30.0,
            high_refresh_rate_sec: float = 1.0,
            probe_host: Optional[
                Callable[[HostInfo], Awaitable[Tuple[Any, HostRole]]]] = None,
            connection_factory: Optional[Callable[[], Awaitable[Any]]] = None,
    ) -> None:
        """
        :param provider: the host list provider whose ``force_refresh`` to
            call each tick.
        :param connection_getter: a zero-arg callable returning the current
            async driver connection (or ``None``). Using a getter lets the
            monitor track connection replacement on failover.
        :param refresh_interval_sec: seconds between refreshes in normal
            (non-panic) mode.
        :param high_refresh_rate_sec: seconds between refreshes while in
            the post-writer-change high-frequency window. Must be small
            enough to catch topology settling quickly (default 1s).
        :param probe_host: optional async callable ``(host_info) ->
            (conn, role)`` used during panic mode. When ``None``, panic
            mode is disabled (backwards compatible). Production wiring
            constructs it from ``AsyncDialectUtils.get_host_role`` plus a
            connection-opener helper.
        :param connection_factory: optional zero-arg async callable that opens
            a FRESH dedicated monitoring connection. When provided, the
            background loop uses its own connection instead of the shared app
            connection (``connection_getter``) -- driver connections such as
            aiomysql cannot service concurrent queries, so refreshing on the
            app's connection corrupts the app's in-flight query. Mirrors sync's
            dedicated-thread monitor, which opens its own connection via
            ``plugin_service.force_connect(initial_host, monitoring_props)``.
            When ``None``, falls back to ``connection_getter`` (test/back-compat).
        """
        self._provider = provider
        self._connection_getter = connection_getter
        self._connection_factory = connection_factory
        self._owned_conn: Optional[Any] = None
        self._interval_sec = max(0.005, float(refresh_interval_sec))
        self._high_refresh_rate_sec = max(0.005, float(high_refresh_rate_sec))
        self._probe_host = probe_host

        self._task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._last_known_writer: Optional[str] = None
        self._high_refresh_until_ns: int = 0
        self._ignore_requests_until_ns: int = 0
        self._last_topology: Topology = ()

        # Panic mode state
        self._is_verified_writer_connection: bool = False
        self._verified_writer_conn: Optional[Any] = None
        self._verified_writer_host_info: Optional[HostInfo] = None
        self._submitted_host_aliases: Set[str] = set()
        self._probe_tasks: Dict[str, asyncio.Task] = {}
        self._writer_found_event: asyncio.Event = asyncio.Event()

    @property
    def high_refresh_rate_sec(self) -> float:
        """Seconds between refreshes while in high-freq mode (read-only)."""
        return self._high_refresh_rate_sec

    @property
    def last_topology(self) -> Topology:
        """Most recently refreshed topology (empty tuple before first tick)."""
        return self._last_topology

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def is_in_panic_mode(self) -> bool:
        """True when panic-mode probe tasks are currently running."""
        return bool(self._probe_tasks)

    def claim_verified_writer(self) -> Optional[Tuple[Any, HostInfo]]:
        """Transfer ownership of the verified writer conn to the caller.

        One-shot: subsequent calls return ``None`` until panic mode finds
        another writer.
        """
        if not self._is_verified_writer_connection:
            return None
        conn = self._verified_writer_conn
        host_info = self._verified_writer_host_info
        self._verified_writer_conn = None
        self._verified_writer_host_info = None
        self._is_verified_writer_connection = False
        self._writer_found_event.clear()
        if conn is None or host_info is None:
            return None
        return (conn, host_info)

    def start(self) -> None:
        """Spawn the background refresh task. No-op if already running."""
        if self.is_running():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run())

    async def _get_monitoring_connection(self) -> Any:
        """Return the connection the background loop should query.

        Prefer a dedicated monitor-owned connection (opened via
        ``connection_factory``) so we never run a topology query on the shared
        app connection concurrently with the app's own query. Reuse it across
        ticks; reopen lazily after a failure drops it. Falls back to the shared
        ``connection_getter`` when no factory was wired.
        """
        if self._connection_factory is None:
            return self._connection_getter()
        if self._owned_conn is None:
            try:
                self._owned_conn = await self._connection_factory()
            except Exception as ex:  # noqa: BLE001 - retry on a later tick
                logger.debug(
                    f"[AsyncClusterTopologyMonitor] failed to open a dedicated "
                    f"monitoring connection; will retry next tick: {ex}")
                self._owned_conn = None
        return self._owned_conn

    async def _drop_owned_connection(self) -> None:
        if self._owned_conn is not None:
            await self._close_best_effort(self._owned_conn)
            self._owned_conn = None

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                conn = await self._get_monitoring_connection()
                if conn is not None:
                    try:
                        # Use the provider's direct-DB path to avoid
                        # recursion; the public force_refresh now
                        # routes through this monitor (N.1b).
                        topology = await self._fetch_via_provider(conn)
                        self._last_topology = topology
                        self._check_for_writer_change(topology)
                    except Exception as ex:
                        # Monitor failures shouldn't crash the task; the cached
                        # topology remains usable. A failure may mean the owned
                        # connection died (e.g. its instance was failed over) --
                        # drop it so the next tick reopens to a live host. Log
                        # so a PERSISTENTLY failing monitor (auth/query mismatch)
                        # isn't invisible while topology silently goes stale.
                        # The (dropped) monitoring connection is reopened lazily
                        # on the next tick.
                        logger.debug(
                            "ClusterTopologyMonitor.ErrorFetchingTopology",
                            self._provider.get_cluster_id(), ex)
                        await self._drop_owned_connection()
                elif self._should_panic():
                    self._spawn_panic_probes()

                # Pick tick interval based on whether we're in high-freq mode.
                interval = self._current_tick_interval()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=interval
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            return
        finally:
            # Cancel any in-flight probes and await their completion so
            # _probe_and_report's finally path runs and closes any opened
            # connections cleanly.
            pending = [t for t in self._probe_tasks.values() if not t.done()]
            for t in pending:
                t.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self._probe_tasks.clear()
            # Close the monitor's dedicated connection (if any) so we don't
            # leak it past task shutdown.
            await self._drop_owned_connection()
            # A panic-mode probe may have stashed a verified-writer connection
            # for the failover caller to claim_verified_writer(). If the monitor
            # is stopped (release_resources_async / shutdown hook) before the
            # caller claims it, that connection would leak -- a winning probe
            # task is already done(), so the cancellation above doesn't touch it.
            # Close it explicitly to honor the no-leak / release_resources_async
            # invariant (for aiomysql it otherwise strands a socket + session).
            if self._verified_writer_conn is not None:
                await self._close_best_effort(self._verified_writer_conn)
                self._verified_writer_conn = None
                self._verified_writer_host_info = None
                self._is_verified_writer_connection = False
                self._writer_found_event.clear()

    def _should_panic(self) -> bool:
        """Enter panic mode iff ``probe_host`` is wired, we have a known
        topology to probe, and we don't already have a verified writer.
        """
        if self._probe_host is None:
            return False
        if self._is_verified_writer_connection:
            return False
        if not self._last_topology:
            return False
        return True

    def _spawn_panic_probes(self) -> None:
        """Spawn probe tasks for each host in ``last_topology`` not already
        submitted. Deduped by ``host_info.as_alias()``.
        """
        # Opportunistic cleanup of finished task refs.
        finished = [k for k, t in self._probe_tasks.items() if t.done()]
        for k in finished:
            self._probe_tasks.pop(k, None)
            # Release dedup slot so a retry can happen on a later tick if
            # the earlier probe failed/returned non-writer.
            self._submitted_host_aliases.discard(k)

        for host_info in self._last_topology:
            alias = host_info.as_alias()
            if alias in self._submitted_host_aliases:
                continue
            self._submitted_host_aliases.add(alias)
            task = asyncio.create_task(self._probe_and_report(host_info))
            self._probe_tasks[alias] = task

    # Aurora PG briefly rejects IAM/PAM auth on instances mid-promotion
    # (the PAM service restarts during the writer-role swap). Mirror the
    # sync ``HostMonitor`` bounded-retry from commit ``724de17`` so a
    # single PAM-transient blip doesn't waste this probe slot and force
    # the outer tick to fully respawn probes. Cancellation
    # (``CancelledError``) propagates through ``await`` and exits the
    # retry loop deterministically.
    _MAX_TRANSIENT_PROBE_ATTEMPTS: int = 10
    _PROBE_RETRY_BACKOFF_SEC: float = 0.5

    async def _probe_and_report(self, host_info: HostInfo) -> None:
        """Run one probe; stash the conn + host on winner, close on loser."""
        assert self._probe_host is not None  # _should_panic gates this
        conn: Optional[Any] = None
        role: Optional[HostRole] = None
        for attempt in range(self._MAX_TRANSIENT_PROBE_ATTEMPTS + 1):
            try:
                conn, role = await self._probe_host(host_info)
                break  # success
            except asyncio.CancelledError:
                # Propagate so ``stop()`` can cleanly cancel this task.
                raise
            except Exception:
                if attempt < self._MAX_TRANSIENT_PROBE_ATTEMPTS:
                    # Mirror sync HostMonitor: short backoff, then retry.
                    # Aurora PAM recovery empirically <5s, which matches
                    # 10 * 0.5s = 5s total budget here.
                    await asyncio.sleep(self._PROBE_RETRY_BACKOFF_SEC)
                    continue
                # Budget exhausted -- swallow per the existing probe
                # contract ("Probe failures are expected -- don't crash").
                return
        if role is None:
            # Defensive: loop exits via break-on-success or return above.
            return

        # Winner gate: the first probe to win the race claims the writer.
        # Check both the event AND the verified-writer flag -- two probes
        # can both pass the event check if they arrive before set() fires.
        # The flag is the atomic consistency anchor.
        if (role == HostRole.WRITER
                and not self._writer_found_event.is_set()
                and not self._is_verified_writer_connection):
            self._verified_writer_conn = conn
            self._verified_writer_host_info = host_info
            self._is_verified_writer_connection = True
            self._writer_found_event.set()
            return

        # Lost the race OR role is reader: close the conn.
        if conn is not None:
            await self._close_best_effort(conn)

    @staticmethod
    async def _close_best_effort(conn: Any) -> None:
        try:
            close = getattr(conn, "close", None)
            if close is None:
                return
            result = close()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            # Best-effort: swallow close errors.
            pass

    def _current_tick_interval(self) -> float:
        """High-freq window active -> short interval; else normal interval."""
        if time.time_ns() < self._high_refresh_until_ns:
            return self._high_refresh_rate_sec
        return self._interval_sec

    def _check_for_writer_change(self, topology: Any) -> None:
        """Detect writer change and enter high-freq mode if so.

        Compares the writer in ``topology`` (a sequence of ``HostInfo``)
        against :attr:`_last_known_writer`. The first-ever writer seen
        does *not* trigger high-freq mode -- only a subsequent *change*
        does. Empty topology or no writer is a no-op.
        """
        if topology is None:
            return
        new_writer: Optional[str] = None
        for h in topology:
            if h.role == HostRole.WRITER:
                new_writer = f"{h.host}:{h.port}"
                break
        if new_writer is None:
            return
        writer_changed = (self._last_known_writer is not None
                          and new_writer != self._last_known_writer)
        is_new_writer = self._last_known_writer is None
        if writer_changed:
            # Writer changed -- enter high-freq mode.
            self._high_refresh_until_ns = (
                time.time_ns()
                + int(self.HIGH_REFRESH_PERIOD_SEC * 1_000_000_000))
        self._last_known_writer = new_writer
        # Writer is confirmed (first-seen or changed) -- start the
        # ignore-request window. Subsequent ticks that re-observe the
        # same writer do NOT re-extend the window, so it naturally
        # expires IGNORE_REQUEST_SEC after the last writer transition.
        if is_new_writer or writer_changed:
            self._ignore_requests_until_ns = (
                time.time_ns()
                + int(self.IGNORE_REQUEST_SEC * 1_000_000_000))

    def should_ignore_refresh_request(self) -> bool:
        """Return True if the monitor recently confirmed the writer and
        external refresh requests should be deferred.

        Mirrors sync cluster_topology_monitor.py:136-141.
        """
        return time.time_ns() < self._ignore_requests_until_ns

    async def force_refresh_with_connection(
            self,
            conn: Any,
            timeout_sec: float = 5.0,
            bypass_ignore_window: bool = False) -> Topology:
        """Probe the topology provider with the caller's ``conn``.

        Short-circuits to the cached ``last_topology`` when the ignore-
        request window is active UNLESS ``bypass_ignore_window`` is True
        (failover recovery wants a fresh probe regardless). Otherwise
        delegates to ``provider.force_refresh(conn)`` under an
        ``asyncio.wait_for(timeout=timeout_sec)`` gate.

        Raises ``TimeoutError`` when the provider doesn't respond within
        ``timeout_sec``.
        """
        if not bypass_ignore_window and self.should_ignore_refresh_request():
            return self._last_topology
        try:
            topology = await asyncio.wait_for(
                self._fetch_via_provider(conn),
                timeout=timeout_sec,
            )
        except asyncio.TimeoutError as e:
            raise TimeoutError(Messages.get_formatted(
                "ClusterTopologyMonitor.TopologyNotUpdated",
                self._provider.get_cluster_id(), timeout_sec * 1000)) from e
        self._last_topology = topology
        self._check_for_writer_change(topology)
        return topology

    async def _fetch_via_provider(self, conn: Any) -> Topology:
        """Call the provider's direct-DB query path, preferring
        :meth:`_fetch_from_db` when available. Falls back to
        ``force_refresh`` for providers (or bare mocks) that don't have
        the split -- preserves pre-N.1b behavior.
        """
        import inspect

        # Check the class first (normal case), then the instance (tests
        # may monkey-patch an async override). Must be a coroutine
        # function so bare MagicMock attrs don't trigger the path.
        cls_direct = getattr(type(self._provider), "_fetch_from_db", None)
        if cls_direct is not None and inspect.iscoroutinefunction(cls_direct):
            inst_direct = getattr(self._provider, "_fetch_from_db", None)
            if inspect.iscoroutinefunction(inst_direct):
                return await inst_direct(conn)
            return await cls_direct(self._provider, conn)
        return await self._provider.force_refresh(conn)

    async def stop(self) -> None:
        """Signal the task to exit and await its termination."""
        self._stop_event.set()
        if self._task is None:
            return
        if not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None


def build_probe_host(
        plugin_service: AsyncPluginService,
        props: Properties) -> Callable[[HostInfo], Awaitable[Tuple[Any, HostRole]]]:
    """Build a probe callable that opens a conn through the plugin pipeline
    and classifies its role via DialectUtils.

    Used by AsyncClusterTopologyMonitor's panic mode to search for a new
    writer when the primary monitoring connection dies. The returned
    coroutine function: (host_info) -> (conn, role). Raises on failure.
    """
    # Import here (runtime) -- at module-top these are TYPE_CHECKING-only.
    from aws_advanced_python_wrapper.utils.properties import \
        Properties as PropertiesRuntime

    async def _probe(host_info: HostInfo) -> Tuple[Any, HostRole]:
        # Open through the plugin pipeline so auth plugins re-apply.
        probe_props = PropertiesRuntime(dict(props))
        probe_props["host"] = host_info.host
        if host_info.is_port_specified():
            probe_props["port"] = str(host_info.port)
        conn = await plugin_service.connect(host_info, probe_props)
        role = await plugin_service.get_host_role(conn)
        return conn, role

    return _probe
