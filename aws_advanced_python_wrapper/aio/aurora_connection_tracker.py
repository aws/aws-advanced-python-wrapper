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

"""Async Aurora connection tracker + invalidate-on-failover plugin.

Port of sync ``aurora_connection_tracker_plugin.py``. Tracks every
connection opened through the plugin pipeline in a class-level
:class:`AsyncOpenedConnectionTracker` registry keyed by the RDS
instance-endpoint alias (mirroring sync normalization). On writer
change (detected via ``AsyncPluginService.all_hosts`` or a raised
:class:`FailoverError`), closes every tracked connection to the old
writer so pooled consumers don't reuse a stale demoted-writer socket.

Simpler than the sync implementation:

* ``WeakSet`` auto-GC -- no prune daemon thread.
* :func:`asyncio.create_task` for fire-and-forget invalidation -- no
  background ``Thread``.

Unlike earlier iterations of this module, the tracked-connections
dict lives on the class (mirroring sync's ``ClassVar[Dict[str,
WeakSet]]`` at ``aurora_connection_tracker_plugin.py:47``) so that
multiple pooled plugin instances share one registry and a single
``invalidate_all`` call closes every peer connection to the same
host.
"""

from __future__ import annotations

import asyncio
import threading
from time import perf_counter_ns
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    Optional, Set)
from weakref import WeakSet

from aws_advanced_python_wrapper.aio.cleanup import register_shutdown_hook
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import FailoverError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncOpenedConnectionTracker:
    """Alias-keyed registry of open connections, shared across plugin instances.

    Keys by the RDS **instance** endpoint alias when one is present in
    ``host_info.as_aliases()`` -- matching sync's
    :class:`OpenedConnectionTracker` at
    ``aurora_connection_tracker_plugin.py:47``. Falls back to
    ``host_info.as_alias()`` when no RDS instance alias is
    discoverable (non-Aurora or custom endpoint connections).

    This lets a cluster-endpoint-connected session still be
    invalidated when a later ``CONVERTED_TO_READER`` notification
    carries the instance alias.

    Class-level ``_tracked`` mirrors sync ``OpenedConnectionTracker`` at
    ``aurora_connection_tracker_plugin.py:47`` so multiple pooled
    :class:`AsyncAuroraConnectionTrackerPlugin` instances can coordinate:
    one plugin's :meth:`invalidate_all` closes every peer connection to
    the same host.

    Still simpler than sync:

    * :class:`WeakSet` for auto-GC of dead references (no prune daemon),
    * :meth:`invalidate_all` awaits ``conn.close()`` (async or sync) directly.
    """

    _tracked: ClassVar[Dict[str, WeakSet[Any]]] = {}

    def __init__(self) -> None:
        # Instance-level nothing -- all state lives on the class dict.
        pass

    @staticmethod
    def _canonical_key(host_info: HostInfo) -> str:
        """Return the instance-endpoint alias if available, else host:port.

        Mirrors sync's multi-alias normalization in
        ``populate_opened_connection_set``/``invalidate_all_connections``/
        ``remove_connection_tracking`` at
        ``aurora_connection_tracker_plugin.py:116-137``.
        """
        rds_utils = RdsUtils()
        if rds_utils.is_rds_instance(host_info.host):
            return host_info.as_alias()
        for alias in host_info.as_aliases():
            if rds_utils.is_rds_instance(rds_utils.remove_port(alias)):
                return alias
        return host_info.as_alias()

    @staticmethod
    def _all_keys(host_info: HostInfo) -> Set[str]:
        """Return every alias worth tracking ``host_info`` under.

        Sync's ``OpenedConnectionTracker`` tracks connections keyed on
        every alias in ``host_info.as_aliases()`` so a later invalidation
        via any alias closes the same underlying connection set. We
        mirror that here: track under each alias plus the canonical
        RDS-instance endpoint.
        """
        keys: Set[str] = set()
        canonical = AsyncOpenedConnectionTracker._canonical_key(host_info)
        if canonical:
            keys.add(canonical)
        try:
            for alias in host_info.as_aliases():
                if alias:
                    keys.add(alias)
        except Exception:  # noqa: BLE001 - as_aliases() is best-effort
            pass
        return keys

    def track(self, host_info: HostInfo, conn: Any) -> None:
        for key in self._all_keys(host_info):
            conn_set = AsyncOpenedConnectionTracker._tracked.setdefault(
                key, WeakSet())
            conn_set.add(conn)

    def remove(self, host_info: HostInfo, conn: Any) -> None:
        for key in self._all_keys(host_info):
            conn_set = AsyncOpenedConnectionTracker._tracked.get(key)
            if conn_set is not None:
                conn_set.discard(conn)

    async def invalidate_all(self, host_info: HostInfo) -> None:
        """Close every tracked connection reachable via any alias of ``host_info``.

        Best-effort: individual close failures are swallowed so one
        broken connection doesn't block closing the rest. Fixes the
        prior canonical-key-only semantics that could miss invalidations
        when the caller held a non-canonical alias.
        """
        # Gather the full connection set from every alias; close each
        # connection exactly once, then purge the alias entries.
        seen_conns: Set[int] = set()
        to_close: list = []
        keys = self._all_keys(host_info)
        for key in keys:
            conn_set = AsyncOpenedConnectionTracker._tracked.get(key)
            if conn_set is None:
                continue
            for c in list(conn_set):
                cid = id(c)
                if cid in seen_conns:
                    continue
                seen_conns.add(cid)
                to_close.append(c)

        for conn in to_close:
            try:
                # Prefer .invalidate() for pool-proxied connections (e.g.
                # SQLAlchemy's ``_AsyncConnectionFairy``) so the underlying
                # driver conn is closed AND removed from the pool's tracking.
                # Plain close() on a pool fairy returns it to the pool, which
                # would later hand a broken connection back to the next caller
                # after writer failover.
                inv = getattr(conn, "invalidate", None)
                if callable(inv):
                    result = inv()
                    if asyncio.iscoroutine(result):
                        await result
                    continue
                close = getattr(conn, "close", None)
                if close is None:
                    continue
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:  # noqa: BLE001 - close is best-effort
                pass

        for key in keys:
            try:
                del AsyncOpenedConnectionTracker._tracked[key]
            except KeyError:
                pass

    def _tracked_for(self, host_info: HostInfo) -> WeakSet[Any]:
        """Test-only accessor using the canonical key derivation."""
        key = self._canonical_key(host_info)
        return AsyncOpenedConnectionTracker._tracked.get(key, WeakSet())


class AsyncAuroraConnectionTrackerPlugin(AsyncPlugin):
    """Async counterpart of :class:`AuroraConnectionTrackerPlugin`.

    Subscribes to CONNECT, CONNECTION_CLOSE, NOTIFY_HOST_LIST_CHANGED, and
    the pipeline's network-bound cursor/connection methods so it can detect
    a writer change mid-session and invalidate pooled connections to the
    demoted writer.
    """

    _CORE_SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.CONNECTION_CLOSE.method_name,
        DbApiMethod.NOTIFY_HOST_LIST_CHANGED.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
    }

    # Post-failover settling window -- port of sync
    # ``aurora_connection_tracker_plugin.py:259-261``. After a FailoverError,
    # topology changes (e.g. a lagging DNS/metadata flip of the demoted
    # writer) are expected for up to 3 minutes, so every subsequent execute
    # keeps refreshing the host list until the window elapses -- not just the
    # single refresh performed inside the FailoverError handler. Class-level
    # so all plugin instances (one per pooled connection) share the window,
    # exactly like sync.
    _host_list_refresh_end_time_ns: ClassVar[int] = 0
    _refresh_lock: ClassVar[threading.Lock] = threading.Lock()
    _TOPOLOGY_CHANGES_EXPECTED_TIME_NS: ClassVar[int] = 3 * 60 * 1_000_000_000  # 3 minutes

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            tracker: Optional[AsyncOpenedConnectionTracker] = None) -> None:
        self._plugin_service = plugin_service
        self._tracker = tracker or AsyncOpenedConnectionTracker()
        self._current_writer: Optional[HostInfo] = None
        self._pending_invalidations: Set[asyncio.Task] = set()

        # Telemetry counter -- the sync tracker plugin doesn't emit this,
        # but writer-change detection is a hot path in pooled async apps
        # so we track it here for parity with the broader counter story.
        tf = self._plugin_service.get_telemetry_factory()
        self._writer_changes_counter = tf.create_counter(
            "aurora_connection_tracker.writer_changes.count")

        register_shutdown_hook(self._shutdown)

    @property
    def last_known_writer_host(self) -> Optional[str]:
        """Backwards-compat accessor (was present on the SP-8 stub)."""
        return self._current_writer.host if self._current_writer else None

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._CORE_SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        conn = await connect_func()
        if conn is not None:
            await self._fill_instance_alias(host_info, conn)
            self._tracker.track(host_info, conn)
            await self._pin_current_writer(host_info, conn)
        return conn

    async def _pin_current_writer(self, host_info: HostInfo, conn: Any) -> None:
        """Pin the current writer at connect time so a later failover is
        detected as a *transition*.

        Unlike sync, the async connect flow never populates
        ``plugin_service.all_hosts`` -- it stays empty until something calls
        ``refresh_host_list`` explicitly, which (for a failover-bound
        connection) first happens only *inside* the failover handler. By then
        ``_current_writer`` is still ``None``, so the post-failover writer
        reads as a first observation in :meth:`_update_writer_from_topology`
        and the demoted writer's tracked (idle) connections are never
        invalidated (``test_writer_failover_in_idle_connections``).

        Refreshing once here pins ``_current_writer`` to the instance the
        connection landed on, so the subsequent failover registers as
        old_writer -> new_writer and triggers ``invalidate_all``. Gated to
        RDS hosts so non-Aurora connections pay no topology round-trip, and
        skipped once a writer is already pinned. Best-effort: a refresh
        failure just leaves the prior (no-pin) behavior in place.
        """
        try:
            if self._current_writer is not None:
                return
            rds = RdsUtils()
            if not (rds.is_rds_instance(host_info.host)
                    or rds.identify_rds_type(host_info.host).is_rds_cluster):
                return
            await self._plugin_service.refresh_host_list(conn)
            self._update_writer_from_topology()
        except Exception:  # noqa: BLE001 - writer pinning is best-effort
            pass

    async def _fill_instance_alias(
            self, host_info: HostInfo, conn: Any) -> None:
        """Add the connection's resolved instance endpoint as an alias.

        A cluster-endpoint connection lands on whichever instance the
        cluster DNS currently points at, but ``host_info.host`` stays the
        cluster endpoint -- so the tracker would key it only under the
        cluster alias. On writer failover the change is detected against
        the *instance* host (``_current_writer_from_hosts`` returns a
        topology row), so ``invalidate_all(writer_instance)`` would never
        match the cluster-keyed entry and the stale idle connection would
        survive the demotion (``test_writer_failover_in_idle_connections``).

        Mirrors sync ``AuroraConnectionTrackerPlugin.connect`` which calls
        ``plugin_service.fill_aliases`` for ``is_rds_cluster`` hosts. Only
        cluster-endpoint connections pay the extra identify round-trip;
        instance-endpoint connections already key correctly. Best-effort:
        on any failure we fall back to cluster-key-only tracking (the
        prior behavior), so a transient identify failure never blocks the
        connect.
        """
        try:
            if not RdsUtils().identify_rds_type(host_info.host).is_rds_cluster:
                return
            identified = await self._plugin_service.identify_connection(conn)
            if identified is None:
                return
            host_info.reset_aliases()
            host_info.add_alias(host_info.as_alias())
            host_info.add_alias(*identified.as_aliases())
        except Exception:  # noqa: BLE001 - alias enrichment is best-effort
            pass

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if method_name != DbApiMethod.CONNECTION_CLOSE.method_name:
            # Sync parity: aurora_connection_tracker_plugin.py:313-328 --
            # while the post-failover settling window is open, keep
            # refreshing topology on every execute so late writer flips are
            # still detected; once it elapses, stop refreshing.
            need_refresh_host_lists = False
            cls = AsyncAuroraConnectionTrackerPlugin
            with cls._refresh_lock:
                end_time_ns = cls._host_list_refresh_end_time_ns
                if end_time_ns > 0:
                    if end_time_ns > perf_counter_ns():
                        need_refresh_host_lists = True
                    else:
                        cls._host_list_refresh_end_time_ns = 0
            if need_refresh_host_lists:
                try:
                    await self._plugin_service.refresh_host_list()
                except Exception:  # noqa: BLE001 - refresh best-effort
                    pass
            self._update_writer_from_topology()
        try:
            return await execute_func()
        except Exception as exc:
            if isinstance(exc, FailoverError):
                # Sync parity: aurora_connection_tracker_plugin.py:335-342 --
                # open the 3-minute settling window, then refresh + recheck
                # the writer immediately.
                cls = AsyncAuroraConnectionTrackerPlugin
                with cls._refresh_lock:
                    cls._host_list_refresh_end_time_ns = (
                        perf_counter_ns()
                        + cls._TOPOLOGY_CHANGES_EXPECTED_TIME_NS)
                try:
                    await self._plugin_service.refresh_host_list()
                except Exception:  # noqa: BLE001 - refresh best-effort
                    pass
                self._update_writer_from_topology()
            raise

    def _update_writer_from_topology(self) -> None:
        writer = self._current_writer_from_hosts()
        if writer is None:
            return
        if self._current_writer is None:
            self._current_writer = writer
            return
        if self._same_host(self._current_writer, writer):
            return
        old_writer = self._current_writer
        self._current_writer = writer
        if self._writer_changes_counter is not None:
            self._writer_changes_counter.inc()
        self._spawn_invalidation(old_writer)

    def _current_writer_from_hosts(self) -> Optional[HostInfo]:
        for h in self._plugin_service.all_hosts:
            if h.role == HostRole.WRITER:
                return h
        return None

    @staticmethod
    def _same_host(a: HostInfo, b: HostInfo) -> bool:
        return a.host == b.host and a.port == b.port

    def _spawn_invalidation(self, old_writer: HostInfo) -> None:
        task = asyncio.create_task(self._tracker.invalidate_all(old_writer))
        self._pending_invalidations.add(task)
        task.add_done_callback(self._pending_invalidations.discard)

    async def _shutdown(self) -> None:
        """Drain pending invalidation tasks on release_resources_async.

        Async apps calling release_resources_async() on exit will see
        every tracked connection's close() attempt complete rather than
        get cut off when the loop tears down.
        """
        if not self._pending_invalidations:
            return
        pending = list(self._pending_invalidations)
        await asyncio.gather(*pending, return_exceptions=True)

    def notify_host_list_changed(
            self,
            changes: Dict[str, Set[HostEvent]]) -> None:
        """React to topology changes.

        Mirrors sync ``aurora_connection_tracker_plugin.py:344-349``:

        * ``CONVERTED_TO_READER`` for a tracked host -> invalidate that
          host's connections.
        * ``CONVERTED_TO_WRITER`` for any host -> clear
          :attr:`_current_writer` so the next ``execute`` re-detects the
          new writer via :meth:`_update_writer_from_topology`.

        With the tracker's canonical-key normalization (instance alias
        over cluster alias), a notify carrying the instance alias for a
        cluster-connected session now correctly finds and invalidates
        the tracked entries.
        """
        for alias, events in changes.items():
            if HostEvent.CONVERTED_TO_READER in events:
                host_part, port_part = self._parse_alias(alias)
                h = HostInfo(host=host_part, port=port_part)
                self._spawn_invalidation(h)
            if HostEvent.CONVERTED_TO_WRITER in events:
                # Force writer recheck on next execute via
                # _update_writer_from_topology.
                self._current_writer = None

    @staticmethod
    def _parse_alias(alias: str) -> tuple:
        """Split a ``host:port`` alias into ``(host, port)``.

        Uses ``rpartition(':')`` so IPv6 aliases of the form
        ``[::1]:5432`` split correctly on the LAST colon. Falls back to
        port 5432 when the alias has no colon (or the port part isn't
        numeric).
        """
        host_part, sep, port_part = alias.rpartition(":")
        if not sep:
            # No colon -> treat entire alias as host, port defaults.
            return alias, 5432
        try:
            return host_part, int(port_part)
        except ValueError:
            return host_part, 5432


__all__ = ["AsyncAuroraConnectionTrackerPlugin", "AsyncOpenedConnectionTracker"]
