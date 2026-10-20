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
:class:`AsyncOpenedConnectionTracker` registry keyed by host id and by
host and port. On writer change (detected via
``AsyncPluginService.all_hosts`` or a raised :class:`FailoverError`),
closes every tracked connection to the old writer so pooled consumers
don't reuse a stale demoted-writer socket.

Simpler than the sync implementation:

* ``WeakSet`` auto-GC -- no prune daemon thread.
* Writer-change invalidation is awaited INLINE in the execute/failover
  path (sync offloads to a daemon ``Thread``, which outlives the failover
  call; an asyncio task dies un-awaited when the loop closes, so inline
  await is the async equivalent of sync's completion guarantee).
  :func:`asyncio.create_task` remains only for the sync-signature
  ``notify_host_list_changed`` path, drained on
  ``release_resources_async``.

The tracked-connections
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
                    Optional, Set, Tuple)
from weakref import WeakSet

from aws_advanced_python_wrapper.aio.cleanup import register_shutdown_hook
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import FailoverError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.utils import Utils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


class AsyncOpenedConnectionTracker:
    """Registry of open connections, shared across plugin instances.

    An instance endpoint names the instance already, so its connections are
    tracked under one key. Any other endpoint may be a custom domain name,
    which is tracked both under the host id and under the endpoint the caller
    used, so a later notification naming either one finds the connections.

    ``_tracked`` is class-level, as sync's registry is, so multiple pooled
    :class:`AsyncAuroraConnectionTrackerPlugin` instances coordinate: one
    plugin's :meth:`invalidate_all` closes every peer connection to the same
    host.

    Still simpler than sync:

    * :class:`WeakSet` for auto-GC of dead references (no prune daemon),
    * :meth:`invalidate_all` awaits ``conn.close()`` (async or sync) directly.
    """

    _tracked: ClassVar[Dict[str, WeakSet[Any]]] = {}

    def __init__(self) -> None:
        # Instance-level nothing -- all state lives on the class dict.
        pass

    _rds_utils: ClassVar[RdsUtils] = RdsUtils()

    @classmethod
    def _tracking_keys(cls, host_info: HostInfo) -> Tuple[str, ...]:
        """The keys ``host_info``'s connections are tracked under."""
        if cls._rds_utils.is_rds_instance(host_info.host):
            return (host_info.get_host_and_port(),)
        return tuple(
            key for key in (host_info.host_id, host_info.get_host_and_port()) if key)

    def track(self, host_info: HostInfo, conn: Any) -> None:
        for key in self._tracking_keys(host_info):
            conn_set = AsyncOpenedConnectionTracker._tracked.setdefault(
                key, WeakSet())
            conn_set.add(conn)

    def remove(self, host_info: HostInfo, conn: Any) -> None:
        for key in self._tracking_keys(host_info):
            conn_set = AsyncOpenedConnectionTracker._tracked.get(key)
            if conn_set is not None:
                conn_set.discard(conn)

    async def invalidate_all(self, host_info: HostInfo) -> None:
        """Close every connection tracked for ``host_info``."""
        logger.debug(
            "OpenedConnectionTracker.InvalidatingConnections", host_info.url)
        await self.invalidate_all_for_keys(
            host_info.get_host_and_port(), host_info.host, host_info.host_id)

    async def invalidate_all_for_keys(self, *keys: Optional[str]) -> None:
        """Close every connection tracked under any of ``keys``.

        Best-effort: individual close failures are swallowed so one broken
        connection does not block closing the rest.
        """
        seen_conns: Set[int] = set()
        to_close: list = []
        present = tuple(key for key in keys if key)
        # Drop each key before the first await. Collecting everything first
        # and popping afterwards would discard a connection a concurrent
        # track() added in between, leaving it open and untracked.
        for key in present:
            conn_set = AsyncOpenedConnectionTracker._tracked.pop(key, None)
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
            except Exception as e:  # noqa: BLE001 - close is best-effort
                # The connection is expected to be broken (its host was just
                # demoted/rebooted), so a failed close is unsurprising -- but
                # log it: a conn whose close failed may still report
                # is_closed=False to its owner.
                logger.debug(
                    f"[AsyncOpenedConnectionTracker] best-effort close of a "
                    f"tracked connection failed: {e}")

    def _tracked_for(self, host_info: HostInfo) -> WeakSet[Any]:
        """Test-only accessor using the first tracking key."""
        keys = self._tracking_keys(host_info)
        if not keys:
            return WeakSet()
        return AsyncOpenedConnectionTracker._tracked.get(keys[0], WeakSet())

    def log_opened_connections(self) -> None:
        """Debug-log the registry contents (sync parity:
        ``OpenedConnectionTracker.log_opened_connections``)."""
        msg_parts = []
        for key, conn_set in AsyncOpenedConnectionTracker._tracked.items():
            conn = "".join(f"\n\t\t{item}" for item in list(conn_set))
            msg_parts.append(f"\t[{key} : {conn}]")
        logger.debug(
            "OpenedConnectionTracker.OpenedConnectionsTracked", "".join(msg_parts))


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
            connection_host_info = await self._identify_connection_host(host_info, conn)
            self._tracker.track(connection_host_info, conn)
            await self._pin_current_writer(connection_host_info, conn)
        return conn

    async def _identify_connection_host(
            self, host_info: HostInfo, conn: Any) -> HostInfo:
        """The host this connection actually landed on.

        A cluster, custom-domain or IP endpoint does not name the instance the
        connection reached, so ask the database and record the answer as the
        routed host. On failure the endpoint the caller used is kept.
        """
        routed = self._plugin_service.routed_host_info
        connection_host_info = routed if routed is not None else host_info
        url_type = AsyncOpenedConnectionTracker._rds_utils.identify_rds_type(
            connection_host_info.host)
        if not Utils.requires_host_identification(url_type):
            return connection_host_info
        try:
            identified = await self._plugin_service.identify_connection(conn)
        except Exception:  # noqa: BLE001 - identification is best-effort
            return connection_host_info
        if identified is None:
            return connection_host_info
        self._plugin_service.routed_host_info = identified
        return identified

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
            await self._invalidate_writer_change()
            # Topology can LAG right after a failover (aurora_replica_status
            # still names the demoted writer for seconds), so the pin above
            # may be STALE -- and a wrong pin defeats BOTH failover-time
            # detection paths: the topology comparison sees "no change" and
            # the departed-host guard refuses because the pinned writer
            # disagrees with the host the connection departed (observed:
            # idle-connection params starting <30s after a prior failover
            # missed invalidation entirely). Ground truth is a live is_reader
            # probe of THIS connection: if it says WRITER, pin the
            # connection's own host over whatever topology claimed.
            role = await self._plugin_service.get_host_role(conn)
            if role == HostRole.WRITER:
                host = self._plugin_service.current_host_info
                if host is not None and (
                        self._current_writer is None
                        or not self._same_host(host, self._current_writer)):
                    self._current_writer = host
        except Exception:  # noqa: BLE001 - writer pinning is best-effort
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
            await self._invalidate_writer_change()
        pre_failover_host = self._plugin_service.current_host_info
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
                    # FORCE refresh, through the post-failover current
                    # connection (the just-verified new writer): a plain
                    # refresh() can serve the monitor's cache from before the
                    # monitor observed the failover, so the writer change goes
                    # undetected. Sync tolerates that staleness because its
                    # daemon-thread/notify paths retry detection after the
                    # failover call returns; in asyncio everything dies with
                    # the loop, so this handler is the ONE deterministic shot
                    # at detecting the change (observed: 1/10 idle-connection
                    # params missed invalidation entirely on a stale refresh).
                    await self._plugin_service.force_refresh_host_list()
                except Exception as e:  # noqa: BLE001 - refresh best-effort
                    logger.debug(
                        f"[AsyncAuroraConnectionTrackerPlugin] post-failover "
                        f"topology refresh failed (best-effort): {e}")
                await self._invalidate_writer_change()
                # Topology-independent detection: right after promotion even a
                # direct topology query can still report the OLD writer
                # (aurora_replica_status lags the promotion by seconds), so the
                # topology comparison above can miss the change -- observed on
                # Aurora as idle connections surviving failover. But the
                # failover plugin itself just MOVED this connection: comparing
                # the connection's host before the failing execute vs after
                # the failover is deterministic. Invalidate the departed host
                # when the connection provably moved off it. Sync doesn't need
                # this: its daemon-thread/notify paths re-detect the change
                # after replica_status catches up; async's last chance is now.
                # Diagnostic (kept at debug): the idle-connection flake showed
                # runs where NO invalidation fired via ANY path; this line
                # makes the handler's decision inputs visible in test logs.
                post_host = self._plugin_service.current_host_info
                logger.debug(
                    f"[AsyncAuroraConnectionTrackerPlugin] failover handler: "
                    f"pre={pre_failover_host.url if pre_failover_host else None} "
                    f"post={post_host.url if post_host else None} "
                    f"pinned={self._current_writer.url if self._current_writer else None}")
                await self._invalidate_departed_host(pre_failover_host)
            raise

    def _update_writer_from_topology(self) -> Optional[HostInfo]:
        """Re-derive the current writer from topology. Returns the OLD writer
        when a writer *change* was detected (caller must invalidate its
        tracked connections), else ``None``."""
        writer = self._current_writer_from_hosts()
        if writer is None:
            return None
        if self._current_writer is None:
            self._current_writer = writer
            return None
        if self._same_host(self._current_writer, writer):
            return None
        old_writer = self._current_writer
        self._current_writer = writer
        if self._writer_changes_counter is not None:
            self._writer_changes_counter.inc()
        return old_writer

    async def _invalidate_departed_host(
            self, pre_failover_host: Optional[HostInfo]) -> None:
        """Invalidate connections to the host this connection provably moved
        OFF during failover (topology-independent; see the failover handler).

        Guarded: only fires when the connection actually changed hosts, and
        only when the pin does NOT name a third host distinct from both ends
        of the move. Concretely, invalidate when the pinned writer is the
        departed host, is None (a failed pin must not disable invalidation),
        or equals POST: if the connect-time pin ever fails silently (observed
        on MySQL before the dialect upgrade moved into the terminal connect
        hook -- update_database_dialect, sync default_plugin.py:82 parity --
        and still possible on any transient connect-time probe error), the
        handler's own force-refresh registers the NEW writer as a
        first-observation pin -- the pin then equals post while pre, the host
        the connection provably departed, still holds the idle connections
        (observed: MySQL idle-connection params failed 7/7 with pre=old,
        post=new, pinned=new). Only a pin naming a third host (e.g.
        reader-mode failover moving off a reader while the real writer is
        pinned elsewhere) vetoes. When the topology comparison already
        invalidated the same host, the registry entries are gone and this is
        a no-op.
        """
        if pre_failover_host is None:
            return
        post = self._plugin_service.current_host_info
        if post is None or self._same_host(pre_failover_host, post):
            return
        if (self._current_writer is not None
                and not self._same_host(pre_failover_host, self._current_writer)
                and not self._same_host(post, self._current_writer)):
            return
        # The connection moved off pre_failover_host via failover: that host
        # was demoted or died. Re-pin to where we are now so a later flip
        # back is detected as a transition.
        self._current_writer = post
        await self._tracker.invalidate_all(pre_failover_host)
        self._tracker.log_opened_connections()

    async def _invalidate_writer_change(self) -> None:
        """Detect a writer change and invalidate the demoted writer's tracked
        connections INLINE (sync parity: aurora_connection_tracker_plugin.py
        :356-357 -- sync offloads the closes to a daemon Thread that outlives
        the failover call; an asyncio task has no such lifetime guarantee, it
        dies un-awaited when the event loop closes, leaving idle connections
        to the demoted writer open. Awaiting here is the async equivalent of
        sync's guarantee that invalidation always completes)."""
        old_writer = self._update_writer_from_topology()
        if old_writer is None:
            return
        await self._tracker.invalidate_all(old_writer)
        self._tracker.log_opened_connections()

    def _current_writer_from_hosts(self) -> Optional[HostInfo]:
        for h in self._plugin_service.all_hosts:
            if h.role == HostRole.WRITER:
                return h
        return None

    @staticmethod
    def _same_host(a: HostInfo, b: HostInfo) -> bool:
        return a.host == b.host and a.port == b.port

    def _spawn_invalidation(self, key: str) -> None:
        """Fire-and-forget invalidation, ONLY for the sync-signature
        :meth:`notify_host_list_changed` path (cannot await there). The
        execute/failover path awaits :meth:`_invalidate_writer_change`
        inline instead -- a spawned task dies un-awaited if the event loop
        closes before it runs. Pending tasks from this path are drained by
        :meth:`_shutdown` on ``release_resources_async``."""
        task = asyncio.create_task(self._tracker.invalidate_all_for_keys(key))
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

        """
        for host, events in changes.items():
            if HostEvent.CONVERTED_TO_READER in events:
                # Change keys are urls; tracking keys are not.
                self._spawn_invalidation(host.rstrip("/"))
            if HostEvent.CONVERTED_TO_WRITER in events:
                # Force writer recheck on next execute via
                # _update_writer_from_topology.
                self._current_writer = None


__all__ = ["AsyncAuroraConnectionTrackerPlugin", "AsyncOpenedConnectionTracker"]
