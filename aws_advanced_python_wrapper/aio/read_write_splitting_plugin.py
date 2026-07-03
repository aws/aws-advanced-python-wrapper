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

"""Async read/write splitting plugin.

Routes connections based on ``set_read_only``: when the app sets
read-only True, the plugin swaps the current connection for one
targeting a reader; when read-only flips back to False, it swaps back
to the writer. Saves the writer connection so returning from read-only
is cheap.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable, List, Optional, Set

from aws_advanced_python_wrapper.aio._rws_failover_eviction import \
    _AsyncRwsFailoverEvictionMixin
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.notifications import \
    OldConnectionSuggestedAction
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

logger = Logger(__name__)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.notifications import ConnectionEvent
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncReadWriteSplittingPlugin(
        _AsyncRwsFailoverEvictionMixin, AsyncPlugin):
    """Async counterpart of :class:`ReadWriteSplittingPlugin`."""

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
        # Execute pipeline -- subscribed so a FailoverError raised while a
        # command runs evicts stale pooled reader/writer connections
        # (sync #1117 parity via _AsyncRwsFailoverEvictionMixin). Without this,
        # an internal-pool RWS connection that fails over mid-query is returned
        # to the pool stale.
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_list_provider: AsyncHostListProvider,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._host_list_provider = host_list_provider
        self._props = props
        self._writer_conn: Optional[Any] = None
        self._writer_host_info: Optional[HostInfo] = None
        self._reader_conn: Optional[Any] = None
        self._reader_host_info: Optional[HostInfo] = None
        # True while an RWS-initiated switch is in flight; makes
        # notify_connection_changed return PRESERVE so the plugin service
        # doesn't close the old connection we still cache (sync parity).
        self._in_read_write_split = False

        # Telemetry counters -- one per successful reader/writer swap.
        # NullTelemetryFactory returns a no-op object; real factories may
        # return None, so every .inc() guards with ``is not None``.
        tf = self._plugin_service.get_telemetry_factory()
        self._switch_to_reader_counter = tf.create_counter(
            "rws.switches.to_reader.count")
        self._switch_to_writer_counter = tf.create_counter(
            "rws.switches.to_writer.count")

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    def notify_connection_changed(
            self, changes: Set[ConnectionEvent]) -> OldConnectionSuggestedAction:
        """Re-cache the current connection under its role whenever the
        connection OBJECT changes (failover or an RWS swap).

        Without this, a reader-failover moves the live connection to a NEW
        reader but the RWS reader cache still points at the OLD reader, so a
        later set_read_only(True) does not return to the failover reader
        (test_failover_to_new_reader__switch_read_only_async). Sync parity:
        ReadWriteSplittingPlugin.notify_connection_changed ->
        _update_internal_connection_info. Uses the already-known host role (no
        query), so it is safe in this synchronous hook. The reuse checks in
        _switch_to_reader/_switch_to_writer still validate the cached
        connection (not closed + in topology) before reusing it.
        """
        current_conn = self._plugin_service.current_connection
        current_host = self._plugin_service.current_host_info
        if current_conn is None or current_host is None:
            return OldConnectionSuggestedAction.NO_OPINION
        if current_host.role == HostRole.WRITER:
            self._writer_conn = current_conn
            self._writer_host_info = current_host
        elif current_host.role == HostRole.READER:
            self._reader_conn = current_conn
            self._reader_host_info = current_host

        # While an RWS-initiated switch is in flight, the "old" connection is
        # one of our cached reader/writer connections -- tell the service to
        # PRESERVE it instead of closing it (sync parity:
        # read_write_splitting_plugin.notify_connection_changed).
        if self._in_read_write_split:
            return OldConnectionSuggestedAction.PRESERVE
        return OldConnectionSuggestedAction.NO_OPINION

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        conn = await connect_func()
        if is_initial_connection:
            # aio/wrapper.py builds the initial HostInfo with no role, so it
            # defaults to HostRole.WRITER even when the app connected straight to
            # a reader instance. Verify the ACTUAL role via the data plane and
            # correct the host info, otherwise a reader is mistaken for the
            # writer: it gets cached as the writer below and a later
            # set_read_only(False) "reuses" it and never switches. Sync parity:
            # read_write_splitting_plugin.connect (lines 459-486).
            try:
                actual_role = await self._plugin_service.get_host_role(conn)
            except Exception:  # noqa: BLE001 - role probe is best-effort
                actual_role = None
            if (actual_role is not None
                    and actual_role != HostRole.UNKNOWN
                    and host_info.role != actual_role):
                host_info.role = actual_role
                initial = self._plugin_service.initial_connection_host_info
                if initial is not None and initial.host == host_info.host:
                    initial.role = actual_role

            # Seed the connection cache under the correct role only (seed the
            # writer cache only when this really is the writer).
            if self._writer_conn is None and host_info.role == HostRole.WRITER:
                self._writer_conn = conn
                self._writer_host_info = host_info
            elif self._reader_conn is None and host_info.role == HostRole.READER:
                self._reader_conn = conn
                self._reader_host_info = host_info
        return conn

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if method_name == DbApiMethod.CONNECTION_SET_READ_ONLY.method_name:
            await self._maybe_switch_for_read_only(args)
        # Funnel every subscribed call (set_read_only + the execute pipeline)
        # through the failover-eviction wrapper so a FailoverError raised while
        # the command runs closes stale pooled reader/writer connections
        # (sync #1117 parity).
        return await self._execute_with_failover_eviction(
            method_name, execute_func)

    async def _maybe_switch_for_read_only(self, args: tuple) -> None:
        """Swap the current connection to a reader/writer to match the
        requested read-only state (no-op when already on the correct kind of
        host). Invoked before the wrapped ``set_read_only`` call in
        :meth:`execute`; raises ReadWriteSplittingError for a mid-transaction
        writer swap.
        """
        read_only = bool(args[0]) if args else False

        driver_dialect = self._plugin_service.driver_dialect
        current = self._plugin_service.current_connection
        if current is None:
            return

        # Current host role gates the switch: don't swap (and risk reusing a
        # stale cache) when we are already on the right kind of host. Mirrors
        # sync _set_read_only's `not _is_reader/_is_writer(current_host)` guards
        # (read_write_splitting_plugin.py:204-222). connect() above corrects the
        # initial role, and set_current_connection keeps it accurate after swaps.
        current_host = self._plugin_service.current_host_info
        current_role = current_host.role if current_host is not None else None

        # current_host.role is cached metadata and can be stale or wrong --
        # notably for a reader-CLUSTER (cluster-ro) endpoint connection, whose
        # host_info.host is the endpoint name (not a topology instance), so the
        # role correction at connect() doesn't reliably stick under load. Gate
        # the switch on a FRESH data-plane probe (a reliable local is_reader
        # query, e.g. pg_is_in_recovery) so we never switch off a connection
        # that is already on the correct kind of host -- this is the flaky
        # test_connect_to_reader_cluster__switch_read_only_async. Fall back to
        # the cached role only if the probe fails.
        try:
            live_role = await self._plugin_service.get_host_role(current)
        except Exception:  # noqa: BLE001 - probe is best-effort
            live_role = None
        if live_role is not None and live_role != HostRole.UNKNOWN:
            current_role = live_role

        if read_only:
            # Sync parity (read_write_splitting_plugin.py:243-249): mid-txn
            # reader swap silently no-ops; caller's set_read_only(True) still
            # proceeds on the writer connection. The swap happens only when
            # safe.
            if (current_role != HostRole.READER
                    and not await driver_dialect.is_in_transaction(current)):
                try:
                    await self._switch_to_reader(driver_dialect, current)
                except Exception:  # noqa: BLE001
                    # Sync parity (read_write_splitting_plugin.py:209-221): if no
                    # reader can be opened (e.g. every reader is in topology but
                    # unreachable) but the current connection is still usable, stay
                    # on it and warn rather than failing the set_read_only(True)
                    # call. Only propagate when the current connection is also dead.
                    if await driver_dialect.is_closed(current):
                        raise
                    host = self._plugin_service.current_host_info
                    logger.warning(
                        "ReadWriteSplittingPlugin.FallbackToCurrentConnection",
                        host.url if host is not None else "current")
        else:
            # Sync parity (read_write_splitting_plugin.py:261-265): mid-txn
            # writer swap raises -- the transaction's fate is undefined if we
            # swap connections underneath it.
            if current_role != HostRole.WRITER:
                if await driver_dialect.is_in_transaction(current):
                    raise ReadWriteSplittingError(
                        "Cannot switch back to the writer while in a transaction. "
                        "Commit or roll back before setting the connection "
                        "read-write.")
                await self._switch_to_writer(driver_dialect, current)

    @staticmethod
    def _host_in_topology(
            host_info: HostInfo,
            topology: tuple) -> bool:
        """Return True if ``host_info`` (by host+port) is in the topology."""
        for h in topology:
            if h.host == host_info.host and h.port == host_info.port:
                return True
        return False

    def _select_reader_candidates(self, topology: tuple) -> List[HostInfo]:
        """Reader hosts eligible for selection. Hook for subclasses (the gdb
        variant restricts to the home region). Base: every reader in the
        topology. Mirrors sync ``_get_reader_host_candidates``.
        """
        return [h for h in topology if h.role == HostRole.READER]

    async def _should_switch_to_writer(self, writer_host: HostInfo) -> bool:
        """Whether to proceed switching the current connection to
        ``writer_host``. Hook for subclasses (the gdb variant enforces the
        home-region restriction / global write forwarding). Base: always
        switch.
        """
        return True

    def _is_pool_connection(self, conn: Any) -> bool:
        """Return True if ``conn`` is managed by a pool provider.

        Primary check: ask the AsyncConnectionProviderManager whether
        a user-registered pool provider accepts the current host info.
        Falls back to the SQLAlchemy-pool module-string heuristic for
        back-compat with deployments that hook SA's pool without
        going through our provider manager.
        """
        if conn is None:
            return False
        # Provider-manager check (preferred).
        try:
            manager = self._plugin_service.get_connection_provider_manager()
            host_info = self._plugin_service.current_host_info
            props = self._plugin_service.props
            if host_info is not None:
                provider = manager.get_connection_provider(host_info, props)
                if provider is not manager.default_provider:
                    return True
        except Exception:  # noqa: BLE001 - best-effort
            pass
        # Back-compat heuristic for SA pool connections.
        module = getattr(type(conn), "__module__", "")
        return module.startswith("sqlalchemy.pool")

    async def _release_pool_conn(
            self,
            conn: Any,
            driver_dialect: AsyncDriverDialect) -> None:
        """Close ``conn`` if it's from SQLAlchemy's pool so it returns to
        the pool rather than staying held by our plugin.
        """
        if not self._is_pool_connection(conn):
            return
        try:
            if await driver_dialect.is_closed(conn):
                return
        except Exception:  # noqa: BLE001 - probe is best-effort
            return
        try:
            close = conn.close
            result = close()
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 - close is best-effort
            pass

    async def _switch_to_reader(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:
        # Fast path: if we're literally on our cached reader connection, stay.
        # The authoritative "is the current connection already a reader" decision
        # is made by execute() via a FRESH get_host_role probe before it calls
        # us, so we deliberately do NOT re-check the (possibly stale)
        # current_host.role here -- doing so conflicted with execute()'s
        # live-role gate (it would early-return on stale READER metadata even
        # when execute() correctly decided to switch).
        if (self._reader_conn is not None
                and self._reader_conn is current
                and not await driver_dialect.is_closed(current)):
            return
        self._in_read_write_split = True
        try:
            await self._switch_to_reader_impl(driver_dialect, current)
        finally:
            self._in_read_write_split = False

    async def _switch_to_reader_impl(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:

        # Cache the writer we came from so we can bounce back quickly.
        if self._writer_conn is None:
            self._writer_conn = current
            if self._plugin_service.current_host_info is not None:
                self._writer_host_info = self._plugin_service.current_host_info

        topology = await self._host_list_provider.refresh(current)
        # Restrict to custom-endpoint members (no-op when none set), so RWS
        # never switches to a host outside the endpoint.
        topology = tuple(self._plugin_service.filter_hosts(list(topology)))

        # Try to reuse cached reader if (a) it's not closed AND (b) its
        # host is still in topology.
        if (self._reader_conn is not None
                and self._reader_host_info is not None
                and self._host_in_topology(self._reader_host_info, topology)
                and not await driver_dialect.is_closed(self._reader_conn)):
            await self._plugin_service.set_current_connection(
                self._reader_conn, self._reader_host_info)
            await self._release_pool_conn(current, driver_dialect)
            if self._switch_to_reader_counter is not None:
                self._switch_to_reader_counter.inc()
            return

        # Cache invalid -> drop and reopen
        self._reader_conn = None
        self._reader_host_info = None

        reader_candidates = self._select_reader_candidates(topology)
        if not reader_candidates:
            # Aurora's ``aurora_replica_status()`` transiently omits a reader
            # row when the replica's LAST_UPDATE_TIMESTAMP lags the query's
            # 300s freshness window, yielding a writer-only topology. Re-probe
            # once with a forced refresh (bypasses the cache the transient may
            # have just poisoned) before giving up.
            topology = await self._host_list_provider.force_refresh(current)
            topology = tuple(self._plugin_service.filter_hosts(list(topology)))
            reader_candidates = self._select_reader_candidates(topology)
        if not reader_candidates:
            if self._plugin_service.allowed_and_blocked_hosts is not None:
                # A custom endpoint constrains the host set and it has no
                # reader member: switching to a reader is genuinely impossible,
                # so fail (the caller's set_read_only(True) must raise) rather
                # than silently staying. (Distinct from a transient
                # reader-less topology below.)
                raise ReadWriteSplittingError(
                    "No reader host is available within the custom endpoint's "
                    "member instances; cannot switch to read-only.")
            # Still no readers. Mirror sync
            # ``ReadWriteSplittingPlugin._initialize_reader_connection``
            # (read_write_splitting_plugin.py:533-542): stay on the current
            # connection and warn rather than raise -- a momentary reader-less
            # topology must not break a ``set_read_only(True)`` call. (When the
            # current connection is itself a reader, this also keeps the caller
            # on a reader, which is the desired read-only routing.)
            host = self._plugin_service.current_host_info
            logger.warning(
                "ReadWriteSplittingPlugin.NoReadersFound",
                host.url if host is not None else "current")
            return

        strategy = (WrapperProperties.READER_HOST_SELECTOR_STRATEGY.get(self._props)
                    or "random")

        # Sync parity (read_write_splitting_plugin.py:578-593): iterate up
        # to 2*len candidates, removing the dead ones. A non-deterministic
        # strategy (round_robin with cluster-level state) may repeat picks;
        # removing from the pool ensures we make forward progress.
        remaining = list(reader_candidates)
        last_error: Optional[Exception] = None
        for _ in range(2 * len(reader_candidates)):
            if not remaining:
                break
            reader = self._plugin_service.get_host_info_by_strategy(
                HostRole.READER, strategy, remaining)
            if reader is None:
                break
            try:
                new_conn = await self._open(driver_dialect, reader)
            except Exception as exc:
                last_error = exc
                remaining = [h for h in remaining if h != reader]
                continue

            self._reader_conn = new_conn
            self._reader_host_info = reader
            await self._plugin_service.set_current_connection(new_conn, reader)
            await self._release_pool_conn(current, driver_dialect)
            if self._switch_to_reader_counter is not None:
                self._switch_to_reader_counter.inc()
            return

        raise ReadWriteSplittingError(
            f"Could not open a reader connection after "
            f"{2 * len(reader_candidates)} candidate attempts."
        ) from last_error

    async def _switch_to_writer(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:
        self._in_read_write_split = True
        try:
            await self._switch_to_writer_impl(driver_dialect, current)
        finally:
            self._in_read_write_split = False

    async def _switch_to_writer_impl(
            self,
            driver_dialect: AsyncDriverDialect,
            current: Any) -> None:
        raw_topology = await self._host_list_provider.refresh(current)
        # Restrict to custom-endpoint members (no-op when none set). If the
        # endpoint has no writer member, the writer pick below is None and we
        # raise -- the caller's set_read_only(False) must fail rather than
        # switch to a non-member writer.
        topology = tuple(self._plugin_service.filter_hosts(list(raw_topology)))

        writer = next(
            (h for h in topology if h.role == HostRole.WRITER),
            None,
        )
        # Subclass hook (gdb): validate/guard the target writer before
        # switching. Returning False skips the switch (e.g. global write
        # forwarding keeps the current reader connection); raising forbids it.
        if writer is not None and not await self._should_switch_to_writer(writer):
            return

        # Try to reuse cached writer if valid + in topology + not closed.
        if (self._writer_conn is not None
                and self._writer_conn is not current
                and self._writer_host_info is not None
                and self._host_in_topology(self._writer_host_info, topology)
                and not await driver_dialect.is_closed(self._writer_conn)):
            await self._plugin_service.set_current_connection(
                self._writer_conn, self._writer_host_info)
            await self._release_pool_conn(current, driver_dialect)
            if self._switch_to_writer_counter is not None:
                self._switch_to_writer_counter.inc()
            return

        # Cache invalid -> drop and reopen
        self._writer_conn = None
        self._writer_host_info = None

        if writer is None:
            raise ReadWriteSplittingError(
                "No writer host available in the current topology."
            )
        new_conn = await self._open(driver_dialect, writer)
        self._writer_conn = new_conn
        self._writer_host_info = writer
        await self._plugin_service.set_current_connection(new_conn, writer)
        await self._release_pool_conn(current, driver_dialect)
        if self._switch_to_writer_counter is not None:
            self._switch_to_writer_counter.inc()

    async def _open(
            self,
            driver_dialect: AsyncDriverDialect,
            target: HostInfo) -> Any:
        # Use the SAME target driver connect callable the wrapper was opened
        # with (aiomysql.connect / psycopg.AsyncConnection.connect), wired on
        # the plugin service at connect time. Hardcoding psycopg here made every
        # reader/writer switch on a MySQL cluster try the PostgreSQL driver
        # against a MySQL host -> "Could not open a reader connection".
        target_func = self._plugin_service.get_target_driver_func()
        if target_func is None:
            raise ReadWriteSplittingError(
                "No target driver connect function is available to open a "
                "reader/writer connection.")

        from aws_advanced_python_wrapper.utils.properties import Properties
        props_copy: Properties = Properties(self._plugin_service.props.copy())  # type: ignore[attr-defined]
        props_copy["host"] = target.host
        if target.is_port_specified():
            props_copy["port"] = str(target.port)
        # Route through the connection provider manager so a registered provider
        # (e.g. the pooled provider) actually owns the reader/writer connection.
        # The least_connections strategy ranks readers by pool checkout count, so
        # reader connections MUST go through the pool or every host reads as 0 and
        # the same reader is always picked. Mirrors aio/default_plugin.connect.
        manager = self._plugin_service.get_connection_provider_manager()
        provider = manager.get_connection_provider(target, props_copy)
        database_dialect = self._plugin_service.database_dialect
        if database_dialect is None:
            return await driver_dialect.connect(
                target, props_copy, target_func)
        return await provider.connect(
            target_func,
            driver_dialect,
            database_dialect,
            target,
            props_copy,
        )
