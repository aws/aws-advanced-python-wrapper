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

"""``AsyncAwsWrapperConnection`` + ``AsyncAwsWrapperCursor``.

Async counterparts of :class:`AwsWrapperConnection` and :class:`AwsWrapperCursor`.
Every DB operation routes through ``AsyncPluginManager`` so plugins
(failover, EFM, R/W splitting -- delivered in later sub-projects) can
intercept it.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import (TYPE_CHECKING, Any, Callable, List, Optional, Sequence,
                    Type, Union)

from aws_advanced_python_wrapper.aio.plugin_manager import AsyncPluginManager
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.database_dialect import DatabaseDialectManager
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.telemetry.default_telemetry_factory import \
    DefaultTelemetryFactory
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

logger = Logger(__name__)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
        TelemetryFactory


_TOPOLOGY_REQUIRING_PLUGINS = frozenset({
    "failover",
    "failover_v2",
    "gdb_failover",
    "read_write_splitting",
    "gdb_rw",
    "custom_endpoint",
    "aurora_connection_tracker",
})


def _build_host_list_provider(
        props: Properties,
        driver_dialect: AsyncDriverDialect,
        database_dialect: Any = None,
        monitor_connection_factory: Any = None) -> AsyncHostListProvider:
    """Pick an async host list provider based on plugins + DB dialect.

    Selection order, matching sync database_dialect.py:
      * ``plugins`` references no topology-requiring plugin -> static.
      * database_dialect is a MultiAz dialect -> MultiAz provider.
      * database_dialect is a GlobalAurora dialect -> GlobalAurora provider.
      * Otherwise -> Aurora provider (the default topology path).

    ``database_dialect`` is passed optionally because early-phase callers
    (tests, legacy paths) may not have resolved the dialect yet; the
    function falls back to a plain Aurora provider in that case.
    """
    from aws_advanced_python_wrapper.aio.host_list_provider import (
        AsyncAuroraHostListProvider, AsyncGlobalAuroraHostListProvider,
        AsyncMultiAzHostListProvider, AsyncStaticHostListProvider)
    from aws_advanced_python_wrapper.aio.plugin_factory import \
        parse_plugins_property

    codes = parse_plugins_property(props) or []
    if not any(c.strip() in _TOPOLOGY_REQUIRING_PLUGINS for c in codes):
        return AsyncStaticHostListProvider(props)

    # Topology-aware path. Pick the provider matching the dialect.
    if database_dialect is not None:
        dialect_name = type(database_dialect).__name__
        if "MultiAz" in dialect_name:
            # Pull MultiAz queries off the dialect class. Sync exposes
            # them as class-level constants; fall back to no provider
            # if they're missing so callers degrade gracefully.
            writer_q = getattr(
                database_dialect, "_WRITER_HOST_QUERY", None)
            topology_q = getattr(
                database_dialect, "_TOPOLOGY_QUERY", None)
            host_id_q = getattr(
                database_dialect, "_HOST_ID_QUERY",
                getattr(database_dialect, "host_id_query", None))
            if writer_q and topology_q and host_id_q:
                # MySQL's writer_host_query (SHOW REPLICA STATUS) carries the
                # writer host at column 39; PG's single-column query uses 0.
                # Sync threads this via MultiAzTopologyUtils -- mirror it here.
                writer_col = getattr(
                    database_dialect, "_WRITER_HOST_COLUMN_INDEX", 0)
                return AsyncMultiAzHostListProvider(
                    props, driver_dialect,
                    writer_host_query=writer_q,
                    topology_query=topology_q,
                    host_id_query=host_id_q,
                    monitor_connection_factory=monitor_connection_factory,
                    writer_host_column_index=writer_col,
                )
        if "GlobalAurora" in dialect_name:
            topology_q = getattr(
                database_dialect, "topology_query", None)
            if topology_q:
                return AsyncGlobalAuroraHostListProvider(
                    props, driver_dialect, topology_query=topology_q,
                    monitor_connection_factory=monitor_connection_factory)

    return AsyncAuroraHostListProvider(
        props, driver_dialect,
        monitor_connection_factory=monitor_connection_factory)


def _resolve_database_dialect(
        driver_dialect: AsyncDriverDialect,
        props: Properties) -> DatabaseDialect:
    """Minimal DatabaseDialect resolution for Phase A.

    Honors the ``wrapper_dialect`` prop if set; otherwise falls back to the
    driver-dialect's default database dialect via :class:`DatabaseDialectManager`.
    Full auto-upgrade (Aurora-vs-stock detection via DB query) lands in a
    later phase.
    """
    manager = DatabaseDialectManager(props)
    return manager.get_dialect(driver_dialect.dialect_code, props)


async def _upgrade_database_dialect_after_connect(
        database_dialect: Any,
        target_conn: Any,
        driver_dialect: AsyncDriverDialect,
        host_list_provider: AsyncHostListProvider) -> Any:
    """Async parity for ``DatabaseDialectManager.query_for_dialect`` (MySQL).

    ``_resolve_database_dialect`` only does the pattern-based initial guess, so
    an Aurora MySQL *instance*-endpoint connection resolves to ``RdsMysqlDialect``
    (no topology query; inherits ``@@read_only``, which does NOT flip on Aurora
    failover). The async connect path never ran the post-connect upgrade probe,
    so topology discovery used the PostgreSQL-default query (-> empty topology ->
    failover stranded on the seed host) and ``get_host_role`` used ``@@read_only``
    (-> the demoted old writer still reports WRITER).

    Probe the live connection for Aurora MySQL and, when present, switch to the
    Aurora MySQL dialect and wire its ``information_schema.replica_host_status``
    topology query + MySQL port into the provider, then prime the topology cache
    through the (still live) connection. PostgreSQL is untouched -- the provider's
    default topology query already matches Aurora-PG. Best-effort: any probe/
    refresh failure leaves the initial dialect in place.

    Returns the (possibly upgraded) database_dialect.
    """
    if getattr(driver_dialect, "_driver_name", None) != "aiomysql":
        return database_dialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncAuroraHostListProvider
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, DatabaseDialectManager, DialectCode,
        MysqlDatabaseDialect)

    # A base/RDS MySQL dialect needs a live probe; a cluster-endpoint connect
    # already resolved to AuroraMysqlDialect by pattern and skips the probe
    # (but still needs the provider wired below).
    if (not isinstance(database_dialect, AuroraMysqlDialect)
            and isinstance(database_dialect, MysqlDatabaseDialect)):
        try:
            async with target_conn.cursor() as cur:
                await cur.execute("SHOW VARIABLES LIKE 'aurora_version'")
                # fetchall (not fetchone) so the result set is fully drained --
                # a partially-read result leaves the shared aiomysql connection
                # dirty and the app's next query reads the leftover row.
                is_aurora = len(await cur.fetchall()) > 0
            if is_aurora:
                aurora = DatabaseDialectManager._known_dialects_by_code.get(
                    DialectCode.AURORA_MYSQL)
                if aurora is not None:
                    database_dialect = aurora
        except Exception:  # noqa: BLE001 - keep initial dialect on probe failure
            pass

    if (isinstance(database_dialect, AuroraMysqlDialect)
            and isinstance(host_list_provider, AsyncAuroraHostListProvider)):
        host_list_provider.reconfigure_topology_query(
            database_dialect.topology_query, default_port=3306)
        # The caller (connect) primes both the provider cache AND
        # plugin_service.all_hosts via force_refresh_host_list right after this
        # returns, so the topology is fetched once with the corrected query.
    return database_dialect


class AsyncAwsWrapperCursor:
    """Async counterpart of :class:`AwsWrapperCursor`.

    Wraps a driver-async cursor; every query/fetch routes through the plugin
    pipeline via :class:`AsyncPluginManager`.
    """

    def __init__(
            self,
            conn: AsyncAwsWrapperConnection,
            target_cursor: Any) -> None:
        self._conn = conn
        self._target_cursor = target_cursor

    @property
    def connection(self) -> AsyncAwsWrapperConnection:
        return self._conn

    @property
    def target_cursor(self) -> Any:
        return self._target_cursor

    @property
    def description(self) -> Any:
        return self._target_cursor.description

    @property
    def rowcount(self) -> int:
        return self._target_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._target_cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._target_cursor.arraysize = value

    @property
    def lastrowid(self) -> Any:
        return self._target_cursor.lastrowid

    async def _resolve_target_cursor(self) -> Any:
        """Resolve and cache the underlying driver cursor.

        psycopg's ``AsyncConnection.cursor()`` returns a ready cursor
        synchronously, so ``AsyncAwsWrapperConnection.cursor()`` -- a sync
        factory matching psycopg semantics -- can wrap it directly. aiomysql's
        ``Connection.cursor()`` instead returns a ``_ContextManager`` that must
        be awaited to yield the real cursor. Await-and-cache on first use so a
        single sync ``cursor()`` factory works for both drivers. Idempotent:
        once resolved, ``_target_cursor`` exposes ``execute`` and is left
        untouched.
        """
        tc = self._target_cursor
        if not hasattr(tc, "execute") and inspect.isawaitable(tc):
            self._target_cursor = await tc
        return self._target_cursor

    async def execute(
            self,
            query: Any,
            params: Any = None,
            **kwargs: Any) -> AsyncAwsWrapperCursor:
        await self._resolve_target_cursor()

        async def _call() -> Any:
            if params is None:
                return await self._target_cursor.execute(query, **kwargs)
            return await self._target_cursor.execute(query, params, **kwargs)

        await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_EXECUTE, _call, query, params,
        )
        return self

    async def executemany(
            self,
            query: Any,
            seq_of_params: Sequence[Any],
            **kwargs: Any) -> AsyncAwsWrapperCursor:
        await self._resolve_target_cursor()

        async def _call() -> Any:
            return await self._target_cursor.executemany(
                query, seq_of_params, **kwargs)

        await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_EXECUTEMANY, _call, query, seq_of_params,
        )
        return self

    async def fetchone(self) -> Any:
        await self._resolve_target_cursor()

        async def _call() -> Any:
            return await self._target_cursor.fetchone()

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_FETCHONE, _call,
        )

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        await self._resolve_target_cursor()

        async def _call() -> Any:
            if size is None:
                return await self._target_cursor.fetchmany()
            return await self._target_cursor.fetchmany(size)

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_FETCHMANY, _call, size,
        )

    async def fetchall(self) -> List[Any]:
        await self._resolve_target_cursor()

        async def _call() -> Any:
            return await self._target_cursor.fetchall()

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_FETCHALL, _call,
        )

    def __aiter__(self) -> AsyncAwsWrapperCursor:
        """Async-idiomatic port of the sync cursor's ``__iter__``
        (wrapper.py:432-433): supports ``async for row in cursor``.

        Deliberate deviation from sync: sync iterates the raw driver cursor
        directly (bypassing plugins); here each row is fetched via the
        plugin-routed :meth:`fetchone` so failover/RWS see the fetches.
        """
        return self

    async def __anext__(self) -> Any:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def close(self) -> None:
        await self._resolve_target_cursor()

        async def _call() -> Any:
            return await self._target_cursor.close()

        await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_CLOSE, _call,
        )

    async def scroll(self, value: int, mode: str = "relative") -> Any:
        """Advance/rewind the cursor (PEP 249 optional extension).

        Sync drivers expose ``scroll`` as sync; async drivers may make it
        a coroutine. We probe the return value and await only when needed
        so the same wrapper method works for both shapes.
        """
        await self._resolve_target_cursor()

        async def _call() -> Any:
            result = self._target_cursor.scroll(value, mode)
            if asyncio.iscoroutine(result):
                return await result
            return result

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_SCROLL, _call, value, mode,
        )

    async def callproc(self, procname: str, args: Any = ()) -> Any:
        """Call a stored procedure (PEP 249 optional extension).

        Like :meth:`scroll`, probes the target's return value and awaits
        only if the driver made ``callproc`` async.
        """
        await self._resolve_target_cursor()

        async def _call() -> Any:
            result = self._target_cursor.callproc(procname, args)
            if asyncio.iscoroutine(result):
                return await result
            return result

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_CALLPROC, _call, procname, args,
        )

    async def nextset(self) -> Optional[bool]:
        """Advance to the next result set (PEP 249).

        Probes the target's return value and awaits only if async.
        """
        await self._resolve_target_cursor()

        async def _call() -> Any:
            result = self._target_cursor.nextset()
            if asyncio.iscoroutine(result):
                return await result
            return result

        return await self._conn._plugin_manager.execute(
            self, DbApiMethod.CURSOR_NEXTSET, _call,
        )

    def setinputsizes(self, sizes: Any) -> None:
        """PEP 249 input-size hint. Pass-through to target cursor (sync,
        no network I/O worth intercepting)."""
        self._target_cursor.setinputsizes(sizes)

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """PEP 249 output-size hint. Pass-through to target cursor (sync,
        no network I/O worth intercepting)."""
        self._target_cursor.setoutputsize(size, column)

    async def __aenter__(self) -> AsyncAwsWrapperCursor:
        await self._resolve_target_cursor()
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Any) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        """Proxy unknown attributes to the underlying cursor.

        Lets driver-specific attrs (e.g., psycopg's ``statusmessage``) work
        transparently when SA or application code asks for them. This includes
        single-underscore driver methods: SQLAlchemy's AsyncAdapt_psycopg_cursor
        .close() calls ``self._cursor._close()`` on the DBAPI cursor, so the
        wrapper must forward ``_close`` to the psycopg cursor. Only dunders are
        kept on the wrapper (pickle/copy internals), and ``_target_cursor`` is
        guarded by name so a miss before __init__ sets it raises cleanly instead
        of recursing through this method.
        """
        if name == "_target_cursor" or name.startswith("__"):
            raise AttributeError(name)
        return getattr(self._target_cursor, name)


class AsyncAwsWrapperConnection:
    """Async counterpart of :class:`AwsWrapperConnection`."""

    __module__ = "aws_advanced_python_wrapper.aio"

    def __init__(
            self,
            plugin_service: AsyncPluginServiceImpl,
            plugin_manager: AsyncPluginManager,
            target_conn: Any) -> None:
        self._plugin_service = plugin_service
        self._plugin_manager = plugin_manager
        self._target_conn = target_conn

    @property
    def target_connection(self) -> Any:
        """The underlying driver async connection."""
        return self._target_conn

    @property
    def autocommit(self) -> bool:
        """Current autocommit setting as a plain ``bool`` (sync property).

        Parity with the sync wrapper's ``autocommit`` property and with this
        wrapper's :attr:`read_only`. The async driver dialect's
        ``get_autocommit`` is a coroutine, but every supported driver exposes
        autocommit *synchronously* -- psycopg ``AsyncConnection.autocommit``
        (a bool property), aiomysql ``Connection.get_autocommit()`` -- so we
        answer here without awaiting. Returning the coroutine instead made
        ``conn.autocommit is False`` (no await) silently false and forced some
        callers to ``await`` it, inconsistent with the sync wrapper. The setter
        is still :meth:`set_autocommit` (a coroutine), since property setters
        can't be async.

        NOTE: because a property getter can't ``await``, this reads the DRIVER
        connection directly and BYPASSES the plugin chain (unlike the sync
        wrapper's ``autocommit`` getter, wrapper.py:131-136, which routes
        ``CONNECTION_AUTOCOMMIT`` through the plugins). Use the coroutine
        :meth:`get_autocommit` for the plugin-routed read.
        """
        target = self._target_conn
        ac = getattr(target, "autocommit", False)
        # aiomysql exposes ``autocommit`` as a setter *method* and reads via
        # ``get_autocommit()``; psycopg exposes ``autocommit`` as a sync bool
        # property. Discriminate on callability (robust vs. driver shape).
        if callable(ac):
            return bool(target.get_autocommit())
        return bool(ac)

    async def set_autocommit(self, value: bool) -> None:
        """Set autocommit on the underlying driver connection.

        Awaited counterpart of the :attr:`autocommit` getter. See that
        docstring for the reason this isn't an ``@autocommit.setter``.
        """
        # Record the desired autocommit in the session-state service so a
        # plugin-driven connection switch (RWS reader/writer swap, failover)
        # re-applies it on the new connection via apply_current_session_state.
        # Mirrors sync wrapper._set_autocommit. Without this, a reader opened by
        # set_read_only(True) keeps its default autocommit and loses the
        # caller's setting (test_autocommit_state_preserved_across_connection_
        # switches_async). Best-effort: state-tracking must not fail the set.
        ss = getattr(self._plugin_service, "session_state_service", None)
        if ss is not None:
            try:
                await ss.setup_pristine_autocommit(value)
            except Exception as ex:  # noqa: BLE001
                logger.debug(
                    f"[AsyncAwsWrapperConnection] failed to record pristine "
                    f"autocommit in session state; it may not be restored on a "
                    f"connection switch: {ex}")
        await self._plugin_service.driver_dialect.set_autocommit(self._target_conn, value)
        if ss is not None:
            try:
                ss.set_autocommit(value)
            except Exception as ex:  # noqa: BLE001
                logger.debug(
                    f"[AsyncAwsWrapperConnection] failed to record autocommit "
                    f"in session state; it may not survive a connection "
                    f"switch (failover / RWS): {ex}")

    async def get_autocommit(self) -> bool:
        """Read autocommit through the plugin chain.

        Coroutine counterpart of the sync wrapper's ``autocommit`` getter
        (wrapper.py:131-136), which routes ``CONNECTION_AUTOCOMMIT`` through
        ``plugin_manager.execute`` with the driver dialect's ``get_autocommit``
        as the terminal call. The sync :attr:`autocommit` property stays as a
        direct driver read for back-compat (it can't await).
        """
        async def _call() -> bool:
            conn = self._plugin_service.current_connection
            if conn is None:
                conn = self._target_conn
            return await self._plugin_service.driver_dialect.get_autocommit(conn)

        return await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_AUTOCOMMIT, _call,
        )

    @property
    def isolation_level(self) -> Any:
        """Current isolation level, read directly from the driver connection.

        Drivers vary in how they model isolation level (psycopg exposes
        it as an attribute; mysql drivers typically don't). We return
        whatever the target exposes, or ``None`` if the driver doesn't
        surface it.
        """
        return getattr(self._target_conn, "isolation_level", None)

    async def set_isolation_level(self, level: Any) -> None:
        """Set isolation level on the underlying driver connection.

        Drivers vary (psycopg uses an enum; mysql uses a SQL string).
        Delegates to the raw connection's ``set_isolation_level`` if
        present (awaiting if async), else assigns the attribute.
        """
        target = self._target_conn
        setter = getattr(target, "set_isolation_level", None)
        if setter is not None:
            result = setter(level)
            if asyncio.iscoroutine(result):
                await result
            return
        # Fallback: attribute assignment.
        target.isolation_level = level

    # ---- psycopg3-parity passthroughs that need explicit handling ------
    #
    # Driver-specific attributes (add_notice_handler, info, cancel, ...) are
    # forwarded automatically by __getattr__. The members below stay explicit
    # because __getattr__ can't express them: they route through the plugin
    # chain (execute/set_read_only/set_autocommit), have cross-driver fallback
    # (set_isolation_level), or implement a SQLAlchemy adapter contract.

    @property
    def closed(self) -> bool:
        """Whether the underlying driver connection is closed.

        SA's psycopg(-async) dialect reads ``closed`` directly during
        pool-invalidation paths after a DBAPI exception is classified.
        Mirrors the sync wrapper's ``closed`` passthrough.
        """
        return bool(getattr(self._target_conn, "closed", False))

    @property
    def is_closed(self) -> bool:
        """Whether the connection is closed -- parity with the sync wrapper's
        ``is_closed`` property (wrapper.py:88) that users and the failover
        parity tests rely on (``assert conn.is_closed is False/True``).

        The async driver dialect's ``is_closed`` is a coroutine, but every
        supported async driver exposes closed-state synchronously
        (psycopg ``AsyncConnection.closed``; aiomysql ``Connection.closed``),
        so we answer here without awaiting. Without this property
        ``__getattr__`` forwards ``is_closed`` to the raw driver connection,
        which has no such attribute -> AttributeError.
        """
        target = self._target_conn
        if hasattr(target, "closed"):
            return bool(target.closed)
        # Both psycopg's AsyncConnection and aiomysql 0.3.2's Connection expose
        # ``closed``; the ``open`` fallback covers a pool proxy of the
        # pymysql/mysql.connector shape (``open``, the inverse of ``closed``).
        if hasattr(target, "open"):
            return not bool(target.open)
        return False

    @property
    def prepare_threshold(self) -> Any:
        return self._target_conn.prepare_threshold

    @prepare_threshold.setter
    def prepare_threshold(self, value: Any) -> None:
        self._target_conn.prepare_threshold = value

    @property
    def prepared_max(self) -> Any:
        return self._target_conn.prepared_max

    @prepared_max.setter
    def prepared_max(self, value: Any) -> None:
        self._target_conn.prepared_max = value

    @property
    def read_only(self) -> bool:
        """Current read-only intent, normalized to a plain ``bool``.

        psycopg exposes ``read_only`` as a tri-state (``None`` == server
        default / unset); aiomysql has no native flag, so the async aiomysql
        driver dialect stashes intent on ``_aws_read_only``. Mirror the async
        driver dialects' ``is_read_only`` normalization so callers get the
        same ``bool`` surface as the sync wrapper -- a bare passthrough
        returns psycopg's ``None`` and fails ``assert conn.read_only is False``.

        NOTE: because a property getter can't ``await``, this reads the DRIVER
        connection directly and BYPASSES the plugin chain (unlike the sync
        wrapper's ``read_only`` getter, wrapper.py:106-111, which routes
        ``CONNECTION_IS_READ_ONLY`` through the plugins). Use the coroutine
        :meth:`get_read_only` for the plugin-routed read.
        """
        val = getattr(self._target_conn, "read_only", None)
        if val is None:
            val = getattr(self._target_conn, "_aws_read_only", False)
        return bool(val)

    async def get_read_only(self) -> bool:
        """Read read-only state through the plugin chain.

        Coroutine counterpart of the sync wrapper's ``read_only`` getter
        (wrapper.py:106-111 + ``_is_read_only`` at :121-124): routes
        ``CONNECTION_IS_READ_ONLY`` through ``plugin_manager.execute``; the
        terminal call asks the driver dialect and records the pristine
        read-only in the session-state service (so a later plugin-driven
        connection switch can restore it). The sync :attr:`read_only` property
        stays as a direct driver read for back-compat (it can't await).
        """
        async def _call() -> bool:
            conn = self._plugin_service.current_connection
            if conn is None:
                conn = self._target_conn
            is_read_only = await self._plugin_service.driver_dialect.is_read_only(conn)
            await self._plugin_service.session_state_service.setup_pristine_readonly(
                is_read_only)
            return is_read_only

        return await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_IS_READ_ONLY, _call,
        )

    async def set_read_only(self, value: Any) -> None:
        """Set read-only, routing through the plugin pipeline.

        Mirrors the sync wrapper's ``read_only`` setter (wrapper.py:99),
        which sends ``CONNECTION_SET_READ_ONLY`` through
        ``plugin_manager.execute``. This is what lets the read/write-splitting
        plugin intercept the call and swap reader/writer connections -- a bare
        ``await self._target_conn.set_read_only(...)`` bypasses every plugin,
        so RWS never switches and ``conn.read_only`` toggles have no routing
        effect.
        """
        # A closed connection can't change read/write mode. Without this the
        # RWS plugin would happily open a *fresh* reader and silently succeed,
        # masking the error the caller expects (parity with the sync wrapper,
        # which fails when operating on a closed connection).
        if self.is_closed:
            raise AwsWrapperError(
                Messages.get("ReadWriteSplittingPlugin.SetReadOnlyOnClosedConnection"))

        async def _call() -> None:
            # Terminal mirrors sync _set_read_only: track session state, then
            # apply on the CURRENT connection -- the RWS plugin may have just
            # swapped it to a reader/writer ahead of this terminal running.
            ss = self._plugin_service.session_state_service
            await ss.setup_pristine_readonly(value)
            dd = self._plugin_service.driver_dialect
            conn = self._plugin_service.current_connection
            # psycopg rejects set_read_only while the connection is
            # mid-transaction ("can't change 'read_only' now: ... INTRANS").
            # After an RWS connection-switch (or SQLAlchemy's pool-reset on
            # close), a transient switch/probe query can leave the (new)
            # connection INTRANS; roll that transient transaction back so the
            # session-level read_only flip can apply
            # (test_sqlalchemy_creator_read_write_splitting).
            #
            # Roll back ONLY a transient, non-USER transaction. A genuine user
            # transaction (the plugin service tracks BEGIN/COMMIT) must NOT be
            # silently rolled back: for set_read_only(True) mid-user-txn the
            # driver (psycopg) correctly REJECTS the change and the caller
            # expects that error (test_set_read_only_true_in_transaction). A
            # set_read_only(False) mid-user-txn never reaches this terminal --
            # the RWS plugin raises first -- so guarding on the user-txn flag
            # only spares switch/reset artifacts from the rollback.
            try:
                if (conn is not None
                        and not self._plugin_service.is_in_transaction
                        and await dd.is_in_transaction(conn)):
                    rb = conn.rollback()
                    if asyncio.iscoroutine(rb):
                        await rb
            except Exception:  # noqa: BLE001 - best-effort cleanup
                pass
            await dd.set_read_only(conn, value)
            ss.set_read_only(value)

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_SET_READ_ONLY, _call, value)

    async def execute(
            self,
            query: Any,
            params: Any = None,
            *,
            prepare: Any = None,
            binary: bool = False) -> Any:
        # psycopg3's AsyncConnection.execute() is a shortcut that opens
        # a cursor and runs the query in one call. Route via our cursor
        # so the query goes through the plugin chain (failover, RWS,
        # etc.) instead of bypassing it -- otherwise SQLAlchemy and
        # other psycopg-aware callers can issue SQL that's invisible
        # to plugins.
        cursor = self.cursor()
        await cursor.execute(query, params, prepare=prepare, binary=binary)
        return cursor

    @staticmethod
    async def connect(
            target: Union[None, str, Callable] = None,
            conninfo: str = "",
            *args: Any,
            plugins: Union[None, str, List[AsyncPlugin]] = None,
            **kwargs: Any) -> AsyncAwsWrapperConnection:
        """Open a new async wrapper connection.

        :param target: the target driver's async connect callable (e.g.,
            ``psycopg.AsyncConnection.connect``). Required.
        :param conninfo: connection info string (driver-specific format).
        :param plugins: accepts three shapes:
            * ``list[AsyncPlugin]`` -- explicit plugin instances; takes
              precedence over any ``plugins`` connection-property string.
            * ``str`` -- comma-separated plugin codes (e.g.,
              ``"failover,host_monitoring_v2"``); routed into the props dict and
              resolved via :mod:`plugin_factory`.
            * ``None`` -- defer to the ``plugins`` connection-property
              string (if present); when absent, no plugins load.
        :param kwargs: merged into the connection properties alongside
            ``conninfo``.
        """
        if not target:
            raise AwsWrapperError(Messages.get("Wrapper.RequiredTargetDriver"))
        if not callable(target):
            raise AwsWrapperError(Messages.get("Wrapper.ConnectMethod"))
        target_func: Callable = target

        # Normalize `plugins` kwarg: string form folds into the props
        # dict so the factory path resolves it just like a property.
        if isinstance(plugins, str):
            kwargs["plugins"] = plugins
            plugins = None

        props: Properties = PropertiesUtils.parse_properties(
            conn_info=conninfo, **kwargs)

        # TOP_LEVEL telemetry span around the connect body, mirroring the
        # sync wrapper (wrapper.py:172-195): the context is named after this
        # module (sync uses its own ``__name__``), gets the exception + a
        # failed-success flag on error, and is closed in ``finally``.
        # None-guarded like the plugin manager's spans -- with telemetry
        # disabled (the default) DefaultTelemetryFactory returns ``None``
        # contexts and this collapses to guard checks.
        telemetry_factory = DefaultTelemetryFactory(props)
        context = telemetry_factory.open_telemetry_context(
            __name__, TelemetryTraceLevel.TOP_LEVEL)
        try:
            return await AsyncAwsWrapperConnection._connect_pipeline(
                target_func, props, plugins, telemetry_factory)
        except Exception as ex:
            if context is not None:
                context.set_exception(ex)
                context.set_success(False)
            raise ex
        finally:
            if context is not None:
                context.close_context()

    @staticmethod
    async def _connect_pipeline(
            target_func: Callable,
            props: Properties,
            plugins: Optional[List[AsyncPlugin]],
            telemetry_factory: TelemetryFactory) -> AsyncAwsWrapperConnection:
        """Body of :meth:`connect`, factored out so the TOP_LEVEL telemetry
        span in ``connect`` brackets it exactly like the sync wrapper's
        try/except/finally (wrapper.py:175-195)."""
        # Parity with the sync wrapper (PluginManager.create_plugins): when
        # NEITHER an explicit plugin list (``plugins=[...]``) NOR a ``plugins``
        # connection property is supplied, apply the default plugin list so
        # failover/EFM/etc. load by default. Inject into props (not into
        # build_async_plugins) so _build_host_list_provider stays in sync -- it
        # reads the same property to choose a topology vs static provider. An
        # explicit list leaves the ``plugins`` param non-None (skipped here); an
        # explicit ``plugins=""`` leaves the key present (skipped -> no plugins).
        # Async MySQL keeps host_monitoring_v2 (aiomysql EFM works via asyncio
        # cancellation), so both async engines use the full DEFAULT_PLUGINS.
        if plugins is None and WrapperProperties.PLUGINS.name not in props:
            props[WrapperProperties.PLUGINS.name] = \
                WrapperProperties.DEFAULT_PLUGINS

        # Pick the driver dialect by the target connect callable's module:
        # aiomysql.connect -> AsyncAiomysqlDriverDialect (MySQL); otherwise the
        # psycopg-async dialect. Previously this was hardcoded to psycopg, so
        # MySQL connections ran through the psycopg dialect -- whose
        # prepare_connect_info leaves a STRING port (aiomysql then raises
        # "%d format: a real number is required, not str"), and whose
        # cursor/transaction/is_closed/abort semantics don't match aiomysql.
        # That mis-selection was the real root cause of the broad MySQL-async
        # failures across all envs, not just the port cast.
        target_module = getattr(target_func, "__module__", "") or ""
        driver_dialect: AsyncDriverDialect
        if "aiomysql" in target_module:
            from aws_advanced_python_wrapper.aio.driver_dialect.aiomysql import \
                AsyncAiomysqlDriverDialect
            driver_dialect = AsyncAiomysqlDriverDialect()
        else:
            from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
                AsyncPsycopgDriverDialect
            driver_dialect = AsyncPsycopgDriverDialect()

        host = props.get("host", "")
        port_raw = props.get("port")
        port = int(port_raw) if port_raw is not None else -1
        host_info = HostInfo(host=host, port=port)

        # Resolve the database dialect first so the host-list-provider
        # selection can pick a MultiAz/GlobalAurora variant when the
        # dialect class indicates it (matches sync's behavior).
        database_dialect = _resolve_database_dialect(driver_dialect, props)

        # Dedicated topology-monitor connection factory: opens a FRESH raw
        # monitoring connection (no plugins) to the seed host with
        # topology-monitoring props. The background topology monitor uses this
        # instead of the shared app connection -- driver connections (aiomysql)
        # can't service concurrent queries, so a background refresh on the app's
        # connection corrupts the app's in-flight query. Mirrors sync's monitor,
        # which force_connects its own connection.
        async def _monitor_conn_factory() -> Any:
            from aws_advanced_python_wrapper.utils.properties import \
                PropertiesUtils
            mon_props = PropertiesUtils.create_topology_monitoring_properties(
                Properties(dict(props)))
            return await driver_dialect.connect(host_info, mon_props, target_func)

        # Host-list-provider selection: if `plugins=...` references any
        # topology-requiring plugin (failover, rws), build a topology
        # provider matching the dialect; otherwise static is enough.
        host_list_provider = _build_host_list_provider(
            props, driver_dialect, database_dialect,
            monitor_connection_factory=_monitor_conn_factory)

        plugin_service = AsyncPluginServiceImpl(
            props=props,
            driver_dialect=driver_dialect,
            host_info=host_info,
        )
        # Share the connect-time telemetry factory (sync parity: sync passes
        # DefaultTelemetryFactory into PluginManager). Must happen BEFORE
        # AsyncPluginManager is built -- the manager caches the service's
        # factory in its __init__.
        plugin_service.set_telemetry_factory(telemetry_factory)
        # Phase A wiring: populate plugin service slots so plugins that
        # reach for them in their own ``connect`` hook (e.g., failover
        # checking ``is_network_exception``) have them available.
        plugin_service.database_dialect = database_dialect
        plugin_service.host_list_provider = host_list_provider
        plugin_service.initial_connection_host_info = host_info

        # Arm the topology monitor's panic-mode writer discovery: when the
        # monitoring connection dies mid-failover, the monitor probes every
        # host through the plugin pipeline to find the new writer (mirrors
        # sync ClusterTopologyMonitor's per-host HostMonitor threads). The
        # probe needs the plugin service, which didn't exist when the
        # provider was built above.
        set_probe = getattr(host_list_provider, "set_probe_host", None)
        if callable(set_probe):
            from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
                build_probe_host
            set_probe(build_probe_host(plugin_service, props))

        # Resolve plugin list. Explicit `plugins=[...]` wins; otherwise
        # parse the `plugins` connection-property string via the factory
        # registry (Task 1-A).
        if plugins is None:
            from aws_advanced_python_wrapper.aio.plugin_factory import \
                build_async_plugins
            plugins = build_async_plugins(
                plugin_service=plugin_service,
                props=props,
                host_list_provider=host_list_provider,
            )

        plugin_manager = AsyncPluginManager(
            plugin_service=plugin_service,
            props=props,
            plugins=plugins,
        )
        plugin_service.plugin_manager = plugin_manager
        plugin_service.set_target_driver_func(target_func)

        target_conn = await plugin_manager.connect(
            target_driver_func=target_func,
            driver_dialect=driver_dialect,
            host_info=host_info,
            props=props,
            is_initial_connection=True,
        )

        if target_conn is None:
            raise AwsWrapperError(
                Messages.get("AwsWrapperConnection.ConnectionNotOpen")
            )

        # Post-connect dialect upgrade (async parity for the sync
        # query_for_dialect step _resolve_database_dialect skips): detect Aurora
        # MySQL on the live connection and wire its topology/role queries before
        # any topology-dependent plugin (failover, RWS) acts on the connection.
        database_dialect = await _upgrade_database_dialect_after_connect(
            database_dialect, target_conn, driver_dialect, host_list_provider)
        plugin_service.database_dialect = database_dialect

        # Prime plugin_service.all_hosts with the correct (post-upgrade)
        # topology. Plugins' own connect hooks ran during plugin_manager.connect
        # -- BEFORE the dialect upgrade above -- so on an instance-endpoint
        # MySQL connect their eager refresh saw the un-upgraded (no-topology)
        # dialect and left all_hosts empty. Without a populated all_hosts BEFORE
        # the first failover, the aurora_connection_tracker observes the new
        # writer as its FIRST writer (not a change) and never invalidates the
        # demoted writer's idle connections (test_writer_failover_in_idle_
        # connections_async). Best-effort: static/no-topology providers no-op.
        try:
            await plugin_service.force_refresh_host_list(target_conn)
        except Exception as ex:  # noqa: BLE001 - topology is (re)fetched on demand
            logger.debug(
                f"[AsyncAwsWrapperConnection] connect-time topology refresh "
                f"failed; topology will be (re)fetched on demand: {ex}")

        # Connect-time topology/role queries -- the eager refresh above and the
        # Aurora-aware default plugins' connect hooks (initial_connection,
        # aurora_connection_tracker, failover) -- run against target_conn. On a
        # NON-Aurora target those Aurora queries fail and leave psycopg's
        # transaction in an aborted state; even on Aurora a successful topology
        # SELECT can leave an open read transaction. Either way the application
        # must receive a clean connection, so roll back any wrapper-internal
        # transaction before handing target_conn to the caller. Without this, a
        # default-plugin connect to a plain (non-Aurora) Postgres leaves the
        # caller's first query dead with ``InFailedSqlTransaction: current
        # transaction is aborted`` (regression once defaults auto-load on
        # non-Aurora targets). Only act when the connection is NOT in autocommit
        # AND is in a transaction -- the only state where a lingering (open or
        # aborted) wrapper-internal transaction can persist; autocommit / idle
        # connections have nothing to clean. Best-effort + driver-agnostic
        # (psycopg's rollback is a coroutine; aiomysql's is sync).
        try:
            in_txn = (
                not await driver_dialect.get_autocommit(target_conn)
                and await driver_dialect.is_in_transaction(target_conn))
            if in_txn:
                rollback = getattr(target_conn, "rollback", None)
                if rollback is not None:
                    result = rollback()
                    if asyncio.iscoroutine(result):
                        await result
        except Exception:  # noqa: BLE001 - best-effort txn cleanup
            pass

        await plugin_service.set_current_connection(target_conn, host_info)
        wrapper = AsyncAwsWrapperConnection(plugin_service, plugin_manager, target_conn)
        # Register so that plugin-driven connection switches (failover / RWS
        # reader-writer swaps) rebind the wrapper's ``_target_conn``. Without
        # this the wrapper stays pinned to the original connection: after a
        # switch, cursor() / commit() still hit the old (often now-closed)
        # connection -- "the connection is closed", RWS never redirects, etc.
        # The sync wrapper sidesteps this because its ``target_connection`` is
        # a live property over ``current_connection``; the async wrapper caches
        # the conn for speed, so it must be kept in sync explicitly.
        plugin_service.set_connection_wrapper(wrapper)
        return wrapper

    def cursor(self, *args: Any, **kwargs: Any) -> AsyncAwsWrapperCursor:
        """Return a new :class:`AsyncAwsWrapperCursor`.

        Matches :meth:`psycopg.AsyncConnection.cursor` semantics: sync call
        that returns an async cursor. Query execution on the cursor is async.
        """
        target_cursor = self._target_conn.cursor(*args, **kwargs)
        return AsyncAwsWrapperCursor(self, target_cursor)

    async def close(self) -> None:
        async def _call() -> Any:
            # psycopg's AsyncConnection.close() is a coroutine; aiomysql's
            # Connection.close() is a sync call that closes the socket and
            # returns None. Probe the return value and await only when the
            # driver made close async, so one wrapper works for both.
            result = self._target_conn.close()
            if asyncio.iscoroutine(result):
                return await result
            return result

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_CLOSE, _call,
        )
        # Sync parity (wrapper.py:197-200, 323-338): close() also releases
        # per-connection plugin-service resources (background topology
        # monitors, the aborted connection itself). The service's
        # release_resources is documented idempotent + non-raising, but the
        # guard keeps close() itself exception-free even against a
        # misbehaving override. The GLOBAL cleanup path
        # (aio.cleanup.release_resources_async) is unaffected -- plugins'
        # long-lived monitors registered there still need it at program exit.
        try:
            await self._plugin_service.release_resources()
        except Exception:  # noqa: BLE001 - release is best-effort
            pass

    async def invalidate(self) -> None:
        """Async equivalent of :meth:`AwsWrapperConnection.invalidate`.

        Prefers the target's ``invalidate()`` (sync or async) when the
        target is a pool fairy, so the pool evicts the entry; falls back
        to ``close()`` for raw driver connections.
        """
        inv = getattr(self._target_conn, "invalidate", None)
        if callable(inv):
            try:
                result = inv()
                if asyncio.iscoroutine(result):
                    await result
                return
            except Exception:  # noqa: BLE001 - fall through to close
                pass
        await self.close()

    async def commit(self) -> None:
        async def _call() -> Any:
            return await self._target_conn.commit()

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_COMMIT, _call,
        )

    async def rollback(self) -> None:
        async def _call() -> Any:
            return await self._target_conn.rollback()

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_ROLLBACK, _call,
        )

    # ---- two-phase-commit (PEP 249 TPC extension) -----------------------
    #
    # Routed through the plugin manager exactly like the sync wrapper
    # (wrapper.py:240-258). psycopg's AsyncConnection exposes these as
    # coroutines; the return value is probed (like close/scroll/callproc)
    # so a driver with sync tpc_* methods also works. aiomysql has no TPC
    # support -- the AttributeError from the raw driver surfaces as-is.

    async def tpc_begin(self, xid: Any) -> None:
        async def _call() -> Any:
            result = self._target_conn.tpc_begin(xid)
            if asyncio.iscoroutine(result):
                return await result
            return result

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_TPC_BEGIN, _call, xid,
        )

    async def tpc_prepare(self) -> None:
        async def _call() -> Any:
            result = self._target_conn.tpc_prepare()
            if asyncio.iscoroutine(result):
                return await result
            return result

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_TPC_PREPARE, _call,
        )

    async def tpc_commit(self, xid: Any = None) -> None:
        async def _call() -> Any:
            result = self._target_conn.tpc_commit(xid)
            if asyncio.iscoroutine(result):
                return await result
            return result

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_TPC_COMMIT, _call, xid,
        )

    async def tpc_rollback(self, xid: Any = None) -> None:
        async def _call() -> Any:
            result = self._target_conn.tpc_rollback(xid)
            if asyncio.iscoroutine(result):
                return await result
            return result

        await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_TPC_ROLLBACK, _call, xid,
        )

    async def tpc_recover(self) -> Any:
        async def _call() -> Any:
            result = self._target_conn.tpc_recover()
            if asyncio.iscoroutine(result):
                return await result
            return result

        return await self._plugin_manager.execute(
            self, DbApiMethod.CONNECTION_TPC_RECOVER, _call,
        )

    async def __aenter__(self) -> AsyncAwsWrapperConnection:
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Any) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        """Proxy unknown attributes to the underlying connection.

        Lets SA's PG dialect and application code reach driver-specific
        state (``info``, ``pgconn``, ``adapters``, etc.) without special
        casing. The connection field is hit only when the attribute is
        NOT defined on the wrapper itself. Single-underscore driver attributes
        are forwarded too (SQLAlchemy's psycopg async adapter reaches for them);
        only dunders are kept on the wrapper (pickle/copy internals), and
        ``_target_conn`` is guarded by name so a miss before __init__ sets it
        raises cleanly instead of recursing through this method.
        """
        if name == "_target_conn" or name.startswith("__"):
            raise AttributeError(name)
        return getattr(self._target_conn, name)
