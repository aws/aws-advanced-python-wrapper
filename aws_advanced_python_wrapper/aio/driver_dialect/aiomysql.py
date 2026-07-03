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

"""``AsyncAiomysqlDriverDialect`` -- concrete ``AsyncDriverDialect`` for aiomysql.

Adapts aiomysql's API surface into the wrapper's ABC:

- ``aiomysql.connect(**kwargs)`` returns a ``_ConnectionContextManager``;
  awaiting it yields a ``Connection``. The ABC's ``connect`` awaits the
  connect callable directly and returns the real connection.
- ``conn.close()`` is sync (closes the socket immediately); async-safe
  shutdown uses ``await conn.ensure_closed()``.
- ``conn.commit()`` / ``conn.rollback()`` / ``conn.autocommit(bool)`` /
  ``conn.ping(reconnect=False)`` are all coroutines.
- ``conn.cursor()`` is sync and returns an awaitable cursor.
- aiomysql's connect kwarg for database is ``db`` (not ``database``).
  The dialect renames ``database`` -> ``db`` before invoking the driver
  so users can stay on the wrapper's preferred property name.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from aws_advanced_python_wrapper.aio.driver_dialect.base import \
    AsyncDriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo

logger = Logger(__name__)


class AsyncAiomysqlDriverDialect(AsyncDriverDialect):
    """Concrete :class:`AsyncDriverDialect` backed by aiomysql."""

    _dialect_code: str = DriverDialectCodes.MYSQL_CONNECTOR_PYTHON  # reuse MySQL code
    _driver_name: str = "aiomysql"
    _network_bound_methods = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
    }

    def supports_connect_timeout(self) -> bool:
        return True

    def supports_socket_timeout(self) -> bool:
        return False  # aiomysql doesn't expose per-socket timeout

    def supports_tcp_keepalive(self) -> bool:
        return False

    def supports_abort_connection(self) -> bool:
        return False  # no async abort primitive; close is closest

    def is_dialect(self, connect_func: Callable) -> bool:
        """Match if ``connect_func`` is aiomysql's connect."""
        import aiomysql
        return connect_func is aiomysql.connect

    def prepare_connect_info(
            self, host_info: HostInfo, props: Properties) -> Properties:
        from aws_advanced_python_wrapper.utils.properties import \
            PropertiesUtils

        # Can't just call super() -- the base class's
        # remove_wrapper_props() strips `database` (a known wrapper
        # property) before we'd see it. Copy + rename + strip locally
        # in that order.
        prop_copy = Properties(props.copy())
        prop_copy["host"] = host_info.host
        if host_info.is_port_specified():
            prop_copy["port"] = host_info.port
        # aiomysql/pymysql format the port with "%d", so it MUST be an int. The
        # port can reach here as a STRING from two paths: host_info.port, or --
        # the case the conditional above misses -- a string `port` already in
        # `props` when host_info has no explicit port. Both
        # AsyncDriverConnectionProvider and AsyncPooledConnectionProvider call
        # prepare_connect_info directly and never hit connect()'s coercion, so
        # cast unconditionally whenever a port is present. A string port
        # otherwise raises "%d format: a real number is required, not str" from
        # aiomysql connection.py (every test_*_connection_async on MySQL).
        port_val = prop_copy.get("port")
        if port_val is not None and port_val != "":
            prop_copy["port"] = int(port_val)
        # aiomysql's connect expects `db=`, not `database=`. Translate
        # before the wrapper-prop stripper drops `database`. Explicit
        # `db=` beats a generic `database=`.
        if "db" not in prop_copy and "database" in prop_copy:
            prop_copy["db"] = prop_copy["database"]
        PropertiesUtils.remove_wrapper_props(prop_copy)
        # remove_wrapper_props() just stripped CONNECT_TIMEOUT_SEC
        # ("connect_timeout"), but aiomysql needs it as a connect kwarg or it
        # waits forever on an unresponsive/blackholed socket: aiomysql's
        # connect_timeout defaults to None, so there's no asyncio.wait_for
        # bound around the handshake. Re-inject it (mirrors the sync
        # MySQLDriverDialect.prepare_connect_info) so a dead-endpoint connect
        # fails fast instead of hanging (test_proxied_wrapper_connection_failed
        # _async). Coerce to float -- aiomysql passes it to asyncio.wait_for,
        # which needs a number, and the wrapper prop may arrive as a string.
        connect_timeout = WrapperProperties.CONNECT_TIMEOUT_SEC.get(props)
        if connect_timeout is not None:
            prop_copy["connect_timeout"] = float(connect_timeout)
        return prop_copy

    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        prepared = self.prepare_connect_info(host_info, props)
        # Defensive: prepare_connect_info already casts port to int (aiomysql
        # rejects string ports), but a custom props dict could still carry a
        # string port that bypasses is_port_specified().
        if "port" in prepared:
            prepared["port"] = int(prepared["port"])
        # aiomysql.connect returns a _ConnectionContextManager; awaiting it
        # yields the Connection. Route through execute_connect so the
        # connect-timeout bounding below applies on this path too.
        return await self.execute_connect(connect_func, prepared, props)

    async def execute_connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            prepared: Properties,
            props: Properties) -> Any:
        # aiomysql's own connect_timeout (passed in via prepare_connect_info)
        # only bounds the TCP connect -- connection.py _connect wraps just
        # _open_connection in asyncio.wait_for, leaving the handshake
        # (_get_server_information) and auth reads unbounded. Against an
        # accept-but-silent endpoint (e.g. a disabled proxy) those reads hang
        # forever. Bound the WHOLE connect with the wrapper's connect_timeout so
        # a dead endpoint fails fast instead of hanging
        # (test_proxied_wrapper_connection_failed_async). This runs for BOTH the
        # dialect connect path and AsyncDriverConnectionProvider, which prepares
        # props itself and would otherwise call the driver connect directly.
        connect_timeout = WrapperProperties.CONNECT_TIMEOUT_SEC.get(props)
        coro = target_func(**prepared)
        if connect_timeout is None:
            return await coro
        return await asyncio.wait_for(coro, timeout=float(connect_timeout))

    async def is_closed(self, conn: Any) -> bool:
        # aiomysql's Connection exposes a ``closed`` property (True once its
        # writer stream is gone); it has NO ``open`` attribute (that's
        # pymysql/mysql.connector). Prefer ``closed``; fall back to ``open``
        # for any pool proxy that exposes that shape instead.
        closed = getattr(conn, "closed", None)
        if closed is not None:
            return bool(closed)
        return not getattr(conn, "open", False)

    async def abort_connection(self, conn: Any) -> None:
        # aiomysql has no abort primitive. Closing the socket is the
        # closest analog -- matches sync MySQL driver dialect.
        conn.close()

    async def is_in_transaction(self, conn: Any) -> bool:
        # aiomysql tracks the REAL transaction state via MySQL's
        # SERVER_STATUS_IN_TRANS flag (refreshed from every statement's OK
        # packet), exposed as ``get_transaction_status()``. Use it. The old
        # ``not get_autocommit()`` approximation was wrong both ways: it MISSED
        # an explicit ``START TRANSACTION`` issued while autocommit was on -- so
        # RWS ``set_read_only(False)`` failed to raise mid-transaction
        # (test_set_read_only_false__read_only_transaction_async,
        # test_writer_fail_within_transaction_start_transaction_async) -- and it
        # false-positived on an idle autocommit=False connection. SQLAlchemy's
        # AsyncAdapt_aiomysql_connection nests the real connection at
        # ``._connection``, so reach through it when the adapter lacks the method.
        get_txn = getattr(conn, "get_transaction_status", None)
        if not callable(get_txn):
            real = getattr(conn, "_connection", None)
            if real is not None:
                get_txn = getattr(real, "get_transaction_status", None)
        if callable(get_txn):
            return bool(get_txn())
        ga = getattr(conn, "get_autocommit", None)
        if callable(ga):
            # Fallback heuristic (autocommit off => assume in-transaction).
            # Less accurate than SERVER_STATUS_IN_TRANS; reachable only for an
            # unexpected connection shape lacking get_transaction_status.
            return not ga()
        # Neither method present: an unexpected connection shape on the
        # connect-time rollback / RWS-switch hot path. Log rather than silently
        # returning False (which could skip a needed rollback).
        logger.debug(
            "[AsyncAiomysqlDriverDialect] is_in_transaction: connection "
            f"exposes neither get_transaction_status nor get_autocommit "
            f"({type(conn).__name__}); assuming not in transaction")
        return False

    async def get_autocommit(self, conn: Any) -> bool:
        return bool(conn.get_autocommit())

    async def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        await conn.autocommit(autocommit)

    async def is_read_only(self, conn: Any) -> bool:
        # aiomysql has no dedicated read_only flag at the Python API
        # level. MySQL's session variable `transaction_read_only`
        # would need a server round-trip; we'd have to track intent
        # ourselves. Approximation: we don't know, return False.
        return bool(getattr(conn, "_aws_read_only", False))

    async def set_read_only(self, conn: Any, read_only: bool) -> None:
        # Apply via SET TRANSACTION and stash intent for
        # transfer_session_state.
        async with conn.cursor() as cur:
            await cur.execute(
                "SET SESSION TRANSACTION READ ONLY"
                if read_only else
                "SET SESSION TRANSACTION READ WRITE"
            )
        # Stash intent on the conn so is_read_only can read it back.
        conn._aws_read_only = bool(read_only)  # type: ignore[attr-defined]

    async def can_execute_query(self, conn: Any) -> bool:
        # aiomysql exposes ``closed`` (no ``open`` -- see is_closed).
        return not await self.is_closed(conn)

    async def transfer_session_state(
            self, from_conn: Any, to_conn: Any) -> None:
        await self.set_autocommit(to_conn, await self.get_autocommit(from_conn))
        try:
            await self.set_read_only(to_conn, await self.is_read_only(from_conn))
        except Exception:
            # Session-state transfer must not block failover.
            pass

    async def ping(self, conn: Any) -> bool:
        if await self.is_closed(conn):
            return False
        try:
            await conn.ping(reconnect=False)
            return True
        except Exception:
            return False
