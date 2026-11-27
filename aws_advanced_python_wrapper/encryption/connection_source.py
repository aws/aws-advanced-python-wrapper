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

"""How the key and metadata layer gets a connection and runs its SQL through the driver dialect."""

from __future__ import annotations

from contextlib import closing, contextmanager
from threading import local
from typing import (TYPE_CHECKING, Any, ContextManager, Dict, Iterator, List,
                    Mapping, Optional, Protocol, Sequence, cast)

from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.decorators import \
    is_connection_abandoned
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.retry_util import RetryUtil

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin import Plugin
    from aws_advanced_python_wrapper.plugin_service import PluginService


class ConnectionSource(Protocol):
    def connection(self) -> ContextManager[Any]:
        """Return a context manager that yields a DB-API connection."""
        ...


class CallerConnectionSource:
    """Runs every operation on a connection the caller owns.

    The connection is never closed, committed, or rolled back here, so a
    multi-step administrative operation stays inside the caller's transaction.
    """

    def __init__(self, conn: Any):
        self._conn = conn

    @contextmanager
    def connection(self) -> Iterator[Any]:
        yield self._conn


class PluginConnectionSource:
    """Opens a short-lived connection through the wrapper's connect pipeline
    and closes it: one per :meth:`connection` call, or one per
    :meth:`statement` scope on the thread.

    Metadata and key reads therefore never use the application's connection,
    and cannot block on or join the application's transaction. The connection
    runs in autocommit mode so a read never leaves a session idle in a
    transaction and a ``last_used_at`` update is not lost on close.
    """

    def __init__(self, plugin_service: PluginService, plugin_to_skip: Optional[Plugin] = None):
        self._plugin_service = plugin_service
        self._plugin_to_skip = plugin_to_skip
        self._scope = local()

    @contextmanager
    def statement(self) -> Iterator[None]:
        """Share one connection among the lookups of one statement on this thread.

        The connection opens at the first lookup inside the scope and closes when
        the outermost scope ends, also when a lookup fails, so a statement uses at
        most one short-lived connection. A connection that a timed-out statement
        still runs on stays open, and that statement's worker frees it when it
        finishes. A scope that looks nothing up opens nothing.
        """
        depth = getattr(self._scope, "depth", 0)
        self._scope.depth = depth + 1
        if depth == 0:
            self._scope.conn = None
        try:
            yield
        finally:
            self._scope.depth = depth
            if depth == 0:
                conn, self._scope.conn = self._scope.conn, None
                # A statement that timed out can leave its worker on the connection; closing it then would crash the
                # process, so the worker frees it when it finishes.
                if conn is not None and not is_connection_abandoned(conn):
                    RetryUtil.close_connection(self._plugin_service, conn)

    @contextmanager
    def connection(self) -> Iterator[Any]:
        if getattr(self._scope, "depth", 0):
            if self._scope.conn is None:
                self._scope.conn = self._open()
            yield self._scope.conn
            return
        conn = self._open()
        try:
            yield conn
        finally:
            # A statement that timed out can leave its worker on the connection; closing it then would crash the
            # process, so the worker frees it when it finishes.
            if not is_connection_abandoned(conn):
                RetryUtil.close_connection(self._plugin_service, conn)

    def _open(self) -> Any:
        service = self._plugin_service
        conn = service.force_connect(
            service.current_host_info, Properties(service.props.copy()), self._plugin_to_skip)
        try:
            service.driver_dialect.set_autocommit(conn, True)
        except BaseException:  # noqa: BLE001 - close the half-opened connection, then re-raise it as is
            RetryUtil.close_connection(service, conn)
            raise
        return conn


def fetch_rows(dialect: DriverDialect, conn: Connection, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    """
    Runs a query and returns its rows as dicts keyed by the lower-cased column name.
    MySQL reports ``information_schema`` names in upper case on some versions, so callers never depend on the case
    of a result key.
    Rows can come back as tuples or, on a connection opened with a dict-like row factory (for example psycopg's
    ``row_factory=dict_row``), as mappings; both are handled.
    Each cursor call runs through the dialect's :meth:`DriverDialect.execute`, so the ``socket_timeout`` of the
    dialect's properties applies, and a timeout raises ``QueryTimeoutError``.

    :param dialect: the driver dialect whose ``execute`` runs each cursor call.
    :param conn: the connection to run the query on.
    :param sql: the query.
    :param params: the query's parameters.
    :return: the rows, each a dict keyed by the lower-cased column name.
    """
    with closing(conn.cursor()) as cursor:
        dialect.execute(DbApiMethod.CURSOR_EXECUTE.method_name, lambda: cursor.execute(sql, params), sql, conn=conn)
        if cursor.description is None:
            return []
        names = [_lower(d[0]) for d in cursor.description]
        rows = cast("List[Any]", dialect.execute(DbApiMethod.CURSOR_FETCHALL.method_name, lambda: cursor.fetchall(), conn=conn))
    return [{_lower(k): v for k, v in row.items()} if isinstance(row, Mapping) else dict(zip(names, row))
            for row in rows]


def _lower(name: Any) -> Any:
    return name.lower() if isinstance(name, str) else name


def run_statement(dialect: DriverDialect, conn: Connection, sql: str, params: Sequence[Any] = ()) -> int:
    """
    Runs a statement and returns the number of rows it affected.
    The cursor call runs through the dialect's :meth:`DriverDialect.execute`, so the ``socket_timeout`` of the
    dialect's properties applies, and a timeout raises ``QueryTimeoutError``.

    :param dialect: the driver dialect whose ``execute`` runs each cursor call.
    :param conn: the connection to run the statement on.
    :param sql: the statement.
    :param params: the statement's parameters.
    :return: the number of rows the statement affected, 0 when the driver doesn't report it.
    """
    with closing(conn.cursor()) as cursor:
        dialect.execute(DbApiMethod.CURSOR_EXECUTE.method_name, lambda: cursor.execute(sql, params), sql, conn=conn)
        return max(cursor.rowcount, 0)
