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

from __future__ import annotations

import re
import socket
from typing import (TYPE_CHECKING, Any, Callable, ClassVar, FrozenSet, Mapping,
                    Optional, Sequence, Set, Tuple)

if TYPE_CHECKING:
    from re import Pattern

    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection, Cursor

from contextlib import closing
from inspect import signature

from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.encryption_driver_dialect import \
    EncryptionDriverDialect
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)

CMYSQL_ENABLED = False

from mysql.connector import MySQLConnection  # noqa: E402
from mysql.connector.cursor import MySQLCursor  # noqa: E402
from mysql.connector.cursor import MySQLCursorPrepared  # noqa: E402

try:
    from mysql.connector import CMySQLConnection  # noqa: E402
    from mysql.connector.cursor_cext import CMySQLCursor  # noqa: E402
    from mysql.connector.cursor_cext import CMySQLCursorPrepared  # noqa: E402

    CMYSQL_ENABLED = True

except ImportError:
    # Do nothing
    pass


class MySQLDriverDialect(EncryptionDriverDialect):
    _driver_name = "MySQL Connector Python"
    TARGET_DRIVER_CODE = "MySQL"
    AUTH_PLUGIN_PARAM = "auth_plugin"
    AUTH_METHOD = "mysql_clear_password"
    IS_CLOSED_TIMEOUT_SEC = 3

    _executor_name: ClassVar[str] = "MySQLDriverDialectExecutor"

    # equals_operator keeps the base's "=": MySQL has no user-defined operators.
    # fold_identifier keeps the base's names as written.
    analyzed_statements: ClassVar[FrozenSet[str]] = frozenset({"SELECT", "WITH", "INSERT", "REPLACE", "UPDATE", "DELETE", "VALUES"})
    sqlglot_dialect: ClassVar[str] = "mysql"
    qualified_insert_columns: ClassVar[bool] = True
    upsert_values_function: ClassVar[bool] = True
    identifier_quote: ClassVar[str] = "`"
    # The structural parser reads "UPDATE LOW_PRIORITY users" as a table named LOW_PRIORITY aliased users. All are
    # reserved words except QUICK, which can only follow DELETE.
    statement_modifiers: ClassVar[Mapping[str, FrozenSet[str]]] = {
        "INSERT": frozenset({"LOW_PRIORITY", "HIGH_PRIORITY", "DELAYED", "IGNORE"}),
        "REPLACE": frozenset({"LOW_PRIORITY", "DELAYED"}),
        "UPDATE": frozenset({"LOW_PRIORITY", "IGNORE"}),
        "DELETE": frozenset({"LOW_PRIORITY", "QUICK", "IGNORE"}),
    }
    charset_introducers: ClassVar[FrozenSet[str]] = frozenset({
        "armscii8", "ascii", "big5", "binary", "cp1250", "cp1251", "cp1256", "cp1257", "cp850", "cp852", "cp866",
        "cp932", "dec8", "eucjpms", "euckr", "gb18030", "gb2312", "gbk", "geostd8", "greek", "hebrew", "hp8",
        "keybcs2", "koi8r", "koi8u", "latin1", "latin2", "latin5", "latin7", "macce", "macroman", "sjis", "swe7",
        "tis620", "ucs2", "ujis", "utf16", "utf16le", "utf32", "utf8", "utf8mb3", "utf8mb4",
    })
    sql_token_pattern: ClassVar[Optional[Pattern[str]]] = re.compile(r"""
          (?P<line_comment>(?:\#|--(?=\s|\Z))[^\n]*)
        | (?P<block_comment>/\*.*?(?P<block_comment_end>\*/|\Z))
        | (?P<string>'(?:[^'\\]|\\.|'')*(?P<string_end>'?))
        | (?P<dqstring>"(?:[^"\\]|\\.|"")*(?P<dqstring_end>"?))
        | (?P<ident>`(?:[^`]|``)*(?P<ident_end>`?))
        | (?P<word>[^\W\d][\w$]*)
        | (?P<number>\d[\w.]*)
        | (?P<semicolon>;)
        | (?P<space>\s+)
        | (?P<other>[^\w\s'"`;/\-\#]+|.)
    """, re.VERBOSE | re.DOTALL)
    executable_comments: ClassVar[bool] = True

    _dialect_code: str = DriverDialectCodes.MYSQL_CONNECTOR_PYTHON
    _network_bound_methods: Set[str] = {
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_AUTOCOMMIT.method_name,
        DbApiMethod.CONNECTION_AUTOCOMMIT_SETTER.method_name,
        DbApiMethod.CONNECTION_IS_READ_ONLY.method_name,
        DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
        DbApiMethod.CONNECTION_CURSOR.method_name,
        DbApiMethod.CURSOR_CLOSE.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_NEXT.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name
    }

    @staticmethod
    def _is_mysql_connection(conn: Connection | object) -> bool:
        return isinstance(conn, MySQLConnection) or (CMYSQL_ENABLED and isinstance(conn, CMySQLConnection))

    @staticmethod
    def _is_cmysql_cursor(obj: object) -> bool:
        return CMYSQL_ENABLED and isinstance(obj, CMySQLCursor)

    def is_dialect(self, connect_func: Callable) -> bool:
        if MySQLDriverDialect.TARGET_DRIVER_CODE not in str(signature(connect_func)):
            return MySQLDriverDialect.TARGET_DRIVER_CODE.lower() in (connect_func.__module__ + connect_func.__qualname__).lower()
        return True

    def is_closed(self, conn: Connection) -> bool:
        if MySQLDriverDialect._is_mysql_connection(conn):

            # is_connected validates the connection using a ping().
            # If there are any unread results from previous executions an error will be thrown.
            if self.can_execute_query(conn):
                socket_timeout = WrapperProperties.SOCKET_TIMEOUT_SEC.get_float(self._props)
                timeout_sec = socket_timeout if socket_timeout > 0 else MySQLDriverDialect.IS_CLOSED_TIMEOUT_SEC
                # Run the liveness ping on the CALLING thread, never a worker
                # thread. mysql.connector connections are not safe for concurrent
                # use. The previous implementation offloaded conn.is_connected()
                # (a COM_PING == SSL I/O) to a thread pool and, on timeout,
                # abandoned the still-running ping on the pool thread while the
                # caller went on to use/close the same connection -- two threads
                # doing SSL read/write on one socket, a use-after-free in
                # OpenSSL/libmysqlclient that crashes the process with SIGSEGV.
                # Bound the ping with a temporary socket read timeout instead so
                # it cannot hang, and never touch the connection from a second
                # thread.
                restore_timeout = MySQLDriverDialect._set_socket_timeout(conn, timeout_sec)
                try:
                    return not conn.is_connected()  # type: ignore[attr-defined]
                finally:
                    if restore_timeout is not None:
                        restore_timeout()
            return False

        raise UnsupportedOperationError(Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "is_connected"))

    @staticmethod
    def _set_socket_timeout(conn: Connection, timeout_sec: float) -> Optional[Callable[[], None]]:
        """Temporarily set a read timeout on the connection's underlying socket so
        that a liveness ping issued on the calling thread cannot block forever.

        Returns a callable that restores the previous timeout, or None when the
        socket is not reachable (e.g. the C-extension connection, whose ping is
        instead bounded by its connect-time read_timeout)."""
        sock = getattr(getattr(conn, "_socket", None), "sock", None)
        if sock is None:
            return None
        try:
            previous = sock.gettimeout()
            sock.settimeout(timeout_sec)
        except OSError:
            return None

        def _restore() -> None:
            try:
                sock.settimeout(previous)
            except OSError:
                pass

        return _restore

    def get_autocommit(self, conn: Connection) -> bool:
        if MySQLDriverDialect._is_mysql_connection(conn):
            return conn.autocommit  # type: ignore[attr-defined]

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if MySQLDriverDialect._is_mysql_connection(conn):
            conn.autocommit = autocommit  # type: ignore[attr-defined]
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_password(self, props: Properties, pwd: str):
        WrapperProperties.PASSWORD.set(props, pwd)
        props[MySQLDriverDialect.AUTH_PLUGIN_PARAM] = MySQLDriverDialect.AUTH_METHOD

    def abort_connection(self, conn: Connection):
        # Shut the connection's underlying socket down to interrupt an in-flight
        # operation so the owning thread's blocked recv returns promptly, WITHOUT
        # freeing the connection (the owning thread closes it -- freeing it here
        # would race a cross-thread use-after-free in the driver, the env-4 SIGSEGV).
        # Only the pure-Python connector exposes the raw socket; best-effort no-op
        # for the C extension.
        if not MySQLDriverDialect._is_mysql_connection(conn):
            raise UnsupportedOperationError(
                Messages.get_formatted(
                    "DriverDialect.UnsupportedOperationError",
                    self._driver_name,
                    "abort_connection"))
        sock = getattr(getattr(conn, "_socket", None), "sock", None)
        if sock is not None and hasattr(sock, "shutdown"):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def can_execute_query(self, conn: Connection) -> bool:
        if MySQLDriverDialect._is_mysql_connection(conn):
            if conn.unread_result:  # type: ignore[attr-defined]
                return conn.can_consume_results  # type: ignore[attr-defined]
        return True

    def is_in_transaction(self, conn: Connection) -> bool:
        if MySQLDriverDialect._is_mysql_connection(conn):
            return bool(conn.in_transaction)  # type: ignore[attr-defined]

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name,
                                   "in_transaction"))

    def get_connection_from_obj(self, obj: object) -> Any:
        if MySQLDriverDialect._is_mysql_connection(obj):
            return obj

        if MySQLDriverDialect._is_cmysql_cursor(obj):
            try:
                conn = None

                if hasattr(obj, '_cnx'):
                    conn = obj._cnx
                elif hasattr(obj, '_connection'):
                    conn = obj._connection
                if conn is None:
                    return None

                if MySQLDriverDialect._is_mysql_connection(conn):
                    return conn

            except ReferenceError:
                return None

        if isinstance(obj, MySQLCursor):
            try:
                if MySQLDriverDialect._is_mysql_connection(obj._connection):
                    return obj._connection
            except ReferenceError:
                return None

        return None

    @classmethod
    def binds_parameters(cls, params: Any) -> bool:
        # Connector/Python substitutes only for a non-empty dict, list or tuple.
        return bool(params) and isinstance(params, (dict, list, tuple))

    @classmethod
    def metadata_key(cls, name: str) -> str:
        # Column names are case-insensitive in MySQL, and table names are too unless lower_case_table_names=0 on a
        # case-sensitive file system, where two tables differing only in case would share encryption metadata. This
        # agrees with the uncached lookup under the default (case-insensitive) collations; metadata tables created with
        # a _bin or _cs collation would compare case-sensitively there.
        return name.lower()

    def upsert_clause(
            self, conflict_columns: Sequence[str], update_columns: Sequence[str], extra_assignments: Sequence[str] = ()) -> str:
        # VALUES(col) rather than a row alias, which needs MySQL 8.0.19+.
        assignments = ", ".join([f"{c} = VALUES({c})" for c in update_columns] + list(extra_assignments))
        return f"ON DUPLICATE KEY UPDATE {assignments}"

    def insert_returning_id(self, conn: Connection, sql: str, params: Sequence[Any]) -> int:
        """
        Runs an ``INSERT`` and returns the generated ``id``.
        The cursor call runs through :meth:`execute`, so the ``socket_timeout`` of this dialect's properties applies,
        and a timeout raises ``QueryTimeoutError``.

        :param conn: the connection to run the statement on.
        :param sql: the ``INSERT`` statement.
        :param params: the statement's parameters.
        :return: the generated ``id``.
        """
        with closing(conn.cursor()) as cursor:
            self.execute(DbApiMethod.CURSOR_EXECUTE.method_name, lambda: cursor.execute(sql, params), sql, conn=conn)
            return int(cursor.lastrowid)  # type: ignore[attr-defined]

    def analyzable_sql(self, sql: Any, cursor: Cursor) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns the text the encryption plugin analyzes for ``sql``, and the codec of the connection's client encoding
        when ``sql`` is ``bytes``.
        ``bytes`` SQL is analyzed as text when every byte is ASCII, the connection's character set reads each ASCII
        byte as itself, and the text encodes back to the same bytes.
        A character set Python has no codec for raises ``LookupError``.

        :param sql: the SQL the application passed to the cursor.
        :param cursor: the driver cursor the SQL runs on.
        :return: the text to analyze, or None when the SQL runs unanalyzed, and the codec ``bytes`` SQL was decoded
            with, or None.
        """
        if not isinstance(sql, bytes):
            return super().analyzable_sql(sql, cursor)
        conn = self.get_connection_from_obj(cursor)
        # Not conn.charset: utf8mb4 is no Python codec.
        codec = None if conn is None else conn.python_charset
        text = self._decode_sql(sql, codec)
        # mysql-connector keeps reporting the character set it negotiated (python_charset) when SQL changes the
        # session's, and every client character set MySQL accepts, except swe7, reads ASCII bytes as ASCII, so only
        # all-ASCII bytes SQL is analyzed. swe7 has no Python codec, so a connection reporting it raises above; a
        # session switched to swe7 in SQL is a documented limit.
        if not sql.isascii():
            return None, codec
        return text, codec

    def binary_placeholder_prefix(self, cursor: Cursor) -> Optional[str]:
        """
        Returns ``_binary``, which makes MySQL read a payload sent as text as binary data rather than as characters
        (Warning 1300).
        A prepared cursor sends a ``bytes`` value as binary data, so it needs no prefix.

        :param cursor: the driver cursor the statement runs on.
        :return: ``"_binary "``, or None on a prepared cursor.
        """
        if isinstance(cursor, MySQLCursorPrepared) or (CMYSQL_ENABLED and isinstance(cursor, CMySQLCursorPrepared)):
            return None
        return "_binary "

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        if MySQLDriverDialect._is_mysql_connection(from_conn) and MySQLDriverDialect._is_mysql_connection(to_conn):
            to_conn.autocommit = from_conn.autocommit  # type: ignore[attr-defined]

    def ping(self, conn: Connection) -> bool:
        return not self.is_closed(conn)

    def prepare_connect_info(self, host_info: HostInfo, original_props: Properties) -> Properties:
        driver_props: Properties = Properties(original_props.copy())
        PropertiesUtils.remove_wrapper_props(driver_props)

        driver_props["host"] = host_info.host
        if host_info.is_port_specified():
            driver_props["port"] = str(host_info.port)

        db = WrapperProperties.DATABASE.get(original_props)
        if db is not None:
            driver_props["database"] = db

        connect_timeout = WrapperProperties.CONNECT_TIMEOUT_SEC.get(original_props)
        if connect_timeout is not None:
            driver_props["connect_timeout"] = connect_timeout

        return driver_props

    def supports_connect_timeout(self) -> bool:
        return True
