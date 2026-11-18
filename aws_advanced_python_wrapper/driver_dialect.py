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

from typing import (TYPE_CHECKING, Any, Callable, ClassVar, Dict, FrozenSet,
                    List, Mapping, Optional, Sequence, Set, Tuple, cast)

if TYPE_CHECKING:
    from re import Pattern

    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection, Cursor

from abc import ABC
from concurrent.futures import TimeoutError
from contextlib import closing

from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.errors import (QueryTimeoutError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.decorators import timeout
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)


class DriverDialect(ABC):
    """
    Driver dialects help the driver-agnostic AWS Python Driver interface with the driver-specific functionality of the underlying Python Driver.
    """
    _QUERY = "SELECT 1"

    _executor_name: ClassVar[str] = "DriverDialectExecutor"
    _dialect_code: str = DriverDialectCodes.GENERIC
    _network_bound_methods: Set[str] = {DbApiMethod.ALL.method_name}
    _read_only: bool = False
    _autocommit: bool = False
    _driver_name: str = "Generic"

    # The SQL engine the driver talks to, "" when the dialect doesn't know it.
    engine_name: ClassVar[str] = ""
    # The equality operator the encryption plugin writes in its own statements.
    equals_operator: ClassVar[str] = "="
    # The leading words of the statements the encryption plugin parses; it reports any other statement without parsing it.
    analyzed_statements: ClassVar[FrozenSet[str]] = frozenset()
    # The sqlglot dialect the encryption plugin parses statements with.
    sqlglot_dialect: ClassVar[str] = ""
    # Whether the engine accepts a qualified column (t.c) in an INSERT column list, which the parser rejects.
    qualified_insert_columns: ClassVar[bool] = False
    # The name upsert_clause gives the incoming row (EXCLUDED.c), or None when the clause names no row.
    upsert_row_alias: ClassVar[Optional[str]] = None
    # Whether upsert_clause reads the incoming row through the VALUES(c) function.
    upsert_values_function: ClassVar[bool] = False
    # The character that quotes an identifier.
    identifier_quote: ClassVar[str] = '"'
    # The modifiers that can follow a statement keyword and change how the statement runs, not what it writes.
    statement_modifiers: ClassVar[Mapping[str, FrozenSet[str]]] = {}
    # The character sets whose introducer (_binary) can come directly before a placeholder.
    charset_introducers: ClassVar[FrozenSet[str]] = frozenset()
    # The SQL token pattern, tried in order at each token boundary, or None when the encryption plugin can't lex SQL.
    # Its groups: line_comment, block_comment (with block_comment_end when comments don't nest), string, estring,
    # dqstring and ident (each with <name>_end, empty when the text ends first), dollar, param, word, number,
    # semicolon, space and other, which matches any character.
    sql_token_pattern: ClassVar[Optional[Pattern[str]]] = None
    # Whether block comments nest.
    nested_block_comments: ClassVar[bool] = False
    # Whether the server runs the body of a /*! ... */ comment.
    executable_comments: ClassVar[bool] = False
    # Whether the driver reads every % as a placeholder or %%, and rejects the rest, when it gets parameters.
    strict_placeholders: ClassVar[bool] = False
    _ASCII: ClassVar[bytes] = bytes(range(0x80))

    def __init__(self, props: Properties):
        self._props = props
        self._thread_pool = services_container.get_thread_pool(self._executor_name)

    @property
    def driver_name(self):
        return self._driver_name

    @property
    def dialect_code(self) -> str:
        return self._dialect_code

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._network_bound_methods

    def is_read_only(self, conn: Connection) -> bool:
        return self._read_only

    def set_read_only(self, conn: Connection, read_only: bool):
        self._read_only = read_only

    def get_autocommit(self, conn: Connection) -> bool:
        return self._autocommit

    def set_autocommit(self, conn: Connection, autocommit: bool):
        self._autocommit = autocommit

    def supports_connect_timeout(self) -> bool:
        return False

    def supports_socket_timeout(self) -> bool:
        return False

    def supports_tcp_keepalive(self) -> bool:
        return False

    # Supports aborting connection from another thread
    def supports_abort_connection(self) -> bool:
        return False

    def can_execute_query(self, conn: Connection) -> bool:
        return True

    def set_password(self, props: Properties, pwd: str):
        WrapperProperties.PASSWORD.set(props, pwd)

    def is_dialect(self, connect_func: Callable) -> bool:
        return True

    def prepare_connect_info(self, host_info: HostInfo, props: Properties) -> Properties:
        prop_copy: Properties = Properties(props.copy())

        prop_copy["host"] = host_info.host

        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)

        PropertiesUtils.remove_wrapper_props(prop_copy)
        return prop_copy

    def is_closed(self, conn: Connection) -> bool:
        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "is_closed"))

    def abort_connection(self, conn: Connection):
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "DriverDialect.UnsupportedOperationError", self._driver_name, "abort_connection"))

    def is_in_transaction(self, conn: Connection) -> bool:
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "DriverDialect.UnsupportedOperationError", self._driver_name, "is_in_transaction"))

    def execute(
            self,
            method_name: str,
            exec_func: Callable,
            *args: Any,
            exec_timeout: Optional[float] = None,
            conn: Optional[Connection] = None,
            **kwargs: Any) -> Cursor:
        # Cursor.__iter__ only creates the driver's iterator; the rows are fetched outside this call.
        if method_name == DbApiMethod.CURSOR_ITER.method_name or (
                DbApiMethod.ALL.method_name not in self.network_bound_methods and method_name not in self.network_bound_methods):
            return exec_func()

        if exec_timeout is None:
            exec_timeout = WrapperProperties.SOCKET_TIMEOUT_SEC.get_float(self._props)

        if exec_timeout > 0:
            try:
                # Pass self (the driver dialect) + conn so that, on timeout, the
                # abandoned operation's socket is shut down (via
                # driver_dialect.abort_connection) and its worker thread is awaited
                # before we propagate -- otherwise a later close/reuse of conn races
                # the still-running operation (cross-thread use-after-free in the
                # driver, env-4 SIGSEGV).
                execute_with_timeout = timeout(self._thread_pool, exec_timeout, self, conn)(exec_func)
                return execute_with_timeout()
            except TimeoutError as e:
                raise QueryTimeoutError(Messages.get_formatted("DriverDialect.ExecuteTimeout", method_name)) from e
        else:
            return exec_func()

    def get_connection_from_obj(self, obj: object) -> Any:
        if hasattr(obj, "connection"):
            return obj.connection

        return None

    def unwrap_connection(self, conn_obj: object) -> Any:
        return conn_obj

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        return

    @staticmethod
    def binds_parameters(params: Any) -> bool:
        """
        Returns whether the driver substitutes the placeholders of a statement when ``execute`` gets ``params``.

        :param params: the second argument of ``execute``.
        :return: True when the driver substitutes the placeholders.
        """
        return params is not None

    @staticmethod
    def fold_identifier(name: str, quoted: bool) -> str:
        """
        Returns the name the server resolves an identifier written in SQL to.

        :param name: the identifier without its quotes, with doubled quotes undone.
        :param quoted: whether the identifier was quoted.
        :return: the name the server resolves the identifier to.
        """
        return name

    @staticmethod
    def metadata_key(name: str) -> str:
        """
        Returns the form in which a table or column name is compared when the encryption plugin looks it up in
        ``encryption_metadata``.
        Names with the same key share encryption metadata.

        :param name: the table or column name.
        :return: the key the name is compared by.
        """
        return name

    def upsert_clause(
            self, conflict_columns: Sequence[str], update_columns: Sequence[str], extra_assignments: Sequence[str] = ()) -> str:
        """
        Returns the trailing upsert clause for an ``INSERT``.
        On a conflict over ``conflict_columns``, the clause overwrites ``update_columns`` from the incoming row and
        applies each of ``extra_assignments``.

        :param conflict_columns: the columns of the unique key the clause matches on.
        :param update_columns: the columns the clause overwrites from the incoming row.
        :param extra_assignments: literal assignments the clause also applies.
        :return: the upsert clause.
        """
        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "upsert_clause"))

    def insert_returning_id(self, conn: Connection, sql: str, params: Sequence[Any]) -> int:
        """
        Runs an ``INSERT`` written without ``RETURNING`` and returns the generated ``id``.

        :param conn: the connection to run the statement on.
        :param sql: the ``INSERT`` statement.
        :param params: the statement's parameters.
        :return: the generated ``id``.
        """
        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "insert_returning_id"))

    def fetch_rows(self, conn: Connection, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        """
        Runs a query and returns its rows as dicts keyed by the lower-cased column name.
        MySQL reports ``information_schema`` names in upper case on some versions, so callers never depend on the case
        of a result key.
        Rows can come back as tuples or, on a connection opened with a dict-like row factory (for example psycopg's
        ``row_factory=dict_row``), as mappings; both are handled.
        Each cursor call runs through :meth:`execute`, so the ``socket_timeout`` of this dialect's properties applies, and
        a timeout raises ``QueryTimeoutError``.

        :param conn: the connection to run the query on.
        :param sql: the query.
        :param params: the query's parameters.
        :return: the rows, each a dict keyed by the lower-cased column name.
        """
        with closing(conn.cursor()) as cursor:
            self.execute(DbApiMethod.CURSOR_EXECUTE.method_name, lambda: cursor.execute(sql, params), sql, conn=conn)
            if cursor.description is None:
                return []
            names = [DriverDialect._lower(d[0]) for d in cursor.description]
            rows = cast("List[Any]", self.execute(DbApiMethod.CURSOR_FETCHALL.method_name, lambda: cursor.fetchall(), conn=conn))
        return [{DriverDialect._lower(k): v for k, v in row.items()} if isinstance(row, Mapping) else dict(zip(names, row))
                for row in rows]

    @staticmethod
    def _lower(name: Any) -> Any:
        return name.lower() if isinstance(name, str) else name

    def run_statement(self, conn: Connection, sql: str, params: Sequence[Any] = ()) -> int:
        """
        Runs a statement and returns the number of rows it affected.
        The cursor call runs through :meth:`execute`, so the ``socket_timeout`` of this dialect's properties applies,
        and a timeout raises ``QueryTimeoutError``.

        :param conn: the connection to run the statement on.
        :param sql: the statement.
        :param params: the statement's parameters.
        :return: the number of rows the statement affected, 0 when the driver doesn't report it.
        """
        with closing(conn.cursor()) as cursor:
            self.execute(DbApiMethod.CURSOR_EXECUTE.method_name, lambda: cursor.execute(sql, params), sql, conn=conn)
            return max(cursor.rowcount, 0)

    def analyzable_sql(self, sql: Any, cursor: Cursor) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns the text the encryption plugin analyzes for ``sql``, and the codec of the connection's client encoding
        when ``sql`` is ``bytes``.

        :param sql: the SQL the application passed to the cursor.
        :param cursor: the driver cursor the SQL runs on.
        :return: the text to analyze, or None when the SQL runs unanalyzed, and the codec ``bytes`` SQL was decoded
            with, or None.
        """
        if isinstance(sql, str):
            return sql, None
        return None, None

    @staticmethod
    def _decode_sql(sql: bytes, codec: Optional[str]) -> Optional[str]:
        """
        Returns the text the encryption plugin analyzes in ``bytes`` SQL.

        :param sql: the SQL the application passed to the cursor.
        :param codec: the codec of the connection's client encoding, or None when the connection is unknown.
        :return: the text, or None when ``codec`` is None, reads an ASCII byte as another character, can't decode
            ``sql``, or doesn't encode the text back to ``sql``.
        """
        if codec is None:
            return None
        try:
            # A codec that reads an ASCII byte as another character (shift_jis_2004 reads 0x5C and 0x7E as a yen sign
            # and an overline), or text that doesn't encode back to the same bytes, would show the analysis another
            # statement.
            if DriverDialect._ASCII.decode(codec) != DriverDialect._ASCII.decode("ascii"):
                return None
            text = sql.decode(codec)
            return text if text.encode(codec) == sql else None
        # Their .object is the whole statement: keep nothing of it.
        except (UnicodeDecodeError, UnicodeEncodeError):
            return None

    def binary_placeholder_prefix(self, cursor: Cursor) -> Optional[str]:
        """
        Returns the text the encryption plugin puts before each placeholder whose value it encrypts, so the server
        reads the payload as binary data.

        :param cursor: the driver cursor the statement runs on.
        :return: the prefix, or None when the server already reads a bound ``bytes`` value as binary data.
        """
        return None

    def unsupported_row_factory(self, cursor: Cursor) -> Optional[Any]:
        """
        Returns the cursor's row factory when the encryption plugin can't decode the rows it builds.

        :param cursor: the driver cursor the rows come from.
        :return: the unsupported row factory, or None.
        """
        return None

    def ping(self, conn: Connection) -> bool:
        try:
            with conn.cursor() as cursor:
                query = DriverDialect._QUERY
                self.execute(DbApiMethod.CURSOR_EXECUTE.method_name, lambda: cursor.execute(query), query, exec_timeout=10, conn=conn)
                cursor.fetchone()
                return True
        except Exception:
            return False
