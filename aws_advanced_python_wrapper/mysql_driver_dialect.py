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

from typing import TYPE_CHECKING, Any, Callable, ClassVar, Set

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection

from concurrent.futures import TimeoutError
from inspect import signature

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.thread_pool_container import \
    ThreadPoolContainer
from aws_advanced_python_wrapper.utils.decorators import timeout
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)

CMYSQL_ENABLED = False

from mysql.connector import MySQLConnection  # noqa: E402
from mysql.connector.cursor import MySQLCursor  # noqa: E402

try:
    from mysql.connector import CMySQLConnection  # noqa: E402
    from mysql.connector.cursor_cext import CMySQLCursor  # noqa: E402

    CMYSQL_ENABLED = True

except ImportError:
    # Do nothing
    pass


class MySQLDriverDialect(DriverDialect):
    _driver_name = "MySQL Connector Python"
    TARGET_DRIVER_CODE = "MySQL"
    AUTH_PLUGIN_PARAM = "auth_plugin"
    AUTH_METHOD = "mysql_clear_password"
    IS_CLOSED_TIMEOUT_SEC = 3

    _executor_name: ClassVar[str] = "MySQLDriverDialectExecutor"

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
                is_connected_with_timeout = timeout(
                    ThreadPoolContainer.get_thread_pool(MySQLDriverDialect._executor_name), timeout_sec)(conn.is_connected)  # type: ignore

                try:
                    return not is_connected_with_timeout()
                except TimeoutError:
                    return False
            return False

        raise UnsupportedOperationError(Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "is_connected"))

    def get_autocommit(self, conn: Connection) -> bool:
        if MySQLDriverDialect._is_mysql_connection(conn):
            return conn.autocommit  # type: ignore

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if MySQLDriverDialect._is_mysql_connection(conn):
            conn.autocommit = autocommit  # type: ignore
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_password(self, props: Properties, pwd: str):
        WrapperProperties.PASSWORD.set(props, pwd)
        props[MySQLDriverDialect.AUTH_PLUGIN_PARAM] = MySQLDriverDialect.AUTH_METHOD

    def abort_connection(self, conn: Connection):
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "DriverDialect.UnsupportedOperationError",
                self._driver_name,
                "abort_connection"))

    def can_execute_query(self, conn: Connection) -> bool:
        if MySQLDriverDialect._is_mysql_connection(conn):
            if conn.unread_result:  # type: ignore
                return conn.can_consume_results  # type: ignore
        return True

    def is_in_transaction(self, conn: Connection) -> bool:
        if MySQLDriverDialect._is_mysql_connection(conn):
            return bool(conn.in_transaction)  # type: ignore

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

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        if MySQLDriverDialect._is_mysql_connection(from_conn) and MySQLDriverDialect._is_mysql_connection(to_conn):
            to_conn.autocommit = from_conn.autocommit  # type: ignore

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
