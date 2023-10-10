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
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection

from concurrent.futures import Executor, ThreadPoolExecutor, TimeoutError
from inspect import signature

from mysql.connector import CMySQLConnection, MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.cursor_cext import CMySQLCursor

from aws_wrapper.errors import UnsupportedOperationError
from aws_wrapper.generic_target_driver_dialect import \
    GenericTargetDriverDialect
from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import (Properties, PropertiesUtils,
                                          WrapperProperties)
from aws_wrapper.utils.timeout import timeout


class MySQLTargetDriverDialect(GenericTargetDriverDialect):
    _driver_name = "MySQL Connector Python"
    TARGET_DRIVER_CODE = "MySQL"
    AUTH_PLUGIN_PARAM = "auth_plugin"
    AUTH_METHOD = "mysql_clear_password"
    IS_CLOSED_TIMEOUT_SEC = 3

    _executor: ClassVar[Executor] = ThreadPoolExecutor()

    _dialect_code: str = TargetDriverDialectCodes.MYSQL_CONNECTOR_PYTHON
    _network_bound_methods: Set[str] = {
        "Connection.commit",
        "Connection.autocommit",
        "Connection.autocommit_setter",
        "Connection.is_read_only",
        "Connection.set_read_only",
        "Connection.rollback",
        "Connection.cursor",
        "Cursor.close",
        "Cursor.execute",
        "Cursor.fetchone",
        "Cursor.fetchmany",
        "Cursor.fetchall"
    }

    def is_dialect(self, connect_func: Callable) -> bool:
        return MySQLTargetDriverDialect.TARGET_DRIVER_CODE in str(signature(connect_func))

    def is_closed(self, conn: Connection) -> bool:
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):

            # is_connected validates the connection using a ping().
            # If there are any unread results from previous executions an error will be thrown.
            if self.can_execute_query(conn):
                is_connected_with_timeout = timeout(
                    MySQLTargetDriverDialect._executor,
                    MySQLTargetDriverDialect.IS_CLOSED_TIMEOUT_SEC,
                    self,
                    conn)(conn.is_connected)
                try:
                    return not is_connected_with_timeout()
                except TimeoutError:
                    return False
            return False

        raise UnsupportedOperationError(Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "is_connected"))

    def get_autocommit(self, conn: Connection) -> bool:
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            return conn.autocommit

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            conn.autocommit = autocommit
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_password(self, props: Properties, pwd: str):
        WrapperProperties.PASSWORD.set(props, pwd)
        props[MySQLTargetDriverDialect.AUTH_PLUGIN_PARAM] = MySQLTargetDriverDialect.AUTH_METHOD

    def abort_connection(self, conn: Connection):
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "TargetDriverDialect.UnsupportedOperationError",
                self._driver_name,
                "abort_connection"))

    def can_execute_query(self, conn: Connection) -> bool:
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            if conn.unread_result:
                return conn.can_consume_results
        return True

    def is_in_transaction(self, conn: Connection) -> bool:
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            return bool(conn.in_transaction)

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name,
                                   "in_transaction"))

    def get_connection_from_obj(self, obj: object) -> Any:
        if isinstance(obj, CMySQLConnection) or isinstance(obj, MySQLConnection):
            return obj

        if isinstance(obj, CMySQLCursor):
            try:
                if isinstance(obj._cnx, CMySQLConnection) or isinstance(obj._cnx, MySQLConnection):
                    return obj._cnx
            except ReferenceError:
                return None

        if isinstance(obj, MySQLCursor):
            try:
                if isinstance(obj._connection, CMySQLConnection) or isinstance(obj._connection, MySQLConnection):
                    return obj._connection
            except ReferenceError:
                return None

        return None

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        if (isinstance(from_conn, CMySQLConnection) or isinstance(from_conn, MySQLConnection)) and (
                isinstance(to_conn, CMySQLConnection) or isinstance(to_conn, MySQLConnection)):
            to_conn.autocommit = from_conn.autocommit

    def prepare_connect_info(self, host_info: HostInfo, original_props: Properties) -> Properties:
        driver_props: Properties = Properties(original_props.copy())
        PropertiesUtils.remove_wrapper_props(driver_props)

        driver_props["host"] = host_info.host
        if host_info.is_port_specified():
            driver_props["port"] = str(host_info.port)

        connect_timeout = WrapperProperties.CONNECT_TIMEOUT_SEC.get(original_props)
        if connect_timeout is not None:
            driver_props["connect_timeout"] = connect_timeout

        return driver_props

    def supports_connect_timeout(self) -> bool:
        return True
