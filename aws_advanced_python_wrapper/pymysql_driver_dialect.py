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

from concurrent.futures import Executor, ThreadPoolExecutor, TimeoutError
from inspect import signature

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.decorators import timeout
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)

from pymysql.connections import Connection as PyMySQLConnection # type: ignore
from pymysql.cursors import Cursor as PyMySQLCursor # type: ignore

class PyMySQLDriverDialect(DriverDialect):
    _driver_name = "PyMySQL"
    TARGET_DRIVER_CODE = "pymysql"
    AUTH_PLUGIN_PARAM = "auth_plugin_map"
    AUTH_METHOD = "mysql_clear_password"

    _executor: ClassVar[Executor] = ThreadPoolExecutor(thread_name_prefix="MySQLDriverDialectExecutor")

    _dialect_code: str = DriverDialectCodes.PYMYSQL
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
        return PyMySQLDriverDialect.TARGET_DRIVER_CODE in (connect_func.__module__ + connect_func.__qualname__).lower()

    def is_closed(self, conn: Connection) -> bool:
        if isinstance(conn, PyMySQLConnection):
            return not conn.open  # type: ignore

        raise UnsupportedOperationError(Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "is_closed"))

    def get_autocommit(self, conn: Connection) -> bool:
        if isinstance(conn, PyMySQLConnection):
            return conn.get_autocommit()  # type: ignore

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if isinstance(conn, PyMySQLConnection):
            conn.autocommit(autocommit)  # type: ignore
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_password(self, props: Properties, pwd: str):
        WrapperProperties.PASSWORD.set(props, pwd)
        # Haven't needed the following during quick smoke tests? TODO: Find when is it required? 
        #props[PyMySQLDriverDialect.AUTH_PLUGIN_PARAM] = {PyMySQLDriverDialect.AUTH_METHOD: None}

    def abort_connection(self, conn: Connection):
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "DriverDialect.UnsupportedOperationError",
                self._driver_name,
                "abort_connection"))

    def is_in_transaction(self, conn: Connection) -> bool:
        if isinstance(conn, PyMySQLConnection):
            with conn.cursor() as cursor:  # type: ignore
                cursor.execute("SELECT @@autocommit")
                (autocommit,) = cursor.fetchone()
                return not bool(autocommit)

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name,
                                   "in_transaction"))

    def get_connection_from_obj(self, obj: object) -> Any:
        if isinstance(obj, PyMySQLConnection):
            return obj

        if isinstance(obj, PyMySQLCursor):
            try:
                conn = obj.connection  # type: ignore
                if isinstance(conn, PyMySQLConnection):
                    return conn
            except (AttributeError, ReferenceError):
                return None

        return None

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        if isinstance(from_conn, PyMySQLConnection) and isinstance(to_conn, PyMySQLConnection):
            from_autocommit = from_conn.get_autocommit()  # type: ignore
            to_conn.autocommit(from_autocommit)  # type: ignore

    def ping(self, conn: Connection) -> bool:
        if isinstance(conn, PyMySQLConnection):
            try:
                conn.ping(reconnect=False) # Only check, not reconnect
                return True
            except Exception:
                return False

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name,
                                   "ping")) 

    def prepare_connect_info(self, host_info: HostInfo, original_props: Properties) -> Properties:
        driver_props: Properties = Properties(original_props.copy())
        PropertiesUtils.remove_wrapper_props(driver_props)

        driver_props["host"] = host_info.host
        if host_info.is_port_specified():
            driver_props["port"] = host_info.port

        db = WrapperProperties.DATABASE.get(original_props)
        if db is not None:
            driver_props["database"] = db

        connect_timeout = WrapperProperties.CONNECT_TIMEOUT_SEC.get(original_props)
        if connect_timeout is not None:
            driver_props["connect_timeout"] = connect_timeout

        return driver_props

    def supports_connect_timeout(self) -> bool:
        return True
