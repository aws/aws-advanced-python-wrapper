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

from typing import TYPE_CHECKING, Any, Callable, Set

from psycopg2.extensions import ( # type: ignore
    connection as Psycopg2Connection,
    cursor as Psycopg2Cursor,
    TRANSACTION_STATUS_IDLE,
)
if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection

from inspect import signature

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)


logger = Logger(__name__)


class Psycopg2DriverDialect(DriverDialect):
    _driver_name: str = "Psycopg2"
    TARGET_DRIVER_CODE: str = "psycopg2"

    _dialect_code: str = DriverDialectCodes.PSYCOPG2
    _network_bound_methods: Set[str] = {
        "Connection.commit",
        "Connection.autocommit",
        "Connection.autocommit_setter",
        "Connection.set_session_readonly", # TODO: investigate this 
        "Connection.rollback",
        "Connection.close",
        "Connection.cursor",
        "Cursor.close",
        "Cursor.callproc",
        "Cursor.execute",
        "Cursor.fetchone",
        "Cursor.fetchmany",
        "Cursor.fetchall"
    }

    def is_dialect(self, connect_func: Callable) -> bool:
        if Psycopg2DriverDialect.TARGET_DRIVER_CODE not in str(signature(connect_func)):
            return Psycopg2DriverDialect.TARGET_DRIVER_CODE.lower() in (connect_func.__module__ + connect_func.__qualname__).lower()
        return True

    def is_closed(self, conn: Connection) -> bool:
        if isinstance(conn, Psycopg2Connection):
            return conn.closed

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "closed"))

    def abort_connection(self, conn: Connection):
        if isinstance(conn, Psycopg2Connection):
            conn.close()
            return
        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "cancel"))

    def is_in_transaction(self, conn: Connection) -> bool:
        if isinstance(conn, Psycopg2Connection):
            # conn.info was added in psycopg2 2.8
            try:
                    return conn.info.transaction_status != TRANSACTION_STATUS_IDLE
            except AttributeError:
                # For psycopg2 < 2.8, use get_transaction_status()
                return conn.get_transaction_status() != TRANSACTION_STATUS_IDLE

        raise UnsupportedOperationError(Messages.get_formatted(
            "DriverDialect.UnsupportedOperationError",
            self._driver_name,
            "transaction_status"))

    def is_read_only(self, conn: Connection) -> bool:
        if isinstance(conn, Psycopg2Connection):
            try:
                return False if conn.readonly is None else conn.readonly
            except AttributeError:
                # psycopg2 < 2.7: fallback using SQL query
                with conn.cursor() as cur:
                    cur.execute("SHOW transaction_read_only;")
                    status = cur.fetchone()
                    # status comes back as a tuple like ('on',) or ('off',)
                    return status[0].lower() == 'on'

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "read_only"))

    def set_read_only(self, conn: Connection, read_only: bool):
        if isinstance(conn, Psycopg2Connection):
            try:
                conn.readonly = read_only  # psycopg2 >= 2.7
            except AttributeError:
                # psycopg2 < 2.7: fallback using set_session
                conn.set_session(readonly=read_only)
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "read_only"))

    def get_autocommit(self, conn: Connection) -> bool:
        if isinstance(conn, Psycopg2Connection):
            return conn.autocommit

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if isinstance(conn, Psycopg2Connection):
            conn.autocommit = autocommit
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        if isinstance(from_conn, Psycopg2Connection) and isinstance(to_conn, Psycopg2Connection):
            self.set_read_only(to_conn, self.is_read_only(from_conn))
            to_conn.autocommit = from_conn.autocommit
            to_conn.isolation_level = from_conn.isolation_level

    def get_connection_from_obj(self, obj: object) -> Any:
        if isinstance(obj, Psycopg2Connection):
            return obj

        if isinstance(obj, Psycopg2Cursor):
            return obj.connection

    def prepare_connect_info(self, host_info: HostInfo, original_props: Properties) -> Properties:
        driver_props: Properties = Properties(original_props.copy())
        PropertiesUtils.remove_wrapper_props(driver_props)

        driver_props["host"] = host_info.host
        if host_info.is_port_specified():
            driver_props["port"] = str(host_info.port)

        db = WrapperProperties.DATABASE.get(original_props)
        if db is not None:
            driver_props["dbname"] = db

        connect_timeout = WrapperProperties.CONNECT_TIMEOUT_SEC.get(original_props)
        if connect_timeout is not None:
            driver_props["connect_timeout"] = connect_timeout

        keepalive = WrapperProperties.TCP_KEEPALIVE.get(original_props)
        if keepalive is not None:
            driver_props["keepalives"] = keepalive

        keepalive_time = WrapperProperties.TCP_KEEPALIVE_TIME_SEC.get(original_props)
        if keepalive_time is not None:
            driver_props["keepalives_idle"] = keepalive_time

        keepalive_interval = WrapperProperties.TCP_KEEPALIVE_INTERVAL_SEC.get(original_props)
        if keepalive_interval is not None:
            driver_props["keepalives_interval"] = keepalive_interval

        keepalive_probes = WrapperProperties.TCP_KEEPALIVE_PROBES.get(original_props)
        if keepalive_probes is not None:
            driver_props["keepalives_count"] = keepalive_probes

        return driver_props

    def supports_connect_timeout(self) -> bool:
        return True

    def supports_tcp_keepalive(self) -> bool:
        return True

    def supports_abort_connection(self) -> bool:
        return True
