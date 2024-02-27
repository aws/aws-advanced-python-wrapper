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

import psycopg

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection

from inspect import signature

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)


class PgDriverDialect(DriverDialect):
    _driver_name: str = "Psycopg"
    TARGET_DRIVER_CODE: str = "psycopg"

    # https://www.psycopg.org/psycopg3/docs/api/pq.html#psycopg.pq.TransactionStatus
    PSYCOPG_IDLE_TRANSACTION_STATUS = 0

    _dialect_code: str = DriverDialectCodes.PSYCOPG
    _network_bound_methods: Set[str] = {
        "Connection.commit",
        "Connection.autocommit",
        "Connection.autocommit_setter",
        "Connection.is_read_only",
        "Connection.set_read_only",
        "Connection.rollback",
        "Connection.cursor",
        "Cursor.close",
        "Cursor.callproc",
        "Cursor.execute",
        "Cursor.fetchone",
        "Cursor.fetchmany",
        "Cursor.fetchall"
    }

    def is_dialect(self, connect_func: Callable) -> bool:
        if PgDriverDialect.TARGET_DRIVER_CODE not in str(signature(connect_func)):
            return PgDriverDialect.TARGET_DRIVER_CODE.lower() in (connect_func.__module__ + connect_func.__qualname__).lower()
        return True

    def is_closed(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            return conn.closed

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "closed"))

    def abort_connection(self, conn: Connection):
        if isinstance(conn, psycopg.Connection):
            conn.close()
            return
        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "cancel"))

    def is_in_transaction(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            status: int = conn.info.transaction_status
            return status != self.PSYCOPG_IDLE_TRANSACTION_STATUS

        raise UnsupportedOperationError(Messages.get_formatted(
            "DriverDialect.UnsupportedOperationError",
            self._driver_name,
            "transaction_status"))

    def is_read_only(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            if conn.read_only is None:
                return False

            return conn.read_only

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "read_only"))

    def set_read_only(self, conn: Connection, read_only: bool):
        if isinstance(conn, psycopg.Connection):
            conn.read_only = read_only
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "read_only"))

    def get_autocommit(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            return conn.autocommit

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if isinstance(conn, psycopg.Connection):
            conn.autocommit = autocommit
            return

        raise UnsupportedOperationError(
            Messages.get_formatted("DriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        if isinstance(from_conn, psycopg.Connection) and isinstance(to_conn, psycopg.Connection):
            to_conn.read_only = from_conn.read_only
            to_conn.autocommit = from_conn.autocommit
            to_conn.isolation_level = from_conn.isolation_level

    def get_connection_from_obj(self, obj: object) -> Any:
        if isinstance(obj, psycopg.Connection):
            return obj

        if isinstance(obj, psycopg.Cursor):
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
