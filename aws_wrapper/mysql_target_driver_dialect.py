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

from inspect import signature
from typing import TYPE_CHECKING, Any, Callable, Set

from mysql.connector.cursor import MySQLCursor
from mysql.connector.cursor_cext import CMySQLCursor

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection

from mysql.connector import CMySQLConnection, MySQLConnection

from aws_wrapper.errors import UnsupportedOperationError
from aws_wrapper.generic_target_driver_dialect import \
    GenericTargetDriverDialect
from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes
from aws_wrapper.utils.messages import Messages


class MySQLTargetDriverDialect(GenericTargetDriverDialect):
    _driver_name = "MySQL Connector Python"
    TARGET_DRIVER_CODE = "MySQL"

    _dialect_code: str = TargetDriverDialectCodes.MYSQL_CONNECTOR_PYTHON
    _network_bound_methods: Set[str] = {
        "Connection.commit",
        "Connection.autocommit",
        "Connection.autocommit_setter",
        "Connection.rollback",
        "Connection.cursor",
        "Cursor.close",
        "Cursor.execute",
        "Cursor.fetchone",
        "Cursor.fetchmany",
        "Cursor.fetchall",
    }

    def is_dialect(self, conn: Callable) -> bool:
        return MySQLTargetDriverDialect.TARGET_DRIVER_CODE in str(signature(conn))

    def is_closed(self, conn: Connection) -> bool:
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            return not conn.is_connected()

        raise UnsupportedOperationError(Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "is_connected"))

    def get_autocommit(self, conn: Connection) -> bool:
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            return conn.autocommit

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            conn.autocommit = autocommit

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def abort_connection(self, conn: Connection):
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "TargetDriverDialect.UnsupportedOperationError",
                self._driver_name,
                "abort_connection"))

    def is_in_transaction(self, conn: Connection) -> bool:
        if isinstance(conn, CMySQLConnection) or isinstance(conn, MySQLConnection):
            return bool(conn.in_transaction)

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name,
                                   "in_transaction"))

    def get_connection_from_obj(self, obj: object) -> Any:
        if isinstance(obj, CMySQLConnection) or isinstance(obj, MySQLConnection):
            return obj

        if isinstance(obj, CMySQLCursor) or isinstance(obj, MySQLCursor):
            return obj._cnx

        return None

    def transfer_session_state(self, from_conn: Connection, to_conn: Connection):
        if (isinstance(from_conn, CMySQLConnection) or isinstance(from_conn, MySQLConnection)) and (
                isinstance(to_conn, CMySQLConnection) or isinstance(to_conn, MySQLConnection)):
            to_conn.autocommit = from_conn.autocommit
