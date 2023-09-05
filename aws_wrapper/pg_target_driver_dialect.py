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

import psycopg

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection

from aws_wrapper.errors import UnsupportedOperationError
from aws_wrapper.target_driver_dialect import GenericTargetDriverDialect
from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes
from aws_wrapper.utils.messages import Messages


class PgTargetDriverDialect(GenericTargetDriverDialect):
    _driver_name: str = "Psycopg"
    TARGET_DRIVER_CODE: str = "psycopg"

    # https://www.psycopg.org/psycopg3/docs/api/pq.html#psycopg.pq.TransactionStatus
    PSYCOPG_ACTIVE_TRANSACTION_STATUS = 1
    PSYCOPG_IN_TRANSACTION_STATUS = 2

    _dialect_code: str = TargetDriverDialectCodes.PSYCOPG
    _network_bound_methods: Set[str] = {
        "Connection.commit",
        "Connection.autocommit",
        "Connection.autocommit_setter",
        "Connection.rollback",
        "Connection.cursor",
        "Cursor.callproc",
        "Cursor.execute",
        "Cursor.fetchone",
        "Cursor.fetchmany",
        "Cursor.fetchall",
        "Connection.transaction_isolation",
        "Connection.transaction_isolation_setter"
    }

    def is_dialect(self, conn: Callable) -> bool:
        return PgTargetDriverDialect.TARGET_DRIVER_CODE in str(signature(conn))

    def is_closed(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            return conn.closed

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "closed"))

    def abort_connection(self, conn: Connection):
        if isinstance(conn, psycopg.Connection):
            conn.cancel()
            return
        raise UnsupportedOperationError(Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "cancel"))

    def is_in_transaction(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            status: int = conn.info.transaction_status
            return status == self.PSYCOPG_ACTIVE_TRANSACTION_STATUS or status == self.PSYCOPG_IN_TRANSACTION_STATUS

        raise UnsupportedOperationError(Messages.get_formatted(
            "TargetDriverDialect.UnsupportedOperationError",
            self._driver_name,
            "transaction_status"))

    def is_read_only(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            if conn.read_only is None:
                return False

            return conn.read_only

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "read_only"))

    def set_read_only(self, conn: Connection, read_only: bool):
        if isinstance(conn, psycopg.Connection):
            conn.read_only = read_only
        else:
            raise UnsupportedOperationError(
                Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "read_only"))

    def get_autocommit(self, conn: Connection) -> bool:
        if isinstance(conn, psycopg.Connection):
            return conn.autocommit

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

    def set_autocommit(self, conn: Connection, autocommit: bool):
        if isinstance(conn, psycopg.Connection):
            conn.autocommit = autocommit

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "autocommit"))

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
