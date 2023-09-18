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
from typing import TYPE_CHECKING, Callable, Set

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection

from aws_wrapper.errors import UnsupportedOperationError
from aws_wrapper.generic_target_driver_dialect import \
    GenericTargetDriverDialect
from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes
from aws_wrapper.utils.messages import Messages


class MariaDBTargetDriverDialect(GenericTargetDriverDialect):
    TARGET_DRIVER_CODE = "Mariadb"
    _driver_name = "MariaDB Connector Python"

    _dialect_code: str = TargetDriverDialectCodes.MARIADB_CONNECTOR_PYTHON
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
        "Cursor.nextset",
    }

    def is_dialect(self, connect_func: Callable) -> bool:
        return MariaDBTargetDriverDialect.TARGET_DRIVER_CODE in str(signature(connect_func))

    def is_closed(self, conn: Connection) -> bool:
        if hasattr(conn, "open"):
            return not conn.open

        raise UnsupportedOperationError(
            Messages.get_formatted("TargetDriverDialect.UnsupportedOperationError", self._driver_name, "open"))

    def abort_connection(self, conn: Connection):
        raise UnsupportedOperationError(
            Messages.get_formatted(
                "TargetDriverDialect.UnsupportedOperationError",
                self._driver_name,
                "abort_connection"))

    def is_in_transaction(self, conn: Connection):
        # MariaDB Connector Python does not provide a connection attribute for the transaction status.
        # Need to use database dialect to track transaction status.

        raise UnsupportedOperationError(
            Messages.get_formatted(
                "TargetDriverDialect.UnsupportedOperationError",
                self._driver_name,
                "is_in_transaction"))
