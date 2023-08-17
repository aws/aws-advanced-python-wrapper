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

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.generic_target_driver_dialect import \
    GenericTargetDriverDialect
from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes
from aws_wrapper.utils.messages import Messages


class MySQLTargetDriverDialect(GenericTargetDriverDialect):
    TARGET_DRIVER = "MySQL"

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
        return MySQLTargetDriverDialect.TARGET_DRIVER in str(signature(conn))

    def is_closed(self, conn: Connection) -> bool:
        is_connected_func = getattr(conn, "is_connected", None)
        if is_connected_func is None:
            raise AwsWrapperError(
                Messages.get_formatted("TargetDriverDialect.InvalidTargetAttribute", "MySQL Connector Python",
                                       "is_connected"))
        return not is_connected_func()

    def abort_connection(self, conn: Connection):
        raise NotImplementedError(
            Messages.get_formatted(
                "TargetDriverDialect.UnImplementedError",
                "MySQL Connector Python",
                "abort_connection"))

    def is_in_transaction(self, conn: Connection) -> bool:
        if hasattr(conn, "in_transaction"):
            return conn.in_transaction

        raise AwsWrapperError(
            Messages.get_formatted("TargetDriverDialect.InvalidTargetAttribute", "MySQL Connector Python",
                                   "in_transaction"))
