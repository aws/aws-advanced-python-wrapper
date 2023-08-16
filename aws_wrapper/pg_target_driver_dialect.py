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
from aws_wrapper.target_driver_dialect import GenericTargetDriverDialect
from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes
from aws_wrapper.utils.messages import Messages


class PgTargetDriverDialect(GenericTargetDriverDialect):
    TARGET_DRIVER = "psycopg"

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
    }

    def is_dialect(self, conn: Callable) -> bool:
        return PgTargetDriverDialect.TARGET_DRIVER in str(signature(conn))

    def is_closed(self, conn: Connection) -> bool:
        if hasattr(conn, "closed"):
            return conn.closed

        raise AwsWrapperError(
            Messages.get_formatted("TargetDriverDialect.InvalidTargetAttribute", "PsycopgDialect", "closed"))

    def abort_connection(self, conn: Connection):
        cancel_func = getattr(conn, "cancel", None)
        if cancel_func is None:
            raise AwsWrapperError(
                Messages.get_formatted("TargetDriverTargetDriverDialect.InvalidTargetAttribute", "PsycopgDialect",
                                       "cancel"))

        cancel_func()

    def is_in_transaction(self, conn: Connection) -> bool:
        # Psycopg3
        if hasattr(conn, "info") and hasattr(conn.info, "transaction_status"):
            status: int = conn.info.transaction_status
            return status == self.PSYCOPG_ACTIVE_TRANSACTION_STATUS or status == self.PSYCOPG_IN_TRANSACTION_STATUS

        raise AwsWrapperError(Messages.get_formatted(
            "TargetDriverDialect.InvalidTargetAttribute",
            "PsycopgDialect",
            "transaction_status"))
