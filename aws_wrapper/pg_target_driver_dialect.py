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
from typing import Callable, Set

from aws_wrapper.target_driver_dialect import GenericTargetDriverDialect
from aws_wrapper.target_driver_dialect_codes import TargetDriverDialectCodes


class PgTargetDriverDialect(GenericTargetDriverDialect):
    TARGET_DRIVER = "psycopg"

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