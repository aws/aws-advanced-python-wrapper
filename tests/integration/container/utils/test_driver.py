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

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database_engine import DatabaseEngine


class TestDriver(str, Enum):
    __test__ = False

    PG = "PG"                # psycopg 3 (sync)
    MYSQL = "MYSQL"          # mysql-connector-python (sync)
    PG_ASYNC = "PG_ASYNC"    # psycopg 3 async (psycopg.AsyncConnection)
    MYSQL_ASYNC = "MYSQL_ASYNC"  # aiomysql

    @property
    def is_async(self) -> bool:
        return self in (TestDriver.PG_ASYNC, TestDriver.MYSQL_ASYNC)

    @property
    def engine(self) -> DatabaseEngine:
        from .database_engine import DatabaseEngine
        if self in (TestDriver.PG, TestDriver.PG_ASYNC):
            return DatabaseEngine.PG
        return DatabaseEngine.MYSQL
