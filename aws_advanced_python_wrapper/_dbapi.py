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

"""Canonical PEP 249 module surface shared by the top-level wrapper module
and the per-driver DBAPI submodules (`aws_advanced_python_wrapper.psycopg`,
`aws_advanced_python_wrapper.mysql_connector`).

Consumers should NOT import from this module directly. The public DBAPI
module surface lives on the top-level module and the per-driver submodules,
populated via `install()`.
"""

from __future__ import annotations

from datetime import date as Date  # noqa: N812
from datetime import datetime as Timestamp  # noqa: N812
from datetime import time as Time  # noqa: N812
from time import localtime
from typing import Callable, Optional

from aws_advanced_python_wrapper.pep249 import DatabaseError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import DataError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import Error  # noqa: F401
from aws_advanced_python_wrapper.pep249 import IntegrityError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import InterfaceError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import InternalError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import NotSupportedError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import OperationalError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import ProgrammingError  # noqa: F401
from aws_advanced_python_wrapper.pep249 import Warning  # noqa: F401

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


def Binary(data: bytes) -> bytes:  # noqa: N802
    return bytes(data)


def DateFromTicks(ticks: float) -> Date:  # noqa: N802
    return Date(*localtime(ticks)[:3])


def TimeFromTicks(ticks: float) -> Time:  # noqa: N802
    return Time(*localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float) -> Timestamp:  # noqa: N802
    return Timestamp(*localtime(ticks)[:6])


class _DBAPISet(frozenset):
    """Type-object singleton per PEP 249: compares equal to any contained type code."""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (int, str)):
            return other in self
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return super().__hash__()


# Type-code sources:
#   PG: psycopg.postgres.types (OIDs)
#   MySQL: mysql.connector.FieldType (ints)
# Union both into each singleton.

# PG text-like OIDs: text(25), varchar(1043), bpchar(1042), char(18),
#                    name(19), json(114), jsonb(3802)
# MySQL FieldType string-like: VAR_STRING(253), STRING(254), VARCHAR(15)
STRING = _DBAPISet([25, 1043, 1042, 18, 19, 114, 3802, 253, 254, 15])

# PG binary: bytea(17)
# MySQL FieldType BLOB family: TINY_BLOB(249), MEDIUM_BLOB(250),
#                              LONG_BLOB(251), BLOB(252)
BINARY = _DBAPISet([17, 249, 250, 251, 252])

# PG numeric: int2(21), int4(23), int8(20), float4(700), float8(701),
#             numeric(1700), money(790)
# MySQL FieldType numeric: DECIMAL(0), TINY(1), SHORT(2), LONG(3),
#                          FLOAT(4), DOUBLE(5), LONGLONG(8), INT24(9),
#                          NEWDECIMAL(246)
NUMBER = _DBAPISet([21, 23, 20, 700, 701, 1700, 790, 0, 1, 2, 3, 4, 5, 8, 9, 246])

# PG datetime: date(1082), time(1083), timestamp(1114), timestamptz(1184),
#              timetz(1266), interval(1186)
# MySQL FieldType datetime: DATE(10), TIME(11), DATETIME(12), YEAR(13),
#                           NEWDATE(14), TIMESTAMP(7)
DATETIME = _DBAPISet([1082, 1083, 1114, 1184, 1266, 1186, 10, 11, 12, 13, 14, 7])

# PG rowid: oid(26). MySQL has no ROWID equivalent; left PG-only.
ROWID = _DBAPISet([26])


_PEP249_NAMES = (
    "Warning", "Error", "InterfaceError", "DatabaseError",
    "DataError", "OperationalError", "IntegrityError",
    "InternalError", "ProgrammingError", "NotSupportedError",
    "Date", "Time", "Timestamp",
    "DateFromTicks", "TimeFromTicks", "TimestampFromTicks",
    "Binary", "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
    "apilevel", "threadsafety", "paramstyle",
)


def install(module_ns: dict, connect: Optional[Callable] = None) -> None:
    """Populate `module_ns` with the PEP 249 module surface.

    If `connect` is provided, `module_ns['connect']` is set to it and 'connect'
    is added to `module_ns['__all__']`.
    """
    source = globals()
    for name in _PEP249_NAMES:
        module_ns[name] = source[name]
    if connect is not None:
        module_ns["connect"] = connect
        module_ns["__all__"] = (*_PEP249_NAMES, "connect")
    else:
        module_ns["__all__"] = tuple(_PEP249_NAMES)
