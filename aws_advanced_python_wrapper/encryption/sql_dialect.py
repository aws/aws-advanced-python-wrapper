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

"""Runs the plugin's own ``encryption_metadata`` / ``key_storage`` SQL.

Both supported drivers (psycopg and MySQL Connector/Python) use the
``pyformat`` paramstyle, so the SQL is written once with ``%s`` placeholders.
Only a few grammar details differ by engine, and :class:`SqlDialect` holds
them. The wrapper's driver dialects manage connection lifecycle only, which is
why this module exists.
"""

from __future__ import annotations

import string
from abc import ABC, abstractmethod
from contextlib import closing
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Sequence

from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.errors import EncryptionConfigError
from aws_advanced_python_wrapper.utils.messages import Messages

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect

Row = Dict[str, Any]


class SqlDialect(ABC):
    """Engine-specific grammar for the plugin's own statements."""

    name: str = ""

    #: Equality operator used in every comparison the plugin writes.
    equals: str = "="

    @abstractmethod
    def upsert_clause(self, conflict_columns: Sequence[str], update_columns: Sequence[str],
                      extra_assignments: Sequence[str] = ()) -> str:
        """The trailing upsert clause for an ``INSERT``: on a conflict over
        ``conflict_columns``, overwrite ``update_columns`` from the incoming
        row and apply any literal ``extra_assignments``."""
        ...

    @abstractmethod
    def insert_returning_id(self, conn: Any, sql: str, params: Sequence[Any]) -> int:
        """Run an ``INSERT`` (without ``RETURNING``) and return the generated
        ``id``."""
        ...

    @abstractmethod
    def fold_identifier(self, name: str, quoted: bool) -> str:
        """The name the server resolves an identifier written in SQL to.
        ``name`` is the identifier without its quotes (doubled quotes undone)
        and ``quoted`` says whether it was quoted."""
        ...

    @abstractmethod
    def metadata_key(self, name: str) -> str:
        """The form a table or column name is compared in when looking it up
        in ``encryption_metadata``. Names with the same key share encryption
        metadata: usually because the engine treats them as the same object,
        but on MySQL with ``lower_case_table_names=0`` table names that differ
        only in case share a key too."""
        ...

    @staticmethod
    def for_driver_dialect(driver_dialect: DriverDialect) -> SqlDialect:
        code = driver_dialect.dialect_code
        if code == DriverDialectCodes.PSYCOPG:
            return PG
        if code == DriverDialectCodes.MYSQL_CONNECTOR_PYTHON:
            return MYSQL
        raise EncryptionConfigError(Messages.get_formatted("SqlDialect.UnsupportedDriver", code))

    @staticmethod
    def for_connection(conn: Any) -> SqlDialect:
        """Infer the engine from a raw psycopg / mysql-connector connection or
        an ``AwsWrapperConnection`` wrapping one."""
        target = getattr(conn, "target_connection", conn)
        module = type(target).__module__
        if module.startswith("psycopg"):
            return PG
        if module.startswith("mysql.connector") or module.startswith("_mysql_connector"):
            return MYSQL
        raise EncryptionConfigError(Messages.get_formatted("SqlDialect.UnsupportedDriver", type(target).__qualname__))

    @staticmethod
    def for_name(name: str) -> SqlDialect:
        lowered = name.lower()
        if lowered in ("pg", "postgres", "postgresql", "psycopg"):
            return PG
        if lowered in ("mysql", "mysql-connector", "mysql-connector-python"):
            return MYSQL
        raise EncryptionConfigError(Messages.get_formatted("SqlDialect.UnsupportedDriver", name))

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"


class PgSqlDialect(SqlDialect):
    name = "postgresql"
    # An unqualified operator resolves through search_path, where a
    # user-defined "=" with a closer type match would win. Pin the built-in.
    equals = "OPERATOR(pg_catalog.=)"

    def upsert_clause(self, conflict_columns: Sequence[str], update_columns: Sequence[str],
                      extra_assignments: Sequence[str] = ()) -> str:
        assignments = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_columns] + list(extra_assignments))
        return f"ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {assignments}"

    def insert_returning_id(self, conn: Any, sql: str, params: Sequence[Any]) -> int:
        with closing(conn.cursor()) as cursor:
            cursor.execute(f"{sql} RETURNING id", params)
            row = cursor.fetchone()
        # A connection opened with a dict-like row factory returns a mapping.
        return int(next(iter(row.values())) if isinstance(row, Mapping) else row[0])

    def fold_identifier(self, name: str, quoted: bool) -> str:
        # PostgreSQL folds an unquoted identifier to lower case, ASCII letters
        # only in a multibyte (e.g. UTF-8) database; a quoted one keeps its
        # case. The result is the name in the system catalog, apart from
        # PostgreSQL's truncation of names longer than 63 bytes.
        return name if quoted else name.translate(_ASCII_LOWER)

    def metadata_key(self, name: str) -> str:
        # Catalog names are case-sensitive: "Users" and users are two tables.
        return name


class MySqlSqlDialect(SqlDialect):
    name = "mysql"
    # MySQL has no user-defined operators.
    equals = "="

    def upsert_clause(self, conflict_columns: Sequence[str], update_columns: Sequence[str],
                      extra_assignments: Sequence[str] = ()) -> str:
        # VALUES(col) rather than a row alias, which needs MySQL 8.0.19+.
        assignments = ", ".join([f"{c} = VALUES({c})" for c in update_columns] + list(extra_assignments))
        return f"ON DUPLICATE KEY UPDATE {assignments}"

    def insert_returning_id(self, conn: Any, sql: str, params: Sequence[Any]) -> int:
        with closing(conn.cursor()) as cursor:
            cursor.execute(sql, params)
            return int(cursor.lastrowid)

    def fold_identifier(self, name: str, quoted: bool) -> str:
        # MySQL keeps identifiers as written (quoting only allows reserved words
        # and special characters). Whether it folds table names depends on
        # lower_case_table_names, which metadata_key allows for by comparing
        # case-insensitively.
        return name

    def metadata_key(self, name: str) -> str:
        # Column names are case-insensitive in MySQL, and table names are too
        # unless lower_case_table_names=0 on a case-sensitive file system, where
        # two tables differing only in case would share encryption metadata.
        # This agrees with the uncached lookup under the default
        # (case-insensitive) collations; metadata tables created with a _bin or
        # _cs collation would compare case-sensitively there.
        return name.lower()


_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)

PG = PgSqlDialect()
MYSQL = MySqlSqlDialect()


def fetch_rows(conn: Any, sql: str, params: Sequence[Any] = ()) -> List[Row]:
    """Run a query and return its rows as dicts keyed by lower-cased column
    name. MySQL reports ``information_schema`` names in upper case on some
    versions, so callers never depend on result-key case.

    Rows may come back as tuples or, on a connection opened with a dict-like
    row factory (for example psycopg's ``row_factory=dict_row``), as
    mappings; both are handled.
    """
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql, params)
        if cursor.description is None:
            return []
        names = [_lower(d[0]) for d in cursor.description]
        return [{_lower(k): v for k, v in row.items()} if isinstance(row, Mapping) else dict(zip(names, row))
                for row in cursor.fetchall()]


def _lower(name: Any) -> Any:
    return name.lower() if isinstance(name, str) else name


def execute(conn: Any, sql: str, params: Sequence[Any] = ()) -> int:
    """Run a statement and return the affected-row count."""
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql, params)
        return max(cursor.rowcount, 0)
