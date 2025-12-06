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

import asyncio
from functools import wraps
from itertools import count
from typing import (Any, Callable, Coroutine, Dict, List, Optional,
                    SupportsInt, Tuple, TypeVar)

import mysql.connector
import sqlparse  # type: ignore[import-untyped]
from mysql.connector import errors
from mysql.connector.charsets import MYSQL_CHARACTER_SETS
from pypika_tortoise import MySQLQuery
from tortoise.backends.base.client import (Capabilities, ConnectionWrapper,
                                           NestedTransactionContext,
                                           TransactionContext)
from tortoise.exceptions import (DBConnectionError, IntegrityError,
                                 OperationalError, TransactionManagementError)

from aws_advanced_python_wrapper.errors import AwsWrapperError, FailoverError
from aws_advanced_python_wrapper.tortoise.backend.base.client import (
    AwsBaseDBAsyncClient, AwsConnectionAsyncWrapper, AwsTransactionalDBClient,
    TortoiseAwsClientConnectionWrapper, TortoiseAwsClientTransactionContext)
from aws_advanced_python_wrapper.tortoise.backend.mysql.executor import \
    AwsMySQLExecutor
from aws_advanced_python_wrapper.tortoise.backend.mysql.schema_generator import \
    AwsMySQLSchemaGenerator
from aws_advanced_python_wrapper.utils.log import Logger

logger = Logger(__name__)
T = TypeVar("T")
FuncType = Callable[..., Coroutine[None, None, T]]


def translate_exceptions(func: FuncType) -> FuncType:
    """Decorator to translate MySQL connector exceptions to Tortoise exceptions."""
    @wraps(func)
    async def translate_exceptions_(self, *args) -> T:
        try:
            try:
                return await func(self, *args)
            except AwsWrapperError as aws_err:  # Unwrap any AwsWrappedErrors
                if aws_err.__cause__:
                    raise aws_err.__cause__
                raise
        except FailoverError:  # Raise any failover errors
            raise
        except errors.IntegrityError as exc:
            raise IntegrityError(exc)
        except (
            errors.OperationalError,
            errors.ProgrammingError,
            errors.DataError,
            errors.InternalError,
            errors.NotSupportedError,
            errors.DatabaseError
        ) as exc:
            raise OperationalError(exc)

    return translate_exceptions_


class AwsMySQLClient(AwsBaseDBAsyncClient):
    """AWS Advanced Python Wrapper MySQL client for Tortoise ORM."""
    query_class = MySQLQuery
    executor_class = AwsMySQLExecutor
    schema_generator = AwsMySQLSchemaGenerator
    capabilities = Capabilities(
        dialect="mysql",
        requires_limit=True,
        inline_comment=True,
        support_index_hint=True,
        support_for_posix_regex_queries=True,
        support_json_attributes=True,
    )

    def __init__(
        self,
        *,
        user: str,
        password: str,
        database: str,
        host: str,
        port: SupportsInt,
        **kwargs,
    ):
        """Initialize AWS MySQL client with connection parameters."""
        super().__init__(**kwargs)

        # Basic connection parameters
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = int(port)
        self.extra = kwargs.copy()

        # Extract MySQL-specific settings
        self.storage_engine = self.extra.pop("storage_engine", "innodb")
        self.charset = self.extra.pop("charset", "utf8mb4")

        # Remove Tortoise-specific parameters
        self.extra.pop("connection_name", None)
        self.extra.pop("fetch_inserted", None)
        self.extra.pop("autocommit", None)
        self.extra.setdefault("sql_mode", "STRICT_TRANS_TABLES")

        # Initialize connection templates
        self._init_connection_templates()

        # Initialize state
        self._template: Dict[str, Any] = {}
        self._connection = None

    def _init_connection_templates(self) -> None:
        """Initialize connection templates for with/without database."""
        base_template = {
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "autocommit": True,
            **self.extra
        }

        self._template_with_db = {**base_template, "database": self.database}
        self._template_no_db = {**base_template, "database": None}

    # Connection Management
    async def create_connection(self, with_db: bool) -> None:
        """Initialize connection pool and configure database settings."""
        # Validate charset
        if self.charset.lower() not in [cs[0] for cs in MYSQL_CHARACTER_SETS if cs is not None]:
            raise DBConnectionError(f"Unknown character set: {self.charset}")

        # Set transaction support based on storage engine
        if self.storage_engine.lower() != "innodb":
            self.capabilities.__dict__["supports_transactions"] = False

        # Set template based on database requirement
        self._template = self._template_with_db if with_db else self._template_no_db

    async def close(self) -> None:
        """Close connections - AWS wrapper handles cleanup internally."""
        pass

    def acquire_connection(self):
        """Acquire a connection from the pool."""
        return self._acquire_connection(with_db=True)

    def _acquire_connection(self, with_db: bool) -> TortoiseAwsClientConnectionWrapper:
        """Create connection wrapper for specified database mode."""
        return TortoiseAwsClientConnectionWrapper(
            self, mysql.connector.Connect, with_db=with_db
        )

    # Database Operations
    async def db_create(self) -> None:
        """Create the database."""
        await self.create_connection(with_db=False)
        await self._execute_script(f"CREATE DATABASE {self.database};", False)
        await self.close()

    async def db_delete(self) -> None:
        """Delete the database."""
        await self.create_connection(with_db=False)
        await self._execute_script(f"DROP DATABASE {self.database};", False)
        await self.close()

    # Query Execution Methods
    @translate_exceptions
    async def execute_insert(self, query: str, values: List[Any]) -> int:
        """Execute an INSERT query and return the last inserted row ID."""
        async with self.acquire_connection() as connection:
            logger.debug(f"{query}: {values}")
            async with connection.cursor() as cursor:
                await cursor.execute(query, values)
                return cursor.lastrowid

    @translate_exceptions
    async def execute_many(self, query: str, values: List[List[Any]]) -> None:
        """Execute a query with multiple parameter sets."""
        async with self.acquire_connection() as connection:
            logger.debug(f"{query}: {values}")
            async with connection.cursor() as cursor:
                if self.capabilities.supports_transactions:
                    await self._execute_many_with_transaction(cursor, connection, query, values)
                else:
                    await cursor.executemany(query, values)

    async def _execute_many_with_transaction(self, cursor: Any, connection: Any, query: str, values: List[List[Any]]) -> None:
        """Execute many queries within a transaction."""
        try:
            await connection.set_autocommit(False)
            try:
                await cursor.executemany(query, values)
            except Exception:
                await connection.rollback()
                raise
            else:
                await connection.commit()
        finally:
            await connection.set_autocommit(True)

    @translate_exceptions
    async def execute_query(self, query: str, values: Optional[List[Any]] = None) -> Tuple[int, List[Dict[str, Any]]]:
        """Execute a query and return row count and results."""
        async with self.acquire_connection() as connection:
            logger.debug(f"{query}: {values}")
            async with connection.cursor() as cursor:
                await cursor.execute(query, values)
                rows = await cursor.fetchall()
                if rows:
                    fields = [desc[0] for desc in cursor.description]
                    return cursor.rowcount, [dict(zip(fields, row)) for row in rows]
                return cursor.rowcount, []

    async def execute_query_dict(self, query: str, values: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return only the results as dictionaries."""
        return (await self.execute_query(query, values))[1]

    async def execute_script(self, query: str) -> None:
        """Execute a script query."""
        await self._execute_script(query, True)

    @translate_exceptions
    async def _execute_script(self, query: str, with_db: bool) -> None:
        """Execute a multi-statement query by parsing and running statements sequentially."""
        async with self._acquire_connection(with_db) as connection:
            logger.debug(f"Executing script: {query}")
            async with connection.cursor() as cursor:
                # Parse multi-statement queries since MySQL Connector doesn't handle them well
                statements = sqlparse.split(query)
                for statement in statements:
                    statement = statement.strip()
                    if statement:
                        await cursor.execute(statement)

    # Transaction Support
    def _in_transaction(self) -> TransactionContext:
        """Create a new transaction context."""
        return TortoiseAwsClientTransactionContext(TransactionWrapper(self))


class TransactionWrapper(AwsMySQLClient, AwsTransactionalDBClient):
    """Transaction wrapper for AWS MySQL client."""

    def __init__(self, connection: AwsMySQLClient) -> None:
        self.connection_name = connection.connection_name
        self._connection: AwsConnectionAsyncWrapper = connection._connection
        self._lock = asyncio.Lock()
        self._savepoint: Optional[str] = None
        self._finalized: bool = False
        self._parent = connection

    def _in_transaction(self) -> TransactionContext:
        """Create a nested transaction context."""
        return NestedTransactionContext(TransactionWrapper(self))

    def acquire_connection(self):
        """Acquire the transaction connection."""
        return ConnectionWrapper(self._lock, self)

    # Transaction Control Methods
    @translate_exceptions
    async def begin(self) -> None:
        """Begin the transaction."""
        await self._connection.set_autocommit(False)
        self._finalized = False

    async def commit(self) -> None:
        """Commit the transaction."""
        if self._finalized:
            raise TransactionManagementError("Transaction already finalized")
        await self._connection.commit()
        await self._connection.set_autocommit(True)
        self._finalized = True

    async def rollback(self) -> None:
        """Rollback the transaction."""
        if self._finalized:
            raise TransactionManagementError("Transaction already finalized")
        await self._connection.rollback()
        await self._connection.set_autocommit(True)
        self._finalized = True

    # Savepoint Management
    @translate_exceptions
    async def savepoint(self) -> None:
        """Create a savepoint."""
        self._savepoint = _gen_savepoint_name()
        async with self._connection.cursor() as cursor:
            await cursor.execute(f"SAVEPOINT {self._savepoint}")

    async def savepoint_rollback(self):
        """Rollback to the savepoint."""
        if self._finalized:
            raise TransactionManagementError("Transaction already finalized")
        if self._savepoint is None:
            raise TransactionManagementError("No savepoint to rollback to")
        async with self._connection.cursor() as cursor:
            await cursor.execute(f"ROLLBACK TO SAVEPOINT {self._savepoint}")
        self._savepoint = None
        self._finalized = True

    async def release_savepoint(self):
        """Release the savepoint."""
        if self._finalized:
            raise TransactionManagementError("Transaction already finalized")
        if self._savepoint is None:
            raise TransactionManagementError("No savepoint to release")
        async with self._connection.cursor() as cursor:
            await cursor.execute(f"RELEASE SAVEPOINT {self._savepoint}")
        self._savepoint = None
        self._finalized = True

    @translate_exceptions
    async def execute_many(self, query: str, values: List[List[Any]]) -> None:
        """Execute many queries without autocommit handling (already in transaction)."""
        async with self.acquire_connection() as connection:
            logger.debug(f"{query}: {values}")
            async with connection.cursor() as cursor:
                await cursor.executemany(query, values)


def _gen_savepoint_name(_c: count = count()) -> str:
    """Generate a unique savepoint name."""
    return f"tortoise_savepoint_{next(_c)}"
