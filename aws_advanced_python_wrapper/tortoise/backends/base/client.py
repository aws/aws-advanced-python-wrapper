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

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Callable, Dict, Generic, cast

import mysql.connector
from tortoise.backends.base.client import (BaseDBAsyncClient, T_conn,
                                           TransactionalDBClient,
                                           TransactionContext)
from tortoise.connection import connections
from tortoise.exceptions import TransactionManagementError

from aws_advanced_python_wrapper import AwsWrapperConnection


class AwsWrapperAsyncConnector:
    """Class for creating and closing AWS wrapper connections."""

    @staticmethod
    async def connect_with_aws_wrapper(connect_func: Callable, **kwargs) -> AwsConnectionAsyncWrapper:
        """Create an AWS wrapper connection with async cursor support."""
        connection = await asyncio.to_thread(
            AwsWrapperConnection.connect, connect_func, **kwargs
        )
        return AwsConnectionAsyncWrapper(connection)

    @staticmethod
    async def close_aws_wrapper(connection: AwsWrapperConnection) -> None:
        """Close an AWS wrapper connection asynchronously."""
        await asyncio.to_thread(connection.close)


class AwsCursorAsyncWrapper:
    """Wraps sync AwsCursor cursor with async support."""

    def __init__(self, sync_cursor):
        self._cursor = sync_cursor

    async def execute(self, query, params=None):
        """Execute a query asynchronously."""
        return await asyncio.to_thread(self._cursor.execute, query, params)

    async def executemany(self, query, params_list):
        """Execute multiple queries asynchronously."""
        return await asyncio.to_thread(self._cursor.executemany, query, params_list)

    async def fetchall(self):
        """Fetch all results asynchronously."""
        return await asyncio.to_thread(self._cursor.fetchall)

    async def fetchone(self):
        """Fetch one result asynchronously."""
        return await asyncio.to_thread(self._cursor.fetchone)

    async def close(self):
        """Close cursor asynchronously."""
        return await asyncio.to_thread(self._cursor.close)

    def __getattr__(self, name):
        """Delegate non-async attributes to the wrapped cursor."""
        return getattr(self._cursor, name)


class AwsConnectionAsyncWrapper(AwsWrapperConnection):
    """Wraps sync AwsConnection with async cursor support."""

    def __init__(self, connection: AwsWrapperConnection):
        self._wrapped_connection = connection

    @asynccontextmanager
    async def cursor(self):
        """Create an async cursor context manager."""
        cursor_obj = await asyncio.to_thread(self._wrapped_connection.cursor)
        try:
            yield AwsCursorAsyncWrapper(cursor_obj)
        finally:
            await asyncio.to_thread(cursor_obj.close)

    async def rollback(self):
        """Rollback the current transaction."""
        return await asyncio.to_thread(self._wrapped_connection.rollback)

    async def commit(self):
        """Commit the current transaction."""
        return await asyncio.to_thread(self._wrapped_connection.commit)

    async def set_autocommit(self, value: bool):
        """Set autocommit mode."""
        return await asyncio.to_thread(setattr, self._wrapped_connection, 'autocommit', value)

    def __getattr__(self, name):
        """Delegate all other attributes/methods to the wrapped connection."""
        return getattr(self._wrapped_connection, name)

    def __del__(self):
        """Delegate cleanup to wrapped connection."""
        if hasattr(self, '_wrapped_connection'):
            # Let the wrapped connection handle its own cleanup
            pass


class AwsBaseDBAsyncClient(BaseDBAsyncClient):
    _template: Dict[str, Any]


class AwsTransactionalDBClient(TransactionalDBClient):
    _template: Dict[str, Any]
    _parent: AwsBaseDBAsyncClient
    pass


class TortoiseAwsClientConnectionWrapper(Generic[T_conn]):
    """Manages acquiring from and releasing connections to a pool."""

    __slots__ = ("client", "connection", "connect_func", "with_db")

    def __init__(
        self,
        client: AwsBaseDBAsyncClient,
        connect_func: Callable,
        with_db: bool = True
    ) -> None:
        self.connect_func = connect_func
        self.client = client
        self.connection: AwsConnectionAsyncWrapper | None = None
        self.with_db = with_db

    async def ensure_connection(self) -> None:
        """Ensure the connection pool is initialized."""
        await self.client.create_connection(with_db=self.with_db)

    async def __aenter__(self) -> T_conn:
        """Acquire connection from pool."""
        await self.ensure_connection()
        self.connection = await AwsWrapperAsyncConnector.connect_with_aws_wrapper(self.connect_func, **self.client._template)
        return cast("T_conn", self.connection)

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close connection and release back to pool."""
        if self.connection:
            await AwsWrapperAsyncConnector.close_aws_wrapper(self.connection)


class TortoiseAwsClientTransactionContext(TransactionContext):
    """Transaction context that uses a pool to acquire connections."""

    __slots__ = ("client", "connection_name", "token")

    def __init__(self, client: AwsTransactionalDBClient) -> None:
        self.client: AwsTransactionalDBClient = client
        self.connection_name = client.connection_name

    async def ensure_connection(self) -> None:
        """Ensure the connection pool is initialized."""
        await self.client._parent.create_connection(with_db=True)

    async def __aenter__(self) -> TransactionalDBClient:
        """Enter transaction context."""
        await self.ensure_connection()

        # Set the context variable so the current task sees a TransactionWrapper connection
        self.token = connections.set(self.connection_name, self.client)

        # Create connection and begin transaction
        self.client._connection = await AwsWrapperAsyncConnector.connect_with_aws_wrapper(
            mysql.connector.Connect,
            **self.client._parent._template
        )
        await self.client.begin()
        return self.client

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit transaction context with proper cleanup."""
        try:
            if not self.client._finalized:
                if exc_type:
                    # Can't rollback a transaction that already failed
                    if exc_type is not TransactionManagementError:
                        await self.client.rollback()
                else:
                    await self.client.commit()
        finally:
            await AwsWrapperAsyncConnector.close_aws_wrapper(self.client._connection)
            connections.reset(self.token)
