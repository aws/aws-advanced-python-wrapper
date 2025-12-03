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
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, Callable, Generic

from tortoise.backends.base.client import BaseDBAsyncClient, T_conn, TransactionalDBClient, TransactionContext
from tortoise.connection import connections
from tortoise.exceptions import TransactionManagementError

from aws_advanced_python_wrapper import AwsWrapperConnection


class AwsWrapperAsyncConnector:
    """Factory class for creating AWS wrapper connections."""
    
    _executor: ThreadPoolExecutor = ThreadPoolExecutor(
        thread_name_prefix="AwsWrapperAsyncExecutor"
    )
    
    @staticmethod
    async def ConnectWithAwsWrapper(connect_func: Callable, **kwargs) -> AwsWrapperConnection:
        """Create an AWS wrapper connection with async cursor support."""
        loop = asyncio.get_event_loop()
        connection = await loop.run_in_executor(
            AwsWrapperAsyncConnector._executor,
            lambda: AwsWrapperConnection.connect(connect_func, **kwargs)
        )
        return AwsConnectionAsyncWrapper(connection)
    
    @staticmethod
    async def CloseAwsWrapper(connection: AwsWrapperConnection) -> None:
        """Close an AWS wrapper connection asynchronously."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            AwsWrapperAsyncConnector._executor,
            connection.close
        )


class AwsCursorAsyncWrapper:
    """Wraps a sync cursor to provide async interface."""
    
    _executor: ThreadPoolExecutor = ThreadPoolExecutor(
        thread_name_prefix="AwsCursorAsyncWrapperExecutor"
    )
    
    def __init__(self, sync_cursor):
        self._cursor = sync_cursor
    
    async def execute(self, query, params=None):
        """Execute a query asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._cursor.execute, query, params)
    
    async def executemany(self, query, params_list):
        """Execute multiple queries asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._cursor.executemany, query, params_list)
    
    async def fetchall(self):
        """Fetch all results asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._cursor.fetchall)
    
    async def fetchone(self):
        """Fetch one result asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._cursor.fetchone)
    
    async def close(self):
        """Close cursor asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._cursor.close)
    
    def __getattr__(self, name):
        """Delegate non-async attributes to the wrapped cursor."""
        return getattr(self._cursor, name)


class AwsConnectionAsyncWrapper(AwsWrapperConnection):
    """AWS wrapper connection with async cursor support."""
    
    _executor: ThreadPoolExecutor = ThreadPoolExecutor(
        thread_name_prefix="AwsConnectionAsyncWrapperExecutor"
    )
    
    def __init__(self, connection: AwsWrapperConnection):
        self._wrapped_connection = connection

    @asynccontextmanager
    async def cursor(self):
        """Create an async cursor context manager."""
        loop = asyncio.get_event_loop()
        cursor_obj = await loop.run_in_executor(self._executor, self._wrapped_connection.cursor)
        try:
            yield AwsCursorAsyncWrapper(cursor_obj)
        finally:
            await loop.run_in_executor(self._executor, cursor_obj.close)

    async def rollback(self):
        """Rollback the current transaction."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._wrapped_connection.rollback)
    
    async def commit(self):
        """Commit the current transaction."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._wrapped_connection.commit)
    
    async def set_autocommit(self, value: bool):
        """Set autocommit mode."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, lambda: setattr(self._wrapped_connection, 'autocommit', value))

    def __getattr__(self, name):
        """Delegate all other attributes/methods to the wrapped connection."""
        return getattr(self._wrapped_connection, name)
    
    def __del__(self):
        """Delegate cleanup to wrapped connection."""
        if hasattr(self, '_wrapped_connection'):
            # Let the wrapped connection handle its own cleanup
            pass


class TortoiseAwsClientConnectionWrapper(Generic[T_conn]):
    """Manages acquiring from and releasing connections to a pool."""

    __slots__ = ("client", "connection", "connect_func", "with_db")

    def __init__(
        self, 
        client: BaseDBAsyncClient, 
        connect_func: Callable, 
        with_db: bool = True
    ) -> None:
        self.connect_func = connect_func
        self.client = client
        self.connection: T_conn | None = None
        self.with_db = with_db

    async def ensure_connection(self) -> None:
        """Ensure the connection pool is initialized."""
        await self.client.create_connection(with_db=self.with_db)

    async def __aenter__(self) -> T_conn:
        """Acquire connection from pool."""
        await self.ensure_connection()
        self.connection = await AwsWrapperAsyncConnector.ConnectWithAwsWrapper(self.connect_func, **self.client._template)
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close connection and release back to pool."""
        if self.connection:
            await AwsWrapperAsyncConnector.CloseAwsWrapper(self.connection)


class TortoiseAwsClientTransactionContext(TransactionContext):
    """Transaction context that uses a pool to acquire connections."""

    __slots__ = ("client", "connection_name", "token")

    def __init__(self, client: TransactionalDBClient) -> None:
        self.client = client
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
        self.client._connection = await AwsWrapperAsyncConnector.ConnectWithAwsWrapper(
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
            connections.reset(self.token)
            await AwsWrapperAsyncConnector.CloseAwsWrapper(self.client._connection)