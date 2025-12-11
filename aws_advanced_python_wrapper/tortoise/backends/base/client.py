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

from typing import Any, Callable, Dict, Generic, cast

import asyncio
import mysql.connector
from tortoise.backends.base.client import (BaseDBAsyncClient, T_conn,
                                           TransactionalDBClient,
                                           TransactionContext)
from tortoise.connection import connections
from tortoise.exceptions import TransactionManagementError

from aws_advanced_python_wrapper.tortoise.async_support.async_wrapper import (
    AwsConnectionAsyncWrapper, AwsWrapperAsyncConnector)


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

class TortoiseAwsClientPooledConnectionWrapper(Generic[T_conn]):
    """Manages acquiring from and releasing connections to a pool."""

    __slots__ = ("client", "connection", "_pool_init_lock", "with_db")

    def __init__(
        self, 
        client: BaseDBAsyncClient, 
        pool_init_lock: asyncio.Lock, 
        with_db: bool = True
    ) -> None:
        self.client = client
        self.connection: T_conn | None = None
        self._pool_init_lock = pool_init_lock
        self.with_db = with_db

    async def ensure_connection(self) -> None:
        """Ensure the connection pool is initialized."""
        if not self.client._pool:
            async with self._pool_init_lock:
                if not self.client._pool:
                    await self.client.create_connection(with_db=self.with_db)

    async def __aenter__(self) -> T_conn:
        """Acquire connection from pool."""
        await self.ensure_connection()
        self.connection = await self.client._pool.acquire()
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close connection and release back to pool."""
        if self.connection:
            await self.connection.release()

class TortoiseAwsClientPooledTransactionContext(TransactionContext):
    """Transaction context that uses a pool to acquire connections."""

    __slots__ = ("client", "connection_name", "token", "_pool_init_lock", "connection")

    def __init__(self, client: TransactionalDBClient, pool_init_lock: asyncio.Lock) -> None:
        self.client = client
        self.connection_name = client.connection_name
        self._pool_init_lock = pool_init_lock
        self.connection = None

    async def ensure_connection(self) -> None:
        """Ensure the connection pool is initialized."""
        if not self.client._parent._pool:
            # a safeguard against multiple concurrent tasks trying to initialize the pool
            async with self._pool_init_lock:
                if not self.client._parent._pool:
                    await self.client._parent.create_connection(with_db=True)

    async def __aenter__(self) -> TransactionalDBClient:
        """Enter transaction context."""
        await self.ensure_connection()
        
        # Set the context variable so the current task sees a TransactionWrapper connection
        self.token = connections.set(self.connection_name, self.client)
        
        # Create connection and begin transaction
        self.connection = await self.client._parent._pool.acquire()
        self.client._connection =  self.connection
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
            if self.client._connection:
                await self.client._connection.release()
                # self.client._connection = None
            connections.reset(self.token)
