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

from typing import TYPE_CHECKING, Any, Dict, Generic, cast

from tortoise.backends.base.client import (BaseDBAsyncClient, T_conn,
                                           TransactionalDBClient,
                                           TransactionContext)
from tortoise.connection import connections
from tortoise.exceptions import TransactionManagementError

if TYPE_CHECKING:
    from asyncio import Lock

    from aws_advanced_python_wrapper.tortoise_orm.async_support.async_connection_pool import \
        AsyncPooledConnectionWrapper


class AwsBaseDBAsyncClient(BaseDBAsyncClient):
    _template: Dict[str, Any]


class AwsTransactionalDBClient(TransactionalDBClient):
    _template: Dict[str, Any]
    _parent: AwsBaseDBAsyncClient
    pass


class TortoiseAwsClientPooledConnectionWrapper(Generic[T_conn]):
    """Manages acquiring from and releasing connections to a pool."""

    __slots__ = ("client", "connection", "_pool_init_lock",)

    def __init__(
        self,
        client: BaseDBAsyncClient,
        pool_init_lock: Lock,
    ) -> None:
        self.client = client
        self.connection: AsyncPooledConnectionWrapper | None = None
        self._pool_init_lock = pool_init_lock

    async def ensure_connection(self) -> None:
        """Ensure the connection pool is initialized."""
        if not self.client._pool:
            async with self._pool_init_lock:
                if not self.client._pool:
                    await self.client.create_connection(with_db=True)

    async def __aenter__(self) -> AsyncPooledConnectionWrapper:
        """Acquire connection from pool."""
        await self.ensure_connection()
        self.connection = await self.client._pool.acquire()
        return cast('AsyncPooledConnectionWrapper', self.connection)

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close connection and release back to pool."""
        if self.connection:
            await self.connection.release()


class TortoiseAwsClientPooledTransactionContext(TransactionContext):
    """Transaction context that uses a pool to acquire connections."""

    __slots__ = ("client", "connection_name", "token", "_pool_init_lock", "connection")

    def __init__(self, client: TransactionalDBClient, pool_init_lock: Lock) -> None:
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
        self.client._connection = self.connection
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
