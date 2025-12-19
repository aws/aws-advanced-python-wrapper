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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_advanced_python_wrapper.tortoise_orm.backends.base.client import (
    TortoiseAwsClientPooledConnectionWrapper,
    TortoiseAwsClientPooledTransactionContext)


class TestTortoiseAwsClientPooledConnectionWrapper:
    def test_init(self):
        mock_client = MagicMock()
        mock_pool_lock = MagicMock()

        wrapper = TortoiseAwsClientPooledConnectionWrapper(mock_client, mock_pool_lock)

        assert wrapper.client == mock_client
        assert wrapper._pool_init_lock == mock_pool_lock
        assert wrapper.connection is None

    @pytest.mark.asyncio
    async def test_ensure_connection(self):
        mock_client = MagicMock()
        mock_client._pool = None
        mock_client.create_connection = AsyncMock()
        mock_pool_lock = AsyncMock()

        wrapper = TortoiseAwsClientPooledConnectionWrapper(mock_client, mock_pool_lock)

        await wrapper.ensure_connection()
        mock_client.create_connection.assert_called_once_with(with_db=True)

    @pytest.mark.asyncio
    async def test_context_manager(self):
        mock_client = MagicMock()
        mock_client._pool = MagicMock()
        mock_pool_connection = MagicMock()
        mock_pool_connection.release = AsyncMock()
        mock_client._pool.acquire = AsyncMock(return_value=mock_pool_connection)
        mock_pool_lock = AsyncMock()

        wrapper = TortoiseAwsClientPooledConnectionWrapper(mock_client, mock_pool_lock)

        async with wrapper as conn:
            assert conn == mock_pool_connection
            assert wrapper.connection == mock_pool_connection

        # Verify release was called on exit
        mock_pool_connection.release.assert_called_once()


class TestTortoiseAwsClientPooledTransactionContext:
    def test_init(self):
        mock_client = MagicMock()
        mock_client.connection_name = "test_conn"
        mock_pool_lock = MagicMock()

        context = TortoiseAwsClientPooledTransactionContext(mock_client, mock_pool_lock)

        assert context.client == mock_client
        assert context.connection_name == "test_conn"
        assert context._pool_init_lock == mock_pool_lock

    @pytest.mark.asyncio
    async def test_ensure_connection(self):
        mock_client = MagicMock()
        mock_client._parent._pool = None
        mock_client._parent.create_connection = AsyncMock()
        mock_pool_lock = AsyncMock()

        context = TortoiseAwsClientPooledTransactionContext(mock_client, mock_pool_lock)

        await context.ensure_connection()
        mock_client._parent.create_connection.assert_called_once_with(with_db=True)

    @pytest.mark.asyncio
    async def test_context_manager_commit(self):
        mock_client = MagicMock()
        mock_client._parent._pool = MagicMock()
        mock_client.connection_name = "test_conn"
        mock_client._finalized = False
        mock_client.begin = AsyncMock()
        mock_client.commit = AsyncMock()
        mock_pool_connection = MagicMock()
        mock_pool_connection.release = AsyncMock()
        mock_client._parent._pool.acquire = AsyncMock(return_value=mock_pool_connection)
        mock_pool_lock = AsyncMock()

        context = TortoiseAwsClientPooledTransactionContext(mock_client, mock_pool_lock)

        with patch('tortoise.connection.connections') as mock_connections:
            mock_connections.set.return_value = "test_token"

            async with context as client:
                assert client == mock_client
                assert mock_client._connection == mock_pool_connection

        # Verify commit was called and connection was released
        mock_client.commit.assert_called_once()
        mock_pool_connection.release.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_rollback_on_exception(self):
        mock_client = MagicMock()
        mock_client._parent._pool = MagicMock()
        mock_client.connection_name = "test_conn"
        mock_client._finalized = False
        mock_client.begin = AsyncMock()
        mock_client.rollback = AsyncMock()
        mock_pool_connection = MagicMock()
        mock_pool_connection.release = AsyncMock()
        mock_client._parent._pool.acquire = AsyncMock(return_value=mock_pool_connection)
        mock_pool_lock = AsyncMock()

        context = TortoiseAwsClientPooledTransactionContext(mock_client, mock_pool_lock)

        with patch('tortoise.connection.connections') as mock_connections:
            mock_connections.set.return_value = "test_token"

            try:
                async with context:
                    raise ValueError("Test exception")
            except ValueError:
                pass

        # Verify rollback was called and connection was released
        mock_client.rollback.assert_called_once()
        mock_pool_connection.release.assert_called_once()
