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

from aws_advanced_python_wrapper.tortoise.async_support.async_wrapper import (
    AwsConnectionAsyncWrapper, AwsCursorAsyncWrapper, AwsWrapperAsyncConnector)
from aws_advanced_python_wrapper.tortoise.backends.base.client import (
    TortoiseAwsClientConnectionWrapper, TortoiseAwsClientTransactionContext)


class TestTortoiseAwsClientConnectionWrapper:
    def test_init(self):
        mock_client = MagicMock()
        mock_connect_func = MagicMock()

        wrapper = TortoiseAwsClientConnectionWrapper(mock_client, mock_connect_func, with_db=True)

        assert wrapper.client == mock_client
        assert wrapper.connect_func == mock_connect_func
        assert wrapper.with_db
        assert wrapper.connection is None

    @pytest.mark.asyncio
    async def test_ensure_connection(self):
        mock_client = MagicMock()
        mock_client.create_connection = AsyncMock()

        wrapper = TortoiseAwsClientConnectionWrapper(mock_client, MagicMock(), with_db=True)

        await wrapper.ensure_connection()
        mock_client.create_connection.assert_called_once_with(with_db=True)

    @pytest.mark.asyncio
    async def test_context_manager(self):
        mock_client = MagicMock()
        mock_client._template = {"host": "localhost"}
        mock_client.create_connection = AsyncMock()
        mock_connect_func = MagicMock()
        mock_connection = MagicMock()

        wrapper = TortoiseAwsClientConnectionWrapper(mock_client, mock_connect_func, with_db=True)

        with patch.object(AwsWrapperAsyncConnector, 'connect_with_aws_wrapper') as mock_connect:
            mock_connect.return_value = mock_connection

            with patch.object(AwsWrapperAsyncConnector, 'close_aws_wrapper') as mock_close:
                async with wrapper as conn:
                    assert conn == mock_connection
                    assert wrapper.connection == mock_connection

                # Verify close was called on exit
                mock_close.assert_called_once_with(mock_connection)


class TestTortoiseAwsClientTransactionContext:
    def test_init(self):
        mock_client = MagicMock()
        mock_client.connection_name = "test_conn"

        context = TortoiseAwsClientTransactionContext(mock_client)

        assert context.client == mock_client
        assert context.connection_name == "test_conn"

    @pytest.mark.asyncio
    async def test_ensure_connection(self):
        mock_client = MagicMock()
        mock_client._parent.create_connection = AsyncMock()

        context = TortoiseAwsClientTransactionContext(mock_client)

        await context.ensure_connection()
        mock_client._parent.create_connection.assert_called_once_with(with_db=True)

    @pytest.mark.asyncio
    async def test_context_manager_commit(self):
        mock_client = MagicMock()
        mock_client._parent._template = {"host": "localhost"}
        mock_client._parent.create_connection = AsyncMock()
        mock_client.connection_name = "test_conn"
        mock_client._finalized = False
        mock_client.begin = AsyncMock()
        mock_client.commit = AsyncMock()
        mock_connection = MagicMock()

        context = TortoiseAwsClientTransactionContext(mock_client)

        with patch.object(AwsWrapperAsyncConnector, 'connect_with_aws_wrapper') as mock_connect:
            mock_connect.return_value = mock_connection
            with patch('tortoise.connection.connections') as mock_connections:
                mock_connections.set.return_value = "test_token"

                with patch.object(AwsWrapperAsyncConnector, 'close_aws_wrapper') as mock_close:
                    async with context as client:
                        assert client == mock_client
                        assert mock_client._connection == mock_connection

        # Verify commit was called and connection was closed
        mock_client.commit.assert_called_once()
        mock_close.assert_called_once_with(mock_connection)

    @pytest.mark.asyncio
    async def test_context_manager_rollback_on_exception(self):
        mock_client = MagicMock()
        mock_client._parent._template = {"host": "localhost"}
        mock_client._parent.create_connection = AsyncMock()
        mock_client.connection_name = "test_conn"
        mock_client._finalized = False
        mock_client.begin = AsyncMock()
        mock_client.rollback = AsyncMock()
        mock_connection = MagicMock()

        context = TortoiseAwsClientTransactionContext(mock_client)

        with patch.object(AwsWrapperAsyncConnector, 'connect_with_aws_wrapper') as mock_connect:
            mock_connect.return_value = mock_connection
            with patch('tortoise.connection.connections') as mock_connections:
                mock_connections.set.return_value = "test_token"

                with patch.object(AwsWrapperAsyncConnector, 'close_aws_wrapper') as mock_close:
                    try:
                        async with context:
                            raise ValueError("Test exception")
                    except ValueError:
                        pass

        # Verify rollback was called and connection was closed
        mock_client.rollback.assert_called_once()
        mock_close.assert_called_once_with(mock_connection)



