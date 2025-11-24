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
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from aws_advanced_python_wrapper.tortoise.backend.base.client import (
    AwsCursorAsyncWrapper,
    AwsConnectionAsyncWrapper,
    TortoiseAwsClientConnectionWrapper,
    TortoiseAwsClientTransactionContext,
    ConnectWithAwsWrapper,
)


class TestAwsCursorAsyncWrapper:
    def test_init(self):
        mock_cursor = MagicMock()
        wrapper = AwsCursorAsyncWrapper(mock_cursor)
        assert wrapper._cursor == mock_cursor

    @pytest.mark.asyncio
    async def test_execute(self):
        mock_cursor = MagicMock()
        wrapper = AwsCursorAsyncWrapper(mock_cursor)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = "result"
            result = await wrapper.execute("SELECT 1", ["param"])
            
            mock_to_thread.assert_called_once_with(mock_cursor.execute, "SELECT 1", ["param"])
            assert result == "result"

    @pytest.mark.asyncio
    async def test_executemany(self):
        mock_cursor = MagicMock()
        wrapper = AwsCursorAsyncWrapper(mock_cursor)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = "result"
            result = await wrapper.executemany("INSERT", [["param1"], ["param2"]])
            
            mock_to_thread.assert_called_once_with(mock_cursor.executemany, "INSERT", [["param1"], ["param2"]])
            assert result == "result"

    @pytest.mark.asyncio
    async def test_fetchall(self):
        mock_cursor = MagicMock()
        wrapper = AwsCursorAsyncWrapper(mock_cursor)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = [("row1",), ("row2",)]
            result = await wrapper.fetchall()
            
            mock_to_thread.assert_called_once_with(mock_cursor.fetchall)
            assert result == [("row1",), ("row2",)]

    @pytest.mark.asyncio
    async def test_fetchone(self):
        mock_cursor = MagicMock()
        wrapper = AwsCursorAsyncWrapper(mock_cursor)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = ("row1",)
            result = await wrapper.fetchone()
            
            mock_to_thread.assert_called_once_with(mock_cursor.fetchone)
            assert result == ("row1",)

    @pytest.mark.asyncio
    async def test_close(self):
        mock_cursor = MagicMock()
        wrapper = AwsCursorAsyncWrapper(mock_cursor)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            await wrapper.close()
            mock_to_thread.assert_called_once_with(mock_cursor.close)

    def test_getattr(self):
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        wrapper = AwsCursorAsyncWrapper(mock_cursor)
        
        assert wrapper.rowcount == 5


class TestAwsConnectionAsyncWrapper:
    def test_init(self):
        mock_connection = MagicMock()
        wrapper = AwsConnectionAsyncWrapper(mock_connection)
        assert wrapper._wrapped_connection == mock_connection

    @pytest.mark.asyncio
    async def test_cursor_context_manager(self):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        wrapper = AwsConnectionAsyncWrapper(mock_connection)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.side_effect = [mock_cursor, None]  # cursor creation, then close
            
            async with wrapper.cursor() as cursor:
                assert isinstance(cursor, AwsCursorAsyncWrapper)
                assert cursor._cursor == mock_cursor
            
            assert mock_to_thread.call_count == 2

    @pytest.mark.asyncio
    async def test_rollback(self):
        mock_connection = MagicMock()
        wrapper = AwsConnectionAsyncWrapper(mock_connection)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = "rollback_result"
            result = await wrapper.rollback()
            
            mock_to_thread.assert_called_once_with(mock_connection.rollback)
            assert result == "rollback_result"

    @pytest.mark.asyncio
    async def test_commit(self):
        mock_connection = MagicMock()
        wrapper = AwsConnectionAsyncWrapper(mock_connection)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = "commit_result"
            result = await wrapper.commit()
            
            mock_to_thread.assert_called_once_with(mock_connection.commit)
            assert result == "commit_result"

    @pytest.mark.asyncio
    async def test_set_autocommit(self):
        mock_connection = MagicMock()
        wrapper = AwsConnectionAsyncWrapper(mock_connection)
        
        with patch('asyncio.to_thread') as mock_to_thread:
            await wrapper.set_autocommit(True)
            
            mock_to_thread.assert_called_once()
            # Verify the lambda function sets autocommit
            lambda_func = mock_to_thread.call_args[0][0]
            lambda_func()
            assert mock_connection.autocommit == True

    def test_getattr(self):
        mock_connection = MagicMock()
        mock_connection.some_attr = "test_value"
        wrapper = AwsConnectionAsyncWrapper(mock_connection)
        
        assert wrapper.some_attr == "test_value"


class TestTortoiseAwsClientConnectionWrapper:
    def test_init(self):
        mock_client = MagicMock()
        mock_lock = asyncio.Lock()
        mock_connect_func = MagicMock()
        
        wrapper = TortoiseAwsClientConnectionWrapper(mock_client, mock_lock, mock_connect_func)
        
        assert wrapper.client == mock_client
        assert wrapper._pool_init_lock == mock_lock
        assert wrapper.connect_func == mock_connect_func
        assert wrapper.connection is None

    @pytest.mark.asyncio
    async def test_ensure_connection(self):
        mock_client = MagicMock()
        mock_client.create_connection = AsyncMock()
        mock_lock = asyncio.Lock()
        
        wrapper = TortoiseAwsClientConnectionWrapper(mock_client, mock_lock, MagicMock())
        
        await wrapper.ensure_connection()
        mock_client.create_connection.assert_called_once_with(with_db=True)

    @pytest.mark.asyncio
    async def test_context_manager(self):
        mock_client = MagicMock()
        mock_client._template = {"host": "localhost"}
        mock_client.create_connection = AsyncMock()
        mock_lock = asyncio.Lock()
        mock_connect_func = MagicMock()
        mock_connection = MagicMock()
        
        wrapper = TortoiseAwsClientConnectionWrapper(mock_client, mock_lock, mock_connect_func)
        
        with patch('aws_advanced_python_wrapper.tortoise.backend.base.client.ConnectWithAwsWrapper') as mock_connect:
            mock_connect.return_value = mock_connection
            
            with patch('asyncio.to_thread') as mock_to_thread:
                async with wrapper as conn:
                    assert conn == mock_connection
                    assert wrapper.connection == mock_connection
                
                # Verify close was called on exit
                mock_to_thread.assert_called_once_with(mock_connection.close)


class TestTortoiseAwsClientTransactionContext:
    def test_init(self):
        mock_client = MagicMock()
        mock_client.connection_name = "test_conn"
        mock_lock = asyncio.Lock()
        
        context = TortoiseAwsClientTransactionContext(mock_client, mock_lock)
        
        assert context.client == mock_client
        assert context.connection_name == "test_conn"
        assert context._pool_init_lock == mock_lock

    @pytest.mark.asyncio
    async def test_ensure_connection(self):
        mock_client = MagicMock()
        mock_client._parent.create_connection = AsyncMock()
        mock_lock = asyncio.Lock()
        
        context = TortoiseAwsClientTransactionContext(mock_client, mock_lock)
        
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
        mock_lock = asyncio.Lock()
        mock_connection = MagicMock()
        
        context = TortoiseAwsClientTransactionContext(mock_client, mock_lock)
        
        mock_to_thread_ref = None
        
        with patch('aws_advanced_python_wrapper.tortoise.backend.base.client.ConnectWithAwsWrapper') as mock_connect:
            mock_connect.return_value = mock_connection
            with patch('tortoise.connection.connections') as mock_connections:
                mock_connections.set.return_value = "test_token"
                
                with patch('asyncio.to_thread') as mock_to_thread:
                    mock_to_thread_ref = mock_to_thread
                    async with context as client:
                        assert client == mock_client
                        assert mock_client._connection == mock_connection
        
        # Verify commit was called and connection was closed
        mock_client.commit.assert_called_once()
        mock_to_thread_ref.assert_called_once_with(mock_connection.close)

    @pytest.mark.asyncio
    async def test_context_manager_rollback_on_exception(self):
        mock_client = MagicMock()
        mock_client._parent._template = {"host": "localhost"}
        mock_client._parent.create_connection = AsyncMock()
        mock_client.connection_name = "test_conn"
        mock_client._finalized = False
        mock_client.begin = AsyncMock()
        mock_client.rollback = AsyncMock()
        mock_lock = asyncio.Lock()
        mock_connection = MagicMock()
        
        context = TortoiseAwsClientTransactionContext(mock_client, mock_lock)
        
        mock_to_thread_ref = None
        
        with patch('aws_advanced_python_wrapper.tortoise.backend.base.client.ConnectWithAwsWrapper') as mock_connect:
            mock_connect.return_value = mock_connection
            with patch('tortoise.connection.connections') as mock_connections:
                mock_connections.set.return_value = "test_token"
                
                with patch('asyncio.to_thread') as mock_to_thread:
                    mock_to_thread_ref = mock_to_thread
                    try:
                        async with context as client:
                            raise ValueError("Test exception")
                    except ValueError:
                        pass
        
        # Verify rollback was called and connection was closed
        mock_client.rollback.assert_called_once()
        mock_to_thread_ref.assert_called_once_with(mock_connection.close)


class TestConnectWithAwsWrapper:
    @pytest.mark.asyncio
    async def test_connect_with_aws_wrapper(self):
        mock_connect_func = MagicMock()
        mock_connection = MagicMock()
        kwargs = {"host": "localhost", "user": "test"}
        
        with patch('aws_advanced_python_wrapper.AwsWrapperConnection.connect') as mock_aws_connect:
            mock_aws_connect.return_value = mock_connection
            with patch('asyncio.to_thread') as mock_to_thread:
                mock_to_thread.return_value = mock_connection
                
                result = await ConnectWithAwsWrapper(mock_connect_func, **kwargs)
                
                mock_to_thread.assert_called_once()
                assert isinstance(result, AwsConnectionAsyncWrapper)
                assert result._wrapped_connection == mock_connection