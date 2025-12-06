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
from mysql.connector import errors
from tortoise.exceptions import (DBConnectionError, IntegrityError,
                                 OperationalError, TransactionManagementError)

from aws_advanced_python_wrapper.tortoise.backend.mysql.client import (
    AwsMySQLClient, TransactionWrapper, _gen_savepoint_name,
    translate_exceptions)


class TestTranslateExceptions:
    @pytest.mark.asyncio
    async def test_translate_operational_error(self):
        @translate_exceptions
        async def test_func(self):
            raise errors.OperationalError("Test error")

        with pytest.raises(OperationalError):
            await test_func(None)

    @pytest.mark.asyncio
    async def test_translate_integrity_error(self):
        @translate_exceptions
        async def test_func(self):
            raise errors.IntegrityError("Test error")

        with pytest.raises(IntegrityError):
            await test_func(None)

    @pytest.mark.asyncio
    async def test_no_exception(self):
        @translate_exceptions
        async def test_func(self):
            return "success"

        result = await test_func(None)
        assert result == "success"


class TestAwsMySQLClient:
    def test_init(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                charset="utf8mb4",
                storage_engine="InnoDB",
                connection_name="test_conn"
            )

        assert client.user == "test_user"
        assert client.password == "test_pass"
        assert client.database == "test_db"
        assert client.host == "localhost"
        assert client.port == 3306
        assert client.charset == "utf8mb4"
        assert client.storage_engine == "InnoDB"

    def test_init_defaults(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        assert client.charset == "utf8mb4"
        assert client.storage_engine == "innodb"

    @pytest.mark.asyncio
    async def test_create_connection_invalid_charset(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                charset="invalid_charset",
                connection_name="test_conn"
            )

        with pytest.raises(DBConnectionError, match="Unknown character set"):
            await client.create_connection(with_db=True)

    @pytest.mark.asyncio
    async def test_create_connection_success(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        with patch('aws_advanced_python_wrapper.tortoise.backend.mysql.client.logger'):
            await client.create_connection(with_db=True)

        assert client._template["user"] == "test_user"
        assert client._template["database"] == "test_db"
        assert client._template["autocommit"] is True

    @pytest.mark.asyncio
    async def test_close(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        # close() method now does nothing (AWS wrapper handles cleanup)
        await client.close()
        # No assertions needed since method is a no-op

    def test_acquire_connection(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        connection_wrapper = client.acquire_connection()
        assert connection_wrapper.client == client

    @pytest.mark.asyncio
    async def test_execute_insert(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.lastrowid = 123

        with patch.object(client, 'acquire_connection') as mock_acquire:
            mock_acquire.return_value.__aenter__ = AsyncMock(return_value=mock_connection)
            mock_acquire.return_value.__aexit__ = AsyncMock()
            mock_connection.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
            mock_connection.cursor.return_value.__aexit__ = AsyncMock()
            mock_cursor.execute = AsyncMock()

            with patch('aws_advanced_python_wrapper.tortoise.backend.mysql.client.logger'):
                result = await client.execute_insert("INSERT INTO test VALUES (?)", ["value"])

            assert result == 123
            mock_cursor.execute.assert_called_once_with("INSERT INTO test VALUES (?)", ["value"])

    @pytest.mark.asyncio
    async def test_execute_many_with_transactions(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn",
                storage_engine="innodb"
            )

        mock_connection = MagicMock()
        mock_cursor = MagicMock()

        with patch.object(client, 'acquire_connection') as mock_acquire:
            mock_acquire.return_value.__aenter__ = AsyncMock(return_value=mock_connection)
            mock_acquire.return_value.__aexit__ = AsyncMock()
            mock_connection.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
            mock_connection.cursor.return_value.__aexit__ = AsyncMock()

            with patch.object(client, '_execute_many_with_transaction') as mock_execute_many_tx:
                mock_execute_many_tx.return_value = None

                with patch('aws_advanced_python_wrapper.tortoise.backend.mysql.client.logger'):
                    await client.execute_many("INSERT INTO test VALUES (?)", [["val1"], ["val2"]])

                mock_execute_many_tx.assert_called_once_with(
                    mock_cursor, mock_connection, "INSERT INTO test VALUES (?)", [["val1"], ["val2"]]
                )

    @pytest.mark.asyncio
    async def test_execute_query(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2
        mock_cursor.description = [("id",), ("name",)]

        with patch.object(client, 'acquire_connection') as mock_acquire:
            mock_acquire.return_value.__aenter__ = AsyncMock(return_value=mock_connection)
            mock_acquire.return_value.__aexit__ = AsyncMock()
            mock_connection.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
            mock_connection.cursor.return_value.__aexit__ = AsyncMock()
            mock_cursor.execute = AsyncMock()
            mock_cursor.fetchall = AsyncMock(return_value=[(1, "test"), (2, "test2")])

            with patch('aws_advanced_python_wrapper.tortoise.backend.mysql.client.logger'):
                rowcount, results = await client.execute_query("SELECT * FROM test")

            assert rowcount == 2
            assert len(results) == 2
            assert results[0] == {"id": 1, "name": "test"}

    @pytest.mark.asyncio
    async def test_execute_query_dict(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        with patch.object(client, 'execute_query') as mock_execute_query:
            mock_execute_query.return_value = (2, [{"id": 1}, {"id": 2}])

            results = await client.execute_query_dict("SELECT * FROM test")

            assert results == [{"id": 1}, {"id": 2}]

    def test_in_transaction(self):
        with patch('builtins.print'):  # Mock the print statement
            client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        transaction_context = client._in_transaction()
        assert transaction_context is not None


class TestTransactionWrapper:
    def test_init(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)

        assert wrapper.connection_name == "test_conn"
        assert wrapper._parent == parent_client
        assert wrapper._finalized is False
        assert wrapper._savepoint is None

    @pytest.mark.asyncio
    async def test_begin(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)
        mock_connection = MagicMock()
        mock_connection.set_autocommit = AsyncMock()
        wrapper._connection = mock_connection

        await wrapper.begin()

        mock_connection.set_autocommit.assert_called_once_with(False)
        assert wrapper._finalized is False

    @pytest.mark.asyncio
    async def test_commit(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)
        mock_connection = MagicMock()
        mock_connection.commit = AsyncMock()
        mock_connection.set_autocommit = AsyncMock()
        wrapper._connection = mock_connection

        await wrapper.commit()

        mock_connection.commit.assert_called_once()
        mock_connection.set_autocommit.assert_called_once_with(True)
        assert wrapper._finalized is True

    @pytest.mark.asyncio
    async def test_commit_already_finalized(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)
        wrapper._finalized = True

        with pytest.raises(TransactionManagementError, match="Transaction already finalized"):
            await wrapper.commit()

    @pytest.mark.asyncio
    async def test_rollback(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)
        mock_connection = MagicMock()
        mock_connection.rollback = AsyncMock()
        mock_connection.set_autocommit = AsyncMock()
        wrapper._connection = mock_connection

        await wrapper.rollback()

        mock_connection.rollback.assert_called_once()
        mock_connection.set_autocommit.assert_called_once_with(True)
        assert wrapper._finalized is True

    @pytest.mark.asyncio
    async def test_savepoint(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()

        wrapper._connection = mock_connection
        mock_connection.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__aexit__ = AsyncMock()

        await wrapper.savepoint()

        assert wrapper._savepoint is not None
        assert wrapper._savepoint.startswith("tortoise_savepoint_")
        mock_cursor.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_savepoint_rollback(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)
        wrapper._savepoint = "test_savepoint"
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute = AsyncMock()

        wrapper._connection = mock_connection
        mock_connection.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__aexit__ = AsyncMock()

        await wrapper.savepoint_rollback()

        mock_cursor.execute.assert_called_once_with("ROLLBACK TO SAVEPOINT test_savepoint")
        assert wrapper._savepoint is None
        assert wrapper._finalized is True

    @pytest.mark.asyncio
    async def test_savepoint_rollback_no_savepoint(self):
        with patch('builtins.print'):  # Mock the print statement
            parent_client = AwsMySQLClient(
                user="test_user",
                password="test_pass",
                database="test_db",
                host="localhost",
                port=3306,
                connection_name="test_conn"
            )

        wrapper = TransactionWrapper(parent_client)
        wrapper._savepoint = None

        with pytest.raises(TransactionManagementError, match="No savepoint to rollback to"):
            await wrapper.savepoint_rollback()


class TestGenSavepointName:
    def test_gen_savepoint_name(self):
        name1 = _gen_savepoint_name()
        name2 = _gen_savepoint_name()

        assert name1.startswith("tortoise_savepoint_")
        assert name2.startswith("tortoise_savepoint_")
        assert name1 != name2  # Should be unique
