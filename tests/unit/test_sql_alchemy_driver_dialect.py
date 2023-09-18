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

import pytest
import sqlalchemy

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.generic_target_driver_dialect import TargetDriverDialect
from aws_wrapper.sqlalchemy_driver_dialect import SqlAlchemyDriverDialect


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock()


@pytest.fixture
def sqlalchemy_mock_conn(mocker):
    return mocker.MagicMock(spec=sqlalchemy.PoolProxiedConnection)


@pytest.fixture
def underlying_dialect(mocker):
    return mocker.MagicMock(spec=TargetDriverDialect)


def test_sqlalchemy_abort_connection(mock_conn, underlying_dialect, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = mock_conn

    dialect = SqlAlchemyDriverDialect(underlying_dialect)

    dialect.abort_connection(sqlalchemy_mock_conn)
    underlying_dialect.abort_connection.assert_called_once()


def test_sqlalchemy_abort_connection_with_null_connection(underlying_dialect, sqlalchemy_mock_conn):
    dialect = SqlAlchemyDriverDialect(underlying_dialect)

    sqlalchemy_mock_conn.driver_connection = None

    dialect.abort_connection(sqlalchemy_mock_conn)
    underlying_dialect.abort_connection.assert_not_called()


def test_is_closed(underlying_dialect, mock_conn, sqlalchemy_mock_conn):
    underlying_dialect.is_closed.return_value = False
    sqlalchemy_mock_conn.driver_connection = mock_conn
    sqlalchemy_dialect = SqlAlchemyDriverDialect(underlying_dialect)
    assert sqlalchemy_dialect.is_closed(sqlalchemy_mock_conn) is False


def test_sqlalchemy_is_closed_with_null_connection(underlying_dialect, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = None
    dialect = SqlAlchemyDriverDialect(underlying_dialect)

    assert dialect.is_closed(sqlalchemy_mock_conn) is True
    underlying_dialect.is_closed.assert_not_called()


def test_is_in_transaction(underlying_dialect, mock_conn, sqlalchemy_mock_conn):
    underlying_dialect.is_in_transaction.return_value = True
    sqlalchemy_mock_conn.driver_connection = mock_conn
    sqlalchemy_dialect = SqlAlchemyDriverDialect(underlying_dialect)
    assert sqlalchemy_dialect.is_in_transaction(sqlalchemy_mock_conn) is True


def test_is_in_transaction_with_null_connection(underlying_dialect, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = None
    sqlalchemy_dialect = SqlAlchemyDriverDialect(underlying_dialect)

    assert sqlalchemy_dialect.is_in_transaction(sqlalchemy_mock_conn) is False
    underlying_dialect.is_in_transaction.assert_not_called()


def test_read_only(underlying_dialect, mock_conn, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = mock_conn
    sqlalchemy_dialect = SqlAlchemyDriverDialect(underlying_dialect)

    underlying_dialect.is_read_only.return_value = False
    assert sqlalchemy_dialect.is_read_only(mock_conn) is False
    sqlalchemy_dialect.set_read_only(mock_conn, True)
    underlying_dialect.set_read_only.assert_called_once()
    underlying_dialect.is_read_only.return_value = True
    assert sqlalchemy_dialect.is_read_only(mock_conn) is True


def test_read_only_with_null_connections(underlying_dialect, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = None
    sqlalchemy_dialect = SqlAlchemyDriverDialect(underlying_dialect)
    with pytest.raises(AwsWrapperError):
        sqlalchemy_dialect.set_read_only(sqlalchemy_mock_conn, False)


def test_transfer_session_states(mocker, underlying_dialect, mock_conn, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = mock_conn
    sqlalchemy_dialect = SqlAlchemyDriverDialect(underlying_dialect)
    sqlalchemy_dialect.transfer_session_state(mock_conn, mocker.MagicMock())

    underlying_dialect.transfer_session_state.assert_called_once()


def test_transfer_session_states_with_null_connection(underlying_dialect, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = None
    sqlalchemy_dialect = SqlAlchemyDriverDialect(underlying_dialect)
    with pytest.raises(AwsWrapperError):
        sqlalchemy_dialect.set_read_only(sqlalchemy_mock_conn, False)
