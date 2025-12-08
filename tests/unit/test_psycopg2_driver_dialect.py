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

import psycopg2 #type:ignore
import pytest # type: ignore
from mysql.connector import MySQLConnection # type: ignore
from psycopg2.extensions import connection as Psycopg2Connection, cursor as Psycopg2Cursor, TRANSACTION_STATUS_IDLE # type: ignore

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.psycopg2_driver_dialect import Psycopg2DriverDialect
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def mock_conn(mocker):
    conn_mock = mocker.MagicMock(spec=Psycopg2Connection)
    return conn_mock


@pytest.fixture
def mock_invalid_conn(mocker):
    return mocker.MagicMock(spec=MySQLConnection)


@pytest.fixture
def dialect():
    return Psycopg2DriverDialect(Properties())


def test_abort_connection(dialect, mock_conn, mock_invalid_conn):
    dialect.abort_connection(mock_conn)
    mock_conn.close.assert_called_once()

    with pytest.raises(AwsWrapperError):
        dialect.abort_connection(mock_invalid_conn)


def test_is_closed(dialect, mock_conn, mock_invalid_conn):
    mock_conn.closed = 1
    assert dialect.is_closed(mock_conn)

    mock_conn.closed = 0
    assert not dialect.is_closed(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_closed(mock_invalid_conn)


def test_is_in_transaction_with_info(dialect, mock_conn, mock_invalid_conn, mocker):
    # Test with conn.info (psycopg2 >= 2.8)
    mock_info = mocker.MagicMock()
    mock_info.transaction_status = 2  # TRANSACTION_STATUS_INTRANS
    mock_conn.info = mock_info
    assert dialect.is_in_transaction(mock_conn)

    mock_info.transaction_status = TRANSACTION_STATUS_IDLE
    assert not dialect.is_in_transaction(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_in_transaction(mock_invalid_conn)


# Added: Test for psycopg2 < 2.8 compatibility where conn.info doesn't exist
def test_is_in_transaction_without_info(dialect, mock_conn):
    # Simulate psycopg2 < 2.8 by removing info attribute
    del mock_conn.info
    mock_conn.get_transaction_status.return_value = 2  # TRANSACTION_STATUS_INTRANS
    assert dialect.is_in_transaction(mock_conn)

    mock_conn.get_transaction_status.return_value = TRANSACTION_STATUS_IDLE
    assert not dialect.is_in_transaction(mock_conn)


def test_is_read_only_with_readonly_attr(dialect, mock_conn, mock_invalid_conn):
    # Test with conn.readonly (psycopg2 >= 2.7)
    mock_conn.readonly = False
    assert not dialect.is_read_only(mock_conn)

    mock_conn.readonly = True
    assert dialect.is_read_only(mock_conn)

    mock_conn.readonly = None
    assert not dialect.is_read_only(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_read_only(mock_invalid_conn)


# Added: Test for psycopg2 < 2.7 compatibility where conn.readonly doesn't exist
def test_is_read_only_without_readonly_attr(dialect, mock_conn, mocker):
    # Simulate psycopg2 < 2.7 by removing readonly attribute
    del mock_conn.readonly
    
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    mock_cursor.fetchone.return_value = ('on',)
    assert dialect.is_read_only(mock_conn)
    
    mock_cursor.fetchone.return_value = ('off',)
    assert not dialect.is_read_only(mock_conn)


def test_set_read_only_with_readonly_attr(dialect, mock_conn, mock_invalid_conn):
    # Test with conn.readonly (psycopg2 >= 2.7)
    mock_conn.readonly = False
    
    dialect.set_read_only(mock_conn, True)
    assert mock_conn.readonly is True

    dialect.set_read_only(mock_conn, False)
    assert mock_conn.readonly is False

    with pytest.raises(AwsWrapperError):
        dialect.set_read_only(mock_invalid_conn, True)


# Added: Test for psycopg2 < 2.7 compatibility where conn.readonly doesn't exist
def test_set_read_only_without_readonly_attr(dialect, mock_conn):
    # Simulate psycopg2 < 2.7 by making readonly assignment raise AttributeError
    def raise_attr_error(self, value):
        raise AttributeError("readonly")
    
    type(mock_conn).readonly = property(lambda self: None, raise_attr_error)
    
    dialect.set_read_only(mock_conn, True)
    mock_conn.set_session.assert_called_with(readonly=True)
    
    dialect.set_read_only(mock_conn, False)
    mock_conn.set_session.assert_called_with(readonly=False)


def test_autocommit(dialect, mock_conn, mock_invalid_conn):
    mock_conn.autocommit = False
    assert not dialect.get_autocommit(mock_conn)

    dialect.set_autocommit(mock_conn, True)
    assert dialect.get_autocommit(mock_conn)

    dialect.set_autocommit(mock_conn, False)
    assert not dialect.get_autocommit(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.get_autocommit(mock_invalid_conn)

    with pytest.raises(AwsWrapperError):
        dialect.set_autocommit(mock_invalid_conn, True)


def test_transfer_session_state(dialect, mocker, mock_conn):
    mock_conn.autocommit = False
    mock_conn.isolation_level = 1

    new_conn = mocker.MagicMock(spec=Psycopg2Connection)
    new_conn.autocommit = True
    new_conn.isolation_level = 0

    # Mock is_read_only and set_read_only to avoid attribute issues
    mocker.patch.object(dialect, 'is_read_only', return_value=True)
    mocker.patch.object(dialect, 'set_read_only')

    dialect.transfer_session_state(mock_conn, new_conn)

    assert new_conn.autocommit is False
    assert new_conn.isolation_level == 1
    dialect.set_read_only.assert_called_once_with(new_conn, True)


def test_get_connection_from_obj(dialect, mocker, mock_conn, mock_invalid_conn):
    assert dialect.get_connection_from_obj(mock_conn) == mock_conn

    mock_cursor = mocker.MagicMock(spec=Psycopg2Cursor)
    mock_cursor.connection = mock_conn
    assert dialect.get_connection_from_obj(mock_cursor) == mock_conn

    assert dialect.get_connection_from_obj(mock_invalid_conn) is None


def test_prepare_connect_info(dialect):
    host_info = HostInfo("localhost", 5432)
    original_props = Properties({
        WrapperProperties.DATABASE.name: "default_db",
        WrapperProperties.CONNECT_TIMEOUT_SEC.name: "30",
        WrapperProperties.TCP_KEEPALIVE.name: "True",
        WrapperProperties.TCP_KEEPALIVE_TIME_SEC.name: "10",
        WrapperProperties.TCP_KEEPALIVE_INTERVAL_SEC.name: "5",
        WrapperProperties.TCP_KEEPALIVE_PROBES.name: "3",
        WrapperProperties.PLUGINS.name: "failover",
        "some_driver_prop": "45"
    })
    expected_props = Properties({
        "host": host_info.host,
        "port": str(host_info.port),
        "dbname": "default_db",
        "connect_timeout": "30",
        "keepalives": "True",
        "keepalives_idle": "10",
        "keepalives_interval": "5",
        "keepalives_count": "3",
        "some_driver_prop": "45"
    })

    result = dialect.prepare_connect_info(host_info, original_props)
    assert result == expected_props


def test_supports_methods(dialect):
    assert dialect.supports_connect_timeout()
    assert dialect.supports_tcp_keepalive()
    assert dialect.supports_abort_connection()
