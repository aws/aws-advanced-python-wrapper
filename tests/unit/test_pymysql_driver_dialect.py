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

import psycopg # type: ignore
import pytest # type: ignore
from pymysql.connections import Connection as PyMySQLConnection # type: ignore
from pymysql.cursors import Cursor as PyMySQLCursor # type: ignore

from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pymysql_driver_dialect import PyMySQLDriverDialect
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def dialect():
    return PyMySQLDriverDialect(Properties())


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=PyMySQLConnection)


@pytest.fixture
def mock_invalid_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


def test_is_closed(dialect, mock_conn, mock_invalid_conn):
    mock_conn.open = False
    assert dialect.is_closed(mock_conn)

    mock_conn.open = True
    assert not dialect.is_closed(mock_conn)

    with pytest.raises(UnsupportedOperationError):
        dialect.is_closed(mock_invalid_conn)


def test_is_in_transaction(dialect, mock_conn, mock_invalid_conn, mocker):
    mock_cursor = mocker.MagicMock()
    mock_cursor.fetchone.return_value = (0,)  # autocommit = 0 means in transaction
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    assert dialect.is_in_transaction(mock_conn)
    mock_cursor.execute.assert_called_with("SELECT @@autocommit")

    mock_cursor.fetchone.return_value = (1,)  # autocommit = 1 means not in transaction
    assert not dialect.is_in_transaction(mock_conn)

    with pytest.raises(UnsupportedOperationError):
        dialect.is_in_transaction(mock_invalid_conn)


def test_autocommit(dialect, mock_conn, mock_invalid_conn):
    mock_conn.get_autocommit.return_value = False
    assert not dialect.get_autocommit(mock_conn)

    mock_conn.get_autocommit.return_value = True
    assert dialect.get_autocommit(mock_conn)

    dialect.set_autocommit(mock_conn, True)
    mock_conn.autocommit.assert_called_with(True)

    dialect.set_autocommit(mock_conn, False)
    mock_conn.autocommit.assert_called_with(False)

    with pytest.raises(UnsupportedOperationError):
        dialect.get_autocommit(mock_invalid_conn)

    with pytest.raises(UnsupportedOperationError):
        dialect.set_autocommit(mock_invalid_conn, True)


def test_transfer_session_state(dialect, mocker, mock_conn):
    mock_conn.get_autocommit.return_value = False
    new_conn = mocker.MagicMock(spec=PyMySQLConnection)

    dialect.transfer_session_state(mock_conn, new_conn)
    new_conn.autocommit.assert_called_with(False)


def test_get_connection_from_obj(dialect, mocker, mock_conn, mock_invalid_conn):
    assert dialect.get_connection_from_obj(mock_conn) == mock_conn

    mock_cursor = mocker.MagicMock(spec=PyMySQLCursor)
    mock_cursor.connection = mock_conn
    assert dialect.get_connection_from_obj(mock_cursor) == mock_conn

    assert dialect.get_connection_from_obj(mock_invalid_conn) is None


def test_prepare_connect_info(dialect):
    host_info = HostInfo("localhost", 3306)
    original_props = Properties({
        WrapperProperties.DATABASE.name: "default_db",
        WrapperProperties.CONNECT_TIMEOUT_SEC.name: "30",
        WrapperProperties.PLUGINS.name: "failover",
        "some_driver_prop": "45"
    })
    expected_props = Properties({
        "host": host_info.host,
        "port": str(host_info.port),
        "database": "default_db",
        "connect_timeout": "30",
        "some_driver_prop": "45"
    })

    result = dialect.prepare_connect_info(host_info, original_props)
    assert result == expected_props


def test_set_password(dialect):
    props = Properties()
    dialect.set_password(props, "test_password")
    
    assert WrapperProperties.PASSWORD.get(props) == "test_password"


def test_ping(dialect, mock_conn):
    mock_conn.ping.return_value = None  # ping() succeeds
    assert dialect.ping(mock_conn)

    mock_conn.ping.side_effect = Exception("Connection lost")
    assert not dialect.ping(mock_conn)


def test_abort_connection(dialect, mock_conn):
    with pytest.raises(UnsupportedOperationError):
        dialect.abort_connection(mock_conn)
