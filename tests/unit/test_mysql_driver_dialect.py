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

import psycopg
import pytest
from mysql.connector import CMySQLConnection
from mysql.connector.cursor_cext import CMySQLCursor

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.mysql_driver_dialect import MySQLDriverDialect
from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperties


@pytest.fixture
def dialect():
    return MySQLDriverDialect()


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=CMySQLConnection)


@pytest.fixture
def mock_invalid_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


def test_is_closed(dialect, mock_conn, mock_invalid_conn):
    mock_conn.is_connected.return_value = False
    assert dialect.is_closed(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_closed(mock_invalid_conn)


def test_is_in_transaction(dialect, mock_conn, mock_invalid_conn):
    mock_conn.in_transaction = True
    assert dialect.is_in_transaction(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_in_transaction(mock_invalid_conn)


def test_read_only(dialect, mock_conn):
    assert not dialect.is_read_only(mock_conn)

    dialect.set_read_only(mock_conn, True)
    assert dialect.is_read_only(mock_conn)

    dialect.set_read_only(mock_conn, False)
    assert not dialect.is_read_only(mock_conn)


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
    new_conn = mocker.MagicMock(spec=CMySQLConnection)

    new_conn.autocommit = True
    dialect.transfer_session_state(mock_conn, new_conn)

    assert new_conn.autocommit is False


def test_get_connection_from_obj(dialect, mocker, mock_conn, mock_invalid_conn):
    assert dialect.get_connection_from_obj(mock_conn) == mock_conn

    mock_cursor = mocker.MagicMock(spec=CMySQLCursor)
    mock_cursor._cnx = mock_conn
    assert dialect.get_connection_from_obj(mock_cursor) == mock_conn

    assert dialect.get_connection_from_obj(mock_invalid_conn) is None


def test_prepare_connect_info(dialect):
    host_info = HostInfo("localhost", 3306)
    original_props = Properties({
        WrapperProperties.CONNECT_TIMEOUT_SEC.name: "30",
        WrapperProperties.PLUGINS.name: "failover",
        "some_driver_prop": "45"
    })
    expected_props = Properties({
        "host": host_info.host,
        "port": str(host_info.port),
        "connect_timeout": "30",
        "some_driver_prop": "45"
    })

    result = dialect.prepare_connect_info(host_info, original_props)
    assert result == expected_props
