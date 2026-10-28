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
from mysql.connector import MySQLConnection
from sqlalchemy import PoolProxiedConnection

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pg_driver_dialect import PgDriverDialect
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def mock_conn(mocker):
    conn_mock = mocker.MagicMock(spec=psycopg.Connection)
    del conn_mock.driver_connection
    return conn_mock


@pytest.fixture
def mock_pool_conn(mocker, mock_conn):
    pool_conn_mock = mocker.MagicMock(spec=PoolProxiedConnection)
    pool_conn_mock.driver_connection = mock_conn
    return pool_conn_mock


@pytest.fixture
def mock_invalid_conn(mocker):
    return mocker.MagicMock(spec=MySQLConnection)


@pytest.fixture
def dialect():
    return PgDriverDialect(Properties())


def test_abort_connection(dialect, mock_conn, mock_invalid_conn, mocker):
    # abort_connection runs on the EFM monitor thread against a connection owned
    # by another thread. It must NOT call close() (psycopg close() is PQfinish:
    # it frees the libpq struct + tears down SSL with no lock, racing a
    # use-after-free in libpq/OpenSSL -> SIGSEGV). It must shut the underlying
    # socket down instead: that interrupts the owning thread's recv (even on an
    # unreachable host) WITHOUT freeing the struct, and releases the fd without
    # closing it (the connection still owns it).
    mock_conn.closed = False
    mock_conn.fileno.return_value = 7
    mock_sock = mocker.MagicMock()
    socket_ctor = mocker.patch(
        "aws_advanced_python_wrapper.pg_driver_dialect.socket.socket", return_value=mock_sock)

    dialect.abort_connection(mock_conn)
    socket_ctor.assert_called_once_with(fileno=7)
    mock_sock.shutdown.assert_called_once()
    mock_sock.detach.assert_called_once()
    mock_conn.close.assert_not_called()

    # already-closed connection: nothing to do
    mock_conn.reset_mock()
    socket_ctor.reset_mock()
    mock_conn.closed = True
    dialect.abort_connection(mock_conn)
    socket_ctor.assert_not_called()
    mock_conn.close.assert_not_called()

    with pytest.raises(AwsWrapperError):
        dialect.abort_connection(mock_invalid_conn)


def test_is_closed(dialect, mock_conn, mock_invalid_conn):
    mock_conn.closed = True
    assert dialect.is_closed(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_closed(mock_invalid_conn)


def test_is_in_transaction(dialect, mock_conn, mock_invalid_conn):
    mock_conn.info.transaction_status = 1
    assert dialect.is_in_transaction(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_in_transaction(mock_invalid_conn)


def test_read_only(dialect, mock_conn, mock_invalid_conn):
    mock_conn.read_only = False
    assert not dialect.is_read_only(mock_conn)

    dialect.set_read_only(mock_conn, True)
    assert dialect.is_read_only(mock_conn)

    dialect.set_read_only(mock_conn, False)
    assert not dialect.is_read_only(mock_conn)

    mock_conn.read_only = None
    assert not dialect.is_read_only(mock_conn)

    with pytest.raises(AwsWrapperError):
        dialect.is_read_only(mock_invalid_conn)

    with pytest.raises(AwsWrapperError):
        dialect.set_read_only(mock_invalid_conn, True)


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
    mock_conn.read_only = True
    mock_conn.isolation_level = 1

    new_conn = mocker.MagicMock(psycopg.Connection)

    new_conn.autocommit = True
    new_conn.read_only = False
    new_conn.isolation_level = 0
    dialect.transfer_session_state(mock_conn, new_conn)

    assert new_conn.autocommit is False
    assert new_conn.read_only is True
    assert new_conn.isolation_level == 1


def test_get_connection_from_obj(dialect, mocker, mock_conn, mock_invalid_conn):
    assert dialect.get_connection_from_obj(mock_conn) == mock_conn

    mock_cursor = mocker.MagicMock(spec=psycopg.Cursor)
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


def test_fold_identifier(dialect):
    assert dialect.fold_identifier("Users", quoted=False) == "users"
    assert dialect.fold_identifier("Users", quoted=True) == "Users"
    assert dialect.fold_identifier("ÉTÉ", quoted=False) == "ÉtÉ"


def test_metadata_key_keeps_case(dialect):
    assert dialect.metadata_key("Users") == "Users"


def test_upsert_clause(dialect):
    assert dialect.upsert_clause(["id"], ["name", "age"], ["updated_at = NOW()"]) == \
        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, age = EXCLUDED.age, updated_at = NOW()"


@pytest.mark.parametrize("row", [(7,), {"id": 7}])
def test_insert_returning_id(dialect, mocker, row):
    conn = mocker.MagicMock()
    cursor = conn.cursor.return_value
    cursor.fetchone.return_value = row
    assert dialect.insert_returning_id(conn, "INSERT INTO t (name) VALUES (%s)", ("a",)) == 7
    cursor.execute.assert_called_once_with("INSERT INTO t (name) VALUES (%s) RETURNING id", ("a",))


def test_analyzable_sql_composable(dialect, mocker):
    cursor = mocker.MagicMock()
    composed = mocker.MagicMock(spec=psycopg.sql.Composed)
    composed.as_string.return_value = "SELECT ssn FROM users"
    assert dialect.analyzable_sql(composed, cursor) == ("SELECT ssn FROM users", None)
    composed.as_string.assert_called_once_with(cursor)


def test_analyzable_sql_bytes(dialect, mocker, mock_conn):
    mock_conn.info.encoding = "utf-8"
    cursor = mocker.MagicMock(spec=psycopg.Cursor)
    cursor.connection = mock_conn
    assert dialect.analyzable_sql(b"SELECT 1", cursor) == ("SELECT 1", "utf-8")
    assert dialect.analyzable_sql("SELECT 1", cursor) == ("SELECT 1", None)


def test_analyzable_sql_bytes_without_connection(dialect, mocker):
    assert dialect.analyzable_sql(b"SELECT 1", mocker.MagicMock()) == (None, None)


@pytest.mark.parametrize("codec", ["shift_jis_2004", "ascii"])
def test_analyzable_sql_bytes_codec_mismatch(dialect, mocker, mock_conn, codec):
    mock_conn.info.encoding = codec
    cursor = mocker.MagicMock(spec=psycopg.Cursor)
    cursor.connection = mock_conn
    assert dialect.analyzable_sql("SELECT 'é'".encode("utf-8"), cursor) == (None, codec)


def test_unsupported_row_factory(dialect, mocker):
    cursor = mocker.MagicMock(spec=psycopg.Cursor)
    for factory in (psycopg.rows.tuple_row, psycopg.rows.dict_row, psycopg.rows.namedtuple_row):
        cursor.row_factory = factory
        assert dialect.unsupported_row_factory(cursor) is None

    cursor.row_factory = psycopg.rows.class_row
    assert dialect.unsupported_row_factory(cursor) is psycopg.rows.class_row
    assert dialect.unsupported_row_factory(mocker.MagicMock()) is None


def test_insert_returning_id_times_out_with_socket_timeout(mocker, timeout_dialect, wait_then_return):
    dialect, event = timeout_dialect
    conn = mocker.MagicMock()
    conn.cursor.return_value.fetchone.side_effect = wait_then_return(event, (7,))
    with pytest.raises(QueryTimeoutError):
        dialect.insert_returning_id(conn, "INSERT INTO t (name) VALUES (%s)", ("a",))


@pytest.mark.parametrize("params, binds", [
    (None, False), ({}, True), ([], True), ((), True), ({"a": 1}, True), ([1], True)])
def test_binds_parameters_any_parameters(params, binds):
    assert PgDriverDialect.binds_parameters(params) is binds
