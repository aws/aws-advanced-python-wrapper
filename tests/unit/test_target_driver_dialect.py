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

import mysql.connector
import psycopg
import pytest

from aws_wrapper.mysql_target_driver_dialect import MySQLTargetDriverDialect
from aws_wrapper.pg_target_driver_dialect import PgTargetDriverDialect


@pytest.fixture
def pg_mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mysql_mock_conn(mocker):
    return mocker.MagicMock(spec=mysql.connector.CMySQLConnection)


def test_abort_connection(pg_mock_conn):
    dialect = PgTargetDriverDialect()

    dialect.abort_connection(pg_mock_conn)
    pg_mock_conn.cancel.assert_called_once()


def test_is_closed(pg_mock_conn, mysql_mock_conn):
    mysql_mock_conn.is_connected.return_value = False
    dialect = MySQLTargetDriverDialect()

    assert dialect.is_closed(mysql_mock_conn) is True

    dialect = PgTargetDriverDialect()
    pg_mock_conn.closed = True

    assert dialect.is_closed(pg_mock_conn) is True


def test_is_in_transaction(pg_mock_conn, mysql_mock_conn):
    mysql_mock_conn.in_transaction = True

    dialect = MySQLTargetDriverDialect()

    assert dialect.is_in_transaction(mysql_mock_conn) is True

    pg_mock_conn.info.transaction_status = 1

    dialect = PgTargetDriverDialect()

    assert dialect.is_in_transaction(pg_mock_conn) is True


def test_read_only(pg_mock_conn, mysql_mock_conn):
    dialect = MySQLTargetDriverDialect()
    assert not dialect.is_read_only(mysql_mock_conn)

    dialect.set_read_only(mysql_mock_conn, True)
    assert dialect.is_read_only(mysql_mock_conn)

    dialect.set_read_only(mysql_mock_conn, False)
    assert not dialect.is_read_only(mysql_mock_conn)

    pg_mock_conn.read_only = False
    dialect = PgTargetDriverDialect()

    assert dialect.is_read_only(pg_mock_conn) is False

    dialect.set_read_only(pg_mock_conn, True)
    assert dialect.is_read_only(pg_mock_conn)

    dialect.set_read_only(pg_mock_conn, False)
    assert not dialect.is_read_only(pg_mock_conn)


def test_mysql_transfer_session_states(mocker, mysql_mock_conn):
    del mysql_mock_conn.driver_connection

    dialect = MySQLTargetDriverDialect()
    mysql_mock_conn.autocommit = False

    new_conn = mocker.MagicMock(mysql.connector.CMySQLConnection)

    new_conn.autocommit = True
    dialect.transfer_session_state(mysql_mock_conn, new_conn)

    assert new_conn.autocommit is False


def test_pg_transfer_session_states(mocker, pg_mock_conn):
    dialect = PgTargetDriverDialect()
    pg_mock_conn.autocommit = False
    pg_mock_conn.read_only = True
    pg_mock_conn.isolation_level = 1

    new_conn = mocker.MagicMock(psycopg.Connection)

    new_conn.autocommit = True
    new_conn.read_only = False
    new_conn.isolation_level = 0
    dialect.transfer_session_state(pg_mock_conn, new_conn)

    assert new_conn.autocommit is False
    assert new_conn.read_only is True
    assert new_conn.isolation_level == 1
