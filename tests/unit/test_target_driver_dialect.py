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
import sqlalchemy

from aws_wrapper.mysql_target_driver_dialect import MySQLTargetDriverDialect
from aws_wrapper.pg_target_driver_dialect import PgTargetDriverDialect
from aws_wrapper.sqlalchemy_driver_dialect import SqlAlchemyDriverDialect


@pytest.fixture
def pg_mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mysql_mock_conn(mocker):
    return mocker.MagicMock(spec=mysql.connector.CMySQLConnection)


@pytest.fixture
def sqlalchemy_mock_conn(mocker):
    return mocker.MagicMock(spec=sqlalchemy.PoolProxiedConnection)


def test_abort_connection(pg_mock_conn):
    dialect = PgTargetDriverDialect()

    dialect.abort_connection(pg_mock_conn)
    pg_mock_conn.cancel.assert_called_once()


def test_sqlalchemy_abort_connection(pg_mock_conn, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = pg_mock_conn

    dialect = PgTargetDriverDialect()
    dialect = SqlAlchemyDriverDialect(dialect)

    dialect.abort_connection(sqlalchemy_mock_conn)
    pg_mock_conn.cancel.assert_called_once()


def test_is_closed(pg_mock_conn, mysql_mock_conn, sqlalchemy_mock_conn):
    mysql_mock_conn.is_connected.return_value = False
    sqlalchemy_mock_conn.driver_connection = mysql_mock_conn
    dialect = MySQLTargetDriverDialect()

    assert dialect.is_closed(mysql_mock_conn) is True

    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)
    assert sqlalchemy_dialect.is_closed(sqlalchemy_mock_conn) is True

    sqlalchemy_mock_conn.driver_connection = pg_mock_conn
    dialect = PgTargetDriverDialect()
    pg_mock_conn.closed = True

    assert dialect.is_closed(pg_mock_conn) is True

    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)
    assert sqlalchemy_dialect.is_closed(sqlalchemy_mock_conn) is True


def test_is_in_transaction(pg_mock_conn, mysql_mock_conn, sqlalchemy_mock_conn):
    mysql_mock_conn.in_transaction = True
    sqlalchemy_mock_conn.driver_connection = mysql_mock_conn

    dialect = MySQLTargetDriverDialect()

    assert dialect.is_in_transaction(mysql_mock_conn) is True

    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)
    assert sqlalchemy_dialect.is_in_transaction(sqlalchemy_mock_conn) is True

    pg_mock_conn.info.transaction_status = 1
    sqlalchemy_mock_conn.driver_connection = pg_mock_conn

    dialect = PgTargetDriverDialect()

    assert dialect.is_in_transaction(pg_mock_conn) is True

    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)
    assert sqlalchemy_dialect.is_in_transaction(sqlalchemy_mock_conn) is True


def test_read_only(pg_mock_conn, mysql_mock_conn, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = mysql_mock_conn
    dialect = MySQLTargetDriverDialect()
    assert dialect.is_read_only(mysql_mock_conn) is False

    dialect.set_read_only(mysql_mock_conn, True)
    assert dialect.is_read_only(mysql_mock_conn) is True

    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)
    assert sqlalchemy_dialect.is_read_only(sqlalchemy_mock_conn) is True

    dialect.set_read_only(mysql_mock_conn, False)
    assert dialect.is_read_only(mysql_mock_conn) is False

    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)
    assert dialect.is_read_only(sqlalchemy_mock_conn) is False
    dialect.set_read_only(pg_mock_conn, True)
    assert sqlalchemy_dialect.is_read_only(sqlalchemy_mock_conn) is True

    pg_mock_conn.read_only = False
    dialect = PgTargetDriverDialect()
    sqlalchemy_mock_conn.driver_connection = pg_mock_conn

    assert dialect.is_read_only(pg_mock_conn) is False

    dialect.set_read_only(pg_mock_conn, True)
    assert dialect.is_read_only(pg_mock_conn)

    dialect.set_read_only(pg_mock_conn, False)
    assert dialect.is_read_only(pg_mock_conn) is False

    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)
    assert dialect.is_read_only(pg_mock_conn) is False
    dialect.set_read_only(pg_mock_conn, True)
    assert sqlalchemy_dialect.is_read_only(sqlalchemy_mock_conn) is True


def test_mysql_transfer_session_states(mocker, mysql_mock_conn, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = mysql_mock_conn
    mysql_mock_conn.autocommit = False

    dialect = MySQLTargetDriverDialect()
    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)

    new_conn = mocker.MagicMock(mysql.connector.CMySQLConnection)
    new_conn.autocommit = True

    dialect.transfer_session_state(mysql_mock_conn, new_conn)

    assert new_conn.autocommit is False
    assert sqlalchemy_dialect.get_autocommit(sqlalchemy_mock_conn) is False


def test_pg_transfer_session_states(mocker, pg_mock_conn, sqlalchemy_mock_conn):
    sqlalchemy_mock_conn.driver_connection = pg_mock_conn

    dialect = PgTargetDriverDialect()
    sqlalchemy_dialect = SqlAlchemyDriverDialect(dialect)

    pg_mock_conn.autocommit = False
    pg_mock_conn.read_only = True
    pg_mock_conn.isolation_level = 1

    assert sqlalchemy_dialect.get_autocommit(sqlalchemy_mock_conn) is False
    assert sqlalchemy_dialect.is_read_only(sqlalchemy_mock_conn) is True

    new_conn = mocker.MagicMock(psycopg.Connection)

    new_conn.autocommit = True
    new_conn.read_only = False
    new_conn.isolation_level = 0
    dialect.transfer_session_state(pg_mock_conn, new_conn)

    assert new_conn.autocommit is False
    assert new_conn.read_only is True
    assert new_conn.isolation_level == 1

    assert sqlalchemy_dialect.get_autocommit(new_conn) is False
    assert sqlalchemy_dialect.is_read_only(new_conn) is True
