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
from mysql.connector.errors import OperationalError

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.sqlalchemy.mysql_orm_dialect import \
    SqlAlchemyOrmMysqlDialect


@pytest.fixture
def dialect():
    # __init__ requires a live DBAPI, so bypass it to unit test do_ping in isolation.
    return SqlAlchemyOrmMysqlDialect.__new__(SqlAlchemyOrmMysqlDialect)


@pytest.fixture
def dbapi_connection(mocker):
    connection = mocker.MagicMock(spec=AwsWrapperConnection)
    connection.target_connection = mocker.MagicMock()
    return connection


def test_do_ping_pings_wrapped_target_connection(dialect, dbapi_connection):
    assert dialect.do_ping(dbapi_connection) is True
    dbapi_connection.target_connection.ping.assert_called_once_with(reconnect=False)


def test_do_ping_returns_false_on_dead_connection(dialect, dbapi_connection):
    dbapi_connection.target_connection.ping.side_effect = OperationalError("MySQL server has gone away")
    assert dialect.do_ping(dbapi_connection) is False
