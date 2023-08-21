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

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.mysql_target_driver_dialect import MySQLTargetDriverDialect
from aws_wrapper.pg_target_driver_dialect import PgTargetDriverDialect


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock()


def test_abort_connection(mock_conn):
    dialect = PgTargetDriverDialect()

    dialect.abort_connection(mock_conn)
    mock_conn.cancel.assert_called_once()

    del mock_conn.cancel
    with pytest.raises(AwsWrapperError):
        dialect.abort_connection(mock_conn)


def test_is_closed(mock_conn):
    mock_conn.is_connected.return_value = False
    dialect = MySQLTargetDriverDialect()

    assert dialect.is_closed(mock_conn) is True

    del mock_conn.is_connected
    with pytest.raises(AwsWrapperError):
        dialect.is_closed(mock_conn)

    dialect = PgTargetDriverDialect()
    mock_conn.closed = True

    assert dialect.is_closed(mock_conn) is True

    del mock_conn.closed
    with pytest.raises(AwsWrapperError):
        dialect.is_closed(mock_conn)


def test_is_in_transaction(mock_conn):
    mock_conn.in_transaction = True

    dialect = MySQLTargetDriverDialect()

    assert dialect.is_in_transaction(mock_conn) is True

    del mock_conn.in_transaction
    with pytest.raises(AwsWrapperError):
        dialect.is_in_transaction(mock_conn)

    mock_conn.info.transaction_status = 1

    dialect = PgTargetDriverDialect()

    assert dialect.is_in_transaction(mock_conn) is True

    del mock_conn.info.transaction_status
    with pytest.raises(AwsWrapperError):
        dialect.is_in_transaction(mock_conn)


def test_read_only(mock_conn):
    dialect = MySQLTargetDriverDialect()
    assert not dialect.is_read_only(mock_conn)

    dialect.set_read_only(mock_conn, True)
    assert dialect.is_read_only(mock_conn)

    dialect.set_read_only(mock_conn, False)
    assert not dialect.is_read_only(mock_conn)

    mock_conn.read_only = False
    dialect = PgTargetDriverDialect()

    assert dialect.is_read_only(mock_conn) is False

    dialect.set_read_only(mock_conn, True)
    assert dialect.is_read_only(mock_conn)

    dialect.set_read_only(mock_conn, False)
    assert not dialect.is_read_only(mock_conn)

    del mock_conn.read_only
    with pytest.raises(AwsWrapperError):
        dialect.is_read_only(mock_conn)
    with pytest.raises(AwsWrapperError):
        dialect.set_read_only(mock_conn, True)
