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

from __future__ import annotations

import pytest
from mysql.connector import DatabaseError
from psycopg.errors import (InternalError, InvalidAuthorizationSpecification,
                            InvalidPassword, OperationalError,
                            ReadOnlySqlTransaction)

from aws_advanced_python_wrapper.errors import (AwsConnectError,
                                                AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.utils.mysql_exception_handler import \
    MySQLExceptionHandler
from aws_advanced_python_wrapper.utils.pg_exception_handler import \
    PgExceptionHandler


@pytest.fixture
def mysql_handler():
    return MySQLExceptionHandler()


@pytest.fixture
def pg_handler():
    return PgExceptionHandler()


def create_nested_error(inner_error):
    return AwsWrapperError("Wrapper error", inner_error)


def test_is_network_exception_with_nested_aws_wrapper_error(mysql_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", DatabaseError(errno=2006, msg="MySQL server has gone away")))

    assert mysql_handler.is_network_exception(error=wrapper_error) is True


def test_is_network_exception_with_deeply_nested_error(mysql_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", DatabaseError(errno=2013, msg="Lost connection to MySQL server")))

    assert mysql_handler.is_network_exception(error=wrapper_error) is True


def test_is_network_exception_with_nested_non_network_error(mysql_handler):
    wrapper_error = create_nested_error(DatabaseError(errno=1234, msg="Some other error"))

    assert mysql_handler.is_network_exception(error=wrapper_error) is False


def test_is_network_exception_with_nested_aws_wrapper_error_pg(pg_handler):
    wrapper_error = create_nested_error(OperationalError("connection failed"))

    assert pg_handler.is_network_exception(error=wrapper_error) is True


def test_is_network_exception_with_deeply_nested_error_pg(pg_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", OperationalError("connection socket closed")))

    assert pg_handler.is_network_exception(error=wrapper_error) is True


def test_is_network_exception_with_nested_non_network_error_mysql(mysql_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", OperationalError("some other error")))

    assert mysql_handler.is_network_exception(error=wrapper_error) is False


def test_triple_nested_mysql_exception(mysql_handler):
    wrapper_error = create_nested_error(
        AwsWrapperError("Level 2", AwsWrapperError("Level 3", DatabaseError(errno=2003, msg="Can't connect to MySQL server"))))

    assert mysql_handler.is_network_exception(error=wrapper_error) is True


def test_triple_nested_pg_exception(pg_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Level 2", AwsWrapperError("Level 3", OperationalError("connection failed"))))

    assert pg_handler.is_network_exception(error=wrapper_error) is True


def test_nested_with_mixed_error_types_mysql(mysql_handler):
    wrapper_error = create_nested_error(QueryTimeoutError("Timeout"))

    assert mysql_handler.is_network_exception(error=wrapper_error) is True


def test_nested_with_mixed_error_types_pg(pg_handler):
    wrapper_error = create_nested_error(AwsConnectError("Connect error", OperationalError("consuming input failed")))

    assert pg_handler.is_network_exception(error=wrapper_error) is True


def test_nested_exception_stops_at_first_non_wrapper(mysql_handler):
    wrapper_error = create_nested_error(DatabaseError(errno=2001, msg="Some error"))

    assert mysql_handler.is_network_exception(error=wrapper_error) is True


def test_nested_exception_with_none_cause(mysql_handler):
    error = AwsWrapperError("Wrapper with no cause")
    error.__cause__ = None

    assert mysql_handler.is_network_exception(error=error) is False


# Tests for is_login_exception with nested AwsWrapperError
def test_is_login_exception_with_nested_aws_wrapper_error_mysql(mysql_handler):
    class MockLoginError(Exception):
        def __init__(self):
            self.sqlstate = "28000"

    wrapper_error = AwsWrapperError("Wrapper error", MockLoginError())

    assert mysql_handler.is_login_exception(error=wrapper_error) is True


def test_is_login_exception_with_deeply_nested_error_mysql(mysql_handler):
    class MockLoginError(Exception):
        def __init__(self):
            self.sqlstate = "28000"

    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", MockLoginError()))

    assert mysql_handler.is_login_exception(error=wrapper_error) is True


def test_is_login_exception_with_nested_non_login_error_mysql(mysql_handler):
    wrapper_error = create_nested_error(DatabaseError(errno=1234, msg="Some other error"))

    assert mysql_handler.is_login_exception(error=wrapper_error) is False


def test_is_login_exception_with_nested_aws_wrapper_error_pg(pg_handler):
    wrapper_error = create_nested_error(InvalidPassword("password authentication failed"))

    assert pg_handler.is_login_exception(error=wrapper_error) is True


def test_is_login_exception_with_deeply_nested_error_pg(pg_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", InvalidAuthorizationSpecification("PAM authentication failed")))

    assert pg_handler.is_login_exception(error=wrapper_error) is True


def test_is_login_exception_with_nested_non_login_error_pg(pg_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", OperationalError("some other error")))

    assert pg_handler.is_login_exception(error=wrapper_error) is False


def test_is_read_only_exception_with_nested_aws_wrapper_error_mysql(mysql_handler):
    class MockReadOnlyError(Exception):
        def __init__(self):
            self.errno = "1836"
            self.msg = "Running in read-only mode"

    wrapper_error = AwsWrapperError("Wrapper error", MockReadOnlyError())

    assert mysql_handler.is_read_only_connection_exception(error=wrapper_error) is True


def test_is_read_only_exception_with_deeply_nested_error_mysql(mysql_handler):
    class MockReadOnlyError(Exception):
        def __init__(self):
            self.errno = "1836"
            self.msg = "Running in read-only mode"

    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", MockReadOnlyError()))

    assert mysql_handler.is_read_only_connection_exception(error=wrapper_error) is True


def test_is_read_only_exception_with_nested_non_readonly_error_mysql(mysql_handler):
    wrapper_error = create_nested_error(DatabaseError(errno=1234, msg="Some other error"))

    assert mysql_handler.is_read_only_connection_exception(error=wrapper_error) is False


def test_is_read_only_exception_with_nested_aws_wrapper_error_pg(pg_handler):
    wrapper_error = create_nested_error(ReadOnlySqlTransaction("cannot execute in a read-only transaction"))

    assert pg_handler.is_read_only_connection_exception(error=wrapper_error) is True


def test_is_read_only_exception_with_deeply_nested_error_pg(pg_handler):
    wrapper_error = create_nested_error(AwsWrapperError("Middle wrapper", InternalError("cannot execute UPDATE in a read-only transaction")))

    assert pg_handler.is_read_only_connection_exception(error=wrapper_error) is True


def test_is_read_only_exception_with_nested_non_readonly_error_pg(pg_handler):
    wrapper_error = create_nested_error(OperationalError("some other error"))
    assert pg_handler.is_read_only_connection_exception(error=wrapper_error) is False


def test_circular_reference_no_match_mysql(mysql_handler):
    inner_error = DatabaseError(errno=1234, msg="Some other error")
    wrapper1 = AwsWrapperError("Wrapper 1", inner_error)
    wrapper2 = AwsWrapperError("Wrapper 2", wrapper1)
    inner_error.__cause__ = wrapper2

    assert mysql_handler.is_network_exception(error=wrapper2) is False
