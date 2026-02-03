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
from psycopg.errors import OperationalError

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


def test_is_network_exception_with_nested_aws_wrapper_error(mysql_handler):
    inner_error = DatabaseError(errno=2006, msg="MySQL server has gone away")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert mysql_handler.is_network_exception(error=wrapper_error) is True


def test_is_network_exception_with_deeply_nested_error(mysql_handler):
    innermost_error = DatabaseError(errno=2013, msg="Lost connection to MySQL server")
    middle_error = AwsWrapperError("Middle wrapper")
    middle_error.__cause__ = innermost_error
    outer_error = AwsWrapperError("Outer wrapper")
    outer_error.__cause__ = middle_error

    assert mysql_handler.is_network_exception(error=outer_error) is True


def test_is_network_exception_with_nested_non_network_error(mysql_handler):
    inner_error = DatabaseError(errno=1234, msg="Some other error")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert mysql_handler.is_network_exception(error=wrapper_error) is False


def test_is_network_exception_with_nested_aws_wrapper_error_pg(pg_handler):
    inner_error = OperationalError("connection failed")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert pg_handler.is_network_exception(error=wrapper_error) is True


def test_is_network_exception_with_deeply_nested_error_pg(pg_handler):
    innermost_error = OperationalError("connection socket closed")
    middle_error = AwsWrapperError("Middle wrapper")
    middle_error.__cause__ = innermost_error
    outer_error = AwsWrapperError("Outer wrapper")
    outer_error.__cause__ = middle_error

    assert pg_handler.is_network_exception(error=outer_error) is True


def test_is_network_exception_with_nested_non_network_error_mysql(mysql_handler):
    inner_error = OperationalError("some other error")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert mysql_handler.is_network_exception(error=wrapper_error) is False


def test_triple_nested_mysql_exception(mysql_handler):
    innermost = DatabaseError(errno=2003, msg="Can't connect to MySQL server")
    level3 = AwsWrapperError("Level 3")
    level3.__cause__ = innermost
    level2 = AwsWrapperError("Level 2")
    level2.__cause__ = level3
    level1 = AwsWrapperError("Level 1")
    level1.__cause__ = level2

    assert mysql_handler.is_network_exception(error=level1) is True


def test_triple_nested_pg_exception(pg_handler):
    innermost = OperationalError("connection failed")
    level3 = AwsWrapperError("Level 3")
    level3.__cause__ = innermost
    level2 = AwsWrapperError("Level 2")
    level2.__cause__ = level3
    level1 = AwsWrapperError("Level 1")
    level1.__cause__ = level2

    assert pg_handler.is_network_exception(error=level1) is True


def test_nested_with_mixed_error_types_mysql(mysql_handler):
    innermost = DatabaseError(errno=2006, msg="MySQL server has gone away")
    middle = QueryTimeoutError("Timeout")
    middle.__cause__ = innermost
    outer = AwsWrapperError("Wrapper")
    outer.__cause__ = middle

    assert mysql_handler.is_network_exception(error=outer) is True


def test_nested_with_mixed_error_types_pg(pg_handler):
    innermost = OperationalError("consuming input failed")
    middle = AwsConnectError("Connect error")
    middle.__cause__ = innermost
    outer = AwsWrapperError("Wrapper")
    outer.__cause__ = middle

    assert pg_handler.is_network_exception(error=outer) is True


def test_nested_exception_stops_at_first_non_wrapper(mysql_handler):
    innermost = DatabaseError(errno=2006, msg="MySQL server has gone away")
    middle = DatabaseError(errno=1234, msg="Some error")
    middle.__cause__ = innermost
    outer = AwsWrapperError("Wrapper")
    outer.__cause__ = middle

    assert mysql_handler.is_network_exception(error=outer) is True


def test_nested_exception_with_none_cause(mysql_handler):
    error = AwsWrapperError("Wrapper with no cause")
    error.__cause__ = None

    assert mysql_handler.is_network_exception(error=error) is False


# Tests for is_login_exception with nested AwsWrapperError
def test_is_login_exception_with_nested_aws_wrapper_error_mysql(mysql_handler):
    class MockLoginError(Exception):
        def __init__(self):
            self.sqlstate = "28000"

    inner_error = MockLoginError()
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert mysql_handler.is_login_exception(error=wrapper_error) is True


def test_is_login_exception_with_deeply_nested_error_mysql(mysql_handler):
    class MockLoginError(Exception):
        def __init__(self):
            self.sqlstate = "28000"

    innermost_error = MockLoginError()
    middle_error = AwsWrapperError("Middle wrapper")
    middle_error.__cause__ = innermost_error
    outer_error = AwsWrapperError("Outer wrapper")
    outer_error.__cause__ = middle_error

    assert mysql_handler.is_login_exception(error=outer_error) is True


def test_is_login_exception_with_nested_non_login_error_mysql(mysql_handler):
    inner_error = DatabaseError(errno=1234, msg="Some other error")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert mysql_handler.is_login_exception(error=wrapper_error) is False


def test_is_login_exception_with_nested_aws_wrapper_error_pg(pg_handler):
    from psycopg.errors import InvalidPassword

    inner_error = InvalidPassword("password authentication failed")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert pg_handler.is_login_exception(error=wrapper_error) is True


def test_is_login_exception_with_deeply_nested_error_pg(pg_handler):
    from psycopg.errors import InvalidAuthorizationSpecification

    innermost_error = InvalidAuthorizationSpecification("PAM authentication failed")
    middle_error = AwsWrapperError("Middle wrapper")
    middle_error.__cause__ = innermost_error
    outer_error = AwsWrapperError("Outer wrapper")
    outer_error.__cause__ = middle_error

    assert pg_handler.is_login_exception(error=outer_error) is True


def test_is_login_exception_with_nested_non_login_error_pg(pg_handler):
    inner_error = OperationalError("some other error")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert pg_handler.is_login_exception(error=wrapper_error) is False


# Tests for is_read_only_connection_exception with nested AwsWrapperError
def test_is_read_only_exception_with_nested_aws_wrapper_error_mysql(mysql_handler):
    class MockReadOnlyError(Exception):
        def __init__(self):
            self.errno = "1836"
            self.msg = "Running in read-only mode"

    inner_error = MockReadOnlyError()
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert mysql_handler.is_read_only_connection_exception(error=wrapper_error) is True


def test_is_read_only_exception_with_deeply_nested_error_mysql(mysql_handler):
    class MockReadOnlyError(Exception):
        def __init__(self):
            self.errno = "1836"
            self.msg = "Running in read-only mode"

    innermost_error = MockReadOnlyError()
    middle_error = AwsWrapperError("Middle wrapper")
    middle_error.__cause__ = innermost_error
    outer_error = AwsWrapperError("Outer wrapper")
    outer_error.__cause__ = middle_error

    assert mysql_handler.is_read_only_connection_exception(error=outer_error) is True


def test_is_read_only_exception_with_nested_non_readonly_error_mysql(mysql_handler):
    inner_error = DatabaseError(errno=1234, msg="Some other error")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert mysql_handler.is_read_only_connection_exception(error=wrapper_error) is False


def test_is_read_only_exception_with_nested_aws_wrapper_error_pg(pg_handler):
    from psycopg.errors import ReadOnlySqlTransaction

    inner_error = ReadOnlySqlTransaction("cannot execute in a read-only transaction")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert pg_handler.is_read_only_connection_exception(error=wrapper_error) is True


def test_is_read_only_exception_with_deeply_nested_error_pg(pg_handler):
    from psycopg.errors import InternalError

    innermost_error = InternalError("cannot execute UPDATE in a read-only transaction")
    middle_error = AwsWrapperError("Middle wrapper")
    middle_error.__cause__ = innermost_error
    outer_error = AwsWrapperError("Outer wrapper")
    outer_error.__cause__ = middle_error

    assert pg_handler.is_read_only_connection_exception(error=outer_error) is True


def test_is_read_only_exception_with_nested_non_readonly_error_pg(pg_handler):
    inner_error = OperationalError("some other error")
    wrapper_error = AwsWrapperError("Wrapper error")
    wrapper_error.__cause__ = inner_error

    assert pg_handler.is_read_only_connection_exception(error=wrapper_error) is False


def test_circular_reference_mysql(mysql_handler):
    wrapper1 = AwsWrapperError("Wrapper 1")
    wrapper2 = AwsWrapperError("Wrapper 2")
    wrapper1.__cause__ = wrapper2
    wrapper2.__cause__ = wrapper1

    assert mysql_handler.is_network_exception(error=wrapper1) is False


def test_circular_reference_pg(pg_handler):
    wrapper1 = AwsWrapperError("Wrapper 1")
    wrapper2 = AwsWrapperError("Wrapper 2")
    wrapper1.__cause__ = wrapper2
    wrapper2.__cause__ = wrapper1

    assert pg_handler.is_network_exception(error=wrapper1) is False


def test_circular_reference_no_match_mysql(mysql_handler):
    inner_error = DatabaseError(errno=1234, msg="Some other error")
    wrapper1 = AwsWrapperError("Wrapper 1")
    wrapper1.__cause__ = inner_error
    wrapper2 = AwsWrapperError("Wrapper 2")
    wrapper2.__cause__ = wrapper1
    inner_error.__cause__ = wrapper2

    assert mysql_handler.is_network_exception(error=wrapper2) is False
