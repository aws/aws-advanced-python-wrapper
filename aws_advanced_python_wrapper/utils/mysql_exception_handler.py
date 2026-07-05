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

from typing import List, Optional

from mysql.connector import DatabaseError, InterfaceError

from aws_advanced_python_wrapper.errors import (AwsConnectError,
                                                AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.exception_handling import ExceptionHandler


class MySQLExceptionHandler(ExceptionHandler):
    _PAM_AUTHENTICATION_FAILED_MSG = "PAM authentication failed"
    _UNAVAILABLE_CONNECTION = "MySQL Connection not available"

    _READ_ONLY_ERROR_MESSAGES: List[str] = [
        # ERROR 1290 (HY000): The MySQL server is running with the --read-only option so it cannot execute this statement
        "running with the --read-only option so it cannot execute this statement",
        # ERROR 1836 (HY000): Running in read-only mode
        "Running in read-only mode"
    ]

    _NETWORK_ERRORS: List[int] = [
        2001,  # Can't create UNIX socket
        2002,  # Can't connect to local MySQL server through socket
        2003,  # Can't connect to MySQL server
        2004,  # Can't create TCP/IP socket
        2006,  # MySQL server has gone away
        2012,  # Error in server handshake
        2013,  # unexpected error
        2026,  # SSL connection error
        2055,  # Lost connection to MySQL server
    ]

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, AwsConnectError) or isinstance(error, QueryTimeoutError):
            return True

        if isinstance(error, AwsWrapperError):
            return self._is_network_error(error.driver_error, sql_state)

        return self._is_network_error(error)

    def _is_network_error(self, error: Optional[BaseException] = None, sql_state: Optional[str] = None):
        if error is None:
            return False

        if isinstance(error, AwsConnectError) or isinstance(error, QueryTimeoutError):
            return True

        if isinstance(error, InterfaceError):
            if error.errno in self._NETWORK_ERRORS:
                return True

            if sql_state is None and error.sqlstate is not None:
                sql_state = error.sqlstate

        if sql_state is not None and (sql_state.startswith("08") or sql_state.startswith("HY")):
            # Connection exceptions may also be returned as a generic error
            # e.g. 2013 (HY000): Lost connection to MySQL server during query
            return True

        if isinstance(error, DatabaseError):
            if error.errno in self._NETWORK_ERRORS:
                return True
        # aiomysql raises pymysql errors, which are NOT mysql.connector
        # InterfaceError/DatabaseError and expose no ``.errno`` or
        # ``.sqlstate``; the MySQL client error code is the first ``args``
        # element instead (e.g. OperationalError(2013, 'Lost connection to
        # MySQL server during query')). Match that shape so async failover
        # triggers on connection loss. Additive: mysql.connector network
        # errors are already caught above, so this only widens coverage to
        # the pymysql shape and never changes the sync verdict.
        args = getattr(error, "args", None)
        if args and isinstance(args[0], int) and args[0] in self._NETWORK_ERRORS:
            return True
        # pymysql InterfaceError(0, 'Not connected'): aiomysql tears the
        # connection down locally when its reader task sees EOF (observed
        # during long Aurora failover outages), and every later operation
        # raises this shape instead of a 2xxx client error code. It is
        # definitionally a lost-connection condition; without this it escaped
        # the async wrapper raw instead of triggering failover. Additive and
        # narrowly matched, like the pymysql block above.
        if (args and len(args) >= 2 and args[0] == 0
                and isinstance(args[1], str) and "Not connected" in args[1]):
            return True
        # ... and aiomysql's own single-string variant of the same condition:
        # InterfaceError("(0, 'Not connected')") -- the tuple's repr embedded
        # in ONE string arg (aiomysql/connection.py). mysql.connector cannot
        # produce this either: its errors always carry (errno, msg, sqlstate)
        # 3-tuples with errno normalized to -1 when unset.
        if (args and len(args) == 1 and isinstance(args[0], str)
                and args[0].lstrip().startswith("(0,")
                and "Not connected" in args[0]):
            return True
        if hasattr(error, 'msg') and error.msg is not None and self._UNAVAILABLE_CONNECTION in error.msg:
            return True

        if (hasattr(error, 'args') and len(error.args) == 1
                and isinstance(error.args[0], str)):
            # Guard isinstance: a pymysql error can carry a single INT arg
            # (e.g. OperationalError(2013) with no message); ``str in int``
            # would raise TypeError into the failover classifier. The int
            # network-error case is already handled by the errno branch above.
            return self._UNAVAILABLE_CONNECTION in error.args[0]

        return False

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, AwsWrapperError):
            return self._is_login_error(error.driver_error, sql_state)

        return self._is_login_error(error, sql_state)

    def _is_login_error(self, error: Optional[BaseException] = None, sql_state: Optional[str] = None) -> bool:
        if error is None:
            return False

        if sql_state is None:
            sql_state = getattr(error, "sqlstate", None)

        if sql_state == "28000":
            return True

        return False

    def is_read_only_connection_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, AwsWrapperError):
            return self._is_read_only_error(error.driver_error, sql_state)

        return self._is_read_only_error(error, sql_state)

    def _is_read_only_error(self, error: Optional[BaseException] = None, sql_state: Optional[str] = None) -> bool:
        if error is None:
            return False

        # mysql.connector exposes the code as ``.errno`` and the text as
        # ``.msg``; aiomysql's pymysql errors carry ``(errno, message)`` in
        # ``args`` with neither attribute. Read both shapes so read-only
        # detection (used by the STRICT_WRITER failover escape hatch) works on
        # the async path too.
        errno = getattr(error, "errno", None)
        if not isinstance(errno, int):
            args = getattr(error, "args", None)
            if args and isinstance(args[0], int):
                errno = args[0]
        if errno == 1836:  # ERROR 1836 (HY000): Running in read-only mode
            return True

        error_msg = getattr(error, "msg", None)
        if error_msg is None:
            error_msg = str(error)
        if any(msg in error_msg for msg in self._READ_ONLY_ERROR_MESSAGES):
            return True

        return False
