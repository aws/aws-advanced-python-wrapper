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

from aws_advanced_python_wrapper.errors import QueryTimeoutError
from aws_advanced_python_wrapper.exception_handling import ExceptionHandler


class MySQLExceptionHandler(ExceptionHandler):
    _PAM_AUTHENTICATION_FAILED_MSG = "PAM authentication failed"
    _UNAVAILABLE_CONNECTION = "MySQL Connection not available"

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
        if isinstance(error, QueryTimeoutError):
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
            if error.msg is not None and self._UNAVAILABLE_CONNECTION in error.msg:
                return True

            if len(error.args) == 1:
                return self._UNAVAILABLE_CONNECTION in error.args[0]

        return False

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if sql_state is None and hasattr(error, "sqlstate"):
            sql_state = getattr(error, "sqlstate")

        if "28000" == sql_state:
            return True

        return False
