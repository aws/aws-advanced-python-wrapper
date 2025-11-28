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

from psycopg.errors import (ConnectionTimeout,
                            InvalidAuthorizationSpecification, InvalidPassword,
                            OperationalError)

from aws_advanced_python_wrapper.errors import QueryTimeoutError
from aws_advanced_python_wrapper.exception_handling import ExceptionHandler


class PgExceptionHandler(ExceptionHandler):
    _PASSWORD_AUTHENTICATION_FAILED_MSG = "password authentication failed"
    _PAM_AUTHENTICATION_FAILED_MSG = "PAM authentication failed"
    _CONNECTION_FAILED = "connection failed"
    _CONSUMING_INPUT_FAILED = "consuming input failed"
    _CONNECTION_SOCKET_CLOSED = "connection socket closed"

    _NETWORK_ERROR_MESSAGES: List[str] = [
        _CONNECTION_FAILED,
        _CONSUMING_INPUT_FAILED,
        _CONNECTION_SOCKET_CLOSED
    ]
    _ACCESS_ERROR_MESSAGES: List[str] = [
        _PASSWORD_AUTHENTICATION_FAILED_MSG,
        _PAM_AUTHENTICATION_FAILED_MSG
    ]
    _NETWORK_ERROR_CODES: List[str]
    _ACCESS_ERROR_CODES: List[str]

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, QueryTimeoutError) or isinstance(error, ConnectionTimeout):
            return True
        if sql_state is None:
            try:
                error_sql_state = getattr(error, "sqlstate")
                if error_sql_state is not None:
                    sql_state = error_sql_state
            except AttributeError:
                # getattr may throw an AttributeError if the error does not have a `sqlstate` attribute
                pass

        if sql_state is not None and sql_state in self._NETWORK_ERROR_CODES:
            return True

        if isinstance(error, OperationalError):
            if len(error.args) == 0:
                return False
            # Check the error message if this is a generic error
            error_msg: str = error.args[0]
            return any(msg in error_msg for msg in self._NETWORK_ERROR_MESSAGES)

        return False

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if error:
            if isinstance(error, InvalidAuthorizationSpecification) or isinstance(error, InvalidPassword):
                return True

            if sql_state is None and hasattr(error, "sqlstate") and error.sqlstate is not None:
                sql_state = error.sqlstate

            if sql_state is not None and sql_state in self._ACCESS_ERROR_CODES:
                return True

            if isinstance(error, OperationalError):
                if len(error.args) == 0:
                    return False

                # Check the error message if this is a generic error
                error_msg: str = error.args[0]
                if any(msg in error_msg for msg in self._ACCESS_ERROR_MESSAGES):
                    return True

        return False


class SingleAzPgExceptionHandler(PgExceptionHandler):
    _NETWORK_ERROR_CODES: List[str] = [
        "53",  # insufficient resources
        "57P01",  # admin shutdown
        "57P02",  # crash shutdown
        "57P03",  # cannot connect now
        "58",  # system error(backend)
        "08",  # connection error
        "99",  # unexpected error
        "F0",  # configuration file error(backend)
        "XX"  # internal error(backend)
    ]

    _ACCESS_ERROR_CODES: List[str] = [
        "28000",  # PAM authentication errors
        "28P01"
    ]


class MultiAzPgExceptionHandler(PgExceptionHandler):
    _NETWORK_ERROR_CODES: List[str] = [
        "28000",  # access denied during reboot, this should be considered a temporary failure
        "53",  # insufficient resources
        "57P01",  # admin shutdown
        "57P02",  # crash shutdown
        "57P03",  # cannot connect now
        "58",  # system error(backend)
        "08",  # connection error
        "99",  # unexpected error
        "F0",  # configuration file error(backend)
        "XX"  # internal error(backend)
    ]

    _ACCESS_ERROR_CODES: List[str] = ["28P01"]
