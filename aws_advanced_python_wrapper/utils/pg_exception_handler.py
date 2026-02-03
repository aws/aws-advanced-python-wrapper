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

from psycopg.errors import (ConnectionTimeout, InternalError,
                            InvalidAuthorizationSpecification, InvalidPassword,
                            OperationalError, ReadOnlySqlTransaction)

from aws_advanced_python_wrapper.errors import (AwsConnectError,
                                                AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.exception_handling import ExceptionHandler


class PgExceptionHandler(ExceptionHandler):
    _PASSWORD_AUTHENTICATION_FAILED_MSG = "password authentication failed"
    _PAM_AUTHENTICATION_FAILED_MSG = "PAM authentication failed"
    _CONNECTION_FAILED = "connection failed"
    _CONSUMING_INPUT_FAILED = "consuming input failed"
    _CONNECTION_SOCKET_CLOSED = "connection socket closed"
    _CONNECTION_CLOSED = "the connection is closed"

    _NETWORK_ERROR_MESSAGES: List[str] = [
        _CONNECTION_FAILED,
        _CONSUMING_INPUT_FAILED,
        _CONNECTION_SOCKET_CLOSED,
        _CONNECTION_CLOSED,
    ]
    _ACCESS_ERROR_MESSAGES: List[str] = [
        _PASSWORD_AUTHENTICATION_FAILED_MSG,
        _PAM_AUTHENTICATION_FAILED_MSG
    ]
    # ERROR: cannot execute {} in a read-only transaction
    _READ_ONLY_ERROR_MSG: str = "in a read-only transaction"
    _NETWORK_ERROR_CODES: List[str]
    _ACCESS_ERROR_CODES: List[str]
    _READ_ONLY_ERROR_CODE: str = "25006"  # read only sql transaction

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, (AwsConnectError, QueryTimeoutError)):
            return True

        if isinstance(error, AwsWrapperError):
            return self._is_network_error(error.driver_error, sql_state)

        return self._is_network_error(error, sql_state)

    def _is_network_error(self, error: Optional[BaseException], sql_state: Optional[str] = None):
        if error is None:
            return False

        if isinstance(error, (AwsConnectError, QueryTimeoutError, ConnectionTimeout)):
            return True

        if sql_state is None:
            sql_state = getattr(error, "sqlstate", None)

        if sql_state is not None and sql_state in self._NETWORK_ERROR_CODES:
            return True

        if isinstance(error, OperationalError):
            if len(error.args) == 0:
                return False
            # Check the error message if this is a generic error
            error_msg: str = error.args[0]
            return any(error_msg.startswith(msg) for msg in self._NETWORK_ERROR_MESSAGES)

        return False

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, AwsWrapperError):
            return self._is_login_error(error.driver_error, sql_state)

        return self._is_login_error(error, sql_state)

    def _is_login_error(self, error: Optional[BaseException] = None, sql_state: Optional[str] = None) -> bool:
        if error is None:
            return False

        if isinstance(error, (InvalidAuthorizationSpecification, InvalidPassword)):
            return True

        if sql_state is None:
            sql_state = getattr(error, "sqlstate", None)

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

    def is_read_only_connection_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, AwsWrapperError):
            return self._is_read_only_error(error.driver_error, sql_state)
        return self._is_read_only_error(error, sql_state)

    def _is_read_only_error(self, error: Optional[BaseException] = None, sql_state: Optional[str] = None) -> bool:
        if error is None:
            return False

        if isinstance(error, ReadOnlySqlTransaction):
            return True

        if sql_state is None:
            sql_state = getattr(error, "sqlstate", None)

        if sql_state is not None and sql_state == self._READ_ONLY_ERROR_CODE:
            return True

        if isinstance(error, InternalError):
            if len(error.args) == 0:
                return False

            # Check the error message
            error_msg: str = error.args[0]
            if self._READ_ONLY_ERROR_MSG in error_msg:
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
