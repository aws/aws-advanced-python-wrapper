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

from typing import TYPE_CHECKING

import mysql

from aws_wrapper.errors import QueryTimeoutError

if TYPE_CHECKING:
    from aws_wrapper.dialect import Dialect

from typing import List, Optional, Protocol

from psycopg.errors import (ConnectionTimeout,
                            InvalidAuthorizationSpecification, InvalidPassword,
                            OperationalError)


class ExceptionHandler(Protocol):
    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        pass

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        pass


class PgExceptionHandler(ExceptionHandler):
    _NETWORK_ERRORS: List[str] = [
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

    _ACCESS_ERRORS: List[str] = [
        "28000",  # PAM authentication errors
        "28P01"
    ]

    _PASSWORD_AUTHENTICATION_FAILED_MSG = "password authentication failed"
    _PAM_AUTHENTICATION_FAILED_MSG = "PAM authentication failed"
    _CONNECTION_FAILED = "connection failed"

    def is_network_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if isinstance(error, QueryTimeoutError) or isinstance(error, ConnectionTimeout):
            return True
        if sql_state is None:
            error_sql_state = getattr(error, "sqlstate")
            if error_sql_state is not None:
                sql_state = error_sql_state

        if sql_state is not None and sql_state in self._NETWORK_ERRORS:
            return True

        if isinstance(error, OperationalError):
            if len(error.args) == 0:
                return False
            # Check the error message if this is a generic error
            error_msg: str = error.args[0]
            if self._CONNECTION_FAILED in error_msg:
                return True

        return False

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if error:
            if isinstance(error, InvalidAuthorizationSpecification) or isinstance(error, InvalidPassword):
                return True

            if sql_state is None and hasattr(error, "sqlstate") and error.sqlstate is not None:
                sql_state = error.sqlstate

            if sql_state is not None and sql_state in self._ACCESS_ERRORS:
                return True

            if isinstance(error, OperationalError):
                if len(error.args) == 0:
                    return False

                # Check the error message if this is a generic error
                error_msg: str = error.args[0]
                if self._PASSWORD_AUTHENTICATION_FAILED_MSG in error_msg \
                        or self._PAM_AUTHENTICATION_FAILED_MSG in error_msg:
                    return True

        return False


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

        if sql_state is None:
            if hasattr(error, "sqlstate"):
                error_sql_state = getattr(error, "sqlstate")
                if error_sql_state is not None:
                    sql_state = error_sql_state

        if sql_state is not None:
            if sql_state.startswith("08") or sql_state.startswith("HY"):
                # Connection exceptions may also be returned as a generic error
                # e.g. 2013 (HY000): Lost connection to MySQL server during query
                return True

        if isinstance(error, mysql.connector.errors.OperationalError):
            if error.errno in self._NETWORK_ERRORS:
                return True
            if error.msg is not None and self._UNAVAILABLE_CONNECTION in error.msg:
                return True

            if len(error.args) == 1:
                return self._UNAVAILABLE_CONNECTION in error.args[0]

        return False

    def is_login_exception(self, error: Optional[Exception] = None, sql_state: Optional[str] = None) -> bool:
        if sql_state is None:
            if hasattr(error, "sqlstate"):
                error_sql_state = getattr(error, "sqlstate")
                if error_sql_state is not None:
                    sql_state = error_sql_state

        if "28000" == sql_state:
            return True

        return False


class ExceptionManager:
    custom_handler: Optional[ExceptionHandler] = None

    @staticmethod
    def set_custom_handler(handler: ExceptionHandler):
        ExceptionManager.custom_handler = handler

    @staticmethod
    def reset_custom_handler():
        ExceptionManager.custom_handler = None

    def is_network_exception(self, dialect: Optional[Dialect], error: Optional[Exception] = None,
                             sql_state: Optional[str] = None) -> bool:
        handler = self._get_handler(dialect)
        if handler is not None:
            return handler.is_network_exception(error=error, sql_state=sql_state)
        return False

    def is_login_exception(self, dialect: Optional[Dialect], error: Optional[Exception] = None,
                           sql_state: Optional[str] = None) -> bool:
        handler = self._get_handler(dialect)
        if handler is not None:
            return handler.is_login_exception(error=error, sql_state=sql_state)
        return False

    def _get_handler(self, dialect: Optional[Dialect]) -> Optional[ExceptionHandler]:
        if dialect is None:
            return None
        return self.custom_handler if self.custom_handler else dialect.exception_handler
