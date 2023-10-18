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

from typing import TYPE_CHECKING, Callable, Dict, Optional, Protocol, Union

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.connection_provider import ConnectionProvider

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.utils import Utils

logger = Logger(__name__)


class DriverDialectProvider(Protocol):
    def get_dialect(self, conn_func: Callable, props: Properties) -> DriverDialect:
        ...

    def get_pool_connection_driver_dialect(
            self,
            connection_provider: ConnectionProvider,
            underlying_driver_dialect: DriverDialect,
            props: Properties) -> DriverDialect:
        ...


class DriverDialectManager(DriverDialectProvider):
    _custom_dialect: Optional[DriverDialect] = None
    known_dialects_by_code: Dict[str, str] = {
        DriverDialectCodes.PSYCOPG: "aws_advanced_python_wrapper.pg_driver_dialect.PgDriverDialect",
        DriverDialectCodes.MYSQL_CONNECTOR_PYTHON: "aws_advanced_python_wrapper.mysql_driver_dialect.MySQLDriverDialect",
        DriverDialectCodes.GENERIC: "aws_advanced_python_wrapper.generic_driver_dialect.GenericDriverDialect",
    }

    pool_connection_driver_dialect: Dict[str, str] = {
        "SqlAlchemyPooledConnectionProvider": "aws_advanced_python_wrapper.sqlalchemy_driver_dialect.SqlAlchemyDriverDialect"
    }

    @staticmethod
    def get_custom_dialect():
        return DriverDialectManager._custom_dialect

    @staticmethod
    def set_custom_dialect(dialect: DriverDialect):
        DriverDialectManager._custom_dialect = dialect

    @staticmethod
    def reset_custom_dialect():
        DriverDialectManager._custom_dialect = None

    def get_dialect(self, conn_func: Callable, props: Properties) -> DriverDialect:
        if self._custom_dialect is not None:
            if self._custom_dialect.is_dialect(conn_func):
                self._log_dialect("custom", self._custom_dialect)
                return self._custom_dialect
            else:
                logger.warning("DriverDialectManager.CustomDialectNotSupported")

        result: Optional[str]
        dialect_code: Optional[str] = WrapperProperties.DRIVER_DIALECT.get(props)
        if dialect_code:
            result = DriverDialectManager.known_dialects_by_code.get(dialect_code)
            if result is None:
                raise AwsWrapperError(Messages.get_formatted(
                    "DriverDialectManager.UnknownDialectCode",
                    dialect_code))
            self._log_dialect(dialect_code, result)
            dialect = Utils.initialize_class(result, props)
            if dialect is None:
                raise AwsWrapperError(Messages.get_formatted(
                    "DriverDialectManager.InitializationError",
                    dialect_code))
            return dialect

        for key, value in DriverDialectManager.known_dialects_by_code.items():
            dialect = Utils.initialize_class(value, props)
            if dialect is not None and dialect.is_dialect(conn_func):
                self._log_dialect(key, value)
                return dialect

        self._log_dialect(DriverDialectCodes.GENERIC, "generic")
        return DriverDialect(props)

    @staticmethod
    def _log_dialect(dialect_code: str, driver_dialect: Union[DriverDialect, str]):
        logger.debug(
            "DriverDialectManager.UseDialect",
            dialect_code,
            driver_dialect)

    def get_pool_connection_driver_dialect(
            self,
            connection_provider: ConnectionProvider,
            underlying_driver_dialect: DriverDialect,
            props: Properties) -> DriverDialect:
        provider_class: str = connection_provider.__class__.__name__
        pool_connection_driver_dialect = self.pool_connection_driver_dialect.get(provider_class)
        if pool_connection_driver_dialect is not None:
            dialect = Utils.initialize_class(pool_connection_driver_dialect, underlying_driver_dialect, props)
            if dialect is not None:
                return dialect
        return underlying_driver_dialect
