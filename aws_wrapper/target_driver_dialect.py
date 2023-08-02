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

from logging import getLogger
from typing import Protocol, Dict, Callable, Optional
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_wrapper.GenericDriverDialect import TargetDriverDialect

from aws_wrapper.GenericDriverDialect import GenericTargetDriverDialect
from aws_wrapper.MariaDBTargetDriverDialect import MariaDBTargetDriverDialect
from aws_wrapper.MySQLTargetDriverDialect import MySQLTargetDriverDialect
from aws_wrapper.PgTargetDriverDialect import PgTargetDriverDialect

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, WrapperProperties

logger = getLogger(__name__)


class TargetDriverDialectCodes:
    PSYCOPG = "psycopg"
    MYSQL_CONNECTOR_PYTHON = "mysql-connector-python"
    MARIADB_CONNECTOR_PYTHON = "mariadb-connector-python"
    GENERIC = "generic"


class TargetDriverDialectProvider(Protocol):
    def get_dialect(self, conn: Callable, props: Properties) -> TargetDriverDialect:
        ...


class TargetDriverDialectManager(TargetDriverDialectProvider):
    _custom_dialect: Optional[TargetDriverDialect] = None
    known_dialects_by_code: Dict[str, TargetDriverDialect] = {
        TargetDriverDialectCodes.PSYCOPG: PgTargetDriverDialect(),
        TargetDriverDialectCodes.MYSQL_CONNECTOR_PYTHON: MySQLTargetDriverDialect(),
        TargetDriverDialectCodes.MARIADB_CONNECTOR_PYTHON: MariaDBTargetDriverDialect(),
        TargetDriverDialectCodes.GENERIC: GenericTargetDriverDialect()
    }

    @staticmethod
    def set_custom_dialect(target_driver_dialect: TargetDriverDialect):
        TargetDriverDialectManager._custom_dialect = target_driver_dialect

    def get_dialect(self, conn: Callable, props: Properties) -> TargetDriverDialect:
        if self._custom_dialect is not None:
            if self._custom_dialect.is_dialect(conn):
                self._log_dialect("custom", self._custom_dialect)
                return self._custom_dialect
        logger.warning(Messages.get("TargetDriverDialectManager.CustomDialectNotSupported"))
        result: Optional[TargetDriverDialect]

        dialect_code: Optional[str] = WrapperProperties.TARGET_DRIVER_DIALECT.get(props)

        if dialect_code:
            result = TargetDriverDialectManager.known_dialects_by_code.get(dialect_code)
            if result is None:
                raise AwsWrapperError(Messages.get_formatted(
                    "TargetDriverDialectManager.UnknownDialectCode",
                    dialect_code))
            self._log_dialect(dialect_code, result)
            return result

        for key, value in TargetDriverDialectManager.known_dialects_by_code.items():
            if value.is_dialect(conn):
                self._log_dialect(key, value)
                return value

        result = GenericTargetDriverDialect()
        self._log_dialect(TargetDriverDialectCodes.GENERIC, result)
        return result

    @staticmethod
    def _log_dialect(dialect_code: str, target_driver_dialect: TargetDriverDialect):
        logger.debug(Messages.get_formatted(
            "TargetDriverDialectManager.UseDialect",
            dialect_code,
            target_driver_dialect))
