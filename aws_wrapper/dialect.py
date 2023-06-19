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

import time

from logging import getLogger
from typing import Protocol

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.pep249 import Connection
from aws_wrapper.utils.properties import Properties
from aws_wrapper.utils.rds_url_type import RdsUrlType
from aws_wrapper.utils.rdsutils import RdsUtils

from enum import Enum, auto

logger = getLogger(__name__)


class DialectCodes:

    _AURORA_MYSQL: str = "aurora-mysql"
    _RDS_MYSQL: str = "rds-mysql"
    _MYSQL: str = "mysql"

    _AURORA_PG: str = "aurora-pg"
    _RDS_PG: str = "rds-pg"
    _PG: str = "pg"

    _MARIADB = "mariadb"

    _UNKNOWN = "unknown"
    _CUSTOM = "custom"


class Dialect(Protocol):

    def get_default_port():
        ...

    def get_host_alias_query():
        ...

    def get_server_version_query():
        ...

    def is_dialect(self, conn: Connection):
        ...

    def append_properties_to_url():
        ...

    def get_dialect_update_candidates():
        ...


class DialectProvider(Protocol):

    def get_dialect(self, url: str, props: Properties):
        ...


class MySqlDialect(Dialect):

    def __init__(self):
        self._dialect_update_candidates = [DialectCodes._AURORA_MYSQL, DialectCodes._RDS_MYSQL]

    def get_host_alias_query():
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    def get_server_version_query():
        return "SHOW VARIABLES LIKE 'version_comment'"

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute(self.get_server_version_query())
        for record in cursor:
            for column in record:
                if "mysql" in column.lower():
                    return True
        return False

    def append_properties_to_url(self):
        return None

    def get_dialect_update_candidates(self):
        return self._dialect_update_candidates


class PgDialect(Dialect):

    def get_default_port():
        return 5432

    def get_host_alias_query():
        return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())"

    def get_server_version_query():
        return "SELECT 'version', VERSION()"

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM pg_proc LIMIT 1')
        if cursor.fetchone is not None:
            return True
        return False

    def append_properties_to_url(self):
        return None

    def get_dialect_update_candidates(self):
        return None


class MariaDbDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates = [DialectCodes._AURORA_PG, DialectCodes._RDS_PG]

    def get_default_port():
        return 3306

    def get_host_alias_query():
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    def get_server_version_query():
        return "SELECT VERSION()"

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute(self.get_server_version_query())
        for record in cursor:
            if "mariadb" in record[1].lower():
                return True
        return False

    def append_properties_to_url(self):
        return None

    def get_dialect_update_candidates(self):
        return self._dialect_update_candidates


class RdsMysqlDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates = [DialectCodes._AURORA_MYSQL]

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute(self.get_server_version_query())
        for record in cursor:
            for column in record:
                if "Source distribution" in column.lower():
                    return True
        return False

    def get_dialect_update_candidates(self):
        return self._dialect_update_candidates


class UnknownDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates = [DialectCodes._AURORA_PG,
                                           DialectCodes._AURORA_MYSQL,
                                           DialectCodes._RDS_PG,
                                           DialectCodes._RDS_MYSQL,
                                           DialectCodes._PG,
                                           DialectCodes._MYSQL,
                                           DialectCodes._MARIADB]

    def get_host_alias_query():
        return None

    def get_server_version_query():
        return None

    def is_dialect(self, conn: Connection):
        return False

    def append_properties_to_url():
        return None

    def get_dialect_update_candidates(self):
        return self._dialect_update_candidates


class DatabaseType(Enum):
    POSTGRES = auto()
    MYSQL = auto()
    MARIADB = auto()
    CUSTOM = auto()
    UNKNOWN = auto()


class DialectManager(DialectProvider):

    def __init__(self):
        self.rds_helper: RdsUtils = RdsUtils()
        self._can_update: bool = False
        self._dialect: Dialect = None
        self._custom_dialect: Dialect = None
        self._dialect_code: str = ""

        self.known_dialects_by_code = {DialectCodes._MYSQL: MySqlDialect(),
                                       DialectCodes._PG: PgDialect()}
        self._ENDPOINT_CACHE_EXPIRATION: time.time = 30
        self._known_endpoint_dialects: dict = {}

    def set_custom_dialect(self, dialect: Dialect):
        self._custom_dialect = dialect

    def reset_custom_dialect(self):
        self._custom_dialect = None

    def reset_endpoint_cache(self):
        self._known_endpoint_dialects.clear

    def get_database_type(self):
        return DatabaseType.POSTGRES

    def get_dialect(self, url: str, props: Properties):
        self._can_update = False
        self._dialect = None

        if self._custom_dialect is not None:
            self._dialect_code = DialectCodes._CUSTOM
            self._dialect = self._custom_dialect
            self.log_current_dialect()
            return self._dialect

        dialect_code: str = self._known_endpoint_dialects.get(url)

        if dialect_code is not None:
            user_dialect: Dialect = self.known_dialects_by_code.get(dialect_code)
            if user_dialect is None:
                self._dialect_code = dialect_code
                self._dialect = user_dialect
                self.log_current_dialect()
                return user_dialect
            else:
                raise AwsWrapperError("Unknown dialect code " + dialect_code)

        host: str = props["host"]

        database_type = self.get_database_type()

        if database_type is DatabaseType.MYSQL:
            type: RdsUrlType = self.rds_helper.identify_rds_type(host)
            if type.is_rds_cluster() is True:
                self._dialect_code = DialectCodes._AURORA_MYSQL
                self.dialect = self.known_dialects_by_code.get(DialectCodes._AURORA_MYSQL)
                return self.dialect
            if type.is_rds is True:
                self._can_update = True
                self._dialect_code = DialectCodes._RDS_MYSQL
                self.dialect = self.known_dialects_by_code.get(DialectCodes._RDS_MYSQL)
                self.log_current_dialect()
                return self.dialect
            self._can_update = True
            self._dialect_code = DialectCodes._MYSQL
            self.dialect = self.known_dialects_by_code.get(DialectCodes._MYSQL)
            self.log_current_dialect()
            return self.dialect

        if database_type is DatabaseType.POSTGRES:
            type: RdsUrlType = self.rds_helper.identify_rds_type(host)
            if type.is_rds_cluster() is True:
                self._dialect_code = DialectCodes._AURORA_PG
                self.dialect = self.known_dialects_by_code.get(DialectCodes._AURORA_PG)
                return self.dialect
            if type.is_rds is True:
                self._can_update = True
                self._dialect_code = DialectCodes._RDS_PG
                self.dialect = self.known_dialects_by_code.get(DialectCodes._RDS_PG)
                self.log_current_dialect()
                return self.dialect
            self._can_update = True
            self._dialect_code = DialectCodes._PG
            self.dialect = self.known_dialects_by_code.get(DialectCodes._PG)
            self.log_current_dialect()
            return self.dialect

        if database_type is DatabaseType.MARIADB:
            self._can_update = True
            self._dialect_code = DialectCodes._MARIADB
            self.dialect = self.known_dialects_by_code.get(DialectCodes._MARIADB)
            self.log_current_dialect()
            return self.dialect

        self._can_update = True
        self._dialect_code = DialectCodes._UNKNOWN
        self.dialect = self.known_dialects_by_code.get(DialectCodes._UNKNOWN)
        self.log_current_dialect()
        return self.dialect

    def log_current_dialect(self):
        logger.info("Current dialect " + self._dialect_code + ", " + self.dialect + ", canUpdate: " + self._can_update)
