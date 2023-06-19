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

from enum import Enum, auto
from logging import getLogger
from typing import List, Optional, Protocol, Set, Union
import typing

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.pep249 import Connection
from aws_wrapper.utils.properties import Properties
from aws_wrapper.utils.rds_url_type import RdsUrlType
from aws_wrapper.utils.rdsutils import RdsUtils

logger = getLogger(__name__)


class DialectCodes(Enum):

    AURORA_MYSQL: str = "aurora-mysql"
    RDS_MYSQL: str = "rds-mysql"
    MYSQL: str = "mysql"

    AURORA_PG: str = "aurora-pg"
    RDS_PG: str = "rds-pg"
    PG: str = "pg"

    MARIADB: str = "mariadb"

    UNKNOWN: str = "unknown"
    CUSTOM: str = "custom"


class DatabaseType(Enum):
    POSTGRES = auto()
    MYSQL = auto()
    MARIADB = auto()
    CUSTOM = auto()
    UNKNOWN = auto()


class TopologyAwareDatabaseCluster(Protocol):

    def get_topology_query(self) -> str:
        ...

    def get_node_id_query(self) -> str:
        ...

    def get_is_reader_query(self) -> str:
        ...


class Dialect(Protocol):

    def get_default_port(self) -> int:
        ...

    def get_host_alias_query(self) -> str:
        ...

    def get_server_version_query(self) -> str:
        ...

    def is_dialect(self, conn: Connection) -> bool:
        ...

    def append_properties_to_url(self) -> Set[str]:
        ...

    def get_dialect_update_candidates(self) -> List[DialectCodes]:
        ...


class DialectProvider(Protocol):

    def get_dialect(self, url: str, props: Properties):
        ...


class MySqlDialect(Dialect):

    def __init__(self) -> None:
        self._dialect_update_candidates = [DialectCodes.AURORA_MYSQL, DialectCodes.RDS_MYSQL]

    def get_default_port(self) -> int:
        return 3306

    def get_host_alias_query(self) -> str:
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    def get_server_version_query(self) -> str:
        return "SHOW VARIABLES LIKE 'version_comment'"

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute(self.get_server_version_query())
        for record in cursor:
            for column in record:
                if "mysql" in column.lower():
                    return True
        return False

    def append_properties_to_url(self) -> Set[str]:
        return set()

    def get_dialect_update_candidates(self) -> List[DialectCodes]:
        return self._dialect_update_candidates


class PgDialect(Dialect):

    def get_default_port(self) -> int:
        return 5432

    def get_host_alias_query(self) -> str:
        return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())"

    def get_server_version_query(self) -> str:
        return "SELECT 'version', VERSION()"

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM pg_proc LIMIT 1')
        if cursor.fetchone is not None:
            return True
        return False

    def append_properties_to_url(self) -> Set[str]:
        return set()

    def get_dialect_update_candidates(self) -> List[DialectCodes]:
        return list()


class MariaDbDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates = [DialectCodes.AURORA_PG, DialectCodes.RDS_PG]

    def get_default_port(self) -> int:
        return 3306

    def get_host_alias_query(self) -> str:
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    def get_server_version_query(self) -> str:
        return "SELECT VERSION()"

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute(self.get_server_version_query())
        for record in cursor:
            if "mariadb" in record[1].lower():
                return True
        return False

    def append_properties_to_url(self) -> Set[str]:
        return set()

    def get_dialect_update_candidates(self) -> List[DialectCodes]:
        return self._dialect_update_candidates


class RdsMysqlDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates = [DialectCodes.AURORA_MYSQL]

    def is_dialect(self, conn: Connection) -> bool:
        cursor = conn.cursor()
        cursor.execute(self.get_server_version_query())
        for record in cursor:
            for column in record:
                if "Source distribution" in column.lower():
                    return True
        return False

    def get_dialect_update_candidates(self) -> List[DialectCodes]:
        return self._dialect_update_candidates


class UnknownDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates: List[DialectCodes] = [DialectCodes.AURORA_PG,
                                                               DialectCodes.AURORA_MYSQL,
                                                               DialectCodes.RDS_PG,
                                                               DialectCodes.RDS_MYSQL,
                                                               DialectCodes.PG,
                                                               DialectCodes.MYSQL,
                                                               DialectCodes.MARIADB]

    def get_host_alias_query(self) -> str:
        return ""

    def get_server_version_query(self) -> str:
        return ""

    def is_dialect(self, conn: Connection) -> bool:
        return False

    def append_properties_to_url(self) -> Set[str]:
        return set()

    def get_dialect_update_candidates(self) -> List[DialectCodes]:
        return self._dialect_update_candidates


class DialectManager(DialectProvider):

    def __init__(self) -> None:
        self.rds_helper: RdsUtils = RdsUtils()
        self._can_update: bool = False
        self._dialect: Union[Dialect, None] = None
        self._custom_dialect: Union[Dialect, None] = None
        self._dialect_code: Union[DialectCodes, None] = None

        self.known_dialects_by_code = {DialectCodes.MYSQL: MySqlDialect(),
                                       DialectCodes.PG: PgDialect()}
        self._known_endpoint_dialects: dict = dict()

    def set_custom_dialect(self, dialect: Dialect):
        self._custom_dialect = dialect

    def reset_custom_dialect(self):
        self._custom_dialect = None

    def reset_endpoint_cache(self):
        self._known_endpoint_dialects.clear

    def get_database_type(self) -> DatabaseType:
        return DatabaseType.POSTGRES

    def get_dialect(self, url: str, props: Properties) -> Union[Dialect, None]:
        self._can_update = False
        self._dialect = None

        if self._custom_dialect is not None:
            self._dialect_code = DialectCodes.CUSTOM
            self._dialect = self._custom_dialect
            self.log_current_dialect()
            return self._dialect

        dialect_code: Optional[DialectCodes] = self._known_endpoint_dialects.get(url)

        if dialect_code is not None:
            user_dialect: Optional[Dialect] = self.known_dialects_by_code.get(dialect_code)
            if user_dialect is None:
                self._dialect_code = dialect_code
                self._dialect = user_dialect
                self.log_current_dialect()
                return user_dialect
            else:
                raise AwsWrapperError("Unknown dialect code ")

        host: str = props["host"]

        database_type = self.get_database_type()
        type: Optional[RdsUrlType] = None
        if database_type is DatabaseType.MYSQL:
            type = self.rds_helper.identify_rds_type(host)
            if type.is_rds_cluster is True:
                self._dialect_code = DialectCodes.AURORA_MYSQL
                self._dialect = self.known_dialects_by_code.get(DialectCodes.AURORA_MYSQL)
                return self._dialect
            if type.is_rds is True:
                self._can_update = True
                self._dialect_code = DialectCodes.RDS_MYSQL
                self._dialect = self.known_dialects_by_code.get(DialectCodes.RDS_MYSQL)
                self.log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCodes.MYSQL
            self._dialect = self.known_dialects_by_code.get(DialectCodes.MYSQL)
            self.log_current_dialect()
            return self._dialect

        if database_type is DatabaseType.POSTGRES:
            type = self.rds_helper.identify_rds_type(host)
            if type.is_rds_cluster is True:
                self._dialect_code = DialectCodes.AURORA_PG
                self._dialect = self.known_dialects_by_code.get(DialectCodes.AURORA_PG)
                return self._dialect
            if type.is_rds is True:
                self._can_update = True
                self._dialect_code = DialectCodes.RDS_PG
                self._dialect = self.known_dialects_by_code.get(DialectCodes.RDS_PG)
                self.log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCodes.PG
            self._dialect = self.known_dialects_by_code.get(DialectCodes.PG)
            self.log_current_dialect()
            return self._dialect

        if database_type is DatabaseType.MARIADB:
            self._can_update = True
            self._dialect_code = DialectCodes.MARIADB
            self._dialect = self.known_dialects_by_code.get(DialectCodes.MARIADB)
            self.log_current_dialect()
            return self._dialect

        self._can_update = True
        self._dialect_code = DialectCodes.UNKNOWN
        self._dialect = self.known_dialects_by_code.get(DialectCodes.UNKNOWN)
        self.log_current_dialect()
        return self._dialect

    def log_current_dialect(self) -> None:
        logger.debug("Current dialect ")
