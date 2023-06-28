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

from abc import ABC, abstractmethod
from contextlib import closing
from enum import Enum, auto
from logging import getLogger
from typing import Dict, List, Optional, Protocol

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.pep249 import Connection
from aws_wrapper.utils.properties import Properties, WrapperProperties
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
    MYSQL = auto()
    POSTGRES = auto()
    MARIADB = auto()
    CUSTOM = auto()
    UNKNOWN = auto()


class TopologyAwareDatabaseCluster(ABC):
    _topology_query: str
    _node_id_query: str
    _is_reader_query: str

    @property
    def topology_query(self) -> str:
        return self._topology_query

    @property
    def node_id_query(self) -> str:
        return self._node_id_query

    @property
    def is_reader_query(self) -> str:
        return self._is_reader_query


class Dialect(ABC):

    @property
    @abstractmethod
    def default_port(self) -> int:
        ...

    @property
    @abstractmethod
    def host_alias_query(self) -> str:
        ...

    @property
    @abstractmethod
    def server_version_query(self) -> str:
        ...

    def is_dialect(self, conn: Connection) -> Optional[bool]:
        ...

    @property
    @abstractmethod
    def dialect_update_candidates(self) -> Optional[List[DialectCodes]]:
        ...


class DialectProvider(Protocol):

    def get_dialect(self, props: Properties):
        ...


class MySqlDialect(Dialect):

    def __init__(self):
        self._dialect_update_candidates = [DialectCodes.AURORA_MYSQL, DialectCodes.RDS_MYSQL]

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    @property
    def server_version_query(self) -> str:
        return "SHOW VARIABLES LIKE 'version_comment'"

    def is_dialect(self, conn: Connection) -> Optional[bool]:
        with closing(conn.cursor()) as aws_cursor:
            aws_cursor.execute(self.server_version_query)
            vals = ["mysql" in value.lower() for column, value in aws_cursor]
            return True in vals

    @property
    def dialect_update_candidates(self) -> Optional[List[DialectCodes]]:
        return self._dialect_update_candidates


class PgDialect(Dialect):

    @property
    def default_port(self) -> int:
        return 5432

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())"

    @property
    def server_version_query(self) -> str:
        return "SELECT 'version', VERSION()"

    def is_dialect(self, conn: Connection) -> Optional[bool]:
        with closing(conn.cursor()) as aws_cursor:
            aws_cursor.execute('SELECT 1 FROM pg_proc LIMIT 1')
            if aws_cursor.fetchone() is not None:
                return True
        return False

    @property
    def dialect_update_candidates(self) -> Optional[List[DialectCodes]]:
        return self.dialect_update_candidates


class MariaDbDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates = None

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    @property
    def server_version_query(self) -> str:
        return "SELECT VERSION()"

    def is_dialect(self, conn: Connection) -> Optional[bool]:
        with closing(conn.cursor()) as aws_cursor:
            aws_cursor.execute(self.server_version_query)
            vals = ["mariadb" in value.lower() for column, value in aws_cursor]
            return True in vals

    @property
    def dialect_update_candidates(self) -> Optional[List[DialectCodes]]:
        return self._dialect_update_candidates


class RdsMySqlDialect(MySqlDialect):
    def __init__(self) -> None:
        self._dialect_update_candidates = [DialectCodes.RDS_MYSQL]

    def is_dialect(self, conn: Connection) -> Optional[bool]:
        with closing(conn.cursor()) as aws_cursor:
            aws_cursor.execute(self.server_version_query)
            vals = ["source distribution" in value.lower() for column, value in aws_cursor]
            return True in vals

    @property
    def dialect_update_candidates(self) -> List[DialectCodes]:
        return self._dialect_update_candidates


class AuroraPgDialect(PgDialect, TopologyAwareDatabaseCluster):
    extension_sql: str = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils FROM pg_settings WHERE name='rds.extensions'"

    topology_sql: str = "SELECT 1 FROM aurora_replica_status() LIMIT 1"

    _topology_query = ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
                       + "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
                       + "FROM aurora_replica_status() "
                         "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' ")

    _node_id_query = "SELECT aurora_db_instance_identifier()"
    _is_reader_query = "SELECT pg_is_in_recovery()"

    def is_dialect(self, conn: Connection) -> Optional[bool]:
        if not PgDialect.is_dialect(self, conn):
            return False

        has_extensions: bool = False
        has_topology: bool = False

        with closing(conn.cursor()) as aws_cursor:
            aws_cursor.execute(self.extension_sql)
            row = aws_cursor.fetchone()
            if row and bool(row[0]):
                logger.debug("has_extensions: True")
                has_extensions = True

            aws_cursor.execute(self.topology_query)
            if aws_cursor.fetchone() is not None:
                logger.debug("has_topology: True")
                has_topology = True

        return has_extensions and has_topology


class UnknownDialect(Dialect):
    def __init__(self) -> None:
        self._dialect_update_candidates: Optional[List[DialectCodes]] = [DialectCodes.MYSQL,
                                                                         DialectCodes.PG,
                                                                         DialectCodes.MARIADB,
                                                                         DialectCodes.RDS_MYSQL,
                                                                         DialectCodes.RDS_PG,
                                                                         DialectCodes.AURORA_MYSQL,
                                                                         DialectCodes.AURORA_PG,
                                                                         DialectCodes.UNKNOWN]

    @property
    def default_port(self) -> int:
        return -1

    @property
    def host_alias_query(self) -> str:
        return ""

    @property
    def server_version_query(self) -> str:
        return ""

    def is_dialect(self, conn: Connection) -> bool:
        return False

    @property
    def dialect_update_candidates(self) -> Optional[List[DialectCodes]]:
        return self._dialect_update_candidates


class DialectManager(DialectProvider):

    def __init__(self, rds_helper: Optional[RdsUtils] = None, _custom_dialect: Optional[Dialect] = None) -> None:
        self.rds_helper: RdsUtils = rds_helper if rds_helper else RdsUtils()
        self._can_update: bool = False
        self._dialect: Optional[Dialect] = None
        self._custom_dialect: Optional[Dialect] = _custom_dialect if _custom_dialect else None
        self._dialect_code: Optional[DialectCodes] = None
        self._known_endpoint_dialects: dict = dict()
        self.known_dialects_by_code: Dict[DialectCodes, Dialect] = {DialectCodes.MYSQL: MySqlDialect(),
                                                                    DialectCodes.PG: PgDialect(),
                                                                    DialectCodes.MARIADB: MariaDbDialect(),
                                                                    DialectCodes.RDS_MYSQL: RdsMySqlDialect(),
                                                                    DialectCodes.RDS_PG: PgDialect(),
                                                                    DialectCodes.AURORA_MYSQL: MySqlDialect(),
                                                                    DialectCodes.AURORA_PG: PgDialect(),
                                                                    DialectCodes.UNKNOWN: UnknownDialect()}

    @property
    def custom_dialect(self):
        return self._custom_dialect

    @custom_dialect.setter
    def custom_dialect(self, dialect: Dialect):
        self._custom_dialect = dialect

    def reset_custom_dialect(self):
        self._custom_dialect = None

    def reset_endpoint_cache(self):
        self._known_endpoint_dialects.clear()

    def get_dialect(self, props: Properties) -> Optional[Dialect]:
        self._can_update = False
        self._dialect = None

        if self._custom_dialect is not None:
            self._dialect_code = DialectCodes.CUSTOM
            self._dialect = self._custom_dialect
            self.log_current_dialect()
            return self._dialect

        user_dialect_setting: Optional[str] = str(WrapperProperties.DIALECT.get(props))
        url = props["host"] if props.get("port") is None else props["host"] + ":" + props["port"]
        dialect_code: Optional[DialectCodes]
        if user_dialect_setting:
            dialect_code = DialectCodes[user_dialect_setting]
        else:
            dialect_code = self._known_endpoint_dialects.get(url)
        if dialect_code:
            user_dialect: Optional[Dialect] = self.known_dialects_by_code.get(dialect_code)
            if user_dialect:
                self._dialect_code = dialect_code
                self._dialect = user_dialect
                self.log_current_dialect()
                return user_dialect
            else:
                raise AwsWrapperError("Unknown dialect code " + str(dialect_code))

        host: str = props["host"]

        # temporary database_type variable
        database_type: DatabaseType = DatabaseType.POSTGRES

        type: Optional[RdsUrlType] = None
        if database_type is DatabaseType.MYSQL:
            type = self.rds_helper.identify_rds_type(host)
            if type.is_rds_cluster:
                self._dialect_code = DialectCodes.AURORA_MYSQL
                self._dialect = self.known_dialects_by_code.get(DialectCodes.AURORA_MYSQL)
                return self._dialect
            if type.is_rds:
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
            if type.is_rds_cluster:
                self._dialect_code = DialectCodes.AURORA_PG
                self._dialect = self.known_dialects_by_code.get(DialectCodes.AURORA_PG)
                return self._dialect
            if type.is_rds:
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

    def log_current_dialect(self):
        logger.debug("Current dialect "
                     + ", "
                     + "null" if self._dialect is None else str(self._dialect)
                     + "can_update: "
                     + str(self._can_update))
