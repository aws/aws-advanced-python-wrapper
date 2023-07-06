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

from abc import abstractmethod
from contextlib import closing
from enum import Enum, auto
from logging import getLogger
from typing import Dict, FrozenSet, Optional, Protocol, runtime_checkable

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.pep249 import Connection, Error
from aws_wrapper.utils.properties import Properties, WrapperProperties
from aws_wrapper.utils.rdsutils import RdsUtils
from .utils.messages import Messages

logger = getLogger(__name__)


class DialectCodes(Enum):

    AURORA_MYSQL: str = "aurora-mysql"
    RDS_MYSQL: str = "rds-mysql"
    MYSQL: str = "mysql"

    AURORA_PG: str = "aurora-pg"
    RDS_PG: str = "rds-pg"
    PG: str = "pg"

    MARIADB: str = "mariadb"

    CUSTOM: str = "custom"
    UNKNOWN: str = "unknown"


class DatabaseType(Enum):
    MYSQL = auto()
    POSTGRES = auto()
    MARIADB = auto()
    CUSTOM = auto()


@runtime_checkable
class TopologyAwareDatabaseDialect(Protocol):
    _topology_query: str
    _host_id_query: str
    _is_reader_query: str

    @property
    def topology_query(self) -> str:
        return self._topology_query

    @property
    def host_id_query(self) -> str:
        return self._host_id_query

    @property
    def is_reader_query(self) -> str:
        return self._is_reader_query


class Dialect(Protocol):

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

    @abstractmethod
    def is_dialect(self, conn: Connection) -> bool:
        ...

    @property
    @abstractmethod
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        ...


class DialectProvider(Protocol):

    def get_dialect(self, props: Properties):
        ...


class MysqlDialect(Dialect):
    _DIALECT_UPDATE_CANDIDATES = frozenset({DialectCodes.AURORA_MYSQL, DialectCodes.RDS_MYSQL})

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    @property
    def server_version_query(self) -> str:
        return "SHOW VARIABLES LIKE 'version_comment'"

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self.server_version_query)
                for record in aws_cursor:
                    for column_value in record:
                        if "mysql" in column_value.lower():
                            return True
        except Error:
            pass

        return False

    @property
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        return MysqlDialect._DIALECT_UPDATE_CANDIDATES


class PgDialect(Dialect):
    _DIALECT_UPDATE_CANDIDATES = frozenset({DialectCodes.AURORA_PG, DialectCodes.RDS_PG})

    @property
    def default_port(self) -> int:
        return 5432

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())"

    @property
    def server_version_query(self) -> str:
        return "SELECT 'version', VERSION()"

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute('SELECT 1 FROM pg_proc LIMIT 1')
                if aws_cursor.fetchone() is not None:
                    return True
        except Error:
            pass

        return False

    @property
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        return PgDialect._DIALECT_UPDATE_CANDIDATES


class MariaDbDialect(Dialect):
    _DIALECT_UPDATE_CANDIDATES = None

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    @property
    def server_version_query(self) -> str:
        return "SELECT VERSION()"

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self.server_version_query)
                for record in aws_cursor:
                    if "mariadb" in record[0].lower():
                        return True
        except Error:
            pass

        return False

    @property
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        return MariaDbDialect._DIALECT_UPDATE_CANDIDATES


class RdsMysqlDialect(MysqlDialect):
    _DIALECT_UPDATE_CANDIDATES = frozenset({DialectCodes.AURORA_MYSQL})

    def is_dialect(self, conn: Connection) -> bool:
        if not super().is_dialect(conn):
            return False

        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self.server_version_query)
                for record in aws_cursor:
                    for column_value in record:
                        if "source distribution" in column_value.lower():
                            return True
        except Error:
            pass

        return False

    @property
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        return RdsMysqlDialect._DIALECT_UPDATE_CANDIDATES


class RdsPgDialect(PgDialect):
    _extensions_sql: str = ("SELECT (setting LIKE '%rds_tools%') AS rds_tools, "
                            "(setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
                            "FROM pg_settings "
                            "WHERE name='rds.extensions'")

    _DIALECT_UPDATE_CANDIDATES = frozenset({DialectCodes.AURORA_PG})

    def is_dialect(self, conn: Connection) -> bool:

        if not super().is_dialect(conn):
            return False

        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self._extensions_sql)
                for row in aws_cursor:
                    rds_tools = bool(row[0])
                    aurora_utils = bool(row[1])
                    logger.debug(f"rds_tools: {rds_tools}, aurora_utils: {aurora_utils}")
                    if rds_tools and not aurora_utils:
                        return True

        except Error:
            pass
        return False

    @property
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        return RdsPgDialect._DIALECT_UPDATE_CANDIDATES


class AuroraMysqlDialect(MysqlDialect, TopologyAwareDatabaseDialect):
    _topology_query: str = ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
                            "CPU, REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP "
                            "FROM information_schema.replica_host_status "
                            "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 "
                            "OR SESSION_ID = 'MASTER_SESSION_ID' ")

    _host_id_query: str = "SELECT @@aurora_server_id"

    _is_reader_query: str = "SELECT @@innodb_read_only"

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute("SHOW VARIABLES LIKE 'aurora_version'")
                # If variable with such name is presented then it means it's an Aurora cluster
                if aws_cursor.fetchone() is not None:
                    return True
        except Error:
            pass

        return False

    @property
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        return None


class AuroraPgDialect(PgDialect, TopologyAwareDatabaseDialect):
    _extensions_sql: str = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils " \
                           "FROM pg_settings WHERE name='rds.extensions'"

    _has_topology_sql: str = "SELECT 1 FROM aurora_replica_status() LIMIT 1"

    _topology_query = ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
                       "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
                       "FROM aurora_replica_status() "
                       "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 "
                       "OR SESSION_ID = 'MASTER_SESSION_ID' ")

    _host_id_query = "SELECT aurora_db_instance_identifier()"
    _is_reader_query = "SELECT pg_is_in_recovery()"

    def is_dialect(self, conn: Connection) -> bool:
        if not super().is_dialect(conn):
            return False

        has_extensions: bool = False
        has_topology: bool = False

        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self._extensions_sql)
                row = aws_cursor.fetchone()
                if row and bool(row[0]):
                    logger.debug("has_extensions: True")
                    has_extensions = True

            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self._has_topology_sql)
                if aws_cursor.fetchone() is not None:
                    logger.debug("has_topology: True")
                    has_topology = True

            return has_extensions and has_topology
        except Error:
            pass

        return False


class UnknownDialect(Dialect):
    _DIALECT_UPDATE_CANDIDATES: Optional[FrozenSet[DialectCodes]] = \
        frozenset({DialectCodes.MYSQL,
                   DialectCodes.PG,
                   DialectCodes.MARIADB,
                   DialectCodes.RDS_MYSQL,
                   DialectCodes.RDS_PG,
                   DialectCodes.AURORA_MYSQL,
                   DialectCodes.AURORA_PG})

    @property
    def default_port(self) -> int:
        return HostInfo.NO_PORT

    @property
    def host_alias_query(self) -> str:
        return ""

    @property
    def server_version_query(self) -> str:
        return ""

    def is_dialect(self, conn: Connection) -> bool:
        return False

    @property
    def dialect_update_candidates(self) -> Optional[FrozenSet[DialectCodes]]:
        return UnknownDialect._DIALECT_UPDATE_CANDIDATES


class DialectManager(DialectProvider):

    def __init__(self, rds_helper: Optional[RdsUtils] = None, custom_dialect: Optional[Dialect] = None) -> None:
        self._rds_helper: RdsUtils = rds_helper if rds_helper else RdsUtils()
        self._can_update: bool = False
        self._dialect: Optional[Dialect] = None
        self._custom_dialect: Optional[Dialect] = custom_dialect if custom_dialect else None
        self._dialect_code: Optional[DialectCodes] = None
        self._known_endpoint_dialects: dict = dict()
        self._known_dialects_by_code: Dict[DialectCodes, Dialect] = {DialectCodes.MYSQL: MysqlDialect(),
                                                                     DialectCodes.PG: PgDialect(),
                                                                     DialectCodes.MARIADB: MariaDbDialect(),
                                                                     DialectCodes.RDS_MYSQL: RdsMysqlDialect(),
                                                                     DialectCodes.RDS_PG: RdsPgDialect(),
                                                                     DialectCodes.AURORA_MYSQL: AuroraMysqlDialect(),
                                                                     DialectCodes.AURORA_PG: AuroraPgDialect(),
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
            user_dialect: Optional[Dialect] = self._known_dialects_by_code.get(dialect_code)
            if user_dialect:
                self._dialect_code = dialect_code
                self._dialect = user_dialect
                self.log_current_dialect()
                return user_dialect
            else:
                raise AwsWrapperError(Messages.get_formatted("Dialect.UnknownDialectCode", str(dialect_code)))

        host: str = props["host"]

        # TODO: replace with a method that identifies which database type we are using (MYSQL, PG, or MARIADB)
        database_type: DatabaseType = DatabaseType.POSTGRES

        if database_type is DatabaseType.MYSQL:
            rds_type = self._rds_helper.identify_rds_type(host)
            if rds_type.is_rds_cluster:
                self._dialect_code = DialectCodes.AURORA_MYSQL
                self._dialect = self._known_dialects_by_code.get(DialectCodes.AURORA_MYSQL)
                return self._dialect
            if rds_type.is_rds:
                self._can_update = True
                self._dialect_code = DialectCodes.RDS_MYSQL
                self._dialect = self._known_dialects_by_code.get(DialectCodes.RDS_MYSQL)
                self.log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCodes.MYSQL
            self._dialect = self._known_dialects_by_code.get(DialectCodes.MYSQL)
            self.log_current_dialect()
            return self._dialect

        if database_type is DatabaseType.POSTGRES:
            rds_type = self._rds_helper.identify_rds_type(host)
            if rds_type.is_rds_cluster:
                self._dialect_code = DialectCodes.AURORA_PG
                self._dialect = self._known_dialects_by_code.get(DialectCodes.AURORA_PG)
                return self._dialect
            if rds_type.is_rds:
                self._can_update = True
                self._dialect_code = DialectCodes.RDS_PG
                self._dialect = self._known_dialects_by_code.get(DialectCodes.RDS_PG)
                self.log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCodes.PG
            self._dialect = self._known_dialects_by_code.get(DialectCodes.PG)
            self.log_current_dialect()
            return self._dialect

        if database_type is DatabaseType.MARIADB:
            self._can_update = True
            self._dialect_code = DialectCodes.MARIADB
            self._dialect = self._known_dialects_by_code.get(DialectCodes.MARIADB)
            self.log_current_dialect()
            return self._dialect

        self._can_update = True
        self._dialect_code = DialectCodes.UNKNOWN
        self._dialect = self._known_dialects_by_code.get(DialectCodes.UNKNOWN)
        self.log_current_dialect()
        return self._dialect

    def log_current_dialect(self):
        dialect_class = "<null>" if self._dialect is None else type(self._dialect).__name__
        logger.debug(f"Current dialect: {self._dialect_code}, {dialect_class}, can_update: {self._can_update}")
