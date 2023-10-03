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

from abc import abstractmethod
from contextlib import closing
from enum import Enum, auto
from logging import getLogger
from typing import (TYPE_CHECKING, Callable, Dict, Optional, Protocol, Tuple,
                    runtime_checkable)

if TYPE_CHECKING:
    from aws_wrapper.pep249 import Connection
    from .generic_target_driver_dialect import TargetDriverDialect

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.host_list_provider import (AuroraHostListProvider,
                                            ConnectionStringHostListProvider)
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.utils.properties import (Properties, PropertiesUtils,
                                          WrapperProperties)
from aws_wrapper.utils.rdsutils import RdsUtils
from .exceptions import ExceptionHandler, PgExceptionHandler
from .target_driver_dialect import TargetDriverDialectCodes
from .utils.cache_map import CacheMap
from .utils.messages import Messages

logger = getLogger(__name__)


class DialectCode(Enum):
    AURORA_MYSQL: str = "aurora-mysql"
    RDS_MYSQL: str = "rds-mysql"
    MYSQL: str = "mysql"

    AURORA_PG: str = "aurora-pg"
    RDS_PG: str = "rds-pg"
    PG: str = "pg"

    CUSTOM: str = "custom"
    UNKNOWN: str = "unknown"

    @staticmethod
    def from_string(value: str) -> DialectCode:
        try:
            return DialectCode(value)
        except ValueError:
            raise AwsWrapperError(Messages.get_formatted("DialectCode.InvalidStringValue", value))


class TargetDriverType(Enum):
    MYSQL = auto()
    POSTGRES = auto()
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

    @property
    @abstractmethod
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        ...

    @property
    @abstractmethod
    def exception_handler(self) -> Optional[ExceptionHandler]:
        ...

    @abstractmethod
    def is_dialect(self, conn: Connection) -> bool:
        ...

    @abstractmethod
    def get_host_list_provider_supplier(self) -> Callable:
        ...


class DialectProvider(Protocol):
    def get_dialect(self, driver_dialect: str, props: Properties) -> Optional[Dialect]:
        """
        Returns the dialect identified by analyzing the AwsWrapperProperties.DIALECT property (if set) or the target
        driver method
        """
        ...

    def query_for_dialect(self, url: str, host_info: Optional[HostInfo], conn: Connection,
                          driver_dialect: TargetDriverDialect) -> Optional[Dialect]:
        """Returns the dialect identified by querying the database to identify the engine type"""
        ...


class MysqlDialect(Dialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (DialectCode.AURORA_MYSQL, DialectCode.RDS_MYSQL)

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(@@hostname, ':', @@port)"

    @property
    def server_version_query(self) -> str:
        return "SHOW VARIABLES LIKE 'version_comment'"

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        # TODO
        return None

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return MysqlDialect._DIALECT_UPDATE_CANDIDATES

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self.server_version_query)
                for record in aws_cursor:
                    for column_value in record:
                        if "mysql" in column_value.lower():
                            return True
        except Exception:
            pass

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)


class PgDialect(Dialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (DialectCode.AURORA_PG, DialectCode.RDS_PG)

    @property
    def default_port(self) -> int:
        return 5432

    @property
    def host_alias_query(self) -> str:
        return "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())"

    @property
    def server_version_query(self) -> str:
        return "SELECT 'version', VERSION()"

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return PgDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        return PgExceptionHandler()

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute('SELECT 1 FROM pg_proc LIMIT 1')
                if aws_cursor.fetchone() is not None:
                    return True
        except Exception:
            # Executing the select statements will start a transaction, if the queries failed due to invalid syntax,
            # the transaction will be aborted, no further commands can be executed. We need to call rollback here.
            conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)


class RdsMysqlDialect(MysqlDialect):
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.AURORA_MYSQL,)

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self.server_version_query)
                for record in aws_cursor:
                    for column_value in record:
                        if "source distribution" in column_value.lower():
                            return True
        except Exception:
            pass

        return False

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return RdsMysqlDialect._DIALECT_UPDATE_CANDIDATES


class RdsPgDialect(PgDialect):
    _EXTENSIONS_QUERY: str = ("SELECT (setting LIKE '%rds_tools%') AS rds_tools, "
                              "(setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
                              "FROM pg_settings "
                              "WHERE name='rds.extensions'")
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.AURORA_PG,)

    def is_dialect(self, conn: Connection) -> bool:

        if not super().is_dialect(conn):
            return False

        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(RdsPgDialect._EXTENSIONS_QUERY)
                for row in aws_cursor:
                    rds_tools = bool(row[0])
                    aurora_utils = bool(row[1])
                    logger.debug(Messages.get_formatted(
                        "RdsPgDialect.RdsToolsAuroraUtils", str(rds_tools), str(aurora_utils)))
                    if rds_tools and not aurora_utils:
                        return True

        except Exception:
            # Executing the select statements will start a transaction, if the queries failed due to invalid syntax,
            # the transaction will be aborted, no further commands can be executed. We need to call rollback here.
            conn.rollback()
        return False

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return RdsPgDialect._DIALECT_UPDATE_CANDIDATES


class AuroraMysqlDialect(MysqlDialect, TopologyAwareDatabaseDialect):
    _topology_query: str = ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
                            "CPU, REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP "
                            "FROM information_schema.replica_host_status "
                            "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 "
                            "OR SESSION_ID = 'MASTER_SESSION_ID' ")

    _host_id_query: str = "SELECT @@aurora_server_id"

    _is_reader_query: str = "SELECT @@innodb_read_only"

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return None

    def is_dialect(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute("SHOW VARIABLES LIKE 'aurora_version'")
                # If variable with such a name is presented then it means it's an Aurora cluster
                if aws_cursor.fetchone() is not None:
                    return True
        except Exception:
            pass

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: AuroraHostListProvider(provider_service, props)


class AuroraPgDialect(PgDialect, TopologyAwareDatabaseDialect):
    _extensions_sql: str = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils " \
                           "FROM pg_settings WHERE name='rds.extensions'"

    _has_topology_sql: str = "SELECT 1 FROM aurora_replica_status() LIMIT 1"

    _topology_query = \
        ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
         "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
         "FROM aurora_replica_status() "
         "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
         "OR LAST_UPDATE_TIMESTAMP IS NULL")

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
                    logger.debug(Messages.get("AuroraPgDialect.HasExtensionsTrue"))
                    has_extensions = True

            with closing(conn.cursor()) as aws_cursor:
                aws_cursor.execute(self._has_topology_sql)
                if aws_cursor.fetchone() is not None:
                    logger.debug(Messages.get("AuroraPgDialect.HasTopologyTrue"))
                    has_topology = True

            return has_extensions and has_topology
        except Exception:
            # Executing the select statements will start a transaction, if the queries failed due to invalid syntax,
            # the transaction will be aborted, no further commands can be executed. We need to call rollback here.
            conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: AuroraHostListProvider(provider_service, props)


class UnknownDialect(Dialect):
    _DIALECT_UPDATE_CANDIDATES: Optional[Tuple[DialectCode, ...]] = \
        (DialectCode.MYSQL,
         DialectCode.PG,
         DialectCode.RDS_MYSQL,
         DialectCode.RDS_PG,
         DialectCode.AURORA_MYSQL,
         DialectCode.AURORA_PG)

    @property
    def default_port(self) -> int:
        return HostInfo.NO_PORT

    @property
    def host_alias_query(self) -> str:
        return ""

    @property
    def server_version_query(self) -> str:
        return ""

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return UnknownDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        return None

    def is_dialect(self, conn: Connection) -> bool:
        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)


class DialectManager(DialectProvider):
    _ENDPOINT_CACHE_EXPIRATION_NS = 30 * 60_000_000_000  # 30 minutes

    def __init__(self, rds_helper: Optional[RdsUtils] = None, custom_dialect: Optional[Dialect] = None) -> None:
        self._rds_helper: RdsUtils = rds_helper if rds_helper else RdsUtils()
        self._can_update: bool = False
        self._dialect: Dialect = UnknownDialect()
        self._dialect_code: DialectCode = DialectCode.UNKNOWN
        self._custom_dialect: Optional[Dialect] = custom_dialect if custom_dialect else None
        self._known_endpoint_dialects: CacheMap[str, DialectCode] = CacheMap()
        self._known_dialects_by_code: Dict[DialectCode, Dialect] = {DialectCode.MYSQL: MysqlDialect(),
                                                                    DialectCode.PG: PgDialect(),
                                                                    DialectCode.RDS_MYSQL: RdsMysqlDialect(),
                                                                    DialectCode.RDS_PG: RdsPgDialect(),
                                                                    DialectCode.AURORA_MYSQL: AuroraMysqlDialect(),
                                                                    DialectCode.AURORA_PG: AuroraPgDialect(),
                                                                    DialectCode.UNKNOWN: UnknownDialect()}

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

    def get_dialect(self, driver_dialect: str, props: Properties) -> Dialect:
        self._can_update = False

        if self._custom_dialect is not None:
            self._dialect_code = DialectCode.CUSTOM
            self._dialect = self._custom_dialect
            self._log_current_dialect()
            return self._dialect

        user_dialect_setting: Optional[str] = WrapperProperties.DIALECT.get(props)
        url = PropertiesUtils.get_url(props)

        if user_dialect_setting is None:
            dialect_code = self._known_endpoint_dialects.get(url)
        else:
            dialect_code = DialectCode.from_string(user_dialect_setting)

        if dialect_code is not None:
            dialect: Optional[Dialect] = self._known_dialects_by_code.get(dialect_code)
            if dialect:
                self._dialect_code = dialect_code
                self._dialect = dialect
                self._log_current_dialect()
                return dialect
            else:
                raise AwsWrapperError(Messages.get_formatted("DialectManager.UnknownDialectCode", str(dialect_code)))

        host: str = props["host"]
        target_driver_type: TargetDriverType = self._get_target_driver_type(driver_dialect)
        if target_driver_type is TargetDriverType.MYSQL:
            rds_type = self._rds_helper.identify_rds_type(host)
            if rds_type.is_rds_cluster:
                self._dialect_code = DialectCode.AURORA_MYSQL
                self._dialect = self._known_dialects_by_code[DialectCode.AURORA_MYSQL]
                return self._dialect
            if rds_type.is_rds:
                self._can_update = True
                self._dialect_code = DialectCode.RDS_MYSQL
                self._dialect = self._known_dialects_by_code[DialectCode.RDS_MYSQL]
                self._log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCode.MYSQL
            self._dialect = self._known_dialects_by_code[DialectCode.MYSQL]
            self._log_current_dialect()
            return self._dialect

        if target_driver_type is TargetDriverType.POSTGRES:
            rds_type = self._rds_helper.identify_rds_type(host)
            if rds_type.is_rds_cluster:
                self._dialect_code = DialectCode.AURORA_PG
                self._dialect = self._known_dialects_by_code[DialectCode.AURORA_PG]
                return self._dialect
            if rds_type.is_rds:
                self._can_update = True
                self._dialect_code = DialectCode.RDS_PG
                self._dialect = self._known_dialects_by_code[DialectCode.RDS_PG]
                self._log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCode.PG
            self._dialect = self._known_dialects_by_code[DialectCode.PG]
            self._log_current_dialect()
            return self._dialect

        self._can_update = True
        self._dialect_code = DialectCode.UNKNOWN
        self._dialect = self._known_dialects_by_code[DialectCode.UNKNOWN]
        self._log_current_dialect()
        return self._dialect

    def _get_target_driver_type(self, driver_dialect: str) -> TargetDriverType:
        if driver_dialect == TargetDriverDialectCodes.PSYCOPG:
            return TargetDriverType.POSTGRES
        if driver_dialect == TargetDriverDialectCodes.MYSQL_CONNECTOR_PYTHON:
            return TargetDriverType.MYSQL

        return TargetDriverType.CUSTOM

    def query_for_dialect(self, url: str, host_info: Optional[HostInfo], conn: Connection,
                          driver_dialect: TargetDriverDialect) -> Dialect:
        if not self._can_update:
            self._log_current_dialect()
            return self._dialect

        dialect_candidates = self._dialect.dialect_update_candidates if self._dialect is not None else None
        if dialect_candidates is not None:
            for dialect_code in dialect_candidates:
                dialect_candidate = self._known_dialects_by_code.get(dialect_code)
                if dialect_candidate is None:
                    raise AwsWrapperError(Messages.get_formatted("DialectManager.UnknownDialectCode", dialect_code))

                initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
                is_dialect = dialect_candidate.is_dialect(conn)
                if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                    # this condition is True when autocommit is False and the query started a new transaction.
                    conn.commit()
                if not is_dialect:
                    continue

                self._can_update = False
                self._dialect_code = dialect_code
                self._dialect = dialect_candidate
                self._known_endpoint_dialects.put(url, dialect_code, DialectManager._ENDPOINT_CACHE_EXPIRATION_NS)
                if host_info is not None:
                    self._known_endpoint_dialects.put(
                        host_info.url, dialect_code, DialectManager._ENDPOINT_CACHE_EXPIRATION_NS)

                self._log_current_dialect()
                return self._dialect

        if self._dialect_code is None or self._dialect_code == DialectCode.UNKNOWN:
            raise AwsWrapperError(Messages.get("DialectManager.UnknownDialect"))

        self._can_update = False
        self._known_endpoint_dialects.put(url, self._dialect_code, DialectManager._ENDPOINT_CACHE_EXPIRATION_NS)
        if host_info is not None:
            self._known_endpoint_dialects.put(
                host_info.url, self._dialect_code, DialectManager._ENDPOINT_CACHE_EXPIRATION_NS)
        self._log_current_dialect()
        return self._dialect

    def _log_current_dialect(self):
        dialect_class = "<null>" if self._dialect is None else type(self._dialect).__name__
        logger.debug(Messages.get_formatted("DialectManager.CurrentDialectCanUpdate", self._dialect_code, dialect_class, self._can_update))
