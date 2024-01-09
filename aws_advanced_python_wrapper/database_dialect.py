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

from typing import (TYPE_CHECKING, Callable, ClassVar, Dict, Optional,
                    Protocol, Tuple, runtime_checkable)

from aws_advanced_python_wrapper.driver_info import DriverInfo

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection
    from .driver_dialect import DriverDialect
    from .exception_handling import ExceptionHandler

from abc import abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor, TimeoutError
from contextlib import closing
from enum import Enum, auto

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError)
from aws_advanced_python_wrapper.host_list_provider import (
    ConnectionStringHostListProvider, MultiAzHostListProvider,
    RdsHostListProvider)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.decorators import \
    preserve_transaction_status_with_timeout
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from .driver_dialect_codes import DriverDialectCodes
from .utils.cache_map import CacheMap
from .utils.messages import Messages
from .utils.utils import Utils

logger = Logger(__name__)


class DialectCode(Enum):
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html
    MULTI_AZ_MYSQL = "multi-az-mysql"
    AURORA_MYSQL = "aurora-mysql"
    RDS_MYSQL = "rds-mysql"
    MYSQL = "mysql"

    MULTI_AZ_PG = "multi-az-pg"
    AURORA_PG = "aurora-pg"
    RDS_PG = "rds-pg"
    PG = "pg"

    CUSTOM = "custom"
    UNKNOWN = "unknown"

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
    _TOPOLOGY_QUERY: str
    _HOST_ID_QUERY: str
    _IS_READER_QUERY: str

    @property
    def topology_query(self) -> str:
        return self._TOPOLOGY_QUERY

    @property
    def host_id_query(self) -> str:
        return self._HOST_ID_QUERY

    @property
    def is_reader_query(self) -> str:
        return self._IS_READER_QUERY


class DatabaseDialect(Protocol):
    """
    Database dialects help the AWS Advanced Python Driver determine what kind of underlying database is being used,
    and configure details unique to specific databases.
    """

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
    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        ...

    @abstractmethod
    def get_host_list_provider_supplier(self) -> Callable:
        ...

    @abstractmethod
    def prepare_conn_props(self, props: Properties):
        ...


class DatabaseDialectProvider(Protocol):
    def get_dialect(self, driver_dialect: str, props: Properties) -> Optional[DatabaseDialect]:
        """
        Returns the dialect identified by analyzing the AwsWrapperProperties.DIALECT property (if set) or the target
        driver method
        """
        ...

    def query_for_dialect(self, url: str, host_info: Optional[HostInfo], conn: Connection,
                          driver_dialect: DriverDialect) -> Optional[DatabaseDialect]:
        """Returns the dialect identified by querying the database to identify the engine type"""
        ...


class MysqlDatabaseDialect(DatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (
        DialectCode.AURORA_MYSQL, DialectCode.MULTI_AZ_MYSQL, DialectCode.RDS_MYSQL)
    _exception_handler: Optional[ExceptionHandler] = None

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
        if MysqlDatabaseDialect._exception_handler is None:
            MysqlDatabaseDialect._exception_handler = Utils.initialize_class(
                "aws_advanced_python_wrapper.utils.mysql_exception_handler.MySQLExceptionHandler")
        return MysqlDatabaseDialect._exception_handler

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return MysqlDatabaseDialect._DIALECT_UPDATE_CANDIDATES

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self.server_version_query)
                for record in cursor:
                    for column_value in record:
                        if "mysql" in column_value.lower():
                            return True
        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)

    def prepare_conn_props(self, props: Properties):
        pass


class PgDatabaseDialect(DatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (
        DialectCode.AURORA_PG, DialectCode.MULTI_AZ_PG, DialectCode.RDS_PG)
    _exception_handler: Optional[ExceptionHandler] = None

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
        return PgDatabaseDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        if PgDatabaseDialect._exception_handler is None:
            PgDatabaseDialect._exception_handler = Utils.initialize_class(
                "aws_advanced_python_wrapper.utils.pg_exception_handler.SingleAzPgExceptionHandler")
        return PgDatabaseDialect._exception_handler

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute('SELECT 1 FROM pg_proc LIMIT 1')
                if cursor.fetchone() is not None:
                    return True
        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)

    def prepare_conn_props(self, props: Properties):
        pass


class RdsMysqlDialect(MysqlDatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.AURORA_MYSQL, DialectCode.MULTI_AZ_MYSQL)

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self.server_version_query)
                for record in cursor:
                    for column_value in record:
                        if "source distribution" in column_value.lower():
                            return True
        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()

        return False

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return RdsMysqlDialect._DIALECT_UPDATE_CANDIDATES


class RdsPgDialect(PgDatabaseDialect):
    _EXTENSIONS_QUERY = ("SELECT (setting LIKE '%rds_tools%') AS rds_tools, "
                         "(setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils "
                         "FROM pg_settings "
                         "WHERE name='rds.extensions'")
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.AURORA_PG, DialectCode.MULTI_AZ_PG)

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        if not super().is_dialect(conn, driver_dialect):
            return False

        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(RdsPgDialect._EXTENSIONS_QUERY)
                for row in cursor:
                    rds_tools = bool(row[0])
                    aurora_utils = bool(row[1])
                    logger.debug(
                        "RdsPgDialect.RdsToolsAuroraUtils", str(rds_tools), str(aurora_utils))
                    if rds_tools and not aurora_utils:
                        return True

        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()
        return False

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return RdsPgDialect._DIALECT_UPDATE_CANDIDATES


class AuroraMysqlDialect(MysqlDatabaseDialect, TopologyAwareDatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.MULTI_AZ_MYSQL,)
    _TOPOLOGY_QUERY = ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
                       "CPU, REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP "
                       "FROM information_schema.replica_host_status "
                       "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 "
                       "OR SESSION_ID = 'MASTER_SESSION_ID' ")
    _HOST_ID_QUERY = "SELECT @@aurora_server_id"
    _IS_READER_QUERY = "SELECT @@innodb_read_only"

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return AuroraMysqlDialect._DIALECT_UPDATE_CANDIDATES

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute("SHOW VARIABLES LIKE 'aurora_version'")
                # If variable with such a name is presented then it means it's an Aurora cluster
                if cursor.fetchone() is not None:
                    return True
        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: RdsHostListProvider(provider_service, props)


class AuroraPgDialect(PgDatabaseDialect, TopologyAwareDatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (DialectCode.MULTI_AZ_PG,)

    _EXTENSIONS_QUERY = "SELECT (setting LIKE '%aurora_stat_utils%') AS aurora_stat_utils " \
                        "FROM pg_settings WHERE name='rds.extensions'"

    _HAS_TOPOLOGY_QUERY = "SELECT 1 FROM aurora_replica_status() LIMIT 1"

    _TOPOLOGY_QUERY = \
        ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
         "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
         "FROM aurora_replica_status() "
         "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
         "OR LAST_UPDATE_TIMESTAMP IS NULL")

    _HOST_ID_QUERY = "SELECT aurora_db_instance_identifier()"
    _IS_READER_QUERY = "SELECT pg_is_in_recovery()"

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return AuroraPgDialect._DIALECT_UPDATE_CANDIDATES

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        if not super().is_dialect(conn, driver_dialect):
            return False

        has_extensions: bool = False
        has_topology: bool = False

        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._EXTENSIONS_QUERY)
                row = cursor.fetchone()
                if row and bool(row[0]):
                    logger.debug("AuroraPgDialect.HasExtensionsTrue")
                    has_extensions = True

            with closing(conn.cursor()) as cursor:
                cursor.execute(self._HAS_TOPOLOGY_QUERY)
                if cursor.fetchone() is not None:
                    logger.debug("AuroraPgDialect.HasTopologyTrue")
                    has_topology = True

            return has_extensions and has_topology
        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: RdsHostListProvider(provider_service, props)


class MultiAzMysqlDialect(MysqlDatabaseDialect, TopologyAwareDatabaseDialect):
    _TOPOLOGY_QUERY = "SELECT id, endpoint, port FROM mysql.rds_topology"
    _WRITER_HOST_QUERY = "SHOW REPLICA STATUS"
    _WRITER_HOST_COLUMN_INDEX = 39
    _HOST_ID_QUERY = "SELECT @@server_id"
    _IS_READER_QUERY = "SELECT @@read_only"

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return None

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(MultiAzMysqlDialect._TOPOLOGY_QUERY)
                records = cursor.fetchall()
                if records is not None and len(records) > 0:
                    return True
        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: MultiAzHostListProvider(
            provider_service,
            props,
            self._TOPOLOGY_QUERY,
            self._HOST_ID_QUERY,
            self._IS_READER_QUERY,
            self._WRITER_HOST_QUERY,
            self._WRITER_HOST_COLUMN_INDEX)

    def prepare_conn_props(self, props: Properties):
        # These props are added for RDS metrics purposes, they are not required for functional correctness.
        # The "conn_attrs" property value is specified as a dict.
        extra_conn_attrs = {
            "python_wrapper_name": "aws_python_driver",
            "python_wrapper_version": DriverInfo.DRIVER_VERSION}
        conn_attrs = props.get("conn_attrs")
        if conn_attrs is None:
            props["conn_attrs"] = extra_conn_attrs
        else:
            props["conn_attrs"].update(extra_conn_attrs)


class MultiAzPgDialect(PgDatabaseDialect, TopologyAwareDatabaseDialect):
    # The driver name passed to show_topology is used for RDS metrics purposes.
    # It is not required for functional correctness.
    _TOPOLOGY_QUERY = \
        f"SELECT id, endpoint, port FROM rds_tools.show_topology('aws_python_driver-{DriverInfo.DRIVER_VERSION}')"
    _WRITER_HOST_QUERY = \
        "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"
    _HOST_ID_QUERY = "SELECT dbi_resource_id FROM rds_tools.dbi_resource_id()"
    _IS_READER_QUERY = "SELECT pg_is_in_recovery()"
    _exception_handler: Optional[ExceptionHandler] = None

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return None

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        if MultiAzPgDialect._exception_handler is None:
            MultiAzPgDialect._exception_handler = Utils.initialize_class(
                "aws_advanced_python_wrapper.utils.pg_exception_handler.MultiAzPgExceptionHandler")
        return MultiAzPgDialect._exception_handler

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        initial_transaction_status: bool = driver_dialect.is_in_transaction(conn)
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(MultiAzPgDialect._WRITER_HOST_QUERY)
                if cursor.fetchone() is not None:
                    return True
        except Exception:
            if not initial_transaction_status and driver_dialect.is_in_transaction(conn):
                conn.rollback()

        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: MultiAzHostListProvider(
            provider_service,
            props,
            self._TOPOLOGY_QUERY,
            self._HOST_ID_QUERY,
            self._IS_READER_QUERY,
            self._WRITER_HOST_QUERY)


class UnknownDatabaseDialect(DatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES: Optional[Tuple[DialectCode, ...]] = \
        (DialectCode.MYSQL,
         DialectCode.PG,
         DialectCode.RDS_MYSQL,
         DialectCode.RDS_PG,
         DialectCode.AURORA_MYSQL,
         DialectCode.AURORA_PG,
         DialectCode.MULTI_AZ_MYSQL,
         DialectCode.MULTI_AZ_PG)

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
        return UnknownDatabaseDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        return None

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        return False

    def get_host_list_provider_supplier(self) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)

    def prepare_conn_props(self, props: Properties):
        pass


class DatabaseDialectManager(DatabaseDialectProvider):
    _ENDPOINT_CACHE_EXPIRATION_NS = 30 * 60_000_000_000  # 30 minutes
    _known_endpoint_dialects: CacheMap[str, DialectCode] = CacheMap()
    _custom_dialect: Optional[DatabaseDialect] = None
    _executor: ClassVar[Executor] = ThreadPoolExecutor(thread_name_prefix="DatabaseDialectManagerExecutor")
    _known_dialects_by_code: Dict[DialectCode, DatabaseDialect] = {
        DialectCode.MYSQL: MysqlDatabaseDialect(),
        DialectCode.RDS_MYSQL: RdsMysqlDialect(),
        DialectCode.AURORA_MYSQL: AuroraMysqlDialect(),
        DialectCode.MULTI_AZ_MYSQL: MultiAzMysqlDialect(),
        DialectCode.PG: PgDatabaseDialect(),
        DialectCode.RDS_PG: RdsPgDialect(),
        DialectCode.AURORA_PG: AuroraPgDialect(),
        DialectCode.MULTI_AZ_PG: MultiAzPgDialect(),
        DialectCode.UNKNOWN: UnknownDatabaseDialect()
    }

    def __init__(self, props: Properties, rds_helper: Optional[RdsUtils] = None):
        self._props: Properties = props
        self._rds_helper: RdsUtils = rds_helper if rds_helper else RdsUtils()
        self._can_update: bool = False
        self._dialect: DatabaseDialect = UnknownDatabaseDialect()
        self._dialect_code: DialectCode = DialectCode.UNKNOWN

    @staticmethod
    def get_custom_dialect():
        return DatabaseDialectManager._custom_dialect

    @staticmethod
    def set_custom_dialect(dialect: DatabaseDialect):
        DatabaseDialectManager._custom_dialect = dialect

    @staticmethod
    def reset_custom_dialect():
        DatabaseDialectManager._custom_dialect = None

    def reset_endpoint_cache(self):
        DatabaseDialectManager._known_endpoint_dialects.clear()

    def get_dialect(self, driver_dialect: str, props: Properties) -> DatabaseDialect:
        self._can_update = False

        if self._custom_dialect is not None:
            self._dialect_code = DialectCode.CUSTOM
            self._dialect = self._custom_dialect
            self._log_current_dialect()
            return self._dialect

        user_dialect_setting: Optional[str] = WrapperProperties.DIALECT.get(props)
        url = PropertiesUtils.get_url(props)

        if user_dialect_setting is None:
            dialect_code = DatabaseDialectManager._known_endpoint_dialects.get(url)
        else:
            dialect_code = DialectCode.from_string(user_dialect_setting)

        if dialect_code is not None:
            dialect: Optional[DatabaseDialect] = DatabaseDialectManager._known_dialects_by_code.get(dialect_code)
            if dialect:
                self._dialect_code = dialect_code
                self._dialect = dialect
                self._log_current_dialect()
                return dialect
            else:
                raise AwsWrapperError(
                    Messages.get_formatted("DatabaseDialectManager.UnknownDialectCode", str(dialect_code)))

        host: str = props["host"]
        target_driver_type: TargetDriverType = self._get_target_driver_type(driver_dialect)
        if target_driver_type is TargetDriverType.MYSQL:
            rds_type = self._rds_helper.identify_rds_type(host)
            if rds_type.is_rds_cluster:
                self._can_update = True
                self._dialect_code = DialectCode.AURORA_MYSQL
                self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.AURORA_MYSQL]
                return self._dialect
            if rds_type.is_rds:
                self._can_update = True
                self._dialect_code = DialectCode.RDS_MYSQL
                self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.RDS_MYSQL]
                self._log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCode.MYSQL
            self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.MYSQL]
            self._log_current_dialect()
            return self._dialect

        if target_driver_type is TargetDriverType.POSTGRES:
            rds_type = self._rds_helper.identify_rds_type(host)
            if rds_type.is_rds_cluster:
                self._can_update = True
                self._dialect_code = DialectCode.AURORA_PG
                self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.AURORA_PG]
                return self._dialect
            if rds_type.is_rds:
                self._can_update = True
                self._dialect_code = DialectCode.RDS_PG
                self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.RDS_PG]
                self._log_current_dialect()
                return self._dialect
            self._can_update = True
            self._dialect_code = DialectCode.PG
            self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.PG]
            self._log_current_dialect()
            return self._dialect

        self._can_update = True
        self._dialect_code = DialectCode.UNKNOWN
        self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.UNKNOWN]
        self._log_current_dialect()
        return self._dialect

    def _get_target_driver_type(self, driver_dialect: str) -> TargetDriverType:
        if driver_dialect == DriverDialectCodes.PSYCOPG:
            return TargetDriverType.POSTGRES
        if driver_dialect == DriverDialectCodes.MYSQL_CONNECTOR_PYTHON:
            return TargetDriverType.MYSQL

        return TargetDriverType.CUSTOM

    def query_for_dialect(self, url: str, host_info: Optional[HostInfo], conn: Connection,
                          driver_dialect: DriverDialect) -> DatabaseDialect:
        if not self._can_update:
            self._log_current_dialect()
            return self._dialect

        dialect_candidates = self._dialect.dialect_update_candidates if self._dialect is not None else None
        if dialect_candidates is not None:
            for dialect_code in dialect_candidates:
                dialect_candidate = DatabaseDialectManager._known_dialects_by_code.get(dialect_code)
                if dialect_candidate is None:
                    raise AwsWrapperError(Messages.get_formatted("DatabaseDialectManager.UnknownDialectCode", dialect_code))

                timeout_sec = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get(self._props)
                try:
                    cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
                        DatabaseDialectManager._executor,
                        timeout_sec,
                        driver_dialect,
                        conn)(dialect_candidate.is_dialect)
                    is_dialect = cursor_execute_func_with_timeout(conn, driver_dialect)
                except TimeoutError as e:
                    raise QueryTimeoutError("DatabaseDialectManager.QueryForDialectTimeout") from e

                if not is_dialect:
                    continue

                self._can_update = False
                self._dialect_code = dialect_code
                self._dialect = dialect_candidate
                DatabaseDialectManager._known_endpoint_dialects.put(url, dialect_code,
                                                                    DatabaseDialectManager._ENDPOINT_CACHE_EXPIRATION_NS)
                if host_info is not None:
                    DatabaseDialectManager._known_endpoint_dialects.put(
                        host_info.url, dialect_code, DatabaseDialectManager._ENDPOINT_CACHE_EXPIRATION_NS)

                self._log_current_dialect()
                return self._dialect

        if self._dialect_code is None or self._dialect_code == DialectCode.UNKNOWN:
            raise AwsWrapperError(Messages.get("DatabaseDialectManager.UnknownDialect"))

        self._can_update = False
        DatabaseDialectManager._known_endpoint_dialects.put(url, self._dialect_code,
                                                            DatabaseDialectManager._ENDPOINT_CACHE_EXPIRATION_NS)
        if host_info is not None:
            DatabaseDialectManager._known_endpoint_dialects.put(
                host_info.url, self._dialect_code, DatabaseDialectManager._ENDPOINT_CACHE_EXPIRATION_NS)
        self._log_current_dialect()
        return self._dialect

    def _log_current_dialect(self):
        dialect_class = "<null>" if self._dialect is None else type(self._dialect).__name__
        logger.debug("DatabaseDialectManager.CurrentDialectCanUpdate", self._dialect_code, dialect_class, self._can_update)
