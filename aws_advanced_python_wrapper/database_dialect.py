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

from typing import (TYPE_CHECKING, Any, Callable, ClassVar, Dict, FrozenSet,
                    List, Optional, Protocol, Sequence, Tuple,
                    runtime_checkable)

from aws_advanced_python_wrapper.dialect_verifier import (
    AsyncDialectVerifier, DialectVerificationRules, Rule, SyncDialectVerifier,
    all_of, col_contains_ignore_case, col_equals_ignore_case, col_falsy,
    col_greater_than, col_non_empty_str, col_truthy)
from aws_advanced_python_wrapper.driver_info import DriverInfo
from aws_advanced_python_wrapper.host_list_provider import (
    AuroraTopologyUtils, ConnectionStringHostListProvider,
    GlobalAuroraHostListProvider, GlobalAuroraTopologyUtils,
    MultiAzTopologyUtils, RdsHostListProvider)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection
    from .driver_dialect import DriverDialect
    from .exception_handling import ExceptionHandler
    from aws_advanced_python_wrapper.plugin_service import PluginService

from abc import ABC, abstractmethod
from concurrent.futures import TimeoutError
from contextlib import closing
from enum import Enum, auto

from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.accessible_regions import \
    AccessibleRegions
from aws_advanced_python_wrapper.utils.decorators import \
    preserve_transaction_status_with_timeout
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          PropertiesUtils,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from .driver_dialect_codes import DriverDialectCodes
from .utils.messages import Messages
from .utils.storage.cache_map import CacheMap
from .utils.utils import Utils

logger = Logger(__name__)


class DialectCode(Enum):
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html
    MULTI_AZ_CLUSTER_MYSQL = "multi-az-mysql"
    GLOBAL_AURORA_MYSQL = "global-aurora-mysql"
    AURORA_MYSQL = "aurora-mysql"
    RDS_MYSQL = "rds-mysql"
    MYSQL = "mysql"

    MULTI_AZ_CLUSTER_PG = "multi-az-pg"
    GLOBAL_AURORA_PG = "global-aurora-pg"
    AURORA_PG = "aurora-pg"
    RDS_PG = "rds-pg"
    PG = "pg"

    CUSTOM = "custom"
    UNKNOWN = "unknown"

    @staticmethod
    def from_string(value: str) -> DialectCode:
        try:
            return DialectCode(value)
        except ValueError as e:
            raise AwsWrapperError(Messages.get_formatted("DialectCode.InvalidStringValue", value), e) from e


class TargetDriverType(Enum):
    MYSQL = auto()
    POSTGRES = auto()
    CUSTOM = auto()


@runtime_checkable
class TopologyAwareDatabaseDialect(Protocol):
    _TOPOLOGY_QUERY: str
    _HOST_ID_QUERY: str
    _WRITER_HOST_QUERY: str

    @property
    def topology_query(self) -> str:
        return self._TOPOLOGY_QUERY

    @property
    def host_id_query(self) -> str:
        return self._HOST_ID_QUERY

    @property
    def writer_id_query(self) -> str:
        return self._WRITER_HOST_QUERY


class GlobalAuroraTopologyDialect(TopologyAwareDatabaseDialect):
    _REGION_BY_INSTANCE_ID_QUERY: str

    @property
    def region_by_instance_id_query(self) -> str:
        return self._REGION_BY_INSTANCE_ID_QUERY


@runtime_checkable
class AuroraLimitlessDialect(Protocol):
    _LIMITLESS_ROUTER_ENDPOINT_QUERY: str

    @property
    def limitless_router_endpoint_query(self) -> str:
        return self._LIMITLESS_ROUTER_ENDPOINT_QUERY


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
    def host_id_query(self) -> str:
        ...

    @property
    @abstractmethod
    def is_reader_query(self) -> str:
        ...

    @property
    @abstractmethod
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        ...

    @property
    @abstractmethod
    def exception_handler(self) -> Optional[ExceptionHandler]:
        ...

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        """The queries and row conditions that identify this dialect.

        Declared as data so one set of rules serves both engines: the sync
        and async verifiers in :mod:`dialect_verifier` run the same rules
        against a DB-API or an async connection respectively. Writing them
        once is the point -- a hand-written sync body and a hand-written
        async body drift, which is how the async layer came to recognise
        only one of the six candidate dialects.

        None means the dialect cannot be identified by querying, so it is
        never selected from a candidate list; the pattern-based guess
        stands. That is the conservative answer and what
        :class:`UnknownDatabaseDialect` wants.
        """
        return None

    def is_dialect(self, conn: Connection, driver_dialect: DriverDialect) -> bool:
        """Verify whether the database dialect via a live connection.
        """
        try:
            return SyncDialectVerifier.run(self.verification_rules, conn, driver_dialect)
        except Exception:  # noqa: BLE001 - failed verification means "not this dialect"
            return False

    async def is_dialect_async(self, conn: Any, driver_dialect: Any) -> bool:
        """AVerify whether the database dialect via a live connection.
        """
        try:
            return await AsyncDialectVerifier.run(self.verification_rules, conn, driver_dialect)
        except Exception:  # noqa: BLE001 - failed verification means "not this dialect"
            return False

    @abstractmethod
    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        ...

    def get_async_host_list_provider_supplier(self) -> Callable:
        """Returns a ``(provider_service, props) -> AsyncHostListProvider``
        callable.Async counterpart of :meth:`get_host_list_provider_supplier`.
        """
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            create_static_provider
        return create_static_provider

    @abstractmethod
    def prepare_conn_props(self, props: Properties):
        ...

    def filter_available_hosts(
        self,
        hosts: Sequence[HostInfo],
        accessible_regions: Optional[FrozenSet[str]],
    ) -> List[HostInfo]:
        """Filter hosts by accessible regions.

        Non-multi-region dialects return the input unchanged. Global Aurora
        dialects override this to exclude hosts whose region is not in the
        accessible set.
        """
        return list(hosts)


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

    def is_confirmed_dialect(self) -> bool:
        """Whether the database dialect has been finalized and can't be changed.
        """
        ...


class MysqlDatabaseDialect(DatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (
        DialectCode.AURORA_MYSQL, DialectCode.GLOBAL_AURORA_MYSQL, DialectCode.MULTI_AZ_CLUSTER_MYSQL, DialectCode.RDS_MYSQL)
    _exception_handler: Optional[ExceptionHandler] = None
    _HOST_ID_EXPRESSION = "CONCAT(@@hostname, ':', @@port)"
    _HOST_ID_QUERY = f"SELECT @@hostname AS host, {_HOST_ID_EXPRESSION} AS host_id"
    _IS_READER_QUERY = "SELECT @@read_only"
    _SERVER_VERSION_QUERY = "SHOW VARIABLES LIKE 'version_comment'"
    _REPORT_HOST_QUERY = "SHOW VARIABLES LIKE 'report_host'"

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def host_alias_query(self) -> str:
        return f"SELECT {self._HOST_ID_EXPRESSION}"

    @property
    def server_version_query(self) -> str:
        return MysqlDatabaseDialect._SERVER_VERSION_QUERY

    @property
    def host_id_query(self) -> str:
        return self._HOST_ID_QUERY

    @property
    def is_reader_query(self) -> str:
        return self._IS_READER_QUERY

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        if MysqlDatabaseDialect._exception_handler is None:
            MysqlDatabaseDialect._exception_handler = Utils.initialize_class(
                "aws_advanced_python_wrapper.utils.mysql_exception_handler.MySQLExceptionHandler")
        return MysqlDatabaseDialect._exception_handler

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return MysqlDatabaseDialect._DIALECT_UPDATE_CANDIDATES

    # The version comment names the engine; MariaDB and others answer the
    # same query without "mysql" in it.
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(_SERVER_VERSION_QUERY,
             col_contains_ignore_case(1, "mysql"),
             match_any_row=True),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return MysqlDatabaseDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)

    def prepare_conn_props(self, props: Properties):
        pass


class PgDatabaseDialect(DatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (
        DialectCode.AURORA_PG, DialectCode.GLOBAL_AURORA_PG, DialectCode.MULTI_AZ_CLUSTER_PG, DialectCode.RDS_PG)
    _exception_handler: Optional[ExceptionHandler] = None
    _HOST_ID_EXPRESSION = "pg_catalog.CONCAT(pg_catalog.inet_server_addr(), ':', pg_catalog.inet_server_port())"
    _HOST_ID_QUERY = f"SELECT pg_catalog.inet_server_addr() AS host, {_HOST_ID_EXPRESSION} AS host_id"
    _IS_READER_QUERY = "SELECT pg_catalog.pg_is_in_recovery()"

    @property
    def default_port(self) -> int:
        return 5432

    @property
    def host_alias_query(self) -> str:
        return f"SELECT {self._HOST_ID_EXPRESSION}"

    @property
    def server_version_query(self) -> str:
        return "SELECT 'version', pg_catalog.VERSION()"

    @property
    def host_id_query(self) -> str:
        return self._HOST_ID_QUERY

    @property
    def is_reader_query(self) -> str:
        return self._IS_READER_QUERY

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return PgDatabaseDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        if PgDatabaseDialect._exception_handler is None:
            PgDatabaseDialect._exception_handler = Utils.initialize_class(
                "aws_advanced_python_wrapper.utils.pg_exception_handler.SingleAzPgExceptionHandler")
        return PgDatabaseDialect._exception_handler

    _PG_CATALOG_EXISTS_QUERY = 'SELECT 1 FROM pg_catalog.pg_proc LIMIT 1'
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(_PG_CATALOG_EXISTS_QUERY),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return PgDatabaseDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)

    def prepare_conn_props(self, props: Properties):
        pass


class BlueGreenDialect(ABC):
    @property
    @abstractmethod
    def blue_green_status_query(self) -> str:
        ...

    @abstractmethod
    def is_blue_green_status_available(self, conn: Connection) -> bool:
        ...


class RdsMysqlDialect(MysqlDatabaseDialect, BlueGreenDialect):
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.AURORA_MYSQL, DialectCode.GLOBAL_AURORA_MYSQL, DialectCode.MULTI_AZ_CLUSTER_MYSQL)

    _HOST_ID_QUERY = ("SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) "
                      "FROM mysql.rds_topology "
                      "WHERE id = @@server_id")
    _BG_STATUS_QUERY = "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
    _BG_STATUS_EXISTS_QUERY = \
        "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"

    @property
    def host_id_query(self) -> str:
        return self._HOST_ID_QUERY

    # "source distribution" in the version comment is how an RDS MySQL host
    # identifies itself, and report_host must be set for the topology
    # queries to return anything usable.
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(MysqlDatabaseDialect._SERVER_VERSION_QUERY,
             col_equals_ignore_case(1, "source distribution")),
        Rule(MysqlDatabaseDialect._REPORT_HOST_QUERY, col_non_empty_str(1)),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return RdsMysqlDialect._VERIFICATION_RULES

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return RdsMysqlDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def blue_green_status_query(self) -> str:
        return RdsMysqlDialect._BG_STATUS_QUERY

    def is_blue_green_status_available(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(RdsMysqlDialect._BG_STATUS_EXISTS_QUERY)
                return cursor.fetchone() is not None
        except Exception:
            return False


class RdsPgDialect(PgDatabaseDialect, BlueGreenDialect):
    _EXTENSIONS_QUERY = ("SELECT (setting OPERATOR(pg_catalog.~~) '%rds_tools%') AS rds_tools, "
                         "(setting OPERATOR(pg_catalog.~~) '%aurora_stat_utils%') AS aurora_stat_utils "
                         "FROM pg_catalog.pg_settings "
                         "WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'")
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.AURORA_PG, DialectCode.GLOBAL_AURORA_PG, DialectCode.MULTI_AZ_CLUSTER_PG)

    _HOST_ID_QUERY = ("SELECT id, SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) "
                      "FROM rds_tools.show_topology() "
                      "WHERE id OPERATOR(pg_catalog.=) rds_tools.dbi_resource_id()")
    _BG_STATUS_QUERY = (f"SELECT version, endpoint, port, role, status "
                        f"FROM rds_tools.show_topology('aws_advanced_python_wrapper-{DriverInfo.DRIVER_VERSION}')")
    _BG_STATUS_EXISTS_QUERY = "SELECT 'rds_tools.show_topology'::pg_catalog.regproc"

    @property
    def host_id_query(self) -> str:
        return self._HOST_ID_QUERY

    _VERIFICATION_RULES = DialectVerificationRules(
        inherits=PgDatabaseDialect._VERIFICATION_RULES,
        rules=(
            Rule(_EXTENSIONS_QUERY,
                 all_of(col_truthy(0), col_falsy(1)),
                 match_any_row=True),
        ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return RdsPgDialect._VERIFICATION_RULES

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return RdsPgDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def blue_green_status_query(self) -> str:
        return RdsPgDialect._BG_STATUS_QUERY

    def is_blue_green_status_available(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(RdsPgDialect._BG_STATUS_EXISTS_QUERY)
                return cursor.fetchone() is not None
        except Exception:
            return False


class AuroraMysqlDialect(MysqlDatabaseDialect, TopologyAwareDatabaseDialect, BlueGreenDialect):
    _DIALECT_UPDATE_CANDIDATES = (DialectCode.GLOBAL_AURORA_MYSQL, DialectCode.MULTI_AZ_CLUSTER_MYSQL)
    _TOPOLOGY_QUERY = ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
                       "CPU, REPLICA_LAG_IN_MILLISECONDS, LAST_UPDATE_TIMESTAMP "
                       "FROM information_schema.replica_host_status "
                       "WHERE time_to_sec(timediff(now(), LAST_UPDATE_TIMESTAMP)) <= 300 "
                       "OR SESSION_ID = 'MASTER_SESSION_ID' ")
    _HOST_ID_QUERY = "SELECT @@aurora_server_id, @@aurora_server_id"
    _IS_READER_QUERY = "SELECT @@innodb_read_only"
    _WRITER_HOST_QUERY = \
        ("SELECT SERVER_ID FROM information_schema.replica_host_status "
         "WHERE SESSION_ID = 'MASTER_SESSION_ID' AND SERVER_ID = @@aurora_server_id")

    _BG_STATUS_QUERY = "SELECT version, endpoint, port, role, status FROM mysql.rds_topology"
    _BG_STATUS_EXISTS_QUERY = \
        "SELECT 1 AS tmp FROM information_schema.tables WHERE table_schema = 'mysql' AND table_name = 'rds_topology'"
    _AURORA_VERSION_QUERY = "SHOW VARIABLES LIKE 'aurora_version'"

    @property
    def is_reader_query(self) -> str:
        return self._IS_READER_QUERY

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return AuroraMysqlDialect._DIALECT_UPDATE_CANDIDATES

    # The variable existing at all means an Aurora cluster.
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(_AURORA_VERSION_QUERY),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return AuroraMysqlDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: RdsHostListProvider(
            provider_service,
            plugin_service,
            props,
            AuroraTopologyUtils(self, props))

    def get_async_host_list_provider_supplier(self) -> Callable:
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            create_aurora_provider
        return lambda provider_service, props: create_aurora_provider(
            provider_service, props,
            topology_query=self.topology_query,
            default_port=self.default_port)

    @property
    def blue_green_status_query(self) -> str:
        return AuroraMysqlDialect._BG_STATUS_QUERY

    def is_blue_green_status_available(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(AuroraMysqlDialect._BG_STATUS_EXISTS_QUERY)
                return cursor.fetchone() is not None
        except Exception:
            return False


class AuroraPgDialect(PgDatabaseDialect, TopologyAwareDatabaseDialect, AuroraLimitlessDialect, BlueGreenDialect):
    _DIALECT_UPDATE_CANDIDATES: Tuple[DialectCode, ...] = (DialectCode.GLOBAL_AURORA_PG, DialectCode.MULTI_AZ_CLUSTER_PG)

    _AURORA_UTILS_EXIST_QUERY = "SELECT (setting OPERATOR(pg_catalog.~~) '%aurora_stat_utils%') AS aurora_stat_utils " \
                                "FROM pg_catalog.pg_settings WHERE name OPERATOR(pg_catalog.=) 'rds.extensions'"

    _HAS_TOPOLOGY_QUERY = "SELECT 1 FROM pg_catalog.aurora_replica_status() LIMIT 1"

    _TOPOLOGY_QUERY = \
        ("SELECT SERVER_ID, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
         "CPU, COALESCE(REPLICA_LAG_IN_MSEC, 0), LAST_UPDATE_TIMESTAMP "
         "FROM pg_catalog.aurora_replica_status() "
         "WHERE EXTRACT(EPOCH FROM(pg_catalog.NOW() OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) OPERATOR(pg_catalog.<=) 300 "
         "OR SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
         "OR LAST_UPDATE_TIMESTAMP IS NULL")

    _HOST_ID_QUERY = "SELECT pg_catalog.aurora_db_instance_identifier(), pg_catalog.aurora_db_instance_identifier()"
    _LIMITLESS_ROUTER_ENDPOINT_QUERY = "SELECT router_endpoint, load FROM pg_catalog.aurora_limitless_router_endpoints()"

    _BG_STATUS_QUERY = (f"SELECT version, endpoint, port, role, status "
                        f"FROM pg_catalog.get_blue_green_fast_switchover_metadata('aws_advanced_python_wrapper-{DriverInfo.DRIVER_VERSION}')")
    _BG_STATUS_EXISTS_QUERY = "SELECT 'pg_catalog.get_blue_green_fast_switchover_metadata'::pg_catalog.regproc"
    _WRITER_HOST_QUERY = \
        ("SELECT SERVER_ID FROM pg_catalog.aurora_replica_status() "
         "WHERE SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
         "AND SERVER_ID OPERATOR(pg_catalog.=) pg_catalog.aurora_db_instance_identifier()")

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return AuroraPgDialect._DIALECT_UPDATE_CANDIDATES

    # Aurora PG needs both the aurora_stat_utils extension and the
    # topology function; plain RDS PG has neither.
    _VERIFICATION_RULES = DialectVerificationRules(
        inherits=PgDatabaseDialect._VERIFICATION_RULES,
        rules=(
            Rule(_AURORA_UTILS_EXIST_QUERY, col_truthy(0)),
            Rule(_HAS_TOPOLOGY_QUERY),
        ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return AuroraPgDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: RdsHostListProvider(
            provider_service,
            plugin_service,
            props,
            AuroraTopologyUtils(self, props))

    def get_async_host_list_provider_supplier(self) -> Callable:
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            create_aurora_provider
        return lambda provider_service, props: create_aurora_provider(
            provider_service, props,
            topology_query=self.topology_query,
            default_port=self.default_port)

    @property
    def blue_green_status_query(self) -> str:
        return AuroraPgDialect._BG_STATUS_QUERY

    def is_blue_green_status_available(self, conn: Connection) -> bool:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(AuroraPgDialect._BG_STATUS_EXISTS_QUERY)
                return cursor.fetchone() is not None
        except Exception:
            return False


class GlobalAuroraMysqlDialect(AuroraMysqlDialect, GlobalAuroraTopologyDialect):
    _GLOBAL_STATUS_TABLE_EXISTS_QUERY = \
        ("SELECT 1 AS tmp FROM information_schema.tables WHERE"
         " upper(table_schema) = 'INFORMATION_SCHEMA' AND upper(table_name) = 'AURORA_GLOBAL_DB_STATUS'")
    _GLOBAL_INSTANCE_STATUS_EXISTS_QUERY = \
        ("SELECT 1 AS tmp FROM information_schema.tables WHERE"
         " upper(table_schema) = 'INFORMATION_SCHEMA' AND upper(table_name) = 'AURORA_GLOBAL_DB_INSTANCE_STATUS'")
    _TOPOLOGY_QUERY = \
        ("SELECT SERVER_ID, CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
         "VISIBILITY_LAG_IN_MSEC, AWS_REGION "
         "FROM information_schema.aurora_global_db_instance_status ")
    _REGION_COUNT_QUERY = "SELECT count(1) FROM information_schema.aurora_global_db_status"
    _REGION_BY_INSTANCE_ID_QUERY = \
        "SELECT AWS_REGION FROM information_schema.aurora_global_db_instance_status WHERE SERVER_ID = %s"

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return None

    # Both global tables must exist AND report more than one region --
    # a single-region Aurora cluster has the tables but is not a global
    # database.
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(_GLOBAL_STATUS_TABLE_EXISTS_QUERY),
        Rule(_GLOBAL_INSTANCE_STATUS_EXISTS_QUERY),
        Rule(_REGION_COUNT_QUERY, col_greater_than(0, 1)),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return GlobalAuroraMysqlDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: GlobalAuroraHostListProvider(
            provider_service,
            plugin_service,
            props,
            GlobalAuroraTopologyUtils(self, props))

    def get_async_host_list_provider_supplier(self) -> Callable:
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            create_global_aurora_provider
        return lambda provider_service, props: create_global_aurora_provider(
            provider_service, props,
            topology_query=self.topology_query,
            default_port=self.default_port)

    def filter_available_hosts(
        self,
        hosts: Sequence[HostInfo],
        accessible_regions: Optional[FrozenSet[str]],
    ) -> List[HostInfo]:
        return AccessibleRegions.filter_available_hosts(hosts, accessible_regions)


class GlobalAuroraPgDialect(AuroraPgDialect, GlobalAuroraTopologyDialect):
    _GLOBAL_STATUS_TABLE_EXISTS_QUERY = "SELECT 'pg_catalog.aurora_global_db_status'::pg_catalog.regproc"
    _GLOBAL_INSTANCE_STATUS_EXISTS_QUERY = "SELECT 'pg_catalog.aurora_global_db_instance_status'::pg_catalog.regproc"
    _TOPOLOGY_QUERY = \
        ("SELECT SERVER_ID, CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN TRUE ELSE FALSE END, "
         "VISIBILITY_LAG_IN_MSEC, AWS_REGION "
         "FROM pg_catalog.aurora_global_db_instance_status()")
    _REGION_COUNT_QUERY = "SELECT pg_catalog.count(1) FROM pg_catalog.aurora_global_db_status()"
    _REGION_BY_INSTANCE_ID_QUERY = \
        "SELECT AWS_REGION FROM pg_catalog.aurora_global_db_instance_status() WHERE SERVER_ID OPERATOR(pg_catalog.=) %s"

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return None

    # Aurora PG's extension first, then both global views, then more than
    # one region -- a single-region cluster is not a global database.
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(AuroraPgDialect._AURORA_UTILS_EXIST_QUERY, col_truthy(0)),
        Rule(_GLOBAL_STATUS_TABLE_EXISTS_QUERY),
        Rule(_GLOBAL_INSTANCE_STATUS_EXISTS_QUERY),
        Rule(_REGION_COUNT_QUERY, col_greater_than(0, 1)),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return GlobalAuroraPgDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: GlobalAuroraHostListProvider(
            provider_service,
            plugin_service,
            props,
            GlobalAuroraTopologyUtils(self, props))

    def get_async_host_list_provider_supplier(self) -> Callable:
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            create_global_aurora_provider
        return lambda provider_service, props: create_global_aurora_provider(
            provider_service, props,
            topology_query=self.topology_query,
            default_port=self.default_port)

    def filter_available_hosts(
        self,
        hosts: Sequence[HostInfo],
        accessible_regions: Optional[FrozenSet[str]],
    ) -> List[HostInfo]:
        return AccessibleRegions.filter_available_hosts(hosts, accessible_regions)


class MultiAzClusterMysqlDialect(MysqlDatabaseDialect, TopologyAwareDatabaseDialect):
    _TOPOLOGY_QUERY = "SELECT id, endpoint, port FROM mysql.rds_topology"
    _WRITER_HOST_QUERY = "SHOW REPLICA STATUS"
    _WRITER_HOST_COLUMN_INDEX = 39
    _HOST_ID_QUERY = ("SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) "
                      "FROM mysql.rds_topology "
                      "WHERE id = @@server_id")

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return None

    # The Multi-AZ topology view returning rows, plus report_host set so
    # those rows can be matched to this host.
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(_TOPOLOGY_QUERY, match_any_row=True),
        Rule(MysqlDatabaseDialect._REPORT_HOST_QUERY, col_non_empty_str(1)),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return MultiAzClusterMysqlDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: RdsHostListProvider(
            provider_service,
            plugin_service,
            props,
            MultiAzTopologyUtils(self, props, self._WRITER_HOST_QUERY, self._WRITER_HOST_COLUMN_INDEX))

    def get_async_host_list_provider_supplier(self) -> Callable:
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            create_multi_az_provider
        return lambda provider_service, props: create_multi_az_provider(
            provider_service, props,
            writer_host_query=self.writer_id_query,
            topology_query=self.topology_query,
            host_id_query=self.host_id_query,
            default_port=self.default_port,
            writer_host_column_index=self._WRITER_HOST_COLUMN_INDEX)

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


class MultiAzClusterPgDialect(PgDatabaseDialect, TopologyAwareDatabaseDialect):
    # The driver name passed to show_topology is used for RDS metrics purposes.
    # It is not required for functional correctness.
    _TOPOLOGY_QUERY = \
        f"SELECT id, endpoint, port FROM rds_tools.show_topology('aws_python_driver-{DriverInfo.DRIVER_VERSION}')"
    _WRITER_HOST_QUERY = \
        "SELECT multi_az_db_cluster_source_dbi_resource_id FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()"
    _HOST_ID_QUERY = ("SELECT id, SUBSTRING(endpoint FROM 0 FOR POSITION('.' IN endpoint)) "
                      "FROM rds_tools.show_topology() "
                      "WHERE id OPERATOR(pg_catalog.=) rds_tools.dbi_resource_id()")
    _exception_handler: Optional[ExceptionHandler] = None

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return None

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        if MultiAzClusterPgDialect._exception_handler is None:
            MultiAzClusterPgDialect._exception_handler = Utils.initialize_class(
                "aws_advanced_python_wrapper.utils.pg_exception_handler.MultiAzPgExceptionHandler")
        return MultiAzClusterPgDialect._exception_handler

    # The writer query answering with an id: only a Multi-AZ PG cluster
    # exposes rds_tools.multi_az_db_cluster_source_dbi_resource_id().
    _VERIFICATION_RULES = DialectVerificationRules(rules=(
        Rule(_WRITER_HOST_QUERY, col_truthy(0)),
    ))

    @property
    def verification_rules(self) -> Optional[DialectVerificationRules]:
        return MultiAzClusterPgDialect._VERIFICATION_RULES

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: RdsHostListProvider(
            provider_service,
            plugin_service,
            props,
            MultiAzTopologyUtils(self, props, self._WRITER_HOST_QUERY))

    def get_async_host_list_provider_supplier(self) -> Callable:
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            create_multi_az_provider

        # No writer_host_column_index: the PG writer query returns a single
        # column.
        return lambda provider_service, props: create_multi_az_provider(
            provider_service, props,
            writer_host_query=self.writer_id_query,
            topology_query=self.topology_query,
            host_id_query=self.host_id_query,
            default_port=self.default_port)


class UnknownDatabaseDialect(DatabaseDialect):
    _DIALECT_UPDATE_CANDIDATES: Optional[Tuple[DialectCode, ...]] = \
        (DialectCode.MYSQL,
         DialectCode.PG,
         DialectCode.RDS_MYSQL,
         DialectCode.RDS_PG,
         DialectCode.AURORA_MYSQL,
         DialectCode.AURORA_PG,
         DialectCode.MULTI_AZ_CLUSTER_MYSQL,
         DialectCode.MULTI_AZ_CLUSTER_PG)

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
    def host_id_query(self) -> str:
        raise UnsupportedOperationError(
            Messages.get_formatted("UnknownDialect.UnsupportedMethod", "host_id_query"))

    @property
    def is_reader_query(self) -> str:
        raise UnsupportedOperationError(
            Messages.get_formatted("UnknownDialect.UnsupportedMethod", "is_reader_query"))

    @property
    def dialect_update_candidates(self) -> Optional[Tuple[DialectCode, ...]]:
        return UnknownDatabaseDialect._DIALECT_UPDATE_CANDIDATES

    @property
    def exception_handler(self) -> Optional[ExceptionHandler]:
        return None

    def get_host_list_provider_supplier(self, plugin_service: PluginService) -> Callable:
        return lambda provider_service, props: ConnectionStringHostListProvider(provider_service, props)

    def prepare_conn_props(self, props: Properties):
        pass


class DatabaseDialectManager(DatabaseDialectProvider):
    _ENDPOINT_CACHE_EXPIRATION_NS = 30 * 60_000_000_000  # 30 minutes
    _known_endpoint_dialects: CacheMap[str, DialectCode] = CacheMap()
    _custom_dialect: Optional[DatabaseDialect] = None
    _executor_name: ClassVar[str] = "DatabaseDialectManagerExecutor"
    _known_dialects_by_code: Dict[DialectCode, DatabaseDialect] = {
        DialectCode.MYSQL: MysqlDatabaseDialect(),
        DialectCode.RDS_MYSQL: RdsMysqlDialect(),
        DialectCode.AURORA_MYSQL: AuroraMysqlDialect(),
        DialectCode.GLOBAL_AURORA_MYSQL: GlobalAuroraMysqlDialect(),
        DialectCode.MULTI_AZ_CLUSTER_MYSQL: MultiAzClusterMysqlDialect(),
        DialectCode.PG: PgDatabaseDialect(),
        DialectCode.RDS_PG: RdsPgDialect(),
        DialectCode.AURORA_PG: AuroraPgDialect(),
        DialectCode.GLOBAL_AURORA_PG: GlobalAuroraPgDialect(),
        DialectCode.MULTI_AZ_CLUSTER_PG: MultiAzClusterPgDialect(),
        DialectCode.UNKNOWN: UnknownDatabaseDialect()
    }

    def __init__(self, props: Properties, rds_helper: Optional[RdsUtils] = None):
        self._props: Properties = props
        self._rds_helper: RdsUtils = rds_helper if rds_helper else RdsUtils()
        self._can_update: bool = False
        # Complement of _can_update: set wherever the resolved dialect is
        # final. Tracked separately because _can_update also goes False on
        # the paths that give up (unknown dialect), where the answer is NOT
        # confirmed.
        self._confirmed: bool = False
        self._dialect: DatabaseDialect = UnknownDatabaseDialect()
        self._dialect_code: DialectCode = DialectCode.UNKNOWN
        self._thread_pool = services_container.get_thread_pool(self._executor_name)

    def is_confirmed_dialect(self) -> bool:
        return self._confirmed

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
        self._confirmed = False

        if self._custom_dialect is not None:
            self._dialect_code = DialectCode.CUSTOM
            self._dialect = self._custom_dialect
            # A custom dialect is the user's explicit choice; no verification can
            # override it.
            self._confirmed = True
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
                # Either the user named the dialect or the endpoint cache
                # already holds a verified answer for this url.
                self._confirmed = True
                self._log_current_dialect()
                return dialect
            else:
                raise AwsWrapperError(
                    Messages.get_formatted("DatabaseDialectManager.UnknownDialectCode", str(dialect_code)))

        host: str = props["host"]
        target_driver_type: TargetDriverType = self._get_target_driver_type(driver_dialect)
        if target_driver_type is TargetDriverType.MYSQL:
            rds_type = self._rds_helper.identify_rds_type(host)
            if rds_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER:
                self._can_update = False
                self._confirmed = True
                self._dialect_code = DialectCode.GLOBAL_AURORA_MYSQL
                self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.GLOBAL_AURORA_MYSQL]
                return self._dialect
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
            if rds_type == RdsUrlType.RDS_AURORA_LIMITLESS_DB_SHARD_GROUP:
                self._can_update = False
                self._confirmed = True
                self._dialect_code = DialectCode.AURORA_PG
                self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.AURORA_PG]
                return self._dialect
            if rds_type == RdsUrlType.RDS_GLOBAL_WRITER_CLUSTER:
                self._can_update = False
                self._confirmed = True
                self._dialect_code = DialectCode.GLOBAL_AURORA_PG
                self._dialect = DatabaseDialectManager._known_dialects_by_code[DialectCode.GLOBAL_AURORA_PG]
                return self._dialect
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
            self._confirmed = True
            self._log_current_dialect()
            return self._dialect

        for dialect_code, dialect_candidate in self._candidates():
            timeout_sec = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get(self._props)
            try:
                cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
                    self._thread_pool,
                    timeout_sec,
                    driver_dialect,
                    conn)(dialect_candidate.is_dialect)
                is_dialect = cursor_execute_func_with_timeout(conn, driver_dialect)
            except TimeoutError as e:
                raise QueryTimeoutError("DatabaseDialectManager.QueryForDialectTimeout") from e

            if is_dialect:
                return self._confirm_candidate(dialect_code, dialect_candidate, url, host_info)

        return self._confirm_current(url, host_info)

    async def query_for_dialect_async(
            self, url: str, host_info: Optional[HostInfo], conn: Any,
            driver_dialect: Any) -> DatabaseDialect:
        """Async counterpart of :meth:`query_for_dialect`.

        Walks the same ``dialect_update_candidates`` chain and shares the
        same bookkeeping -- the endpoint cache, ``can_update`` and
        ``confirmed`` -- so an async connect contributes to and benefits
        from the cache exactly as a sync one does.

        The timeout comes from ``asyncio.wait_for`` rather than the thread
        pool the sync path uses, since verification is awaited rather than
        offloaded. The transaction-status restore that
        ``preserve_transaction_status_with_timeout`` provides on the sync
        side is done by :class:`AsyncDialectVerifier` instead.

        Runs the verifier directly rather than through
        ``is_dialect_async`` so that a candidate that could not be verified
        is distinguishable from one that answered no. When no candidate
        matched *and* a verification failed, this raises instead of settling:
        settling would write an unverified guess into the endpoint cache,
        where every later connection to that host would reuse it.
        """
        import asyncio

        if not self._can_update:
            self._confirmed = True
            self._log_current_dialect()
            return self._dialect

        timeout_sec = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get_float(self._props)
        for dialect_code, dialect_candidate in self._candidates():
            try:
                is_dialect = await asyncio.wait_for(
                    AsyncDialectVerifier.run(
                        dialect_candidate.verification_rules, conn, driver_dialect),
                    timeout=timeout_sec)
            except (asyncio.TimeoutError, TimeoutError) as e:
                raise QueryTimeoutError(
                    Messages.get("DatabaseDialectManager.QueryForDialectTimeout")) from e
            except Exception:  # noqa: BLE001 - try the next candidate
                # Verification asks about objects a given engine may not have, so
                # one candidate erroring says nothing about the next.
                continue

            if is_dialect:
                return self._confirm_candidate(dialect_code, dialect_candidate, url, host_info)

        return self._confirm_current(url, host_info)

    def _candidates(self) -> List[Tuple[DialectCode, DatabaseDialect]]:
        """The dialects the current one says it may upgrade to, resolved."""
        dialect_candidates = self._dialect.dialect_update_candidates if self._dialect is not None else None
        if dialect_candidates is None:
            return []

        resolved = []
        for dialect_code in dialect_candidates:
            dialect_candidate = DatabaseDialectManager._known_dialects_by_code.get(dialect_code)
            if dialect_candidate is None:
                raise AwsWrapperError(Messages.get_formatted("DatabaseDialectManager.UnknownDialectCode", dialect_code))
            resolved.append((dialect_code, dialect_candidate))
        return resolved

    def _confirm_candidate(
            self, dialect_code: DialectCode, dialect_candidate: DatabaseDialect,
            url: str, host_info: Optional[HostInfo]) -> DatabaseDialect:
        """Settle on a verified candidate and remember it for this endpoint."""
        self._can_update = False
        self._confirmed = True
        self._dialect_code = dialect_code
        self._dialect = dialect_candidate
        self._cache_endpoint_dialect(url, host_info, dialect_code)
        self._log_current_dialect()
        return self._dialect

    def _confirm_current(self, url: str, host_info: Optional[HostInfo]) -> DatabaseDialect:
        """No candidate matched, so the current dialect is the answer."""
        if self._dialect_code is None or self._dialect_code == DialectCode.UNKNOWN:
            raise AwsWrapperError(Messages.get("DatabaseDialectManager.UnknownDialect"))

        self._can_update = False
        self._confirmed = True
        self._cache_endpoint_dialect(url, host_info, self._dialect_code)
        self._log_current_dialect()
        return self._dialect

    @staticmethod
    def _cache_endpoint_dialect(
            url: str, host_info: Optional[HostInfo], dialect_code: DialectCode) -> None:
        DatabaseDialectManager._known_endpoint_dialects.put(
            url, dialect_code, DatabaseDialectManager._ENDPOINT_CACHE_EXPIRATION_NS)
        if host_info is not None:
            DatabaseDialectManager._known_endpoint_dialects.put(
                host_info.url, dialect_code, DatabaseDialectManager._ENDPOINT_CACHE_EXPIRATION_NS)

    def _log_current_dialect(self):
        dialect_class = "<null>" if self._dialect is None else type(self._dialect).__name__
        logger.debug("DatabaseDialectManager.CurrentDialectCanUpdate", self._dialect_code, dialect_class, self._can_update)


class DialectUtils:
    @staticmethod
    def check_existence_queries(conn: Connection, existence_queries: Tuple[str, ...]) -> bool:
        for existence_query in existence_queries:
            with closing(conn.cursor()) as cursor:
                cursor.execute(existence_query)
                if cursor.fetchone() is None:
                    return False

        return True

    @staticmethod
    def get_host_role(conn: Connection, driver_dialect: DriverDialect, is_reader_query: str,
                      thread_pool, timeout_sec: float) -> HostRole:
        try:
            cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
                thread_pool, timeout_sec, driver_dialect, conn)(DialectUtils._execute_is_reader_query)
            result = cursor_execute_func_with_timeout(conn, is_reader_query)
            if result is not None:
                is_reader = bool(result[0])
                return HostRole.READER if is_reader else HostRole.WRITER
        except TimeoutError as e:
            raise QueryTimeoutError(Messages.get("DialectUtils.GetHostRoleTimeout")) from e
        except Exception as e:
            raise AwsWrapperError(Messages.get("DialectUtils.ErrorGettingHostRole")) from e

        raise AwsWrapperError(Messages.get("DialectUtils.ErrorGettingHostRole"))

    @staticmethod
    def _execute_is_reader_query(conn: Connection, is_reader_query: str):
        with closing(conn.cursor()) as cursor:
            cursor.execute(is_reader_query)
            return cursor.fetchone()

    @staticmethod
    def get_instance_id(conn: Connection, driver_dialect: DriverDialect, instance_id_query: str,
                        thread_pool, timeout_sec: float) -> Optional[Tuple[str, str]]:
        cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
            thread_pool, timeout_sec, driver_dialect, conn)(DialectUtils._execute_instance_id_query)
        result = cursor_execute_func_with_timeout(conn, instance_id_query)
        if result is not None and len(result) >= 2:
            return (str(result[0]), str(result[1]))

        return None

    @staticmethod
    def _execute_instance_id_query(conn: Connection, instance_id_query: str):
        with closing(conn.cursor()) as cursor:
            cursor.execute(instance_id_query)
            return cursor.fetchone()
