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

import uuid
from abc import ABC, abstractmethod
from concurrent.futures import TimeoutError
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from threading import RLock
from typing import (TYPE_CHECKING, ClassVar, List, Optional, Protocol, Tuple,
                    runtime_checkable)

from aws_advanced_python_wrapper.cluster_topology_monitor import (
    ClusterTopologyMonitor, ClusterTopologyMonitorImpl,
    GlobalAuroraTopologyMonitor)
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.decorators import \
    preserve_transaction_status_with_timeout
from aws_advanced_python_wrapper.utils.events import MonitorStopEvent

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.plugin_service import PluginService

import aws_advanced_python_wrapper.database_dialect as db_dialect
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import (
    HostAvailability, create_host_availability_strategy)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole, Topology
from aws_advanced_python_wrapper.pep249 import (Connection, Cursor,
                                                ProgrammingError)
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.utils import LogUtils

logger = Logger(__name__)


class HostListProvider(Protocol):
    def refresh(self, connection: Optional[Connection] = None) -> Topology:
        ...

    def force_refresh(self, connection: Optional[Connection] = None) -> Topology:
        ...

    def get_current_topology(self, connection: Connection, initial_host_info: HostInfo) -> Topology:
        """
        Get current topology from the given connection immediately.
        Does NOT use monitor or cache - direct query only.

        :param connection: the connection to use to fetch topology information.
        :param initial_host_info: the host details of the initial connection.
        :return: a tuple of hosts representing the database topology.
        """
        ...

    def force_monitoring_refresh(self, should_verify_writer: bool, timeout_sec: int) -> Topology:
        ...

    def get_cluster_id(self) -> str:
        ...

    def stop_monitor(self) -> None:
        ...


@runtime_checkable
class DynamicHostListProvider(HostListProvider, Protocol):
    """
    A marker interface for providers that can fetch a host list that may change over time depending on database status.

    DynamicHostListProvider instances should be used if the database has a cluster configuration where the
    cluster topology (the instances in the cluster, their roles, and their statuses) can change over time. Examples
    include Aurora DB clusters and Patroni DB clusters, among others.
    """
    ...


@runtime_checkable
class StaticHostListProvider(HostListProvider, Protocol):
    """
    A marker interface for providers that determine the host list once while initializing and assume the host list does not change.
    An example would be a provider that parses the connection string to determine host information.
    """
    ...


class HostListProviderService(Protocol):
    @property
    @abstractmethod
    def current_connection(self) -> Optional[Connection]:
        ...

    @property
    @abstractmethod
    def current_host_info(self) -> Optional[HostInfo]:
        ...

    @property
    @abstractmethod
    def database_dialect(self) -> db_dialect.DatabaseDialect:
        ...

    @property
    @abstractmethod
    def driver_dialect(self) -> DriverDialect:
        ...

    @property
    @abstractmethod
    def host_list_provider(self) -> HostListProvider:
        ...

    @host_list_provider.setter
    def host_list_provider(self, value: HostListProvider):
        ...

    @property
    @abstractmethod
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        ...

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: HostInfo):
        ...

    def is_static_host_list_provider(self) -> bool:
        ...


class RdsHostListProvider(DynamicHostListProvider, HostListProvider):
    _MONITOR_CLEANUP_NANO: ClassVar[int] = 15 * 60 * 1_000_000_000  # 15 minutes
    _DEFAULT_TOPOLOGY_QUERY_TIMEOUT_SEC: ClassVar[int] = 5

    def __init__(
            self,
            host_list_provider_service: HostListProviderService,
            plugin_service: PluginService,
            props: Properties,
            topology_utils: TopologyUtils):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        self._props: Properties = props
        self._topology_utils = topology_utils

        self._rds_utils: RdsUtils = RdsUtils()
        self._hosts: Topology = ()
        self._cluster_id: str = str(uuid.uuid4())
        self._initial_hosts: Topology = ()
        self._rds_url_type: Optional[RdsUrlType] = None

        self._is_initialized: bool = False
        self._lock: RLock = RLock()
        self._refresh_rate_ns: int = WrapperProperties.TOPOLOGY_REFRESH_MS.get_int(self._props) * 1_000_000
        self._plugin_service: PluginService = plugin_service
        self._high_refresh_rate_ns = (
            WrapperProperties.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.get_int(self._props) * 1_000_000)

        self._monitor_service = services_container.get_monitor_service()
        self._monitor_service.register_monitor_type(
            ClusterTopologyMonitorImpl,
            expiration_timeout_ns=RdsHostListProvider._MONITOR_CLEANUP_NANO,
            produced_data_type=Topology)

    def _initialize(self):
        if self._is_initialized:
            return
        with self._lock:
            if self._is_initialized:
                return

            self._init_settings()
            self._is_initialized = True

    def _init_settings(self):
        """Initialize settings - can be overridden by subclasses"""
        self._initial_hosts: Topology = (self._topology_utils.initial_host_info,)
        self._host_list_provider_service.initial_connection_host_info = self._topology_utils.initial_host_info

        self._rds_url_type: RdsUrlType = self._rds_utils.identify_rds_type(self._topology_utils.initial_host_info.host)
        cluster_id = WrapperProperties.CLUSTER_ID.get(self._props)
        if cluster_id:
            self._cluster_id = cluster_id

    def _get_topology(self, conn: Optional[Connection], force_update: bool = False) -> FetchTopologyResult:
        """
        Get topology information for the database cluster. This method executes a database query if `force_update` is True,
        if there is no information for the cluster in the cache, or if the cached topology is outdated.
        Otherwise, the cached topology will be returned.

        :param conn: the connection to use to fetch topology information, if necessary.
        :param force_update: set to true to force the driver to query the database for
        up-to-date topology information instead of relying on any cached information.
        :return: a :py:class:`FetchTopologyResult` object containing the topology information
        and whether the information came from the cache or a database query.
        If the database was queried and the results did not include a writer instance, the topology information tuple will be empty.
        """
        self._initialize()

        cached_hosts = services_container.get_storage_service().get(Topology, self._cluster_id)
        if not cached_hosts or force_update:
            if not conn:
                # Cannot fetch topology without a connection
                # Return the original hosts passed to the connect method
                return RdsHostListProvider.FetchTopologyResult(self._initial_hosts, False)

            hosts = self._force_refresh_monitor(False, self._DEFAULT_TOPOLOGY_QUERY_TIMEOUT_SEC)
            if hosts is not None and len(hosts) > 0:
                return RdsHostListProvider.FetchTopologyResult(hosts, False)

        if cached_hosts:
            return RdsHostListProvider.FetchTopologyResult(cached_hosts, True)
        else:
            return RdsHostListProvider.FetchTopologyResult(self._initial_hosts, False)

    def _get_or_create_monitor(self) -> Optional[ClusterTopologyMonitor]:
        """Get or create monitor - matches Java's getOrCreateMonitor"""
        return self._monitor_service.run_if_absent(
            ClusterTopologyMonitorImpl,
            self.get_cluster_id(),
            lambda: ClusterTopologyMonitorImpl(
                self._plugin_service,
                self._topology_utils,
                self._cluster_id,
                self._topology_utils.initial_host_info,
                self._props,
                self._topology_utils.instance_template,
                self._refresh_rate_ns,
                self._high_refresh_rate_ns
            )
        )

    def _force_refresh_monitor(self, should_verify_writer: bool, timeout_sec: int) -> Optional[Topology]:
        """Force refresh using monitor - matches Java's forceRefreshMonitor"""
        monitor = self._get_or_create_monitor()
        if monitor is None:
            return None
        try:
            return monitor.force_refresh(should_verify_writer, timeout_sec)
        except TimeoutError:
            return None

    def get_current_topology(self, connection: Connection, initial_host_info: HostInfo) -> Topology:
        """
        Get current topology from the given connection immediately.
        Does NOT use monitor or cache - direct query only.
        Equivalent to Java's getCurrentTopology.

        :param connection: the connection to use to fetch topology information.
        :param initial_host_info: the host details of the initial connection.
        :return: a tuple of hosts representing the database topology.
        """
        self._initialize()
        driver_dialect = self._host_list_provider_service.driver_dialect
        hosts = self._topology_utils.query_for_topology(connection, driver_dialect)
        if hosts:
            return hosts
        return ()

    def force_monitoring_refresh(self, should_verify_writer: bool, timeout_sec: int) -> Topology:
        """Public API for forcing monitor refresh"""
        self._initialize()
        hosts = self._force_refresh_monitor(should_verify_writer, timeout_sec)
        return hosts if hosts else ()

    def refresh(self, connection: Optional[Connection] = None) -> Topology:
        """
        Get topology information for the database cluster.
        This method executes a database query if there is no information for the cluster in the cache, or if the cached topology is outdated.
        Otherwise, the cached topology will be returned.

        :param connection: the connection to use to fetch topology information, if necessary.
        :return: a tuple of hosts representing the database topology.
        An empty tuple will be returned if the query results did not include a writer instance.
        """
        self._initialize()
        connection = connection if connection else self._host_list_provider_service.current_connection
        topology = self._get_topology(connection, False)
        logger.debug("LogUtils.Topology", LogUtils.log_topology(topology.hosts))
        self._hosts = topology.hosts
        return tuple(self._hosts)

    def force_refresh(self, connection: Optional[Connection] = None) -> Topology:
        """
        Execute a database query to retrieve information for the current cluster topology. Any cached topology information will be ignored.

        :param connection: the connection to use to fetch topology information.
        :return: a tuple of hosts representing the database topology.
        An empty tuple will be returned if the query results did not include a writer instance.
        """

        self._initialize()
        connection = connection if connection else self._host_list_provider_service.current_connection
        topology = self._get_topology(connection, True)
        logger.debug("LogUtils.Topology", LogUtils.log_topology(topology.hosts))
        self._hosts = topology.hosts
        return tuple(self._hosts)

    def get_cluster_id(self):
        self._initialize()
        return self._cluster_id

    def stop_monitor(self) -> None:
        services_container.get_event_publisher().publish(
            MonitorStopEvent(monitor_type=ClusterTopologyMonitorImpl, key=self._cluster_id))

    @dataclass()
    class FetchTopologyResult:
        hosts: Topology
        is_cached_data: bool


class ConnectionStringHostListProvider(StaticHostListProvider):

    def __init__(self, host_list_provider_service: HostListProviderService, props: Properties):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        self._props: Properties = props
        self._hosts: Topology = ()
        self._is_initialized: bool = False
        self._initial_host_info: Optional[HostInfo] = None

    def _initialize(self):
        if self._is_initialized:
            return

        self._initial_host_info = HostInfo(
            host=self._props.get("host"),
            port=self._props.get("port", HostInfo.NO_PORT),
            host_availability_strategy=create_host_availability_strategy(self._props))

        self._hosts += (self._initial_host_info,)

        self._host_list_provider_service.initial_connection_host_info = self._initial_host_info
        self._is_initialized = True

    def refresh(self, connection: Optional[Connection] = None) -> Topology:
        self._initialize()
        return tuple(self._hosts)

    def force_refresh(self, connection: Optional[Connection] = None) -> Topology:
        self._initialize()
        return tuple(self._hosts)

    def get_current_topology(self, connection: Connection, initial_host_info: HostInfo) -> Topology:
        self._initialize()
        return tuple(self._hosts)

    def force_monitoring_refresh(self, should_verify_writer: bool, timeout_sec: int) -> Topology:
        raise AwsWrapperError(
                Messages.get_formatted("HostListProvider.ForceMonitoringRefreshUnsupported", "ConnectionStringHostListProvider"))

    def get_cluster_id(self):
        return "<none>"

    def stop_monitor(self) -> None:
        pass


class GlobalAuroraHostListProvider(RdsHostListProvider):
    _global_topology_utils: GlobalAuroraTopologyUtils

    def __init__(
            self,
            host_list_provider_service: HostListProviderService,
            plugin_service: PluginService,
            props: Properties,
            topology_utils: GlobalAuroraTopologyUtils
            ):
        super().__init__(host_list_provider_service, plugin_service, props, topology_utils)
        self._global_topology_utils: GlobalAuroraTopologyUtils = topology_utils
        self._instance_templates_by_region: dict[str, HostInfo] = {}

    def _init_settings(self):
        """Override to add global cluster specific initialization"""
        super()._init_settings()

        instance_templates_str = WrapperProperties.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.get(self._props)
        self._instance_templates_by_region = \
            self._global_topology_utils.parse_instance_templates(instance_templates_str)

    def _get_or_create_monitor(self) -> Optional[ClusterTopologyMonitor]:
        """Override to create GlobalAuroraTopologyMonitor"""
        return self._monitor_service.run_if_absent(
            ClusterTopologyMonitorImpl,
            self.get_cluster_id(),
            lambda: GlobalAuroraTopologyMonitor(
                self._plugin_service,
                self._global_topology_utils,
                self._cluster_id,
                self._topology_utils.initial_host_info,
                self._props,
                self._topology_utils.instance_template,
                self._refresh_rate_ns,
                self._high_refresh_rate_ns,
                self._instance_templates_by_region
            )
        )

    def get_current_topology(self, connection: Connection, initial_host_info: HostInfo) -> Topology:
        """Override to use region-specific templates"""
        self._initialize()
        hosts = self._global_topology_utils.query_for_topology_with_regions(
            connection, self._instance_templates_by_region)
        if hosts:
            return hosts
        return ()


class TopologyUtils(ABC):
    """
    An abstract class defining utility methods that can be used to retrieve and process
    database topology information. This class can be overridden to define logic specific
    to various database engine deployments (e.g. Aurora, Multi-AZ, etc.).
    """

    _executor_name: ClassVar[str] = "TopologyUtils"

    def __init__(self, dialect: db_dialect.TopologyAwareDatabaseDialect, props: Properties):
        self._dialect: db_dialect.TopologyAwareDatabaseDialect = dialect
        self._rds_utils = RdsUtils()
        self._host_availability_strategy = create_host_availability_strategy(props)
        self.initial_host_info: HostInfo = HostInfo(
                host=str(props.get("host")),
                port=props.get("port", HostInfo.NO_PORT),
                host_availability_strategy=self._host_availability_strategy)

        host_pattern = WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.get(props)
        if host_pattern:
            if host_pattern.find(":") > -1:
                host_pattern, port_str = host_pattern.split(":")
                port = int(port_str)
            else:
                port = HostInfo.NO_PORT

            instance_template = HostInfo(
                host=host_pattern,
                port=port,
                host_availability_strategy=self._host_availability_strategy)
        else:
            instance_template = HostInfo(
                host=self._rds_utils.get_rds_instance_host_pattern(self.initial_host_info.host),
                host_id=self.initial_host_info.host_id,
                port=self.initial_host_info.port,
                host_availability_strategy=self._host_availability_strategy)
        self._validate_host_pattern(instance_template.host)

        self.instance_template: HostInfo = instance_template
        self._max_timeout_sec = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get_int(props)
        self._thread_pool = services_container.get_thread_pool(self._executor_name)

    def _validate_host_pattern(self, host: str):
        if not self._rds_utils.is_dns_pattern_valid(host):
            message = "RdsHostListProvider.InvalidPattern"
            logger.error(message)
            raise AwsWrapperError(Messages.get(message))

        url_type = self._rds_utils.identify_rds_type(host)
        if url_type == RdsUrlType.RDS_PROXY:
            message = "RdsHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSProxy"
            logger.error(message)
            raise AwsWrapperError(Messages.get(message))

        if url_type == RdsUrlType.RDS_CUSTOM_CLUSTER:
            message = "RdsHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSCustom"
            logger.error(message)
            raise AwsWrapperError(Messages.get(message))

    def query_for_topology(
        self,
        conn: Connection,
        driver_dialect: DriverDialect,
    ) -> Optional[Topology]:
        """
        Query the database for topology information.

        :param conn: the connection to use to fetch topology information.
        :return: a tuple of :py:class:`HostInfo` objects representing the database topology. If the query results did not include a writer instance,
        an empty tuple will be returned.
        """
        query_for_topology_func_with_timeout = preserve_transaction_status_with_timeout(
                    self._thread_pool, self._max_timeout_sec, driver_dialect, conn)(self._query_for_topology)
        x = query_for_topology_func_with_timeout(conn)
        return x

    @abstractmethod
    def _query_for_topology(self, conn: Connection) -> Optional[Topology]:
        pass

    def _create_host(self, record: Tuple) -> HostInfo:
        """
        Convert a topology query record into a :py:class:`HostInfo`
        object containing the information for a database instance in the cluster.

        :param record: a query record containing information about a database instance in the cluster.
        :return: a :py:class:`HostInfo` object representing a database instance in the cluster.
        """

        # According to TopologyAwareDatabaseDialect.topology_query the result set
        # should contain 4 columns: instance ID, 1/0 (writer/reader), CPU utilization, host lag in ms.
        # There might be a 5th column specifying the last update time.
        if not self.instance_template:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.UninitializedClusterInstanceTemplate"))
        if not self.initial_host_info:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.UninitializedInitialHostInfo"))

        host_id: str = record[0]
        is_writer: bool = record[1]
        last_update: datetime
        if len(record) > 4 and isinstance(record[4], datetime):
            last_update = record[4]
        else:
            last_update = datetime.now()

        host_id = host_id if host_id else "?"
        return self.create_host(host_id, is_writer, last_update, self.instance_template, self.initial_host_info)

    def create_host(
            self,
            host_id: str,
            is_writer: bool,
            last_update: datetime,
            cluster_instance_template: HostInfo,
            initial_host_info: HostInfo
            ) -> HostInfo:
        endpoint = cluster_instance_template.host.replace("?", host_id)
        port = cluster_instance_template.port \
            if cluster_instance_template.is_port_specified() \
            else initial_host_info.port

        host_info = HostInfo(
            host=endpoint,
            port=port,
            availability=HostAvailability.AVAILABLE,
            host_availability_strategy=self._host_availability_strategy,
            role=HostRole.WRITER if is_writer else HostRole.READER,
            last_update_time=last_update,
            host_id=host_id)
        host_info.add_alias(host_id)
        return host_info

    def get_writer_id_if_connected(self, connection: Connection, driver_dialect: DriverDialect) -> Optional[str]:
        try:
            cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
                self._thread_pool, self._max_timeout_sec, driver_dialect, connection)(self._get_writer_id)
            result = cursor_execute_func_with_timeout(connection)
            if result:
                host_id: str = result[0]
                return host_id
            return None
        except Exception:
            return None

    def _get_writer_id(self, conn: Connection):
        with closing(conn.cursor()) as cursor:
            cursor.execute(self._dialect.writer_id_query)
            return cursor.fetchone()


class AuroraTopologyUtils(TopologyUtils):

    _executor_name: ClassVar[str] = "AuroraTopologyUtils"

    def _query_for_topology(self, conn: Connection) -> Optional[Topology]:
        """
        Query the database for topology information.

        :param conn: the connection to use to fetch topology information.
        :return: a tuple of :py:class:`HostInfo` objects representing the database topology. If the query results did not include a writer instance,
        an empty tuple will be returned.
        """
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._dialect.topology_query)
                return self._process_query_results(cursor)
        except ProgrammingError as e:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.InvalidQuery"), e) from e

    def _process_query_results(self, cursor: Cursor) -> Topology:
        """
        Form a list of hosts from the results of the topology query.
        :param cursor: The Cursor object containing a reference to the results of the topology query.
        :return: a tuple of hosts representing the database topology.
        An empty tuple will be returned if the query results did not include a writer instance.
        """
        host_map = {}
        for record in cursor:
            host: HostInfo = self._create_host(record)
            host_map[host.host] = host

        hosts = []
        writers = []
        for host in host_map.values():
            if host.role == HostRole.WRITER:
                writers.append(host)
            else:
                hosts.append(host)

        if len(writers) == 0:
            logger.error("RdsHostListProvider.InvalidTopology")
            hosts.clear()
        elif len(writers) == 1:
            hosts.append(writers[0])
        else:
            # Take the latest updated writer host as the current writer. All others will be ignored.
            existing_writers: List[HostInfo] = [x for x in writers if x is not None]
            existing_writers.sort(reverse=True, key=lambda h: h.last_update_time is not None and h.last_update_time)
            hosts.append(existing_writers[0])

        return tuple(hosts)


class MultiAzTopologyUtils(TopologyUtils):

    _executor_name: ClassVar[str] = "MultiAzTopologyUtils"

    def __init__(
        self,
        dialect: db_dialect.TopologyAwareDatabaseDialect,
        props: Properties,
        writer_host_query: str,
        writer_host_column_index: int = 0
    ):
        super().__init__(dialect, props)
        self._writer_host_query = writer_host_query
        self._writer_host_column_index = writer_host_column_index

    def _query_for_topology(self, conn: Connection) -> Optional[Topology]:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._writer_host_query)
                row = cursor.fetchone()
                if row is not None:
                    writer_id = row[self._writer_host_column_index]
                else:
                    # In MySQL, the writer host query above will be empty if we are connected to the writer.
                    # Consequently, this block is only entered if we are connected to a MySQL writer.
                    cursor.execute(self._dialect.host_id_query)
                    writer_id = cursor.fetchone()[0]
                cursor.execute(self._dialect.topology_query)
                return self._process_multi_az_query_results(cursor, writer_id)
        except ProgrammingError as e:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.InvalidQuery"), e) from e

    def _process_multi_az_query_results(self, cursor: Cursor, writer_id: str) -> Topology:
        hosts_dict = {}
        for record in cursor:
            host: HostInfo = self._create_multi_az_host(record, writer_id)
            hosts_dict[host.host] = host

        hosts = []
        writers = []
        for host in hosts_dict.values():
            if host.role == HostRole.WRITER:
                writers.append(host)
            else:
                hosts.append(host)

        if len(writers) == 0:
            logger.error("RdsHostListProvider.InvalidTopology")
            hosts.clear()
        else:
            hosts.append(writers[0])

        return tuple(hosts)

    def _create_multi_az_host(self, record: Tuple, writer_id: str) -> HostInfo:
        id = record[0]  # The ID will look something like '0123456789' (MySQL) or 'db-ABC1DE2FGHI' (Postgres)
        host = record[1]
        port = record[2]
        role = HostRole.WRITER if id == writer_id else HostRole.READER

        if self.instance_template:
            instance_name = self._rds_utils.get_instance_id(host)  # e.g. 'postgres-instance-1'
            if instance_name is None:
                raise AwsWrapperError(Messages.get("MultiAzTopologyUtils.UnableToParseInstanceName"))

            host = self.instance_template.host.replace("?", instance_name)
            if host.find(":") > -1:
                host, port = host.split(":")

        host_info = HostInfo(
            host=host, port=port, role=role, availability=HostAvailability.AVAILABLE, weight=0, host_id=id)
        host_info.add_alias(host)
        return host_info


class GlobalAuroraTopologyUtils(AuroraTopologyUtils):
    _dialect: db_dialect.GlobalAuroraTopologyDialect

    def __init__(self, dialect: db_dialect.GlobalAuroraTopologyDialect, props: Properties):
        super().__init__(dialect, props)
        self._dialect: db_dialect.GlobalAuroraTopologyDialect = dialect
        self._instance_templates_by_region: dict[str, HostInfo] = {}

    def _query_for_topology(self, conn: Connection) -> Optional[Topology]:
        raise UnsupportedOperationError(
            Messages.get_formatted("GlobalAuroraTopologyUtils.UnsupportedOperationError", "query_for_topology"))

    def query_for_topology_with_regions(
            self,
            conn: Connection,
            instance_templates_by_region: dict[str, HostInfo]
    ) -> Optional[Topology]:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._dialect.topology_query)
                return self._process_global_query_results(cursor, instance_templates_by_region)
        except ProgrammingError as e:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.InvalidQuery"), e) from e

    def _process_global_query_results(
            self,
            cursor: Cursor,
            instance_templates_by_region: dict[str, HostInfo]
    ) -> Topology:
        hosts_map = {}
        for record in cursor:
            host = self._create_global_host(record, instance_templates_by_region)
            hosts_map[host.host] = host

        hosts = []
        writers = []
        for host in hosts_map.values():
            if host.role == HostRole.WRITER:
                writers.append(host)
            else:
                hosts.append(host)

        if not writers:
            logger.error("RdsHostListProvider.InvalidTopology")
            hosts.clear()
        elif len(writers) == 1:
            hosts.append(writers[0])
        else:
            existing_writers: List[HostInfo] = [x for x in writers if x is not None]
            existing_writers.sort(reverse=True, key=lambda h: h.last_update_time or datetime.min)
            hosts.append(existing_writers[0])

        return tuple(hosts)

    def _create_global_host(
            self,
            record: Tuple,
            instance_templates_by_region: dict[str, HostInfo]
    ) -> HostInfo:
        host_id: str = record[0]
        is_writer: bool = record[1]
        # node_lag: float = record[2]  # Not currently used but available for future weight calculations
        aws_region: str = record[3]
        last_update: datetime = datetime.now()

        instance_template = instance_templates_by_region.get(aws_region)
        if not instance_template:
            raise AwsWrapperError(
                Messages.get_formatted("GlobalAuroraTopologyMonitor.cannotFindRegionTemplate", aws_region))

        return self.create_host(host_id, is_writer, last_update, instance_template, self.initial_host_info)

    def get_region(self, instance_id: str, conn: Connection) -> Optional[str]:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._dialect.region_by_instance_id_query, (instance_id,))
                row = cursor.fetchone()
                if row:
                    aws_region = row[0]
                    return aws_region if aws_region else None
        except Exception:
            pass
        return None

    def parse_instance_templates(self, instance_templates_string: str) -> dict[str, HostInfo]:
        if not instance_templates_string or not instance_templates_string.strip():
            raise AwsWrapperError(
                Messages.get("GlobalAuroraTopologyUtils.globalClusterInstanceHostPatternsRequired"))

        instance_templates = {}
        for pattern in instance_templates_string.split(","):
            pattern = pattern.strip()
            if not pattern:
                continue

            # Parse format: region:host:port or region:host
            parts = pattern.split(":", 2)
            if len(parts) < 2:
                raise AwsWrapperError(
                    Messages.get_formatted("GlobalAuroraTopologyUtils.invalidInstanceTemplate", pattern))

            region = parts[0]
            host = parts[1]
            port = int(parts[2]) if len(parts) > 2 else HostInfo.NO_PORT

            self._validate_host_pattern(host)

            instance_templates[region] = HostInfo(
                host=host,
                port=port,
                host_availability_strategy=self._host_availability_strategy)

        logger.debug("GlobalAuroraTopologyUtils.detectedGdbPatterns", instance_templates)
        return instance_templates
