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
from abc import abstractmethod
from concurrent.futures import Executor, ThreadPoolExecutor, TimeoutError
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from threading import RLock
from typing import (TYPE_CHECKING, ClassVar, List, Optional, Protocol, Tuple,
                    runtime_checkable)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect

import aws_advanced_python_wrapper.database_dialect as db_dialect
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import (
    HostAvailability, create_host_availability_strategy)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249 import (Connection, Cursor,
                                                ProgrammingError)
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
from aws_advanced_python_wrapper.utils.decorators import \
    preserve_transaction_status_with_timeout
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.utils import LogUtils

logger = Logger(__name__)


class HostListProvider(Protocol):
    def refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        ...

    def force_refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        ...

    def get_host_role(self, connection: Connection) -> HostRole:
        """
        Evaluates the host role of the given connection - either a writer or a reader.

        :param connection: a connection to the database instance whose role should be determined.
        :return: the role of the given connection - either a writer or a reader.
        """
        ...

    def identify_connection(self, connection: Optional[Connection]) -> Optional[HostInfo]:
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
    _topology_cache: CacheMap[str, Tuple[HostInfo, ...]] = CacheMap()
    # Maps cluster IDs to a boolean representing whether they are a primary cluster ID or not. A primary cluster ID is a
    # cluster ID that is equivalent to a cluster URL. Topology info is shared between RdsHostListProviders that have
    # the same cluster ID.
    _is_primary_cluster_id_cache: CacheMap[str, bool] = CacheMap()
    # Maps existing cluster IDs to suggested cluster IDs. This is used to update non-primary cluster IDs to primary
    # cluster IDs so that connections to the same clusters can share topology info.
    _cluster_ids_to_update: CacheMap[str, str] = CacheMap()

    _executor: ClassVar[Executor] = ThreadPoolExecutor(thread_name_prefix="RdsHostListProviderExecutor")

    def __init__(self, host_list_provider_service: HostListProviderService, props: Properties):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        self._props: Properties = props

        self._max_timeout = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get_int(self._props)
        self._rds_utils: RdsUtils = RdsUtils()
        self._hosts: Tuple[HostInfo, ...] = ()
        self._cluster_id: str = str(uuid.uuid4())
        self._initial_host_info: Optional[HostInfo] = None
        self._initial_hosts: Tuple[HostInfo, ...] = ()
        self._cluster_instance_template: Optional[HostInfo] = None
        self._rds_url_type: Optional[RdsUrlType] = None

        dialect = self._host_list_provider_service.database_dialect
        if not isinstance(dialect, db_dialect.TopologyAwareDatabaseDialect):
            raise AwsWrapperError(Messages.get_formatted("RdsHostListProvider.InvalidDialect", dialect))
        self._dialect: db_dialect.TopologyAwareDatabaseDialect = dialect

        self._is_primary_cluster_id: bool = False
        self._is_initialized: bool = False
        self._suggested_cluster_id_refresh_ns: int = 600_000_000_000  # 10 minutes
        self._lock: RLock = RLock()
        self._refresh_rate_ns: int = WrapperProperties.TOPOLOGY_REFRESH_MS.get_int(self._props) * 1_000_000

    def _initialize(self):
        if self._is_initialized:
            return
        with self._lock:
            if self._is_initialized:
                return

            host_availability_strategy = create_host_availability_strategy(self._props)
            self._initial_host_info: HostInfo = HostInfo(
                host=self._props.get("host"),
                port=self._props.get("port", HostInfo.NO_PORT),
                host_availability_strategy=host_availability_strategy)
            self._initial_hosts: Tuple[HostInfo, ...] = (self._initial_host_info,)
            self._host_list_provider_service.initial_connection_host_info = self._initial_host_info

            host_pattern = WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.get(self._props)
            if host_pattern:
                if host_pattern.find(":") > -1:
                    host_pattern, port = host_pattern.split(":")
                else:
                    port = HostInfo.NO_PORT

                self._cluster_instance_template = HostInfo(
                    host=host_pattern,
                    port=port,
                    host_availability_strategy=host_availability_strategy)
            else:
                self._cluster_instance_template = HostInfo(
                    host=self._rds_utils.get_rds_instance_host_pattern(self._initial_host_info.host),
                    host_id=self._initial_host_info.host_id,
                    port=self._initial_host_info.port,
                    host_availability_strategy=host_availability_strategy)
            self._validate_host_pattern(self._cluster_instance_template.host)

            self._rds_url_type: RdsUrlType = self._rds_utils.identify_rds_type(self._initial_host_info.host)
            cluster_id = WrapperProperties.CLUSTER_ID.get(self._props)
            if cluster_id:
                self._cluster_id = cluster_id
            elif self._rds_url_type == RdsUrlType.RDS_PROXY:
                self._cluster_id = self._initial_host_info.url
            elif self._rds_url_type.is_rds:
                cluster_id_suggestion = self._get_suggested_cluster_id(self._initial_host_info.url)
                if cluster_id_suggestion and cluster_id_suggestion.cluster_id:
                    # The initial URL matches an entry in the topology cache for an existing cluster ID.
                    # Update this cluster ID to match the existing one so that topology info can be shared.
                    self._cluster_id = cluster_id_suggestion.cluster_id
                    self._is_primary_cluster_id = cluster_id_suggestion.is_primary_cluster_id
                else:
                    cluster_url = self._rds_utils.get_rds_cluster_host_url(self._initial_host_info.host)
                    if cluster_url is not None:
                        self._cluster_id = f"{cluster_url}:{self._cluster_instance_template.port}" \
                            if self._cluster_instance_template.is_port_specified() else cluster_url
                        self._is_primary_cluster_id = True
                        self._is_primary_cluster_id_cache.put(self._cluster_id, True,
                                                              self._suggested_cluster_id_refresh_ns)

                    self._is_initialized = True

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

    def _get_suggested_cluster_id(self, url: str) -> Optional[ClusterIdSuggestion]:
        for key, hosts in RdsHostListProvider._topology_cache.get_dict().items():
            is_primary_cluster_id = \
                RdsHostListProvider._is_primary_cluster_id_cache.get_with_default(
                    key, False, self._suggested_cluster_id_refresh_ns)
            if key == url:
                return RdsHostListProvider.ClusterIdSuggestion(url, is_primary_cluster_id)
            if not hosts:
                continue
            for host in hosts:
                if host.url == url:
                    logger.debug("RdsHostListProvider.SuggestedClusterId", key, url)
                    return RdsHostListProvider.ClusterIdSuggestion(key, is_primary_cluster_id)
        return None

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

        suggested_primary_cluster_id = RdsHostListProvider._cluster_ids_to_update.get(self._cluster_id)
        if suggested_primary_cluster_id and self._cluster_id != suggested_primary_cluster_id:
            self._cluster_id = suggested_primary_cluster_id
            self._is_primary_cluster_id = True

        cached_hosts = RdsHostListProvider._topology_cache.get(self._cluster_id)
        if not cached_hosts or force_update:
            if not conn:
                # Cannot fetch topology without a connection
                # Return the original hosts passed to the connect method
                return RdsHostListProvider.FetchTopologyResult(self._initial_hosts, False)

            try:
                driver_dialect = self._host_list_provider_service.driver_dialect

                query_for_topology_func_with_timeout = preserve_transaction_status_with_timeout(
                    RdsHostListProvider._executor, self._max_timeout, driver_dialect, conn)(self._query_for_topology)
                hosts = query_for_topology_func_with_timeout(conn)
                if hosts is not None and len(hosts) > 0:
                    RdsHostListProvider._topology_cache.put(self._cluster_id, hosts, self._refresh_rate_ns)
                    if self._is_primary_cluster_id and cached_hosts is None:
                        # This cluster_id is primary and a new entry was just created in the cache. When this happens,
                        # we check for non-primary cluster IDs associated with the same cluster so that the topology
                        # info can be shared.
                        self._suggest_cluster_id(hosts)
                    return RdsHostListProvider.FetchTopologyResult(hosts, False)
            except TimeoutError as e:
                raise QueryTimeoutError(Messages.get("RdsHostListProvider.QueryForTopologyTimeout")) from e

        if cached_hosts:
            return RdsHostListProvider.FetchTopologyResult(cached_hosts, True)
        else:
            return RdsHostListProvider.FetchTopologyResult(self._initial_hosts, False)

    def _suggest_cluster_id(self, primary_cluster_id_hosts: Tuple[HostInfo, ...]):
        if not primary_cluster_id_hosts:
            return

        primary_cluster_id_urls = {host.url for host in primary_cluster_id_hosts}
        for cluster_id, hosts in RdsHostListProvider._topology_cache.get_dict().items():
            is_primary_cluster = RdsHostListProvider._is_primary_cluster_id_cache.get_with_default(
                cluster_id, False, self._suggested_cluster_id_refresh_ns)
            suggested_primary_cluster_id = RdsHostListProvider._cluster_ids_to_update.get(cluster_id)
            if is_primary_cluster or suggested_primary_cluster_id or not hosts:
                continue

            # The entry is non-primary
            for host in hosts:
                if host.url in primary_cluster_id_urls:
                    # An instance URL in this topology cache entry matches an instance URL in the primary cluster entry.
                    # The associated cluster ID should be updated to match the primary ID so that they can share
                    # topology info.
                    RdsHostListProvider._cluster_ids_to_update.put(
                        cluster_id, self._cluster_id, self._suggested_cluster_id_refresh_ns)
                    break

    def _query_for_topology(self, conn: Connection) -> Optional[Tuple[HostInfo, ...]]:
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
            raise AwsWrapperError(Messages.get("RdsHostListProvider.InvalidQuery")) from e

    def _process_query_results(self, cursor: Cursor) -> Tuple[HostInfo, ...]:
        """
        Form a list of hosts from the results of the topology query.
        :param cursor: The Cursor object containing a reference to the results of the topology query.
        :return: a tuple of hosts representing the database topology.
        An empty tuple will be returned if the query results did not include a writer instance.
        """
        host_map = {}
        for record in cursor:
            host = self._create_host(record)
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
        if not self._cluster_instance_template:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.UninitializedClusterInstanceTemplate"))
        if not self._initial_host_info:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.UninitializedInitialHostInfo"))

        host_id: str = record[0]
        is_writer: bool = record[1]
        last_update: datetime
        if len(record) > 4 and isinstance(record[4], datetime):
            last_update = record[4]
        else:
            last_update = datetime.now()

        host_id = host_id if host_id else "?"
        endpoint = self._cluster_instance_template.host.replace("?", host_id)
        port = self._cluster_instance_template.port \
            if self._cluster_instance_template.is_port_specified() \
            else self._initial_host_info.port
        host_info = HostInfo(
            host=endpoint,
            port=port,
            availability=HostAvailability.AVAILABLE,
            host_availability_strategy=create_host_availability_strategy(self._props),
            role=HostRole.WRITER if is_writer else HostRole.READER,
            last_update_time=last_update,
            host_id=host_id)
        host_info.add_alias(host_id)
        return host_info

    def refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
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

    def force_refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
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

    def get_host_role(self, connection: Connection) -> HostRole:
        driver_dialect = self._host_list_provider_service.driver_dialect

        try:
            cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
                RdsHostListProvider._executor, self._max_timeout, driver_dialect, connection)(self._get_host_role)
            result = cursor_execute_func_with_timeout(connection)
            if result is not None:
                is_reader = result[0]
                return HostRole.READER if is_reader else HostRole.WRITER
        except TimeoutError as e:
            raise QueryTimeoutError(Messages.get("RdsHostListProvider.GetHostRoleTimeout")) from e

        raise AwsWrapperError(Messages.get("RdsHostListProvider.ErrorGettingHostRole"))

    def _get_host_role(self, conn: Connection):
        with closing(conn.cursor()) as cursor:
            cursor.execute(self._dialect.is_reader_query)
            return cursor.fetchone()

    def identify_connection(self, connection: Optional[Connection]) -> Optional[HostInfo]:
        """
        Identify which host the given connection points to.
        :param connection: an opened connection.
        :return: a :py:class:`HostInfo` object containing host information for the given connection.
        """
        if connection is None:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.ErrorIdentifyConnection"))

        driver_dialect = self._host_list_provider_service.driver_dialect
        try:
            cursor_execute_func_with_timeout = preserve_transaction_status_with_timeout(
                RdsHostListProvider._executor, self._max_timeout, driver_dialect, connection)(self._identify_connection)
            result = cursor_execute_func_with_timeout(connection)
            if result:
                host_id = result[0]
                hosts = self.refresh(connection)
                is_force_refresh = False
                if not hosts:
                    hosts = self.force_refresh(connection)
                    is_force_refresh = True

                if not hosts:
                    return None

                found_host: Optional[HostInfo] = next((host_info for host_info in hosts if host_info.host_id == host_id), None)
                if not found_host and not is_force_refresh:
                    hosts = self.force_refresh(connection)
                    if not hosts:
                        return None

                    found_host = next(
                        (host_info for host_info in hosts if host_info.host_id == host_id),
                        None)

                return found_host
        except TimeoutError as e:
            raise QueryTimeoutError(Messages.get("RdsHostListProvider.IdentifyConnectionTimeout")) from e

        raise AwsWrapperError(Messages.get("RdsHostListProvider.ErrorIdentifyConnection"))

    def _identify_connection(self, conn: Connection):
        with closing(conn.cursor()) as cursor:
            cursor.execute(self._dialect.host_id_query)
            return cursor.fetchone()

    @dataclass()
    class ClusterIdSuggestion:
        cluster_id: str
        is_primary_cluster_id: bool

    @dataclass()
    class FetchTopologyResult:
        hosts: Tuple[HostInfo, ...]
        is_cached_data: bool


class MultiAzHostListProvider(RdsHostListProvider):
    def __init__(
            self,
            provider_service: HostListProviderService,
            props: Properties,
            topology_query: str,
            host_id_query: str,
            is_reader_query: str,
            writer_host_query: str,
            writer_host_column_index: int = 0):
        super().__init__(provider_service, props)
        self._topology_query = topology_query
        self._host_id_query = host_id_query
        self._is_reader_query = is_reader_query
        self._writer_host_query = writer_host_query
        self._writer_host_column_index = writer_host_column_index

    def _query_for_topology(self, conn: Connection) -> Optional[Tuple[HostInfo, ...]]:
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._writer_host_query)
                row = cursor.fetchone()
                if row is not None:
                    writer_id = row[self._writer_host_column_index]
                else:
                    # In MySQL, the writer host query above will be empty if we are connected to the writer.
                    # Consequently, this block is only entered if we are connected to a MySQL writer.
                    cursor.execute(self._host_id_query)
                    writer_id = cursor.fetchone()[0]
                cursor.execute(self._topology_query)
                return self._process_multi_az_query_results(cursor, writer_id)
        except ProgrammingError as e:
            raise AwsWrapperError(Messages.get("RdsHostListProvider.InvalidQuery")) from e

    def _process_multi_az_query_results(self, cursor: Cursor, writer_id: str) -> Tuple[HostInfo, ...]:
        hosts_dict = {}
        for record in cursor:
            host = self._create_multi_az_host(record, writer_id)
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

        host_pattern = WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.get(self._props)
        if host_pattern:
            instance_name = self._rds_utils.get_instance_id(host)  # e.g. 'postgres-instance-1'
            if instance_name is None:
                raise AwsWrapperError(Messages.get("MultiAzHostListProvider.UnableToParseInstanceName"))

            host = host_pattern.replace("?", instance_name)
            if host.find(":") > -1:
                host, port = host.split(":")

        host_info = HostInfo(
            host=host, port=port, role=role, availability=HostAvailability.AVAILABLE, weight=0, host_id=id)
        host_info.add_alias(host)
        return host_info


class ConnectionStringHostListProvider(StaticHostListProvider):

    def __init__(self, host_list_provider_service: HostListProviderService, props: Properties):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        self._props: Properties = props
        self._hosts: Tuple[HostInfo, ...] = ()
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

    def refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        self._initialize()
        return tuple(self._hosts)

    def force_refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        self._initialize()
        return tuple(self._hosts)

    def get_host_role(self, connection: Connection) -> HostRole:
        raise UnsupportedOperationError(
            Messages.get_formatted("ConnectionStringHostListProvider.UnsupportedMethod", "get_host_role"))

    def identify_connection(self, connection: Optional[Connection]) -> Optional[HostInfo]:
        raise UnsupportedOperationError(
            Messages.get_formatted("ConnectionStringHostListProvider.UnsupportedMethod", "identify_connection"))
