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

import uuid
from abc import abstractmethod
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
from threading import RLock
from typing import List, Optional, Protocol, Tuple, runtime_checkable

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.pep249 import Connection, Cursor, Error, ProgrammingError
from aws_wrapper.utils.cache_map import CacheMap
from aws_wrapper.utils.dialect import Dialect, TopologyAwareDatabaseDialect
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, WrapperProperties
from aws_wrapper.utils.rds_url_type import RdsUrlType
from aws_wrapper.utils.rdsutils import RdsUtils
from aws_wrapper.utils.utils import Utils

logger = getLogger(__name__)


class HostListProvider(Protocol):
    def refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        ...

    def force_refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        ...

    def get_host_role(self, connection: Connection) -> HostRole:
        ...

    def identify_connection(self, connection: Connection) -> Optional[HostInfo]:
        ...


@runtime_checkable
class DynamicHostListProvider(HostListProvider, Protocol):
    ...


@runtime_checkable
class StaticHostListProvider(HostListProvider, Protocol):
    ...


class ConnectionStringHostListProvider(StaticHostListProvider):
    def refresh(self, connection: Optional[Connection] = None):
        ...

    def force_refresh(self, connection: Optional[Connection] = None):
        ...

    def get_host_role(self, connection: Optional[Connection] = None):
        ...

    def identify_connection(self, connection: Optional[Connection] = None):
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
    def dialect(self) -> Dialect:
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


class AuroraHostListProvider(DynamicHostListProvider, HostListProvider):
    _topology_cache: CacheMap[str, List[HostInfo]] = CacheMap()
    _suggested_primary_cluster_id_cache: CacheMap[str, str] = CacheMap()
    _primary_cluster_id_cache: CacheMap[str, bool] = CacheMap()

    def __init__(self, host_list_provider_service: HostListProviderService, props: Properties):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        self._props: Properties = props

        self._rds_utils: RdsUtils = RdsUtils()
        self._hosts: List[HostInfo] = []
        self._cluster_id: str = str(uuid.uuid4())
        self._initial_host_info: Optional[HostInfo] = None
        self._initial_hosts: List[HostInfo] = []
        self._cluster_instance_template: Optional[HostInfo] = None
        self._rds_url_type: Optional[RdsUrlType] = None
        self._topology_aware_dialect: Optional[TopologyAwareDatabaseDialect] = None
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

            self._initial_host_info: HostInfo = \
                HostInfo(self._props["host"], int(self._props["port"]) if "port" in self._props else HostInfo.NO_PORT)
            self._initial_hosts: List[HostInfo] = [self._initial_host_info]
            self._host_list_provider_service.initial_connection_host_info = self._initial_host_info

            self._cluster_instance_template: HostInfo
            host_pattern = WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.get(self._props)
            if host_pattern:
                self._cluster_instance_template = HostInfo(
                    WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.get(self._props))
            else:
                self._cluster_instance_template = \
                    HostInfo(self._rds_utils.get_rds_instance_host_pattern(self._initial_host_info.host))
            self._validate_host_pattern(self._cluster_instance_template.host)

            self._rds_url_type: RdsUrlType = self._rds_utils.identify_rds_type(self._initial_host_info.host)
            cluster_id = WrapperProperties.CLUSTER_ID.get(self._props)
            if cluster_id:
                self._cluster_id = cluster_id
            elif self._rds_url_type == RdsUrlType.RDS_PROXY:
                self._cluster_id = self._initial_host_info.url
            elif self._rds_url_type.is_rds:
                cluster_id_suggestion = self._get_suggested_cluster_id(self._initial_host_info.url)
                if cluster_id_suggestion is not None and cluster_id_suggestion.cluster_id:
                    self._cluster_id = cluster_id_suggestion.cluster_id
                    self._is_primary_cluster_id = cluster_id_suggestion.is_primary_cluster_id
                else:
                    cluster_url = self._rds_utils.get_rds_cluster_host_url(self._initial_host_info.url)
                    if not cluster_url:
                        return

                    self._cluster_id = cluster_url
                    self._is_primary_cluster_id = True
                    self._primary_cluster_id_cache.put(self._cluster_id, True, self._suggested_cluster_id_refresh_ns)

        self._is_initialized = True

    def _validate_host_pattern(self, host: str):
        if not self._rds_utils.is_dns_pattern_valid(host):
            message = Messages.get("AuroraHostListProvider.InvalidPattern")
            logger.error(message)
            raise AwsWrapperError(message)

        url_type = self._rds_utils.identify_rds_type(host)
        if url_type == RdsUrlType.RDS_PROXY:
            message = Messages.get("AuroraHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSProxy")
            logger.error(message)
            raise AwsWrapperError(message)

        if url_type == RdsUrlType.RDS_CUSTOM_CLUSTER:
            message = Messages.get("AuroraHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSCustom")
            logger.error(message)
            raise AwsWrapperError(message)

    def _get_suggested_cluster_id(self, url: str) -> 'Optional[AuroraHostListProvider.ClusterIdSuggestion]':
        for key, hosts in AuroraHostListProvider._topology_cache.get_dict().items():
            is_primary_cluster_id = \
                AuroraHostListProvider._primary_cluster_id_cache.get_with_default(
                    key, False, self._suggested_cluster_id_refresh_ns)
            if key == url:
                return AuroraHostListProvider.ClusterIdSuggestion(url, is_primary_cluster_id)
            if hosts is None:
                continue
            for host in hosts:
                if host.url == url:
                    logger.debug(Messages.get_formatted("AuroraHostListProvider.SuggestedClusterId", key, url))
                    return AuroraHostListProvider.ClusterIdSuggestion(key, is_primary_cluster_id)
        return None

    def _get_topology(self, conn: Optional[Connection], force_update: bool) -> 'AuroraHostListProvider.FetchTopologyResult':
        self._initialize()

        suggested_primary_cluster_id = AuroraHostListProvider._suggested_primary_cluster_id_cache.get(self._cluster_id)
        if suggested_primary_cluster_id and self._cluster_id != suggested_primary_cluster_id:
            self._cluster_id = suggested_primary_cluster_id
            self._is_primary_cluster_id = True

        cached_hosts = AuroraHostListProvider._topology_cache.get(self._cluster_id)
        # If True, this cluster_id is primary and is about to create a new entry in the cache.
        # When a primary entry is created it needs to be suggested to other (non-primary) entries.
        suggest_hosts = cached_hosts is None and self._is_primary_cluster_id

        if not cached_hosts or force_update:
            if conn is None:
                # Cannot fetch topology without a connection
                # Return the original hosts passed to the connect method
                return AuroraHostListProvider.FetchTopologyResult(self._initial_hosts, False)

            hosts = self._query_for_topology(conn)
            if hosts:
                AuroraHostListProvider._topology_cache.put(self._cluster_id, hosts, self._refresh_rate_ns)
                if suggest_hosts:
                    self._suggest_hosts(hosts)
                return AuroraHostListProvider.FetchTopologyResult(hosts, False)

        if cached_hosts:
            return AuroraHostListProvider.FetchTopologyResult(cached_hosts, True)
        else:
            return AuroraHostListProvider.FetchTopologyResult(self._initial_hosts, False)

    def _suggest_hosts(self, primary_cluster_hosts: List[HostInfo]):
        if not primary_cluster_hosts:
            return

        primary_cluster_urls = {host.url for host in primary_cluster_hosts}
        for cluster_id, hosts in AuroraHostListProvider._topology_cache.get_dict().items():
            is_primary_cluster = AuroraHostListProvider._primary_cluster_id_cache.get_with_default(
                cluster_id, False, self._suggested_cluster_id_refresh_ns)
            suggested_primary_cluster_id = AuroraHostListProvider._suggested_primary_cluster_id_cache.get(cluster_id)
            if is_primary_cluster or suggested_primary_cluster_id or not hosts:
                continue

            # The entry is non-primary
            for host in hosts:
                if host.url in primary_cluster_urls:
                    # An instance URL in this cluster matches an instance URL in the primary cluster.
                    # Suggest the primary cluster_id for this cluster.
                    AuroraHostListProvider._suggested_primary_cluster_id_cache.put(
                        cluster_id, self._cluster_id, self._suggested_cluster_id_refresh_ns)
                    break

    def _query_for_topology(self, conn: Connection) -> Optional[List[HostInfo]]:
        if self._topology_aware_dialect is None:
            if not isinstance(self._host_list_provider_service.dialect, TopologyAwareDatabaseDialect):
                logger.warning(Messages.get("AuroraHostListProvider.InvalidDialect"))
                return None
            self._topology_aware_dialect = self._host_list_provider_service.dialect

        # TODO: Set network timeout to ensure topology query does not execute indefinitely
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._topology_aware_dialect.get_topology_query())
                return self._process_query_results(cursor)
        except ProgrammingError as e:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.InvalidQuery")) from e

    def _process_query_results(self, cursor: Cursor) -> List[HostInfo]:
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
            logger.error(Messages.get("AuroraHostListProvider.InvalidTopology"))
            hosts.clear()
        elif len(writers) == 1:
            hosts.append(writers[0])
        else:
            # Take the latest updated writer host as the current writer. All others will be ignored.
            writers.sort(reverse=True, key=lambda h: h.last_update_time)
            hosts.append(writers[0])

        return hosts

    def _create_host(self, record: Tuple) -> HostInfo:
        # According to TopologyAwareDatabaseCluster.getTopologyQuery() the result set
        # should contain 4 columns: instance ID, 1/0 (writer/reader), CPU utilization, host lag in ms.
        # There might be a 5th column specifying the last update time.
        if self._cluster_instance_template is None:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.UninitializedClusterInstanceTemplate"))
        if self._initial_host_info is None:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.UninitializedInitialHostInfo"))

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
            role=HostRole.WRITER if is_writer else HostRole.READER,
            last_update_time=last_update,
            host_id=host_id)
        host_info.add_alias(host_id)
        return host_info

    def refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        self._initialize()
        connection = connection if connection else self._host_list_provider_service.current_connection
        topology = self._get_topology(connection, False)
        logger.debug(Utils.log_topology(topology.hosts))
        self._hosts = topology.hosts
        return tuple(self._hosts)

    def force_refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        self._initialize()
        connection = connection if connection else self._host_list_provider_service.current_connection
        topology = self._get_topology(connection, True)
        logger.debug(Utils.log_topology(topology.hosts))
        self._hosts = topology.hosts
        return tuple(self._hosts)

    def get_host_role(self, connection: Connection) -> HostRole:
        try:
            with closing(connection.cursor()) as cursor:
                topology_aware_dialect = \
                    self._get_topology_aware_dialect("AuroraHostListProvider.InvalidDialectForGetHostRole")
                cursor.execute(topology_aware_dialect.get_is_reader_query())
                result = cursor.fetchone()
                if result:
                    is_reader = result[0]
                    return HostRole.READER if is_reader else HostRole.WRITER
        except Error as e:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorGettingHostRole")) from e
        raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorGettingHostRole"))

    def identify_connection(self, connection: Connection) -> Optional[HostInfo]:
        try:
            with closing(connection.cursor()) as cursor:
                topology_aware_dialect = \
                    self._get_topology_aware_dialect("AuroraHostListProvider.InvalidDialectForIdentifyConnection")
                cursor.execute(topology_aware_dialect.get_host_id_query())
                result = cursor.fetchone()
                if result:
                    host_id = result[0]
                    hosts = self.refresh()
                    if not hosts:
                        return None
                    return next((host_info for host_info in hosts if host_info.host_id == host_id), None)
        except Error as e:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorIdentifyConnection")) from e
        raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorIdentifyConnection"))

    def _get_topology_aware_dialect(self, exception_msg_id: str) -> TopologyAwareDatabaseDialect:
        if self._topology_aware_dialect is None:
            dialect = self._host_list_provider_service.dialect
            if not isinstance(dialect, TopologyAwareDatabaseDialect):
                raise AwsWrapperError(Messages.get_formatted(exception_msg_id, dialect))
            self._topology_aware_dialect = dialect
        return self._topology_aware_dialect

    @dataclass()
    class ClusterIdSuggestion:
        cluster_id: str
        is_primary_cluster_id: bool

    @dataclass()
    class FetchTopologyResult:
        hosts: List[HostInfo]
        is_cached_data: bool
