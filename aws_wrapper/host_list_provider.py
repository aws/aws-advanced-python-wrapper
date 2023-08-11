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
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
from threading import RLock
from typing import (TYPE_CHECKING, Callable, List, Optional, Protocol, Set,
                    Tuple, runtime_checkable)

if TYPE_CHECKING:
    from aws_wrapper.plugin_service import PluginService

from aws_wrapper.dialect import Dialect, TopologyAwareDatabaseDialect
from aws_wrapper.errors import AwsWrapperError, QueryTimeoutError
from aws_wrapper.hostinfo import HostAvailability, HostInfo, HostRole
from aws_wrapper.pep249 import Connection, Cursor, Error, ProgrammingError
from aws_wrapper.plugin import Plugin, PluginFactory
from aws_wrapper.utils.cache_map import CacheMap
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, WrapperProperties
from aws_wrapper.utils.rds_url_type import RdsUrlType
from aws_wrapper.utils.rdsutils import RdsUtils
from aws_wrapper.utils.timeout import timeout
from aws_wrapper.utils.utils import Utils

logger = getLogger(__name__)


class HostListProvider(Protocol):
    def refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        ...

    def force_refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        ...

    def get_host_role(self, connection: Optional[Connection]) -> HostRole:
        ...

    def identify_connection(self, connection: Optional[Connection]) -> Optional[HostInfo]:
        ...


@runtime_checkable
class DynamicHostListProvider(HostListProvider, Protocol):
    ...


@runtime_checkable
class StaticHostListProvider(HostListProvider, Protocol):
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
    def dialect(self) -> Optional[Dialect]:
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
    # Maps cluster IDs to a boolean representing whether they are a primary cluster ID or not. A primary cluster ID is a
    # cluster ID that is equivalent to a cluster URL. Topology info is shared between AuroraHostListProviders that have
    # the same cluster ID.
    _is_primary_cluster_id_cache: CacheMap[str, bool] = CacheMap()
    # Maps existing cluster IDs to suggested cluster IDs. This is used to update non-primary cluster IDs to primary
    # cluster IDs so that connections to the same clusters can share topology info.
    _cluster_ids_to_update: CacheMap[str, str] = CacheMap()

    def __init__(self, host_list_provider_service: HostListProviderService, props: Properties):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        self._props: Properties = props

        self._max_timeout = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get_int(self._props)
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
                HostInfo(self._props.get("host"), self._props.get("port", HostInfo.NO_PORT))
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
                if cluster_id_suggestion and cluster_id_suggestion.cluster_id:
                    # The initial URL matches an entry in the topology cache for an existing cluster ID.
                    # Update this cluster ID to match the existing one so that topology info can be shared.
                    self._cluster_id = cluster_id_suggestion.cluster_id
                    self._is_primary_cluster_id = cluster_id_suggestion.is_primary_cluster_id
                else:
                    cluster_url = self._rds_utils.get_rds_cluster_host_url(self._initial_host_info.url)
                    if cluster_url is not None:
                        self._cluster_id = cluster_url
                        self._is_primary_cluster_id = True
                        self._is_primary_cluster_id_cache.put(self._cluster_id, True, self._suggested_cluster_id_refresh_ns)

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

    def _get_suggested_cluster_id(self, url: str) -> Optional[AuroraHostListProvider.ClusterIdSuggestion]:
        for key, hosts in AuroraHostListProvider._topology_cache.get_dict().items():
            is_primary_cluster_id = \
                AuroraHostListProvider._is_primary_cluster_id_cache.get_with_default(
                    key, False, self._suggested_cluster_id_refresh_ns)
            if key == url:
                return AuroraHostListProvider.ClusterIdSuggestion(url, is_primary_cluster_id)
            if not hosts:
                continue
            for host in hosts:
                if host.url == url:
                    logger.debug(Messages.get_formatted("AuroraHostListProvider.SuggestedClusterId", key, url))
                    return AuroraHostListProvider.ClusterIdSuggestion(key, is_primary_cluster_id)
        return None

    def _get_topology(self, conn: Optional[Connection], force_update: bool = False) \
            -> AuroraHostListProvider.FetchTopologyResult:
        self._initialize()

        suggested_primary_cluster_id = AuroraHostListProvider._cluster_ids_to_update.get(self._cluster_id)
        if suggested_primary_cluster_id and self._cluster_id != suggested_primary_cluster_id:
            self._cluster_id = suggested_primary_cluster_id
            self._is_primary_cluster_id = True

        cached_hosts = AuroraHostListProvider._topology_cache.get(self._cluster_id)
        if not cached_hosts or force_update:
            if not conn:
                # Cannot fetch topology without a connection
                # Return the original hosts passed to the connect method
                return AuroraHostListProvider.FetchTopologyResult(self._initial_hosts, False)

            try:
                query_for_topology_func_with_timeout = timeout(self._max_timeout)(self._query_for_topology)
                hosts = query_for_topology_func_with_timeout(conn)
                if hosts is not None and len(hosts) > 0:
                    AuroraHostListProvider._topology_cache.put(self._cluster_id, hosts, self._refresh_rate_ns)
                    if self._is_primary_cluster_id and cached_hosts is None:
                        # This cluster_id is primary and a new entry was just created in the cache. When this happens, we
                        # check for non-primary cluster IDs associated with the same cluster so that the topology info can
                        # be shared.
                        self._suggest_cluster_id(hosts)
                    return AuroraHostListProvider.FetchTopologyResult(hosts, False)
            except Exception as e:
                raise QueryTimeoutError(Messages.get("AuroraHostListProvider.TopologyTimeout")) from e

        if cached_hosts:
            return AuroraHostListProvider.FetchTopologyResult(cached_hosts, True)
        else:
            return AuroraHostListProvider.FetchTopologyResult(self._initial_hosts, False)

    def _suggest_cluster_id(self, primary_cluster_id_hosts: List[HostInfo]):
        if not primary_cluster_id_hosts:
            return

        primary_cluster_id_urls = {host.url for host in primary_cluster_id_hosts}
        for cluster_id, hosts in AuroraHostListProvider._topology_cache.get_dict().items():
            is_primary_cluster = AuroraHostListProvider._is_primary_cluster_id_cache.get_with_default(
                cluster_id, False, self._suggested_cluster_id_refresh_ns)
            suggested_primary_cluster_id = AuroraHostListProvider._cluster_ids_to_update.get(cluster_id)
            if is_primary_cluster or suggested_primary_cluster_id or not hosts:
                continue

            # The entry is non-primary
            for host in hosts:
                if host.url in primary_cluster_id_urls:
                    # An instance URL in this topology cache entry matches an instance URL in the primary cluster entry.
                    # The associated cluster ID should be updated to match the primary ID so that they can share
                    # topology info.
                    AuroraHostListProvider._cluster_ids_to_update.put(
                        cluster_id, self._cluster_id, self._suggested_cluster_id_refresh_ns)
                    break

    def _query_for_topology(self, conn: Connection) -> Optional[List[HostInfo]]:
        if not self._topology_aware_dialect:
            if not isinstance(self._host_list_provider_service.dialect, TopologyAwareDatabaseDialect):
                logger.warning(Messages.get("AuroraHostListProvider.InvalidDialect"))
                return None
            self._topology_aware_dialect = self._host_list_provider_service.dialect

        # TODO: Set network timeout to ensure topology query does not execute indefinitely
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(self._topology_aware_dialect.topology_query)
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
        # According to TopologyAwareDatabaseDialect.topology_query the result set
        # should contain 4 columns: instance ID, 1/0 (writer/reader), CPU utilization, host lag in ms.
        # There might be a 5th column specifying the last update time.
        if not self._cluster_instance_template:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.UninitializedClusterInstanceTemplate"))
        if not self._initial_host_info:
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

    def get_host_role(self, connection: Optional[Connection]) -> HostRole:
        if connection is None:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorGettingHostRole"))

        try:
            with closing(connection.cursor()) as cursor:
                topology_aware_dialect = \
                    self._get_topology_aware_dialect("AuroraHostListProvider.InvalidDialectForGetHostRole")
                cursor_execute_func_with_timeout = timeout(self._max_timeout)(cursor.execute)
                cursor_execute_func_with_timeout(topology_aware_dialect.is_reader_query)
                result = cursor.fetchone()
                if result:
                    is_reader = result[0]
                    return HostRole.READER if is_reader else HostRole.WRITER
        except Error as e:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorGettingHostRole")) from e
        raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorGettingHostRole"))

    def identify_connection(self, connection: Optional[Connection]) -> Optional[HostInfo]:
        if connection is None:
            raise AwsWrapperError(Messages.get("AuroraHostListProvider.ErrorIdentifyConnection"))
        try:
            with closing(connection.cursor()) as cursor:
                topology_aware_dialect = \
                    self._get_topology_aware_dialect("AuroraHostListProvider.InvalidDialectForIdentifyConnection")
                cursor_execute_func_with_timeout = timeout(self._max_timeout)(cursor.execute)
                cursor_execute_func_with_timeout(topology_aware_dialect.host_id_query)
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
        if not self._topology_aware_dialect:
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


class ConnectionStringHostListProvider(StaticHostListProvider):

    def __init__(self, host_list_provider_service: HostListProviderService, props: Properties):
        self._host_list_provider_service: HostListProviderService = host_list_provider_service
        self._props: Properties = props
        self._hosts: List[HostInfo] = []
        self._is_initialized: bool = False
        self._initial_host_info: Optional[HostInfo] = None

    def _initialize(self):
        if self._is_initialized:
            return

        self._initial_host_info = \
            HostInfo(self._props.get("host"), self._props.get("port", HostInfo.NO_PORT))

        self._hosts.append(self._initial_host_info)

        self._host_list_provider_service.initial_connection_host_info = self._initial_host_info
        self._is_initialized = True

    def refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        self._initialize()
        return tuple(self._hosts)

    def force_refresh(self, connection: Optional[Connection] = None) -> Tuple[HostInfo, ...]:
        self._initialize()
        return tuple(self._hosts)

    def get_host_role(self, connection: Optional[Connection]) -> HostRole:
        raise AwsWrapperError(Messages.get("ConnectionStringHostListProvider.ErrorDoesNotSupportHostRole"))

    def identify_connection(self, connection: Optional[Connection]) -> Optional[HostInfo]:
        raise AwsWrapperError(Messages.get("ConnectionStringHostListProvider.ErrorDoesNotSupportIdentifyConnection"))


class AuroraHostListPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"init_host_provider"}

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def init_host_provider(
            self,
            props: Properties,
            host_list_provider_service: HostListProviderService,
            init_host_provider_func: Callable):
        provider: HostListProvider = host_list_provider_service.host_list_provider
        if provider is None:
            init_host_provider_func()
            return

        if host_list_provider_service.is_static_host_list_provider():
            host_list_provider_service.host_list_provider = AuroraHostListProvider(host_list_provider_service, props)
        elif not isinstance(provider, AuroraHostListProvider):
            raise AwsWrapperError(Messages.get_formatted(
                "AuroraHostListPlugin.ProviderAlreadySet",
                provider.__class__.__name__))

        init_host_provider_func()


class AuroraHostListPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return AuroraHostListPlugin()
