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

from typing import (TYPE_CHECKING, Callable, ClassVar, Dict, List, Optional,
                    Protocol, Tuple)

if TYPE_CHECKING:
    from aws_wrapper.hostinfo import HostInfo, HostRole
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.target_driver_dialect import TargetDriverDialect

from threading import Lock

from sqlalchemy import QueuePool, pool

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostselector import HostSelector, RandomHostSelector
from aws_wrapper.plugin import CanReleaseResources
from aws_wrapper.utils.log import Logger
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import (Properties, PropertiesUtils,
                                          WrapperProperties)
from aws_wrapper.utils.rds_url_type import RdsUrlType
from aws_wrapper.utils.rdsutils import RdsUtils
from aws_wrapper.utils.sliding_expiration_cache import SlidingExpirationCache

logger = Logger(__name__)


class ConnectionProvider(Protocol):

    def accepts_host_info(self, host_info: HostInfo, properties: Properties) -> bool:
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        ...

    def connect(
            self,
            target_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            properties: Properties) -> Connection:
        ...


class DriverConnectionProvider(ConnectionProvider):
    _accepted_strategies: Dict[str, HostSelector] = {"random": RandomHostSelector()}

    def accepts_host_info(self, host_info: HostInfo, properties: Properties) -> bool:
        return True

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy in self._accepted_strategies

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        host_selector: Optional[HostSelector] = self._accepted_strategies.get(strategy)
        if host_selector is not None:
            return host_selector.get_host(hosts, role)

        raise AwsWrapperError(
            Messages.get_formatted("DriverConnectionProvider.UnsupportedStrategy", strategy))

    def connect(
            self,
            target_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            properties: Properties) -> Connection:
        prepared_properties = target_driver_dialect.prepare_connect_info(host_info, properties)
        logger.debug("DriverConnectionProvider.ConnectingToHost", host_info.host, PropertiesUtils.log_properties(prepared_properties))
        return target_func(**prepared_properties)


class ConnectionProviderManager:
    _lock: Lock = Lock()
    _conn_provider: Optional[ConnectionProvider] = None

    def __init__(self, default_provider: ConnectionProvider = DriverConnectionProvider()):
        self._default_provider: ConnectionProvider = default_provider

    @property
    def default_provider(self):
        return self._default_provider

    @staticmethod
    def set_connection_provider(connection_provider: ConnectionProvider):
        with ConnectionProviderManager._lock:
            ConnectionProviderManager._conn_provider = connection_provider

    def get_connection_provider(self, host_info: HostInfo, properties: Properties) -> ConnectionProvider:
        if ConnectionProviderManager._conn_provider is None:
            return self.default_provider

        with ConnectionProviderManager._lock:
            if ConnectionProviderManager._conn_provider is not None \
                    and ConnectionProviderManager._conn_provider.accepts_host_info(host_info, properties):
                return ConnectionProviderManager._conn_provider

        return self._default_provider

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        accepts_strategy: bool = False
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                accepts_strategy = ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy)

        if not accepts_strategy:
            accepts_strategy = self._default_provider.accepts_strategy(role, strategy)

        return accepts_strategy

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if ConnectionProviderManager._conn_provider is not None \
                        and ConnectionProviderManager._conn_provider.accepts_strategy(role, strategy):
                    return ConnectionProviderManager._conn_provider.get_host_info_by_strategy(hosts, role, strategy)

        return self._default_provider.get_host_info_by_strategy(hosts, role, strategy)

    @staticmethod
    def reset_provider():
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                ConnectionProviderManager._conn_provider = None

    @staticmethod
    def release_resources():
        if ConnectionProviderManager._conn_provider is not None:
            with ConnectionProviderManager._lock:
                if isinstance(ConnectionProviderManager._conn_provider, CanReleaseResources):
                    ConnectionProviderManager._conn_provider.release_resources()


class SqlAlchemyPooledConnectionProvider(ConnectionProvider, CanReleaseResources):
    _POOL_EXPIRATION_CHECK_NS: ClassVar[int] = 30 * 60_000_000_000  # 30 minutes
    _LEAST_CONNECTIONS: ClassVar[str] = "least_connections"
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()
    _database_pools: ClassVar[SlidingExpirationCache[PoolKey, QueuePool]] = SlidingExpirationCache(
        should_dispose_func=lambda queue_pool: queue_pool.checkedout() == 0,
        item_disposal_func=lambda queue_pool: queue_pool.dispose()
    )

    def __init__(
            self,
            pool_configurator: Optional[Callable] = None,
            pool_mapping: Optional[Callable] = None,
            pool_expiration_check_ns: int = -1,
            pool_cleanup_interval_ns: int = -1):
        self._pool_configurator = pool_configurator
        self._pool_mapping = pool_mapping

        if pool_expiration_check_ns > -1:
            SqlAlchemyPooledConnectionProvider._POOL_EXPIRATION_CHECK_NS = pool_expiration_check_ns

        if pool_cleanup_interval_ns > -1:
            SqlAlchemyPooledConnectionProvider._database_pools.set_cleanup_interval_ns(pool_cleanup_interval_ns)

    @property
    def num_pools(self):
        return len(self._database_pools)

    @property
    def pool_urls(self):
        return {pool_key.url for pool_key, _ in self._database_pools.items()}

    def keys(self):
        return self._database_pools.keys()

    def accepts_host_info(self, host_info: HostInfo, properties: Properties) -> bool:
        url_type = SqlAlchemyPooledConnectionProvider._rds_utils.identify_rds_type(host_info.host)
        return RdsUrlType.RDS_INSTANCE == url_type

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy == SqlAlchemyPooledConnectionProvider._LEAST_CONNECTIONS

    def get_host_info_by_strategy(self, hosts: List[HostInfo], role: HostRole, strategy: str) -> HostInfo:
        if strategy != SqlAlchemyPooledConnectionProvider._LEAST_CONNECTIONS:
            raise AwsWrapperError(Messages.get_formatted(
                "ConnectionProvider.UnsupportedHostSelectorStrategy",
                strategy, SqlAlchemyPooledConnectionProvider.__class__.__name__))

        valid_hosts = [host for host in hosts if host.role == role]
        valid_hosts.sort(key=lambda host: self._num_connections(host))

        if len(valid_hosts) == 0:
            raise AwsWrapperError(Messages.get_formatted("HostSelector.NoHostsMatchingRole", role))

        return valid_hosts[0]

    def _num_connections(self, host_info: HostInfo) -> int:
        num_connections = 0
        for pool_key, cache_item in SqlAlchemyPooledConnectionProvider._database_pools.items():
            if pool_key.url == host_info.url:
                num_connections += cache_item.item.checkedout()
        return num_connections

    def connect(
            self,
            target_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            properties: Properties):
        queue_pool: Optional[QueuePool] = SqlAlchemyPooledConnectionProvider._database_pools.compute_if_absent(
            PoolKey(host_info.url, self._get_extra_key(host_info, properties)),
            lambda _: self._create_pool(target_func, target_driver_dialect, host_info, properties),
            SqlAlchemyPooledConnectionProvider._POOL_EXPIRATION_CHECK_NS
        )

        if queue_pool is None:
            raise AwsWrapperError(Messages.get_formatted("SqlAlchemyPooledConnectionProvider.PoolNone", host_info.url))

        return queue_pool.connect()

    # The pool key should always be retrieved using this method, because the username
    # must always be included to avoid sharing privileged connections with other users.
    def _get_extra_key(self, host_info: HostInfo, props: Properties) -> str:
        if self._pool_mapping is not None:
            return self._pool_mapping(host_info, props)

        # Otherwise use default map key
        user = props.get(WrapperProperties.USER.name, None)
        if user is None or user == "":
            raise AwsWrapperError(Messages.get("SqlAlchemyPooledConnectionProvider.UnableToCreateDefaultKey"))
        return user

    def _create_pool(
            self,
            target_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            props: Properties):
        kwargs = dict() if self._pool_configurator is None else self._pool_configurator(host_info, props)
        prepared_properties = target_driver_dialect.prepare_connect_info(host_info, props)
        kwargs["creator"] = self._get_connection_func(target_func, prepared_properties)
        return self._create_sql_alchemy_pool(**kwargs)

    def _get_connection_func(self, target_connect_func: Callable, props: Properties):
        return lambda: target_connect_func(**props)

    def _create_sql_alchemy_pool(self, **kwargs):
        return pool.QueuePool(**kwargs)

    def release_resources(self):
        for _, cache_item in SqlAlchemyPooledConnectionProvider._database_pools.items():
            cache_item.item.dispose()
        SqlAlchemyPooledConnectionProvider._database_pools.clear()


class PoolKey:
    def __init__(self, url: str, extra_key: Optional[str] = None):
        self._url = url
        self._extra_key = extra_key

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.__members() == other.__members()
        else:
            return False

    def __hash__(self):
        return hash(self.__members())

    def __members(self) -> Tuple[str, Optional[str]]:
        return self._url, self._extra_key

    @property
    def url(self):
        return self._url

    @property
    def extra_key(self):
        return self._extra_key
