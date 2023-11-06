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

from typing import TYPE_CHECKING, Callable, ClassVar, Dict, Optional, Tuple

from aws_advanced_python_wrapper.connection_provider import ConnectionProvider
from aws_advanced_python_wrapper.hostselector import HostSelector, RandomHostSelector, RoundRobinHostSelector

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect

from sqlalchemy import QueuePool, pool

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils
from aws_advanced_python_wrapper.utils.sliding_expiration_cache import \
    SlidingExpirationCache


class SqlAlchemyPooledConnectionProvider(ConnectionProvider, CanReleaseResources):
    """
    This class can be passed to :py:method:`ConnectionProviderManager.connection_provider` to enable internal connection pools
    for each database instance in a cluster. By maintaining internal connection pools,
    the driver can improve performance by reusing old connection objects.
    """
    _POOL_EXPIRATION_CHECK_NS: ClassVar[int] = 30 * 60_000_000_000  # 30 minutes
    _accepted_strategies: Dict[str, HostSelector] = {"random": RandomHostSelector(), "round_robin": RoundRobinHostSelector()}
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
        return strategy in self._accepted_strategies.keys()

    def get_host_info_by_strategy(self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        if not self.accepts_strategy(role, strategy):
            raise AwsWrapperError(Messages.get_formatted(
                "ConnectionProvider.UnsupportedHostSelectorStrategy",
                strategy, SqlAlchemyPooledConnectionProvider.__class__.__name__))

        return self._accepted_strategies.get(strategy).get_host(hosts, role, props)

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            properties: Properties):
        queue_pool: Optional[QueuePool] = SqlAlchemyPooledConnectionProvider._database_pools.compute_if_absent(
            PoolKey(host_info.url, self._get_extra_key(host_info, properties)),
            lambda _: self._create_pool(target_func, driver_dialect, host_info, properties),
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
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties):
        kwargs = dict() if self._pool_configurator is None else self._pool_configurator(host_info, props)
        prepared_properties = driver_dialect.prepare_connect_info(host_info, props)
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
