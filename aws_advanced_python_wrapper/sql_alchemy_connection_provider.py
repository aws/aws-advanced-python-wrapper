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

import time
from typing import (TYPE_CHECKING, Any, Callable, ClassVar, Dict, Optional,
                    Tuple)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect

from sqlalchemy import QueuePool, pool

from aws_advanced_python_wrapper.connection_provider import ConnectionProvider
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import (
    HighestWeightHostSelector, HostSelector, RandomHostSelector,
    RoundRobinHostSelector, WeightedRandomHostSelector)
from aws_advanced_python_wrapper.plugin import CanReleaseResources
from aws_advanced_python_wrapper.utils import transient_connect
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils
from aws_advanced_python_wrapper.utils.storage.sliding_expiration_cache import \
    SlidingExpirationCache


class SqlAlchemyPooledConnectionProvider(ConnectionProvider, CanReleaseResources):
    """
    This class can be passed to :py:method:`ConnectionProviderManager.connection_provider` to enable internal connection pools
    for each database instance in a cluster. By maintaining internal connection pools,
    the driver can improve performance by reusing old connection objects.
    """
    _POOL_EXPIRATION_CHECK_NS: ClassVar[int] = 30 * 60_000_000_000  # 30 minutes
    _LEAST_CONNECTIONS: ClassVar[str] = "least_connections"
    _accepted_strategies: Dict[str, HostSelector] = {"random": RandomHostSelector(),
                                                     "round_robin": RoundRobinHostSelector(),
                                                     "weighted_random": WeightedRandomHostSelector(),
                                                     "highest_weight": HighestWeightHostSelector()}
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()
    _database_pools: ClassVar[SlidingExpirationCache[PoolKey, Tuple[QueuePool, Properties]]] = SlidingExpirationCache(
        should_dispose_func=lambda pool_pair: pool_pair[0].checkedout() == 0,
        item_disposal_func=lambda pool_pair: pool_pair[0].dispose()
    )

    def __init__(
            self,
            pool_configurator: Optional[Callable] = None,
            pool_mapping: Optional[Callable] = None,
            accept_url_func: Optional[Callable] = None,
            pool_expiration_check_ns: int = -1,
            pool_cleanup_interval_ns: int = -1):
        self._pool_configurator = pool_configurator
        self._pool_mapping = pool_mapping
        self._accept_url_func = accept_url_func

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

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        if self._accept_url_func:
            return self._accept_url_func(host_info, props)
        url_type = SqlAlchemyPooledConnectionProvider._rds_utils.identify_rds_type(host_info.host)
        return RdsUrlType.RDS_INSTANCE == url_type

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy == SqlAlchemyPooledConnectionProvider._LEAST_CONNECTIONS or strategy in self._accepted_strategies

    def get_host_info_by_strategy(self, hosts: Tuple[HostInfo, ...], role: HostRole, strategy: str, props: Optional[Properties]) -> HostInfo:
        if not self.accepts_strategy(role, strategy):
            raise AwsWrapperError(Messages.get_formatted(
                "ConnectionProvider.UnsupportedHostSelectorStrategy",
                strategy, SqlAlchemyPooledConnectionProvider.__class__.__name__))

        if strategy == SqlAlchemyPooledConnectionProvider._LEAST_CONNECTIONS:
            valid_hosts = [host for host in hosts if host.role == role]
            valid_hosts.sort(key=lambda host: self._num_connections(host))

            if len(valid_hosts) == 0:
                raise AwsWrapperError(Messages.get_formatted("HostSelector.NoHostsMatchingRole", role))

            return valid_hosts[0]

        return self._accepted_strategies[strategy].get_host(hosts, role, props)

    def _num_connections(self, host_info: HostInfo) -> int:
        """
        Returns the number of active pooled connections to a specific host.
        :param host_info: the host to analyze.
        :return: number of connections opened in the connection pool to the given host.
        """
        num_connections = 0
        for pool_key, cache_item in SqlAlchemyPooledConnectionProvider._database_pools.items():
            if pool_key.url == host_info.url:
                queue_pool, _ = cache_item.item
                num_connections += queue_pool.checkedout()
        return num_connections

    def connect(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties):
        db_pool: Optional[Tuple[QueuePool, Properties]] = SqlAlchemyPooledConnectionProvider._database_pools.compute_if_absent(
            PoolKey(host_info.url, self._get_extra_key(host_info, props)),
            lambda _: self._create_pool(target_func, driver_dialect, database_dialect, host_info, props),
            SqlAlchemyPooledConnectionProvider._POOL_EXPIRATION_CHECK_NS
        )

        if db_pool is None:
            raise AwsWrapperError(Messages.get_formatted("SqlAlchemyPooledConnectionProvider.PoolNone", host_info.url))

        queue_pool, creator_props = db_pool
        # Refresh the password held by the pool's creator closure so subsequent new
        # connections use the latest credential (e.g. a freshly minted IAM token).
        password = WrapperProperties.PASSWORD.get(props)
        if password is not None:
            creator_props[WrapperProperties.PASSWORD.name] = password

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
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties):
        kwargs = dict() if self._pool_configurator is None else self._pool_configurator(host_info, props)
        prepared_properties = driver_dialect.prepare_connect_info(host_info, props)
        database_dialect.prepare_conn_props(prepared_properties)
        kwargs["creator"] = self._get_connection_func(target_func, prepared_properties)
        return self._create_sql_alchemy_pool(**kwargs), prepared_properties

    # SA's pool refill goes straight through ``target_connect_func`` and
    # bypasses the wrapper's plugin chain, so this layer needs its own
    # transient-retry. Classification and backoff are centralised in
    # ``utils.transient_connect`` so all retry sites (sync pool, Django
    # backend) stay in sync. The budget itself is
    # configurable via ``WrapperProperties.CONNECTION_RETRY_MAX_ATTEMPTS``
    # and ``CONNECTION_RETRY_MAX_BACKOFF_S`` — defaults match
    # ``transient_connect.DEFAULT_*`` for backwards compatibility.
    _TRANSIENT_CONNECT_MAX_ATTEMPTS: ClassVar[int] = transient_connect.DEFAULT_MAX_ATTEMPTS

    def _get_connection_func(self, target_connect_func: Callable, props: Properties):
        max_attempts = WrapperProperties.CONNECTION_RETRY_MAX_ATTEMPTS.get_int(props)
        max_backoff = WrapperProperties.CONNECTION_RETRY_MAX_BACKOFF_S.get_float(props)

        def _connect_with_transient_retry() -> Any:
            last_exc: Optional[BaseException] = None
            for attempt in range(max_attempts):
                try:
                    return target_connect_func(**props)
                except Exception as exc:
                    last_exc = exc
                    if transient_connect.is_transient_connect_error(exc) \
                            and attempt < max_attempts - 1:
                        time.sleep(transient_connect.compute_backoff(
                            attempt, max_backoff=max_backoff))
                        continue
                    raise
            # Defensive: loop exits via return or raise above; this is
            # unreachable but keeps mypy from inferring an implicit None.
            assert last_exc is not None
            raise last_exc
        return _connect_with_transient_retry

    def _create_sql_alchemy_pool(self, **kwargs):
        return pool.QueuePool(**kwargs)

    def release_resources(self):
        for _, cache_item in SqlAlchemyPooledConnectionProvider._database_pools.items():
            try:
                queue_pool, _ = cache_item.item
                queue_pool.dispose()
            except Exception:
                # Swallow exception, connections may already be dead
                pass
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
