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

"""Per-host, per-user async connection pool registry.

:class:`AsyncPooledConnectionProvider` is the async counterpart of
:class:`aws_advanced_python_wrapper.sql_alchemy_connection_provider.SqlAlchemyPooledConnectionProvider`,
implemented over asyncio primitives instead of SQLAlchemy's pool. The
underlying pool (:class:`_AsyncPool`) holds raw async connections
(``psycopg.AsyncConnection`` / ``aiomysql.Connection``); the proxy
(:class:`_PooledAsyncConnectionProxy`) routes ``close()`` /
``__aexit__`` to release-to-pool semantics.
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, ClassVar, Dict,
                    List, Optional, Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncCanReleaseResources
from aws_advanced_python_wrapper.aio.storage.sliding_expiration_cache_async import \
    AsyncSlidingExpirationCache
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_selector import (
    HighestWeightHostSelector, HostSelector, RandomHostSelector,
    RoundRobinHostSelector, WeightedRandomHostSelector)
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import PoolKey
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.connection_provider import \
        AsyncConnectionProvider  # noqa: F401  (Protocol the class satisfies)
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole

logger = Logger(__name__)


class _AsyncPool:
    """Bounded async connection pool over asyncio primitives.

    The pool's max concurrency is ``max_size + max_overflow``; ``acquire``
    blocks (with a timeout) when the cap is reached. Idle connections are
    held in an ``asyncio.Queue``; new ones are created on demand via the
    async ``creator`` callable.

    Disposal is one-way: once :meth:`dispose` is called the pool refuses
    new acquires and closes idle conns immediately; in-flight conns close
    on the next :meth:`release` instead of returning to the queue.
    """

    def __init__(
            self,
            creator: Callable[[], Awaitable[Any]],
            max_size: int = 5,
            max_overflow: int = 10,
            timeout_seconds: float = 30.0,
    ):
        self._creator = creator
        self._max_size = max_size
        self._max_overflow = max_overflow
        self._timeout_seconds = timeout_seconds
        self._idle: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_size + max_overflow)
        self._semaphore = asyncio.Semaphore(max_size + max_overflow)
        self._checkedout: int = 0
        self._disposing: bool = False

    def checkedout(self) -> int:
        return self._checkedout

    def is_disposing(self) -> bool:
        return self._disposing

    async def acquire(self) -> Any:
        if self._disposing:
            raise AwsWrapperError(
                Messages.get_formatted("AsyncPooledConnectionProvider.PoolDisposed", "<pool>"))
        await self._acquire_permit()
        try:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                conn = await self._creator()
            self._checkedout += 1
            return conn
        except BaseException:
            self._semaphore.release()
            raise

    async def _acquire_permit(self) -> None:
        """Acquire a pool permit, bounded by ``self._timeout_seconds``, without
        leaking the permit on timeout.

        ``asyncio.wait_for(semaphore.acquire(), t)`` can leak on Python 3.10:
        the permit may be granted in the same event-loop tick the waiter is
        cancelled, and ``Semaphore.acquire`` doesn't roll it back -- permanently
        shrinking effective pool capacity (fixed in 3.11's wait_for rewrite;
        3.10 is a supported version). Use ``asyncio.wait`` (which does NOT
        cancel on timeout), then cancel explicitly and release the permit if
        the acquire completed anyway.
        """
        acquire_task = asyncio.ensure_future(self._semaphore.acquire())
        done, _ = await asyncio.wait(
            {acquire_task}, timeout=self._timeout_seconds)
        if acquire_task in done:
            # Surfaces acquire() errors, if any (result is normally True).
            acquire_task.result()
            return
        acquire_task.cancel()
        acquired = False
        try:
            acquired = bool(await acquire_task)
        except asyncio.CancelledError:
            acquired = False
        if acquired:
            self._semaphore.release()
        raise asyncio.TimeoutError()

    async def release(self, conn: Any, invalidated: bool = False) -> None:
        try:
            self._checkedout -= 1
            if invalidated or self._disposing:
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
                return
            try:
                self._idle.put_nowait(conn)
            except asyncio.QueueFull:
                # Should not normally happen — semaphore should have prevented it.
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001 - best-effort close
                    pass
        finally:
            self._semaphore.release()

    async def dispose(self) -> None:
        self._disposing = True
        # Drain idle queue and close each conn.
        while True:
            try:
                conn = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await conn.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass


class _PooledAsyncConnectionProxy:
    """Wraps a raw async connection acquired from a :class:`_AsyncPool`.

    ``close()`` and ``__aexit__`` route to ``pool.release(...)`` instead
    of closing the underlying connection. Use :meth:`invalidate` before
    closing to force an actual close (e.g., after a SQL error left the
    connection in a bad state).

    All other attributes are delegated to the underlying connection via
    ``__getattr__``, so cursor / transaction / driver-specific methods
    work transparently.
    """

    def __init__(self, raw_conn: Any, pool: _AsyncPool):
        # Set as instance attributes so __getattr__ doesn't fall through to raw_conn.
        self._raw = raw_conn
        self._pool = pool
        self._invalidated = False
        self._released = False

    def invalidate(self, soft: bool = False) -> None:
        # Accept ``soft`` for parity with SQLAlchemy's
        # ``PoolProxiedConnection.invalidate(soft=...)``: the failover plugin's
        # ``_invalidate_current_connection`` calls ``invalidate(soft=True)`` to
        # mark a broken connection for eviction. Without the kwarg that call
        # raised TypeError, fell through, and the dead connection was returned
        # to the pool's idle queue (so the next checkout handed it back). The
        # flag makes ``close()`` actually close the raw connection instead.
        self._invalidated = True

    @property
    def driver_connection(self) -> Any:
        """The underlying raw driver connection beneath the pool proxy.

        Parity with SQLAlchemy's ``PoolProxiedConnection.driver_connection``
        (which the sync ``SqlAlchemyPooledConnectionProvider`` yields): lets
        callers reach the real connection, e.g. to assert the pool handed out
        a *fresh* underlying connection after an eviction.
        """
        return self._raw

    async def close(self) -> None:
        # Idempotent, like SQLAlchemy's pool fairy close: the wrapper's close
        # pipeline closes the target AND plugin_service.release_resources
        # closes the current connection afterwards (sync parity) -- a second
        # close must be a no-op, not a double pool.release (which corrupts
        # the checked-out count, the semaphore, and the idle queue).
        if self._released:
            return
        self._released = True
        await self._pool.release(self._raw, invalidated=self._invalidated)

    async def __aenter__(self) -> _PooledAsyncConnectionProxy:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    def __getattr__(self, name: str) -> Any:
        # Only fires for attributes not on `self`. The `_raw` / `_pool` /
        # `_invalidated` attributes set in __init__ shadow any equivalents
        # on the underlying conn.
        return getattr(self._raw, name)


class AsyncPooledConnectionProvider(AsyncCanReleaseResources):
    """Async counterpart of :class:`SqlAlchemyPooledConnectionProvider`.

    Per-host pooling keyed by ``PoolKey(host.url, extra_key)``. Pools
    expire after a sliding window of inactivity (default 30 minutes).

    Configuration callables mirror the sync class:

      * ``pool_configurator(host_info, props) -> dict`` -- kwargs forwarded
        to ``_AsyncPool.__init__`` (``max_size``, ``max_overflow``,
        ``timeout_seconds``). Defaults if omitted.
      * ``pool_mapping(host_info, props) -> str`` -- override the default
        "user" extra-key for pool-cache lookup.
      * ``accept_url_func(host_info, props) -> bool`` -- override the
        default RDS-instance-only filter.

    Five host-selector strategies supported: ``random``, ``round_robin``,
    ``weighted_random``, ``highest_weight``, and ``least_connections``
    (which counts active checked-out conns per host).
    """

    _POOL_EXPIRATION_CHECK_NS: ClassVar[int] = 30 * 60_000_000_000  # 30 minutes
    _LEAST_CONNECTIONS: ClassVar[str] = "least_connections"

    _accepted_strategies: ClassVar[Dict[str, HostSelector]] = {
        "random": RandomHostSelector(),
        "round_robin": RoundRobinHostSelector(),
        "weighted_random": WeightedRandomHostSelector(),
        "highest_weight": HighestWeightHostSelector(),
    }
    _rds_utils: ClassVar[RdsUtils] = RdsUtils()
    _database_pools: ClassVar[AsyncSlidingExpirationCache] = (
        AsyncSlidingExpirationCache(
            should_dispose_func=lambda pool_pair: pool_pair[0].checkedout() == 0,
            item_disposal_func=lambda pool_pair: pool_pair[0].dispose(),
        )
    )

    def __init__(
            self,
            pool_configurator: Optional[Callable[[Any, Properties], Dict[str, Any]]] = None,
            pool_mapping: Optional[Callable[[Any, Properties], str]] = None,
            accept_url_func: Optional[Callable[[Any, Properties], bool]] = None,
            pool_expiration_check_ns: int = -1,
            pool_cleanup_interval_ns: int = -1,
    ):
        self._pool_configurator = pool_configurator
        self._pool_mapping = pool_mapping
        self._accept_url_func = accept_url_func

        if pool_expiration_check_ns > -1:
            AsyncPooledConnectionProvider._POOL_EXPIRATION_CHECK_NS = pool_expiration_check_ns
        if pool_cleanup_interval_ns > -1:
            AsyncPooledConnectionProvider._database_pools.set_cleanup_interval_ns(
                pool_cleanup_interval_ns)

    @property
    def num_pools(self) -> int:
        return len(AsyncPooledConnectionProvider._database_pools)

    @property
    def pool_urls(self) -> Set[str]:
        return {pool_key.url for pool_key, _ in AsyncPooledConnectionProvider._database_pools.items()}

    def keys(self) -> List[PoolKey]:
        return AsyncPooledConnectionProvider._database_pools.keys()

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        if self._accept_url_func is not None:
            return self._accept_url_func(host_info, props)
        url_type = AsyncPooledConnectionProvider._rds_utils.identify_rds_type(host_info.host)
        return url_type == RdsUrlType.RDS_INSTANCE

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return (strategy == AsyncPooledConnectionProvider._LEAST_CONNECTIONS
                or strategy in self._accepted_strategies)

    def get_host_info_by_strategy(
            self,
            hosts: Tuple[HostInfo, ...],
            role: HostRole,
            strategy: str,
            props: Optional[Properties],
    ) -> HostInfo:
        if not self.accepts_strategy(role, strategy):
            raise AwsWrapperError(Messages.get_formatted(
                "ConnectionProvider.UnsupportedHostSelectorStrategy",
                strategy, type(self).__name__))

        if strategy == AsyncPooledConnectionProvider._LEAST_CONNECTIONS:
            valid_hosts = [host for host in hosts if host.role == role]
            valid_hosts.sort(key=lambda host: self._num_connections(host))
            if len(valid_hosts) == 0:
                raise AwsWrapperError(Messages.get_formatted("HostSelector.NoHostsMatchingRole", role))
            return valid_hosts[0]

        return self._accepted_strategies[strategy].get_host(hosts, role, props)

    def _num_connections(self, host_info: HostInfo) -> int:
        total = 0
        for pool_key, cache_item in AsyncPooledConnectionProvider._database_pools.items():
            if pool_key.url == host_info.url:
                pool, _ = cache_item.item
                total += pool.checkedout()
        return total

    async def connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect: AsyncDriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties,
    ) -> _PooledAsyncConnectionProxy:
        pool_key = PoolKey(host_info.url, self._get_extra_key(host_info, props))

        async def _create(_key: PoolKey) -> Tuple[_AsyncPool, Properties]:
            return await self._create_pool(target_func, driver_dialect, database_dialect, host_info, props)

        db_pool: Optional[Tuple[_AsyncPool, Properties]] = await AsyncPooledConnectionProvider._database_pools.compute_if_absent(
            pool_key, _create, AsyncPooledConnectionProvider._POOL_EXPIRATION_CHECK_NS)
        if db_pool is None:
            raise AwsWrapperError(
                Messages.get_formatted("AsyncPooledConnectionProvider.PoolNone", host_info.url))

        pool, creator_props = db_pool
        # Refresh the password held by the pool's creator closure so subsequent new
        # connections use the latest credential (e.g. a freshly minted IAM token).
        password = WrapperProperties.PASSWORD.get(props)
        if password is not None:
            creator_props[WrapperProperties.PASSWORD.name] = password

        raw_conn = await pool.acquire()
        return _PooledAsyncConnectionProxy(raw_conn, pool)

    def _get_extra_key(self, host_info: HostInfo, props: Properties) -> str:
        if self._pool_mapping is not None:
            return self._pool_mapping(host_info, props)
        user = props.get(WrapperProperties.USER.name, None)
        if user is None or user == "":
            raise AwsWrapperError(Messages.get("AsyncPooledConnectionProvider.UnableToCreateDefaultKey"))
        return user

    async def _create_pool(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect: AsyncDriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties,
    ) -> Tuple[_AsyncPool, Properties]:
        kwargs: Dict[str, Any] = (
            {} if self._pool_configurator is None else dict(self._pool_configurator(host_info, props)))
        # The pool_configurator contract is shared with the sync
        # SqlAlchemyPooledConnectionProvider, whose configurator dict feeds
        # SQLAlchemy's QueuePool (keys: pool_size, max_overflow, timeout).
        # _AsyncPool uses native names (max_size, max_overflow,
        # timeout_seconds); translate the QueuePool-style aliases so one
        # configurator works against either provider (async parity). Native
        # names, if also present, take precedence.
        if "pool_size" in kwargs:
            kwargs.setdefault("max_size", kwargs.pop("pool_size"))
        if "timeout" in kwargs:
            kwargs.setdefault("timeout_seconds", kwargs.pop("timeout"))
        prepared = driver_dialect.prepare_connect_info(host_info, props)
        database_dialect.prepare_conn_props(prepared)
        creator = self._get_connection_func(target_func, prepared)
        return _AsyncPool(creator=creator, **kwargs), prepared

    def _get_connection_func(
            self,
            target_connect_func: Callable[..., Awaitable[Any]],
            props: Properties,
    ) -> Callable[[], Awaitable[Any]]:
        async def _creator() -> Any:
            return await target_connect_func(**props)
        return _creator

    async def release_resources(self) -> None:
        items = list(AsyncPooledConnectionProvider._database_pools.items())
        for _, cache_item in items:
            try:
                pool, _ = cache_item.item
                await pool.dispose()
            except Exception:  # noqa: BLE001 - best-effort teardown; conns may already be dead
                pass
        await AsyncPooledConnectionProvider._database_pools.clear()


__all__ = [
    "AsyncPooledConnectionProvider",
    "PoolKey",
    # Private but re-exported for tests:
    "_AsyncPool",
    "_PooledAsyncConnectionProxy",
]
