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

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.pooled_connection_provider import (
    AsyncPooledConnectionProvider, PoolKey, _AsyncPool,
    _PooledAsyncConnectionProxy)
from aws_advanced_python_wrapper.aio.storage.sliding_expiration_cache_async import \
    AsyncSlidingExpirationCache
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

# ---------- _AsyncPool tests ----------


def test_pool_acquire_invokes_creator_when_idle_empty():
    async def inner():
        creator = AsyncMock(return_value="conn-1")
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        assert conn == "conn-1"
        assert pool.checkedout() == 1
        creator.assert_awaited_once()

    asyncio.run(inner())


def test_pool_acquire_reuses_idle_conn():
    async def inner():
        creator = AsyncMock(side_effect=["conn-A", "conn-B"])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        c1 = await pool.acquire()
        await pool.release(c1)  # back to idle
        c2 = await pool.acquire()
        assert c2 == "conn-A"  # reused
        assert creator.await_count == 1  # not called again

    asyncio.run(inner())


def test_pool_release_invalidated_actually_closes():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        await pool.release(conn, invalidated=True)
        raw.close.assert_awaited_once()
        assert pool.checkedout() == 0

    asyncio.run(inner())


def test_pool_dispose_closes_idle_conns():
    async def inner():
        raw_a, raw_b = AsyncMock(), AsyncMock()
        raw_a.close = AsyncMock()
        raw_b.close = AsyncMock()
        creator = AsyncMock(side_effect=[raw_a, raw_b])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        await pool.release(c1)
        await pool.release(c2)
        assert pool.checkedout() == 0
        await pool.dispose()
        raw_a.close.assert_awaited_once()
        raw_b.close.assert_awaited_once()
        assert pool.is_disposing()

    asyncio.run(inner())


def test_pool_acquire_after_dispose_raises():
    async def inner():
        creator = AsyncMock(return_value="c")
        pool = _AsyncPool(creator=creator, max_size=1, max_overflow=0)
        await pool.dispose()
        with pytest.raises(AwsWrapperError, match="PoolDisposed"):
            await pool.acquire()

    asyncio.run(inner())


def test_pool_release_when_disposing_actually_closes():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        # Mark for disposal (do not await dispose itself — release must observe state)
        pool._disposing = True  # internal state inspection for test
        await pool.release(conn)
        raw.close.assert_awaited_once()

    asyncio.run(inner())


def test_pool_acquire_blocks_at_max_when_overflow_zero():
    async def inner():
        creator = AsyncMock(side_effect=["a", "b", "c"])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0, timeout_seconds=0.05)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        # Third acquire should time out because semaphore is full
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.1)
        # Cleanup
        await pool.release(c1)
        await pool.release(c2)

    asyncio.run(inner())


def test_pool_acquire_uses_overflow_when_configured():
    async def inner():
        creator = AsyncMock(side_effect=["a", "b", "c"])
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=1, timeout_seconds=0.1)
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        c3 = await pool.acquire()  # overflow permits one more
        assert pool.checkedout() == 3
        # 4th would block / timeout
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.1)
        await pool.release(c1)
        await pool.release(c2)
        await pool.release(c3)

    asyncio.run(inner())


def test_pool_concurrent_acquire_release_keeps_checkedout_correct():
    async def inner():
        n = 50
        creator = AsyncMock(side_effect=[f"c{i}" for i in range(n)])
        pool = _AsyncPool(creator=creator, max_size=10, max_overflow=10)

        async def worker():
            conn = await pool.acquire()
            await asyncio.sleep(0.001)
            await pool.release(conn)

        await asyncio.gather(*(worker() for _ in range(n)))
        assert pool.checkedout() == 0

    asyncio.run(inner())


# ---------- _PooledAsyncConnectionProxy tests ----------


def test_proxy_invalidate_accepts_soft_kwarg():
    # The failover plugin's _invalidate_current_connection calls
    # invalidate(soft=True). Without the kwarg it raised TypeError and the dead
    # conn was returned to the pool instead of being evicted.
    proxy = _PooledAsyncConnectionProxy(MagicMock(), MagicMock())
    proxy.invalidate(soft=True)
    assert proxy._invalidated is True


def test_proxy_invalidated_close_actually_closes_raw():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        proxy.invalidate(soft=True)
        await proxy.close()
        # Invalidated -> raw is actually closed (evicted), not returned to idle.
        raw.close.assert_awaited()

    asyncio.run(inner())


def test_proxy_driver_connection_returns_raw():
    # Parity with SQLAlchemy's PoolProxiedConnection.driver_connection: the
    # proxy exposes the underlying raw driver connection so callers can assert
    # the pool handed out a fresh one after eviction (RWS pooled-failover tests).
    raw = MagicMock(name="raw")
    pool = MagicMock()
    proxy = _PooledAsyncConnectionProxy(raw, pool)
    assert proxy.driver_connection is raw


def test_proxy_close_returns_to_pool_not_actual_close():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        await proxy.close()
        # raw conn was NOT actually closed — returned to pool
        raw.close.assert_not_awaited()
        assert pool.checkedout() == 0
        # Re-acquire returns the same raw
        again = await pool.acquire()
        assert again is raw

    asyncio.run(inner())


def test_proxy_aexit_returns_to_pool():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        async with proxy:
            pass  # __aexit__ should release
        raw.close.assert_not_awaited()
        assert pool.checkedout() == 0

    asyncio.run(inner())


def test_proxy_invalidate_forces_actual_close_on_release():
    async def inner():
        raw = AsyncMock()
        raw.close = AsyncMock()
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        proxy.invalidate()
        await proxy.close()
        raw.close.assert_awaited_once()
        assert pool.checkedout() == 0

    asyncio.run(inner())


def test_proxy_delegates_unknown_attrs_to_raw_conn():
    async def inner():
        raw = AsyncMock()
        raw.cursor = AsyncMock(return_value="cursor-obj")
        creator = AsyncMock(return_value=raw)
        pool = _AsyncPool(creator=creator, max_size=2, max_overflow=0)
        conn = await pool.acquire()
        proxy = _PooledAsyncConnectionProxy(conn, pool)
        # Access an attribute not defined on the proxy itself
        result = await proxy.cursor()
        assert result == "cursor-obj"
        await proxy.close()

    asyncio.run(inner())


# ---------- AsyncPooledConnectionProvider tests ----------


def _make_host_info(host: str = "my-instance.abc123.us-east-1.rds.amazonaws.com",
                    port: int = 5432, role: HostRole = HostRole.WRITER) -> HostInfo:
    return HostInfo(host=host, port=port, role=role)


def _make_props(user: str = "admin", password: str = "secret") -> Properties:
    p = Properties()
    p[WrapperProperties.USER.name] = user
    p[WrapperProperties.PASSWORD.name] = password
    return p


def _reset_class_state():
    """Clear class-level pool cache between tests."""
    AsyncPooledConnectionProvider._database_pools = AsyncSlidingExpirationCache(
        cleanup_interval_ns=10 * 60_000_000_000,
        should_dispose_func=lambda pool: pool.checkedout() == 0,
        item_disposal_func=lambda pool: pool.dispose(),
    )


def test_accepts_host_info_default_filters_to_rds_instance():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    instance_host = _make_host_info()
    cluster_host = _make_host_info(host="my-cluster.cluster-abc123.us-east-1.rds.amazonaws.com")
    assert provider.accepts_host_info(instance_host, _make_props()) is True
    assert provider.accepts_host_info(cluster_host, _make_props()) is False


def test_accepts_host_info_uses_custom_accept_url_func():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider(
        accept_url_func=lambda host_info, props: host_info.host == "allow-me",
    )
    assert provider.accepts_host_info(_make_host_info(host="allow-me"), _make_props()) is True
    assert provider.accepts_host_info(_make_host_info(host="deny-me"), _make_props()) is False


def test_accepts_strategy_default_strategies():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    for strat in ("random", "round_robin", "weighted_random", "highest_weight"):
        assert provider.accepts_strategy(HostRole.WRITER, strat) is True


def test_accepts_strategy_least_connections():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    assert provider.accepts_strategy(HostRole.WRITER, "least_connections") is True


def test_accepts_strategy_unknown_returns_false():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    assert provider.accepts_strategy(HostRole.WRITER, "totally_made_up") is False


def test_get_host_info_by_strategy_least_connections_picks_lowest_checkedout():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        h1 = _make_host_info(host="h1")
        h2 = _make_host_info(host="h2")
        # Pre-seed pools with known checkout counts via direct manipulation.
        # Cache entries are (pool, Properties) tuples -- the same shape
        # ``connect`` stores and ``_num_connections`` unpacks (pool, _).
        pool_h1 = _AsyncPool(creator=AsyncMock(return_value="c"))
        pool_h2 = _AsyncPool(creator=AsyncMock(return_value="c"))
        pool_h1._checkedout = 5
        pool_h2._checkedout = 1
        await AsyncPooledConnectionProvider._database_pools.put(
            PoolKey(h1.url, "user"), (pool_h1, _make_props()), 10**12)
        await AsyncPooledConnectionProvider._database_pools.put(
            PoolKey(h2.url, "user"), (pool_h2, _make_props()), 10**12)
        chosen = provider.get_host_info_by_strategy(
            (h1, h2), HostRole.WRITER, "least_connections", _make_props())
        assert chosen is h2

    asyncio.run(inner())


def test_get_host_info_by_strategy_random_invokes_selector():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    h1 = _make_host_info(host="h1")
    h2 = _make_host_info(host="h2")
    chosen = provider.get_host_info_by_strategy(
        (h1, h2), HostRole.WRITER, "random", _make_props())
    assert chosen in (h1, h2)


def test_get_host_info_by_strategy_unsupported_raises():
    _reset_class_state()
    provider = AsyncPooledConnectionProvider()
    with pytest.raises(AwsWrapperError):
        provider.get_host_info_by_strategy(
            (_make_host_info(),), HostRole.WRITER, "totally_made_up", _make_props())


def test_connect_creates_pool_on_first_call_then_reuses():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        host = _make_host_info()
        props = _make_props()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(return_value=props)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        proxy1 = await provider.connect(target, driver_dialect, database_dialect, host, props)
        await proxy1.close()
        proxy2 = await provider.connect(target, driver_dialect, database_dialect, host, props)
        await proxy2.close()
        # Same pool key → only one pool created
        assert provider.num_pools == 1
        # Connection reused
        assert target.await_count == 1

    asyncio.run(inner())


def test_connect_creates_separate_pool_per_user():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        host = _make_host_info()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="alice"))
        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="bob"))
        assert provider.num_pools == 2

    asyncio.run(inner())


def test_connect_creates_separate_pool_per_url():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        h1 = _make_host_info(host="h1")
        h2 = _make_host_info(host="h2")
        await provider.connect(target, driver_dialect, database_dialect, h1, _make_props())
        await provider.connect(target, driver_dialect, database_dialect, h2, _make_props())
        assert provider.num_pools == 2

    asyncio.run(inner())


def test_pool_mapping_overrides_default_user_key():
    async def inner():
        _reset_class_state()
        # Use host:port as the pool-mapping key — NOT user
        provider = AsyncPooledConnectionProvider(
            pool_mapping=lambda host_info, props: f"{host_info.host}:{host_info.port}",
        )
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        host = _make_host_info()
        # Different users — should still hit ONE pool because mapping ignores user
        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="alice"))
        await provider.connect(target, driver_dialect, database_dialect, host, _make_props(user="bob"))
        assert provider.num_pools == 1

    asyncio.run(inner())


def test_pool_configurator_kwargs_applied():
    async def inner():
        _reset_class_state()

        def configurator(host_info, props):
            return {"max_size": 7, "max_overflow": 3, "timeout_seconds": 1.5}

        provider = AsyncPooledConnectionProvider(pool_configurator=configurator)
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        await provider.connect(target, driver_dialect, database_dialect, _make_host_info(), _make_props())
        # Inspect the created pool
        items = list(AsyncPooledConnectionProvider._database_pools.items())
        assert len(items) == 1
        # Cache entry is a (pool, Properties) tuple.
        pool, _ = items[0][1].item
        assert pool._max_size == 7
        assert pool._max_overflow == 3
        assert pool._timeout_seconds == 1.5

    asyncio.run(inner())


def test_pool_configurator_sqlalchemy_aliases_map_to_native():
    # Parity with the sync SqlAlchemyPooledConnectionProvider: a single
    # pool_configurator returning SQLAlchemy QueuePool-style keys (pool_size /
    # timeout) must work against the async provider too. _create_pool
    # translates pool_size -> max_size and timeout -> timeout_seconds.
    async def inner():
        _reset_class_state()

        def configurator(host_info, props):
            return {"pool_size": 7, "max_overflow": 3, "timeout": 1.5}

        provider = AsyncPooledConnectionProvider(pool_configurator=configurator)
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        await provider.connect(target, driver_dialect, database_dialect, _make_host_info(), _make_props())
        items = list(AsyncPooledConnectionProvider._database_pools.items())
        assert len(items) == 1
        pool, _ = items[0][1].item
        assert pool._max_size == 7
        assert pool._max_overflow == 3
        assert pool._timeout_seconds == 1.5

    asyncio.run(inner())


def test_default_pool_key_missing_user_raises():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        # Props without "user"
        empty = Properties()
        target = AsyncMock(return_value="raw-conn")
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        with pytest.raises(AwsWrapperError, match="Unable to create a default key"):
            await provider.connect(target, driver_dialect, database_dialect, _make_host_info(), empty)

    asyncio.run(inner())


def test_release_resources_disposes_all_pools():
    async def inner():
        _reset_class_state()
        provider = AsyncPooledConnectionProvider()
        target = AsyncMock(return_value=AsyncMock(close=AsyncMock()))
        driver_dialect = MagicMock()
        driver_dialect.prepare_connect_info = MagicMock(side_effect=lambda h, p: p)
        database_dialect = MagicMock()
        database_dialect.prepare_conn_props = MagicMock()

        await provider.connect(target, driver_dialect, database_dialect, _make_host_info(host="h1"), _make_props())
        await provider.connect(target, driver_dialect, database_dialect, _make_host_info(host="h2"), _make_props())
        assert provider.num_pools == 2

        await provider.release_resources()
        assert provider.num_pools == 0

    asyncio.run(inner())
