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

"""Unit tests for :class:`AsyncLimitlessPlugin` (minimal port).

Covers the five load-bearing branches of the minimal async port:

1. Dialect missing ``limitless_router_endpoint_query`` -> return the
   initial connection unchanged.
2. Query returns no rows -> return the initial connection unchanged.
3. Query returns routers and the strategy picks a different host ->
   open a router connection, abort the initial one.
4. Query returns routers and the picked router equals the original host
   -> keep the initial connection (no new connect).
5. Cache hit: a second connect within the TTL window re-uses the cached
   router list without re-querying the database.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.limitless_plugin import (
    AsyncLimitlessPlugin, AsyncLimitlessRouterCache,
    AsyncLimitlessRouterService)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _reset_limitless_singletons():
    """Clear the cluster-level cache + running monitors between tests."""
    AsyncLimitlessRouterCache.clear()
    AsyncLimitlessRouterService._reset_for_tests()
    yield
    try:
        asyncio.run(AsyncLimitlessRouterService.stop_all())
    except RuntimeError:
        pass
    AsyncLimitlessRouterCache.clear()
    AsyncLimitlessRouterService._reset_for_tests()

# ---- Helpers -----------------------------------------------------------


def _host(host: str, port: int = 5432) -> HostInfo:
    return HostInfo(host=host, port=port, role=HostRole.WRITER)


def _make_rows_cursor(rows: list) -> MagicMock:
    """Build a mock cursor that acts as a psycopg/aiomysql async cursor.

    The real cursor is returned from a sync ``connection.cursor()`` call,
    supports ``async with`` context management, and exposes ``execute``
    / ``fetchall`` as awaitables.
    """
    cur = MagicMock(name="cursor")
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=None)
    cur.execute = AsyncMock(return_value=None)
    cur.fetchall = AsyncMock(return_value=rows)
    return cur


def _make_conn(rows: list) -> MagicMock:
    conn = MagicMock(name="initial_conn")
    conn.cursor = MagicMock(return_value=_make_rows_cursor(rows))
    return conn


def _build(
        *,
        rows: list,
        dialect_has_query: bool = True,
        strategy_returns: Any = None,
) -> tuple:
    """Construct plugin + mocks.

    Returns ``(plugin, plugin_service, driver_dialect, initial_conn)``.
    ``strategy_returns`` is used as the return value of
    ``plugin_service.get_host_info_by_strategy``.
    """
    initial_conn = _make_conn(rows)

    database_dialect: Any
    if dialect_has_query:
        database_dialect = MagicMock(name="database_dialect")
        database_dialect.limitless_router_endpoint_query = (
            "SELECT router_endpoint, load FROM x")
    else:
        # Bare object() has no ``limitless_router_endpoint_query`` attr,
        # so ``getattr(..., default=None)`` returns None and the plugin
        # treats this as a non-Limitless dialect. MagicMock would
        # auto-create the attribute, so it's unsuitable here.
        database_dialect = object()

    plugin_service = MagicMock(name="plugin_service")
    plugin_service.database_dialect = database_dialect
    plugin_service.get_host_info_by_strategy = MagicMock(
        return_value=strategy_returns)

    driver_dialect = MagicMock(name="driver_dialect")
    driver_dialect.connect = AsyncMock()
    driver_dialect.abort_connection = AsyncMock()

    # Router connections now route through plugin_service.connect (pipeline).
    # Alias it onto driver_dialect.connect so tests that configure
    # driver_dialect.connect.return_value carry over; individual tests
    # may override plugin_service.connect directly for finer control.
    plugin_service.connect = driver_dialect.connect

    plugin = AsyncLimitlessPlugin(plugin_service, Properties({}))
    return plugin, plugin_service, driver_dialect, initial_conn


async def _connect_func_factory(conn: Any):
    async def _connect_func() -> Any:
        return conn
    return _connect_func


# ---- 1: dialect missing query ----------------------------------------


def test_connect_returns_initial_conn_when_dialect_has_no_query():
    async def _body() -> None:
        plugin, plugin_service, driver_dialect, initial_conn = _build(
            rows=[], dialect_has_query=False)
        connect_func = await _connect_func_factory(initial_conn)

        result = await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=_host("shard-group.example"),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=connect_func,
        )

        assert result is initial_conn
        # No router selection attempted, no driver.connect, no abort.
        plugin_service.get_host_info_by_strategy.assert_not_called()
        driver_dialect.connect.assert_not_awaited()
        driver_dialect.abort_connection.assert_not_awaited()

    asyncio.run(_body())


# ---- 2: empty router list ---------------------------------------------


def test_connect_returns_initial_conn_when_router_list_is_empty():
    async def _body() -> None:
        plugin, plugin_service, driver_dialect, initial_conn = _build(
            rows=[])
        connect_func = await _connect_func_factory(initial_conn)

        result = await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=_host("shard-group.example"),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=connect_func,
        )

        assert result is initial_conn
        plugin_service.get_host_info_by_strategy.assert_not_called()
        driver_dialect.connect.assert_not_awaited()

    asyncio.run(_body())


# ---- 3: strategy picks a router -> swap connection --------------------


def test_connect_opens_router_connection_when_strategy_picks_one():
    async def _body() -> None:
        rows = [("router-a.example", 0.1), ("router-b.example", 0.5)]
        picked = _host("router-a.example")
        plugin, plugin_service, driver_dialect, initial_conn = _build(
            rows=rows, strategy_returns=picked)

        router_conn = MagicMock(name="router_conn")
        plugin_service.connect = AsyncMock(return_value=router_conn)

        connect_func = await _connect_func_factory(initial_conn)

        result = await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=_host("shard-group.example"),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=connect_func,
        )

        assert result is router_conn
        # The strategy must be asked for a router.
        plugin_service.get_host_info_by_strategy.assert_called_once()
        args, _ = plugin_service.get_host_info_by_strategy.call_args
        # role, strategy, router-list
        assert args[0] == HostRole.WRITER
        assert args[1] == "weighted_random"
        # Two routers parsed from rows.
        assert len(args[2]) == 2
        # Router conn was opened through the pipeline; initial conn
        # was aborted.
        plugin_service.connect.assert_awaited_once()
        driver_dialect.abort_connection.assert_awaited_once_with(
            initial_conn)

    asyncio.run(_body())


# ---- 4: picked router == original host -> keep initial conn -----------


def test_connect_keeps_initial_conn_when_host_already_a_router():
    async def _body() -> None:
        # host_info.host matches one of the router endpoints; plugin
        # must short-circuit and not open a second connection.
        rows = [("shard-group.example", 0.1), ("router-b.example", 0.5)]
        plugin, plugin_service, driver_dialect, initial_conn = _build(
            rows=rows, strategy_returns=_host("router-b.example"))

        connect_func = await _connect_func_factory(initial_conn)

        result = await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=_host("shard-group.example"),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=connect_func,
        )

        assert result is initial_conn
        # Strategy should NOT be consulted -- we short-circuit before it.
        plugin_service.get_host_info_by_strategy.assert_not_called()
        driver_dialect.connect.assert_not_awaited()
        driver_dialect.abort_connection.assert_not_awaited()

    asyncio.run(_body())


# ---- 5: cache hit on second connect within TTL ------------------------


def test_second_connect_within_ttl_reuses_cached_router_list():
    async def _body() -> None:
        rows = [("router-a.example", 0.1)]
        picked = _host("router-a.example")
        plugin, plugin_service, driver_dialect, initial_conn = _build(
            rows=rows, strategy_returns=picked)

        # First connect: query fires, cache gets populated.
        router_conn1 = MagicMock(name="router_conn1")
        router_conn2 = MagicMock(name="router_conn2")
        plugin_service.connect = AsyncMock(
            side_effect=[router_conn1, router_conn2])

        connect_func1 = await _connect_func_factory(initial_conn)
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=_host("shard-group.example"),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=connect_func1,
        )
        first_cursor_calls = initial_conn.cursor.call_count
        assert first_cursor_calls == 1  # one query issued

        # Second connect: fresh initial conn, but the plugin's cache is
        # still warm so no cursor() should be invoked on the new conn.
        initial_conn2 = _make_conn(rows)
        connect_func2 = await _connect_func_factory(initial_conn2)
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=_host("shard-group.example"),
            props=Properties({}),
            is_initial_connection=False,
            connect_func=connect_func2,
        )

        assert initial_conn2.cursor.call_count == 0, (
            "Second connect within TTL must NOT re-query the database.")
        # Strategy should still have been consulted both times.
        assert plugin_service.get_host_info_by_strategy.call_count == 2

    asyncio.run(_body())
