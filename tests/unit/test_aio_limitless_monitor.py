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

"""Tests for AsyncLimitlessRouterMonitor + Cache + Service (L.1)."""

from __future__ import annotations

import asyncio
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.limitless_plugin import (
    AsyncLimitlessRouterCache, AsyncLimitlessRouterMonitor,
    AsyncLimitlessRouterService)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _reset_limitless_singletons():
    AsyncLimitlessRouterCache.clear()
    AsyncLimitlessRouterService._reset_for_tests()
    yield
    try:
        asyncio.run(AsyncLimitlessRouterService.stop_all())
    except RuntimeError:
        pass
    AsyncLimitlessRouterCache.clear()
    AsyncLimitlessRouterService._reset_for_tests()


# ----- AsyncLimitlessRouterCache -----------------------------------


def test_cache_put_and_get() -> None:
    hosts = [HostInfo("r1", 5432, role=HostRole.WRITER)]
    AsyncLimitlessRouterCache.put("cluster-a", hosts)
    result = AsyncLimitlessRouterCache.get("cluster-a")
    assert len(result) == 1
    assert result[0].host == "r1"


def test_cache_returns_empty_for_unknown_cluster() -> None:
    assert AsyncLimitlessRouterCache.get("nope") == []


def test_cache_put_replaces_previous_value() -> None:
    AsyncLimitlessRouterCache.put(
        "c", [HostInfo("old", 5432, role=HostRole.WRITER)])
    AsyncLimitlessRouterCache.put(
        "c", [HostInfo("new", 5432, role=HostRole.WRITER)])
    result = AsyncLimitlessRouterCache.get("c")
    assert [h.host for h in result] == ["new"]


def test_cache_clear_wipes_everything() -> None:
    AsyncLimitlessRouterCache.put("a", [HostInfo("x", 5432)])
    AsyncLimitlessRouterCache.put("b", [HostInfo("y", 5432)])
    AsyncLimitlessRouterCache.clear()
    assert AsyncLimitlessRouterCache.get("a") == []
    assert AsyncLimitlessRouterCache.get("b") == []


def test_cache_get_returns_copy_not_reference() -> None:
    hosts = [HostInfo("r1", 5432, role=HostRole.WRITER)]
    AsyncLimitlessRouterCache.put("c", hosts)
    result = AsyncLimitlessRouterCache.get("c")
    result.append(HostInfo("r2", 5432))  # type: ignore[arg-type]
    # Mutating the result must not affect the cached copy.
    assert len(AsyncLimitlessRouterCache.get("c")) == 1


# ----- AsyncLimitlessRouterService ---------------------------------


def _make_plugin_service(probe_conn: Any = None) -> Any:
    svc = MagicMock()
    svc.database_dialect = MagicMock()
    svc.database_dialect.limitless_router_endpoint_query = "SELECT * FROM limitless"
    svc.driver_dialect = MagicMock()
    svc.driver_dialect.abort_connection = AsyncMock()
    svc.connect = AsyncMock(return_value=probe_conn)
    return svc


def test_service_ensure_monitor_starts_task() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)

    async def _body():
        m = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-x")
        assert m.is_running() is True
        assert m.cluster_id == "cluster-x"

    asyncio.run(_body())


def test_service_ensure_monitor_dedupes_by_cluster_id() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)

    async def _body():
        m1 = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-x")
        m2 = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-x")
        assert m1 is m2

    asyncio.run(_body())


def test_service_separate_monitors_per_cluster() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)

    async def _body():
        m1 = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-a")
        m2 = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-b")
        assert m1 is not m2

    asyncio.run(_body())


def test_service_stop_all_terminates_running_monitors() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)

    async def _body():
        m = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-x")
        await AsyncLimitlessRouterService.stop_all()
        assert m.is_running() is False

    asyncio.run(_body())


# ----- AsyncLimitlessRouterMonitor lifecycle ----------------------


def _mock_cursor(rows: List[tuple]) -> MagicMock:
    cur = MagicMock(name="cursor")
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=None)
    cur.execute = AsyncMock(return_value=None)
    cur.fetchall = AsyncMock(return_value=rows)
    return cur


def _mock_conn(rows: List[tuple]) -> MagicMock:
    conn = MagicMock(name="probe_conn")
    conn.cursor = MagicMock(return_value=_mock_cursor(rows))
    return conn


def test_monitor_not_running_before_start() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    m = AsyncLimitlessRouterMonitor(
        svc, host, Properties(), 10_000, "c")
    assert m.is_running() is False


def test_monitor_refresh_populates_cache() -> None:
    probe_conn = _mock_conn([("router-1", 0.3), ("router-2", 0.1)])
    svc = _make_plugin_service(probe_conn=probe_conn)
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    m = AsyncLimitlessRouterMonitor(
        svc, host, Properties(), 10_000, "cluster-refresh")

    async def _body():
        # Drive one refresh explicitly rather than through the loop.
        await m._refresh_once()

    asyncio.run(_body())

    cached = AsyncLimitlessRouterCache.get("cluster-refresh")
    assert [h.host for h in cached] == ["router-1", "router-2"]


def test_monitor_refresh_swallows_probe_failures() -> None:
    svc = _make_plugin_service(probe_conn=None)
    svc.connect = AsyncMock(side_effect=RuntimeError("broken"))
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    m = AsyncLimitlessRouterMonitor(
        svc, host, Properties(), 10_000, "cluster-broken")

    async def _body():
        # Must not raise.
        await m._refresh_once()

    asyncio.run(_body())

    # Cache remains empty.
    assert AsyncLimitlessRouterCache.get("cluster-broken") == []


def test_monitor_stop_cancels_task_cleanly() -> None:
    probe_conn = _mock_conn([])
    svc = _make_plugin_service(probe_conn=probe_conn)
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    m = AsyncLimitlessRouterMonitor(
        svc, host, Properties(), 100, "cluster-stop")

    async def _body():
        m.start()
        assert m.is_running() is True
        await m.stop()
        assert m.is_running() is False

    asyncio.run(_body())


def test_monitor_stop_is_idempotent() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    m = AsyncLimitlessRouterMonitor(
        svc, host, Properties(), 100, "c")

    async def _body():
        await m.stop()
        await m.stop()

    asyncio.run(_body())
