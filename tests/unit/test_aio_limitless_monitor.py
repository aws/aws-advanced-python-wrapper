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

"""Tests for AsyncLimitlessRouterMonitor + Cache + Service + QueryHelper.

Covers the standing-monitor machinery ported from sync ``LimitlessRouterMonitor``:
force_connect probe, router-cache TTL (LIMITLESS_MONITOR_DISPOSAL_TIME_MS),
idle-monitor disposal, dialect-gated + 5s-bounded router query.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.limitless_plugin import (
    AsyncLimitlessQueryHelper, AsyncLimitlessRouterCache,
    AsyncLimitlessRouterMonitor, AsyncLimitlessRouterService)
from aws_advanced_python_wrapper.database_dialect import AuroraPgDialect
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.messages import Messages
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


# ----- helpers -----------------------------------------------------


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


def _make_plugin_service(probe_conn: Any = None) -> Any:
    """Plugin service whose ``force_connect`` returns the probe connection.

    The monitor now probes via ``force_connect`` (not ``connect``) to match sync
    limitless_plugin.py:225.
    """
    svc = MagicMock()
    # A real dialect is required: the runtime_checkable AuroraLimitlessDialect
    # isinstance check does not accept a bare MagicMock (Python 3.12+).
    svc.database_dialect = AuroraPgDialect()
    svc.driver_dialect = MagicMock()
    svc.driver_dialect.is_closed = AsyncMock(return_value=False)
    svc.driver_dialect.abort_connection = AsyncMock()
    svc.force_connect = AsyncMock(return_value=probe_conn)
    svc.get_telemetry_factory = MagicMock(return_value=MagicMock())
    return svc


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


def test_cache_entry_expires_after_ttl() -> None:
    # A zero (already-elapsed) TTL: the next monotonic read is >= the deadline,
    # so the entry is treated as absent -- mirrors sync's item-expiration TTL.
    AsyncLimitlessRouterCache.put(
        "c", [HostInfo("r1", 5432, role=HostRole.WRITER)], ttl_ns=0)
    assert AsyncLimitlessRouterCache.get("c") == []


def test_cache_entry_survives_within_ttl() -> None:
    AsyncLimitlessRouterCache.put(
        "c", [HostInfo("r1", 5432, role=HostRole.WRITER)], ttl_ns=60 * 1_000_000_000)
    assert len(AsyncLimitlessRouterCache.get("c")) == 1


# ----- AsyncLimitlessQueryHelper -----------------------------------


def test_query_helper_unsupported_dialect_raises() -> None:
    svc = MagicMock()
    # A bare object lacks the AuroraLimitlessDialect protocol members, so the
    # runtime_checkable isinstance fails (a MagicMock would auto-satisfy it).
    svc.database_dialect = object()
    helper = AsyncLimitlessQueryHelper(svc)

    async def _body():
        with pytest.raises(UnsupportedOperationError) as exc:
            await helper.query_for_limitless_routers(_mock_conn([]), 5432)
        assert str(exc.value) == Messages.get(
            "LimitlessQueryHelper.UnsupportedDialectOrDatabase")

    asyncio.run(_body())


def test_query_helper_maps_rows_to_weighted_hosts() -> None:
    svc = _make_plugin_service()
    helper = AsyncLimitlessQueryHelper(svc)
    conn = _mock_conn([("router-1", 0.3), ("router-2", 0.1)])

    async def _body():
        return await helper.query_for_limitless_routers(conn, 5432)

    hosts = asyncio.run(_body())
    assert [h.host for h in hosts] == ["router-1", "router-2"]
    # weight = clamp(10 - floor(cpu * 10), 1, 10)
    assert [h.weight for h in hosts] == [7, 9]
    assert all(h.port == 5432 for h in hosts)


def test_query_helper_clamps_invalid_load_to_weight_one() -> None:
    svc = _make_plugin_service()
    helper = AsyncLimitlessQueryHelper(svc)
    # cpu == 1.0 -> 10 - floor(10) == 0, which is < 1 and clamps to 1.
    conn = _mock_conn([("router-maxed", 1.0)])

    async def _body():
        return await helper.query_for_limitless_routers(conn, 5432)

    hosts = asyncio.run(_body())
    assert len(hosts) == 1
    assert hosts[0].weight == 1


def test_query_helper_times_out(monkeypatch) -> None:
    monkeypatch.setattr(
        AsyncLimitlessQueryHelper, "_DEFAULT_QUERY_TIMEOUT_SEC", 0.05)
    svc = _make_plugin_service()
    helper = AsyncLimitlessQueryHelper(svc)

    class _SlowCursor:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def execute(self, query):
            await asyncio.sleep(0.5)

        async def fetchall(self):
            return []

    conn = MagicMock(name="slow_conn")
    conn.cursor = MagicMock(return_value=_SlowCursor())

    async def _body():
        with pytest.raises(asyncio.TimeoutError):
            await helper.query_for_limitless_routers(conn, 5432)

    asyncio.run(_body())


# ----- AsyncLimitlessRouterService monitor registry ----------------


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


def test_service_disposes_stale_monitor() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)

    async def _body():
        m = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-idle")
        # Backdate activity beyond the disposal window so the monitor is stale.
        # (No await occurs between ensure_monitor and here, so the monitor task
        # has not yet refreshed ``_last_activity_ns``.)
        m._last_activity_ns = time.perf_counter_ns() - (m._disposal_time_ns + 1_000_000_000)
        await AsyncLimitlessRouterService.dispose_stale_monitors()
        assert m.is_running() is False
        assert "cluster-idle" not in AsyncLimitlessRouterService._monitors

    asyncio.run(_body())


def test_service_keeps_fresh_monitor() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)

    async def _body():
        m = AsyncLimitlessRouterService.ensure_monitor(
            svc, host, Properties(), 10_000, "cluster-fresh")
        await AsyncLimitlessRouterService.dispose_stale_monitors()
        assert "cluster-fresh" in AsyncLimitlessRouterService._monitors
        assert m.is_running() is True

    asyncio.run(_body())


# ----- AsyncLimitlessRouterMonitor lifecycle ----------------------


def test_monitor_not_running_before_start() -> None:
    svc = _make_plugin_service()
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    m = AsyncLimitlessRouterMonitor(
        svc, host, Properties(), 10_000, "c")
    assert m.is_running() is False


def test_monitor_refresh_populates_cache_via_force_connect() -> None:
    probe_conn = _mock_conn([("router-1", 0.3), ("router-2", 0.1)])
    svc = _make_plugin_service(probe_conn=probe_conn)
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    m = AsyncLimitlessRouterMonitor(
        svc, host, Properties(), 10_000, "cluster-refresh")

    async def _body():
        # Drive one refresh explicitly rather than through the loop.
        await m._refresh_once()

    asyncio.run(_body())

    svc.force_connect.assert_awaited()
    cached = AsyncLimitlessRouterCache.get("cluster-refresh")
    assert [h.host for h in cached] == ["router-1", "router-2"]


def test_monitor_probe_props_strip_prefix_and_force_no_wait() -> None:
    from aws_advanced_python_wrapper.utils.properties import WrapperProperties

    svc = _make_plugin_service(probe_conn=_mock_conn([]))
    host = HostInfo("h", 5432, role=HostRole.WRITER)
    props = Properties({
        "limitless-router-monitor-connect_timeout": "3",
        "limitless_wait_for_transaction_router_info": True,
    })
    m = AsyncLimitlessRouterMonitor(svc, host, props, 10_000, "cluster-prefix")

    # The monitoring-prefixed key is promoted to its bare form...
    assert m._properties.get("connect_timeout") == "3"
    assert "limitless-router-monitor-connect_timeout" not in m._properties
    # ...and WAIT_FOR_ROUTER_INFO is forced False so the probe never
    # re-triggers router discovery.
    assert WrapperProperties.WAIT_FOR_ROUTER_INFO.get(m._properties) is False


def test_monitor_refresh_swallows_probe_failures() -> None:
    svc = _make_plugin_service(probe_conn=None)
    svc.force_connect = AsyncMock(side_effect=RuntimeError("broken"))
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
