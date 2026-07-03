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

"""Tests for the N.1b monitor-wiring refactor on AsyncAuroraHostListProvider.

Verifies:
- force_refresh delegates through AsyncClusterTopologyMonitor (so
  panic-mode probing auto-engages).
- Monitor's internal refresh path uses _fetch_from_db (no public-API
  recursion).
- Fallback path: if the monitor can't be built or raises, force_refresh
  still returns a topology via the direct-DB fallback.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio import cleanup as aio_cleanup
from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
    AsyncClusterTopologyMonitor
from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncAuroraHostListProvider
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _clear_hooks():
    aio_cleanup.clear_shutdown_hooks()
    yield
    aio_cleanup.clear_shutdown_hooks()


def _provider_with_rows(rows: list) -> AsyncAuroraHostListProvider:
    """Build a provider whose _run_topology_query returns ``rows``."""
    provider = AsyncAuroraHostListProvider(
        Properties({"host": "cluster.example", "port": "5432"}),
        MagicMock(),  # driver_dialect not used in these tests
    )

    async def _rows(_conn):  # noqa: ARG001 - signature fixed
        return rows

    provider._run_topology_query = _rows  # type: ignore[method-assign]
    return provider


def test_force_refresh_goes_through_monitor() -> None:
    """After N.1b, force_refresh must build + consult the monitor
    rather than query the DB directly."""
    provider = _provider_with_rows([("srv-1", True), ("srv-2", False)])

    async def _body():
        conn = object()
        topology = await provider.force_refresh(conn)
        # Topology came back, so either the monitor OR the fallback
        # populated the cache. Monitor existence is the marker of
        # proper N.1b wiring.
        assert provider._monitor is not None
        assert isinstance(provider._monitor, AsyncClusterTopologyMonitor)
        assert len(topology) == 2
        await provider._monitor.stop()

    asyncio.run(_body())


def test_monitor_uses_fetch_from_db_not_force_refresh() -> None:
    """Recursion check: monitor's internal refresh path must call
    _fetch_from_db, not the public force_refresh (which now routes
    through the monitor)."""
    provider = _provider_with_rows([("srv-1", True)])

    fetch_calls: list = []
    original_fetch = provider._fetch_from_db

    async def _instrumented_fetch(conn):
        fetch_calls.append(conn)
        return await original_fetch(conn)

    provider._fetch_from_db = _instrumented_fetch  # type: ignore[method-assign]

    async def _body():
        monitor = AsyncClusterTopologyMonitor(
            provider=provider,
            connection_getter=lambda: None,
        )
        topology = await monitor.force_refresh_with_connection(
            conn=object(),
            bypass_ignore_window=True,
        )
        assert len(topology) == 1
        # _fetch_from_db was called exactly once (no recursion).
        assert len(fetch_calls) == 1

    asyncio.run(_body())


def test_fetch_from_db_does_not_recurse_into_monitor() -> None:
    """_fetch_from_db is the internal path used BY the monitor; it
    must not itself engage the monitor."""
    provider = _provider_with_rows([("srv-1", True)])

    async def _body():
        topology = await provider._fetch_from_db(object())
        assert len(topology) == 1
        # _fetch_from_db is the internal path; it must not build a
        # monitor. (If it did, we'd recurse.)
        assert provider._monitor is None

    asyncio.run(_body())


def test_force_refresh_falls_back_to_direct_query_on_monitor_failure() -> None:
    """If the monitor raises, force_refresh still returns a topology
    via _fetch_and_cache."""
    provider = _provider_with_rows([("srv-1", True)])

    async def _body():
        # Force a failure by swapping out the monitor's
        # force_refresh_with_connection to raise.
        real_get_monitor = provider._get_or_create_monitor

        def _broken_monitor():
            m = real_get_monitor()
            m.force_refresh_with_connection = AsyncMock(  # type: ignore[method-assign]
                side_effect=RuntimeError("monitor boom"))
            return m

        provider._get_or_create_monitor = _broken_monitor  # type: ignore[method-assign]
        topology = await provider.force_refresh(object())
        assert len(topology) == 1
        if provider._monitor is not None:
            await provider._monitor.stop()

    asyncio.run(_body())


def test_monitor_teardown_registered_with_cleanup_hook() -> None:
    """Building the monitor must register its stop() with the global
    cleanup hook so release_resources_async tears it down."""
    provider = _provider_with_rows([("srv-1", True)])

    async def _body():
        hooks_before = len(aio_cleanup._registered_shutdown_hooks)
        monitor = provider._get_or_create_monitor()
        assert monitor is not None
        hooks_after = len(aio_cleanup._registered_shutdown_hooks)
        assert hooks_after > hooks_before
        await monitor.stop()

    asyncio.run(_body())


def test_refresh_cache_short_circuits_without_hitting_monitor() -> None:
    """refresh() with a fresh cache must not build a monitor."""
    provider = _provider_with_rows([("srv-1", True)])

    async def _body():
        # Prime the cache via force_refresh.
        conn = object()
        await provider.force_refresh(conn)
        if provider._monitor is not None:
            await provider._monitor.stop()
        # Clear the monitor reference to simulate post-cleanup state.
        provider._monitor = None

        # refresh() should return cached topology without building a
        # new monitor.
        result = await provider.refresh(conn)
        assert len(result) == 1
        assert provider._monitor is None

    asyncio.run(_body())
