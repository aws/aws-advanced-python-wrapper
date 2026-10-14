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

"""Tests for how the async provider acquires its topology monitor.

The monitor comes from the monitor service, keyed by cluster and event loop,
so providers on one cluster share it. The monitor holds topology utils rather
than a provider, which is what lets it be shared.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

from aws_advanced_python_wrapper.aio import cleanup as aio_cleanup
from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
    AsyncClusterTopologyMonitor
from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncAuroraHostListProvider
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.properties import Properties

HOST = "cluster.example"


def _provider(rows: list, **props: str) -> AsyncAuroraHostListProvider:
    """A provider whose topology query returns ``rows``."""
    base = {"host": HOST, "port": "5432"}
    base.update(props)
    provider = AsyncAuroraHostListProvider(Properties(base), MagicMock())

    async def _fetch_all(_conn, _query):
        return rows

    provider._topology_utils._fetch_all = _fetch_all  # type: ignore[method-assign]
    return provider


def _monitor_count() -> int:
    return services_container.get_monitor_service().count(
        AsyncClusterTopologyMonitor)


def test_force_refresh_goes_through_the_monitor() -> None:
    """Delegating is what engages panic-mode probing when writer discovery
    stalls, so a direct query that returns the right hosts is not enough."""
    provider = _provider([("srv-1", True), ("srv-2", False)])

    async def _body():
        monitor = provider._get_or_create_monitor()
        assert isinstance(monitor, AsyncClusterTopologyMonitor)
        spy = AsyncMock(wraps=monitor.force_refresh_with_connection)
        monitor.force_refresh_with_connection = spy  # type: ignore[method-assign]

        topology = await provider.force_refresh(object())

        spy.assert_awaited_once()
        assert len(topology) == 2
        await monitor.aclose()

    asyncio.run(_body())


def test_the_monitor_holds_no_provider_reference() -> None:
    """What makes one monitor serve every provider on the cluster."""
    provider = _provider([("srv-1", True)])

    async def _body():
        monitor = provider._get_or_create_monitor()
        assert monitor is not None
        assert monitor._topology_utils is provider._topology_utils
        # Nothing on the monitor refers back to a provider.
        assert not any(getattr(monitor, name, None) is provider
                       for name in vars(monitor))
        await monitor.aclose()

    asyncio.run(_body())


def test_two_providers_on_one_cluster_share_one_monitor() -> None:
    first = _provider([("srv-1", True)])
    second = _provider([("srv-1", True)])

    async def _body():
        monitor_one = first._get_or_create_monitor()
        monitor_two = second._get_or_create_monitor()
        assert monitor_one is monitor_two
        assert _monitor_count() == 1
        await monitor_one.aclose()

    asyncio.run(_body())


def test_providers_on_different_clusters_get_different_monitors() -> None:
    first = _provider([("srv-1", True)], cluster_id="cluster-a")
    second = _provider([("srv-1", True)], cluster_id="cluster-b")

    async def _body():
        monitor_one = first._get_or_create_monitor()
        monitor_two = second._get_or_create_monitor()
        assert monitor_one is not monitor_two
        assert _monitor_count() == 2
        await monitor_one.aclose()
        await monitor_two.aclose()

    asyncio.run(_body())


def test_a_monitor_that_is_not_running_is_replaced() -> None:
    """The monitor service has no liveness check of its own.

    Its cache extends expiration on every hit, so a monitor whose loop has
    closed would be handed out until the stuck-monitor sweep removed it, and
    meanwhile every blocking refresh waits for an update that cannot arrive.
    """
    provider = _provider([("srv-1", True)])

    async def _body():
        first = provider._get_or_create_monitor()
        assert first is not None
        await first.aclose()
        assert not first.is_running()

        second = provider._get_or_create_monitor()
        assert second is not first
        assert second.is_running()
        # The dead one was removed rather than left alongside its replacement.
        assert _monitor_count() == 1
        await second.aclose()

    asyncio.run(_body())


def test_force_refresh_falls_back_to_a_direct_query_when_the_monitor_raises() -> None:
    provider = _provider([("srv-1", True)])

    async def _body():
        real_get_monitor = provider._get_or_create_monitor

        def _broken_monitor():
            monitor = real_get_monitor()
            monitor.force_refresh_with_connection = AsyncMock(  # type: ignore[method-assign]
                side_effect=RuntimeError("monitor boom"))
            return monitor

        provider._get_or_create_monitor = _broken_monitor  # type: ignore[method-assign]
        topology = await provider.force_refresh(object())
        assert len(topology) == 1

    asyncio.run(_body())


def test_monitor_teardown_is_registered_with_the_cleanup_hook() -> None:
    """release_resources clears the monitor cache, so the hook must be
    registered to survive it."""
    aio_cleanup.clear_shutdown_hooks()
    provider = _provider([("srv-1", True)])

    async def _body():
        before = len(aio_cleanup._registered_shutdown_hooks)
        monitor = provider._get_or_create_monitor()
        assert monitor is not None
        assert len(aio_cleanup._registered_shutdown_hooks) > before
        await monitor.aclose()

    asyncio.run(_body())
    aio_cleanup.clear_shutdown_hooks()


def test_refresh_returns_the_stored_topology_without_building_a_monitor() -> None:
    provider = _provider([("srv-1", True)])

    async def _body():
        provider._store_topology(
            (MagicMock(host="srv-1.example", port=5432),))
        assert _monitor_count() == 0
        result = await provider.refresh(object())
        assert len(result) == 1
        assert _monitor_count() == 0

    asyncio.run(_body())
