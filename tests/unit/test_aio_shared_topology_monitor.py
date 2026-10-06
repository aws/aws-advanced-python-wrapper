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

"""Async topology monitors are shared per cluster, like sync's
``monitor_service.run_if_absent(ClusterTopologyMonitorImpl, cluster_id, ...)``.

Every async connect builds its own host list provider, so a per-provider
monitor meant one dedicated monitoring connection per application connection
(issue #1284).
"""

from __future__ import annotations

import asyncio
import gc
from typing import List
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio import cleanup as aio_cleanup
from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncAuroraHostListProvider
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _clear_hooks():
    aio_cleanup.clear_shutdown_hooks()
    yield
    aio_cleanup.clear_shutdown_hooks()


class _ConnFactory:
    """Counts dedicated monitoring connections opened."""

    def __init__(self) -> None:
        self.opened = 0

    async def __call__(self):
        self.opened += 1
        return object()


def _provider(
        factory: _ConnFactory,
        host: str = "cluster.example",
        rows=(("srv-1", True), ("srv-2", False))) -> AsyncAuroraHostListProvider:
    provider = AsyncAuroraHostListProvider(
        Properties({"host": host, "port": "5432"}),
        MagicMock(),
        monitor_connection_factory=factory,
    )

    async def _rows(_conn):  # noqa: ARG001 - signature fixed
        return list(rows)

    provider._run_topology_query = _rows  # type: ignore[method-assign]
    return provider


async def _stop_all(providers: List[AsyncAuroraHostListProvider]) -> None:
    for p in providers:
        await p.stop()


def test_providers_for_same_cluster_share_one_monitor() -> None:
    factory = _ConnFactory()

    async def _body():
        providers = [_provider(factory) for _ in range(10)]
        for p in providers:
            await p.force_refresh(object())
        await asyncio.sleep(0.05)  # let the background task open its connection

        monitors = {id(p._get_or_create_monitor()) for p in providers}
        assert len(monitors) == 1
        assert factory.opened == 1
        await _stop_all(providers)

    asyncio.run(_body())


def test_shared_monitor_publishes_topology_to_every_provider() -> None:
    factory = _ConnFactory()

    async def _body():
        first, second = _provider(factory), _provider(factory)
        await first.force_refresh(object())
        await second.force_refresh(object())
        second._topology_cache = None
        # Simulate a background tick discovering a new topology.
        monitor = first._get_or_create_monitor()
        monitor._publish_topology(await first._fetch_from_db(object()))

        assert second._topology_cache is not None
        assert {h.host for h in second._topology_cache} == \
            {h.host for h in first._topology_cache}
        await _stop_all([first, second])

    asyncio.run(_body())


def test_providers_for_different_clusters_get_separate_monitors() -> None:
    factory = _ConnFactory()

    async def _body():
        a = _provider(factory, host="cluster-a.example")
        b = _provider(factory, host="cluster-b.example")
        await a.force_refresh(object())
        await b.force_refresh(object())

        assert a._get_or_create_monitor() is not b._get_or_create_monitor()
        await _stop_all([a, b])

    asyncio.run(_body())


def test_monitor_outlives_its_connections_and_is_reused() -> None:
    # Sync parity: closing a connection doesn't stop the cluster's monitor, so
    # connections that come and go (NullPool, a pool draining to zero) reuse
    # one monitoring connection instead of opening a new one each time.
    factory = _ConnFactory()

    async def _body():
        first = _provider(factory)
        await first.force_refresh(object())
        await asyncio.sleep(0.05)
        monitor = first._get_or_create_monitor()
        del first
        gc.collect()

        later = _provider(factory)
        await later.force_refresh(object())
        await asyncio.sleep(0.05)
        assert later._get_or_create_monitor() is monitor
        assert monitor.is_running()
        assert factory.opened == 1
        await later.stop()

    asyncio.run(_body())


def test_stop_stops_the_shared_monitor_for_the_cluster() -> None:
    # Sync parity: RdsHostListProvider.stop_monitor stops the cluster's monitor
    # (blue/green switchover); the next provider starts a fresh one.
    factory = _ConnFactory()

    async def _body():
        first, second = _provider(factory), _provider(factory)
        await first.force_refresh(object())
        await second.force_refresh(object())
        monitor = first._get_or_create_monitor()

        await first.stop()
        assert not monitor.is_running()

        later = _provider(factory)
        await later.force_refresh(object())
        assert later._get_or_create_monitor() is not monitor
        await later.stop()

    asyncio.run(_body())
