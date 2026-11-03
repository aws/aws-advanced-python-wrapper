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

"""Tests for the async topology monitor as a managed monitor."""

from __future__ import annotations

import asyncio
from time import perf_counter_ns
from unittest.mock import AsyncMock, MagicMock

from aws_advanced_python_wrapper.aio import topology_store
from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
    AsyncClusterTopologyMonitor
from aws_advanced_python_wrapper.aio.topology_utils import \
    AsyncAuroraTopologyUtils
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.events import (MonitorResetEvent,
                                                      MonitorStopEvent)
from aws_advanced_python_wrapper.utils.monitor_service import Monitor
from aws_advanced_python_wrapper.utils.properties import Properties

CLUSTER_ID = "my-cluster"
KEY = (CLUSTER_ID, "token")
HOSTS = (HostInfo("writer.example", 5432, HostRole.WRITER),)


def _utils(topology=()) -> AsyncAuroraTopologyUtils:
    utils = AsyncAuroraTopologyUtils(
        Properties({"host": "cluster-xyz.us-east-1.rds.amazonaws.com", "port": "5432"}),
        "SELECT 1", 5432)
    utils.query_for_topology = AsyncMock(return_value=topology)  # type: ignore[method-assign]
    return utils


def _monitor(topology=(), **kwargs) -> AsyncClusterTopologyMonitor:
    return AsyncClusterTopologyMonitor(
        topology_utils=_utils(topology),
        cluster_id=CLUSTER_ID,
        key=KEY,
        connection_getter=lambda: None,
        **kwargs)


def test_the_monitor_satisfies_the_monitor_protocol() -> None:
    """The monitor service type-checks against this protocol at runtime."""
    assert isinstance(_monitor(), Monitor)


def test_stop_does_not_block_on_teardown() -> None:
    """MonitorStopEvent is delivered on the publisher's thread and async
    blue/green publishes from a coroutine, so a blocking stop would wedge
    that loop."""
    async def _body():
        monitor = _monitor()
        monitor.start()
        assert monitor.is_running()
        monitor.stop()  # returns immediately, teardown is scheduled
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not monitor.is_running()

    asyncio.run(_body())


def test_stop_without_a_live_loop_does_not_raise() -> None:
    """A monitor whose loop is gone cannot close its connection. Say so
    rather than report a clean teardown."""
    monitor = _monitor()
    monitor.stop()  # never started, so no loop was ever recorded
    assert not monitor.is_running()


def test_aclose_is_idempotent() -> None:
    async def _body():
        monitor = _monitor()
        monitor.start()
        await monitor.aclose()
        await monitor.aclose()
        assert not monitor.is_running()

    asyncio.run(_body())


def test_aclose_closes_a_monitor_that_never_started() -> None:
    """_run's finally releases the connection when its task is cancelled, but
    a monitor that never started still owns one."""
    conn = MagicMock()
    conn.close = MagicMock()

    async def _body():
        monitor = _monitor()
        monitor._owned_conn = conn
        await monitor.aclose()
        conn.close.assert_called_once()

    asyncio.run(_body())


def test_can_dispose_is_false_while_a_refresh_is_in_flight() -> None:
    """Only the expiry path consults can_dispose; the stuck-monitor path
    removes regardless, so this does not protect a blocked caller."""
    async def _body():
        monitor = _monitor()
        assert monitor.can_dispose is True

        monitor.start()
        waiter = asyncio.create_task(
            monitor.force_monitoring_refresh(False, timeout_sec=5.0))
        await asyncio.sleep(0.01)
        assert monitor.can_dispose is False

        monitor._publish_topology(HOSTS)
        await waiter
        assert monitor.can_dispose is True
        await monitor.aclose()

    asyncio.run(_body())


def test_last_activity_uses_the_same_clock_as_the_cleanup_sweep() -> None:
    """MonitorService._run_cleanup compares this against perf_counter_ns, so a
    different clock would make every monitor look stuck, or never idle."""
    async def _body():
        before = perf_counter_ns()
        monitor = _monitor()
        monitor.start()
        await asyncio.sleep(0.01)
        after = perf_counter_ns()
        assert before <= monitor.last_activity_ns <= after
        await monitor.aclose()

    asyncio.run(_body())


def test_a_reset_event_for_this_cluster_drops_the_stored_topology() -> None:
    """A switchover must not keep serving the pre-switchover host list."""
    async def _body():
        monitor = _monitor()
        monitor.start()
        topology_store.put_topology(KEY, HOSTS)
        monitor._last_topology = HOSTS
        monitor._last_known_writer = "writer.example:5432"

        monitor.process_event(
            MonitorResetEvent(cluster_id=CLUSTER_ID, endpoints=frozenset()))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert topology_store.get_topology(KEY) is None
        assert monitor._last_topology == ()
        assert monitor._last_known_writer is None
        await monitor.aclose()

    asyncio.run(_body())


def test_a_reset_event_for_another_cluster_is_ignored() -> None:
    async def _body():
        monitor = _monitor()
        monitor.start()
        topology_store.put_topology(KEY, HOSTS)

        monitor.process_event(
            MonitorResetEvent(cluster_id="someone-else", endpoints=frozenset()))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert topology_store.get_topology(KEY) == HOSTS
        await monitor.aclose()

    asyncio.run(_body())


def test_a_stop_event_disposes_the_registered_monitor() -> None:
    async def _body():
        monitor = _monitor()
        monitor.start()
        monitor_service = services_container.get_monitor_service()
        monitor_service.register_monitor_type(AsyncClusterTopologyMonitor)
        monitor_service.run_if_absent(
            AsyncClusterTopologyMonitor, KEY, lambda: monitor)
        assert monitor_service.count(AsyncClusterTopologyMonitor) == 1

        services_container.get_event_publisher().publish(
            MonitorStopEvent(monitor_type=AsyncClusterTopologyMonitor, key=KEY))

        assert monitor_service.count(AsyncClusterTopologyMonitor) == 0
        await monitor.aclose()

    asyncio.run(_body())


def test_stop_and_remove_value_leaves_a_replacement_alone() -> None:
    """Removing by key would dispose whatever sits at the key now, which may
    be a replacement another caller is already using."""
    monitor_service = services_container.get_monitor_service()
    monitor_service.register_monitor_type(AsyncClusterTopologyMonitor)

    stale, replacement = _monitor(), _monitor()
    monitor_service.run_if_absent(
        AsyncClusterTopologyMonitor, KEY, lambda: replacement)

    monitor_service.stop_and_remove_value(AsyncClusterTopologyMonitor, stale)

    assert monitor_service.get(AsyncClusterTopologyMonitor, KEY) is replacement
