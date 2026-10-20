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

"""Lifecycle regressions in the shared async topology monitor.

Each test pins a defect in the monitor's start, stop, reset or cleanup paths.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from unittest.mock import AsyncMock, MagicMock

from aws_advanced_python_wrapper.aio import topology_store
from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
    AsyncClusterTopologyMonitor
from aws_advanced_python_wrapper.aio.host_list_provider import \
    AsyncAuroraHostListProvider
from aws_advanced_python_wrapper.aio.topology_utils import \
    AsyncAuroraTopologyUtils
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.events import MonitorResetEvent
from aws_advanced_python_wrapper.utils.properties import Properties

CLUSTER_ID = "my-cluster"
KEY = (CLUSTER_ID, "token")
OLD = (HostInfo("blue-writer.example", 5432, HostRole.WRITER),)
NEW = (HostInfo("green-writer.example", 5432, HostRole.WRITER),)


def _utils(topology=()) -> AsyncAuroraTopologyUtils:
    utils = AsyncAuroraTopologyUtils(
        Properties({"host": "cluster-xyz.us-east-1.rds.amazonaws.com", "port": "5432"}),
        "SELECT 1", 5432)
    utils.query_for_topology = AsyncMock(return_value=topology)  # type: ignore[method-assign]
    return utils


def _monitor(topology=()) -> AsyncClusterTopologyMonitor:
    return AsyncClusterTopologyMonitor(
        topology_utils=_utils(topology),
        cluster_id=CLUSTER_ID,
        key=KEY,
        connection_getter=lambda: None)


def test_stop_on_a_closed_loop_does_not_raise() -> None:
    """Setting the stop event before checking the loop raised "Event loop is
    closed", and the monitor service swallowed it at debug level."""
    loop = asyncio.new_event_loop()
    monitor = _monitor()
    try:
        loop.run_until_complete(_start_and_park(monitor))
    finally:
        loop.close()

    # A waiter is parked on the stop event, and the loop is gone.
    assert monitor._stop_event._waiters  # type: ignore[attr-defined]
    monitor.stop()  # must not raise


async def _start_and_park(monitor: AsyncClusterTopologyMonitor) -> None:
    monitor.start()
    await asyncio.sleep(0.02)


def test_the_monitor_does_not_keep_its_event_loop_alive() -> None:
    """A shutdown hook holds the monitor for the life of the process, so a
    strong loop reference leaks a loop per loop in a loop-per-request app."""
    loop = asyncio.new_event_loop()
    monitor = _monitor()
    loop.run_until_complete(_start_and_park(monitor))
    loop.run_until_complete(monitor.aclose())
    loop.close()

    ref = weakref.ref(loop)
    del loop
    gc.collect()
    assert ref() is None, "the monitor is still holding its event loop"


def test_a_reset_discards_a_publication_that_began_before_it() -> None:
    """A refresh in flight when a blue/green reset lands describes the
    pre-switchover cluster, so publishing it afterwards restores the host list
    the reset just cleared."""
    async def _body():
        monitor = _monitor()
        monitor.start()
        topology_store.put_topology(KEY, OLD)

        reset_count = monitor._reset_count
        monitor.process_event(
            MonitorResetEvent(cluster_id=CLUSTER_ID, endpoints=frozenset()))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert topology_store.get_topology(KEY) is None

        # The pre-reset fetch now completes and tries to publish.
        monitor._publish_topology(OLD, reset_count)
        assert topology_store.get_topology(KEY) is None
        assert monitor._last_topology == ()

        # A publication from after the reset still lands.
        monitor._publish_topology(NEW, monitor._reset_count)
        assert topology_store.get_topology(KEY) == NEW
        await monitor.aclose()

    asyncio.run(_body())


def test_a_reset_wakes_the_loop_instead_of_waiting_out_a_tick() -> None:
    """Without waking the loop, a switchover serves nothing until the next
    tick, which the reset itself just made a full-length one."""
    async def _body():
        monitor = _monitor()
        monitor.start()
        monitor._request_tick.clear()

        monitor.process_event(
            MonitorResetEvent(cluster_id=CLUSTER_ID, endpoints=frozenset()))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert monitor._request_tick.is_set()
        await monitor.aclose()

    asyncio.run(_body())


def test_a_closed_monitor_is_not_restarted() -> None:
    """force_monitoring_refresh auto-starts, and a monitor restarted after
    aclose() is out of the cache, so nothing would stop its task."""
    async def _body():
        monitor = _monitor()
        monitor.start()
        await monitor.aclose()

        monitor.start()
        assert not monitor.is_running()

    asyncio.run(_body())


def test_the_inactive_timeout_outlasts_a_long_refresh_interval() -> None:
    """Activity is stamped once per tick, so a refresh interval longer than
    the inactive timeout had every cleanup sweep disposing a healthy monitor."""
    services_container.get_monitor_service()._monitor_caches.pop(
        AsyncClusterTopologyMonitor, None)
    props = Properties({
        "host": "mydb.cluster-abc123.us-east-2.rds.amazonaws.com",
        "port": "5432",
        "topology_refresh_ms": "300000",   # 5 minutes, well past the 3-min default
    })
    AsyncAuroraHostListProvider(props, MagicMock())

    container = services_container.get_monitor_service()._monitor_caches[
        AsyncClusterTopologyMonitor]
    assert container.settings.inactive_timeout_ns > 300_000 * 1_000_000


def test_detach_value_reports_whether_it_removed_the_entry() -> None:
    """stop_and_remove_value relies on this to tell its caller whether the
    removal was theirs."""
    from aws_advanced_python_wrapper.utils.storage.expiration_tracking_cache import \
        ExpirationTrackingCache

    cache: ExpirationTrackingCache = ExpirationTrackingCache(
        expiration_timeout_ns=60 * 1_000_000_000)
    monitor = _monitor()
    cache.compute_if_absent(KEY, lambda _k: monitor)

    assert cache.detach_value(monitor) is True
    assert cache.detach_value(monitor) is False


def test_a_cluster_on_a_non_default_port_keeps_that_port() -> None:
    """The port fell back to the dialect default, so an Aurora cluster reached
    on 5433 produced topology hosts on 5432."""
    utils = AsyncAuroraTopologyUtils(
        Properties({"host": "mydb.cluster-abc123.us-east-2.rds.amazonaws.com",
                    "port": "5433"}),
        "SELECT 1", 5432)
    host = utils.create_host("instance-1", is_writer=True)
    assert host.port == 5433


def test_global_aurora_reduces_two_writers_to_one() -> None:
    """A cross-region switchover can report two writers, and consumers read a
    two-writer topology as ambiguous."""
    from aws_advanced_python_wrapper.aio.topology_utils import \
        AsyncGlobalAuroraTopologyUtils

    utils = AsyncGlobalAuroraTopologyUtils(
        Properties({"host": "mydb.cluster-abc123.us-east-2.rds.amazonaws.com",
                    "port": "5432"}),
        "SELECT 1", 5432,
        instance_templates_by_region={"us-east-1": "?.a.us-east-1.rds.amazonaws.com",
                                      "us-west-2": "?.b.us-west-2.rds.amazonaws.com"})
    rows = [
        ("old-writer", True, 0, "us-east-1"),
        ("new-writer", True, 0, "us-west-2"),
    ]

    topology = utils._process_global_query_results(rows)

    writers = [h for h in topology if h.role == HostRole.WRITER]
    assert len(writers) == 1, f"expected one writer, got {writers}"


def test_aclose_wakes_a_caller_blocked_on_a_refresh() -> None:
    """A blocked caller awaits the event object it read, so aclose() has to
    set it before swapping in a fresh one or the caller waits out its own
    timeout against an orphan."""
    async def _body():
        monitor = _monitor()
        monitor.start()
        waiter = asyncio.create_task(
            monitor.force_monitoring_refresh(False, timeout_sec=30.0))
        await asyncio.sleep(0.01)
        assert not waiter.done()

        await monitor.aclose()

        # Returns promptly rather than after the 30 second timeout.
        result = await asyncio.wait_for(waiter, timeout=1.0)
        assert result == ()

    asyncio.run(_body())


def test_stop_releases_the_monitoring_connection() -> None:
    """The monitor service disposes a monitor by calling stop(), so a teardown
    it does not schedule leaks a connection on every sweep."""
    conn = MagicMock()
    conn.close = MagicMock()

    async def _body():
        monitor = _monitor()
        monitor.start()
        monitor._owned_conn = conn

        monitor.stop()
        for _ in range(10):
            await asyncio.sleep(0)

        conn.close.assert_called_once()
        assert not monitor.is_running()
        # Only aclose() sets _closed, so a task that merely exited on the stop
        # signal and closed the connection in its own finally does not pass.
        assert monitor._closed, "stop() did not schedule aclose()"

    asyncio.run(_body())


def test_the_provider_removes_a_dead_monitor_by_identity() -> None:
    """Removing by key would dispose a live replacement a cleanup pass had
    already put there, under the caller using it."""
    services_container.get_monitor_service()._monitor_caches.pop(
        AsyncClusterTopologyMonitor, None)
    provider = AsyncAuroraHostListProvider(
        Properties({"host": "mydb.cluster-abc123.us-east-2.rds.amazonaws.com",
                    "port": "5432"}),
        MagicMock())

    async def _body():
        monitor_service = services_container.get_monitor_service()
        key = provider._storage_key()

        # A dead monitor is handed out, and a live replacement is already at
        # the key by the time the provider reacts.
        dead = _monitor()
        live = _monitor()
        live.start()
        monitor_service.run_if_absent(AsyncClusterTopologyMonitor, key, lambda: live)

        monitor_service.stop_and_remove_value(AsyncClusterTopologyMonitor, dead)

        assert monitor_service.get(AsyncClusterTopologyMonitor, key) is live
        assert live.is_running(), "the live replacement was disposed"
        await live.aclose()

    asyncio.run(_body())
