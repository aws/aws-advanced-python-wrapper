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

"""F3-B Phase D: Aurora connection tracker + invalidate-on-failover."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.aurora_connection_tracker import (
    AsyncAuroraConnectionTrackerPlugin, AsyncOpenedConnectionTracker)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import FailoverSuccessError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _reset_refresh_window():
    """The post-failover settling window is ClassVar state (sync parity);
    reset it around every test so a FailoverError in one test doesn't leak
    topology refreshes into the next."""
    AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns = 0
    yield
    AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns = 0


def _build():
    props = Properties({"host": "cluster.example.com", "port": "5432"})
    driver_dialect = MagicMock()
    svc = AsyncPluginServiceImpl(props, driver_dialect)
    tracker = AsyncOpenedConnectionTracker()
    plugin = AsyncAuroraConnectionTrackerPlugin(svc, tracker=tracker)
    return plugin, svc, driver_dialect, tracker


def _plain_conn(name, close_side_effect=None):
    """A driver-style async connection mock: has ``close()``, no ``invalidate()``.

    Real psycopg / aiomysql async connections expose ``close()`` but not the
    SQLAlchemy pool-fairy ``invalidate()``. A bare ``MagicMock`` auto-creates
    an ``invalidate`` attribute, which sends ``AsyncOpenedConnectionTracker``
    down its pool-fairy ``invalidate()`` branch (added in c0ca385) instead of
    ``close()``. Deleting ``invalidate`` models a plain driver connection so
    these tests exercise the intended close path.
    """
    c = MagicMock(name=name)
    del c.invalidate
    c.close = MagicMock(side_effect=close_side_effect)
    return c


def test_tracker_records_connection_on_connect():
    plugin, svc, driver_dialect, tracker = _build()
    host = HostInfo(host="instance-1", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="new_conn")

    async def _connect_func():
        return conn

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func)

    asyncio.run(_run())
    tracked = tracker._tracked_for(host)
    assert conn in tracked


def test_connect_enriches_cluster_endpoint_with_resolved_instance_alias():
    """A cluster-endpoint connection must be tracked under the resolved
    *instance* alias so writer-failover ``invalidate_all(instance)`` closes
    it -- regression for ``test_writer_failover_in_idle_connections`` where
    idle cluster-connected sockets survived the writer demotion because they
    were keyed only under the cluster endpoint.
    """
    plugin, svc, driver_dialect, tracker = _build()
    cluster_host = HostInfo(
        host="idledb.cluster-xy9.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.WRITER,
    )
    instance_host = HostInfo(
        host="idle-inst-1.xy9.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.WRITER,
    )
    svc.identify_connection = AsyncMock(return_value=instance_host)
    conn = _plain_conn("idle_conn")

    async def _connect_func():
        return conn

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=cluster_host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func)
        # Failover detects the change against the *instance* topology row;
        # invalidate_all(instance) must now find the cluster-connected conn.
        await tracker.invalidate_all(
            HostInfo(host="idle-inst-1.xy9.us-east-1.rds.amazonaws.com", port=5432))

    asyncio.run(_run())
    svc.identify_connection.assert_awaited_once()
    conn.close.assert_called()


def test_connect_falls_back_to_cluster_key_when_identify_returns_none():
    """If instance resolution yields nothing, the connect still succeeds and
    the conn stays tracked under the cluster key (prior behavior, no regression)."""
    plugin, svc, driver_dialect, tracker = _build()
    cluster_host = HostInfo(
        host="fbdb.cluster-zz1.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.WRITER,
    )
    svc.identify_connection = AsyncMock(return_value=None)
    conn = _plain_conn("fb_conn")

    async def _connect_func():
        return conn

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=cluster_host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func)

    asyncio.run(_run())
    svc.identify_connection.assert_awaited_once()
    assert conn in tracker._tracked_for(cluster_host)


def test_connect_skips_identify_for_instance_endpoint():
    """Instance-endpoint connections already key correctly -- no extra
    identify round-trip is paid."""
    plugin, svc, driver_dialect, tracker = _build()
    instance_host = HostInfo(
        host="direct-inst.zz2.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.WRITER,
    )
    svc.identify_connection = AsyncMock(return_value=instance_host)
    conn = MagicMock(name="direct_conn")

    async def _connect_func():
        return conn

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=driver_dialect,
            host_info=instance_host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_connect_func)

    asyncio.run(_run())
    svc.identify_connection.assert_not_awaited()
    assert conn in tracker._tracked_for(instance_host)


def test_connect_pins_writer_so_failover_invalidates_idle_conns():
    """End-to-end Group 2 regression for
    ``test_writer_failover_in_idle_connections``: the async connect flow does
    not populate ``all_hosts``, so unless the tracker pins the writer at
    connect, a post-failover writer reads as a *first observation* and the
    demoted writer's idle connections are never invalidated. Pinning at
    connect makes the failover a detectable transition (w1 -> w2)."""
    plugin, svc, driver_dialect, tracker = _build()
    writer1 = HostInfo(
        host="test-pg-x-1.cvqiy8w244sz.us-east-2.rds.amazonaws.com",
        port=5432, role=HostRole.WRITER)
    writer2 = HostInfo(
        host="test-pg-x-2.cvqiy8w244sz.us-east-2.rds.amazonaws.com",
        port=5432, role=HostRole.WRITER)

    async def _refresh_to_w1(*a, **k):
        svc._all_hosts = (writer1,)
    svc.refresh_host_list = AsyncMock(side_effect=_refresh_to_w1)
    svc.force_refresh_host_list = AsyncMock(side_effect=_refresh_to_w1)

    active_conn = _plain_conn("active")
    idle_conn = _plain_conn("idle")

    async def _connect_func():
        return active_conn

    async def _run():
        # Connect pins _current_writer to writer1 via the connect-time refresh.
        await plugin.connect(
            target_driver_func=MagicMock(), driver_dialect=driver_dialect,
            host_info=writer1, props=svc.props,
            is_initial_connection=True, connect_func=_connect_func)
        assert plugin._current_writer is not None
        assert plugin._current_writer.host == writer1.host

        # An idle connection to the same (old) writer.
        tracker.track(writer1, idle_conn)

        # Failover: execute raises FailoverError; refresh now reports writer2.
        async def _refresh_to_w2(*a, **k):
            svc._all_hosts = (writer2,)
        svc.refresh_host_list = AsyncMock(side_effect=_refresh_to_w2)
        svc.force_refresh_host_list = AsyncMock(side_effect=_refresh_to_w2)

        async def _raising():
            raise FailoverSuccessError("failover")
        with pytest.raises(FailoverSuccessError):
            await plugin.execute(MagicMock(), "Cursor.execute", _raising)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    idle_conn.close.assert_called()


def test_connect_does_not_pin_writer_for_non_rds_host():
    """Non-RDS connections pay no connect-time topology round-trip."""
    plugin, svc, driver_dialect, tracker = _build()
    svc.refresh_host_list = AsyncMock()
    svc.force_refresh_host_list = AsyncMock()
    conn = MagicMock(name="conn")

    async def _connect_func():
        return conn

    async def _run():
        await plugin.connect(
            target_driver_func=MagicMock(), driver_dialect=driver_dialect,
            host_info=HostInfo(host="plain.example.com", port=5432),
            props=svc.props, is_initial_connection=True,
            connect_func=_connect_func)

    asyncio.run(_run())
    svc.refresh_host_list.assert_not_awaited()
    assert plugin._current_writer is None


def test_tracker_returns_empty_set_for_unknown_host():
    tracker = AsyncOpenedConnectionTracker()
    h = HostInfo(host="ghost", port=5432)
    assert len(tracker._tracked_for(h)) == 0


def test_invalidate_all_closes_tracked_connections():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    c1 = _plain_conn("c1")
    c2 = _plain_conn("c2")
    tracker.track(host, c1)
    tracker.track(host, c2)

    asyncio.run(tracker.invalidate_all(host))

    c1.close.assert_called()
    c2.close.assert_called()


def test_invalidate_all_survives_close_errors():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    bad = _plain_conn("bad", RuntimeError("broken"))
    good = _plain_conn("good")
    tracker.track(host, bad)
    tracker.track(host, good)

    asyncio.run(tracker.invalidate_all(host))
    good.close.assert_called()


def test_plugin_invalidates_old_writer_on_writer_change():
    """Execute with a changed writer in all_hosts triggers invalidation of old writer."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    conn_to_old = _plain_conn("conn_to_old")

    svc._all_hosts = (old_writer,)
    tracker.track(old_writer, conn_to_old)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        svc._all_hosts = (new_writer,)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn_to_old.close.assert_called()


def test_plugin_no_invalidation_when_writer_unchanged():
    plugin, svc, driver_dialect, tracker = _build()
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="conn")
    conn.close = MagicMock()

    svc._all_hosts = (writer,)
    tracker.track(writer, conn)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn.close.assert_not_called()


def test_plugin_invalidates_on_failover_error():
    """A FailoverError raised from the inner execute triggers writer-change recheck."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    conn_to_old = _plain_conn("conn_to_old")

    svc._all_hosts = (old_writer,)
    tracker.track(old_writer, conn_to_old)

    async def _refresh(*args, **kwargs):
        svc._all_hosts = (new_writer,)

    svc.refresh_host_list = AsyncMock(side_effect=_refresh)
    svc.force_refresh_host_list = AsyncMock(side_effect=_refresh)

    async def _run():
        async def _noop():
            return None

        # First execute pins old writer
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)

        # Second execute raises FailoverSuccessError
        async def _raising():
            raise FailoverSuccessError("failover")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(MagicMock(), "Cursor.execute", _raising)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn_to_old.close.assert_called()


def test_failover_invalidation_completes_before_exception_propagates():
    """Integration regression (Aurora multi-5, test_writer_failover_in_idle
    _connections_async 2/10 flake): writer-change invalidation used to be a
    fire-and-forget asyncio task, which dies un-awaited if the event loop
    closes before it runs -- idle connections to the demoted writer stayed
    open. It must complete INLINE, before the FailoverError reaches the
    caller (sync parity in guarantee: sync's daemon invalidation Thread
    always outlives the failover call)."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    idle_conns = [_plain_conn(f"idle-{i}") for i in range(3)]

    svc._all_hosts = (old_writer,)
    for c in idle_conns:
        tracker.track(old_writer, c)

    async def _refresh(*args, **kwargs):
        svc._all_hosts = (new_writer,)

    svc.refresh_host_list = AsyncMock(side_effect=_refresh)
    svc.force_refresh_host_list = AsyncMock(side_effect=_refresh)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)

        async def _raising():
            raise FailoverSuccessError("failover")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(MagicMock(), "Cursor.execute", _raising)
        # NO cooperative yield (no sleep/await) after the exception: every
        # tracked connection must ALREADY be closed, and nothing may be left
        # riding on a pending task that a closing loop would cancel.
        for c in idle_conns:
            c.close.assert_called()
        assert not plugin._pending_invalidations

    asyncio.run(_run())


def test_plugin_subscribed_methods_includes_connect_and_execute():
    plugin, *_ = _build()
    subs = plugin.subscribed_methods
    # DbApiMethod.CONNECT.method_name is literally "connect" (pipeline
    # marker), not "Connect.connect".
    assert "connect" in subs
    assert "Cursor.execute" in subs


def test_tracker_remove_drops_connection_from_set():
    tracker = AsyncOpenedConnectionTracker()
    host = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    conn = MagicMock(name="conn")
    tracker.track(host, conn)
    assert conn in tracker._tracked_for(host)
    tracker.remove(host, conn)
    assert conn not in tracker._tracked_for(host)


def test_tracker_state_is_shared_across_instances():
    """Class-level _tracked means two plugins/trackers see the same connections."""
    tracker_a = AsyncOpenedConnectionTracker()
    tracker_b = AsyncOpenedConnectionTracker()
    host = HostInfo(host="shared-w", port=5432, role=HostRole.WRITER)
    conn = _plain_conn("shared_conn")

    tracker_a.track(host, conn)
    # tracker_b should see it too
    assert conn in tracker_b._tracked_for(host)

    # And invalidating via tracker_b closes it
    asyncio.run(tracker_b.invalidate_all(host))
    conn.close.assert_called()


def test_notify_converted_to_reader_invalidates_that_host():
    plugin, svc, driver_dialect, tracker = _build()
    host = HostInfo(host="demoted-w", port=5432, role=HostRole.WRITER)
    conn = _plain_conn("conn")
    tracker.track(host, conn)

    from aws_advanced_python_wrapper.utils.notifications import HostEvent
    alias = host.as_alias()

    async def _run():
        plugin.notify_host_list_changed({alias: {HostEvent.CONVERTED_TO_READER}})
        # Give the spawned invalidation task a moment
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn.close.assert_called()


def test_notify_converted_to_writer_resets_current_writer():
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    svc._all_hosts = (old_writer,)

    async def _first_execute():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)

    asyncio.run(_first_execute())
    # Plugin has pinned old_writer as its _current_writer
    assert plugin._current_writer is not None
    assert plugin._current_writer.host == "old-w"

    # Notify CONVERTED_TO_WRITER for a new host -- plugin should reset _current_writer
    from aws_advanced_python_wrapper.utils.notifications import HostEvent
    plugin.notify_host_list_changed(
        {"new-w:5432": {HostEvent.CONVERTED_TO_WRITER}})
    assert plugin._current_writer is None


def test_notify_ignores_events_that_are_not_converted():
    """WENT_DOWN, HOST_ADDED etc. don't trigger invalidation via notify."""
    plugin, svc, driver_dialect, tracker = _build()
    host = HostInfo(host="some-host", port=5432, role=HostRole.READER)
    conn = MagicMock(name="conn")
    conn.close = MagicMock()
    tracker.track(host, conn)

    from aws_advanced_python_wrapper.utils.notifications import HostEvent

    async def _run():
        plugin.notify_host_list_changed(
            {host.as_alias(): {HostEvent.WENT_DOWN}})
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    # WENT_DOWN is not a CONVERTED_* event -> no invalidation
    conn.close.assert_not_called()


def test_release_resources_async_drains_pending_invalidations():
    from aws_advanced_python_wrapper.aio.cleanup import release_resources_async

    plugin, svc, driver_dialect, tracker = _build()
    old = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)

    # Slow close to ensure the invalidation task is still running when
    # release_resources_async is called.
    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    async def _slow_close():
        close_started.set()
        await asyncio.sleep(0.05)
        close_finished.set()

    conn = _plain_conn("slow_conn", lambda: _slow_close())
    tracker.track(old, conn)

    svc._all_hosts = (old,)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        svc._all_hosts = (new,)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Wait until close has actually started, then release
        await close_started.wait()
        await release_resources_async()

    asyncio.run(_run())

    # close_finished should have been set -- the invalidation task ran to completion
    assert close_finished.is_set()


def test_notify_host_list_changed_handles_ipv6_alias():
    """CONVERTED_TO_READER for an IPv6 alias routes to the correct host."""
    plugin, svc, driver_dialect, tracker = _build()
    # Track a conn under an IPv6-shaped host
    ipv6_host = HostInfo(host="[::1]", port=5432, role=HostRole.WRITER)
    conn = _plain_conn("ipv6_conn")
    tracker.track(ipv6_host, conn)

    from aws_advanced_python_wrapper.utils.notifications import HostEvent

    async def _run():
        plugin.notify_host_list_changed({"[::1]:5432": {HostEvent.CONVERTED_TO_READER}})
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn.close.assert_called()


def test_notify_host_list_changed_handles_alias_without_port():
    """An alias with no colon falls back to port 5432 (doesn't raise)."""
    plugin, svc, driver_dialect, tracker = _build()

    from aws_advanced_python_wrapper.utils.notifications import HostEvent

    async def _run():
        # Must not raise; _spawn_invalidation schedules via asyncio.create_task
        # so needs a running loop.
        plugin.notify_host_list_changed({"host-no-port": {HostEvent.CONVERTED_TO_READER}})
        await asyncio.sleep(0.01)

    asyncio.run(_run())


def test_parse_alias_helper():
    """Direct helper test: IPv4, IPv6, bare host, invalid port."""
    from aws_advanced_python_wrapper.aio.aurora_connection_tracker import \
        AsyncAuroraConnectionTrackerPlugin

    assert AsyncAuroraConnectionTrackerPlugin._parse_alias("h.example:5432") == ("h.example", 5432)
    assert AsyncAuroraConnectionTrackerPlugin._parse_alias("[::1]:5432") == ("[::1]", 5432)
    assert AsyncAuroraConnectionTrackerPlugin._parse_alias("h-no-port") == ("h-no-port", 5432)
    assert AsyncAuroraConnectionTrackerPlugin._parse_alias("h:not-a-port") == ("h", 5432)


def test_tracker_keys_by_instance_endpoint_for_cluster_connected_conn():
    """When tracking via a cluster endpoint HostInfo whose aliases
    include the instance endpoint, the canonical key is the instance
    endpoint -- so notify-based invalidation by instance alias finds it."""
    tracker = AsyncOpenedConnectionTracker()
    # Cluster endpoint hostname that is NOT an RDS instance per is_rds_instance,
    # but aliases include the instance endpoint.
    cluster_host = HostInfo(
        host="mydb.cluster-abc123.us-east-1.rds.amazonaws.com",
        port=5432,
        role=HostRole.WRITER,
    )
    # Add a known RDS instance alias
    cluster_host.add_alias("myinstance.abc123.us-east-1.rds.amazonaws.com:5432")

    conn = _plain_conn("conn")
    tracker.track(cluster_host, conn)

    # Canonical key is the instance alias, not the cluster host:port
    assert AsyncOpenedConnectionTracker._canonical_key(cluster_host) == \
        "myinstance.abc123.us-east-1.rds.amazonaws.com:5432"

    # invalidate_all via a HostInfo that resolves to the same instance alias
    # should close the tracked conn
    instance_host = HostInfo(
        host="myinstance.abc123.us-east-1.rds.amazonaws.com",
        port=5432,
    )
    asyncio.run(tracker.invalidate_all(instance_host))
    conn.close.assert_called()


def test_tracker_fallback_to_host_port_for_non_rds_host():
    """Non-RDS hostnames use host:port as the canonical key (no RDS alias found)."""
    host = HostInfo(host="custom.example.com", port=5432)
    assert AsyncOpenedConnectionTracker._canonical_key(host) == "custom.example.com:5432"


# ---- B2: post-failover settling window ----------------------------------


def test_failover_error_opens_settling_window_and_keeps_refreshing():
    """After a FailoverError, subsequent executes keep refreshing topology
    until the 3-minute window elapses (sync
    aurora_connection_tracker_plugin.py:313-342) -- not just the single
    refresh inside the FailoverError handler."""
    plugin, svc, driver_dialect, tracker = _build()
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    svc._all_hosts = (writer,)
    svc.refresh_host_list = AsyncMock()
    svc.force_refresh_host_list = AsyncMock()

    async def _run():
        async def _raising():
            raise FailoverSuccessError("failover")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(MagicMock(), "Cursor.execute", _raising)

        # Window opened + the handler's immediate refresh is a FORCE refresh
        # through the post-failover connection (a plain refresh can serve the
        # monitor's pre-failover cache and miss the writer change entirely).
        assert AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns > 0
        assert svc.force_refresh_host_list.await_count == 1
        assert svc.refresh_host_list.await_count == 0

        # Every execute inside the window refreshes again (plain refresh --
        # sync parity for the settling window).
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        assert svc.refresh_host_list.await_count == 2
        # Window still open (3 min is far longer than this test).
        assert AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns > 0

    asyncio.run(_run())


def test_settling_window_expires_and_stops_refreshing():
    """Once the window has elapsed, the next execute resets it to 0 and
    stops refreshing (sync aurora_connection_tracker_plugin.py:321-324)."""
    from time import perf_counter_ns

    plugin, svc, driver_dialect, tracker = _build()
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    svc._all_hosts = (writer,)
    svc.refresh_host_list = AsyncMock()
    svc.force_refresh_host_list = AsyncMock()

    # Simulate an already-elapsed window.
    AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns = \
        perf_counter_ns() - 1

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)

    asyncio.run(_run())
    svc.refresh_host_list.assert_not_awaited()
    assert AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns == 0


def test_settling_window_skips_connection_close():
    """Connection.close never triggers a window refresh (sync gates the
    window logic on non-close methods, aurora_connection_tracker_plugin.py:312)."""
    from time import perf_counter_ns

    plugin, svc, driver_dialect, tracker = _build()
    svc.refresh_host_list = AsyncMock()
    svc.force_refresh_host_list = AsyncMock()

    # Open window.
    AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns = \
        perf_counter_ns() + 60 * 1_000_000_000

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Connection.close", _noop)

    asyncio.run(_run())
    svc.refresh_host_list.assert_not_awaited()
    # Window untouched by the close.
    assert AsyncAuroraConnectionTrackerPlugin._host_list_refresh_end_time_ns > 0


def test_settling_window_refresh_detects_late_writer_change():
    """A writer flip that surfaces only on a later refresh inside the window
    still invalidates the demoted writer's connections."""
    plugin, svc, driver_dialect, tracker = _build()
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    conn_to_old = _plain_conn("conn_to_old")

    svc._all_hosts = (old_writer,)
    tracker.track(old_writer, conn_to_old)

    # First refresh (inside the FailoverError handler) still reports the old
    # writer -- the metadata is lagging. A later in-window refresh reports
    # the new writer.
    refresh_results = [(old_writer,), (new_writer,)]

    async def _refresh(*args, **kwargs):
        svc._all_hosts = refresh_results.pop(0) if refresh_results \
            else svc._all_hosts

    svc.refresh_host_list = AsyncMock(side_effect=_refresh)
    svc.force_refresh_host_list = AsyncMock(side_effect=_refresh)

    async def _run():
        async def _noop():
            return None

        # Pin the old writer.
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)

        async def _raising():
            raise FailoverSuccessError("failover")

        # Failover: handler refreshes -> still old writer -> no invalidation.
        with pytest.raises(FailoverSuccessError):
            await plugin.execute(MagicMock(), "Cursor.execute", _raising)
        await asyncio.sleep(0.01)
        conn_to_old.close.assert_not_called()

        # Next execute inside the window refreshes again -> new writer ->
        # invalidation fires.
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.01)

    asyncio.run(_run())
    conn_to_old.close.assert_called()


# ---- Telemetry counters ------------------------------------------------


def test_writer_change_emits_writer_changes_counter():
    """When _update_writer_from_topology detects a new writer, the
    aurora_connection_tracker.writer_changes.count counter increments.

    The writer change also kicks off a fire-and-forget invalidation task
    (asyncio.create_task), so the test body runs inside an event loop.
    """
    props = Properties({"host": "cluster.example.com", "port": "5432"})
    driver_dialect = MagicMock()
    svc = AsyncPluginServiceImpl(props, driver_dialect)

    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)
    svc.set_telemetry_factory(fake_tf)

    tracker = AsyncOpenedConnectionTracker()
    plugin = AsyncAuroraConnectionTrackerPlugin(svc, tracker=tracker)

    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)

    async def _body() -> None:
        # Seed first writer -- should NOT tick the counter (first observation).
        svc._all_hosts = (old_writer,)
        plugin._update_writer_from_topology()
        assert not fake_counters[
            "aurora_connection_tracker.writer_changes.count"
        ].inc.called

        # Second observation with different writer -> counter inc.
        svc._all_hosts = (new_writer,)
        plugin._update_writer_from_topology()
        assert fake_counters[
            "aurora_connection_tracker.writer_changes.count"
        ].inc.called
        # Drain the invalidation task spawned by _update_writer_from_topology.
        await asyncio.sleep(0)

    asyncio.run(_body())
