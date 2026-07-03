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

"""Task 1-B: async custom endpoint monitor + plugin."""

from __future__ import annotations

import asyncio
from typing import List, Tuple
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aws_advanced_python_wrapper.aio import cleanup as aio_cleanup
from aws_advanced_python_wrapper.aio.custom_endpoint_monitor import (
    AsyncCustomEndpointMonitor, AsyncCustomEndpointPlugin)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Monitor lifecycle -------------------------------------------------


def test_monitor_starts_and_stops_cleanly():
    async def _body() -> None:
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["instance-1", "instance-2"], []),
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="my-cluster",
                custom_endpoint_identifier="my-endpoint",
                refresh_interval_sec=0.5,
            )
            assert monitor.is_running() is False
            monitor.start()
            assert monitor.is_running() is True
            await asyncio.sleep(0.05)
            await monitor.stop()
            assert monitor.is_running() is False
            assert monitor.member_instance_ids == ("instance-1", "instance-2")

    asyncio.run(_body())


def test_monitor_start_is_idempotent():
    async def _body() -> None:
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="c",
                custom_endpoint_identifier="e",
                refresh_interval_sec=0.5,
            )
            monitor.start()
            first_thread = monitor._thread
            monitor.start()
            assert monitor._thread is first_thread
            await monitor.stop()

    asyncio.run(_body())


def test_monitor_survives_boto3_errors():
    async def _body() -> None:
        call_count = [0]

        # The polling thread calls the (static) blocking fetch directly; first
        # call raises, subsequent calls succeed. The monitor must swallow the
        # error and keep polling. patch.object replaces the staticmethod with a
        # MagicMock (not bound), so it's called as (endpoint_id, region).
        def _flaky(endpoint_id: str, region) -> Tuple[List[str], List[str]]:
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient AWS failure")
            return ["i-good"], []

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            side_effect=_flaky,
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="c",
                custom_endpoint_identifier="e",
                refresh_interval_sec=0.02,
            )
            monitor.start()
            # Give it time for two iterations.
            await asyncio.sleep(0.2)
            await monitor.stop()
            # Must have survived the first exception and cached the
            # second result.
            assert monitor.member_instance_ids == ("i-good",)
            assert call_count[0] >= 2

    asyncio.run(_body())


def test_monitor_extracts_static_and_excluded_members_from_describe_response():
    """_fetch_members_blocking aggregates StaticMembers AND ExcludedMembers
    across returned endpoints (sync CustomEndpointInfo parity)."""
    fake_client = MagicMock()
    fake_client.describe_db_cluster_endpoints = MagicMock(
        return_value={
            "DBClusterEndpoints": [
                {"StaticMembers": ["instance-1", "instance-2"],
                 "ExcludedMembers": ["instance-9"]},
                {"StaticMembers": ["instance-3"]},
            ]
        }
    )
    with patch("boto3.client", return_value=fake_client):
        members, excluded = AsyncCustomEndpointMonitor._fetch_members_blocking(
            "e", "us-east-1"
        )
    assert members == ["instance-1", "instance-2", "instance-3"]
    assert excluded == ["instance-9"]
    # Resolved by endpoint id + custom-type filter only -- never by
    # DBClusterIdentifier (the wrapper's CLUSTER_ID is not the real RDS id).
    fake_client.describe_db_cluster_endpoints.assert_called_once_with(
        DBClusterEndpointIdentifier="e",
        Filters=[{"Name": "db-cluster-endpoint-type", "Values": ["custom"]}],
    )


def test_monitor_propagates_excluded_members_to_allowed_and_blocked_hosts():
    """C4: ExcludedMembers become blocked_host_ids on the plugin service
    (sync custom_endpoint_plugin.py:193 parity)."""
    async def _body() -> None:
        svc = MagicMock()
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["i-1", "i-2"], ["i-x"]),
        ):
            monitor = AsyncCustomEndpointMonitor(
                custom_endpoint_identifier="e",
                region="us-east-1",
                refresh_interval_sec=0.02,
                plugin_service=svc,
            )
            monitor.start()
            got = await monitor.wait_for_info(timeout_sec=1.0)
            await monitor.stop()
        assert got is True
        assert monitor.member_instance_ids == ("i-1", "i-2")
        assert monitor.excluded_member_instance_ids == ("i-x",)
        hosts = svc.allowed_and_blocked_hosts
        assert hosts.allowed_host_ids == {"i-1", "i-2"}
        assert hosts.blocked_host_ids == {"i-x"}

    asyncio.run(_body())


def test_monitor_maps_empty_excluded_members_to_none():
    """No ExcludedMembers -> blocked_host_ids is None (not an empty set)."""
    async def _body() -> None:
        svc = MagicMock()
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["i-1"], []),
        ):
            monitor = AsyncCustomEndpointMonitor(
                custom_endpoint_identifier="e",
                region="us-east-1",
                refresh_interval_sec=0.02,
                plugin_service=svc,
            )
            monitor.start()
            got = await monitor.wait_for_info(timeout_sec=1.0)
            await monitor.stop()
        assert got is True
        hosts = svc.allowed_and_blocked_hosts
        assert hosts.allowed_host_ids == {"i-1"}
        assert hosts.blocked_host_ids is None

    asyncio.run(_body())


# ---- Plugin integration ------------------------------------------------


def _svc(props: Properties) -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(props, MagicMock(), HostInfo("h", 5432))


def test_plugin_subscribes_to_connect_and_network_bound_methods():
    """C5: sync parity (custom_endpoint_plugin.py:246+271) -- the plugin
    intercepts CONNECT plus the network-bound execute methods so it can
    re-ensure the monitor before queries run."""
    props = Properties({"host": "h"})
    plugin = AsyncCustomEndpointPlugin(_svc(props), props)
    subs = plugin.subscribed_methods
    assert DbApiMethod.CONNECT.method_name in subs
    assert DbApiMethod.CURSOR_EXECUTE.method_name in subs
    assert DbApiMethod.CURSOR_FETCHONE.method_name in subs
    assert DbApiMethod.CONNECTION_COMMIT.method_name in subs
    assert DbApiMethod.CONNECTION_ROLLBACK.method_name in subs


def test_plugin_threads_refresh_rate_prop_into_monitor_interval():
    """C2: sync parity (custom_endpoint_plugin.py:322) -- the monitor's
    refresh cadence comes from CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS."""
    props = Properties({
        "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
        "custom_endpoint_info_refresh_rate_ms": "1500",
    })
    plugin = AsyncCustomEndpointPlugin(_svc(props), props)
    monitor = plugin._build_monitor(
        HostInfo("ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432),
        props)
    assert monitor is not None
    assert monitor._interval_sec == pytest.approx(1.5)


def test_plugin_monitor_interval_defaults_to_30s_without_prop():
    props = Properties({
        "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
    })
    plugin = AsyncCustomEndpointPlugin(_svc(props), props)
    monitor = plugin._build_monitor(
        HostInfo("ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432),
        props)
    assert monitor is not None
    assert monitor._interval_sec == pytest.approx(30.0)


def test_plugin_does_not_spawn_monitor_for_non_custom_endpoint_host():
    async def _body() -> None:
        props = Properties({
            "host": "mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
            "cluster_id": "my-cluster",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)
        raw_conn = MagicMock()

        async def _connect_func() -> object:
            return raw_conn

        await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo("mydb.cluster-xyz.us-east-1.rds.amazonaws.com", 5432),
            props=props,
            is_initial_connection=True,
            connect_func=_connect_func,
        )
        assert plugin.monitor is None
        assert plugin.member_instance_ids == ()

    asyncio.run(_body())


def test_plugin_spawns_monitor_for_custom_endpoint_host():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "my-cluster",
            "iam_region": "us-east-1",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["instance-a"], []),
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            try:
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(
                        "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
                        5432,
                    ),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
                assert plugin.monitor is not None
                assert plugin.monitor.is_running() is True
                # Give it a tick to fetch.
                await asyncio.sleep(0.05)
                assert plugin.member_instance_ids == ("instance-a",)
            finally:
                # Drain any scheduled shutdown hooks.
                await aio_cleanup.release_resources_async()
                # Monitor should be stopped after release_resources_async.
                assert plugin.monitor is not None
                assert plugin.monitor.is_running() is False

    asyncio.run(_body())


def test_plugin_spawns_monitor_without_cluster_id():
    """C6: sync parity -- monitor creation requires only the endpoint id +
    region (custom_endpoint_plugin.py:291-302). CLUSTER_ID (an internal
    wrapper alias) must NOT gate membership enforcement."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            # No cluster_id set.
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)
        raw_conn = MagicMock()

        async def _connect_func() -> object:
            return raw_conn

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["instance-a"], []),
        ):
            try:
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(
                        "my-endpoint.cluster-custom-abc.us-east-1.rds.amazonaws.com",
                        5432,
                    ),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
                assert plugin.monitor is not None
                assert plugin.monitor.is_running() is True
            finally:
                await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_plugin_registers_stop_hook_with_release_resources_async():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        # Disable wait_for_info so the test doesn't block/raise on the
        # N.3 timeout path -- the point here is the shutdown-hook wiring.
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "c",
            "wait_for_custom_endpoint_info": "false",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            await plugin.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(
                    "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432,
                ),
                props=props,
                is_initial_connection=True,
                connect_func=_connect_func,
            )
            # Shutdown hook registered.
            assert aio_cleanup._registered_shutdown_hooks, (
                "plugin didn't register its monitor.stop with release_resources_async"
            )
            await aio_cleanup.release_resources_async()

    asyncio.run(_body())


# ---- C5: execute re-ensures monitor + waits for info --------------------


def test_execute_passes_through_when_no_custom_endpoint_connection():
    """Sync parity (custom_endpoint_plugin.py:352-353): a connection that
    never went through a custom endpoint executes with zero overhead."""
    async def _body() -> None:
        props = Properties({"host": "plain.example.com"})
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        async def _work() -> str:
            return "rows"

        result = await plugin.execute(object(), "Cursor.execute", _work)
        assert result == "rows"
        assert plugin.monitor is None

    asyncio.run(_body())


def test_execute_restarts_stopped_monitor_and_waits_for_info():
    """Sync parity (custom_endpoint_plugin.py:351-359): execute re-creates
    an absent/stopped monitor for the recorded custom endpoint and waits
    for its info before running the query."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "wait_for_custom_endpoint_info_timeout_ms": "2000",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)
        # Simulate a prior connect through the custom endpoint whose monitor
        # has since been stopped (e.g. released).
        plugin._custom_endpoint_host_info = HostInfo(
            "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432)
        assert plugin.monitor is None

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["i-exec"], []),
        ):
            async def _work() -> str:
                return "rows"

            try:
                result = await plugin.execute(object(), "Cursor.execute", _work)
                assert result == "rows"
                assert plugin.monitor is not None
                assert plugin.monitor.is_running() is True
                # The wait completed -- info is populated before the query ran.
                assert plugin.member_instance_ids == ("i-exec",)
            finally:
                await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_execute_raises_when_info_never_arrives():
    """Sync parity: the execute-path wait times out with AwsWrapperError
    when the monitor cannot produce custom endpoint info."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "wait_for_custom_endpoint_info_timeout_ms": "50",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)
        plugin._custom_endpoint_host_info = HostInfo(
            "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            work_calls = [0]

            async def _work() -> str:
                work_calls[0] += 1
                return "rows"

            try:
                from aws_advanced_python_wrapper.errors import AwsWrapperError
                with pytest.raises(AwsWrapperError):
                    await plugin.execute(object(), "Cursor.execute", _work)
                assert work_calls[0] == 0
            finally:
                await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_connect_records_custom_endpoint_host_for_execute_path():
    """connect() to a custom endpoint records the host so later executes
    can re-ensure the monitor (sync stores _custom_endpoint_host_info the
    same way, custom_endpoint_plugin.py:288)."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "wait_for_custom_endpoint_info": "false",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)
        raw_conn = MagicMock()

        async def _connect_func() -> object:
            return raw_conn

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            try:
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(
                        "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
                        5432,
                    ),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
                assert plugin._custom_endpoint_host_info is not None
                assert plugin._custom_endpoint_host_info.host == \
                    "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com"
            finally:
                await aio_cleanup.release_resources_async()

    asyncio.run(_body())


# ---- Phase J: wait-for-info blocking semantics -------------------------


def test_wait_for_info_returns_true_when_event_is_set():
    async def _body() -> None:
        monitor = AsyncCustomEndpointMonitor(
            cluster_identifier="c",
            custom_endpoint_identifier="e",
            refresh_interval_sec=0.5,
        )
        # Pre-set the event -- wait should return immediately with True.
        monitor._info_ready_event.set()
        result = await monitor.wait_for_info(timeout_sec=0.5)
        assert result is True

    asyncio.run(_body())


def test_wait_for_info_returns_false_on_timeout():
    async def _body() -> None:
        monitor = AsyncCustomEndpointMonitor(
            cluster_identifier="c",
            custom_endpoint_identifier="e",
            refresh_interval_sec=0.5,
        )
        # Event never set -- short timeout should return False.
        result = await monitor.wait_for_info(timeout_sec=0.05)
        assert result is False

    asyncio.run(_body())


def test_monitor_sets_info_ready_event_after_first_non_empty_refresh():
    async def _body() -> None:
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["instance-x"], []),
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="c",
                custom_endpoint_identifier="e",
                refresh_interval_sec=0.02,
            )
            assert monitor._info_ready_event.is_set() is False
            monitor.start()
            # wait_for_info should trip once the background task completes
            # one refresh iteration.
            got = await monitor.wait_for_info(timeout_sec=1.0)
            assert got is True
            assert monitor._info_ready_event.is_set() is True
            await monitor.stop()

    asyncio.run(_body())


def test_monitor_does_not_set_info_ready_on_empty_members():
    """Empty describe response must not trip the event -- callers are
    supposed to block until *useful* info arrives (or time out)."""
    async def _body() -> None:
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            monitor = AsyncCustomEndpointMonitor(
                cluster_identifier="c",
                custom_endpoint_identifier="e",
                refresh_interval_sec=0.02,
            )
            monitor.start()
            await asyncio.sleep(0.1)
            assert monitor._info_ready_event.is_set() is False
            await monitor.stop()

    asyncio.run(_body())


def test_plugin_connect_waits_for_info_and_returns_conn_on_success():
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "c",
            "wait_for_custom_endpoint_info_timeout_ms": "2000",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=(["i-waited"], []),
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            try:
                conn = await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(
                        "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432,
                    ),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
                assert conn is raw_conn
                # After connect returns, monitor must have populated info.
                assert plugin.monitor is not None
                assert plugin.member_instance_ids == ("i-waited",)
                assert plugin.monitor._info_ready_event.is_set() is True
            finally:
                await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_plugin_connect_raises_on_wait_timeout():
    """On wait_for_info timeout, connect raises AwsWrapperError WITHOUT
    connecting -- the monitor wait now happens before connect_func() so the
    allowed-hosts filter is in place before the connection is made (sync
    parity). Since no connection was established, none is aborted."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "c",
            # Aggressive timeout so the test runs fast.
            "wait_for_custom_endpoint_info_timeout_ms": "50",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        # Monitor returns an empty member list -- event never trips.
        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            raw_conn = MagicMock()
            connect_called = [0]

            async def _connect_func() -> object:
                connect_called[0] += 1
                return raw_conn

            driver_dialect = MagicMock()
            driver_dialect.abort_connection = AsyncMock()

            try:
                from aws_advanced_python_wrapper.errors import AwsWrapperError
                with pytest.raises(AwsWrapperError):
                    await plugin.connect(
                        target_driver_func=lambda: None,
                        driver_dialect=driver_dialect,
                        host_info=HostInfo(
                            "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432,
                        ),
                        props=props,
                        is_initial_connection=True,
                        connect_func=_connect_func,
                    )
                # We raise BEFORE connecting, so connect_func is never called
                # and there is no connection to abort.
                assert connect_called[0] == 0
                driver_dialect.abort_connection.assert_not_awaited()
            finally:
                await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_plugin_connect_skips_wait_when_wait_for_info_disabled():
    """With WAIT_FOR_CUSTOM_ENDPOINT_INFO=false, connect returns as soon
    as the monitor is started -- no await on wait_for_info."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "c",
            "wait_for_custom_endpoint_info": "false",
            # Large timeout would wedge the test if the wait ran.
            "wait_for_custom_endpoint_info_timeout_ms": "10000",
        })
        plugin = AsyncCustomEndpointPlugin(_svc(props), props)

        wait_calls: List[float] = []

        original = AsyncCustomEndpointMonitor.wait_for_info

        async def _tracking_wait(self: AsyncCustomEndpointMonitor, timeout_sec: float) -> bool:
            wait_calls.append(timeout_sec)
            return await original(self, timeout_sec)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ), patch.object(
            AsyncCustomEndpointMonitor,
            "wait_for_info",
            _tracking_wait,
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            try:
                conn = await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(
                        "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com", 5432,
                    ),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
                assert conn is raw_conn
                # wait_for_info must not have been invoked.
                assert wait_calls == []
                assert plugin.monitor is not None
                assert plugin.monitor.is_running() is True
            finally:
                await aio_cleanup.release_resources_async()

    asyncio.run(_body())


def test_factory_registers_active_plugin_post_task_1b():
    """Task 1-B replaces the SP-8 stub -- factory should build the active one."""
    from aws_advanced_python_wrapper.aio.plugin_factory import \
        build_async_plugins

    props = Properties({
        "host": "h", "port": "5432",
        "plugins": "custom_endpoint",
    })
    plugins = build_async_plugins(_svc(props), props)
    assert len(plugins) == 1
    # The active class -- has `connect` that actually does work.
    assert isinstance(plugins[0], AsyncCustomEndpointPlugin)
    # C5: CONNECT plus the network-bound execute methods.
    assert DbApiMethod.CONNECT.method_name in plugins[0].subscribed_methods
    assert DbApiMethod.CURSOR_EXECUTE.method_name in plugins[0].subscribed_methods


# ---- Telemetry counters ------------------------------------------------


def test_plugin_emits_wait_for_info_counter_when_actually_waiting():
    """custom_endpoint.wait_for_info.count increments when the plugin
    actually awaits wait_for_info. Disabling the wait (via
    wait_for_custom_endpoint_info=false) must leave the counter untouched."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "c",
            "wait_for_custom_endpoint_info_timeout_ms": "50",
        })

        fake_counters: dict = {}

        def _create_counter(name):
            c = MagicMock(name=f"counter:{name}")
            fake_counters[name] = c
            return c

        fake_tf = MagicMock()
        fake_tf.create_counter = MagicMock(side_effect=_create_counter)

        svc = AsyncPluginServiceImpl(props, MagicMock(), HostInfo("h", 5432))
        svc.set_telemetry_factory(fake_tf)
        plugin = AsyncCustomEndpointPlugin(svc, props)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            try:
                # Expect a raise on timeout now (N.3 realignment), but
                # the counter still increments because the plugin
                # entered the wait path before timing out.
                driver_dialect = MagicMock()
                driver_dialect.abort_connection = AsyncMock()
                from aws_advanced_python_wrapper.errors import AwsWrapperError
                with pytest.raises(AwsWrapperError):
                    await plugin.connect(
                        target_driver_func=lambda: None,
                        driver_dialect=driver_dialect,
                        host_info=HostInfo(
                            "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
                            5432,
                        ),
                        props=props,
                        is_initial_connection=True,
                        connect_func=_connect_func,
                    )
            finally:
                await aio_cleanup.release_resources_async()

        assert fake_counters["custom_endpoint.wait_for_info.count"].inc.called

    asyncio.run(_body())


def test_plugin_skips_wait_for_info_counter_when_wait_disabled():
    """Disabling the wait (wait_for_custom_endpoint_info=false) must leave
    the counter untouched -- no inc when the await path is skipped."""
    async def _body() -> None:
        aio_cleanup.clear_shutdown_hooks()
        props = Properties({
            "host": "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
            "cluster_id": "c",
            "wait_for_custom_endpoint_info": "false",
        })

        fake_counters: dict = {}

        def _create_counter(name):
            c = MagicMock(name=f"counter:{name}")
            fake_counters[name] = c
            return c

        fake_tf = MagicMock()
        fake_tf.create_counter = MagicMock(side_effect=_create_counter)

        svc = AsyncPluginServiceImpl(props, MagicMock(), HostInfo("h", 5432))
        svc.set_telemetry_factory(fake_tf)
        plugin = AsyncCustomEndpointPlugin(svc, props)

        with patch.object(
            AsyncCustomEndpointMonitor,
            "_fetch_members_blocking",
            return_value=([], []),
        ):
            raw_conn = MagicMock()

            async def _connect_func() -> object:
                return raw_conn

            try:
                await plugin.connect(
                    target_driver_func=lambda: None,
                    driver_dialect=MagicMock(),
                    host_info=HostInfo(
                        "ep.cluster-custom-abc.us-east-1.rds.amazonaws.com",
                        5432,
                    ),
                    props=props,
                    is_initial_connection=True,
                    connect_func=_connect_func,
                )
            finally:
                await aio_cleanup.release_resources_async()

        assert fake_counters["custom_endpoint.wait_for_info.count"].inc.called is False

    asyncio.run(_body())
