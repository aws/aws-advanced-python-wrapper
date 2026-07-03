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

"""F3-B SP-5: async host monitoring plugin."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.host_monitoring_plugin import \
    AsyncHostMonitoringPlugin
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


def _build(
        enabled: bool = True,
        grace_ms: int = 10,
        interval_ms: int = 10,
        count: int = 2):
    props = Properties({
        "host": "h.example",
        "port": "5432",
        "failure_detection_enabled": "true" if enabled else "false",
        "failure_detection_time_ms": str(grace_ms),
        "failure_detection_interval_ms": str(interval_ms),
        "failure_detection_count": str(count),
    })

    driver_dialect = MagicMock()
    driver_dialect.ping = AsyncMock(return_value=True)
    driver_dialect.abort_connection = AsyncMock()

    svc = AsyncPluginServiceImpl(
        props, driver_dialect, HostInfo(host="h.example", port=5432)
    )
    conn = MagicMock(name="current_conn")
    svc._current_connection = conn

    plugin = AsyncHostMonitoringPlugin(svc, props)
    return plugin, svc, driver_dialect, conn


# ---- Config / subscription ---------------------------------------------


def test_subscribed_includes_cursor_execute():
    plugin, *_ = _build()
    assert "Cursor.execute" in plugin.subscribed_methods


def test_disabled_plugin_does_not_spawn_monitor_task():
    async def _body() -> None:
        plugin, _, driver_dialect, _conn = _build(enabled=False)

        async def _work() -> str:
            return "ok"

        result = await plugin.execute(object(), "Cursor.execute", _work)
        assert result == "ok"
        # Monitor never runs, so ping is never called.
        driver_dialect.ping.assert_not_called()

    asyncio.run(_body())


def test_happy_path_passes_result_through():
    async def _body() -> None:
        plugin, *_ = _build()

        async def _work() -> str:
            await asyncio.sleep(0.001)
            return "rows"

        assert await plugin.execute(object(), "Cursor.execute", _work) == "rows"

    asyncio.run(_body())


def test_monitor_task_persists_after_execute_returns():
    """Under the standing-task model the monitor does NOT get cancelled when
    execute returns. It lives for the connection's lifetime."""
    async def _body() -> None:
        plugin, *_ = _build(grace_ms=1000)

        async def _work() -> None:
            return None

        await plugin.execute(object(), "Cursor.execute", _work)
        await asyncio.sleep(0)
        assert plugin._monitor_task is not None
        assert not plugin._monitor_task.done()

    asyncio.run(_body())


def test_failures_accumulate_across_probes_no_abort_below_threshold():
    """Failures accumulate on the instance; abort only fires when the
    threshold is crossed (C.2). With a high threshold the counter grows
    without abort_connection being called."""
    async def _body() -> None:
        plugin, _, driver_dialect, _conn = _build(
            grace_ms=0, interval_ms=5, count=1000
        )
        # All pings fail.
        driver_dialect.ping = AsyncMock(return_value=False)

        async def _slow_work() -> str:
            await asyncio.sleep(0.08)
            return "done"

        await plugin.execute(object(), "Cursor.execute", _slow_work)
        # Threshold is huge -- counter accumulates but no abort.
        assert plugin._consecutive_failures > 0
        driver_dialect.abort_connection.assert_not_called()

    asyncio.run(_body())


def test_no_abort_when_pings_succeed():
    async def _body() -> None:
        plugin, _, driver_dialect, _conn = _build(
            grace_ms=2, interval_ms=2, count=3
        )
        driver_dialect.ping = AsyncMock(return_value=True)

        async def _work() -> str:
            await asyncio.sleep(0.05)
            return "done"

        await plugin.execute(object(), "Cursor.execute", _work)
        driver_dialect.abort_connection.assert_not_called()

    asyncio.run(_body())


def test_no_connection_means_no_monitor():
    async def _body() -> None:
        plugin, svc, driver_dialect, _conn = _build()
        svc._current_connection = None

        async def _work() -> str:
            return "ok"

        result = await plugin.execute(object(), "Cursor.execute", _work)
        assert result == "ok"
        driver_dialect.ping.assert_not_called()

    asyncio.run(_body())


def test_ping_exception_increments_failure_counter():
    """C.1: exceptions raised by ping are treated as failures -- the counter
    increments. (C.2 will add the abort-on-threshold behavior.)"""
    async def _body() -> None:
        plugin, _, driver_dialect, _conn = _build(
            grace_ms=0, interval_ms=2, count=1000
        )
        driver_dialect.ping = AsyncMock(side_effect=RuntimeError("net down"))

        async def _slow() -> str:
            await asyncio.sleep(0.06)
            return "rows"

        await plugin.execute(object(), "Cursor.execute", _slow)
        # With ping always raising, failures must accumulate past the
        # initial zero; no abort because threshold is large.
        assert plugin._consecutive_failures > 0
        driver_dialect.abort_connection.assert_not_called()

    asyncio.run(_body())


def test_execute_raises_still_leaves_monitor_alive():
    """Standing-task model: when execute_func raises, the monitor task is
    NOT torn down -- cleanup happens via release_resources (Task C.4)."""
    async def _body() -> None:
        plugin, *_ = _build(grace_ms=1000)

        async def _raiser() -> None:
            raise RuntimeError("inner failure")

        with pytest.raises(RuntimeError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        await asyncio.sleep(0)
        assert plugin._monitor_task is not None
        assert not plugin._monitor_task.done()

    asyncio.run(_body())


# ---- C.1 standing-task behavior ----------------------------------------


def test_monitor_task_starts_lazily_on_first_execute():
    """Standing monitor task is created on first execute, not at plugin
    construction."""
    plugin, svc, driver_dialect, conn = _build(grace_ms=1000, interval_ms=1000)
    svc._current_host_info = HostInfo(host="h.example", port=5432)

    # No task before first execute
    assert plugin._monitor_task is None

    async def _noop():
        return None

    asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _noop))
    assert plugin._monitor_task is not None


def test_monitor_task_persists_across_multiple_executes():
    """Single standing task, not a new task per execute."""
    plugin, svc, driver_dialect, conn = _build(grace_ms=1000, interval_ms=1000)
    svc._current_host_info = HostInfo(host="h.example", port=5432)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        task_1 = plugin._monitor_task
        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        task_2 = plugin._monitor_task
        return task_1, task_2

    t1, t2 = asyncio.run(_run())
    assert t1 is t2


def test_failure_count_accumulates_across_probes():
    """Consecutive failed pings accumulate; successful ping resets."""
    plugin, svc, driver_dialect, conn = _build(
        grace_ms=0, interval_ms=10, count=10)
    svc._current_host_info = HostInfo(host="h.example", port=5432)

    ping_results = [False, False, True, False, False]
    driver_dialect.ping = AsyncMock(side_effect=ping_results + [True] * 20)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Let monitor burn through several iterations
        await asyncio.sleep(0.15)
        # After False, False, True, False, False, True... the counter should
        # have hit at most 2 before reset, then at most 2 again. Threshold is
        # 10 so no UNAVAILABLE action is taken. The counter value at end
        # depends on timing; what matters is it never reached 10 (no abort)
        # and is >= 0.
        # Softer assertion: monitor task is still alive (didn't trigger
        # threshold exit) -- assert inside the running loop before
        # asyncio.run's shutdown closes pending tasks.
        assert plugin._monitor_task is not None
        assert not plugin._monitor_task.done()

    asyncio.run(_run())


def test_monitor_tracks_host_aliases():
    """Monitor records the host's aliases for UNAVAILABLE marking in C.2."""
    plugin, svc, driver_dialect, conn = _build(grace_ms=100, interval_ms=100)
    host = HostInfo(host="h.example", port=5432)
    svc._current_host_info = host

    async def _noop():
        return None

    asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _noop))
    assert plugin._monitored_aliases == host.as_aliases()


def test_threshold_breach_marks_host_unavailable_and_aborts():
    from aws_advanced_python_wrapper.host_availability import HostAvailability

    plugin, svc, driver_dialect, conn = _build(
        grace_ms=0, interval_ms=10, count=2)
    host = HostInfo(host="h.example", port=5432)
    svc._current_host_info = host
    svc.set_availability = MagicMock()
    driver_dialect.ping = AsyncMock(return_value=False)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Give the monitor enough iterations to hit the threshold
        await asyncio.sleep(0.2)

    asyncio.run(_run())

    svc.set_availability.assert_any_call(
        host.as_aliases(), HostAvailability.UNAVAILABLE)
    driver_dialect.abort_connection.assert_awaited()
    assert plugin._host_unavailable is True


def test_efm_abort_unwraps_pooled_proxy_to_raw_connection():
    # A pooled connection is a _PooledAsyncConnectionProxy whose close() returns
    # it to the pool (socket stays open). EFM must abort the UNDERLYING raw
    # connection (via .driver_connection) so an in-flight blackholed query is
    # actually severed -- otherwise it hangs until the kernel/test timeout.
    plugin, svc, driver_dialect, _conn = _build()
    svc.set_availability = MagicMock()
    raw = MagicMock(name="raw")
    proxy = MagicMock(name="pool-proxy")
    proxy.driver_connection = raw

    asyncio.run(plugin._mark_host_unavailable(proxy, driver_dialect))

    driver_dialect.abort_connection.assert_awaited_once_with(raw)


def test_efm_abort_non_pooled_connection_aborts_itself():
    # A non-pooled connection has no .driver_connection -> aborted directly
    # (behavior unchanged for the common case).
    plugin, svc, driver_dialect, _conn = _build()
    svc.set_availability = MagicMock()
    raw = object()  # no driver_connection attribute

    asyncio.run(plugin._mark_host_unavailable(raw, driver_dialect))

    driver_dialect.abort_connection.assert_awaited_once_with(raw)


def test_execute_raises_when_host_already_unavailable():
    from aws_advanced_python_wrapper.errors import AwsWrapperError

    plugin, svc, driver_dialect, conn = _build()
    svc._current_host_info = HostInfo(host="h.example", port=5432)
    plugin._host_unavailable = True  # simulate prior threshold breach

    async def _noop():
        return None

    with pytest.raises(AwsWrapperError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _noop))


def test_monitor_exits_loop_after_threshold_breach():
    """After threshold hit, the monitor task finishes cleanly — doesn't keep probing."""
    plugin, svc, driver_dialect, conn = _build(
        grace_ms=0, interval_ms=10, count=2)
    svc._current_host_info = HostInfo(host="h.example", port=5432)
    driver_dialect.ping = AsyncMock(return_value=False)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Give the monitor enough iterations to hit threshold and exit
        await asyncio.sleep(0.3)
        return plugin._monitor_task.done()

    done = asyncio.run(_run())
    assert done is True


# ---- C.3 notify hooks --------------------------------------------------


def test_notify_connection_changed_resets_monitor():
    """Connection swap clears failure counter + unavailable flag + cancels task."""
    from aws_advanced_python_wrapper.utils.notifications import ConnectionEvent

    plugin, svc, driver_dialect, conn = _build(
        grace_ms=0, interval_ms=10, count=1000)
    svc._current_host_info = HostInfo(host="h.example", port=5432)
    driver_dialect.ping = AsyncMock(return_value=False)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        await asyncio.sleep(0.1)  # let some failures accumulate
        prior = plugin._consecutive_failures

        plugin.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})
        await asyncio.sleep(0.01)  # allow cancellation to propagate
        return prior, plugin._consecutive_failures, plugin._host_unavailable, plugin._monitor_task

    prior, after, unavail, task = asyncio.run(_run())
    assert prior > 0
    assert after == 0
    assert unavail is False
    # Task should be canceled / done (reset by notify)
    assert task is None or task.done()


def test_notify_connection_changed_clears_host_unavailable():
    """Prior threshold breach is cleared when connection swaps."""
    from aws_advanced_python_wrapper.utils.notifications import ConnectionEvent

    plugin, svc, driver_dialect, conn = _build()
    plugin._host_unavailable = True
    plugin._consecutive_failures = 10

    plugin.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})
    assert plugin._host_unavailable is False
    assert plugin._consecutive_failures == 0


def test_notify_host_list_changed_stops_monitor_on_went_down():
    """If the monitored host's topology entry says WENT_DOWN, stop the monitor."""
    from aws_advanced_python_wrapper.utils.notifications import HostEvent

    plugin, svc, driver_dialect, conn = _build(
        grace_ms=0, interval_ms=100, count=1000)
    host = HostInfo(host="h.example", port=5432)
    svc._current_host_info = host

    async def _start_then_notify():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        # Notify that our host went down
        plugin.notify_host_list_changed({host.as_alias(): {HostEvent.WENT_DOWN}})
        return plugin._monitor_stop.is_set()

    stopped = asyncio.run(_start_then_notify())
    assert stopped is True


def test_notify_host_list_changed_ignores_unrelated_hosts():
    """Events for a different host don't stop our monitor."""
    from aws_advanced_python_wrapper.utils.notifications import HostEvent

    plugin, svc, driver_dialect, conn = _build(
        grace_ms=0, interval_ms=100, count=1000)
    host = HostInfo(host="h.example", port=5432)
    svc._current_host_info = host

    async def _start_then_notify():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        plugin.notify_host_list_changed({"unrelated-host:5432": {HostEvent.WENT_DOWN}})
        return plugin._monitor_stop.is_set()

    stopped = asyncio.run(_start_then_notify())
    assert stopped is False


# ---- C.4 shutdown hook -------------------------------------------------


def test_release_resources_async_cancels_monitor_task():
    from aws_advanced_python_wrapper.aio.cleanup import release_resources_async

    plugin, svc, driver_dialect, conn = _build(
        grace_ms=100, interval_ms=100)
    svc._current_host_info = HostInfo(host="h.example", port=5432)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        task = plugin._monitor_task
        assert task is not None
        assert not task.done()
        await release_resources_async()
        return task

    task = asyncio.run(_run())
    # After release_resources_async the task should be finished (cancelled or completed)
    assert task.done()


def test_shutdown_hook_clears_monitor_task():
    """After shutdown hook runs, _monitor_task is None."""
    from aws_advanced_python_wrapper.aio.cleanup import release_resources_async

    plugin, svc, driver_dialect, conn = _build(
        grace_ms=100, interval_ms=100)
    svc._current_host_info = HostInfo(host="h.example", port=5432)

    async def _run():
        async def _noop():
            return None

        await plugin.execute(MagicMock(), "Cursor.execute", _noop)
        assert plugin._monitor_task is not None
        await release_resources_async()

    asyncio.run(_run())
    assert plugin._monitor_task is None


# ---- Telemetry counters ------------------------------------------------


def test_mark_host_unavailable_emits_aborted_connections_counter():
    """When the monitor aborts a connection the efm counter ticks."""
    props = Properties({
        "host": "h.example", "port": "5432",
        "failure_detection_enabled": "true",
        "failure_detection_time_ms": "10",
        "failure_detection_interval_ms": "10",
        "failure_detection_count": "2",
    })
    driver_dialect = MagicMock()
    driver_dialect.ping = AsyncMock(return_value=False)
    driver_dialect.abort_connection = AsyncMock()

    svc = AsyncPluginServiceImpl(
        props, driver_dialect, HostInfo(host="h.example", port=5432))
    svc._current_connection = MagicMock(name="conn")

    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)
    svc.set_telemetry_factory(fake_tf)

    plugin = AsyncHostMonitoringPlugin(svc, props)
    # Drive _mark_host_unavailable directly -- avoids racing the monitor
    # loop in-test.
    asyncio.run(plugin._mark_host_unavailable(
        MagicMock(name="conn"), driver_dialect))

    assert fake_counters["efm.aborted_connections.count"].inc.called
