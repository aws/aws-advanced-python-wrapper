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

"""Async Host Monitoring (EFM v2) plugin -- faithful port of sync v2 semantics.

Covers: shared-monitor keying, duration-based failure math, no-probe-while-idle,
context deactivation in finally, dead-host abort + UNAVAILABLE, the
ConfigurationNotSupported guard, monitor disposal on idle expiry + host-deleted
notification, the F17 leak fix (plugin collectable after the execute scope ends),
and prompt wake of an in-flight execute on abort.
"""

from __future__ import annotations

import asyncio
import gc
import socket as socket_mod
import time
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio import cleanup as aio_cleanup
from aws_advanced_python_wrapper.aio import host_monitoring_plugin as efm
from aws_advanced_python_wrapper.aio.host_monitoring_plugin import (
    AsyncHostMonitoringPlugin, AsyncHostMonitorV2, _cleanup_idle_monitors,
    _monitor_key, _monitors, _stop_all_monitors)
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import Properties

_NETWORK_BOUND = {
    "connect", "Connection.commit", "Connection.rollback",
    "Cursor.execute", "Cursor.executemany",
    "Cursor.fetchone", "Cursor.fetchmany", "Cursor.fetchall",
}


class _FakeConn:
    """A weakref-able connection stand-in that -- unlike a bare MagicMock --
    does NOT fabricate a ``driver_connection`` attribute, so the pooled-proxy
    unwrap in the abort path behaves like a real non-pooled connection."""


@pytest.fixture(autouse=True)
def _reset_efm_registry():
    # Each test uses its own asyncio.run() loop; module-level monitors from a
    # prior test belong to a now-closed loop, so drop them (and any registered
    # shutdown hooks) before and after every test.
    efm._reset_monitor_registry()
    aio_cleanup.clear_shutdown_hooks()
    yield
    efm._reset_monitor_registry()
    aio_cleanup.clear_shutdown_hooks()


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
    driver_dialect.network_bound_methods = set(_NETWORK_BOUND)
    driver_dialect.supports_abort_connection = MagicMock(return_value=True)
    driver_dialect.is_closed = AsyncMock(return_value=False)
    driver_dialect.ping = AsyncMock(return_value=True)
    driver_dialect.abort_connection = AsyncMock()

    svc = AsyncPluginServiceImpl(
        props, driver_dialect, HostInfo(host="h.example", port=5432))
    conn = _FakeConn()
    svc._current_connection = conn

    plugin = AsyncHostMonitoringPlugin(svc, props)
    return plugin, svc, driver_dialect, conn


async def _execute_once(plugin, work=None):
    async def _default():
        return "ok"

    return await plugin.execute(object(), "Cursor.execute", work or _default)


# ---- Config / subscription ---------------------------------------------


def test_subscribed_includes_connect_and_network_bound_methods():
    plugin, *_ = _build()
    assert "connect" in plugin.subscribed_methods
    assert "Cursor.execute" in plugin.subscribed_methods


def test_configuration_not_supported_guard_raises_on_init():
    props = Properties({"host": "h.example", "port": "5432"})
    dd = MagicMock()
    dd.supports_abort_connection = MagicMock(return_value=False)
    svc = AsyncPluginServiceImpl(props, dd, HostInfo(host="h.example", port=5432))
    with pytest.raises(AwsWrapperError):
        AsyncHostMonitoringPlugin(svc, props)


def test_execute_raises_on_none_connection():
    plugin, svc, _dd, _conn = _build()
    svc._current_connection = None

    async def _work():
        return "ok"

    with pytest.raises(AwsWrapperError):
        asyncio.run(plugin.execute(object(), "Cursor.execute", _work))


def test_disabled_plugin_passes_through_without_monitor():
    async def _body():
        plugin, svc, dd, _conn = _build(enabled=False)
        svc.force_connect = AsyncMock()

        assert await _execute_once(plugin) == "ok"
        assert len(_monitors) == 0
        svc.force_connect.assert_not_called()
        dd.ping.assert_not_called()

    asyncio.run(_body())


def test_non_network_bound_method_passes_through():
    async def _body():
        plugin, svc, _dd, _conn = _build()
        svc.force_connect = AsyncMock()

        assert await plugin.execute(object(), "Cursor.close", lambda: _ok()) == "ok"  # noqa: E731
        assert len(_monitors) == 0

    async def _ok():
        return "ok"

    asyncio.run(_body())


# ---- Shared monitor keying ---------------------------------------------


def test_shared_monitor_keying_two_plugins_same_host_share_one_monitor():
    async def _body():
        p1, *_ = _build(grace_ms=100000)
        p2, *_ = _build(grace_ms=100000)
        await _execute_once(p1)
        await _execute_once(p2)
        assert len(_monitors) == 1
        await _stop_all_monitors()

    asyncio.run(_body())


def test_different_params_get_distinct_monitors():
    async def _body():
        p1, *_ = _build(grace_ms=100000, count=2)
        p2, *_ = _build(grace_ms=100000, count=3)
        await _execute_once(p1)
        await _execute_once(p2)
        assert len(_monitors) == 2
        await _stop_all_monitors()

    asyncio.run(_body())


# ---- Duration-based failure math ---------------------------------------


def test_duration_based_threshold_math():
    async def _body():
        plugin, svc, _dd, _conn = _build(interval_ms=100, count=3)
        # interval 0.1s, count 3 -> dead once invalid for interval*(count-1)=0.2s.
        monitor = AsyncHostMonitorV2(
            svc, HostInfo(host="h.example", port=5432), plugin._properties,
            0, 100, 3, None, "k")

        monitor._update_host_health_status(False, 1.0, 1.1)  # 0.1s < 0.2s
        assert monitor._is_unhealthy is False
        assert monitor._failure_count == 1

        monitor._update_host_health_status(False, 1.15, 1.25)  # 1.25-1.0 = 0.25 >= 0.2
        assert monitor._is_unhealthy is True

        # A valid probe brings it back.
        monitor._update_host_health_status(True, 1.30, 1.31)
        assert monitor._is_unhealthy is False
        assert monitor._failure_count == 0
        assert monitor._invalid_host_start_time == 0.0

    asyncio.run(_body())


def test_threshold_math_is_duration_not_consecutive_count():
    async def _body():
        plugin, svc, _dd, _conn = _build()
        # count=5, interval 0.1s -> dead threshold 0.4s. Many consecutive
        # failures within a short duration must NOT trip it (proves duration-,
        # not count-based).
        monitor = AsyncHostMonitorV2(
            svc, HostInfo(host="h.example", port=5432), plugin._properties,
            0, 100, 5, None, "k")
        t = 1.0
        for _ in range(10):
            monitor._update_host_health_status(False, t, t + 0.001)
            t += 0.001
        assert monitor._failure_count == 10
        assert monitor._is_unhealthy is False  # only ~0.01s invalid, < 0.4s

    asyncio.run(_body())


def test_count_one_declares_dead_on_first_failure():
    async def _body():
        plugin, svc, _dd, _conn = _build()
        # count=1 -> threshold interval*0 = 0 -> any single failure is fatal.
        monitor = AsyncHostMonitorV2(
            svc, HostInfo(host="h.example", port=5432), plugin._properties,
            0, 100, 1, None, "k")
        monitor._update_host_health_status(False, 1.0, 1.0)
        assert monitor._is_unhealthy is True

    asyncio.run(_body())


# ---- No probe while idle -----------------------------------------------


def test_no_probe_while_no_active_context():
    async def _body():
        plugin, svc, dd, _conn = _build(grace_ms=100000)  # grace never elapses
        svc.force_connect = AsyncMock()

        await _execute_once(plugin)
        await asyncio.sleep(0.05)  # let the monitor spin idle cycles

        svc.force_connect.assert_not_called()
        dd.ping.assert_not_called()
        await _stop_all_monitors()

    asyncio.run(_body())


# ---- Context deactivated in finally ------------------------------------


def test_context_set_inactive_in_finally():
    async def _body():
        plugin, svc, _dd, _conn = _build(grace_ms=100000)
        svc.force_connect = AsyncMock()

        captured = {}
        original = plugin._monitor_service.start_monitoring

        async def _capture(*a, **k):
            ctx = await original(*a, **k)
            captured["ctx"] = ctx
            return ctx

        plugin._monitor_service.start_monitoring = _capture
        await _execute_once(plugin)

        assert captured["ctx"].is_active() is False
        await _stop_all_monitors()

    asyncio.run(_body())


def test_context_set_inactive_even_when_execute_raises():
    async def _body():
        plugin, svc, _dd, _conn = _build(grace_ms=100000)
        svc.force_connect = AsyncMock()

        captured = {}
        original = plugin._monitor_service.start_monitoring

        async def _capture(*a, **k):
            ctx = await original(*a, **k)
            captured["ctx"] = ctx
            return ctx

        plugin._monitor_service.start_monitoring = _capture

        async def _raiser():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await _execute_once(plugin, _raiser)

        assert captured["ctx"].is_active() is False
        await _stop_all_monitors()

    asyncio.run(_body())


# ---- Dead host -> abort + UNAVAILABLE -----------------------------------


def test_dead_host_marks_unavailable_and_aborts_active_context():
    async def _body():
        plugin, svc, dd, conn = _build(grace_ms=0, interval_ms=10, count=1)
        svc.set_availability = MagicMock()
        svc.force_connect = AsyncMock(return_value=_FakeConn())
        # First probe opens the monitoring conn (True); subsequent pings fail.
        dd.ping = AsyncMock(return_value=False)

        started = asyncio.Event()

        async def _work():
            started.set()
            await asyncio.sleep(1.0)  # stay in-flight until aborted
            return "done"

        task = asyncio.ensure_future(_execute_once(plugin, _work))
        await started.wait()
        # Give the monitor time to open, probe-fail, and declare the host dead.
        await asyncio.sleep(0.3)

        svc.set_availability.assert_any_call(
            HostInfo(host="h.example", port=5432).as_aliases(),
            HostAvailability.UNAVAILABLE)
        dd.abort_connection.assert_awaited()
        aborted_with = dd.abort_connection.await_args_list[0].args[0]
        assert aborted_with is conn  # non-pooled conn aborted directly

        task.cancel()
        with pytest.raises((asyncio.CancelledError, Exception)):
            await task
        await _stop_all_monitors()

    asyncio.run(_body())


# ---- Prompt wake of in-flight execute on abort -------------------------


def test_in_flight_execute_wakes_promptly_on_abort():
    async def _body():
        plugin, svc, dd, conn = _build(grace_ms=0, interval_ms=10, count=1)
        svc.set_availability = MagicMock()
        svc.force_connect = AsyncMock(return_value=_FakeConn())
        dd.ping = AsyncMock(return_value=False)

        abort_signal = asyncio.Event()

        async def _abort(target):
            # Simulate the socket sever waking the suspended read.
            abort_signal.set()

        dd.abort_connection = AsyncMock(side_effect=_abort)

        async def _work():
            await abort_signal.wait()
            raise ConnectionError("socket severed by monitor")

        # If abort didn't wake the in-flight read, this hangs and wait_for trips.
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(
                plugin.execute(object(), "Cursor.execute", _work), timeout=2.0)

        await _stop_all_monitors()

    asyncio.run(_body())


# ---- Abort unwraps pooled proxy ----------------------------------------


def test_abort_unwraps_pooled_proxy_to_raw_connection():
    async def _body():
        dd = MagicMock()
        dd.is_closed = AsyncMock(return_value=False)
        dd.abort_connection = AsyncMock()
        raw = _FakeConn()
        proxy = _FakeConn()
        proxy.driver_connection = raw

        await efm._abort_target_connection(dd, proxy)
        dd.abort_connection.assert_awaited_once_with(raw)

    asyncio.run(_body())


def test_abort_non_pooled_connection_aborts_itself():
    async def _body():
        dd = MagicMock()
        dd.is_closed = AsyncMock(return_value=False)
        dd.abort_connection = AsyncMock()
        raw = _FakeConn()  # no driver_connection

        await efm._abort_target_connection(dd, raw)
        dd.abort_connection.assert_awaited_once_with(raw)

    asyncio.run(_body())


def test_abort_skips_already_closed_connection():
    async def _body():
        dd = MagicMock()
        dd.is_closed = AsyncMock(return_value=True)
        dd.abort_connection = AsyncMock()

        await efm._abort_target_connection(dd, _FakeConn())
        dd.abort_connection.assert_not_awaited()

    asyncio.run(_body())


# ---- Monitor disposal --------------------------------------------------


def test_monitor_disposed_on_idle_expiry():
    async def _body():
        plugin, svc, _dd, _conn = _build()
        host = HostInfo(host="h.example", port=5432)
        key = _monitor_key(0, 1000, 1, host.url)
        monitor = AsyncHostMonitorV2(
            svc, host, plugin._properties, 0, 1000, 1, None, key)
        monitor.start()
        _monitors[key] = monitor

        assert monitor.can_dispose is True  # no contexts registered
        monitor._last_used = time.monotonic() - (efm._MONITOR_EXPIRATION_SEC + 1)

        _cleanup_idle_monitors()

        assert key not in _monitors
        assert monitor._stopped is True
        await monitor.aclose()

    asyncio.run(_body())


def test_active_monitor_not_disposed_on_cleanup():
    async def _body():
        plugin, svc, _dd, _conn = _build(grace_ms=100000)
        svc.force_connect = AsyncMock()
        await _execute_once(plugin)
        key = next(iter(_monitors))
        monitor = _monitors[key]
        # A context is still registered (grace never elapsed) so it isn't idle
        # even with an old last_used.
        monitor._last_used = time.monotonic() - (efm._MONITOR_EXPIRATION_SEC + 1)

        _cleanup_idle_monitors()

        assert key in _monitors  # not disposed: still has a pending context
        await _stop_all_monitors()

    asyncio.run(_body())


def test_monitor_disposed_on_host_deleted_notification():
    async def _body():
        plugin, svc, _dd, _conn = _build()
        host = HostInfo(host="h.example", port=5432)
        key = _monitor_key(0, 1000, 1, host.url)
        monitor = AsyncHostMonitorV2(
            svc, host, plugin._properties, 0, 1000, 1, None, key)
        monitor.start()
        _monitors[key] = monitor

        plugin.notify_host_list_changed({host.url: {HostEvent.HOST_DELETED}})

        assert key not in _monitors
        assert monitor._stopped is True
        await monitor.aclose()

    asyncio.run(_body())


def test_notify_host_list_changed_ignores_non_deleted_events():
    async def _body():
        plugin, svc, _dd, _conn = _build()
        host = HostInfo(host="h.example", port=5432)
        key = _monitor_key(0, 1000, 1, host.url)
        monitor = AsyncHostMonitorV2(
            svc, host, plugin._properties, 0, 1000, 1, None, key)
        monitor.start()
        _monitors[key] = monitor

        plugin.notify_host_list_changed({host.url: {HostEvent.WENT_DOWN}})
        assert key in _monitors  # WENT_DOWN alone doesn't dispose
        await _stop_all_monitors()

    asyncio.run(_body())


# ---- notify_connection_changed -----------------------------------------


def test_notify_connection_changed_resets_monitoring_host_and_votes_no_opinion():
    plugin, *_ = _build()
    plugin._monitoring_host_info = HostInfo(host="i.example", port=5432)

    result = plugin.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})

    assert plugin._monitoring_host_info is None
    assert result == OldConnectionSuggestedAction.NO_OPINION


def test_notify_connection_changed_other_event_is_no_opinion():
    plugin, *_ = _build()
    plugin._monitoring_host_info = HostInfo(host="i.example", port=5432)

    result = plugin.notify_connection_changed({ConnectionEvent.INITIAL_CONNECTION})

    assert plugin._monitoring_host_info is not None  # untouched
    assert result == OldConnectionSuggestedAction.NO_OPINION


# ---- Leak fix (F17) ----------------------------------------------------


def test_plugin_collectable_after_execute_scope_ends():
    # The plugin must NOT be retained by any module-level structure (the old
    # design leaked it via register_shutdown_hook(self._shutdown)).
    def _make_ref():
        async def _body():
            plugin, svc, _dd, _conn = _build(grace_ms=100000)
            svc.force_connect = AsyncMock()
            await _execute_once(plugin)
            return weakref.ref(plugin)

        return asyncio.run(_body())

    ref = _make_ref()
    gc.collect()
    assert ref() is None


def test_plugin_does_not_register_per_instance_shutdown_hook():
    # Building the plugin (and running an execute) must not append a per-plugin
    # bound method to the global shutdown-hook list -- at most the single
    # module-level registry hook.
    aio_cleanup.clear_shutdown_hooks()

    async def _body():
        plugin, svc, _dd, _conn = _build(grace_ms=100000)
        svc.force_connect = AsyncMock()
        await _execute_once(plugin)

    asyncio.run(_body())
    assert len(aio_cleanup._registered_shutdown_hooks) <= 1


def test_stop_all_monitors_shutdown_hook_clears_registry():
    async def _body():
        plugin, svc, _dd, _conn = _build(grace_ms=100000)
        svc.force_connect = AsyncMock()
        await _execute_once(plugin)
        assert len(_monitors) == 1
        await aio_cleanup.release_resources_async()
        assert len(_monitors) == 0

    asyncio.run(_body())


# ---- Telemetry counter -------------------------------------------------


def test_telemetry_counter_uses_efm2_name():
    props = Properties({
        "host": "h.example", "port": "5432",
        "failure_detection_enabled": "true",
        "failure_detection_time_ms": "10",
        "failure_detection_interval_ms": "10",
        "failure_detection_count": "2",
    })
    dd = MagicMock()
    dd.network_bound_methods = set(_NETWORK_BOUND)
    dd.supports_abort_connection = MagicMock(return_value=True)

    created: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        created[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)

    svc = AsyncPluginServiceImpl(props, dd, HostInfo(host="h.example", port=5432))
    svc.set_telemetry_factory(fake_tf)

    AsyncHostMonitoringPlugin(svc, props)

    assert "efm2.connections.aborted" in created
    assert "efm.aborted_connections.count" not in created


# ---- psycopg dialect socket-shutdown abort -----------------------------


def test_psycopg_abort_connection_shuts_socket_down():
    from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
        AsyncPsycopgDriverDialect

    a, b = socket_mod.socketpair()
    try:
        class _FakePgConn:
            closed = False

            def fileno(self):
                return a.fileno()

        dialect = AsyncPsycopgDriverDialect()
        asyncio.run(dialect.abort_connection(_FakePgConn()))

        # SHUT_RDWR on 'a' makes the peer 'b' observe EOF on read.
        b.setblocking(True)
        b.settimeout(2.0)
        assert b.recv(16) == b""
    finally:
        a.close()
        b.close()


def test_psycopg_abort_connection_noop_on_closed():
    from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
        AsyncPsycopgDriverDialect

    class _ClosedConn:
        closed = True

        def fileno(self):
            raise AssertionError("fileno must not be touched on a closed conn")

    dialect = AsyncPsycopgDriverDialect()
    asyncio.run(dialect.abort_connection(_ClosedConn()))  # no raise


def test_async_dialects_support_abort_connection():
    from aws_advanced_python_wrapper.aio.driver_dialect.aiomysql import \
        AsyncAiomysqlDriverDialect
    from aws_advanced_python_wrapper.aio.driver_dialect.psycopg import \
        AsyncPsycopgDriverDialect

    assert AsyncPsycopgDriverDialect().supports_abort_connection() is True
    assert AsyncAiomysqlDriverDialect().supports_abort_connection() is True
