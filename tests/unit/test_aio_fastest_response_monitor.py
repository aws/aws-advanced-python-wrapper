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

"""Tests for AsyncHostResponseTimeMonitor + Cache + Service (L.2)."""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.fastest_response_strategy_plugin import (
    AsyncHostResponseTimeCache, AsyncHostResponseTimeMonitor,
    AsyncHostResponseTimeService)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

_MAX_RESPONSE_NS = 10 ** 18


@pytest.fixture(autouse=True)
def _reset_frt_singletons():
    AsyncHostResponseTimeCache.clear()
    AsyncHostResponseTimeService._reset_for_tests()
    yield
    try:
        asyncio.run(AsyncHostResponseTimeService.stop_all())
    except RuntimeError:
        pass
    AsyncHostResponseTimeCache.clear()
    AsyncHostResponseTimeService._reset_for_tests()


def _host(name: str, role: HostRole = HostRole.READER) -> HostInfo:
    return HostInfo(host=name, port=5432, role=role, host_id=name)


def _make_plugin_service(
        probe_conn: Any = None,
        ping_result: bool = True) -> Any:
    svc = MagicMock()
    svc.driver_dialect = MagicMock()
    svc.driver_dialect.ping = AsyncMock(return_value=ping_result)
    svc.driver_dialect.abort_connection = AsyncMock()
    # The monitor probes via force_connect (sync parity), not connect.
    svc.force_connect = AsyncMock(return_value=probe_conn)
    svc.connect = AsyncMock(return_value=probe_conn)
    svc.current_host_info = None
    return svc


# ----- Cache -----------------------------------------------------------


def test_cache_put_get() -> None:
    AsyncHostResponseTimeCache.put("url-1", 12_345_678)
    assert AsyncHostResponseTimeCache.get("url-1") == 12_345_678


def test_cache_get_returns_sentinel_for_unknown() -> None:
    assert AsyncHostResponseTimeCache.get("never-recorded") == _MAX_RESPONSE_NS


def test_cache_put_overrides_previous_value() -> None:
    AsyncHostResponseTimeCache.put("u", 100)
    AsyncHostResponseTimeCache.put("u", 200)
    assert AsyncHostResponseTimeCache.get("u") == 200


def test_cache_remove_wipes_entry() -> None:
    AsyncHostResponseTimeCache.put("u", 100)
    AsyncHostResponseTimeCache.remove("u")
    assert AsyncHostResponseTimeCache.get("u") == _MAX_RESPONSE_NS


# ----- Monitor ---------------------------------------------------------


def test_monitor_not_running_before_start() -> None:
    svc = _make_plugin_service()
    m = AsyncHostResponseTimeMonitor(svc, _host("h1"), Properties(), 10_000)
    assert m.is_running() is False


def test_monitor_measure_once_publishes_result() -> None:
    probe = MagicMock(name="probe_conn")
    svc = _make_plugin_service(probe_conn=probe, ping_result=True)
    host = _host("h1")
    m = AsyncHostResponseTimeMonitor(svc, host, Properties(), 10_000)

    async def _body():
        await m._measure_once()

    asyncio.run(_body())
    # Ping succeeds -> cache gets a finite positive number.
    rt = AsyncHostResponseTimeCache.get(host.url)
    assert rt > 0
    assert rt < _MAX_RESPONSE_NS


def test_monitor_measure_once_ping_failure_marks_unreachable() -> None:
    probe = MagicMock(name="probe_conn")
    svc = _make_plugin_service(probe_conn=probe, ping_result=False)
    host = _host("h2")
    m = AsyncHostResponseTimeMonitor(svc, host, Properties(), 10_000)

    async def _body():
        await m._measure_once()

    asyncio.run(_body())
    assert AsyncHostResponseTimeCache.get(host.url) == _MAX_RESPONSE_NS


def test_monitor_measure_once_connect_failure_marks_unreachable() -> None:
    svc = _make_plugin_service(probe_conn=None)
    svc.force_connect = AsyncMock(side_effect=RuntimeError("probe open failed"))
    host = _host("h3")
    m = AsyncHostResponseTimeMonitor(svc, host, Properties(), 10_000)

    async def _body():
        await m._measure_once()

    asyncio.run(_body())
    assert AsyncHostResponseTimeCache.get(host.url) == _MAX_RESPONSE_NS


def test_monitor_probes_via_force_connect_with_stripped_props() -> None:
    """The monitor connects with force_connect, strips the frt- prefix from
    monitoring props, and defaults CONNECT_TIMEOUT_SEC to 10."""
    probe = MagicMock(name="probe_conn")
    svc = _make_plugin_service(probe_conn=probe, ping_result=True)
    props = Properties({"frt-connect_timeout": "3", "keep_me": "yes"})
    host = _host("h1")
    m = AsyncHostResponseTimeMonitor(svc, host, props, 10_000)

    async def _body():
        await m._measure_once()

    asyncio.run(_body())

    svc.force_connect.assert_awaited()
    svc.connect.assert_not_awaited()
    used_props = svc.force_connect.call_args.args[1]
    # frt- prefix stripped: frt-connect_timeout -> connect_timeout=3 (user
    # override wins over the default).
    assert used_props.get("connect_timeout") == "3"
    assert "frt-connect_timeout" not in used_props
    assert used_props.get("keep_me") == "yes"


def test_monitor_defaults_connect_timeout_to_10() -> None:
    probe = MagicMock(name="probe_conn")
    svc = _make_plugin_service(probe_conn=probe, ping_result=True)
    host = _host("h1")
    m = AsyncHostResponseTimeMonitor(svc, host, Properties(), 10_000)

    async def _body():
        await m._measure_once()

    asyncio.run(_body())
    used_props = svc.force_connect.call_args.args[1]
    assert used_props.get("connect_timeout") == 10


def test_monitor_request_reset_evicts_cache_and_drops_probe_conn() -> None:
    probe = MagicMock(name="probe_conn")
    svc = _make_plugin_service(probe_conn=probe, ping_result=True)
    host = _host("h1")
    m = AsyncHostResponseTimeMonitor(svc, host, Properties(), 10_000)

    async def _body():
        await m._measure_once()
        assert AsyncHostResponseTimeCache.get(host.url) < _MAX_RESPONSE_NS
        # Reset drops the cached time immediately and flags a probe reopen.
        m.request_reset()
        assert AsyncHostResponseTimeCache.get(host.url) == _MAX_RESPONSE_NS
        # Next measurement closes the old probe before reusing it.
        await m._measure_once()
        svc.driver_dialect.abort_connection.assert_awaited()

    asyncio.run(_body())


def test_monitor_stop_is_idempotent() -> None:
    svc = _make_plugin_service()
    m = AsyncHostResponseTimeMonitor(svc, _host("h1"), Properties(), 10_000)

    async def _body():
        await m.stop()
        await m.stop()

    asyncio.run(_body())


def test_monitor_start_then_stop_cleanly() -> None:
    probe = MagicMock(name="probe_conn")
    svc = _make_plugin_service(probe_conn=probe)
    m = AsyncHostResponseTimeMonitor(svc, _host("h1"), Properties(), 100)

    async def _body():
        m.start()
        assert m.is_running() is True
        await m.stop()
        assert m.is_running() is False

    asyncio.run(_body())


# ----- Service ---------------------------------------------------------


def test_service_spawns_monitor_per_host_on_set_hosts_in_loop() -> None:
    svc = _make_plugin_service()
    service = AsyncHostResponseTimeService(svc, Properties(), 10_000)

    async def _body():
        service.set_hosts((_host("a"), _host("b")))
        # Both monitors should be tracked by the class-level registry.
        assert set(AsyncHostResponseTimeService._monitors.keys()) == {
            _host("a").url, _host("b").url}
        await AsyncHostResponseTimeService.stop_all()

    asyncio.run(_body())


def test_service_set_hosts_out_of_loop_tracks_known_urls() -> None:
    """Calling set_hosts without a running loop is a no-op for monitor
    lifecycle but still records the known URL set for the next reconciliation."""
    svc = _make_plugin_service()
    service = AsyncHostResponseTimeService(svc, Properties(), 10_000)
    service.set_hosts((_host("a"), _host("b")))
    # No monitors created (no running loop).
    assert AsyncHostResponseTimeService._monitors == {}
    # Internal tracking updated.
    assert service._known_urls == {_host("a").url, _host("b").url}


def test_service_removes_monitors_when_hosts_drop_out() -> None:
    svc = _make_plugin_service()
    service = AsyncHostResponseTimeService(svc, Properties(), 10_000)

    async def _body():
        service.set_hosts((_host("a"), _host("b")))
        assert len(AsyncHostResponseTimeService._monitors) == 2
        # Drop 'b'.
        service.set_hosts((_host("a"),))
        # 'b' monitor gets dropped; stop is scheduled as a task. Yield
        # to the loop so the scheduled stop runs before we assert.
        await asyncio.sleep(0)
        assert _host("b").url not in AsyncHostResponseTimeService._monitors
        assert _host("a").url in AsyncHostResponseTimeService._monitors
        await AsyncHostResponseTimeService.stop_all()

    asyncio.run(_body())


def test_service_holds_strong_ref_on_spawned_stop_tasks() -> None:
    """Regression guard: ``set_hosts`` spawns fire-and-forget stop
    tasks via ``asyncio.create_task``. Without a strong reference the
    GC can collect the task mid-cancel, which surfaces as "Task was
    destroyed but it is pending" (fatal under pytest -Werror).
    The service keeps the task in ``_stop_tasks`` until done."""
    svc = _make_plugin_service()
    service = AsyncHostResponseTimeService(svc, Properties(), 10_000)

    async def _body():
        service.set_hosts((_host("a"), _host("b")))
        # Drop 'b' -> spawns a stop task. Inspect _stop_tasks before
        # yielding: the task must be tracked there (not just a local
        # variable that can be collected).
        service.set_hosts((_host("a"),))
        tracked_before = set(AsyncHostResponseTimeService._stop_tasks)
        assert len(tracked_before) >= 1
        # Await the tracked stop tasks directly so we observe the
        # done_callback removing each from _stop_tasks. Using
        # stop_all() wouldn't help -- it tears down the 'a' monitor
        # whose stop task is spawned synchronously inside stop_all,
        # not the prior 'b' stop task spawned by set_hosts.
        await asyncio.gather(*tracked_before, return_exceptions=True)
        # After completion the done_callback has discarded each.
        for task in tracked_before:
            assert task not in AsyncHostResponseTimeService._stop_tasks
        await AsyncHostResponseTimeService.stop_all()

    asyncio.run(_body())


def test_service_get_response_time_ns_reads_cache() -> None:
    h = _host("cached-host")
    AsyncHostResponseTimeCache.put(h.url, 5_000_000)
    assert AsyncHostResponseTimeService.get_response_time_ns(h) == 5_000_000


def test_service_stop_all_clears_registry() -> None:
    svc = _make_plugin_service()
    service = AsyncHostResponseTimeService(svc, Properties(), 10_000)

    async def _body():
        service.set_hosts((_host("a"),))
        await AsyncHostResponseTimeService.stop_all()
        assert AsyncHostResponseTimeService._monitors == {}

    asyncio.run(_body())


# ----- Expiry + reset --------------------------------------------------


def test_cache_entry_expires_after_ttl(monkeypatch) -> None:
    # Shrink the TTL so a freshly-put entry is already expired on read.
    monkeypatch.setattr(AsyncHostResponseTimeCache, "_ttl_ns", -1)
    AsyncHostResponseTimeCache.put("u", 123)
    assert AsyncHostResponseTimeCache.get("u") == _MAX_RESPONSE_NS


def test_cache_entry_live_within_ttl() -> None:
    AsyncHostResponseTimeCache.put("u", 123)
    assert AsyncHostResponseTimeCache.get("u") == 123


def test_service_reset_host_evicts_and_flags_monitor() -> None:
    svc = _make_plugin_service()
    service = AsyncHostResponseTimeService(svc, Properties(), 10_000)
    host = _host("a")
    monitor = AsyncHostResponseTimeMonitor(svc, host, Properties(), 10_000)
    AsyncHostResponseTimeService._monitors[host.url] = monitor
    AsyncHostResponseTimeCache.put(host.url, 42)

    service.reset_host(host.url)

    assert monitor._reset_requested is True
    assert AsyncHostResponseTimeCache.get(host.url) == _MAX_RESPONSE_NS


def test_service_prunes_idle_monitors_on_set_hosts() -> None:
    svc = _make_plugin_service()
    service = AsyncHostResponseTimeService(svc, Properties(), 10_000)
    url = _host("a").url

    async def _body():
        service.set_hosts((_host("a"),))
        old = AsyncHostResponseTimeService._monitors[url]
        # Age the monitor past the 10-minute idle window.
        old._last_activity_ns = time.perf_counter_ns() - (11 * 60 * 1_000_000_000)
        # A subsequent reconcile prunes the idle monitor; since 'a' is still in
        # the topology it is replaced by a fresh monitor.
        service.set_hosts((_host("a"),))
        await asyncio.sleep(0)
        new = AsyncHostResponseTimeService._monitors.get(url)
        assert new is not None
        assert new is not old
        await AsyncHostResponseTimeService.stop_all()

    asyncio.run(_body())
