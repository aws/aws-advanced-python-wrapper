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

"""Unit tests for :class:`AsyncFastestResponseStrategyPlugin` (minimal port).

Covers the six load-bearing branches of the async strategy plugin:

1. ``accepts_strategy`` returns True only for ``"fastest_response"``.
2. ``get_host_info_by_strategy`` raises for unsupported strategies.
3. ``get_host_info_by_strategy`` falls back to Random when no cache.
4. ``get_host_info_by_strategy`` returns the cached winner when fresh.
5. ``measure_and_cache`` picks the fastest probed host (gather parallel).
6. ``notify_host_list_changed`` clears the cache.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.fastest_response_strategy_plugin import \
    AsyncFastestResponseStrategyPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Helpers -----------------------------------------------------------


def _reader(host: str, port: int = 5432) -> HostInfo:
    return HostInfo(host=host, port=port, role=HostRole.READER)


def _writer(host: str = "writer.example.com", port: int = 5432) -> HostInfo:
    return HostInfo(host=host, port=port, role=HostRole.WRITER)


def _build(
        all_hosts: tuple = (),
        props: Any = None,
) -> tuple:
    """Construct plugin + mock plugin_service.

    Returns ``(plugin, plugin_service, driver_dialect)`` so tests can
    manipulate the mocks directly.
    """
    if props is None:
        props = Properties({})
    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock()
    driver_dialect.ping = AsyncMock(return_value=True)
    driver_dialect.abort_connection = AsyncMock()

    plugin_service = MagicMock()
    plugin_service.all_hosts = all_hosts
    plugin_service.driver_dialect = driver_dialect

    plugin = AsyncFastestResponseStrategyPlugin(plugin_service, props)
    return plugin, plugin_service, driver_dialect


# ---- 1: accepts_strategy ----------------------------------------------


def test_accepts_strategy_only_for_fastest_response():
    plugin, *_ = _build()
    assert plugin.accepts_strategy(HostRole.READER, "fastest_response") is True
    assert plugin.accepts_strategy(HostRole.WRITER, "fastest_response") is True
    assert plugin.accepts_strategy(HostRole.READER, "random") is False
    assert plugin.accepts_strategy(HostRole.READER, "") is False


# ---- 2: get_host_info_by_strategy rejects unsupported strategy --------


def test_get_host_info_by_strategy_raises_for_unsupported_strategy():
    plugin, *_ = _build(all_hosts=(_reader("r1"),))
    with pytest.raises(AwsWrapperError):
        plugin.get_host_info_by_strategy(HostRole.READER, "round_robin")


# ---- 3: no cache -> falls back to Random -------------------------------


def test_get_host_info_by_strategy_no_cache_falls_back_to_random():
    readers = (_reader("r1"), _reader("r2"), _reader("r3"))
    plugin, *_ = _build(all_hosts=readers)

    # No cache primed; must return one of the candidates via RandomHostSelector.
    picked = plugin.get_host_info_by_strategy(HostRole.READER, "fastest_response")
    assert picked is not None
    assert picked.host in {"r1", "r2", "r3"}
    # Cache must stay empty after a Random fallback.
    assert plugin._cached_fastest == {}


def test_get_host_info_by_strategy_returns_none_for_empty_candidates():
    # all_hosts contains only a writer; asking for a reader yields None.
    plugin, *_ = _build(all_hosts=(_writer(),))
    result = plugin.get_host_info_by_strategy(HostRole.READER, "fastest_response")
    assert result is None


# ---- 4: cached winner returned ----------------------------------------


def test_get_host_info_by_strategy_returns_cached_winner_when_fresh():
    readers = (_reader("r1"), _reader("r2"), _reader("r3"))
    plugin, *_ = _build(all_hosts=readers)

    # Prime cache: winner = r2 with expiry far in the future.
    winner = readers[1]
    plugin._cached_fastest[HostRole.READER.name] = (
        winner, plugin._loop_time() + 3600.0)

    picked = plugin.get_host_info_by_strategy(HostRole.READER, "fastest_response")
    assert picked is winner


def test_get_host_info_by_strategy_drops_stale_cache_and_falls_back():
    readers = (_reader("r1"), _reader("r2"))
    plugin, *_ = _build(all_hosts=readers)

    # Winner is an instance no longer in topology; cache entry is otherwise
    # still "fresh" time-wise.
    gone = _reader("r-gone")
    plugin._cached_fastest[HostRole.READER.name] = (
        gone, plugin._loop_time() + 3600.0)

    picked = plugin.get_host_info_by_strategy(HostRole.READER, "fastest_response")
    assert picked is not None
    assert picked.host in {"r1", "r2"}
    # Stale entry should have been removed.
    assert HostRole.READER.name not in plugin._cached_fastest


# ---- 5: measure_and_cache picks fastest -------------------------------


def test_measure_and_cache_picks_fastest_probed_host():
    async def _body() -> None:
        readers = (_reader("slow"), _reader("fast"), _reader("mid"))
        plugin, plugin_service, driver_dialect = _build(all_hosts=readers)

        # target_driver_func must be non-None for probes to run.
        plugin._target_driver_func = MagicMock(name="target_driver_func")

        # driver_dialect.connect returns a unique mock conn per call so we
        # can pair each connect with a ping latency (via side_effect
        # consuming in the same order as gather schedules).
        conn_slow = MagicMock(name="conn_slow")
        conn_fast = MagicMock(name="conn_fast")
        conn_mid = MagicMock(name="conn_mid")

        async def _connect(host_info, props, target_driver_func):
            # Simulate different connect latencies per host. Fast is
            # fastest, mid middle, slow slowest.
            if host_info.host == "slow":
                await asyncio.sleep(0.030)
                return conn_slow
            if host_info.host == "mid":
                await asyncio.sleep(0.015)
                return conn_mid
            # fast
            await asyncio.sleep(0.001)
            return conn_fast

        driver_dialect.connect = AsyncMock(side_effect=_connect)
        driver_dialect.ping = AsyncMock(return_value=True)

        winner = await plugin.measure_and_cache(HostRole.READER)

        assert winner is not None
        assert winner.host == "fast"
        # Cache populated for that role.
        cached = plugin._cached_fastest[HostRole.READER.name]
        assert cached[0] is winner

    asyncio.run(_body())


def test_measure_and_cache_returns_none_when_every_probe_fails():
    async def _body() -> None:
        readers = (_reader("r1"), _reader("r2"))
        plugin, _, driver_dialect = _build(all_hosts=readers)
        plugin._target_driver_func = MagicMock()

        # Every connect attempt raises -> all probes fail -> no winner.
        driver_dialect.connect = AsyncMock(side_effect=RuntimeError("boom"))

        winner = await plugin.measure_and_cache(HostRole.READER)
        assert winner is None
        assert plugin._cached_fastest == {}

    asyncio.run(_body())


def test_measure_and_cache_without_target_driver_func_skips_probes():
    async def _body() -> None:
        readers = (_reader("r1"),)
        plugin, _, driver_dialect = _build(all_hosts=readers)

        # _target_driver_func stays None -- no connect should happen.
        winner = await plugin.measure_and_cache(HostRole.READER)
        assert winner is None
        driver_dialect.connect.assert_not_awaited()

    asyncio.run(_body())


# ---- 6: notify_host_list_changed clears cache -------------------------


def test_notify_host_list_changed_clears_cache():
    plugin, *_ = _build(all_hosts=(_reader("r1"),))
    plugin._cached_fastest[HostRole.READER.name] = (
        _reader("r1"), plugin._loop_time() + 3600.0)
    plugin._cached_fastest[HostRole.WRITER.name] = (
        _writer(), plugin._loop_time() + 3600.0)

    plugin.notify_host_list_changed({})

    assert plugin._cached_fastest == {}


# ---- Subscribed methods sanity ----------------------------------------


def test_subscribed_methods_covers_expected_hooks():
    plugin, *_ = _build()
    subs = plugin.subscribed_methods
    assert "accepts_strategy" in subs
    assert "get_host_info_by_strategy" in subs
    assert "notify_host_list_changed" in subs
    # connect interception lets the plugin capture target_driver_func.
    assert "connect" in subs


def test_connect_passes_through_and_captures_target_driver_func():
    async def _body() -> None:
        plugin, _, driver_dialect = _build()
        target_driver_func = MagicMock(name="target")
        expected_conn = MagicMock(name="conn")

        async def _connect_func() -> Any:
            return expected_conn

        result = await plugin.connect(
            target_driver_func=target_driver_func,
            driver_dialect=driver_dialect,
            host_info=_writer(),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=_connect_func,
        )

        assert result is expected_conn
        assert plugin._target_driver_func is target_driver_func

    asyncio.run(_body())
