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

"""F3-B SP-8: minor async plugins."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.minor_plugins import (
    AsyncConnectTimePlugin, AsyncCustomEndpointPlugin, AsyncDeveloperPlugin,
    AsyncExecuteTimePlugin)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


def test_connect_time_plugin_accumulates_elapsed_time():
    async def _body() -> None:
        p = AsyncConnectTimePlugin()

        async def _connect() -> str:
            await asyncio.sleep(0.005)
            return "ok"

        await p.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo(host="h", port=5432),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=_connect,
        )
        assert p.connect_count == 1
        assert p.total_connect_time_ns > 0

    asyncio.run(_body())


def test_connect_time_plugin_records_even_when_connect_raises():
    async def _body() -> None:
        p = AsyncConnectTimePlugin()

        async def _raiser() -> None:
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            await p.connect(
                target_driver_func=lambda: None,
                driver_dialect=MagicMock(),
                host_info=HostInfo(host="h", port=5432),
                props=Properties({}),
                is_initial_connection=True,
                connect_func=_raiser,
            )
        assert p.connect_count == 1

    asyncio.run(_body())


def test_execute_time_plugin_accumulates_elapsed_time():
    async def _body() -> None:
        p = AsyncExecuteTimePlugin()

        async def _work() -> str:
            await asyncio.sleep(0.005)
            return "rows"

        assert await p.execute(object(), "Cursor.execute", _work) == "rows"
        assert p.execute_count == 1
        assert p.total_execute_time_ns > 0

    asyncio.run(_body())


def test_execute_time_plugin_subscribed_methods_covers_cursor_ops():
    p = AsyncExecuteTimePlugin()
    assert DbApiMethod.CURSOR_EXECUTE.method_name in p.subscribed_methods
    assert DbApiMethod.CURSOR_FETCHONE.method_name in p.subscribed_methods


def test_developer_plugin_injects_configured_exception_then_passes_through():
    async def _body() -> None:
        p = AsyncDeveloperPlugin()
        p.set_next_exception(ValueError("injected"))

        async def _work() -> str:
            return "clean"

        with pytest.raises(ValueError):
            await p.execute(object(), "Cursor.execute", _work)
        # Second call: exception was one-shot, so pass-through.
        assert await p.execute(object(), "Cursor.execute", _work) == "clean"

    asyncio.run(_body())


def test_developer_plugin_pass_through_when_no_injection():
    async def _body() -> None:
        p = AsyncDeveloperPlugin()

        async def _work() -> int:
            return 42

        assert await p.execute(object(), "Cursor.execute", _work) == 42

    asyncio.run(_body())


def test_custom_endpoint_plugin_is_stub():
    p = AsyncCustomEndpointPlugin()
    assert p.subscribed_methods == set()


# --------------------------------------------------------------------------- #
# AsyncDeveloperPlugin: 4-mode injection parity with sync DeveloperPlugin.
# conftest.pytest_runtest_setup calls AsyncDeveloperPlugin.clear(), so each
# test starts with all 4 slots empty without an explicit fixture here.
# --------------------------------------------------------------------------- #


def _make_connect_kwargs():
    return {
        "target_driver_func": lambda: None,
        "driver_dialect": MagicMock(),
        "host_info": HostInfo(host="h", port=5432),
        "props": Properties({}),
        "is_initial_connection": True,
    }


def test_developer_plugin_next_connect_exception_is_one_shot():
    async def _body() -> None:
        AsyncDeveloperPlugin.set_next_connect_exception(RuntimeError("boom"))
        p = AsyncDeveloperPlugin()
        calls = {"n": 0}

        async def _connect() -> str:
            calls["n"] += 1
            return "conn"

        with pytest.raises(RuntimeError, match="boom"):
            await p.connect(connect_func=_connect, **_make_connect_kwargs())
        assert calls["n"] == 0
        # Slot cleared after firing.
        assert AsyncDeveloperPlugin._next_connect_exception is None

    asyncio.run(_body())


def test_developer_plugin_connect_after_exception_fires_normal_flow():
    async def _body() -> None:
        AsyncDeveloperPlugin.set_next_connect_exception(RuntimeError("boom"))
        p = AsyncDeveloperPlugin()

        async def _connect() -> str:
            return "conn"

        with pytest.raises(RuntimeError):
            await p.connect(connect_func=_connect, **_make_connect_kwargs())
        # Fresh connect after the one-shot fires should pass through.
        result = await p.connect(connect_func=_connect, **_make_connect_kwargs())
        assert result == "conn"

    asyncio.run(_body())


def test_developer_plugin_connect_callback_fires_every_connect():
    async def _body() -> None:
        p = AsyncDeveloperPlugin()
        hits: list[tuple[Any, Any]] = []

        def _cb(host_info: Any, props: Any) -> None:
            hits.append((host_info, props))

        AsyncDeveloperPlugin.set_connect_callback(_cb)

        async def _connect() -> str:
            return "conn"

        for _ in range(3):
            assert await p.connect(connect_func=_connect, **_make_connect_kwargs()) == "conn"
        assert len(hits) == 3
        # Callback slot is NOT cleared by successful calls.
        assert AsyncDeveloperPlugin._connect_callback is _cb

    asyncio.run(_body())


def test_developer_plugin_connect_callback_raise_propagates_and_preserves_one_shot():
    async def _body() -> None:
        # Arrange: install BOTH a raising callback and a one-shot exception.
        # The callback runs first and raises; the one-shot exception slot
        # must still be populated afterwards because a callback raise is not
        # the one-shot firing.
        sentinel = RuntimeError("one-shot")
        AsyncDeveloperPlugin.set_next_connect_exception(sentinel)

        def _raiser(host_info: Any, props: Any) -> None:
            raise ValueError("from callback")

        AsyncDeveloperPlugin.set_connect_callback(_raiser)
        p = AsyncDeveloperPlugin()

        async def _connect() -> str:
            return "conn"

        with pytest.raises(ValueError, match="from callback"):
            await p.connect(connect_func=_connect, **_make_connect_kwargs())
        # One-shot NOT consumed.
        assert AsyncDeveloperPlugin._next_connect_exception is sentinel
        # Callback persisted.
        assert AsyncDeveloperPlugin._connect_callback is _raiser

    asyncio.run(_body())


def test_developer_plugin_method_exception_and_callback():
    async def _body() -> None:
        p = AsyncDeveloperPlugin()

        # One-shot method exception.
        AsyncDeveloperPlugin.set_next_method_exception(KeyError("bad"))

        async def _work() -> str:
            return "ok"

        with pytest.raises(KeyError):
            await p.execute(object(), "Cursor.execute", _work)
        assert AsyncDeveloperPlugin._next_method_exception is None
        assert await p.execute(object(), "Cursor.execute", _work) == "ok"

        # Method callback fires on every call.
        hits: list[str] = []

        def _cb(method_name: str, args: Any, kwargs: Any) -> None:
            hits.append(method_name)

        AsyncDeveloperPlugin.set_method_callback(_cb)
        for _ in range(3):
            await p.execute(object(), "Cursor.execute", _work)
        assert hits == ["Cursor.execute"] * 3

    asyncio.run(_body())


def test_developer_plugin_async_callback_is_awaited():
    async def _body() -> None:
        p = AsyncDeveloperPlugin()
        hits: list[str] = []

        async def _async_cb(method_name: str, args: Any, kwargs: Any) -> None:
            # Yield to prove we're actually awaited (coroutine, not a plain fn).
            await asyncio.sleep(0)
            hits.append(method_name)

        AsyncDeveloperPlugin.set_method_callback(_async_cb)

        async def _work() -> int:
            return 1

        assert await p.execute(object(), "Cursor.execute", _work) == 1
        assert hits == ["Cursor.execute"]

        # Same for connect callback.
        connect_hits: list[Any] = []

        async def _async_connect_cb(host_info: Any, props: Any) -> None:
            await asyncio.sleep(0)
            connect_hits.append(host_info)

        AsyncDeveloperPlugin.set_connect_callback(_async_connect_cb)

        async def _connect() -> str:
            return "conn"

        await p.connect(connect_func=_connect, **_make_connect_kwargs())
        assert len(connect_hits) == 1

    asyncio.run(_body())


def test_developer_plugin_clear_resets_all_slots():
    AsyncDeveloperPlugin.set_next_connect_exception(RuntimeError("a"))
    AsyncDeveloperPlugin.set_connect_callback(lambda *_a, **_k: None)
    AsyncDeveloperPlugin.set_next_method_exception(RuntimeError("b"))
    AsyncDeveloperPlugin.set_method_callback(lambda *_a, **_k: None)

    AsyncDeveloperPlugin.clear()

    assert AsyncDeveloperPlugin._next_connect_exception is None
    assert AsyncDeveloperPlugin._connect_callback is None
    assert AsyncDeveloperPlugin._next_method_exception is None
    assert AsyncDeveloperPlugin._method_callback is None


def test_developer_plugin_state_is_class_level_shared_across_instances():
    async def _body() -> None:
        # Two independent instances must see the same ClassVar state.
        p1 = AsyncDeveloperPlugin()
        p2 = AsyncDeveloperPlugin()

        sentinel = RuntimeError("shared")
        AsyncDeveloperPlugin.set_next_method_exception(sentinel)

        async def _work() -> str:
            return "ok"

        # p2 sees the exception set via the class even though p1 was created first.
        with pytest.raises(RuntimeError, match="shared"):
            await p2.execute(object(), "Cursor.execute", _work)
        # ...and now p1 sees the slot cleared by p2's call.
        assert AsyncDeveloperPlugin._next_method_exception is None
        assert await p1.execute(object(), "Cursor.execute", _work) == "ok"

    asyncio.run(_body())


# --------------------------------------------------------------------------- #
# Telemetry counters on ConnectTime / ExecuteTime plugins.
# --------------------------------------------------------------------------- #


def _svc_with_counters(props: Properties):
    """Build an AsyncPluginServiceImpl with a MagicMock telemetry factory.

    Returns (svc, counters) where counters is a dict of name -> MagicMock.
    """
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl

    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)

    svc = AsyncPluginServiceImpl(props, MagicMock(), HostInfo(host="h", port=5432))
    svc.set_telemetry_factory(fake_tf)
    return svc, fake_counters


def test_connect_time_plugin_emits_total_count_counter():
    """connect_time.total.count increments once per connect call."""
    async def _body() -> None:
        svc, counters = _svc_with_counters(Properties({"host": "h"}))
        p = AsyncConnectTimePlugin(svc)

        async def _connect() -> str:
            return "ok"

        await p.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo(host="h", port=5432),
            props=Properties({}),
            is_initial_connection=True,
            connect_func=_connect,
        )
        assert counters["connect_time.total.count"].inc.call_count == 1

        # Second call increments again.
        await p.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo(host="h", port=5432),
            props=Properties({}),
            is_initial_connection=False,
            connect_func=_connect,
        )
        assert counters["connect_time.total.count"].inc.call_count == 2

    asyncio.run(_body())


def test_execute_time_plugin_emits_total_count_counter():
    """execute_time.total.count increments once per execute call."""
    async def _body() -> None:
        svc, counters = _svc_with_counters(Properties({"host": "h"}))
        p = AsyncExecuteTimePlugin(svc)

        async def _work() -> str:
            return "rows"

        await p.execute(object(), "Cursor.execute", _work)
        await p.execute(object(), "Cursor.execute", _work)
        assert counters["execute_time.total.count"].inc.call_count == 2

    asyncio.run(_body())
