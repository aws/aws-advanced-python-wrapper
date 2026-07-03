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

"""F3-B SP-1 shell test: toy async plugins walking the pipeline.

Reuses the ``FakeAsyncDriverDialect`` pattern from
``test_aio_contracts.py`` (duplicated here to keep the test file
self-contained). Purpose: prove ``AsyncPluginManager`` correctly builds
and dispatches the chain for ``connect`` and ``execute``, and that
``AsyncDefaultPlugin`` is always terminal.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Set

from aws_advanced_python_wrapper.aio.default_plugin import AsyncDefaultPlugin
from aws_advanced_python_wrapper.aio.driver_dialect.base import \
    AsyncDriverDialect
from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.aio.plugin_manager import AsyncPluginManager
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.notifications import ConnectionEvent
from aws_advanced_python_wrapper.utils.properties import Properties

# ---- Fakes --------------------------------------------------------------


class _FakeAsyncConnection:
    def __init__(self, host: str):
        self.host = host


class FakeAsyncDriverDialect(AsyncDriverDialect):
    _dialect_code = "fake"
    _driver_name = "FakeAsync"

    def __init__(self) -> None:
        self.connect_count = 0
        self._rw: Dict[int, bool] = {}
        self._ac: Dict[int, bool] = {}

    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> _FakeAsyncConnection:
        self.connect_count += 1
        return _FakeAsyncConnection(host_info.host)

    async def is_closed(self, conn: Any) -> bool:
        return False

    async def abort_connection(self, conn: Any) -> None:
        return None

    async def is_in_transaction(self, conn: Any) -> bool:
        return False

    async def get_autocommit(self, conn: Any) -> bool:
        return self._ac.get(id(conn), True)

    async def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        self._ac[id(conn)] = autocommit

    async def is_read_only(self, conn: Any) -> bool:
        return self._rw.get(id(conn), False)

    async def set_read_only(self, conn: Any, read_only: bool) -> None:
        self._rw[id(conn)] = read_only

    async def can_execute_query(self, conn: Any) -> bool:
        return True

    async def transfer_session_state(self, from_conn: Any, to_conn: Any) -> None:
        self._rw[id(to_conn)] = self._rw.get(id(from_conn), False)
        self._ac[id(to_conn)] = self._ac.get(id(from_conn), True)

    async def ping(self, conn: Any) -> bool:
        return True


class RecorderPlugin(AsyncPlugin):
    """Toy plugin that logs every pipeline call it sees."""

    def __init__(self, name: str, log: List[str]) -> None:
        self.name = name
        self.log = log

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.ALL.method_name}

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self.log.append(f"{self.name}:connect:enter")
        result = await connect_func()
        self.log.append(f"{self.name}:connect:exit")
        return result

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        self.log.append(f"{self.name}:execute:enter:{method_name}")
        result = await execute_func()
        self.log.append(f"{self.name}:execute:exit:{method_name}")
        return result


class SubscribedOnlyConnectPlugin(AsyncPlugin):
    """Subscribes to CONNECT only -- used to verify execute() skips it."""

    def __init__(self, log: List[str]) -> None:
        self.log = log

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.CONNECT.method_name}

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        self.log.append("SubOnly:connect")
        return await connect_func()

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:  # pragma: no cover - should not be called
        self.log.append(f"SubOnly:execute:{method_name}")
        return await execute_func()


class _NotifyRecorderPlugin(AsyncPlugin):
    """Records every notify_connection_changed call it receives."""

    def __init__(self) -> None:
        self.notified: List[Set[ConnectionEvent]] = []

    @property
    def subscribed_methods(self) -> Set[str]:
        return {DbApiMethod.ALL.method_name}

    def notify_connection_changed(self, changes: Set[ConnectionEvent]) -> None:
        self.notified.append(set(changes))


# ---- Helpers ------------------------------------------------------------


def _host_info() -> HostInfo:
    return HostInfo(host="example.local", port=5432)


def _props() -> Properties:
    return Properties({"user": "u", "password": "p"})


def _mk_service() -> AsyncPluginServiceImpl:
    return AsyncPluginServiceImpl(_props(), FakeAsyncDriverDialect(), _host_info())


# ---- Tests --------------------------------------------------------------


def test_plugin_manager_always_appends_default_plugin_as_terminal():
    svc = _mk_service()
    mgr = AsyncPluginManager(svc, _props(), plugins=[])
    assert mgr.num_plugins == 1
    assert isinstance(mgr.plugins[-1], AsyncDefaultPlugin)


def test_plugin_manager_preserves_user_plugin_order():
    svc = _mk_service()
    a = RecorderPlugin("A", [])
    b = RecorderPlugin("B", [])
    mgr = AsyncPluginManager(svc, _props(), plugins=[a, b])
    assert mgr.plugins[0] is a
    assert mgr.plugins[1] is b
    assert isinstance(mgr.plugins[2], AsyncDefaultPlugin)


def test_connect_walks_pipeline_in_order_and_default_plugin_opens_connection():
    log: List[str] = []
    svc = _mk_service()
    dialect: FakeAsyncDriverDialect = svc.driver_dialect  # type: ignore[assignment]
    a = RecorderPlugin("A", log)
    b = RecorderPlugin("B", log)
    mgr = AsyncPluginManager(svc, _props(), plugins=[a, b])

    async def _target() -> None:
        return None

    conn = asyncio.run(
        mgr.connect(
            target_driver_func=_target,
            driver_dialect=dialect,
            host_info=_host_info(),
            props=_props(),
            is_initial_connection=True,
        )
    )

    # Both plugins saw enter before exit; A wraps B; default plugin opened the conn.
    assert log == [
        "A:connect:enter",
        "B:connect:enter",
        "B:connect:exit",
        "A:connect:exit",
    ]
    assert isinstance(conn, _FakeAsyncConnection)
    assert dialect.connect_count == 1


def test_execute_walks_pipeline_with_no_user_plugins_returns_terminal():
    """With only AsyncDefaultPlugin in the chain, execute() just runs the terminal."""
    svc = _mk_service()
    mgr = AsyncPluginManager(svc, _props(), plugins=[])

    async def _target() -> str:
        return "driver-result"

    result = asyncio.run(
        mgr.execute(
            target=object,
            method=DbApiMethod.CURSOR_EXECUTE,
            target_driver_func=_target,
        )
    )
    assert result == "driver-result"


def test_execute_walks_pipeline_in_order_for_subscribed_plugins():
    log: List[str] = []
    svc = _mk_service()
    a = RecorderPlugin("A", log)
    b = RecorderPlugin("B", log)
    mgr = AsyncPluginManager(svc, _props(), plugins=[a, b])

    async def _target() -> str:
        log.append("driver:call")
        return "rows"

    result = asyncio.run(
        mgr.execute(
            target=object,
            method=DbApiMethod.CURSOR_EXECUTE,
            target_driver_func=_target,
        )
    )
    assert result == "rows"
    # A wraps B wraps Default wraps driver; each plugin's execute method was called.
    assert log == [
        "A:execute:enter:Cursor.execute",
        "B:execute:enter:Cursor.execute",
        "driver:call",
        "B:execute:exit:Cursor.execute",
        "A:execute:exit:Cursor.execute",
    ]


def test_execute_skips_plugin_not_subscribed_to_method():
    log: List[str] = []
    svc = _mk_service()
    sub_only = SubscribedOnlyConnectPlugin(log)
    recorder = RecorderPlugin("R", log)
    mgr = AsyncPluginManager(svc, _props(), plugins=[sub_only, recorder])

    async def _target() -> str:
        log.append("driver")
        return "rows"

    asyncio.run(
        mgr.execute(
            target=object,
            method=DbApiMethod.CURSOR_EXECUTE,
            target_driver_func=_target,
        )
    )
    assert "SubOnly:execute:Cursor.execute" not in log
    assert log == [
        "R:execute:enter:Cursor.execute",
        "driver",
        "R:execute:exit:Cursor.execute",
    ]


def test_plugin_service_impl_tracks_connection_and_host_info():
    async def _body() -> None:
        dialect = FakeAsyncDriverDialect()
        svc = AsyncPluginServiceImpl(_props(), dialect, _host_info())
        assert svc.current_connection is None
        assert svc.current_host_info == _host_info()
        assert svc.driver_dialect is dialect
        assert svc.props == _props()

        # First connection: no prior, no session transfer.
        c1 = _FakeAsyncConnection("host1")
        await svc.set_current_connection(c1, HostInfo(host="host1", port=5432))
        assert svc.current_connection is c1
        assert svc.current_host_info.host == "host1"

        # Second connection: session state transferred via dialect.
        await dialect.set_read_only(c1, True)
        c2 = _FakeAsyncConnection("host2")
        await svc.set_current_connection(c2, HostInfo(host="host2", port=5432))
        assert svc.current_connection is c2
        assert await dialect.is_read_only(c2) is True

    asyncio.run(_body())


def test_notify_connection_changed_dispatches_to_all_plugins():
    svc = _mk_service()
    a = _NotifyRecorderPlugin()
    b = _NotifyRecorderPlugin()
    mgr = AsyncPluginManager(svc, _props(), plugins=[a, b])
    mgr.notify_connection_changed({ConnectionEvent.CONNECTION_OBJECT_CHANGED})
    assert a.notified == [{ConnectionEvent.CONNECTION_OBJECT_CHANGED}]
    assert b.notified == [{ConnectionEvent.CONNECTION_OBJECT_CHANGED}]


def test_set_current_connection_notifies_plugins_on_initial_and_swap():
    # The connection-changed notification is what lets the EFM plugin reset its
    # per-connection UNAVAILABLE flag after failover swaps the connection
    # (test_fail_from_reader_to_writer). Verify the swap actually fires it.
    async def _body() -> None:
        svc = _mk_service()
        rec = _NotifyRecorderPlugin()
        svc.plugin_manager = AsyncPluginManager(svc, _props(), plugins=[rec])

        c1 = _FakeAsyncConnection("host1")
        await svc.set_current_connection(c1, HostInfo(host="host1", port=5432))
        assert rec.notified == [{ConnectionEvent.INITIAL_CONNECTION}]

        c2 = _FakeAsyncConnection("host2")
        await svc.set_current_connection(c2, HostInfo(host="host2", port=5432))
        assert rec.notified[-1] == {ConnectionEvent.CONNECTION_OBJECT_CHANGED}
        assert len(rec.notified) == 2

        # Re-setting the SAME connection must NOT notify (no real change).
        await svc.set_current_connection(c2, HostInfo(host="host2", port=5432))
        assert len(rec.notified) == 2

    asyncio.run(_body())


def test_plugin_service_is_network_bound_method_delegates_to_driver_dialect():
    svc = _mk_service()
    # Default FakeAsyncDriverDialect declares "*" in network_bound_methods,
    # so every method should count as network-bound.
    assert svc.is_network_bound_method(DbApiMethod.CURSOR_EXECUTE.method_name) is True
    assert svc.is_network_bound_method("any.random.method") is True
