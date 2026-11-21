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

from __future__ import annotations

import asyncio
from typing import Any, List, Optional
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aio.connection_lifetime_plugin import \
    AsyncConnectionLifetimePlugin
from aws_advanced_python_wrapper.errors import (ConnectionLifetimeExpiredError,
                                                ConnectionLifetimeFailedError,
                                                ConnectionLifetimeSuccessError)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties

EXECUTE = DbApiMethod.CURSOR_EXECUTE.method_name
LIFETIME_MS = 1000
MS = 1_000_000
HOST = HostInfo("instance-1.xyz.us-east-1.rds.amazonaws.com")


class AiomysqlLikeConnection:
    """close() is synchronous; ensure_closed() is the graceful coroutine."""

    def __init__(self, ensure_closed_error: Optional[Exception] = None) -> None:
        self.closed = False
        self.ensure_closed_calls = 0
        self._ensure_closed_error = ensure_closed_error

    def close(self) -> None:
        self.closed = True

    async def ensure_closed(self) -> None:
        self.ensure_closed_calls += 1
        if self._ensure_closed_error is not None:
            raise self._ensure_closed_error
        self.close()


class PsycopgLikeConnection:
    """close() is a coroutine."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeClock:
    def __init__(self) -> None:
        self.now_ns = 0

    def __call__(self) -> int:
        return self.now_ns

    def advance_ms(self, ms: int) -> None:
        self.now_ns += ms * MS


class FakeDriverDialect:
    async def is_closed(self, connection: Any) -> bool:
        return bool(connection.closed)


class FakeAsyncPluginService:
    def __init__(self) -> None:
        self.network_bound_methods = {EXECUTE, DbApiMethod.CONNECTION_CLOSE.method_name}
        self.current_connection: Optional[Any] = None
        self.is_in_transaction = False
        self.initial_connection_host_info: Optional[HostInfo] = None
        self.replacements: List[Any] = []
        self.connect_hosts: List[HostInfo] = []
        self.connect_error: Optional[Exception] = None
        self.driver_dialect = FakeDriverDialect()
        self.preserve_previous = False
        self.install_error: Optional[Exception] = None
        # Each evicted connection with whether it was already closed at the time.
        self.evicted: List[Any] = []
        self.connection_provider_manager = MagicMock()
        self.connection_provider_manager.evict_connection.side_effect = \
            lambda conn: self.evicted.append((conn, bool(conn.closed)))

    def get_connection_provider_manager(self) -> Any:
        return self.connection_provider_manager

    def is_network_bound_method(self, method_name: str) -> bool:
        return method_name in self.network_bound_methods

    async def connect(self, host_info: HostInfo, props: Properties, plugin_to_skip: Any) -> Any:
        self.connect_hosts.append(host_info)
        if self.connect_error is not None:
            raise self.connect_error
        return self.replacements.pop(0)

    async def set_current_connection(self, connection: Any, host_info: HostInfo) -> None:
        previous = self.current_connection
        self.current_connection = connection
        if previous is not None and previous is not connection and not self.preserve_previous:
            result = previous.close()
            if asyncio.iscoroutine(result):
                await result
        if self.install_error is not None:
            raise self.install_error


def make_plugin(plugin_service, clock, action: str = "reconnect") -> AsyncConnectionLifetimePlugin:
    props = Properties({
        "connection_lifetime_ms": str(LIFETIME_MS),
        "connection_lifetime_action": action,
    })
    return AsyncConnectionLifetimePlugin(plugin_service, props, clock_ns=clock)


async def open_connection(plugin, plugin_service, connection) -> None:
    async def connect_func() -> Any:
        return connection

    await plugin.connect(None, None, HOST, Properties(), True, connect_func)
    plugin_service.current_connection = connection
    plugin.notify_connection_changed(set())


async def execute(plugin, result: str = "rows") -> Any:
    async def execute_func() -> str:
        return result

    return await plugin.execute(None, EXECUTE, execute_func)


def test_connection_before_lifetime_is_used():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock, "close")
        connection = PsycopgLikeConnection()
        await open_connection(plugin, plugin_service, connection)

        clock.advance_ms(LIFETIME_MS - 1)

        assert await execute(plugin) == "rows"
        assert not connection.closed

    asyncio.run(_body())


def test_close_action_uses_ensure_closed_for_aiomysql():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock, "close")
        connection = AiomysqlLikeConnection()
        await open_connection(plugin, plugin_service, connection)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeExpiredError):
            await execute(plugin)

        assert connection.ensure_closed_calls == 1
        assert connection.closed

    asyncio.run(_body())


def test_close_action_closes_socket_when_ensure_closed_fails():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock, "close")
        quit_error = ConnectionResetError("peer reset while sending COM_QUIT")
        connection = AiomysqlLikeConnection(ensure_closed_error=quit_error)
        await open_connection(plugin, plugin_service, connection)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeExpiredError) as raised:
            await execute(plugin)

        assert raised.value.__cause__ is quit_error
        assert connection.closed

    asyncio.run(_body())


def test_close_action_awaits_psycopg_close():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock, "close")
        connection = PsycopgLikeConnection()
        await open_connection(plugin, plugin_service, connection)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeExpiredError):
            await execute(plugin)

        assert connection.closed

    asyncio.run(_body())


def test_close_action_evicts_before_closing():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock, "close")
        connection = PsycopgLikeConnection()
        await open_connection(plugin, plugin_service, connection)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeExpiredError):
            await execute(plugin)

        assert plugin_service.evicted == [(connection, False)]
        assert connection.closed

    asyncio.run(_body())


def test_close_action_on_mock_connection_calls_close():
    # A MagicMock answers every attribute, including ensure_closed; the plugin
    # must not mistake that for aiomysql's coroutine.
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin_service.driver_dialect = MagicMock()
        plugin = make_plugin(plugin_service, clock, "close")
        connection = MagicMock()
        await open_connection(plugin, plugin_service, connection)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeExpiredError):
            await execute(plugin)

        connection.close.assert_called_once()
        connection.ensure_closed.assert_not_called()

    asyncio.run(_body())


def test_closed_wrapper_connection_stays_closed():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock, "close")
        await open_connection(plugin, plugin_service, PsycopgLikeConnection())
        clock.advance_ms(LIFETIME_MS)
        with pytest.raises(ConnectionLifetimeExpiredError):
            await execute(plugin)

        async def must_not_run() -> None:
            raise AssertionError("execute_func ran on a closed wrapper connection")

        with pytest.raises(ConnectionLifetimeExpiredError):
            await plugin.execute(None, EXECUTE, must_not_run)

    asyncio.run(_body())


def test_action_waits_for_open_transaction():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock, "close")
        connection = PsycopgLikeConnection()
        await open_connection(plugin, plugin_service, connection)
        clock.advance_ms(LIFETIME_MS)

        plugin_service.is_in_transaction = True
        assert await execute(plugin) == "rows"
        assert not connection.closed

        plugin_service.is_in_transaction = False
        with pytest.raises(ConnectionLifetimeExpiredError):
            await execute(plugin)

    asyncio.run(_body())


def test_reconnect_action_replaces_connection_on_original_host():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock)
        first = PsycopgLikeConnection()
        replacement = PsycopgLikeConnection()
        plugin_service.replacements.append(replacement)
        await open_connection(plugin, plugin_service, first)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeSuccessError):
            await execute(plugin)

        assert plugin_service.connect_hosts == [HOST]
        assert plugin_service.current_connection is replacement
        assert first.closed

        clock.advance_ms(LIFETIME_MS - 1)
        assert await execute(plugin) == "rows"

    asyncio.run(_body())


def test_reconnect_evicts_superseded_connection_before_closing():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock)
        first = PsycopgLikeConnection()
        plugin_service.replacements.append(PsycopgLikeConnection())
        await open_connection(plugin, plugin_service, first)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeSuccessError):
            await execute(plugin)

        assert plugin_service.evicted == [(first, False)]
        assert first.closed

    asyncio.run(_body())


def test_reconnect_leaves_preserved_connection_open():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock)
        first = PsycopgLikeConnection()
        plugin_service.replacements.append(PsycopgLikeConnection())
        plugin_service.preserve_previous = True
        await open_connection(plugin, plugin_service, first)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeSuccessError):
            await execute(plugin)

        # Eviction only marks the connection; the plugin that preserved it can keep using it.
        assert plugin_service.evicted == [(first, False)]
        assert not first.closed

    asyncio.run(_body())


def test_terminal_reconnect_failure_evicts_only_previous_connection():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock)
        first = PsycopgLikeConnection()
        replacement = PsycopgLikeConnection()
        plugin_service.replacements.append(replacement)
        plugin_service.install_error = RuntimeError("session state could not be applied")
        await open_connection(plugin, plugin_service, first)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeFailedError):
            await execute(plugin)

        assert plugin_service.evicted == [(first, False)]
        assert first.closed
        assert replacement.closed
        with pytest.raises(ConnectionLifetimeExpiredError):
            await execute(plugin)

    asyncio.run(_body())


def test_failed_reconnect_keeps_previous_connection():
    async def _body() -> None:
        plugin_service, clock = FakeAsyncPluginService(), FakeClock()
        plugin = make_plugin(plugin_service, clock)
        first = PsycopgLikeConnection()
        connect_error = RuntimeError("host unreachable")
        plugin_service.connect_error = connect_error
        await open_connection(plugin, plugin_service, first)
        clock.advance_ms(LIFETIME_MS)

        with pytest.raises(ConnectionLifetimeFailedError) as raised:
            await execute(plugin)

        assert raised.value.__cause__ is connect_error
        assert plugin_service.current_connection is first
        assert not first.closed
        assert plugin_service.evicted == []

    asyncio.run(_body())
