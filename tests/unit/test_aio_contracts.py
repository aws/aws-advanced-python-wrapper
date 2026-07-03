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

"""Acid test for F3-B SP-0 contracts.

Purpose: prove that ``AsyncDriverDialect`` exposes enough surface that a
plugin can be written without importing any driver-specific module (psycopg,
aiomysql, etc.). The test drives every abstract method on the ABC through
a fake plugin and fake driver dialect. If this test can't be satisfied
without leaking driver-specific access into the plugin, the ABC is
incomplete -- fix it HERE before SP-1+ write real code against a leaky
interface.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Set

import pytest

from aws_advanced_python_wrapper.aio import AsyncAwsWrapperConnection
from aws_advanced_python_wrapper.aio.driver_dialect import AsyncDriverDialect
from aws_advanced_python_wrapper.aio.plugin import (AsyncConnectionProvider,
                                                    AsyncPlugin)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


class _FakeAsyncConnection:
    """Stand-in for a driver-specific async connection object."""

    def __init__(self, host: str):
        self.host = host
        self.closed = False


class FakeAsyncDriverDialect(AsyncDriverDialect):
    """Concrete ABC implementation for contract testing.

    All abstract methods are implemented with canned return values and a
    call log so the test can assert the plugin exercised every op.
    """

    _dialect_code = "fake"
    _driver_name = "FakeAsync"

    def __init__(self) -> None:
        self.calls: List[str] = []
        self._read_only_state: Dict[int, bool] = {}
        self._autocommit_state: Dict[int, bool] = {}
        self._in_tx_state: Dict[int, bool] = {}
        self._closed_state: Dict[int, bool] = {}

    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            connect_func: Callable[..., Awaitable[Any]]) -> _FakeAsyncConnection:
        self.calls.append("connect")
        return _FakeAsyncConnection(host_info.host)

    async def is_closed(self, conn: Any) -> bool:
        self.calls.append("is_closed")
        return self._closed_state.get(id(conn), False)

    async def abort_connection(self, conn: Any) -> None:
        self.calls.append("abort_connection")
        self._closed_state[id(conn)] = True

    async def is_in_transaction(self, conn: Any) -> bool:
        self.calls.append("is_in_transaction")
        return self._in_tx_state.get(id(conn), False)

    async def get_autocommit(self, conn: Any) -> bool:
        self.calls.append("get_autocommit")
        return self._autocommit_state.get(id(conn), True)

    async def set_autocommit(self, conn: Any, autocommit: bool) -> None:
        self.calls.append("set_autocommit")
        self._autocommit_state[id(conn)] = autocommit

    async def is_read_only(self, conn: Any) -> bool:
        self.calls.append("is_read_only")
        return self._read_only_state.get(id(conn), False)

    async def set_read_only(self, conn: Any, read_only: bool) -> None:
        self.calls.append("set_read_only")
        self._read_only_state[id(conn)] = read_only

    async def can_execute_query(self, conn: Any) -> bool:
        self.calls.append("can_execute_query")
        return not self._closed_state.get(id(conn), False)

    async def transfer_session_state(self, from_conn: Any, to_conn: Any) -> None:
        self.calls.append("transfer_session_state")
        self._autocommit_state[id(to_conn)] = self._autocommit_state.get(
            id(from_conn), True
        )
        self._read_only_state[id(to_conn)] = self._read_only_state.get(
            id(from_conn), False
        )

    async def ping(self, conn: Any) -> bool:
        self.calls.append("ping")
        return not self._closed_state.get(id(conn), False)


class FakeAsyncPlugin(AsyncPlugin):
    """Uses ONLY methods on ``AsyncDriverDialect``.

    If any line below requires ``import psycopg`` / ``import aiomysql`` /
    direct access to a driver-specific attribute, the ABC is leaky and
    needs expansion.
    """

    @property
    def subscribed_methods(self) -> Set[str]:
        return {"connect"}

    async def exercise_entire_abc(
            self,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties) -> _FakeAsyncConnection:
        """Drive every AsyncDriverDialect method via the ABC."""
        # Static helpers
        assert driver_dialect.driver_name == "FakeAsync"
        assert driver_dialect.dialect_code == "fake"
        assert isinstance(driver_dialect.network_bound_methods, set)
        assert driver_dialect.supports_connect_timeout() is False
        assert driver_dialect.supports_socket_timeout() is False
        assert driver_dialect.supports_tcp_keepalive() is False
        assert driver_dialect.supports_abort_connection() is False
        assert driver_dialect.is_dialect(lambda: None) is True
        prepared = driver_dialect.prepare_connect_info(host_info, props)
        assert prepared["host"] == host_info.host

        # Open + probe + state management
        conn = await driver_dialect.connect(host_info, prepared, _unused_connect_func)
        assert await driver_dialect.is_closed(conn) is False
        assert await driver_dialect.is_in_transaction(conn) is False
        assert await driver_dialect.can_execute_query(conn) is True
        assert await driver_dialect.ping(conn) is True
        assert await driver_dialect.get_autocommit(conn) is True
        await driver_dialect.set_autocommit(conn, False)
        assert await driver_dialect.get_autocommit(conn) is False
        assert await driver_dialect.is_read_only(conn) is False
        await driver_dialect.set_read_only(conn, True)
        assert await driver_dialect.is_read_only(conn) is True

        # Session transfer between two connections
        other = await driver_dialect.connect(host_info, prepared, _unused_connect_func)
        await driver_dialect.transfer_session_state(conn, other)
        assert await driver_dialect.get_autocommit(other) is False
        assert await driver_dialect.is_read_only(other) is True

        # Abort
        await driver_dialect.abort_connection(conn)
        assert await driver_dialect.is_closed(conn) is True
        assert await driver_dialect.can_execute_query(conn) is False

        # Unwrap + get_connection_from_obj are sync helpers
        assert driver_dialect.unwrap_connection(conn) is conn

        class _Holder:
            def __init__(self, c: _FakeAsyncConnection) -> None:
                self.connection = c

        assert driver_dialect.get_connection_from_obj(_Holder(conn)) is conn
        assert driver_dialect.get_connection_from_obj(object()) is None

        return conn


async def _unused_connect_func() -> None:
    """Placeholder connect_func for ABC.connect signature; FakeAsyncDriverDialect
    doesn't delegate to it."""
    return None


# ---- Tests --------------------------------------------------------------


def _host_info() -> HostInfo:
    return HostInfo(host="example.cluster.local", port=5432)


def _props() -> Properties:
    return Properties({"user": "john", "password": "pwd", "dbname": "db"})


def test_fake_plugin_exercises_every_abc_method():
    """Acid test: a fake plugin drives the full ABC with no driver-specific imports."""
    dialect = FakeAsyncDriverDialect()
    plugin = FakeAsyncPlugin()

    async def _body() -> _FakeAsyncConnection:
        return await plugin.exercise_entire_abc(dialect, _host_info(), _props())

    conn = asyncio.run(_body())
    assert isinstance(conn, _FakeAsyncConnection)

    # Every abstract method must have been invoked
    required_ops = {
        "connect",
        "is_closed",
        "abort_connection",
        "is_in_transaction",
        "get_autocommit",
        "set_autocommit",
        "is_read_only",
        "set_read_only",
        "can_execute_query",
        "transfer_session_state",
        "ping",
    }
    missing = required_ops - set(dialect.calls)
    assert not missing, (
        f"AsyncDriverDialect ABC ops not exercised by fake plugin: {missing}. "
        "Either the plugin is incomplete, or the ABC is missing an op."
    )


def test_async_driver_dialect_is_abstract():
    """AsyncDriverDialect cannot be instantiated directly."""
    with pytest.raises(TypeError):
        AsyncDriverDialect()  # type: ignore[abstract]


def test_async_plugin_is_abstract():
    """AsyncPlugin cannot be instantiated directly (subscribed_methods is abstract)."""
    with pytest.raises(TypeError):
        AsyncPlugin()  # type: ignore[abstract]


def test_async_plugin_default_behaviors():
    """AsyncPlugin.connect / force_connect / execute defaults are pass-through."""

    class _Trivial(AsyncPlugin):
        @property
        def subscribed_methods(self) -> Set[str]:
            return set()

    p = _Trivial()
    # The default implementations just await the pass-through callable. Verify
    # they are coroutines (not sync methods) -- critical for the parallel
    # hierarchy decision (SP-0 D3).
    import inspect

    assert inspect.iscoroutinefunction(p.connect)
    assert inspect.iscoroutinefunction(p.force_connect)
    assert inspect.iscoroutinefunction(p.execute)


def test_async_connection_provider_is_protocol():
    """AsyncConnectionProvider is a structural Protocol."""
    from typing import get_type_hints  # noqa: F401

    # Class exists, is a Protocol, has the expected methods.
    assert hasattr(AsyncConnectionProvider, "accepts_host_info")
    assert hasattr(AsyncConnectionProvider, "accepts_strategy")
    assert hasattr(AsyncConnectionProvider, "get_host_info_by_strategy")
    assert hasattr(AsyncConnectionProvider, "connect")


def test_async_aws_wrapper_connection_rejects_missing_target():
    """AsyncAwsWrapperConnection.connect requires the target driver callable.

    Regression guard (adapted from SP-0 where all methods were stubs):
    SP-2 implements the class, but the contract that ``target`` must be a
    callable still holds.
    """
    from aws_advanced_python_wrapper.errors import AwsWrapperError

    async def _body() -> None:
        with pytest.raises(AwsWrapperError):
            await AsyncAwsWrapperConnection.connect()
        with pytest.raises(AwsWrapperError):
            await AsyncAwsWrapperConnection.connect("not-a-callable")

    asyncio.run(_body())


def test_async_driver_dialect_prepare_connect_info_strips_wrapper_props():
    """AsyncDriverDialect.prepare_connect_info is pure-sync and shared logic."""
    from aws_advanced_python_wrapper.utils.properties import WrapperProperties

    dialect = FakeAsyncDriverDialect()
    props = Properties({"user": "u", "password": "p"})
    WrapperProperties.PLUGINS.set(props, "failover")

    prepared = dialect.prepare_connect_info(
        HostInfo(host="h.example.com", port=5432), props
    )
    assert prepared["host"] == "h.example.com"
    assert prepared["port"] == "5432"
    assert WrapperProperties.PLUGINS.name not in prepared
    assert prepared.get("user") == "u"
