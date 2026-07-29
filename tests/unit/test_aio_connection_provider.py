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

"""Unit tests for AsyncConnectionProvider / Manager (K.1)."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional, Tuple

import pytest

from aws_advanced_python_wrapper.aio.connection_provider import (
    AsyncConnectionProvider, AsyncConnectionProviderManager,
    AsyncDriverConnectionProvider)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _reset_manager_singleton():
    """Clear the class-level provider slot between tests."""
    AsyncConnectionProviderManager.reset_provider()
    yield
    AsyncConnectionProviderManager.reset_provider()


class _FakeDatabaseDialect:
    def prepare_conn_props(self, props):  # noqa: D401
        return props


class _FakeDriverDialect:
    def prepare_connect_info(self, host_info, props):
        return dict(props)

    async def execute_connect(self, target_func, prepared, props):
        # Mirror AsyncDriverDialect.execute_connect's default (plain connect);
        # the provider now funnels the connect through this hook.
        return await target_func(**prepared)


class _RecordingProvider(AsyncConnectionProvider):
    """Custom provider that records every call for assertion."""

    def __init__(self, accepts_hosts: bool = True,
                 accepts_strategies: Tuple[str, ...] = ("pool",)) -> None:
        self._accepts_hosts = accepts_hosts
        self._accepts_strategies = set(accepts_strategies)
        self.connect_called = 0
        self.resources_released = False

    def accepts_host_info(self, host_info: HostInfo,
                          props: Properties) -> bool:
        return self._accepts_hosts

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        return strategy in self._accepts_strategies

    def get_host_info_by_strategy(
            self, hosts: Tuple[HostInfo, ...], role: HostRole,
            strategy: str,
            props: Optional[Properties]) -> HostInfo:
        return hosts[0]

    async def connect(
            self,
            target_func: Callable[..., Awaitable[Any]],
            driver_dialect, database_dialect,
            host_info: HostInfo, props: Properties) -> Any:
        self.connect_called += 1
        return await target_func(**props)

    async def release_resources(self) -> None:
        self.resources_released = True


def _reader_host(name: str) -> HostInfo:
    """Build a READER host that selectors consider eligible."""
    host = HostInfo(name, role=HostRole.READER)
    host.set_availability(HostAvailability.AVAILABLE)
    return host


# ----- AsyncDriverConnectionProvider ------------------------------------


def test_driver_provider_accepts_all_hosts() -> None:
    provider = AsyncDriverConnectionProvider()
    host = HostInfo("h")
    assert provider.accepts_host_info(host, Properties()) is True


def test_driver_provider_accepted_strategies_view() -> None:
    mapping = AsyncDriverConnectionProvider.accepted_strategies()
    assert set(mapping.keys()) == {
        "random", "round_robin", "weighted_random", "highest_weight"}
    with pytest.raises(TypeError):
        mapping["new"] = object()  # type: ignore[index]


def test_driver_provider_rejects_unknown_strategy() -> None:
    provider = AsyncDriverConnectionProvider()
    with pytest.raises(AwsWrapperError):
        provider.get_host_info_by_strategy(
            (_reader_host("h"),), HostRole.READER,
            "not_a_real_strategy", None)


def test_driver_provider_connect_calls_target_func() -> None:
    provider = AsyncDriverConnectionProvider()
    called_with = {}

    async def target(**kwargs):
        called_with.update(kwargs)
        return object()

    host = HostInfo("h")
    props = Properties({"host": "h"})

    async def _run():
        return await provider.connect(
            target, _FakeDriverDialect(), _FakeDatabaseDialect(), host, props)

    result = asyncio.run(_run())
    assert result is not None
    assert called_with == {"host": "h"}


# ----- AsyncConnectionProviderManager -----------------------------------


def test_manager_default_provider_is_driver_provider() -> None:
    mgr = AsyncConnectionProviderManager()
    assert isinstance(mgr.default_provider, AsyncDriverConnectionProvider)


def test_manager_returns_default_when_no_custom_set() -> None:
    mgr = AsyncConnectionProviderManager()
    host = HostInfo("h")
    assert mgr.get_connection_provider(host, Properties()) is mgr.default_provider


def test_manager_prefers_custom_when_set_and_accepts() -> None:
    custom = _RecordingProvider(accepts_hosts=True)
    AsyncConnectionProviderManager.set_connection_provider(custom)
    mgr = AsyncConnectionProviderManager()
    host = HostInfo("h")
    chosen = mgr.get_connection_provider(host, Properties())
    assert chosen is custom


def test_manager_falls_back_when_custom_rejects_host() -> None:
    custom = _RecordingProvider(accepts_hosts=False)
    AsyncConnectionProviderManager.set_connection_provider(custom)
    mgr = AsyncConnectionProviderManager()
    host = HostInfo("h")
    chosen = mgr.get_connection_provider(host, Properties())
    assert chosen is mgr.default_provider


def test_manager_reset_clears_custom_provider() -> None:
    custom = _RecordingProvider()
    AsyncConnectionProviderManager.set_connection_provider(custom)
    AsyncConnectionProviderManager.reset_provider()
    mgr = AsyncConnectionProviderManager()
    assert mgr.get_connection_provider(
        HostInfo("h"), Properties()) is mgr.default_provider


def test_manager_accepts_strategy_consults_custom_first() -> None:
    custom = _RecordingProvider(accepts_strategies=("pool",))
    AsyncConnectionProviderManager.set_connection_provider(custom)
    mgr = AsyncConnectionProviderManager()
    assert mgr.accepts_strategy(HostRole.READER, "pool") is True
    # Falls through to default when custom doesn't accept.
    assert mgr.accepts_strategy(HostRole.READER, "random") is True


def test_manager_get_host_info_by_strategy_dispatches_to_custom() -> None:
    custom = _RecordingProvider(accepts_strategies=("pool",))
    AsyncConnectionProviderManager.set_connection_provider(custom)
    mgr = AsyncConnectionProviderManager()
    hosts = (_reader_host("a"), _reader_host("b"))
    # "pool" handled by custom.
    assert mgr.get_host_info_by_strategy(
        hosts, HostRole.READER, "pool", None) is hosts[0]
    # "random" falls through to the default provider (sync selector).
    picked = mgr.get_host_info_by_strategy(
        hosts, HostRole.READER, "random", None)
    assert picked in hosts


def test_manager_release_resources_awaits_async_teardown() -> None:
    custom = _RecordingProvider()
    AsyncConnectionProviderManager.set_connection_provider(custom)
    asyncio.run(AsyncConnectionProviderManager.release_resources())
    assert custom.resources_released is True


def test_manager_release_resources_noop_when_unset() -> None:
    # Should not raise even with no custom provider installed.
    asyncio.run(AsyncConnectionProviderManager.release_resources())
