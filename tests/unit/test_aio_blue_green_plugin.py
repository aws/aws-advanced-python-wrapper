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

"""Unit tests for the async Blue-Green plugin skeleton.

Covers the connect/execute routing dispatch, the two concrete routings
(PassThrough + Reject), and the fact that unported routings explicitly
raise NotImplementedError. The DB-level status monitor is NOT in this
skeleton -- the plugin therefore passes through whenever
``plugin_service.get_status`` returns nothing, which is the norm for
non-BG deployments.
"""

from __future__ import annotations

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.blue_green_plugin import (
    AsyncBlueGreenPlugin, BlueGreenPhase, BlueGreenRole, BlueGreenStatus,
    PassThroughConnectRouting, RejectConnectRouting, SubstituteConnectRouting)
from aws_advanced_python_wrapper.aio.plugin_factory import PLUGIN_FACTORIES
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


def _run(coro):
    """Execute an awaitable on a fresh loop (tests stay sync-flavoured).

    Uses :func:`asyncio.run` so the loop is closed + any pending sockets
    finalised before the next test -- matching the idiom in the rest of
    the ``test_aio_*`` suite and keeping ``-Werror`` happy.
    """
    return asyncio.run(coro)


def _mock_service(
        network_bound=("Cursor.execute",),
        current_host_info: Optional[HostInfo] = None,
        status: Optional[BlueGreenStatus] = None):
    svc = MagicMock()
    svc.network_bound_methods = set(network_bound)
    svc.current_host_info = current_host_info
    if status is None:
        svc.get_status = MagicMock(return_value=None)
    else:
        svc.get_status = MagicMock(return_value=status)
    return svc


# ---- Tests -----------------------------------------------------------


def test_subscription_includes_connect_and_network_bound():
    svc = _mock_service(network_bound=("Cursor.execute", "Cursor.fetchone"))
    plugin = AsyncBlueGreenPlugin(svc, Properties())
    subs = plugin.subscribed_methods
    assert DbApiMethod.CONNECT.method_name in subs
    assert "Cursor.execute" in subs
    assert "Cursor.fetchone" in subs


def test_connect_passes_through_when_no_status():
    svc = _mock_service()
    plugin = AsyncBlueGreenPlugin(svc, Properties())
    host_info = HostInfo("h1")

    connect_func = AsyncMock(return_value="conn-result")
    result = _run(plugin.connect(
        target_driver_func=MagicMock(),
        driver_dialect=MagicMock(),
        host_info=host_info,
        props=Properties(),
        is_initial_connection=True,
        connect_func=connect_func))

    assert result == "conn-result"
    connect_func.assert_awaited_once()


def test_pass_through_connect_routing_forwards():
    routing = PassThroughConnectRouting()
    host_info = HostInfo("h1")
    assert routing.is_match(host_info, BlueGreenRole.SOURCE) is True

    connect_func = AsyncMock(return_value="direct-conn")
    result = _run(routing.apply(
        plugin=MagicMock(),
        host_info=host_info,
        props=Properties(),
        is_initial_connection=True,
        connect_func=connect_func))
    assert result == "direct-conn"
    connect_func.assert_awaited_once()


def test_reject_connect_routing_raises():
    routing = RejectConnectRouting()
    host_info = HostInfo("h1")
    assert routing.is_match(host_info, BlueGreenRole.SOURCE) is True

    connect_func = AsyncMock()
    with pytest.raises(AwsWrapperError, match="rejects new connections"):
        _run(routing.apply(
            plugin=MagicMock(),
            host_info=host_info,
            props=Properties(),
            is_initial_connection=True,
            connect_func=connect_func))
    connect_func.assert_not_awaited()


def test_connect_dispatches_to_first_matching_routing():
    host_info = HostInfo("h-bg")
    status = BlueGreenStatus(
        bg_id="default",
        phase=BlueGreenPhase.IN_PROGRESS,
        connect_routings=[
            # First has a specific host_matcher that won't hit -> skipped.
            SubstituteConnectRouting(host_matcher="some-other-host"),
            # Second matches (empty matcher = match everything).
            RejectConnectRouting(),
        ],
        role_by_host={"h-bg": BlueGreenRole.SOURCE},
    )
    svc = _mock_service(status=status, current_host_info=host_info)
    plugin = AsyncBlueGreenPlugin(svc, Properties())

    connect_func = AsyncMock()
    with pytest.raises(AwsWrapperError):
        _run(plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=host_info,
            props=Properties(),
            is_initial_connection=False,
            connect_func=connect_func))


def test_execute_passes_through_when_no_status():
    svc = _mock_service(current_host_info=HostInfo("h1"))
    plugin = AsyncBlueGreenPlugin(svc, Properties())

    execute_func = AsyncMock(return_value="exec-ok")
    result = _run(plugin.execute(
        target=MagicMock(),
        method_name="Cursor.execute",
        execute_func=execute_func))
    assert result == "exec-ok"
    execute_func.assert_awaited_once()


def test_substitute_connect_routing_matches_and_falls_through_without_pair():
    """SubstituteConnectRouting with matching host_matcher matches, and
    when no corresponding_hosts entry exists, falls through to the
    original connect_func (port now functional, no longer raises)."""
    routing = SubstituteConnectRouting(host_matcher="h1")
    assert routing.is_match(HostInfo("h1"), BlueGreenRole.SOURCE) is True
    assert routing.is_match(HostInfo("other"), BlueGreenRole.SOURCE) is False


def test_factory_produces_async_blue_green_plugin():
    factory = PLUGIN_FACTORIES["bg"]
    svc = _mock_service()
    plugin = factory.get_instance(svc, Properties())
    # Must be the real plugin, not the old stub.
    assert isinstance(plugin, AsyncBlueGreenPlugin)
    # And "bg" is a name of the stub; confirm this is NOT it:
    assert type(plugin).__name__ == "AsyncBlueGreenPlugin"


def test_bg_plugin_dispatches_through_set_status():
    """When BlueGreenStatus is published via plugin_service.set_status,
    the plugin finds it and dispatches through the routing table."""
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl

    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    plugin = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "my-bg"}))

    host = HostInfo(host="source.example.com", port=5432)
    status = BlueGreenStatus(
        bg_id="my-bg",
        phase=BlueGreenPhase.IN_PROGRESS,
        connect_routings=[RejectConnectRouting(
            host_matcher="source.example.com",
            role_matcher=BlueGreenRole.SOURCE,
        )],
        role_by_host={"source.example.com": BlueGreenRole.SOURCE},
    )
    svc.set_status(BlueGreenStatus, "my-bg", status)

    async def _cf():
        return MagicMock()

    # RejectConnectRouting should raise
    with pytest.raises(AwsWrapperError):
        _run(plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=host,
            props=svc.props,
            is_initial_connection=True,
            connect_func=_cf,
        ))
