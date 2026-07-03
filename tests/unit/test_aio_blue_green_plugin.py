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

"""Unit tests for the async Blue/Green plugin's connect/execute dispatch.

Covers subscription, pass-through when no status is published, first-match
routing selection, the routing re-selection loop (a routing returning None
re-selects against the latest published status instead of falling straight
through), and hold-time tracking. Routing internals (suspend/substitute
timeouts, provider machinery, monitor) live in
``test_aio_blue_green_monitor.py``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.blue_green_plugin import (
    AsyncBlueGreenPlugin, AsyncBlueGreenStatusProvider, BlueGreenPhase,
    BlueGreenRole, BlueGreenStatus, ConnectRouting, PassThroughConnectRouting,
    RejectConnectRouting, SubstituteConnectRouting)
from aws_advanced_python_wrapper.aio.plugin_factory import PLUGIN_FACTORIES
from aws_advanced_python_wrapper.database_dialect import BlueGreenDialect
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def _reset_bg_singletons(monkeypatch):
    # Plugin tests exercise connect/execute dispatch, not monitor lifecycle;
    # neutralize monitor startup so eager provider init doesn't spawn tasks.
    monkeypatch.setattr(AsyncBlueGreenStatusProvider, "schedule_start", lambda self: None)
    AsyncBlueGreenPlugin._reset_for_tests()
    yield
    AsyncBlueGreenPlugin._reset_for_tests()


def _bg_service(
        network_bound=("Cursor.execute",),
        current_host_info: Optional[HostInfo] = None,
        status: Optional[BlueGreenStatus] = None):
    """A mock service whose database_dialect passes the BlueGreenDialect check,
    so the eager provider init in ``execute`` succeeds."""
    svc = _mock_service(network_bound=network_bound, current_host_info=current_host_info, status=status)
    svc.database_dialect = MagicMock(spec=BlueGreenDialect)
    return svc


def _run(coro):
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


class _NoneConnectRouting(ConnectRouting):
    """A matching routing that always releases (returns None)."""

    def is_match(self, host_info: Optional[HostInfo], role: BlueGreenRole) -> bool:
        return True

    async def apply(self, plugin: Any, host_info: HostInfo, props: Properties,
                    is_initial_connection: bool, connect_func: Any) -> Optional[Any]:
        return None


# ---- Tests -----------------------------------------------------------


def test_subscription_includes_connect_and_network_bound():
    svc = _mock_service(network_bound=("Cursor.execute", "Cursor.fetchone"))
    plugin = AsyncBlueGreenPlugin(svc, Properties())
    subs = plugin.subscribed_methods
    assert DbApiMethod.CONNECT.method_name in subs
    assert "Cursor.execute" in subs
    assert "Cursor.fetchone" in subs


def test_bg_id_defaults_to_1_when_unset():
    svc = _mock_service()
    plugin = AsyncBlueGreenPlugin(svc, Properties())
    assert plugin._bg_id == "1"


def test_bg_id_is_trimmed_and_lowercased():
    svc = _mock_service()
    plugin = AsyncBlueGreenPlugin(svc, Properties({"bg_id": "  MyBG  "}))
    assert plugin._bg_id == "mybg"


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
        is_initial_connection=False,
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
    with pytest.raises(AwsWrapperError, match="can't be opened"):
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
        bg_id="1",
        phase=BlueGreenPhase.IN_PROGRESS,
        # First routing matches SOURCE role and rejects; a later match is
        # never reached because the first is chosen.
        connect_routings=[
            RejectConnectRouting(None, BlueGreenRole.SOURCE),
            PassThroughConnectRouting(None, BlueGreenRole.SOURCE),
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


def test_connect_reselection_falls_through_when_latest_has_no_match():
    """A routing returning None re-selects against the LATEST status; when the
    latest status has no matching routing, the connect falls through to
    connect_func rather than looping on the stale routing."""
    host_info = HostInfo("h", 5432)
    status_active = BlueGreenStatus(
        bg_id="1",
        phase=BlueGreenPhase.IN_PROGRESS,
        connect_routings=[_NoneConnectRouting()],
        role_by_host={"h": BlueGreenRole.SOURCE},
    )
    status_done = BlueGreenStatus(
        bg_id="1",
        phase=BlueGreenPhase.COMPLETED,
        connect_routings=[],
        role_by_host={"h": BlueGreenRole.SOURCE},
    )
    svc = _mock_service()
    svc.get_status = MagicMock(side_effect=[status_active, status_done])
    plugin = AsyncBlueGreenPlugin(svc, Properties())

    fallback = object()
    connect_func = AsyncMock(return_value=fallback)
    result = _run(plugin.connect(
        target_driver_func=MagicMock(),
        driver_dialect=MagicMock(),
        host_info=host_info,
        props=Properties(),
        is_initial_connection=False,
        connect_func=connect_func))

    assert result is fallback
    connect_func.assert_awaited_once()


def test_connect_reselection_picks_routing_from_latest_status():
    """After a routing releases (None), re-selection uses the latest status's
    routing table -- a different, matching routing then produces the
    connection."""
    host_info = HostInfo("h", 5432)
    status_active = BlueGreenStatus(
        bg_id="1",
        phase=BlueGreenPhase.IN_PROGRESS,
        connect_routings=[_NoneConnectRouting()],
        role_by_host={"h": BlueGreenRole.SOURCE},
    )
    status_post = BlueGreenStatus(
        bg_id="1",
        phase=BlueGreenPhase.POST,
        connect_routings=[PassThroughConnectRouting(None, BlueGreenRole.SOURCE)],
        role_by_host={"h": BlueGreenRole.SOURCE},
    )
    svc = _mock_service()
    svc.get_status = MagicMock(side_effect=[status_active, status_post])
    plugin = AsyncBlueGreenPlugin(svc, Properties())

    direct = object()
    connect_func = AsyncMock(return_value=direct)
    result = _run(plugin.connect(
        target_driver_func=MagicMock(),
        driver_dialect=MagicMock(),
        host_info=host_info,
        props=Properties(),
        is_initial_connection=False,
        connect_func=connect_func))

    # PassThroughConnectRouting invokes connect_func to open the connection.
    assert result is direct
    connect_func.assert_awaited_once()
    # Hold-time was tracked across the routed connect.
    assert plugin.get_hold_time_ns() >= 0
    assert plugin._start_time_ns > 0


def test_execute_passes_through_when_no_status():
    svc = _bg_service(current_host_info=HostInfo("h1"))
    plugin = AsyncBlueGreenPlugin(svc, Properties())

    execute_func = AsyncMock(return_value="exec-ok")
    result = _run(plugin.execute(
        target=MagicMock(),
        method_name="Cursor.execute",
        execute_func=execute_func))
    assert result == "exec-ok"
    execute_func.assert_awaited_once()


def test_execute_passes_through_when_host_not_in_bg():
    host_info = HostInfo("outsider")
    status = BlueGreenStatus(
        bg_id="1",
        phase=BlueGreenPhase.IN_PROGRESS,
        role_by_host={"someone-else": BlueGreenRole.SOURCE},
    )
    svc = _bg_service(status=status, current_host_info=host_info)
    plugin = AsyncBlueGreenPlugin(svc, Properties())

    execute_func = AsyncMock(return_value="exec-ok")
    result = _run(plugin.execute(
        target=MagicMock(),
        method_name="Cursor.execute",
        execute_func=execute_func))
    assert result == "exec-ok"
    execute_func.assert_awaited_once()


def test_substitute_connect_routing_is_match_by_url():
    """Endpoint matching mirrors sync BaseRouting.is_match (host_info.url)."""
    routing = SubstituteConnectRouting(
        HostInfo("10.0.0.1", 5432), "h:5432/", BlueGreenRole.SOURCE)
    assert routing.is_match(HostInfo("h", 5432), BlueGreenRole.SOURCE) is True
    assert routing.is_match(HostInfo("other", 5432), BlueGreenRole.SOURCE) is False


def test_factory_produces_async_blue_green_plugin():
    factory = PLUGIN_FACTORIES["bg"]
    svc = _mock_service()
    plugin = factory.get_instance(svc, Properties())
    assert isinstance(plugin, AsyncBlueGreenPlugin)
    assert type(plugin).__name__ == "AsyncBlueGreenPlugin"


def test_bg_plugin_dispatches_through_set_status():
    """When BlueGreenStatus is published via plugin_service.set_status, the
    plugin finds it and dispatches through the routing table."""
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
            None, BlueGreenRole.SOURCE)],
        role_by_host={"source.example.com": BlueGreenRole.SOURCE},
    )
    svc.set_status(BlueGreenStatus, "my-bg", status)

    async def _cf():
        return MagicMock()

    with pytest.raises(AwsWrapperError):
        _run(plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=host,
            props=svc.props,
            is_initial_connection=False,
            connect_func=_cf,
        ))
