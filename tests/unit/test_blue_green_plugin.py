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

from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.blue_green_plugin import (
    BlueGreenPhase, BlueGreenPlugin, BlueGreenRole, BlueGreenStatus,
    ConnectRouting, ExecuteRouting, PassThroughConnectRouting)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.value_container import ValueContainer


@pytest.fixture(autouse=True)
def _clear_status_providers():
    BlueGreenPlugin._status_providers.clear()
    yield
    BlueGreenPlugin._status_providers.clear()


class _NoneConnectRouting(ConnectRouting):
    """Matches every host and never produces a connection, like a suspend routing whose wait ended."""

    def __init__(self):
        self.calls = 0

    def is_match(self, host_info: Optional[HostInfo], role: BlueGreenRole) -> bool:
        return True

    def apply(self, plugin, host_info, props, is_initial_connection, connect_func, plugin_service):
        self.calls += 1
        return None


class _EmptyExecuteRouting(ExecuteRouting):
    """Matches every host and never produces a result."""

    def __init__(self):
        self.calls = 0

    def is_match(self, host_info: Optional[HostInfo], role: BlueGreenRole) -> bool:
        return True

    def apply(self, plugin, plugin_service, props, target, method_name, execute_func,
              *args: Any, **kwargs: Any) -> ValueContainer[Any]:
        self.calls += 1
        return ValueContainer.empty()


def _status(phase=BlueGreenPhase.IN_PROGRESS, connect_routings=None, execute_routings=None, host="h"):
    return BlueGreenStatus(
        "1", phase, connect_routings, execute_routings, {host: BlueGreenRole.SOURCE})


def _plugin(statuses, current_host_info: Optional[HostInfo] = None):
    svc = MagicMock()
    svc.network_bound_methods = {"Cursor.execute"}
    svc.current_host_info = current_host_info
    # A bounded side_effect makes a regression fail with StopIteration instead of looping forever.
    svc.get_status = MagicMock(side_effect=statuses)
    plugin = BlueGreenPlugin(svc, Properties())
    plugin._provider_supplier = lambda *args: MagicMock()
    return plugin


def test_blue_green_log_message_keys_resolve():
    """Regression (parity review): both Blue/Green keys referenced by logger
    calls exist in the bundle. The formatted
    WaitConnectUntilCorrespondingHostFound call previously raised
    NotInResourceBundleError whenever DEBUG logging was enabled during a
    switchover (Logger's formatted path has no missing-key guard)."""
    formatted = Messages.get_formatted(
        "SuspendConnectRouting.WaitConnectUntilCorrespondingHostFound", "host-1")
    assert "host-1" in formatted
    assert Messages.get("BlueGreenStatusProvider.AllGreenHostsChangedName")


def test_routing_is_match_by_host_and_port():
    """Routing endpoints are "host" or "host:port"; host_info.url ends with "/" and must not be compared."""
    with_port = PassThroughConnectRouting("h:5432", BlueGreenRole.SOURCE)
    assert with_port.is_match(HostInfo("h", 5432), BlueGreenRole.SOURCE) is True
    assert with_port.is_match(HostInfo("H", 5432), BlueGreenRole.SOURCE) is True
    assert with_port.is_match(HostInfo("h", 5433), BlueGreenRole.SOURCE) is False
    assert with_port.is_match(HostInfo("h", 5432), BlueGreenRole.TARGET) is False

    host_only = PassThroughConnectRouting("h", BlueGreenRole.SOURCE)
    assert host_only.is_match(HostInfo("h"), BlueGreenRole.SOURCE) is True

    assert PassThroughConnectRouting("h:5432/", BlueGreenRole.SOURCE).is_match(
        HostInfo("h", 5432), BlueGreenRole.SOURCE) is False


def test_connect_reselects_from_latest_status_and_falls_through():
    """Once the published status has no matching routing, connect stops retrying the stale routing."""
    stale = _NoneConnectRouting()
    status_active = _status(connect_routings=[stale])
    status_done = _status(phase=BlueGreenPhase.COMPLETED)
    plugin = _plugin([status_active, status_done])

    conn = MagicMock()
    connect_func = MagicMock(return_value=conn)
    result = plugin.connect(MagicMock(), MagicMock(), HostInfo("h", 5432), Properties(), False, connect_func)

    assert result is conn
    assert stale.calls == 1
    connect_func.assert_called_once()


def test_connect_reselects_routing_from_latest_status():
    stale = _NoneConnectRouting()
    status_active = _status(connect_routings=[stale])
    status_next = _status(phase=BlueGreenPhase.POST, connect_routings=[PassThroughConnectRouting(None, None)])
    plugin = _plugin([status_active, status_next])

    conn = MagicMock()
    connect_func = MagicMock(return_value=conn)
    result = plugin.connect(MagicMock(), MagicMock(), HostInfo("h", 5432), Properties(), False, connect_func)

    assert result is conn
    assert stale.calls == 1
    assert plugin._bg_status is status_next


def test_execute_reselects_from_latest_status_and_falls_through():
    stale = _EmptyExecuteRouting()
    status_active = _status(execute_routings=[stale])
    status_done = _status(phase=BlueGreenPhase.COMPLETED)
    plugin = _plugin([status_active, status_done], current_host_info=HostInfo("h", 5432))

    execute_func = MagicMock(return_value="exec-ok")
    result = plugin.execute(MagicMock(), "Cursor.execute", execute_func)

    assert result == "exec-ok"
    assert stale.calls == 1
    execute_func.assert_called_once()
