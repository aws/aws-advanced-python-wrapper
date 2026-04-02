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

import psycopg
import pytest
from _weakref import ref

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_monitoring_plugin import \
    HostMonitorService
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_monitor(mocker):
    monitor = mocker.MagicMock()
    monitor.is_stopped = False
    return monitor


@pytest.fixture
def monitor_service(mock_plugin_service, mocker, mock_monitor):
    mocker.patch(
        "aws_advanced_python_wrapper.host_monitoring_plugin.Monitor.__init__", return_value=None)
    service = HostMonitorService(mock_plugin_service)
    return service


@pytest.fixture(autouse=True)
def cleanup():
    yield
    release_resources()


def test_start_monitoring(mocker, monitor_service, mock_plugin_service, mock_monitor, mock_conn):
    aliases = frozenset({"instance-1"})

    mocker.patch.object(monitor_service._monitor_service, 'run_if_absent_with_aliases', return_value=mock_monitor)

    monitor_service.start_monitoring(
        mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    mock_monitor.start_monitoring.assert_called_once()
    assert mock_monitor == monitor_service._cached_monitor()
    assert aliases == monitor_service._cached_monitor_aliases


def test_start_monitoring__cached_monitor(
        mocker, monitor_service, mock_plugin_service, mock_monitor, mock_conn):
    aliases = frozenset({"instance-1"})
    monitor_service._cached_monitor = ref(mock_monitor)
    monitor_service._cached_monitor_aliases = aliases

    monitor_service.start_monitoring(
        mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    mock_monitor.start_monitoring.assert_called_once()
    assert mock_monitor == monitor_service._cached_monitor()
    assert aliases == monitor_service._cached_monitor_aliases


def test_start_monitoring__errors(monitor_service, mock_conn):
    with pytest.raises(AwsWrapperError):
        monitor_service.start_monitoring(
            mock_conn, frozenset(), HostInfo("instance-1"), Properties(), 5000, 1000, 3)


def test_stop_monitoring(mocker, monitor_service, mock_monitor, mock_conn):
    aliases = frozenset({"instance-1"})
    mocker.patch.object(monitor_service._monitor_service, 'run_if_absent_with_aliases', return_value=mock_monitor)

    context = monitor_service.start_monitoring(
        mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)
    monitor_service.stop_monitoring(context)
    mock_monitor.stop_monitoring.assert_called_once_with(context)


def test_stop_monitoring_host(mocker, monitor_service, mock_monitor):
    aliases = frozenset({"alias-1"})
    mocker.patch.object(monitor_service._monitor_service, 'get', return_value=mock_monitor)

    monitor_service.stop_monitoring_host(aliases)
    mock_monitor.clear_contexts.assert_called_once()
