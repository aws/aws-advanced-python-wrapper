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

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_monitoring_plugin import (
    MonitoringThreadContainer, MonitorService)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_thread_container(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_monitor(mocker):
    monitor = mocker.MagicMock()
    monitor.is_stopped = False
    return monitor


@pytest.fixture
def thread_container():
    return MonitoringThreadContainer()


@pytest.fixture
def monitor_service_mocked_container(mock_plugin_service, mock_thread_container):
    service = MonitorService(mock_plugin_service)
    service._monitor_container = mock_thread_container
    return service


@pytest.fixture
def monitor_service_with_container(mock_plugin_service, thread_container):
    service = MonitorService(mock_plugin_service)
    service._monitor_container = thread_container
    return service


@pytest.fixture(autouse=True)
def setup_teardown(mocker, mock_thread_container, mock_plugin_service, mock_monitor):
    mock_thread_container.get_or_create_monitor.return_value = mock_monitor
    mocker.patch(
        "aws_advanced_python_wrapper.host_monitoring_plugin.MonitorService._create_monitor", return_value=mock_monitor)

    yield

    while MonitoringThreadContainer._instance is not None:
        MonitoringThreadContainer.clean_up()


def test_start_monitoring(
        monitor_service_mocked_container,
        mock_plugin_service,
        mock_monitor,
        mock_conn,
        mock_thread_container):
    aliases = frozenset({"instance-1"})

    monitor_service_mocked_container.start_monitoring(
        mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    mock_monitor.start_monitoring.assert_called_once()
    assert mock_monitor == monitor_service_mocked_container._cached_monitor()
    assert aliases == monitor_service_mocked_container._cached_monitor_aliases


def test_start_monitoring__multiple_calls(monitor_service_with_container, mock_monitor, mock_conn, mocker):
    aliases = frozenset({"instance-1"})

    # Mock ThreadPoolContainer.get_thread_pool
    mock_thread_pool = mocker.MagicMock()
    mocker.patch('aws_advanced_python_wrapper.host_monitoring_plugin.ThreadPoolContainer.get_thread_pool', return_value=mock_thread_pool)

    num_calls = 5
    for _ in range(num_calls):
        monitor_service_with_container.start_monitoring(
            mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    assert num_calls == mock_monitor.start_monitoring.call_count
    mock_thread_pool.submit.assert_called_once_with(mock_monitor.run)
    assert mock_monitor == monitor_service_with_container._cached_monitor()
    assert aliases == monitor_service_with_container._cached_monitor_aliases


def test_start_monitoring__cached_monitor(
        monitor_service_mocked_container, mock_plugin_service, mock_monitor, mock_conn, mock_thread_container):
    aliases = frozenset({"instance-1"})
    monitor_service_mocked_container._cached_monitor = ref(mock_monitor)
    monitor_service_mocked_container._cached_monitor_aliases = aliases

    monitor_service_mocked_container.start_monitoring(
        mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    mock_plugin_service.get_dialect.assert_not_called()
    mock_thread_container.get_or_create_monitor.assert_not_called()
    mock_monitor.start_monitoring.assert_called_once()
    assert mock_monitor == monitor_service_mocked_container._cached_monitor()
    assert aliases == monitor_service_mocked_container._cached_monitor_aliases


def test_start_monitoring__errors(monitor_service_mocked_container, mock_conn, mock_plugin_service):
    with pytest.raises(AwsWrapperError):
        monitor_service_mocked_container.start_monitoring(
            mock_conn, frozenset(), HostInfo("instance-1"), Properties(), 5000, 1000, 3)


def test_stop_monitoring(monitor_service_with_container, mock_monitor, mock_conn):
    aliases = frozenset({"instance-1"})
    context = monitor_service_with_container.start_monitoring(
            mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)
    monitor_service_with_container.stop_monitoring(context)
    mock_monitor.stop_monitoring.assert_called_once_with(context)


def test_stop_monitoring__multiple_calls(monitor_service_with_container, mock_monitor, mock_conn):
    aliases = frozenset({"instance-1"})
    context = monitor_service_with_container.start_monitoring(
            mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)
    monitor_service_with_container.stop_monitoring(context)
    mock_monitor.stop_monitoring.assert_called_once_with(context)
    monitor_service_with_container.stop_monitoring(context)
    assert 2 == mock_monitor.stop_monitoring.call_count


def test_stop_monitoring_host_connections(mocker, monitor_service_with_container, thread_container):
    aliases1 = frozenset({"alias-1"})
    aliases2 = frozenset({"alias-2"})
    mock_monitor1 = mocker.MagicMock()
    mock_monitor2 = mocker.MagicMock()
    thread_container.get_or_create_monitor(aliases1, lambda: mock_monitor1)
    thread_container.get_or_create_monitor(aliases2, lambda: mock_monitor2)

    monitor_service_with_container.stop_monitoring_host(aliases1)
    mock_monitor1.clear_contexts.assert_called_once()

    monitor_service_with_container.stop_monitoring_host(aliases2)
    mock_monitor2.clear_contexts.assert_called_once()
