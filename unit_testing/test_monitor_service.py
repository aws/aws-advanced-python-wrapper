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

import pytest

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.host_monitoring_plugin import (MonitorService,
                                                MonitorThreadContainer)
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.utils.properties import Properties


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_thread_container(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_monitor(mocker, mock_plugin_service):
    return mocker.MagicMock()


@pytest.fixture
def monitor_service(mock_plugin_service):
    return MonitorService(mock_plugin_service)


@pytest.fixture(autouse=True)
def mock_default_behavior(monitor_service, mock_thread_container, mock_plugin_service, mock_dialect, mock_monitor):
    monitor_service._thread_container = mock_thread_container
    mock_thread_container.get_or_create_monitor.return_value = mock_monitor
    mock_plugin_service.dialect = mock_dialect


def test_start_monitoring(
        mocker, monitor_service, mock_plugin_service, mock_monitor, mock_conn, mock_dialect, mock_thread_container):
    mock_dialect_property = mocker.PropertyMock(side_effect=[None, mock_dialect])
    type(mock_plugin_service).dialect = mock_dialect_property
    aliases = frozenset({"instance-1"})

    monitor_service.start_monitoring(
        mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    mock_plugin_service.update_dialect.assert_called_once()
    mock_monitor.start_monitoring.assert_called_once()
    assert mock_monitor == monitor_service._cached_monitor
    assert aliases == monitor_service._cached_monitor_aliases


def test_start_monitoring_cached_monitor(
        monitor_service, mock_plugin_service, mock_monitor, mock_conn, mock_dialect, mock_thread_container):
    aliases = frozenset({"instance-1"})
    monitor_service._cached_monitor = mock_monitor
    monitor_service._cached_monitor_aliases = aliases

    monitor_service.start_monitoring(
        mock_conn, aliases, HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    mock_plugin_service.update_dialect.assert_not_called()
    mock_thread_container.get_or_create_monitor.assert_not_called()
    mock_monitor.start_monitoring.assert_called_once()
    assert mock_monitor == monitor_service._cached_monitor
    assert aliases == monitor_service._cached_monitor_aliases


def test_start_monitoring_errors(monitor_service, mock_conn, mock_plugin_service):
    with pytest.raises(AwsWrapperError):
        monitor_service.start_monitoring(
            mock_conn, frozenset(), HostInfo("instance-1"), Properties(), 5000, 1000, 3)

    mock_plugin_service.dialect = None
    with pytest.raises(AwsWrapperError):
        monitor_service.start_monitoring(
            mock_conn, frozenset({"instance-1"}), HostInfo("instance-1"), Properties(), 5000, 1000, 3)


def test_stop_monitoring(mocker, monitor_service, mock_monitor):
    mock_context = mocker.MagicMock()
    mock_context.monitor = mock_monitor

    monitor_service.stop_monitoring(mock_context)
    mock_monitor.stop_monitoring.assert_called_once()


def test_stop_monitoring_host_connections(mocker, monitor_service, mock_thread_container, mock_monitor):
    alias1 = "alias1"
    alias2 = "alias2"
    aliases = frozenset({alias1, alias2})
    mock_thread_container.get_monitor.side_effect = [None, mock_monitor]

    monitor_service.stop_monitoring_host_connections(aliases)
    mock_monitor.clear_contexts.assert_called_once()
    mock_thread_container.reset_resource.assert_called_once()


def test_release_resources(mocker, monitor_service):
    spy = mocker.spy(MonitorThreadContainer, 'release_instance')

    monitor_service.release_resources()
    assert monitor_service._thread_container is None
    spy.assert_called_once()
